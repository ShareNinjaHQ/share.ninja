//! Backend API client for Share.Ninja

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{TimeZone, Utc};
use thiserror::Error;
use tokio::sync::{Notify, RwLock};

use share_ninja_api::apis::configuration::Configuration;
use share_ninja_api::apis::{
    agents_api, api_keys_api, auth_api, events_api, links_api, rtc_api, shares_api, usage_api, users_api, vault_api,
};
use share_ninja_api::models;

pub(crate) fn fmt_api_error<T: std::fmt::Debug>(e: &share_ninja_api::apis::Error<T>) -> String {
    use share_ninja_api::apis::Error as ApiError;
    match e {
        ApiError::ResponseError(rc) => {
            let trimmed = rc.content.trim();
            if !trimmed.is_empty() {
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(trimmed) {
                    let pick = |k: &str| v.get(k).and_then(|x| x.as_str()).map(str::to_string);
                    if let Some(msg) = pick("status").or_else(|| pick("error")).or_else(|| pick("message")) {
                        return format!("HTTP {}: {}", rc.status.as_u16(), msg);
                    }
                }
                return format!("HTTP {}: {}", rc.status.as_u16(), trimmed);
            }
            format!("HTTP {}", rc.status.as_u16())
        }
        _ => e.to_string(),
    }
}

/// The backend's machine `status` code from an error response body
/// (`transfer_not_found`, `party_offline`, …), when one is present.
pub(crate) fn api_status<T>(e: &share_ninja_api::apis::Error<T>) -> Option<String> {
    if let share_ninja_api::apis::Error::ResponseError(rc) = e
        && let Ok(v) = serde_json::from_str::<serde_json::Value>(rc.content.trim())
    {
        return v.get("status").and_then(|x| x.as_str()).map(str::to_string);
    }
    None
}

/// The backend's machine `code` from an error response body — the promoted
/// domain codes (`storage_quota_exceeded`, …) live here, while `status`
/// carries the granular context string.
pub(crate) fn api_error_code<T>(e: &share_ninja_api::apis::Error<T>) -> Option<String> {
    if let share_ninja_api::apis::Error::ResponseError(rc) = e
        && let Ok(v) = serde_json::from_str::<serde_json::Value>(rc.content.trim())
    {
        return v.get("code").and_then(|x| x.as_str()).map(str::to_string);
    }
    None
}

/// The step-up refusal of an `x-confirm` operation: HTTP 403 with
/// `status: challenge_required`, carrying the token minted for this exact call
/// and the factors on offer. The backend emails the code on its own only when
/// `email_code` is the sole factor; otherwise it waits for
/// `send_step_up_code`. `None` for any other failure.
pub(crate) fn challenge_required<T>(
    e: &share_ninja_api::apis::Error<T>,
) -> Option<(String, String, Vec<ChallengeFactor>)> {
    let share_ninja_api::apis::Error::ResponseError(rc) = e else {
        return None;
    };
    if rc.status.as_u16() != 403 {
        return None;
    }
    let body: models::ChallengeRequiredResponse = serde_json::from_str(rc.content.trim()).ok()?;
    if body.status != models::challenge_required_response::Status::ChallengeRequired {
        return None;
    }
    let token = body.token?;
    let expires_at = body.expires_at.map(|t| t.to_rfc3339()).unwrap_or_default();
    let factors = body
        .factors
        .unwrap_or_default()
        .into_iter()
        .map(|f| ChallengeFactor {
            id: f.id,
            factor_type: f.r#type,
        })
        .collect();
    Some((token, expires_at, factors))
}

/// Error code for a vault join decision: the backend's `join_request_*`
/// machine status verbatim when the request already reached a terminal
/// outcome (decided elsewhere, expired) — the caller treats those as settled,
/// not failed — else `fallback`.
pub(crate) fn join_decision_code<T>(e: &share_ninja_api::apis::Error<T>, fallback: &str) -> String {
    match api_status(e) {
        Some(s) if s.starts_with("join_request_") => s,
        _ => fallback.to_string(),
    }
}

/// Error mapper for session-bound transfer endpoints: a backend
/// `transfer_not_found` (the session lapsed to the stale-cleanup worker or the
/// backend restarted) surfaces verbatim as the `code` so upper layers can
/// classify it as an expired session; a tier-quota refusal passes through the
/// same way (upload presigns are storage-metered); every other failure keeps
/// `fallback`.
pub(crate) fn transfer_api_error<T: std::fmt::Debug>(e: &share_ninja_api::apis::Error<T>, fallback: &str) -> BackError {
    if let Some(t) = transport_error(e) {
        return t;
    }
    let code = match api_status(e).as_deref() {
        Some("transfer_not_found") => "transfer_not_found".to_string(),
        _ => return quota_api_error(e, fallback),
    };
    BackError::Api {
        code,
        message: fmt_api_error(e),
    }
}

/// Error mapper for storage-metered endpoints: the backend's promoted
/// `storage_quota_exceeded` rejection (409 — the owner's tier storage cap)
/// surfaces verbatim as the `code` so upper layers can classify it, and the
/// presign/multipart size rejection (403 with `status:
/// size_limit_exceeded` — a declared or actual size over the link's
/// estimate or the tier's storage cap) is kept the same way; every other
/// failure keeps `fallback`.
pub(crate) fn quota_api_error<T: std::fmt::Debug>(e: &share_ninja_api::apis::Error<T>, fallback: &str) -> BackError {
    if let Some(t) = transport_error(e) {
        return t;
    }
    let code = if api_error_code(e).as_deref() == Some("storage_quota_exceeded") {
        "storage_quota_exceeded".to_string()
    } else if api_status(e).as_deref() == Some("size_limit_exceeded") {
        "size_limit_exceeded".to_string()
    } else {
        fallback.to_string()
    };
    BackError::Api {
        code,
        message: fmt_api_error(e),
    }
}

/// A failure below HTTP — the request never got an answer (connect/read
/// timeout, reset, DNS, a body cut mid-stream). Carried as its own kind so
/// upper layers can tell "the wire died" from "the backend said no".
pub(crate) fn transport_error<T: std::fmt::Debug>(e: &share_ninja_api::apis::Error<T>) -> Option<BackError> {
    use share_ninja_api::apis::Error as ApiError;
    match e {
        ApiError::Reqwest(_) | ApiError::ReqwestMiddleware(_) | ApiError::Io(_) => {
            Some(BackError::Transport(error_chain(e)))
        }
        _ => None,
    }
}

/// An error with its causes, `: `-joined. reqwest's `Display` stops at
/// "error sending request for url (…)" and keeps the cause (timed out, reset,
/// DNS) in `source()`, so a transport failure logged by `Display` alone
/// cannot be told apart from any other.
pub fn error_chain(e: &dyn std::error::Error) -> String {
    let mut text = e.to_string();
    let mut cause = e.source();
    while let Some(c) = cause {
        text.push_str(": ");
        text.push_str(&c.to_string());
        cause = c.source();
    }
    text
}

use crate::secrets::SecureStore;
use crate::utils::result::Result as SharonResult;

use super::ws::{WsConnection, WsHandlers};

/// Backend connection parameters
#[derive(Debug, Clone)]
pub struct BackParams {
    pub base_url: String,
    pub ws_url: String,
    pub oauth_url: Option<String>,
    pub user_agent: String,
    pub device_name: Option<String>,
    pub product_id: String,
    pub product_version: String,
    pub os: String,
    pub os_version: String,
    pub lang: String,
    pub response_mode: String,
    /// Response mode: "cookie" or "token"
    pub device_public_key: Vec<u8>,
    pub device_serial: Option<String>,
    pub device_model: Option<String>,
    /// Physical device form factor ("desktop"|"laptop"|"tablet"|"phone"),
    /// reported by the shell. Mapped to the API enum at registration.
    pub device_form_factor: Option<String>,
    pub log_api: bool,
    /// Initial nearby visibility, sent on every `POST /auth/agents`. `None`
    /// omits the field, leaving whatever the server already stored untouched —
    /// for shells with no such setting. Seeds the runtime value that
    /// [`Back::set_nearby_visibility`] updates.
    pub nearby_visibility: Option<models::NearbyVisibility>,
    /// Opaque id of the client runtime this process is (`POST /auth/agents`
    /// `instance_id`), stamped onto every P2P link it creates so the link's
    /// P2P availability follows this process's socket instead of the whole
    /// agent: it dies with the process, and a new process is a new instance
    /// that does not resurrect the link. `None` for shells whose file access
    /// outlives the process (desktop, mobile) — they re-serve after a restart.
    pub instance_id: Option<String>,
    /// Test-harness hook (the e2e ChaosProxy, docs/app/reconnect.md): dial
    /// `host` at this address instead of resolving it — HTTP client and WS
    /// alike — while the URL, SNI and certificate validation keep the real
    /// hostname. `None` (always, outside the harness) resolves normally.
    #[doc(hidden)]
    pub connect_override: Option<(String, std::net::SocketAddr)>,
}

/// Link creation info
#[derive(Debug, Clone, Default)]
pub struct LinkInfo {
    pub description: Option<String>,
    pub validity: Option<i64>,
    pub cloud_storage: Option<bool>,
    pub p2p_storage: Option<bool>,
    pub estimated_bytes: Option<i64>,
    pub encrypted_metadata: Option<String>,
    /// Owner's own wrapped share key (base64) — stored as the owner's vault
    /// key-store row in the create transaction and returned on `GET /links`
    /// / `GET /links/{url_token}` as `encrypted_share_key`.
    pub encrypted_share_key: Option<String>,
    pub workspace_id: Option<String>,
    /// Email the owner on every download session start (paid tiers only).
    pub notify_on_open: Option<bool>,
    /// Email the owner on every downloaded file (paid tiers only).
    pub notify_on_download: Option<bool>,
}

/// S3 data source configuration
#[derive(Debug, Clone)]
pub struct LinkDataSourceS3 {
    pub datasource_id: String,
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    pub prefix: String,
    pub invalidate_at: String,
    /// The backend's download manifest returned at start. Mirrors the TS
    /// `LinkDataSourceS3.presigned_files`.
    pub presigned_files: Option<Vec<PresignedFile>>,
}

/// A single presigned GET/PUT URL for one object (relative to the link prefix).
/// Mirrors the TS `PresignedFile` / OpenAPI `PresignedFile`.
#[derive(Debug, Clone)]
pub struct PresignedFile {
    pub path: String,
    pub url: String,
    /// Headers folded into the signature (`If-Match`, `If-None-Match`); the
    /// client sends them verbatim or the request fails signature validation.
    pub headers: Vec<(String, String)>,
}

/// What to presign for one path. Mirrors the OpenAPI `TransferFileRequest`
/// options; the conditions are upload-only.
#[derive(Debug, Clone, Default)]
pub struct PresignRequest {
    pub path: String,
    /// Upload: the object's exact byte size (the encrypted bytes actually
    /// PUT). Signed into the URL as Content-Length and admitted against the
    /// owner's storage quota at mint time. `None` on download presigns (the
    /// server rejects it there).
    pub size: Option<u64>,
    /// Upload: S3 `If-Match` — succeed only if the current ETag equals this.
    pub if_match: Option<String>,
    /// Upload: S3 `If-None-Match: *` — succeed only if the key does not exist.
    pub if_none_match: bool,
}

impl PresignRequest {
    pub fn plain(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            ..Default::default()
        }
    }
}

/// One object from a live server-side listing (`list_objects`). Path is relative
/// to the link prefix. Mirrors the TS `ListedObject` / OpenAPI `ListedObject`.
#[derive(Debug, Clone)]
pub struct ListedObject {
    pub path: String,
    pub size_bytes: Option<u64>,
    pub etag: Option<String>,
    pub last_modified: Option<chrono::DateTime<chrono::Utc>>,
}

/// A presigned UploadPart URL for one part of a multipart upload.
/// Mirrors the TS `PresignedPart` / OpenAPI `PresignedPart`.
#[derive(Debug, Clone)]
pub struct PresignedPart {
    pub part_number: u32,
    pub url: String,
}

/// An in-progress multipart upload under the link prefix (resume discovery).
/// Mirrors the TS `MultipartUploadInfo` / OpenAPI `MultipartUpload`.
#[derive(Debug, Clone)]
pub struct MultipartUploadInfo {
    pub path: String,
    pub upload_id: String,
}

/// A part already stored for an open multipart upload (resume).
/// Mirrors the TS `MultipartUploadedPart` / OpenAPI `MultipartUploadedPart`.
#[derive(Debug, Clone)]
pub struct MultipartUploadedPart {
    pub part_number: u32,
    pub etag: String,
    pub size_bytes: u64,
}

/// A completed part `(part_number, etag)` passed to multipart complete.
/// Mirrors the TS `MultipartCompletedPart` / OpenAPI `MultipartCompletedPart`.
#[derive(Debug, Clone)]
pub struct MultipartCompletedPart {
    pub part_number: u32,
    pub etag: String,
}

/// P2P data source configuration
#[derive(Debug, Clone)]
pub struct LinkDataSourceP2P {
    pub datasource_id: String,
    pub agent_id: String,
}

/// Data source (S3 or P2P)
#[derive(Debug, Clone)]
// A transfer start carries at most a handful of these, short-lived — not worth
// boxing the S3 variant over.
#[allow(clippy::large_enum_variant)]
pub enum LinkDataSource {
    S3(LinkDataSourceS3),
    P2P(LinkDataSourceP2P),
}

/// Transfer start response
#[derive(Debug, Clone)]
pub struct TransferStartAnswer {
    pub datasources: Vec<LinkDataSource>,
    pub transfer_id: String,
}

/// Transfer statistics
#[derive(Debug, Clone, Default)]
pub struct TransferStats {
    pub bytes: u64,
    pub files: u64,
    pub dirs: u64,
    pub symlinks: u64,
    /// Elapsed wall-clock time in **milliseconds** — sent as `duration_ms`.
    pub duration: u64,
}

/// Datasource transfer stats
#[derive(Debug, Clone)]
pub struct TransferDSStat {
    pub datasource_id: String,
    pub bytes: u64,
}

/// ICE server configuration
#[derive(Debug, Clone)]
pub struct IceServer {
    pub uri: String,
    pub username: String,
    pub password: String,
}

/// Relay configuration for fallback signaling
#[derive(Debug, Clone)]
pub struct RelayConfig {
    pub send_url: String,
    pub receive_url: String,
}

/// Outgoing call info
#[derive(Debug, Clone)]
pub struct CallOutInfo {
    pub call_id: String,
    pub ice_servers: Vec<IceServer>,
    pub relay: Option<RelayConfig>,
}

/// Incoming call info
#[derive(Debug, Clone)]
pub struct CallInInfo {
    pub call_id: String,
    pub peer_id: Option<String>,
    pub ice_servers: Vec<IceServer>,
    pub url_token: String,
    pub peer_name: Option<String>,
    pub relay: Option<RelayConfig>,
}

impl CallInInfo {
    pub fn from_ws_call(c: models::WsCall) -> Self {
        let ice_servers: Vec<IceServer> = c
            .ice_servers
            .into_iter()
            .map(|s| IceServer {
                uri: s.uri,
                username: s.username.unwrap_or_default(),
                password: s.password.unwrap_or_default(),
            })
            .collect();
        let relay = build_relay_config(c.relays.as_ref(), &c.jwt);
        Self {
            call_id: c.call_id,
            peer_id: Some(c.caller_id),
            ice_servers,
            url_token: c.url_token,
            peer_name: c.caller_name,
            relay,
        }
    }
}

/// Build relay config from relay URLs and a JWT token.
fn build_relay_config(relays: Option<&Vec<String>>, jwt: &str) -> Option<RelayConfig> {
    let relay_base = relays.and_then(|r| r.first())?;
    let base = relay_base.trim_end_matches('/');
    Some(RelayConfig {
        send_url: format!("{}/send?token={}", base, jwt),
        receive_url: format!("{}/receive?token={}", base, jwt),
    })
}

/// Nearby agent info
#[derive(Debug, Clone)]
pub struct BackNearby {
    pub agent_id: String,
    pub name: String,
    pub description: String,
    pub device_public_key: Option<Vec<u8>>,
    pub fingerprint: Option<String>,
}

/// Authentication challenge: the first step of a login, any one of its
/// sign-in methods completes it.
#[derive(Debug, Clone)]
pub struct Challenge {
    pub token: String,
    pub expires_at: String,
    pub sign_in_methods: Vec<ChallengeFactor>,
}

/// Challenge factor
#[derive(Debug, Clone)]
pub struct ChallengeFactor {
    pub id: String,
    pub factor_type: String,
}

/// Backend connection state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackState {
    NoConnection,
    Connecting,
    Online,
    OnlineAndLoggedIn,
}

impl BackState {
    pub fn is_online(self) -> bool {
        matches!(self, BackState::Online | BackState::OnlineAndLoggedIn)
    }
}

/// One registered `BackState` observer.
pub(crate) type StateCallback = Arc<dyn Fn(BackState) + Send + Sync>;
/// The shared observer list, cloned into `AuthManager` and the reconnect tasks.
pub(crate) type StateCallbacks = Arc<RwLock<Vec<StateCallback>>>;

/// Backend errors
#[derive(Error, Debug)]
pub enum BackError {
    #[error("HTTP error: {0}")]
    Http(String),

    #[error("JSON error: {0}")]
    Json(String),

    #[error("WebSocket error: {0}")]
    WebSocket(String),

    #[error("Backend offline")]
    Offline,

    /// The request never got an answer (see `transport_error`).
    #[error("transport error: {0}")]
    Transport(String),

    #[error("API error: {code} - {message}")]
    Api { code: String, message: String },

    /// An `x-confirm` operation refused pending step-up: verify the emailed
    /// code against `token` (`verify_step_up`) and retry the same call with the
    /// confirmed token. The token is bound to the exact request body.
    #[error("step-up confirmation required")]
    ChallengeRequired {
        token: String,
        expires_at: String,
        /// Factors the challenge accepts, any one of them. More than one means
        /// the email code was not sent with the refusal (see `send_step_up_code`).
        factors: Vec<ChallengeFactor>,
    },

    #[error("Other error: {0}")]
    Other(String),

    /// The register handshake was terminated by an authoritative credential
    /// change (OAuth completion, logout) that registers again itself.
    #[error("register superseded by a credential change")]
    Superseded,
}

/// Error mapper for `x-confirm` endpoints: the step-up refusal becomes
/// `ChallengeRequired`, anything else the usual `Api` envelope.
fn step_up_error<T: std::fmt::Debug>(code: &str, e: &share_ninja_api::apis::Error<T>) -> BackError {
    if let Some((token, expires_at, factors)) = challenge_required(e) {
        return BackError::ChallengeRequired { token, expires_at, factors };
    }
    BackError::Api {
        code: code.to_string(),
        message: fmt_api_error(e),
    }
}

impl BackError {
    /// True when the backend reported the transfer session as gone
    /// (`transfer_not_found`) — the recoverable "reconnect and retry" case.
    pub fn is_session_expired(&self) -> bool {
        matches!(self, BackError::Api { code, .. } if code == "transfer_not_found")
    }

    /// True when the backend was not reached at all: not connected
    /// (`Offline`), or the wire died under the call. Nothing about the
    /// session or the operation can be concluded from it.
    pub fn is_transport(&self) -> bool {
        matches!(self, BackError::Offline | BackError::Http(_) | BackError::Transport(_))
    }

    /// True when the backend refused a storage-metered call over storage
    /// limits: the tier quota rejection (`409 storage_quota_exceeded` — the
    /// link owner's cloud storage cap is reached), or the presign/multipart
    /// size rejection (`403 size_limit_exceeded` — a declared or actual size
    /// over the link's estimate or that same cap).
    pub fn is_quota_exceeded(&self) -> bool {
        matches!(self, BackError::Api { code, .. }
            if code == "storage_quota_exceeded" || code == "size_limit_exceeded")
    }

    /// True when the backend refused a call addressed to another agent
    /// because that agent is not online (`403 party_offline`).
    pub fn is_party_offline(&self) -> bool {
        matches!(self, BackError::Api { code, .. } if code == "party_offline")
    }
}

impl From<reqwest::Error> for BackError {
    fn from(e: reqwest::Error) -> Self {
        BackError::Http(e.to_string())
    }
}

impl From<serde_json::Error> for BackError {
    fn from(e: serde_json::Error) -> Self {
        BackError::Json(e.to_string())
    }
}

pub struct Back {
    pub params: BackParams,
    pub agent_id: String,
    pub agent_name: String,
    pub ws_ticket: Option<String>,
    /// `GET /me` as last fetched by the register handshake (refreshed by
    /// `refresh_me`): the tier, its limits and the usage.
    /// `None` until the first successful fetch.
    pub me: Option<models::Me>,
    /// The signed-in user: `me.user` at fetch time, then kept current by the
    /// auth flows that return a user (challenge completion, profile update).
    pub user: Option<models::User>,
    pub vault_status: Option<String>,
    pub encrypted_identity_key: Option<Vec<u8>>,
    shared_state: Arc<std::sync::Mutex<BackState>>,
    /// Mirror of `shared_state` for `state_watch()` subscribers.
    state_tx: tokio::sync::watch::Sender<BackState>,
    fail_timer_sec: u64,
    state_callbacks: StateCallbacks,
    auth: Arc<super::auth::AuthManager>,
    /// The one connection pool every request shares: the API client
    /// (`api_config`) and the auth manager's refresh client reach it through
    /// the `PoolRouter` at the end of their middleware stacks,
    /// `S3Storage` takes it per request. Connect timeout only — the
    /// API side bounds each call as a whole (`RequestDeadline`), the S3 side
    /// watches its own read and send progress. Behind its own lock so a
    /// network path change replaces it under `&self` (`renew_http`): a
    /// `back.write()` there would queue behind every call holding a read
    /// guard over a request stuck on the old path, and a queued writer
    /// stalls every new reader until they time out.
    http: Arc<std::sync::RwLock<reqwest::Client>>,
    api_config: Configuration,
    api_logger: super::logging_client::ApiLogger,
    /// The live WS, under its own lock for the same reason as `http`: a
    /// path change drops it under `&self`. `Arc` so a send in flight keeps
    /// the connection alive until it is done; the last clone's drop aborts
    /// the read loop.
    ws_connection: std::sync::Mutex<Option<Arc<WsConnection>>>,
    ws_handlers: Arc<RwLock<WsHandlers>>,
    network_change: Arc<Notify>,
    /// See [`RegisterTasks`]; `reconnecting` is the reconnect loop's
    /// single-instance latch — true exactly while a loop is alive, reset
    /// only by the loop itself on exit.
    register_tasks: Arc<std::sync::Mutex<RegisterTasks>>,
    reconnecting: Arc<AtomicBool>,
    /// The backend's last 429 deadline (see [`RateLimit`]).
    rate_limit: Arc<super::debug_middleware::RateLimit>,
    /// Runtime nearby visibility, seeded from `params.nearby_visibility` and
    /// re-read by every register. Interior mutability so the UI toggle can
    /// update it through `&self`, like `log_api` via the `ApiLogger`.
    nearby_visibility: std::sync::Mutex<Option<models::NearbyVisibility>>,
}

pub(crate) fn apply_state_change(
    shared_state: &std::sync::Mutex<BackState>,
    state_callbacks: &RwLock<Vec<StateCallback>>,
    state: BackState,
) -> bool {
    {
        let mut s = shared_state.lock().unwrap_or_else(|e| e.into_inner());
        if *s == state {
            return false;
        }
        *s = state;
    }
    tracing::debug!("Back state -> {:?}", state);
    if let Ok(callbacks) = state_callbacks.try_read() {
        for cb in callbacks.iter() {
            cb(state);
        }
    }
    true
}

/// The register handshakes in flight (each `run_register` caller's). An
/// authoritative credential change (OAuth completion, logout) terminates
/// them before registering with the new credentials — a handshake started
/// under the old ones would otherwise land later and overwrite them in
/// `apply_register`. The reconnect loop is not in here: it is never aborted
/// (its own handshake is, and it retries with the current credentials).
#[derive(Default)]
struct RegisterTasks {
    handshakes: Vec<tokio::task::AbortHandle>,
}

fn spawn_reconnect(
    weak: Weak<RwLock<Back>>,
    reconnecting: Arc<AtomicBool>,
    base_delay_sec: u64,
    network_change: Arc<Notify>,
    rate_limit: Arc<super::debug_middleware::RateLimit>,
) {
    tokio::spawn(async move {
        let base = Duration::from_secs(base_delay_sec.max(1));
        let max_backoff = Duration::from_secs(60);
        let mut backoff = base;
        loop {
            tracing::info!(backoff_secs = backoff.as_secs(), "Back: reconnecting after backoff");
            tokio::select! {
                _ = tokio::time::sleep(backoff) => {}
                _ = network_change.notified() => {
                    tracing::info!("Back: network-change wake, retrying immediately");
                    backoff = base;
                }
            }

            let Some(back) = weak.upgrade() else {
                tracing::debug!("Back: dropped, ending reconnect loop");
                return;
            };

            if matches!(
                back.read().await.current_state(),
                BackState::Online | BackState::OnlineAndLoggedIn
            ) {
                tracing::debug!("Back: already online, ending reconnect loop");
                reconnecting.store(false, Ordering::SeqCst);
                return;
            }

            let result = Back::run_register(back.clone()).await;

            match result {
                Ok(()) => {
                    tracing::info!("Back: reconnected successfully");
                    reconnecting.store(false, Ordering::SeqCst);
                    return;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Back: reconnect attempt failed");
                    // Throttled: the backend said when to come back, and
                    // doubling our own guess only stretches the outage.
                    backoff = match rate_limit.remaining() {
                        Some(wait) => wait,
                        None => (backoff * 2).min(max_backoff),
                    };
                }
            }
        }
    });
}

/// The connection pool: connect timeout, keep-alive below the server's, the
/// Android TLS roots and the harness's dial override. No read timeout — see
/// `Back::http`.
fn build_http(params: &BackParams) -> reqwest::Client {
    #[cfg_attr(not(target_os = "android"), allow(unused_mut))]
    let mut builder = reqwest::Client::builder()
        .user_agent(&params.user_agent)
        // Below the server-side keep-alive timeout (~60s), so we never
        // reuse a pooled connection the server has already closed.
        .pool_idle_timeout(std::time::Duration::from_secs(50))
        .connect_timeout(std::time::Duration::from_secs(15));
    // Android only: webpki verification dodges Android's OCSP-based
    // revocation rejecting the backend's Let's Encrypt cert.
    #[cfg(target_os = "android")]
    {
        builder = builder.use_preconfigured_tls(super::webpki_client_config());
    }
    if let Some((host, addr)) = &params.connect_override {
        builder = builder.resolve(host, *addr);
    }
    builder.build().expect("Failed to build reqwest client")
}

/// The API client over the pool: token refresh + bearer, request ids, the
/// per-call deadline, the optional wire log, and last the `PoolRouter`
/// that runs the request on the pool as it is now.
fn api_stack(
    pool: Arc<std::sync::RwLock<reqwest::Client>>,
    auth: &Arc<super::auth::AuthManager>,
    rate_limit: &Arc<super::debug_middleware::RateLimit>,
    log_api: bool,
) -> reqwest_middleware::ClientWithMiddleware {
    let seed = pool.read().unwrap_or_else(|e| e.into_inner()).clone();
    let mut builder = reqwest_middleware::ClientBuilder::new(seed)
        .with(super::debug_middleware::AuthMiddleware {
            auth: auth.clone(),
            rate_limit: rate_limit.clone(),
        })
        .with(super::debug_middleware::RequestIdMiddleware)
        .with(super::debug_middleware::RequestDeadline);
    if log_api {
        builder = builder.with(super::debug_middleware::DebugLoggingMiddleware);
    }
    builder.with(super::debug_middleware::PoolRouter { pool }).build()
}

impl Back {
    /// `secure_store` is the refresh token's only home (see `AuthManager`):
    /// the device secrets the shell wired up, shared by every `Back` that runs
    /// against the same keychain item.
    pub fn new(params: BackParams, secure_store: Arc<SecureStore>) -> Self {
        let base_path = params.base_url.clone();
        let user_agent = params.user_agent.clone();

        let shared_state = Arc::new(std::sync::Mutex::new(BackState::NoConnection));
        // Every transition also lands on a `watch` channel: a loop that has
        // to wait for `Online` (transfer keepalive, P2P reconnect) holds a
        // receiver and drops it when it is done — unlike `add_state_callback`,
        // whose observers are never removed.
        let (state_tx, _) = tokio::sync::watch::channel(BackState::NoConnection);
        let watch_tx = state_tx.clone();
        let forward_to_watch: StateCallback = Arc::new(move |s: BackState| {
            // `send_replace`, not `send`: `send` drops the value when no
            // receiver exists yet, and the first subscriber would then wait
            // on a stale `NoConnection` forever.
            watch_tx.send_replace(s);
        });
        let state_callbacks: StateCallbacks = Arc::new(RwLock::new(vec![forward_to_watch]));

        let http = Arc::new(std::sync::RwLock::new(build_http(&params)));
        let auth = super::auth::AuthManager::new(
            &params,
            http.clone(),
            secure_store,
            shared_state.clone(),
            state_callbacks.clone(),
        );
        let rate_limit = Arc::new(super::debug_middleware::RateLimit::default());

        let api_config = Configuration {
            base_path,
            user_agent: Some(user_agent),
            client: api_stack(http.clone(), &auth, &rate_limit, params.log_api),
            ..Configuration::default()
        };

        let api_logger = super::logging_client::ApiLogger::new(params.log_api);
        let nearby_visibility = std::sync::Mutex::new(params.nearby_visibility);

        Self {
            params,
            agent_id: String::new(),
            agent_name: String::new(),
            ws_ticket: None,
            me: None,
            user: None,
            vault_status: None,
            encrypted_identity_key: None,
            shared_state,
            state_tx,
            fail_timer_sec: 5,
            state_callbacks,
            auth,
            http,
            api_config,
            api_logger,
            ws_connection: std::sync::Mutex::new(None),
            ws_handlers: Arc::new(RwLock::new(WsHandlers::new())),
            network_change: Arc::new(Notify::new()),
            register_tasks: Arc::new(std::sync::Mutex::new(RegisterTasks::default())),
            reconnecting: Arc::new(AtomicBool::new(false)),
            rate_limit,
            nearby_visibility,
        }
    }

    pub async fn new_shared(params: BackParams, secure_store: Arc<SecureStore>) -> Arc<RwLock<Back>> {
        let back = Arc::new(RwLock::new(Back::new(params, secure_store)));
        let (base_delay, network_change, reconnecting, rate_limit) = {
            let g = back.read().await;
            (
                g.fail_timer_sec,
                g.network_change.clone(),
                g.reconnecting.clone(),
                g.rate_limit.clone(),
            )
        };
        let weak = Arc::downgrade(&back);
        back.read()
            .await
            .add_state_callback(move |state| {
                if state == BackState::NoConnection && !reconnecting.swap(true, Ordering::SeqCst) {
                    spawn_reconnect(
                        weak.clone(),
                        reconnecting.clone(),
                        base_delay,
                        network_change.clone(),
                        rate_limit.clone(),
                    );
                }
            })
            .await;
        back
    }

    /// Current access token (refreshed lazily by the auth middleware).
    pub fn access_token(&self) -> Option<String> {
        self.auth.access_token()
    }

    /// The refresh token the secure store holds right now.
    pub fn refresh_token(&self) -> Option<String> {
        self.auth.refresh_token()
    }

    /// Enable or disable API logging at runtime
    pub fn set_log_api(&self, enabled: bool) {
        self.api_logger.set_enabled(enabled);
    }

    /// Check if API logging is enabled
    pub fn is_log_api_enabled(&self) -> bool {
        self.api_logger.is_enabled()
    }

    /// Log an API call (request and outcome; bodies are the middleware's)
    fn log_api_call<T, E: std::fmt::Debug>(&self, method: &str, endpoint: &str, result: &Result<T, E>) {
        self.api_logger.log_request(method, endpoint);
        self.api_logger.log_response(method, endpoint, result);
    }

    pub fn is_online(&self) -> bool {
        self.current_state().is_online()
    }

    fn check_online(&self) -> Result<(), BackError> {
        if !self.is_online() {
            return Err(BackError::Offline);
        }
        Ok(())
    }

    /// Read the current state (safe to call from any context, including async).
    pub fn current_state(&self) -> BackState {
        *self.shared_state.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn set_state(&self, state: BackState) {
        self.transition_to_state(state);
    }

    /// Connectivity likely returned (app foreground, machine wake, a path
    /// that became satisfied): wake a reconnect waiting out its backoff.
    /// No-op while connected — a "maybe" signal must not touch a live
    /// connection.
    pub fn notify_network_change(&self) {
        self.network_change.notify_one();
    }

    pub fn network_change_notifier(&self) -> Arc<Notify> {
        self.network_change.clone()
    }

    /// The platform saw the network path change under us (Wi-Fi to
    /// cellular, a new default network). Every socket opened on the old path
    /// is a black hole now, not an error: the WS would stay "Online" until
    /// its keepalive runs out and every pooled HTTP connection would eat one
    /// request's deadline before leaving the pool. So the pool is replaced
    /// whatever the state (a handshake in flight on the old path fails on
    /// its deadline and retries on the new pool), and, while connected, the
    /// change is a connection loss on purpose: the WS goes (its `Drop` ends
    /// the read loop), the state drops to `NoConnection` — which starts the
    /// reconnect loop — and the loop is handed the wake so it registers now
    /// instead of after its backoff. While not connected it is the plain
    /// wake. `&self`, under a read guard: never a `back.write()` here — see
    /// `http`.
    pub fn network_path_changed(&self) {
        self.renew_http();
        if !self.is_online() {
            self.network_change.notify_one();
            return;
        }
        tracing::info!("Back: network path changed; reconnecting on the new path");
        self.ws_connection.lock().unwrap_or_else(|e| e.into_inner()).take();
        self.transition_to_state(BackState::NoConnection);
        self.network_change.notify_one();
    }

    /// Replace the pool so nothing opened on the old network path is
    /// reused; the middleware stacks route to it through `PoolRouter`.
    /// Requests in flight keep their own clone of the old client and fail
    /// on their own bound.
    fn renew_http(&self) {
        *self.http.write().unwrap_or_else(|e| e.into_inner()) = build_http(&self.params);
    }

    /// The pool, for a request the storage layer issues itself (S3 GET /
    /// PUT). A clone, so the caller never holds `Back` across the request.
    pub fn http(&self) -> reqwest::Client {
        self.http.read().unwrap_or_else(|e| e.into_inner()).clone()
    }

    fn transition_to_state(&self, state: BackState) {
        apply_state_change(&self.shared_state, &self.state_callbacks, state);
    }

    pub async fn add_state_callback<F>(&self, callback: F)
    where
        F: Fn(BackState) + Send + Sync + 'static,
    {
        self.state_callbacks.write().await.push(Arc::new(callback));
    }

    /// A receiver of every state transition, current value first. The
    /// leak-free way to wait for `Online` from a long-lived loop:
    /// `rx.wait_for(|s| s.is_online()).await`.
    pub fn state_watch(&self) -> tokio::sync::watch::Receiver<BackState> {
        // `shared_state` is the truth; the callback path that mirrors it can
        // skip a transition (`try_read` under a queued writer), so a
        // subscriber starts from the truth, not from the mirror.
        self.state_tx.send_replace(self.current_state());
        self.state_tx.subscribe()
    }

    pub async fn link(&self, link_info: &LinkInfo) -> Result<models::LinksCreate200Response, BackError> {
        self.check_online()?;

        tracing::debug!(
            "Starting link for '{:?}', cloud {:?}, p2p {:?}, validity {:?}, estimated_bytes {:?}",
            link_info.description,
            link_info.cloud_storage,
            link_info.p2p_storage,
            link_info.validity,
            link_info.estimated_bytes
        );

        let encrypted_metadata = link_info
            .encrypted_metadata
            .as_deref()
            .map(crate::utils::base64::from_base64)
            .transpose()
            .map_err(|e| BackError::Api {
                code: "link_error".to_string(),
                message: format!("invalid encrypted_metadata base64: {e}"),
            })?;

        let encrypted_share_key = link_info
            .encrypted_share_key
            .as_deref()
            .map(crate::utils::base64::from_base64)
            .transpose()
            .map_err(|e| BackError::Api {
                code: "link_error".to_string(),
                message: format!("invalid encrypted_share_key base64: {e}"),
            })?;

        // A P2P link from an instance-scoped runtime is bound to that instance
        // (see `BackParams::instance_id`). The server rejects `instance_id`
        // without `p2p_storage`, hence the gate; an omitted `p2p_storage`
        // defaults to true on the server.
        let instance_id = if link_info.p2p_storage.unwrap_or(true) {
            self.params.instance_id.clone()
        } else {
            None
        };
        let request = models::LinksCreateRequest {
            description: link_info.description.clone(),
            validity: link_info.validity.map(|v| v as i32),
            cloud_storage: link_info.cloud_storage,
            p2p_storage: link_info.p2p_storage,
            public: None,
            provider: Some("z1".to_string()),
            estimated_bytes: link_info.estimated_bytes,
            encrypted_metadata,
            encrypted_share_key,
            tmk_generation: None,
            tmk_encrypted_share_key: None,
            workspace_id: link_info.workspace_id.clone(),
            instance_id,
            download_limit: None,
            notify_on_open: link_info.notify_on_open,
            notify_on_download: link_info.notify_on_download,
        };

        let result = links_api::links_create(&self.api_config, request.clone()).await;
        self.log_api_call("POST", "/links", &result);

        result.map_err(|e| quota_api_error(&e, "link_error"))
    }

    /// Patch an existing link (partial — only `Some` fields are sent). Used to set
    /// `estimated_bytes` + enriched metadata once the source size is known. A
    /// re-declared `estimated_bytes` is measured against the owner's storage
    /// cap — the 409 quota refusal keeps its machine code for the caller.
    pub async fn link_update(&self, url_token: &str, request: models::LinkUpdateRequest) -> Result<(), BackError> {
        let result = links_api::link_update(&self.api_config, url_token, request.clone()).await;
        self.log_api_call("PATCH", &format!("/links/{}", url_token), &result);
        result.map_err(|e| quota_api_error(&e, "link_update_error"))
    }

    /// Get link details by URL token. With `claim_token_hash`, `encrypted_share_key`
    /// comes from the claim (wrapped with HKDF(claim_token)) instead of the caller's
    /// user-subject grant. Mirrors the TS `Back.linkGet()`.
    pub async fn link_get(&self, url_token: &str, claim_token_hash: Option<&str>) -> Result<models::Link, BackError> {
        self.check_online()?;
        tracing::debug!("linkGet for {}", url_token);

        let result = links_api::links_get(&self.api_config, url_token, claim_token_hash).await;
        self.log_api_call("GET", &format!("/links/{}", url_token), &result);

        result.map_err(|e| BackError::Api {
            code: "link_get_error".to_string(),
            message: fmt_api_error(&e),
        })
    }

    pub async fn upload_start(&self, url_token: &str) -> Result<TransferStartAnswer, BackError> {
        self.check_online()?;
        tracing::debug!("uploadStart for {}", url_token);

        // Presigned mode (the only one this client speaks): request presigned
        // URLs, no files up front (minted on demand).
        let body = Some(models::TransferStartRequest {
            presigned: Some(true),
            files: Some(Vec::new()),
        });
        let result = links_api::links_upload_start(&self.api_config, url_token, body.clone()).await;
        self.log_api_call("POST", &format!("/links/{}/upload/start", url_token), &result);

        let response = result.map_err(|e| quota_api_error(&e, "upload_start_error"))?;

        Ok(TransferStartAnswer {
            transfer_id: response.transfer_id,
            datasources: self.convert_datasources(&response.datasources),
        })
    }

    pub async fn download_start(&self, url_token: &str) -> Result<TransferStartAnswer, BackError> {
        self.check_online()?;
        tracing::debug!("downloadStart for {}", url_token);

        // Presigned mode (the only one this client speaks): the server returns
        // the presigned manifest. Files are ignored on download.
        let body = Some(models::TransferStartRequest {
            presigned: Some(true),
            files: None,
        });
        let result = links_api::links_download_start(&self.api_config, url_token, body.clone()).await;
        self.log_api_call("POST", &format!("/links/{}/download/start", url_token), &result);

        let response = result.map_err(|e| {
            // Log the full error details for debugging
            tracing::debug!("download_start error details: {:?}", e);
            transfer_api_error(&e, "download_start_error")
        })?;

        Ok(TransferStartAnswer {
            transfer_id: response.transfer_id,
            datasources: self.convert_datasources(&response.datasources),
        })
    }

    /// Presigned mode: mint fresh single-shot GET URLs for the named paths
    /// (relative to the link prefix). The alternative to STS creds — the client
    /// then does plain HTTP against the returned URLs. Callable repeatedly to
    /// renew a URL that expired mid-transfer. Mirrors the TS `Back.downloadPresign`.
    pub async fn download_presign(
        &self,
        url_token: &str,
        files: &[PresignRequest],
    ) -> Result<Vec<PresignedFile>, BackError> {
        self.check_online()?;

        let request = models::PresignFilesRequest {
            files: files.iter().map(presign_request_to_model).collect(),
        };
        let result = links_api::links_download_presign(&self.api_config, url_token, request.clone()).await;
        self.log_api_call("POST", &format!("/links/{}/download/presign", url_token), &result);

        let response = result.map_err(|e| transfer_api_error(&e, "download_presign_error"))?;

        Ok(response.files.into_iter().map(convert_presigned_file).collect())
    }

    /// Presigned mode: mint fresh single-shot PUT URLs for the named paths.
    /// Mirrors the TS `Back.uploadPresign`. `if_match` / `if_none_match` on a
    /// request are signed into the URL and come back in `PresignedFile::headers`.
    pub async fn upload_presign(
        &self,
        url_token: &str,
        files: &[PresignRequest],
    ) -> Result<Vec<PresignedFile>, BackError> {
        self.check_online()?;

        let request = models::PresignFilesRequest {
            files: files.iter().map(presign_request_to_model).collect(),
        };
        let result = links_api::links_upload_presign(&self.api_config, url_token, request.clone()).await;
        self.log_api_call("POST", &format!("/links/{}/upload/presign", url_token), &result);

        let response = result.map_err(|e| transfer_api_error(&e, "upload_presign_error"))?;

        Ok(response.files.into_iter().map(convert_presigned_file).collect())
    }

    /// Live server-side ListObjectsV2 under the link prefix, one page per call
    /// (paginate by passing the returned cursor back). Paths are relative to the
    /// link prefix; returned paths are relative to it too. `start_after`
    /// (exclusive, first page only) begins the listing after a known key.
    /// Mirrors the TS `Back.listObjects`.
    pub async fn list_objects(
        &self,
        url_token: &str,
        prefix: &str,
        cursor: Option<&str>,
        start_after: Option<&str>,
    ) -> Result<(Vec<ListedObject>, Option<String>), BackError> {
        self.check_online()?;

        let request = models::ListObjectsRequest {
            prefix: Some(prefix.to_string()),
            cursor: cursor.map(|c| c.to_string()),
            start_after: start_after.map(|c| c.to_string()),
        };
        let result = links_api::links_list_objects(&self.api_config, url_token, request.clone()).await;
        self.log_api_call("POST", &format!("/links/{}/list", url_token), &result);

        let response = result.map_err(|e| BackError::Api {
            code: "list_objects_error".to_string(),
            message: fmt_api_error(&e),
        })?;

        let objects = response
            .objects
            .into_iter()
            .map(|o| ListedObject {
                path: o.path,
                size_bytes: o.size_bytes.map(|s| s as u64),
                etag: o.etag,
                last_modified: o.last_modified.map(|t| t.with_timezone(&chrono::Utc)),
            })
            .collect();
        Ok((objects, response.next_cursor))
    }

    /// Start a multipart upload for one large file; returns the `upload_id`.
    /// Mirrors the TS `Back.multipartBegin`.
    pub async fn multipart_begin(&self, url_token: &str, path: &str) -> Result<String, BackError> {
        self.check_online()?;

        // TODO(quota-admission): declare the object's total byte size (the
        // post-encryption object bytes) once the upload paths plumb it here —
        // the contract admits size-less begins as earlier-contract clients.
        let request = models::MultipartBeginRequest {
            path: path.to_string(),
            size: None,
        };
        let result = links_api::links_upload_multipart_begin(&self.api_config, url_token, request.clone()).await;
        self.log_api_call("POST", &format!("/links/{}/upload/multipart/begin", url_token), &result);

        let response = result.map_err(|e| quota_api_error(&e, "multipart_begin_error"))?;

        Ok(response.upload_id)
    }

    /// Mint presigned UploadPart URLs for the named part numbers of an open
    /// multipart upload. Mirrors the TS `Back.multipartParts`.
    pub async fn multipart_parts(
        &self,
        url_token: &str,
        path: &str,
        upload_id: &str,
        part_numbers: &[u32],
    ) -> Result<Vec<PresignedPart>, BackError> {
        self.check_online()?;

        let request = models::MultipartPartsRequest {
            path: path.to_string(),
            upload_id: upload_id.to_string(),
            part_numbers: part_numbers.iter().map(|n| *n as i32).collect(),
        };
        let result = links_api::links_upload_multipart_parts(&self.api_config, url_token, request.clone()).await;
        self.log_api_call("POST", &format!("/links/{}/upload/multipart/parts", url_token), &result);

        let response = result.map_err(|e| quota_api_error(&e, "multipart_parts_error"))?;

        Ok(response
            .part_urls
            .into_iter()
            .map(|p| PresignedPart {
                part_number: p.part_number as u32,
                url: p.url,
            })
            .collect())
    }

    /// Complete a multipart upload with the collected `(part_number, etag)` list;
    /// returns the assembled object's ETag. Mirrors the TS `Back.multipartComplete`.
    pub async fn multipart_complete(
        &self,
        url_token: &str,
        path: &str,
        upload_id: &str,
        parts: &[MultipartCompletedPart],
    ) -> Result<String, BackError> {
        self.check_online()?;

        let request = models::MultipartCompleteRequest {
            path: path.to_string(),
            upload_id: upload_id.to_string(),
            parts: parts
                .iter()
                .map(|p| models::MultipartCompletedPart {
                    part_number: p.part_number as i32,
                    etag: p.etag.clone(),
                })
                .collect(),
        };
        let result = links_api::links_upload_multipart_complete(&self.api_config, url_token, request.clone()).await;
        self.log_api_call(
            "POST",
            &format!("/links/{}/upload/multipart/complete", url_token),
            &result,
        );

        let response = result.map_err(|e| quota_api_error(&e, "multipart_complete_error"))?;

        Ok(response.etag)
    }

    /// Delete one object under the link prefix (server calls S3 DeleteObject).
    /// Idempotent server-side. Mirrors the TS `Back.deleteObject`.
    pub async fn delete_object(&self, url_token: &str, path: &str) -> Result<(), BackError> {
        self.check_online()?;

        let request = models::DeleteObjectRequest { path: path.to_string() };
        let result = links_api::links_upload_delete(&self.api_config, url_token, request.clone()).await;
        self.log_api_call("POST", &format!("/links/{}/upload/delete", url_token), &result);

        result.map_err(|e| BackError::Api {
            code: "delete_object_error".to_string(),
            message: fmt_api_error(&e),
        })
    }

    /// Abort an open multipart upload (server calls S3 AbortMultipartUpload).
    /// Mirrors the TS `Back.multipartAbort`.
    pub async fn multipart_abort(&self, url_token: &str, path: &str, upload_id: &str) -> Result<(), BackError> {
        self.check_online()?;

        let request = models::MultipartAbortRequest {
            path: path.to_string(),
            upload_id: upload_id.to_string(),
        };
        let result = links_api::links_upload_multipart_abort(&self.api_config, url_token, request.clone()).await;
        self.log_api_call("POST", &format!("/links/{}/upload/multipart/abort", url_token), &result);

        result.map_err(|e| BackError::Api {
            code: "multipart_abort_error".to_string(),
            message: fmt_api_error(&e),
        })
    }

    /// List the parts already stored for one open multipart upload (resume).
    /// Mirrors the TS `Back.multipartListParts`.
    pub async fn multipart_list_parts(
        &self,
        url_token: &str,
        path: &str,
        upload_id: &str,
    ) -> Result<Vec<MultipartUploadedPart>, BackError> {
        self.check_online()?;

        let request = models::MultipartListPartsRequest {
            path: path.to_string(),
            upload_id: upload_id.to_string(),
        };
        let result = links_api::links_upload_multipart_list_parts(&self.api_config, url_token, request.clone()).await;
        self.log_api_call(
            "POST",
            &format!("/links/{}/upload/multipart/list-parts", url_token),
            &result,
        );

        let response = result.map_err(|e| BackError::Api {
            code: "multipart_list_parts_error".to_string(),
            message: fmt_api_error(&e),
        })?;

        Ok(response
            .parts
            .into_iter()
            .map(|p| MultipartUploadedPart {
                part_number: p.part_number as u32,
                etag: p.etag,
                size_bytes: p.size_bytes as u64,
            })
            .collect())
    }

    /// List the in-progress multipart uploads under the link prefix (resume
    /// discovery). Mirrors the TS `Back.multipartListUploads`.
    pub async fn multipart_list_uploads(&self, url_token: &str) -> Result<Vec<MultipartUploadInfo>, BackError> {
        self.check_online()?;

        let result = links_api::links_upload_multipart_list_uploads(&self.api_config, url_token).await;
        self.log_api_call(
            "POST",
            &format!("/links/{}/upload/multipart/list-uploads", url_token),
            &result,
        );

        let response = result.map_err(|e| BackError::Api {
            code: "multipart_list_uploads_error".to_string(),
            message: fmt_api_error(&e),
        })?;

        Ok(response
            .uploads
            .into_iter()
            .map(|u| MultipartUploadInfo {
                path: u.path,
                upload_id: u.upload_id,
            })
            .collect())
    }

    /// Explicitly end a transfer session server-side (`{direction}/abort`).
    /// The e2e harness uses it as the fast path to a reaped session — the
    /// same 404s a `ReapAbandonedTransfers` pass would leave behind.
    pub async fn transfer_abort(
        &self,
        is_download: bool,
        url_token: &str,
        transfer_id: &str,
        reason: Option<&str>,
    ) -> Result<(), BackError> {
        self.check_online()?;
        let request = models::TransferAbortRequest {
            transfer_id: transfer_id.to_string(),
            reason: reason.map(str::to_string),
        };
        if is_download {
            let result = links_api::links_download_abort(&self.api_config, url_token, request.clone()).await;
            self.log_api_call("POST", &format!("/links/{}/download/abort", url_token), &result);
            result.map_err(|e| transfer_api_error(&e, "transfer_abort_error"))
        } else {
            let result = links_api::links_upload_abort(&self.api_config, url_token, request.clone()).await;
            self.log_api_call("POST", &format!("/links/{}/upload/abort", url_token), &result);
            result.map_err(|e| transfer_api_error(&e, "transfer_abort_error"))
        }
    }

    pub async fn transfer_update(
        &self,
        is_download: bool,
        url_token: &str,
        transfer_id: &str,
        stats: &TransferStats,
        ds_stats: &[TransferDSStat],
    ) -> Result<(), BackError> {
        self.check_online()?;
        tracing::debug!("transferUpdate for {}", transfer_id);

        let request = models::TransferUpdateRequest {
            transfer_id: transfer_id.to_string(),
            stats: Some(Box::new(models::TransferStats {
                bytes_transferred: Some(stats.bytes as i64),
                files_completed: Some(stats.files as i32),
                files_total: Some((stats.files + stats.dirs) as i32),
                dirs: Some(stats.dirs as i32),
                symlinks: Some(stats.symlinks as i32),
                duration_ms: Some(stats.duration as i32),
            })),
            datasources: Some(
                ds_stats
                    .iter()
                    .map(|d| models::TransferDatasourceStat {
                        datasource_id: d.datasource_id.clone(),
                        bytes: d.bytes as i64,
                    })
                    .collect(),
            ),
            files: None,
            info: None,
        };

        // Session-bound like every other transfer call: a `transfer_not_found`
        // must surface as the expired-session code (`is_session_expired`),
        // not disappear behind the fallback — the tickers and the keepalive
        // key on it.
        if is_download {
            let result = links_api::links_download_update(&self.api_config, url_token, request.clone()).await;
            self.log_api_call("POST", &format!("/links/{}/download/update", url_token), &result);
            result.map_err(|e| transfer_api_error(&e, "transfer_update_error"))?;
        } else {
            let result = links_api::links_upload_update(&self.api_config, url_token, request.clone()).await;
            self.log_api_call("POST", &format!("/links/{}/upload/update", url_token), &result);
            result.map_err(|e| transfer_api_error(&e, "transfer_update_error"))?;
        }

        Ok(())
    }

    pub async fn transfer_end(
        &self,
        is_download: bool,
        url_token: &str,
        transfer_id: &str,
        stats: &TransferStats,
        ds_stats: &[TransferDSStat],
        error: Option<&str>,
    ) -> Result<(), BackError> {
        self.check_online()?;
        tracing::debug!("sending transferEnd for {}", transfer_id);

        let request = models::TransferEndRequest {
            transfer_id: transfer_id.to_string(),
            success: error.is_none(),
            error: error.map(|e| e.to_string()),
            stats: Some(Box::new(models::TransferStats {
                bytes_transferred: Some(stats.bytes as i64),
                files_completed: Some(stats.files as i32),
                files_total: Some((stats.files + stats.dirs) as i32),
                dirs: Some(stats.dirs as i32),
                symlinks: Some(stats.symlinks as i32),
                duration_ms: Some(stats.duration as i32),
            })),
            datasources: Some(
                ds_stats
                    .iter()
                    .map(|d| models::TransferDatasourceStat {
                        datasource_id: d.datasource_id.clone(),
                        bytes: d.bytes as i64,
                    })
                    .collect(),
            ),
            info: None,
        };

        if is_download {
            let result = links_api::links_download_end(&self.api_config, url_token, request.clone()).await;
            self.log_api_call("POST", &format!("/links/{}/download/end", url_token), &result);
            result.map_err(|e| BackError::Api {
                code: "transfer_end_error".to_string(),
                message: fmt_api_error(&e),
            })?;
        } else {
            let result = links_api::links_upload_end(&self.api_config, url_token, request.clone()).await;
            self.log_api_call("POST", &format!("/links/{}/upload/end", url_token), &result);
            result.map_err(|e| BackError::Api {
                code: "transfer_end_error".to_string(),
                message: fmt_api_error(&e),
            })?;
        }

        tracing::debug!("transferEnd finish");
        Ok(())
    }

    /// Open a per-file transfer within an active session (TS
    /// `downloadFileStart`/`uploadFileStart`). Returns the `file_transfer_id` to
    /// pass to [`file_end`](Self::file_end). `encrypted_metadata` is an opaque
    /// blob the backend never inspects.
    pub async fn file_start(
        &self,
        is_download: bool,
        url_token: &str,
        transfer_id: &str,
        encrypted_metadata: Vec<u8>,
        estimated_bytes: Option<i64>,
        idempotency_key: Option<&str>,
    ) -> Result<String, BackError> {
        self.check_online()?;
        tracing::debug!("fileStart for {}", transfer_id);

        let request = models::FileTransferStartRequest {
            transfer_id: transfer_id.to_string(),
            encrypted_metadata,
            estimated_bytes,
        };

        let response = if is_download {
            let result =
                links_api::links_download_file_start(&self.api_config, url_token, request.clone(), idempotency_key)
                    .await;
            self.log_api_call("POST", &format!("/links/{}/download/files", url_token), &result);
            result.map_err(|e| transfer_api_error(&e, "file_start_error"))?
        } else {
            let result =
                links_api::links_upload_file_start(&self.api_config, url_token, request.clone(), idempotency_key).await;
            self.log_api_call("POST", &format!("/links/{}/upload/files", url_token), &result);
            result.map_err(|e| transfer_api_error(&e, "file_start_error"))?
        };

        tracing::debug!("fileStart finish {}", response.file_transfer_id);
        Ok(response.file_transfer_id)
    }

    pub async fn file_end(
        &self,
        is_download: bool,
        url_token: &str,
        file_transfer_id: &str,
        success: bool,
        stats: Option<&TransferStats>,
        error: Option<&str>,
    ) -> Result<(), BackError> {
        self.check_online()?;
        tracing::debug!("fileEnd for {}", file_transfer_id);

        let request = models::FileTransferEndRequest {
            success,
            error: error.map(|e| e.to_string()),
            stats: stats.map(|s| {
                Box::new(models::TransferStats {
                    bytes_transferred: Some(s.bytes as i64),
                    files_completed: Some(s.files as i32),
                    files_total: Some((s.files + s.dirs) as i32),
                    dirs: Some(s.dirs as i32),
                    symlinks: Some(s.symlinks as i32),
                    duration_ms: Some(s.duration as i32),
                })
            }),
        };

        if is_download {
            let result =
                links_api::links_download_file_end(&self.api_config, url_token, file_transfer_id, request.clone())
                    .await;
            self.log_api_call(
                "POST",
                &format!("/links/{}/download/files/{}/end", url_token, file_transfer_id),
                &result,
            );
            result.map_err(|e| BackError::Api {
                code: "file_end_error".to_string(),
                message: fmt_api_error(&e),
            })?;
        } else {
            let result =
                links_api::links_upload_file_end(&self.api_config, url_token, file_transfer_id, request.clone()).await;
            self.log_api_call(
                "POST",
                &format!("/links/{}/upload/files/{}/end", url_token, file_transfer_id),
                &result,
            );
            result.map_err(|e| BackError::Api {
                code: "file_end_error".to_string(),
                message: fmt_api_error(&e),
            })?;
        }

        tracing::debug!("fileEnd finish");
        Ok(())
    }

    pub async fn call(
        &self,
        target_agent_id: &str,
        transfer_id: &str,
    ) -> Result<(SharonResult, Option<CallOutInfo>), BackError> {
        self.check_online()?;

        let request = models::WebrtcCallCreateRequest {
            callee_id: target_agent_id.to_string(),
            transfer_id: transfer_id.to_string(),
        };

        let result = rtc_api::webrtc_call_create(&self.api_config, request.clone()).await;
        self.log_api_call("POST", "/rtc/calls", &result);

        match result {
            Ok(response) => {
                let ice_servers = response
                    .ice_servers
                    .unwrap_or_default()
                    .into_iter()
                    .map(|s| IceServer {
                        uri: s.uri,
                        username: s.username.unwrap_or_default(),
                        password: s.password.unwrap_or_default(),
                    })
                    .collect();

                let relay = self.relay_urls(
                    response.relays.as_ref().and_then(|r| r.first()).map(|s| s.as_str()),
                    response.jwt.as_deref(),
                );
                let call_info = CallOutInfo {
                    call_id: response.call_id.unwrap_or_default(),
                    ice_servers,
                    relay,
                };
                Ok((SharonResult::OK, Some(call_info)))
            }
            Err(e) => {
                if let share_ninja_api::apis::Error::ResponseError(ref resp) = e {
                    let status = match resp.entity {
                        Some(rtc_api::WebrtcCallCreateError::Status500(ref er))
                        | Some(rtc_api::WebrtcCallCreateError::Status415(ref er)) => Some(er.status.as_str()),
                        _ => None,
                    };
                    // Mapped statuses return a typed result the UI can translate,
                    // but they are still failures the user reports — log them at
                    // warn so they appear without `--debug-api`.
                    match status {
                        Some("party_offline") => {
                            tracing::warn!(target_agent = %target_agent_id, "call rejected: party_offline");
                            return Ok((SharonResult::PartyOffline, None));
                        }
                        Some("party_not_found") => {
                            tracing::warn!(target_agent = %target_agent_id, "call rejected: party_not_found");
                            return Ok((SharonResult::PartyNotFound, None));
                        }
                        // Anything else falls through to the generic log below, which
                        // records the status verbatim so an unmapped code is visible
                        // instead of silently collapsing into a bare `Fail`.
                        _ => {}
                    }
                }
                // A stale transfer session is recoverable via reconnect — return
                // the classified error instead of collapsing into `Fail` so
                // `P2PStorage::connect` can surface `SessionExpired`.
                if api_status(&e).as_deref() == Some("transfer_not_found") {
                    tracing::warn!(target_agent = %target_agent_id, "call rejected: transfer_not_found");
                    return Err(transfer_api_error(&e, "call_error"));
                }
                tracing::warn!(target_agent = %target_agent_id, "call failed: {}", fmt_api_error(&e));
                tracing::debug!("Call error detail: {:?}", e);
                Ok((SharonResult::Fail, None))
            }
        }
    }

    pub async fn run_register(back: Arc<RwLock<Back>>) -> Result<(), BackError> {
        tracing::debug!("run_register");
        // The handshake runs as its own task so an authoritative credential
        // change can terminate it (`abort_register`) — the caller then sees
        // `Superseded` instead of a result computed under stale credentials.
        let tasks = back.read().await.register_tasks.clone();
        let handle = tokio::spawn(Self::run_register_task(back));
        {
            let mut t = tasks.lock().unwrap_or_else(|e| e.into_inner());
            t.handshakes.retain(|h| !h.is_finished());
            t.handshakes.push(handle.abort_handle());
        }
        match handle.await {
            Ok(result) => result,
            Err(e) if e.is_cancelled() => Err(BackError::Superseded),
            Err(e) => Err(BackError::Other(format!("register task: {e}"))),
        }
    }

    async fn run_register_task(back: Arc<RwLock<Back>>) -> Result<(), BackError> {
        back.read().await.set_state(BackState::Connecting);
        let result = Self::register_handshake(&back).await;
        if let Err(ref e) = result {
            tracing::warn!(error = %e, "register handshake failed; dropping to NoConnection");
            back.read().await.set_state(BackState::NoConnection);
        }
        result
    }

    /// Terminate every register handshake in flight because the credentials
    /// are about to change and a handshake started under the old ones must
    /// not land afterwards (its `apply_register` would overwrite the new
    /// session). The reconnect loop is left alone: it is the one owner of
    /// "register again while offline", reads the credentials fresh on every
    /// attempt, and an aborted attempt of its own is just a failed one it
    /// retries after backoff. Killing it here left the app parked in
    /// `NoConnection` for good when the abort came while already offline —
    /// no transition remained to arm a new loop.
    pub fn abort_register(&self) {
        let mut t = self.register_tasks.lock().unwrap_or_else(|e| e.into_inner());
        let n = t.handshakes.len();
        for h in t.handshakes.drain(..) {
            h.abort();
        }
        tracing::debug!(handshakes = n, "register handshakes terminated");
    }

    async fn register_handshake(back: &Arc<RwLock<Back>>) -> Result<(), BackError> {
        // POST /auth/agents — network under a read lock.
        let response = back.read().await.register_call().await?;
        back.write().await.apply_register(response);

        // Open the WS, then commit the connection.
        let conn = back.read().await.connect_ws_call().await?;
        *back.read().await.ws_connection.lock().unwrap_or_else(|e| e.into_inner()) = Some(Arc::new(conn));

        // Who we are, from /me. A failure fails the handshake: `run_register_task`
        // drops to NoConnection and the reconnect loop retries — the session
        // is never reported Online with a `user` we did not get to read (a
        // transient error here used to demote a signed-in session to anonymous
        // until the next WS drop). A dead session is the auth middleware's
        // business: a 401 it cannot refresh tears the session down, the retried
        // handshake registers anonymously and /me answers 200 with no user.
        let me = back.read().await.me().await?;
        back.write().await.store_me(Some(me));

        let logged_in = back.read().await.user.is_some();
        let topics: Vec<String> = if logged_in {
            ["link:*", "user:*", "workspace:*"]
                .iter()
                .map(|s| s.to_string())
                .collect()
        } else {
            vec!["link:*".to_string()]
        };
        back.read().await.event_subscribe(topics).await?;

        if logged_in {
            back.read().await.set_state(BackState::OnlineAndLoggedIn);
        } else {
            back.read().await.set_state(BackState::Online);
        }
        Ok(())
    }

    /// Nearby visibility this device currently claims, as sent on register.
    pub fn nearby_visibility(&self) -> Option<models::NearbyVisibility> {
        *self.nearby_visibility.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Set this device's nearby visibility (`PUT /agents/me/visibility`) — who
    /// may see it in their nearby list. Discovery only: the device keeps
    /// receiving its own nearby list and keeps accepting offers either way.
    ///
    /// The value is stored locally *before* the call, because register is the
    /// authoritative channel: visibility is kept per device **and** network, so
    /// only the `nearby_visibility` on the next `POST /auth/agents` re-applies
    /// it after a network change. The PUT is the take-effect-now path; if it
    /// fails (offline included) the stored value still reaches the server on
    /// the next register.
    pub async fn set_nearby_visibility(&self, visibility: models::NearbyVisibility) -> Result<(), BackError> {
        *self.nearby_visibility.lock().unwrap_or_else(|e| e.into_inner()) = Some(visibility);
        self.check_online()?;
        tracing::debug!("setNearbyVisibility {}", visibility);
        let request = models::AgentVisibility::new(visibility);
        let result = agents_api::agent_visibility_set(&self.api_config, request.clone()).await;
        self.log_api_call("PUT", "/agents/me/visibility", &result);
        result.map(|_| ()).map_err(|e| BackError::Api {
            code: "agent_visibility_set_error".to_string(),
            message: fmt_api_error(&e),
        })
    }

    /// Build the `POST /auth/agents` request body from the static params.
    fn build_register_request(&self) -> models::AgentRegisterRequest {
        use models::agent_register_request_device_info::DeviceFormFactor;
        let device_form_factor = self.params.device_form_factor.as_deref().and_then(|s| match s {
            "desktop" => Some(DeviceFormFactor::Desktop),
            "laptop" => Some(DeviceFormFactor::Laptop),
            "tablet" => Some(DeviceFormFactor::Tablet),
            "phone" => Some(DeviceFormFactor::Phone),
            _ => None,
        });
        let device_info = models::AgentRegisterRequestDeviceInfo {
            device_locale: Some(self.params.lang.clone()),
            device_serial: self.params.device_serial.clone(),
            device_model: self.params.device_model.clone(),
            device_form_factor,
            device_name: self.params.device_name.clone(),
            release_channel: Some("production".to_string()),
            device_os: Some(self.params.os.clone()),
            device_os_version: Some(self.params.os_version.clone()),
            product_id: Some(self.params.product_id.clone()),
        };
        models::AgentRegisterRequest {
            name: None,
            client_build: Some(self.params.product_version.clone()),
            device_info: Some(device_info),
            device_public_key: self.params.device_public_key.clone(),
            nearby_visibility: self.nearby_visibility(),
            instance_id: self.params.instance_id.clone(),
        }
    }

    async fn register_call(&self) -> Result<models::AgentRegister200Response, BackError> {
        let request = self.build_register_request();
        // The auth middleware injects the current access token (and refreshes
        // it first if needed) on the register request, just like TS onRequest.
        let result = auth_api::agent_register(
            &self.api_config,
            request.clone(),
            Some(&self.params.response_mode),
        )
        .await;
        self.log_api_call("POST", "/auth/agents", &result);
        result.map_err(|e| {
            let error_msg = format!("{}", e);
            let (code, message) = if error_msg.contains("error sending request") {
                (
                    "network_error".to_string(),
                    format!("Failed to connect to API server: {}", self.params.base_url),
                )
            } else if error_msg.contains("timed out") {
                (
                    "timeout".to_string(),
                    format!("Connection to API server timed out: {}", self.params.base_url),
                )
            } else if error_msg.contains("dns") || error_msg.contains("resolve") {
                (
                    "dns_error".to_string(),
                    format!("Could not resolve API server hostname: {}", self.params.base_url),
                )
            } else {
                ("register_failed".to_string(), error_msg)
            };
            BackError::Api { code, message }
        })
    }

    fn apply_register(&mut self, response: models::AgentRegister200Response) {
        tracing::debug!("Register finish");
        self.agent_id = response.agent_id;
        self.agent_name = response.name;
        let expire_ts = response
            .expires_in
            .map(|e| e as i64 * 1000 + chrono::Utc::now().timestamp_millis());
        // A new session's register issues a refresh token — stored. A
        // re-register over a live session issues none ("keep the one you
        // have"): the store keeps what it holds, untouched.
        self.auth
            .apply_issued(response.access_token.clone(), expire_ts, response.refresh_token.clone());
        self.ws_ticket = response.ticket;

        self.vault_status = response.vault_status.map(|vs| match vs {
            models::agent_register_200_response::VaultStatus::NotInitialized => "not_initialized".to_string(),
            models::agent_register_200_response::VaultStatus::NotJoined => "not_joined".to_string(),
            models::agent_register_200_response::VaultStatus::PendingApproval => "pending_approval".to_string(),
            models::agent_register_200_response::VaultStatus::Active => "active".to_string(),
        });
        self.encrypted_identity_key = response.encrypted_identity_key;

        // Log JWT info at info level (visible with --verbose)
        if let Some(token) = self.auth.access_token() {
            log_jwt_info(&token);
        }
    }

    async fn connect_ws_call(&self) -> Result<WsConnection, BackError> {
        let ticket = self
            .ws_ticket
            .as_ref()
            .ok_or_else(|| BackError::WebSocket("No WS ticket available".to_string()))?;

        let state_callbacks = self.state_callbacks.clone();
        let shared_state = self.shared_state.clone();
        {
            let mut handlers = self.ws_handlers.write().await;
            handlers.state_change_handler = Some(Arc::new(move |state| {
                apply_state_change(&shared_state, &state_callbacks, state);
            }));
        }

        WsConnection::connect(
            &self.params.base_url,
            ticket,
            self.ws_handlers.clone(),
            self.params.connect_override.as_ref(),
        )
        .await
        .map_err(|e| {
            if let super::ws::WsError::RateLimited(retry_after) = &e {
                self.rate_limit.note(*retry_after);
            }
            BackError::WebSocket(e.to_string())
        })
    }

    /// Send a message over WebSocket
    pub async fn send_ws(&self, msg: serde_json::Value) -> Result<(), BackError> {
        // Cloned out so the send never runs under the slot's lock.
        let conn = self
            .ws_connection
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
            .ok_or_else(|| BackError::WebSocket("Not connected".to_string()))?;
        conn.send_json(msg)
            .await
            .map_err(|e| BackError::WebSocket(e.to_string()))
    }

    pub async fn set_raw_ws_message_handler<F>(&self, handler: F)
    where
        F: Fn(&str) + Send + Sync + 'static,
    {
        let mut handlers = self.ws_handlers.write().await;
        handlers.raw_message_handler = Some(Arc::new(handler));
    }

    /// Send a share offer to another agent. The offer carries no plaintext
    /// description — the recipient decrypts the link's `encrypted_metadata`
    /// with the offered share key instead.
    pub async fn send_share_offer(
        &self,
        target_agent_id: &str,
        url_token: &str,
        encrypted_share_key: Option<Vec<u8>>,
    ) -> Result<String, BackError> {
        self.check_online()?;
        tracing::debug!("sendShareOffer to {}", target_agent_id);

        let request = models::SharesOfferCreateRequest {
            target_agent_id: target_agent_id.to_string(),
            url_token: url_token.to_string(),
            encrypted_share_key,
        };

        let result = shares_api::shares_offer_create(&self.api_config, request.clone()).await;
        self.log_api_call("POST", "/agents/shares/offers", &result);

        match result {
            Ok(response) => {
                tracing::debug!("sendShareOffer success, offer_token: {:?}", response.offer_token);
                Ok(response.offer_token.unwrap_or_default())
            }
            Err(e) => {
                // The message is the only thing the UI gets (AppError::Backend has
                // no user code), so log it here too — otherwise the toast is the
                // sole record of the failure and nothing reaches the log file
                // unless the app was launched with `--debug-api`.
                let message = fmt_api_error(&e);
                tracing::warn!(target_agent = %target_agent_id, "sendShareOffer failed: {message}");
                tracing::debug!("sendShareOffer error detail: {:?}", e);
                // The target device not being online is the one refusal the
                // user can act on — it keeps the backend's own status as the
                // code so upper layers can classify it.
                let code = if api_status(&e).as_deref() == Some("party_offline") {
                    "party_offline".to_string()
                } else {
                    "share_offer_error".to_string()
                };
                Err(BackError::Api { code, message })
            }
        }
    }

    /// Send a share answer (accept or reject)
    pub async fn send_share_answer(&self, offer_token: &str, answer: &str) -> Result<(), BackError> {
        self.check_online()?;
        tracing::debug!("sendShareAnswer {} for {}", answer, offer_token);

        let answer_enum = match answer {
            "accept" => models::shares_offer_answer_request::Answer::Accept,
            "reject" => models::shares_offer_answer_request::Answer::Reject,
            _ => {
                return Err(BackError::Other(format!("Invalid answer: {}", answer)));
            }
        };

        let request = models::SharesOfferAnswerRequest {
            offer_token: offer_token.to_string(),
            answer: answer_enum,
        };

        let result = shares_api::shares_offer_answer(&self.api_config, request.clone()).await;
        self.log_api_call("POST", "/agents/shares/answers", &result);

        match result {
            Ok(_) => {
                tracing::debug!("sendShareAnswer success");
                Ok(())
            }
            Err(e) => Err(BackError::Api {
                code: "share_answer_error".to_string(),
                message: fmt_api_error(&e),
            }),
        }
    }

    pub async fn send_share_cancel(&self, offer_token: &str) -> Result<(), BackError> {
        self.check_online()?;
        tracing::debug!("sendShareCancel for {}", offer_token);

        let request = models::SharesOfferCancelRequest {
            offer_token: offer_token.to_string(),
        };

        let result = shares_api::shares_offer_cancel(&self.api_config, request.clone()).await;
        self.log_api_call("POST", "/agents/shares/cancellations", &result);

        match result {
            Ok(_) => {
                tracing::debug!("sendShareCancel success");
                Ok(())
            }
            Err(e) => Err(BackError::Api {
                code: "share_cancel_error".to_string(),
                message: fmt_api_error(&e),
            }),
        }
    }

    pub async fn refresh_access_token_if_needed(&mut self) -> Result<(), BackError> {
        self.auth.ensure_fresh().await
    }

    pub async fn refresh_access_token(&mut self) -> Result<(), BackError> {
        self.auth.refresh().await
    }

    /// Exchange an OAuth authorization code for tokens, then bring the
    /// connection up via the shared lock-light [`run_register`](Self::run_register)
    /// handshake — the same path startup and auto-reconnect use, so there is a
    /// single register implementation. Takes the `Arc` (not `&mut self`) so the
    /// handshake keeps `status()` responsive instead of holding the write lock
    /// across the whole multi-second round-trip.
    pub async fn exchange_oauth_code(
        back: Arc<RwLock<Back>>,
        code: &str,
        redirect_uri: &str,
        code_verifier: &str,
    ) -> Result<(), BackError> {
        tracing::debug!("exchange_oauth_code");
        // The callback is authoritative: a reconnect that woke on the app's
        // return from the browser (or any register still in flight) was
        // started with the pre-login credentials and would land after the
        // exchange, overwriting it. Terminate it first, then exchange, then
        // register properly with the new session.
        back.read().await.abort_register();
        // Token exchange mutates only the interior-mutable auth state, so a read
        // lock suffices; the register handshake then runs lock-light.
        let exchanged = back
            .read()
            .await
            .exchange_oauth_token(code, redirect_uri, code_verifier)
            .await;
        // Register on every exit, not only after a successful exchange. The
        // abort above may have killed a handshake at `Connecting` — an aborted
        // task never reaches its drop to `NoConnection`, and only that
        // transition arms the reconnect loop — so a failed exchange that
        // returned here would leave the app parked in `Connecting` for good.
        // The handshake runs with whatever session the exchange left: the new
        // one, or the previous one when the exchange failed.
        let registered = Back::run_register(back).await;
        exchanged?;
        registered
    }

    /// POST `/auth/token` (authorization_code grant) and stash the returned
    /// tokens in the shared [`AuthManager`](super::auth::AuthManager). `&self`:
    /// mutates only interior-mutable auth state, so it is safe under a read lock.
    async fn exchange_oauth_token(&self, code: &str, redirect_uri: &str, code_verifier: &str) -> Result<(), BackError> {
        let grant = models::AuthorizationCodeGrant {
            grant_type: models::authorization_code_grant::GrantType::AuthorizationCode,
            code: code.to_string(),
            redirect_uri: redirect_uri.to_string(),
            code_verifier: code_verifier.to_string(),
        };
        let request = models::TokenRequest::AuthorizationCode(Box::new(grant));

        let result = auth_api::auth_token(&self.api_config, request.clone(), Some("token")).await;
        self.log_api_call("POST", "/auth/token", &result);

        match result {
            Ok(response) => {
                tracing::debug!("OAuth token exchange successful");
                self.auth.apply_issued(
                    Some(response.access_token.clone()),
                    Some(response.expires_in as i64 * 1000 + chrono::Utc::now().timestamp_millis()),
                    response.refresh_token,
                );
                Ok(())
            }
            Err(e) => {
                tracing::debug!("OAuth token exchange failed: {}", e);
                Err(BackError::Api {
                    code: "oauth_token_exchange_failed".to_string(),
                    message: fmt_api_error(&e),
                })
            }
        }
    }

    /// `GET /me` now, adopted on success (`store_me`); a failure leaves the
    /// cached `me` / `user` as they were. Lock-light like the register
    /// handshake: the fetch runs under a read guard and only the adoption
    /// takes the write lock — a fetch under `back.write()` would stall every
    /// reader for the call's duration, up to its deadline on a dead wire.
    pub async fn refresh_me(back: &Arc<RwLock<Back>>) -> Result<models::Me, BackError> {
        let me = back.read().await.me().await?;
        back.write().await.store_me(Some(me.clone()));
        Ok(me)
    }

    /// `GET /me/usage/history`: the `/me` counters by day over `[from, to]`
    /// (inclusive, at most a year), one entry per day and key. The owner
    /// resolves as for `/me`, so this works for anonymous agents too.
    pub async fn usage_history(
        &self,
        from: chrono::NaiveDate,
        to: chrono::NaiveDate,
    ) -> Result<Vec<models::UsageHistory>, BackError> {
        let result = usage_api::me_get_usage_history(&self.api_config, Some(from), Some(to)).await;
        self.log_api_call("GET", "/me/usage/history", &result);
        result.map(|list| list.items).map_err(|e| BackError::Api {
            code: api_status(&e).unwrap_or_else(|| "usage_history_error".to_string()),
            message: fmt_api_error(&e),
        })
    }

    /// Adopt a `GET /me` answer: `me` whole, `user` from it (absent user =
    /// not signed in; `None` = the fetch failed, login is best-effort).
    fn store_me(&mut self, me: Option<models::Me>) {
        self.user = me.as_ref().and_then(|m| m.user.as_deref().cloned());
        self.me = me;
    }

    /// A `usage.updated` frame: the counters `GET /me` would report now,
    /// applied to the cached `me` without a fetch. Nothing cached = nothing
    /// to update (the next `refresh_me` brings them anyway).
    pub fn apply_usage(&mut self, usage: models::MeUsage) {
        if let Some(me) = self.me.as_mut() {
            *me.usage = usage;
        }
    }

    pub async fn logout(&mut self) -> Result<(), BackError> {
        // No user session, nothing to log out: a second sign-out queued on
        // the write lock behind the first finds it already cleared and must
        // not post again (each of those posts counted against the rate
        // limit and the burst then throttled the anonymous re-register).
        if self.user.is_none() {
            tracing::debug!("logout: no user session, nothing to do");
            return Ok(());
        }
        // Same rule as OAuth completion, mirrored: a register in flight under
        // the user session must not land after the anonymous re-register.
        self.abort_register();
        let request = models::AuthLogoutRequest {
            refresh_token: self.auth.refresh_token(),
        };

        let result = auth_api::auth_logout(&self.api_config, Some(request.clone())).await;
        self.log_api_call("POST", "/auth/logout", &result);

        // The server-side revoke is best-effort: clear the local session
        // regardless of whether it succeeded, so logout never gets stuck on a
        // transport error (the session expires server-side anyway).
        match &result {
            Ok(_) => tracing::debug!("auth/logout finish"),
            Err(e) => tracing::warn!(error = %fmt_api_error(e), "auth/logout failed; clearing local session anyway"),
        }

        self.clear_session();
        Ok(())
    }

    /// `DELETE /users/me`: the account, its workspaces and every session on
    /// every device are gone server-side (irreversible). Only a 204 clears
    /// the local session — a refusal (409 `user_owns_workspaces` /
    /// `billing_cancellation_required`) leaves the user signed in with the
    /// backend's code to act on.
    pub async fn delete_account(&mut self) -> Result<(), BackError> {
        let result = users_api::me_delete(&self.api_config, None).await;
        self.log_api_call("DELETE", "/users/me", &result);
        match result {
            Ok(()) => {
                self.abort_register();
                self.clear_session();
                Ok(())
            }
            Err(e) => Err(BackError::Api {
                code: api_status(&e).unwrap_or_else(|| "account_delete_error".to_string()),
                message: fmt_api_error(&e),
            }),
        }
    }

    /// Forget the signed-in session locally: tokens, `me`, `user`. The
    /// anonymous re-register is driven by the caller via `run_register`
    /// (lock-light), not here — so neither logout nor account deletion holds
    /// the lock across the re-register handshake.
    fn clear_session(&mut self) {
        self.auth.clear();
        self.me = None;
        self.user = None;
    }

    /// Login or register with email - returns a challenge that must be verified
    pub async fn login_register(
        &self,
        email: &str,
        display_name: Option<&str>,
        accept_terms: Option<bool>,
        remember_me: Option<bool>,
    ) -> Result<Challenge, BackError> {
        tracing::debug!("login_register for {}", email);

        let request = models::SessionLoginRequest {
            email: email.to_string(),
            display_name: display_name.map(|s| Some(s.to_string())),
            accept_terms,
            remember_me,
            invite_token: None,
            time_zone: None,
        };

        let result = auth_api::users_sessions_login(&self.api_config, request.clone()).await;
        self.log_api_call("POST", "/auth/sessions", &result);

        match result {
            Ok(response) => {
                tracing::debug!("auth/sessions finish");
                let challenge = *response
                    .challenge
                    .flatten()
                    .ok_or_else(|| BackError::Other("No challenge in response".to_string()))?;
                Ok(Challenge {
                    token: challenge.token,
                    expires_at: challenge.expires_at.to_rfc3339(),
                    sign_in_methods: challenge
                        .sign_in_methods
                        .into_iter()
                        .map(|f| ChallengeFactor {
                            id: f.id,
                            factor_type: f.r#type,
                        })
                        .collect(),
                })
            }
            Err(e) => Err(BackError::Api {
                code: "login_register_error".to_string(),
                message: fmt_api_error(&e),
            }),
        }
    }

    /// Verify an email challenge with a code
    pub async fn check_email_challenge(&mut self, token: &str, code: &str) -> Result<bool, BackError> {
        tracing::debug!("check_email_challenge for token {}", token);

        let request = models::AuthChallengeVerifyRequest {
            factor_id: "email_code".to_string(),
            credentials: Box::new(models::AuthChallengeVerifyRequestCredentials::CodeCredentials(
                Box::new(models::CodeCredentials::new(code.to_string())),
            )),
        };

        let result = auth_api::auth_challenges_verify(&self.api_config, token, request.clone(), None).await;
        self.log_api_call("POST", &format!("/auth/challenges/{}", token), &result);

        match result {
            Ok(response) => {
                tracing::debug!("auth/challenges succeeded");
                if response.complete {
                    if let Some(access_token) = response.access_token {
                        self.auth.set_access_token(Some(access_token));
                    }
                    if let Some(user) = response.user.flatten().map(|u| *u) {
                        self.user = Some(user);
                    }
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            Err(e) => {
                tracing::debug!("auth/challenges failed: {}", e);
                Err(BackError::Api {
                    code: "challenge_verify_error".to_string(),
                    message: fmt_api_error(&e),
                })
            }
        }
    }

    /// Start SSO login flow (Google or Apple)
    pub async fn login_register_sso(&self, provider: &str, redirect_uri: &str) -> Result<String, BackError> {
        tracing::debug!("login_register_sso for provider {}", provider);

        let result = auth_api::sso_start(&self.api_config, provider, Some(redirect_uri)).await;
        self.log_api_call("GET", &format!("/auth/sso/{}", provider), &result);

        match result {
            Ok(response) => {
                tracing::debug!("auth/sso finish");
                Ok(response.auth_url)
            }
            Err(e) => Err(BackError::Api {
                code: "sso_start_error".to_string(),
                message: fmt_api_error(&e),
            }),
        }
    }

    /// Generate OAuth authorization URL (browser-facing consent page).
    /// `agent_id` identifies this (requesting) device: the consent page passes
    /// it through GET/POST /auth/authorize so the minted code — and the
    /// session its exchange creates — belongs to this device, not to the
    /// browser that approves the consent.
    pub fn gen_request_oauth(&self, redirect_uri: &str, code_challenge: &str) -> String {
        let base = self.params.base_url.trim_end_matches("/api");
        let mut url = format!(
            "{}/oauth/consent?callback={}&challenge={}",
            base, redirect_uri, code_challenge
        );
        if !self.agent_id.is_empty() {
            url.push_str(&format!("&agent_id={}", self.agent_id));
        }
        url
    }

    /// Request OAuth authorization
    pub async fn login_request_oauth(&self, redirect_uri: &str, code_challenge: &str) -> Result<String, BackError> {
        tracing::debug!("login_request_oauth");

        let result = auth_api::oauth_authorize_get(
            &self.api_config,
            redirect_uri,
            code_challenge,
            None,
            None,
            None,
            // Requesting from this device for this device — bind the code here.
            (!self.agent_id.is_empty()).then_some(self.agent_id.as_str()),
        )
        .await;
        self.log_api_call("GET", "/auth/authorize", &result);

        match result {
            Ok(response) => {
                tracing::debug!("auth/authorize get finish");
                Ok(response.redirect_uri.unwrap_or_default())
            }
            Err(e) => Err(BackError::Api {
                code: "oauth_request_error".to_string(),
                message: fmt_api_error(&e),
            }),
        }
    }

    /// List all links, following the cursor to completion. The endpoint is
    /// paginated (default page size), so a single page would cap the list — the
    /// renderer's old ~100-item wall. The page-count guard stops a misbehaving
    /// server (`has_more` with a stale cursor) from looping forever. Mirrors the
    /// TS `Back.links()`.
    pub async fn links(&self) -> Result<Vec<models::Link>, BackError> {
        self.check_online()?;
        tracing::debug!("fetching links list");
        let mut all: Vec<models::Link> = Vec::new();
        let mut cursor: Option<String> = None;
        for _ in 0..1000 {
            let result = links_api::links_list(&self.api_config, None, cursor.as_deref(), None, Some(100)).await;
            self.log_api_call("GET", "/links", &result);
            let response = result.map_err(|e| BackError::Api {
                code: "links_list_error".to_string(),
                message: fmt_api_error(&e),
            })?;
            all.extend(response.links);
            match response.next_cursor {
                Some(next) if response.has_more && Some(next.as_str()) != cursor.as_deref() => {
                    cursor = Some(next);
                }
                _ => break,
            }
        }
        tracing::debug!("links list: {} items", all.len());
        Ok(all)
    }

    /// The signed-in user's live API keys (`GET /users/api-keys`, all pages,
    /// oldest first). The secret is never listed — `prefix` tells keys apart.
    /// A user session is required: an API key cannot manage keys.
    pub async fn api_keys(&self) -> Result<Vec<models::UserApiKey>, BackError> {
        self.check_online()?;
        let mut all: Vec<models::UserApiKey> = Vec::new();
        let mut cursor: Option<String> = None;
        for _ in 0..1000 {
            let result = api_keys_api::api_keys_list(&self.api_config, cursor.as_deref(), None, Some(100)).await;
            self.log_api_call("GET", "/users/api-keys", &result);
            let response = result.map_err(|e| BackError::Api {
                code: api_status(&e).unwrap_or_else(|| "api_keys_list_error".to_string()),
                message: fmt_api_error(&e),
            })?;
            all.extend(response.items);
            let page = *response.page_info;
            match page.next_cursor.flatten() {
                Some(next) if page.has_more && Some(next.as_str()) != cursor.as_deref() => {
                    cursor = Some(next);
                }
                _ => break,
            }
        }
        Ok(all)
    }

    /// Mint an API key (`POST /users/api-keys`): the returned `token` is shown
    /// this once and never again. Empty `scopes` = the user's full rights;
    /// `expires_at` `None` = never. Refusals carry the backend's status
    /// (`name_required`, `invalid_scopes`, `invalid_expires_at`).
    pub async fn api_key_create(
        &self,
        name: &str,
        scopes: Vec<String>,
        expires_at: Option<chrono::DateTime<chrono::FixedOffset>>,
    ) -> Result<models::UserApiKeyWithSecret, BackError> {
        self.check_online()?;
        let request = models::UserApiKeyCreateRequest {
            name: name.to_string(),
            scopes: Some(scopes),
            expires_at: Some(expires_at),
        };
        let result = api_keys_api::api_key_create(&self.api_config, request.clone(), None).await;
        self.log_api_call("POST", "/users/api-keys", &result);
        result.map_err(|e| BackError::Api {
            code: api_status(&e).unwrap_or_else(|| "api_key_create_error".to_string()),
            message: fmt_api_error(&e),
        })
    }

    /// Revoke one of the user's API keys (`DELETE /users/api-keys/{key_id}`):
    /// it stops authenticating at once. `api_key_not_found` for a key that is
    /// not the caller's or does not exist.
    pub async fn api_key_revoke(&self, key_id: &str) -> Result<(), BackError> {
        self.check_online()?;
        let result = api_keys_api::api_key_delete(&self.api_config, key_id, None).await;
        self.log_api_call("DELETE", &format!("/users/api-keys/{key_id}"), &result);
        result.map_err(|e| BackError::Api {
            code: api_status(&e).unwrap_or_else(|| "api_key_revoke_error".to_string()),
            message: fmt_api_error(&e),
        })
    }

    /// File an abuse report on a link (`POST /links/{url_token}/report`).
    /// Public — works signed-in or anonymous. The share key travels only in
    /// `link_key`, attached by the reporter's explicit choice ("open my
    /// content for this report"). Mirrors the TS `Back.linkReport()`.
    pub async fn link_report(&self, url_token: &str, req: models::LinksReportRequest) -> Result<(), BackError> {
        self.check_online()?;
        tracing::debug!("linkReport for {}", url_token);
        let result = links_api::links_report(&self.api_config, url_token, req).await;
        self.log_api_call("POST", &format!("/links/{}/report", url_token), &result);
        result.map(|_| ()).map_err(|e| BackError::Api {
            code: "link_report_error".to_string(),
            message: fmt_api_error(&e),
        })
    }

    /// Delete a link
    /// Idempotent: a link the server no longer has (`link_not_found`) is
    /// already deleted — the caller drops its local rows the same way.
    pub async fn link_delete(&self, url_token: &str) -> Result<(), BackError> {
        self.check_online()?;
        let result = links_api::links_delete(&self.api_config, url_token).await;
        self.log_api_call("DELETE", &format!("/links/{}", url_token), &result);
        match result {
            Ok(()) => Ok(()),
            Err(e) if api_status(&e).as_deref() == Some("link_not_found") => {
                tracing::info!(token = %url_token, "link_delete: already gone server-side");
                Ok(())
            }
            Err(e) => Err(BackError::Api {
                code: "link_delete_error".to_string(),
                message: fmt_api_error(&e),
            }),
        }
    }

    /// List ACL entries for a link
    pub async fn link_acl_list(&self, url_token: &str) -> Result<Vec<models::LinkAclEntry>, BackError> {
        self.check_online()?;
        tracing::debug!("linkAclList for {}", url_token);
        let result = links_api::links_acl_list(&self.api_config, url_token).await;
        self.log_api_call("GET", &format!("/links/{}/acl", url_token), &result);
        let response = result.map_err(|e| BackError::Api {
            code: "link_acl_list_error".to_string(),
            message: fmt_api_error(&e),
        })?;
        Ok(response.items)
    }

    /// Grant an ACL entry on a link
    pub async fn link_acl_grant(&self, url_token: &str, grant: models::LinkAclGrantRequest) -> Result<(), BackError> {
        self.check_online()?;
        tracing::debug!("linkAclGrant for {} subject={:?}", url_token, grant.subject);
        // No If-Match: the mutation applies unconditionally, as before the ACL
        // ETag existed. 409 already_granted_different remains the second net.
        let result = links_api::links_acl_grant(&self.api_config, url_token, grant.clone(), None).await;
        self.log_api_call("POST", &format!("/links/{}/acl/grant", url_token), &result);
        result.map_err(|e| BackError::Api {
            code: "link_acl_grant_error".to_string(),
            message: fmt_api_error(&e),
        })?;
        Ok(())
    }

    /// Revoke an ACL entry on a link
    pub async fn link_acl_revoke(
        &self,
        url_token: &str,
        revoke: models::LinkAclRevokeRequest,
    ) -> Result<(), BackError> {
        self.check_online()?;
        tracing::debug!("linkAclRevoke for {} subject={:?}", url_token, revoke.subject);
        let result = links_api::links_acl_revoke(&self.api_config, url_token, revoke.clone(), None).await;
        self.log_api_call("POST", &format!("/links/{}/acl/revoke", url_token), &result);
        result.map_err(|e| BackError::Api {
            code: "link_acl_revoke_error".to_string(),
            message: fmt_api_error(&e),
        })?;
        Ok(())
    }

    /// Fetch the signed-in user's avatar bytes: `(bytes, mime)`, `None` when
    /// no avatar is set (or nobody is signed in). Uploaded avatars go through
    /// the authenticated client (the backend 302s to a signed URL; reqwest
    /// strips `Authorization` on the cross-host redirect). SSO avatars live on
    /// the provider's host and are fetched with a bare client so no Bearer
    /// header reaches a third party.
    pub async fn fetch_avatar(&self) -> Result<Option<(Vec<u8>, String)>, BackError> {
        let Some(user) = self.user.as_ref() else {
            return Ok(None);
        };
        let Some(Some(url)) = user.avatar_url.clone() else {
            return Ok(None);
        };
        let sso = matches!(user.avatar_source, Some(models::user::AvatarSource::Sso));
        let api_err = |message: String| BackError::Api {
            code: "avatar_fetch_error".to_string(),
            message,
        };
        let response = if sso {
            reqwest::Client::new()
                .get(&url)
                .send()
                .await
                .map_err(|e| api_err(e.to_string()))?
        } else {
            self.api_config
                .client
                .get(&url)
                .send()
                .await
                .map_err(|e| api_err(e.to_string()))?
        };
        if !response.status().is_success() {
            return Err(api_err(format!("avatar fetch failed: {}", response.status())));
        }
        let mime = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("image/png")
            .to_string();
        let bytes = response.bytes().await.map_err(|e| api_err(e.to_string()))?;
        Ok(Some((bytes.to_vec(), mime)))
    }

    /// Another user's uploaded avatar by id (`GET /users/{user_id}/avatar`) —
    /// any authenticated caller, an anonymous device included, may fetch it.
    /// `Ok(None)` when that user has no uploaded avatar (`avatar_not_found`).
    pub async fn fetch_user_avatar(&self, user_id: &str) -> Result<Option<(Vec<u8>, String)>, BackError> {
        let api_err = |message: String| BackError::Api {
            code: "avatar_fetch_error".to_string(),
            message,
        };
        let url = format!(
            "{}/users/{}/avatar",
            self.api_config.base_path,
            share_ninja_api::apis::urlencode(user_id)
        );
        let response = self
            .api_config
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| api_err(e.to_string()))?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err(api_err(format!("avatar fetch failed: {}", response.status())));
        }
        let mime = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("image/png")
            .to_string();
        let bytes = response.bytes().await.map_err(|e| api_err(e.to_string()))?;
        Ok(Some((bytes.to_vec(), mime)))
    }

    /// Update user profile
    /// `PATCH /users/me`, the answered user adopted as `user`. Lock-light
    /// like `refresh_me`: the call under a read guard, the adoption under a
    /// short write.
    pub async fn update_profile(
        back: &Arc<RwLock<Back>>,
        name: &str,
        avatar_data_uri: Option<&str>,
    ) -> Result<(), BackError> {
        let user = back.read().await.profile_update_call(name, avatar_data_uri).await?;
        back.write().await.user = Some(user);
        Ok(())
    }

    /// Network-only `PATCH /users/me`; `update_profile` adopts the answer.
    async fn profile_update_call(&self, name: &str, avatar_data_uri: Option<&str>) -> Result<models::User, BackError> {
        let request = models::UserUpdateRequest {
            display_name: Some(name.to_string()),
            avatar: avatar_data_uri.map(|s| Some(s.to_string())),
            language: None,
            time_zone: None,
            download_digest_period: None,
        };
        let result = users_api::me_update_profile(&self.api_config, request.clone()).await;
        self.log_api_call("PATCH", "/users/me", &result);
        result.map_err(|e| BackError::Api {
            code: "profile_update_error".to_string(),
            message: fmt_api_error(&e),
        })
    }

    /// Network-only `GET /me`: device, signed-in user, and the tier / limits /
    /// usage of whichever owns the session's links (a limit key left out = no
    /// limit of that kind). Safe to run under a read lock — the lock-light
    /// register handshake fetches here and adopts with `store_me` under a
    /// write lock, and `refresh_me` does the same for everyone else.
    async fn me(&self) -> Result<models::Me, BackError> {
        let result = usage_api::me_get(&self.api_config).await;
        self.log_api_call("GET", "/me", &result);
        result.map_err(|e| BackError::Api {
            code: "me_error".to_string(),
            message: fmt_api_error(&e),
        })
    }

    /// Unified device overview: one card per device with the caller's live
    /// sessions and vault trust standing (supersedes the three read lists).
    pub async fn devices_overview(&self) -> Result<models::DevicesOverviewResponse, BackError> {
        let result = users_api::users_devices_overview(&self.api_config).await;
        self.log_api_call("GET", "/users/me/devices", &result);
        result.map_err(|e| BackError::Api {
            code: "devices_overview_error".to_string(),
            message: fmt_api_error(&e),
        })
    }

    // --- Events ---

    /// List active event subscriptions
    pub async fn event_subscriptions(&self) -> Result<Vec<models::EventSubscriptionInfo>, BackError> {
        self.check_online()?;
        let result = events_api::list_event_subscriptions(&self.api_config).await;
        self.log_api_call("GET", "/events/subscriptions", &result);
        let response = result.map_err(|e| BackError::Api {
            code: "event_subscriptions_error".to_string(),
            message: fmt_api_error(&e),
        })?;
        Ok(response.subscriptions)
    }

    /// Subscribe to event topics
    pub async fn event_subscribe(&self, topics: Vec<String>) -> Result<Vec<models::EventSubscribeResult>, BackError> {
        let request = models::EventSubscribeRequest { topics };
        let result = events_api::subscribe_events(&self.api_config, request.clone()).await;
        self.log_api_call("POST", "/events/subscriptions", &result);
        let response = result.map_err(|e| BackError::Api {
            code: "event_subscribe_error".to_string(),
            message: fmt_api_error(&e),
        })?;
        Ok(response.results)
    }

    /// Unsubscribe from event topics
    pub async fn event_unsubscribe(&self, topics: Vec<String>) -> Result<(), BackError> {
        let request = models::EventUnsubscribeRequest { topics };
        let result = events_api::unsubscribe_events(&self.api_config, request.clone()).await;
        self.log_api_call("DELETE", "/events/subscriptions", &result);
        result.map_err(|e| BackError::Api {
            code: "event_unsubscribe_error".to_string(),
            message: fmt_api_error(&e),
        })?;
        Ok(())
    }

    // --- Vault ---

    /// Get vault cipher suite configuration
    pub async fn vault_get_config(&self) -> Result<models::VaultConfig, BackError> {
        let result = vault_api::vault_get_config(&self.api_config).await;
        self.log_api_call("GET", "/vault/config", &result);
        result.map_err(|e| BackError::Api {
            code: "vault_config_error".to_string(),
            message: fmt_api_error(&e),
        })
    }

    /// Register the first device in the vault trust circle
    pub async fn vault_device_register(
        &self,
        request: models::VaultDeviceRegisterRequest,
    ) -> Result<models::VaultDevice, BackError> {
        let result = vault_api::vault_device_register(&self.api_config, request.clone()).await;
        self.log_api_call("POST", "/vault/devices/register", &result);
        result.map_err(|e| BackError::Api {
            code: "vault_device_register_error".to_string(),
            message: fmt_api_error(&e),
        })
    }

    /// Submit a join request from a new device
    pub async fn vault_device_join(&self) -> Result<models::VaultDeviceJoinResponse, BackError> {
        let result = vault_api::vault_device_join(&self.api_config).await;
        self.log_api_call("POST", "/vault/devices/join", &result);
        result.map_err(|e| {
            if let share_ninja_api::apis::Error::ResponseError(ref resp) = e
                && resp.status.as_u16() == 409
            {
                return BackError::Api {
                    code: "vault_device_join_conflict".to_string(),
                    message: "vault_device_join_conflict".to_string(),
                };
            }
            BackError::Api {
                code: "vault_device_join_error".to_string(),
                message: fmt_api_error(&e),
            }
        })
    }

    /// Approve a pending vault device join request
    pub async fn vault_device_approve(
        &self,
        agent_id: &str,
        request: models::VaultDeviceApproveRequest,
    ) -> Result<models::VaultDevice, BackError> {
        let result = vault_api::vault_device_approve(&self.api_config, agent_id, request.clone()).await;
        self.log_api_call("POST", &format!("/vault/devices/{}/approve", agent_id), &result);
        result.map_err(|e| BackError::Api {
            code: join_decision_code(&e, "vault_device_approve_error"),
            message: fmt_api_error(&e),
        })
    }

    /// Reject a pending vault device join request
    pub async fn vault_device_reject(&self, agent_id: &str) -> Result<(), BackError> {
        let result = vault_api::vault_device_reject(&self.api_config, agent_id, None).await;
        self.log_api_call("POST", &format!("/vault/devices/{}/reject", agent_id), &result);
        result.map_err(|e| BackError::Api {
            code: join_decision_code(&e, "vault_device_reject_error"),
            message: fmt_api_error(&e),
        })
    }

    /// Revoke a device from the vault trust circle
    pub async fn vault_device_revoke(&self, agent_id: &str) -> Result<(), BackError> {
        let result = vault_api::vault_device_revoke(&self.api_config, agent_id).await;
        self.log_api_call("DELETE", &format!("/vault/devices/{}", agent_id), &result);
        result.map_err(|e| BackError::Api {
            code: "vault_device_revoke_error".to_string(),
            message: fmt_api_error(&e),
        })
    }

    /// Upsert vault recovery with encrypted identity key.
    /// Server picks setup-vs-regenerate based on whether a row already exists.
    pub async fn vault_recovery_upsert(&self, request: models::VaultRecoveryRequest) -> Result<(), BackError> {
        let result = vault_api::vault_recovery_upsert(&self.api_config, request.clone()).await;
        self.log_api_call("PUT", "/vault/recovery", &result);
        result.map_err(|e| BackError::Api {
            code: "vault_recovery_upsert_error".to_string(),
            message: fmt_api_error(&e),
        })
    }

    /// Whether a recovery-encrypted identity key exists for the user
    /// (`GET /vault/recovery`) — any authenticated session may ask.
    pub async fn vault_recovery_status(&self) -> Result<models::VaultRecoveryStatus, BackError> {
        let result = vault_api::vault_recovery_status(&self.api_config).await;
        self.log_api_call("GET", "/vault/recovery", &result);
        result.map_err(|e| BackError::Api {
            code: "vault_recovery_status_error".to_string(),
            message: fmt_api_error(&e),
        })
    }

    /// Recover vault identity key using recovery key
    pub async fn vault_recovery_recover(
        &self,
        request: models::VaultRecoveryRecoverRequest,
    ) -> Result<models::VaultRecoveryRecoverResponse, BackError> {
        let result = vault_api::vault_recovery_recover(&self.api_config, request.clone()).await;
        self.log_api_call("POST", "/vault/recovery/recover", &result);
        result.map_err(|e| BackError::Api {
            code: "vault_recovery_recover_error".to_string(),
            message: fmt_api_error(&e),
        })
    }

    /// Complete recovery: atomically revoke prior devices and register this one
    /// with the recovered identity key. Step-up gated (`x-confirm: self-factor`):
    /// without `challenge` the backend answers `ChallengeRequired`; retry with
    /// the confirmed token and the byte-identical request.
    pub async fn vault_recovery_complete(
        &self,
        request: models::VaultRecoveryCompleteRequest,
        challenge: Option<&str>,
    ) -> Result<models::VaultDevice, BackError> {
        let result = vault_api::vault_recovery_complete(&self.api_config, request.clone(), challenge).await;
        self.log_api_call("POST", "/vault/recovery/complete", &result);
        result.map_err(|e| step_up_error("vault_recovery_complete_error", &e))
    }

    /// Full vault reset (trust circle AND recovery phrase lost): swap in a fresh
    /// identity + recovery wrap. Step-up gated like `vault_recovery_complete`.
    /// The backend refuses it from a device still enrolled in the circle.
    pub async fn vault_reset(
        &self,
        request: models::VaultResetRequest,
        challenge: Option<&str>,
    ) -> Result<models::VaultDevice, BackError> {
        let result = vault_api::vault_reset(&self.api_config, request.clone(), challenge).await;
        self.log_api_call("POST", "/vault/reset", &result);
        result.map_err(|e| step_up_error("vault_reset_error", &e))
    }

    /// Ask the backend to email the `email_code` for a `ChallengeRequired`
    /// token (`POST /auth/challenges/{token}/code`). Needed when the refusal
    /// listed several factors: the code goes out on its own only for a sole
    /// email factor. Also the way to re-send.
    pub async fn send_step_up_code(&self, token: &str) -> Result<(), BackError> {
        let request = models::ChallengeCodeRequest::new("email_code".to_string());
        let result = auth_api::auth_challenges_send_code(&self.api_config, token, request.clone()).await;
        self.log_api_call("POST", &format!("/auth/challenges/{}/code", token), &result);
        result.map_err(|e| BackError::Api {
            code: "auth_challenges_send_code_error".to_string(),
            message: fmt_api_error(&e),
        })
    }

    /// Verify the emailed step-up code against a `ChallengeRequired` token;
    /// returns the confirmed token to send in `X-Auth-Challenge` on the retry.
    /// Any one factor confirms a step-up, so `complete` is expected on the
    /// first successful verify.
    pub async fn verify_step_up(&self, token: &str, code: &str) -> Result<String, BackError> {
        let request = models::AuthChallengeVerifyRequest {
            factor_id: "email_code".to_string(),
            credentials: Box::new(models::AuthChallengeVerifyRequestCredentials::CodeCredentials(
                Box::new(models::CodeCredentials::new(code.to_string())),
            )),
        };
        let result = auth_api::auth_challenges_verify(&self.api_config, token, request.clone(), None).await;
        self.log_api_call("POST", &format!("/auth/challenges/{}", token), &result);
        match result {
            Ok(response) => {
                if !response.complete {
                    return Err(BackError::Other("step-up verify: more factors required".to_string()));
                }
                response
                    .token
                    .flatten()
                    .ok_or_else(|| BackError::Other("step-up verify: no confirmed token in response".to_string()))
            }
            Err(e) => Err(BackError::Api {
                code: api_status(&e).unwrap_or_else(|| "challenge_verify_error".to_string()),
                message: fmt_api_error(&e),
            }),
        }
    }

    // --- Vault Claims (anonymous E2E sharing) ---

    pub async fn vault_claim_create(
        &self,
        request: models::VaultClaimCreateRequest,
    ) -> Result<models::VaultClaimCreateResponse, BackError> {
        let result = vault_api::vault_claim_create(&self.api_config, request.clone()).await;
        self.log_api_call("POST", "/vault/claims", &result);
        result.map_err(|e| BackError::Api {
            code: "vault_claim_create_error".to_string(),
            message: fmt_api_error(&e),
        })
    }

    /// Deliver wrapped share keys into the vault key-store (key-only, atomic all-or-none).
    pub async fn vault_keys_batch(
        &self,
        items: Vec<models::VaultKeyBatchItem>,
    ) -> Result<models::VaultKeyBatchResponse, BackError> {
        let request = models::VaultKeyBatchRequest { items };
        let result = vault_api::vault_keys_batch(&self.api_config, request.clone()).await;
        self.log_api_call("POST", "/vault/keys/batch", &result);
        result.map_err(|e| BackError::Api {
            code: "vault_keys_batch_error".to_string(),
            message: fmt_api_error(&e),
        })
    }

    /// `GET /vault/pending-keys`: one page of the share-key asks the caller can
    /// fulfil (links it owns or holds a key-store row on) — the catch-up behind
    /// `vault.share_key_needed` for a holder that was offline when the ask was
    /// queued. `cursor` continues a previous page; `None` starts over.
    pub async fn vault_pending_keys(&self, cursor: Option<&str>) -> Result<models::VaultPendingKeysList, BackError> {
        let result = vault_api::vault_pending_keys_list(&self.api_config, None, cursor, Some(100)).await;
        self.log_api_call("GET", "/vault/pending-keys", &result);
        result.map_err(|e| BackError::Api {
            code: "vault_pending_keys_error".to_string(),
            message: fmt_api_error(&e),
        })
    }

    pub async fn vault_claim_get(&self, claim_token_hash: String) -> Result<models::VaultClaimGetResponse, BackError> {
        let result = vault_api::vault_claim_get(&self.api_config, &claim_token_hash).await;
        self.log_api_call::<_, share_ninja_api::apis::Error<_>>(
            "GET",
            &format!("/vault/claims/{}", claim_token_hash),
            &result,
        );
        result.map_err(|e| BackError::Api {
            code: "vault_claim_get_error".to_string(),
            message: fmt_api_error(&e),
        })
    }

    pub async fn vault_claim_bind(
        &self,
        claim_token_hash: String,
        request: models::VaultClaimBindRequest,
    ) -> Result<models::VaultClaimBindResponse, BackError> {
        let result = vault_api::vault_claim_bind(&self.api_config, &claim_token_hash, request.clone()).await;
        self.log_api_call("POST", &format!("/vault/claims/{}/claim", claim_token_hash), &result);
        result.map_err(|e| BackError::Api {
            code: "vault_claim_bind_error".to_string(),
            message: fmt_api_error(&e),
        })
    }

    // --- Sessions ---

    /// Revoke a specific session by ID
    pub async fn revoke_session(&self, session_id: &str) -> Result<(), BackError> {
        self.check_online()?;
        let result = auth_api::sessions_revoke_by_id(&self.api_config, session_id).await;
        self.log_api_call("DELETE", &format!("/auth/sessions/{}", session_id), &result);
        result.map_err(|e| BackError::Api {
            code: "revoke_session_error".to_string(),
            message: fmt_api_error(&e),
        })
    }

    /// Revoke all active sessions
    pub async fn revoke_all_sessions(&self) -> Result<(), BackError> {
        self.check_online()?;
        let result = auth_api::sessions_revoke_all(&self.api_config).await;
        self.log_api_call("POST", "/auth/sessions/revoke-all", &result);
        result.map_err(|e| BackError::Api {
            code: "revoke_all_sessions_error".to_string(),
            message: fmt_api_error(&e),
        })
    }

    /// Build relay URLs from relay base URL and JWT token
    pub fn relay_urls(&self, relay: Option<&str>, jwt: Option<&str>) -> Option<RelayConfig> {
        match (relay, jwt) {
            (Some(relay_base), Some(jwt_token)) => {
                let send_url = format!(
                    "{}?token={}",
                    relay_base.trim_end_matches('/').to_string() + "/send",
                    jwt_token
                );
                let receive_url = format!(
                    "{}?token={}",
                    relay_base.trim_end_matches('/').to_string() + "/receive",
                    jwt_token
                );
                Some(RelayConfig { send_url, receive_url })
            }
            _ => None,
        }
    }

    fn convert_datasources(&self, datasources: &[models::TransferDatasource]) -> Vec<LinkDataSource> {
        datasources
            .iter()
            .map(|ds| {
                match ds {
                    // Cloud access is one flat struct with the mode in optional
                    // fields; this client only speaks presigned, so the core DTO
                    // carries `presigned_files` and ignores the STS `creds`.
                    models::TransferDatasource::S3(cloud) => {
                        let presigned_files = cloud
                            .presigned_files
                            .as_ref()
                            .map(|files| files.iter().cloned().map(convert_presigned_file).collect());
                        LinkDataSource::S3(LinkDataSourceS3 {
                            datasource_id: cloud.datasource_id.clone(),
                            endpoint: cloud.endpoint.clone(),
                            region: cloud.region.clone(),
                            bucket: cloud.bucket.clone(),
                            prefix: cloud.prefix.clone(),
                            invalidate_at: cloud.invalidate_at.map(|dt| dt.to_rfc3339()).unwrap_or_default(),
                            presigned_files,
                        })
                    }
                    models::TransferDatasource::P2p(p2p) => LinkDataSource::P2P(LinkDataSourceP2P {
                        datasource_id: p2p.datasource_id.clone(),
                        agent_id: p2p.agent_id.clone().unwrap_or_default(),
                    }),
                }
            })
            .collect()
    }
}

/// Convert a generated `PresignedFile` model into the plain core DTO.
fn convert_presigned_file(f: models::PresignedFile) -> PresignedFile {
    PresignedFile {
        path: f.path,
        url: f.url,
        headers: f.headers.map(|h| h.into_iter().collect()).unwrap_or_default(),
    }
}

fn presign_request_to_model(r: &PresignRequest) -> models::TransferFileRequest {
    models::TransferFileRequest {
        path: r.path.clone(),
        size: r.size.map(|s| s as i64),
        if_match: r.if_match.clone(),
        if_none_match: r.if_none_match.then_some(true),
    }
}

/// Log JWT info at info level (visible with --verbose)
pub fn log_jwt_info(token: &str) {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return;
    }

    let payload = match URL_SAFE_NO_PAD.decode(parts[1]) {
        Ok(bytes) => bytes,
        Err(_) => return,
    };

    let claims: serde_json::Value = match serde_json::from_slice(&payload) {
        Ok(v) => v,
        Err(_) => return,
    };

    let format_timestamp = |ts: i64| -> String {
        Utc.timestamp_opt(ts, 0)
            .single()
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
            .unwrap_or_else(|| ts.to_string())
    };

    let sub = claims.get("sub").and_then(|v| v.as_str()).unwrap_or("-");
    let iss = claims.get("iss").and_then(|v| v.as_str()).unwrap_or("-");
    let sid = claims.get("sid").and_then(|v| v.as_str()).unwrap_or("-");
    let mode = claims.get("mode").and_then(|v| v.as_str()).unwrap_or("-");

    let exp_info = claims
        .get("exp")
        .and_then(|v| v.as_i64())
        .map(|exp| {
            let now = Utc::now().timestamp();
            if exp < now {
                format!("{} (EXPIRED)", format_timestamp(exp))
            } else {
                let remaining = exp - now;
                let mins = remaining / 60;
                format!("{} (in {}m)", format_timestamp(exp), mins)
            }
        })
        .unwrap_or_else(|| "-".to_string());

    tracing::info!(
        subject = sub,
        issuer = iss,
        session_id = sid,
        mode = mode,
        expires = %exp_info,
        "JWT info"
    );
}

#[cfg(test)]
mod session_tests {
    use super::*;
    use crate::back::test_support::{Answer, MockBackend};

    fn stats() -> TransferStats {
        TransferStats {
            bytes: 1,
            files: 0,
            dirs: 0,
            symlinks: 0,
            duration: 5,
        }
    }

    /// The reaped session's 404 must surface as the expired-session code —
    /// the session loop keys on it (docs/app/reconnect.md, change set 2).
    #[tokio::test]
    async fn transfer_update_maps_transfer_not_found() {
        let mock = MockBackend::start().await;
        mock.script("download/update", &[Answer::NotFound]);
        let back = mock.back();
        let err = back
            .read()
            .await
            .transfer_update(true, "tok", "tid-1", &stats(), &[])
            .await
            .unwrap_err();
        assert!(err.is_session_expired(), "got {err:?}");
        assert_eq!(mock.count("download/update"), 1);
    }

    /// Any other backend answer keeps the fallback code — not a verdict on
    /// the session.
    #[tokio::test]
    async fn transfer_update_other_errors_are_not_session_expiry() {
        let mock = MockBackend::start().await;
        mock.script("download/update", &[Answer::ServerError]);
        let back = mock.back();
        let err = back
            .read()
            .await
            .transfer_update(true, "tok", "tid-1", &stats(), &[])
            .await
            .unwrap_err();
        assert!(!err.is_session_expired(), "got {err:?}");
        assert!(!err.is_transport(), "got {err:?}");
    }

    /// A session call whose answer never comes fails on the per-read
    /// timeout — within ~30 virtual seconds, as a transport-class error —
    /// instead of hanging until the OS notices (docs/app/reconnect.md,
    /// change set 5).
    #[tokio::test(start_paused = true)]
    async fn back_read_timeout_fails_a_frozen_call() {
        use crate::back::test_support::{advance, settle};
        let mock = MockBackend::start().await;
        mock.script("download/update", &[Answer::Hang]);
        let back = mock.back();
        let task = tokio::spawn(async move {
            back.read()
                .await
                .transfer_update(true, "tok", "tid-1", &stats(), &[])
                .await
        });
        settle().await;
        assert_eq!(
            mock.count("download/update"),
            1,
            "the request reached the backend and hangs"
        );
        assert!(!task.is_finished());
        advance(35).await;
        let err = task.await.unwrap().unwrap_err();
        assert!(err.is_transport(), "got {err:?}");
    }

    /// With the WS down `Back` fails fast — no HTTP goes out.
    #[tokio::test]
    async fn offline_back_makes_no_call() {
        let mock = MockBackend::start().await;
        let back = mock.back_in(BackState::NoConnection);
        let err = back
            .read()
            .await
            .transfer_update(true, "tok", "tid-1", &stats(), &[])
            .await
            .unwrap_err();
        assert!(err.is_transport(), "got {err:?}");
        assert_eq!(mock.count("download/update"), 0);
    }
}
