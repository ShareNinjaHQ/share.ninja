//! The commander's engine: the backend session (`Back` + device keypair +
//! unlocked vault identity, as `share-ninja-client` sets it up), the open
//! storages behind the panels, the one running copy, and the vault state the
//! dialogs render. Every method is `&self` on an `Arc<Engine>`; the TUI loop
//! spawns them on the runtime and reads the outcome off the [`UiEvent`]
//! channel, which the engine also feeds with backend-driven events.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Weak};

use async_trait::async_trait;
use tokio::sync::{Mutex, RwLock, mpsc, oneshot};

use share_ninja_client::Session;
use share_ninja_client::oauth::{LoginFlow, open_in_browser};
use share_ninja_client::share::{parse_share_url, resolve_share_access};
use share_ninja_client::ws::BackWsExt;
use share_ninja_core::back::{CallInInfo, LinkInfo};
use share_ninja_core::fs::{FsSource, LocalFs, volume_free_capacity};
use share_ninja_core::p2p::CallServer;
use share_ninja_core::transfers::{
    Conflict, ConflictDecision, ConflictResolver, ErrorDecision, ErrorResolver, OnError, OnExisting, SearchOptions,
    SearchResult, StorageDirStats, StorageSearchQuery, TransferDownload, TransferItem, TransferItemState,
    TransferPolicies, TransferUpload,
};
use share_ninja_core::utils::crypting::{
    LinkMetadata, P2P_HYBRID, decrypt_link_description, decrypt_metadata_description, encrypt_metadata, hybrid_decrypt,
    hybrid_encrypt,
};
use share_ninja_core::vault::{self, VaultIdentity};
use share_ninja_core::{
    Back, BackError, BackState, Storage, StorageDest, StorageDestMultipart, StorageError, StorageFileType,
    StorageObjectInfo, StorageSource, models,
};

use crate::archive;
use crate::events::{EventSender, StartTransfer, TransferDest, UiEvent};
use crate::types::{
    BackendConnection, BackendStatus, Drive, InboundOfferView, LinkRow, NearbyDevice, OfferStatus, OfferView, Space,
    PendingJoin, StepUpOutcome, StorageConnectRequest, StorageInfo, StorageType, VaultJoinApprover, VaultJoinRequest,
    VaultStateSnapshot,
};

/// A storage error as the panels show it: the "ran out of sources" class
/// (core's `sourceUnavailable` — the P2P peer gone, every source exhausted)
/// in the desktop's words, anything else as its own text.
pub fn storage_error_text(e: &StorageError) -> String {
    match e.user_code() {
        Some(("sourceUnavailable", _)) => "Source unavailable — the sender is offline".to_string(),
        _ => e.to_string(),
    }
}

/// The root local storage both panels start on.
/// Validity of a share created from the Links panel: 24 hours.
const NEW_SHARE_VALIDITY_SECS: i64 = 24 * 3600;
/// Validity of the P2P link behind a nearby offer (the desktop app's).
const OFFER_LINK_VALIDITY_SECS: i64 = 24 * 3600;
/// An offer nobody answers flips to `NoResponse` after this (the app's 30 s);
/// it is not withdrawn.
const OFFER_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
/// Loopback port the OAuth consent page redirects to: 0 = OS-picked free
/// port (same default as the `ninja` CLI's `auth login`); LoginFlow puts the
/// actually-bound port in the redirect_uri.
const OAUTH_CALLBACK_PORT: u16 = 0;
const OAUTH_CALLBACK_PATH: &str = "/oauth/callback";
/// Concurrent streams per copy (the CLI's default).
const N_STREAMS: usize = 5;

enum Backend {
    Local(Arc<RwLock<LocalFs>>),
    /// A share, browsed on the warm download transfer's composed source.
    Ninja {
        transfer: Arc<TransferDownload>,
        source: Arc<RwLock<dyn StorageSource>>,
    },
    /// An archive file on another storage, browsed in place (`crate::archive`).
    Archive(Arc<RwLock<dyn StorageSource>>),
}

/// A nearby offer we sent — one per device, a new one replaces it (the
/// desktop app's `OutboundOffer`).
struct OutboundOffer {
    offer_token: String,
    url_token: String,
    status: OfferStatus,
    bytes: Option<u64>,
    total_bytes: Option<u64>,
    /// The no-response timeout, while `Waiting`.
    timer: Option<tokio::task::AbortHandle>,
}

/// The one pending inbound nearby offer (the app's `pending_inbound`): the
/// share key that rode device-sealed in `share.offered`, unwrapped — the
/// offer IS the key channel, no key-store row exists for it. A newer offer
/// replaces it.
struct InboundOffer {
    offer_token: String,
    url_token: String,
    /// `None` when the frame's key did not unwrap with our device key.
    key: Option<[u8; 32]>,
    /// What the dialog shows; the storage's name on accept.
    view: InboundOfferView,
}

/// What an offered link's incoming P2P calls are served from.
struct Served {
    roots: Vec<PathBuf>,
    key: [u8; 32],
}

/// What a `link.transfer_*` frame tells an offer.
enum TransferSignal {
    Progress(Option<u64>),
    Completed {
        failed: bool,
        session: bool,
        bytes: Option<u64>,
    },
}

struct StorageEntry {
    id: String,
    name: String,
    kind: StorageType,
    /// The share URL / token a `Ninja` storage was opened from.
    link_url: Option<String>,
    backend: Backend,
}

/// The vault standing the dialogs render (mutable part).
#[derive(Default)]
struct VaultLocal {
    pending_join_requests: Vec<VaultJoinRequest>,
    setup_recovery_words: Option<Vec<String>>,
    revoked_pending: bool,
    pending_join: Option<PendingJoin>,
}

/// The standing join request from the backend's answer, the device line
/// derived like the app's device cards.
fn pending_join(resp: models::VaultDeviceJoinResponse) -> PendingJoin {
    PendingJoin {
        fingerprint: resp.device_fingerprint,
        approvers: resp
            .approvers
            .into_iter()
            .map(|a| VaultJoinApprover {
                desc: Some(share_ninja_core::utils::device::device_card_label(&a.device, a.product_id.as_deref()))
                    .filter(|s| !s.is_empty()),
                display_name: a.display_name,
                online: a.online,
                last_seen_at: a.last_seen_at,
            })
            .collect(),
    }
}

/// A recover parked behind the emailed step-up code.
struct PendingRecover {
    token: String,
    req: models::VaultRecoveryCompleteRequest,
    identity: VaultIdentity,
}

/// The one running copy.
struct ActiveTransfer {
    item: Arc<TransferItem>,
    conflict_slot: Arc<Mutex<Option<oneshot::Sender<ConflictDecision>>>>,
    error_slot: Arc<Mutex<Option<oneshot::Sender<ErrorDecision>>>>,
    /// The run task — aborted when a cancel isn't honoured in time (a source
    /// call that never returns, e.g. a dropped P2P peer).
    run_abort: tokio::task::AbortHandle,
}

/// How long a cancel waits for the run to end on its own before the task
/// is aborted.
const CANCEL_GRACE: std::time::Duration = std::time::Duration::from_secs(3);

pub struct Engine {
    /// The session's `Back` — the same `Arc` `session` holds, reachable
    /// without the session lock.
    pub back: Arc<RwLock<Back>>,
    /// The backend session: device keypair, vault standing, unlocked identity.
    session: RwLock<Session>,
    vault: RwLock<VaultLocal>,
    pending_step_up: Mutex<Option<PendingRecover>>,
    storages: RwLock<Vec<StorageEntry>>,
    transfer: Mutex<Option<ActiveTransfer>>,
    /// The last `nearby.updated` — what the Nearby storage lists.
    nearby: RwLock<Vec<NearbyDevice>>,
    /// Outbound nearby offers by recipient agent id.
    offers: Mutex<HashMap<String, OutboundOffer>>,
    /// The inbound nearby offer awaiting our answer.
    inbound: Mutex<Option<InboundOffer>>,
    /// Offered links we serve over P2P, by url token.
    served: Mutex<HashMap<String, Served>>,
    /// Answers the peers' `call.incoming` for the served links.
    call_server: Arc<Mutex<CallServer>>,
    tx: EventSender,
}

impl Engine {
    pub fn new(session: Session, tx: EventSender) -> Self {
        let mut call_server = CallServer::new();
        call_server.set_back(session.back.clone());
        Self {
            back: session.back.clone(),
            session: RwLock::new(session),
            vault: RwLock::new(VaultLocal::default()),
            pending_step_up: Mutex::new(None),
            storages: RwLock::new(Vec::new()),
            transfer: Mutex::new(None),
            nearby: RwLock::new(Vec::new()),
            offers: Mutex::new(HashMap::new()),
            inbound: Mutex::new(None),
            served: Mutex::new(HashMap::new()),
            call_server: Arc::new(Mutex::new(call_server)),
            tx,
        }
    }

    /// Subscribe to the backend: connection-state transitions and every raw
    /// WS frame. Weak self — the hooks outlive nothing.
    pub async fn install_hooks(self: &Arc<Self>) {
        let weak: Weak<Engine> = Arc::downgrade(self);
        let back = self.back.read().await;
        {
            let weak = weak.clone();
            back.add_state_callback(move |state| {
                if let Some(engine) = weak.upgrade() {
                    tokio::spawn(async move { engine.on_state(state).await });
                }
            })
            .await;
        }
        back.set_ws_message_handler(move |msg: models::WsMessage| {
            if let Some(engine) = weak.upgrade() {
                tokio::spawn(async move { engine.on_ws(msg).await });
            }
        })
        .await;
    }

    fn push(&self, event: UiEvent) {
        let _ = self.tx.send(event);
    }

    // ── backend state / vault standing ─────────────────────────────────────

    async fn on_state(&self, state: BackState) {
        tracing::info!(?state, "backend state");
        self.push(UiEvent::BackendStatus(self.backend_status().await));
        if matches!(state, BackState::Online | BackState::OnlineAndLoggedIn) {
            let res = self.vault_bind().await;
            self.push(UiEvent::VaultBindResult(res));
            self.push(UiEvent::Usage(self.usage_space().await));
        }
    }

    /// The account's storage quota standing (`GET /me`): free = cap − used.
    /// `None` when the tier has no storage cap or the call fails.
    pub async fn usage_space(&self) -> Option<Space> {
        let me = Back::refresh_me(&self.back).await.ok()?;
        let total = u64::try_from(me.limits.storage_bytes?).ok()?;
        let used = u64::try_from(me.usage.storage_bytes).unwrap_or(0);
        Some(Space {
            free: total.saturating_sub(used),
            total,
        })
    }

    /// The connection pane's view of `Back`.
    pub async fn backend_status(&self) -> BackendStatus {
        let b = self.back.read().await;
        BackendStatus {
            params: b.params.clone(),
            connection: BackendConnection {
                state: b.current_state(),
                agent_id: b.agent_id.clone(),
                agent_name: b.agent_name.clone(),
                access_token: b.access_token().unwrap_or_default(),
                refresh_token: b.refresh_token().unwrap_or_default(),
                vault_status: b.vault_status.clone(),
                encrypted_identity_key: b.encrypted_identity_key.clone(),
                user: b.user.clone(),
            },
        }
    }

    pub async fn snapshot(&self) -> VaultStateSnapshot {
        let s = self.session.read().await;
        let v = self.vault.read().await;
        VaultStateSnapshot {
            status: s.vault_status.clone(),
            identity_loaded: s.identity.is_some(),
            pending_join_requests: v.pending_join_requests.clone(),
            setup_recovery_words: v.setup_recovery_words.clone(),
            revoked_pending: v.revoked_pending,
            pending_join: v.pending_join.clone(),
        }
    }

    async fn push_snapshot(&self) {
        self.push(UiEvent::VaultSnapshot(self.snapshot().await));
    }

    /// Re-read the vault standing from the (re)registered backend
    /// (`Session::refresh_vault`); the join fingerprint only stays while the
    /// join is still pending.
    async fn refresh_vault(&self) {
        let mut s = self.session.write().await;
        s.refresh_vault().await;
        if s.vault_status.as_deref() != Some("pending_approval") {
            self.vault.write().await.pending_join = None;
        }
    }

    /// Run the vault flow for the backend's current status: `not_initialized`
    /// → setup (first device; the recovery words are shown once), `active` →
    /// unlock, `pending_approval` → refresh our fingerprint. `not_joined` is
    /// left to the explicit join prompt.
    pub async fn vault_bind(&self) -> Result<(), String> {
        self.refresh_vault().await;
        let (status, device) = {
            let s = self.session.read().await;
            (s.vault_status.clone(), s.device)
        };
        let res = match status.as_deref() {
            Some("not_initialized") => match vault::setup(&self.back, device).await {
                Ok(id) => {
                    let mut s = self.session.write().await;
                    s.identity = Some(id);
                    s.vault_status = Some("active".to_string());
                    Ok(())
                }
                Err(e) => Err(format!("vault setup failed: {e}")),
            },
            Some("pending_approval") => match vault::join_start(&self.back).await {
                Ok(resp) => {
                    self.vault.write().await.pending_join = Some(pending_join(resp));
                    Ok(())
                }
                Err(e) => Err(format!("join request refresh failed: {e}")),
            },
            _ => Ok(()),
        };
        self.push_snapshot().await;
        res
    }

    // ── WS frames ──────────────────────────────────────────────────────────

    async fn on_ws(&self, msg: models::WsMessage) {
        use models::WsMessage as M;
        match msg {
            M::NearbyUpdated(data) => {
                *self.nearby.write().await = data.nearby.clone();
                self.push(UiEvent::NearbyDevices { devices: data.nearby });
            }
            // A peer pulls an offered link: serve it over the relay WebRTC path.
            M::CallIncoming(data) => {
                self.serve_call(CallInInfo::from_ws_call(*data)).await;
            }
            M::ShareAnswered(data) => {
                let reject = matches!(data.answer, models::ws_share_answer::Answer::Reject);
                self.on_share_answered(&data.offer_token, reject).await;
            }
            // A nearby device offers us a share.
            M::ShareOffered(data) => {
                self.on_share_offered(*data).await;
            }
            // The sender withdrew it (only the pending one matters — an
            // answered offer is past withdrawing here).
            M::ShareCancelled(data) => {
                let matched = {
                    let mut inbound = self.inbound.lock().await;
                    if inbound.as_ref().is_some_and(|o| o.offer_token == data.offer_token) {
                        *inbound = None;
                        true
                    } else {
                        false
                    }
                };
                if matched {
                    tracing::info!(offer = %data.offer_token, "nearby offer withdrawn");
                    self.push(UiEvent::ShareOfferCancelled {
                        offer_token: data.offer_token,
                    });
                }
            }
            M::LinkTransferProgress(d) => {
                use models::ws_event_link_transfer_progress::Direction;
                let bytes = d.stats.as_ref().map(|s| s.bytes.max(0) as u64);
                self.on_link_transfer(
                    &d.topic,
                    &d.actor_id,
                    d.direction == Direction::Download,
                    TransferSignal::Progress(bytes),
                )
                .await;
            }
            M::LinkTransferCompleted(d) => {
                use models::ws_event_link_transfer_completed::{Direction, Scope};
                let bytes = d.stats.as_ref().map(|s| s.bytes.max(0) as u64);
                self.on_link_transfer(
                    &d.topic,
                    &d.actor_id,
                    d.direction == Direction::Download,
                    TransferSignal::Completed {
                        failed: matches!(d.success, Some(Some(false))),
                        session: d.scope != Some(Scope::File),
                        bytes,
                    },
                )
                .await;
            }
            M::VaultDeviceJoinRequested(data) => {
                let req = VaultJoinRequest {
                    request_id: data.request_id.clone(),
                    agent_id: data.agent_id.clone(),
                    device_name: data.device_name.clone(),
                    device_fingerprint: data.device_fingerprint.clone(),
                    device_public_key: data.device_public_key.clone(),
                    ip: data.ip.clone(),
                    country: data.country.clone(),
                    product: data.product_id.as_deref().map(share_ninja_core::utils::device::product),
                    expires_at: None,
                };
                {
                    let mut v = self.vault.write().await;
                    v.pending_join_requests.retain(|r| r.agent_id != req.agent_id);
                    v.pending_join_requests.push(req.clone());
                }
                self.push_snapshot().await;
                self.push(UiEvent::VaultJoinRequestArrived(req));
            }
            // The server's full pending set (sent on connect, not replayed):
            // replace ours with it.
            M::VaultJoinRequestsSnapshot(data) => {
                let reqs: Vec<VaultJoinRequest> = data
                    .requests
                    .iter()
                    .map(|r| VaultJoinRequest {
                        request_id: r.request_id.clone(),
                        agent_id: r.agent_id.clone(),
                        device_name: r.device_name.clone(),
                        device_fingerprint: r.device_fingerprint.clone(),
                        device_public_key: r.device_public_key.clone(),
                        ip: r.ip.clone(),
                        country: r.country.clone(),
                        product: r.product_id.as_deref().map(share_ninja_core::utils::device::product),
                        expires_at: r.expires_at,
                    })
                    .collect();
                self.vault.write().await.pending_join_requests = reqs;
                self.push_snapshot().await;
            }
            // A trusted device wrapped our identity key for us: unlock.
            M::VaultDeviceApproved(data) => {
                let (device, identity_pub) = {
                    let s = self.session.read().await;
                    (s.device, s.identity_public_key_b64().await)
                };
                let res = match vault::handle_device_approved(&data.encrypted_identity_key, device, identity_pub) {
                    Ok(id) => {
                        {
                            let mut s = self.session.write().await;
                            s.identity = Some(id);
                            s.vault_status = Some("active".to_string());
                        }
                        self.vault.write().await.pending_join = None;
                        Ok(())
                    }
                    Err(e) => Err(e.to_string()),
                };
                self.push_snapshot().await;
                self.push(UiEvent::VaultDeviceApproved(res));
            }
            M::VaultDeviceRejected(data) => {
                self.session.write().await.vault_status = Some("not_joined".to_string());
                self.vault.write().await.pending_join = None;
                self.push_snapshot().await;
                self.push(UiEvent::VaultDeviceRejected { reason: data.reason });
            }
            // Naming us: this device was removed from the trust circle. The
            // direct self_revoked converges on the same cleanup.
            M::VaultDeviceRevoked(data) => {
                let own = self.back.read().await.agent_id.clone();
                if own == data.agent_id {
                    self.handle_self_revoked().await;
                }
            }
            M::VaultDeviceSelfRevoked(_) => self.handle_self_revoked().await,
            M::LinkP2pOnline(d) => self.on_link_p2p(&d.topic, &d.online_agents),
            M::LinkP2pOffline(d) => self.on_link_p2p(&d.topic, &d.online_agents),
            // Resolved elsewhere (a sibling device decided it, or it expired):
            // drop it from our pending set. Expiry naming us: back to
            // not_joined so the prompt can offer to ask again.
            M::VaultDeviceApprovedBroadcast(data) => self.resolve_pending_join(&data.agent_id).await,
            M::VaultDeviceRejectedBroadcast(data) => self.resolve_pending_join(&data.agent_id).await,
            M::VaultDeviceJoinExpired(data) => {
                let own = self.back.read().await.agent_id.clone();
                if own == data.agent_id {
                    self.session.write().await.vault_status = Some("not_joined".to_string());
                    self.vault.write().await.pending_join = None;
                }
                self.resolve_pending_join(&data.agent_id).await;
            }
            _ => {}
        }
    }

    /// `link.p2p_online` / `link.p2p_offline`: a link's P2P liveness for the
    /// Links rows. `online_agents` is the complete set after the change, so
    /// a link another agent still serves stays reachable (as the desktop
    /// app reads it).
    fn on_link_p2p(&self, topic: &str, online_agents: &[String]) {
        let Some(token) = topic.strip_prefix("link:").filter(|t| !t.is_empty()) else {
            tracing::warn!("link p2p event with unexpected topic: {topic}");
            return;
        };
        self.push(UiEvent::LinkP2p {
            token: token.to_string(),
            online: !online_agents.is_empty(),
        });
    }

    async fn resolve_pending_join(&self, agent_id: &str) {
        self.vault
            .write()
            .await
            .pending_join_requests
            .retain(|r| r.agent_id != agent_id);
        self.push_snapshot().await;
    }

    async fn handle_self_revoked(&self) {
        {
            let mut s = self.session.write().await;
            s.identity = None;
            s.vault_status = Some("not_joined".to_string());
        }
        {
            let mut v = self.vault.write().await;
            v.pending_join = None;
            v.revoked_pending = true;
        }
        self.push_snapshot().await;
        self.push(UiEvent::VaultRevoked);
    }

    // ── session ────────────────────────────────────────────────────────────

    /// Browser OAuth login over the loopback callback, then the register
    /// handshake (`back.user` set) and a fresh vault standing.
    pub async fn login(&self) -> Result<(), String> {
        let flow = LoginFlow::begin(&self.back, "127.0.0.1", OAUTH_CALLBACK_PORT, OAUTH_CALLBACK_PATH)
            .await
            .map_err(|e| e.to_string())?;
        tracing::info!(url = %flow.consent_url, "login: opening browser");
        open_in_browser(&flow.consent_url).map_err(|e| e.to_string())?;
        flow.finish(self.back.clone()).await.map_err(|e| e.to_string())?;
        self.vault_bind().await?;
        self.push(UiEvent::BackendStatus(self.backend_status().await));
        Ok(())
    }

    /// End the user session and re-register anonymously.
    pub async fn logout(&self) -> Result<(), String> {
        self.back.write().await.logout().await.map_err(|e| e.to_string())?;
        Back::run_register(self.back.clone()).await.map_err(|e| e.to_string())?;
        self.refresh_vault().await;
        {
            let mut v = self.vault.write().await;
            v.pending_join_requests.clear();
            v.setup_recovery_words = None;
        }
        *self.pending_step_up.lock().await = None;
        self.push_snapshot().await;
        self.push(UiEvent::BackendStatus(self.backend_status().await));
        Ok(())
    }

    // ── storages ───────────────────────────────────────────────────────────

    pub async fn list_storages(&self) -> Vec<StorageInfo> {
        self.storages
            .read()
            .await
            .iter()
            .map(|s| StorageInfo {
                storage_id: s.id.clone(),
                name: s.name.clone(),
                r#type: s.kind,
                link_url: s.link_url.clone(),
            })
            .collect()
    }

    async fn register_storage(&self, entry: StorageEntry) -> String {
        let id = entry.id.clone();
        let mut storages = self.storages.write().await;
        storages.retain(|s| s.id != id);
        storages.push(entry);
        id
    }

    /// The local volumes with their space, off the runtime's blocking pool
    /// (a network drive's space query can stall).
    pub async fn local_drives(&self) -> Vec<Drive> {
        tokio::task::spawn_blocking(enumerate_drives).await.unwrap_or_default()
    }

    async fn is_registered(&self, id: &str, kind: StorageType) -> bool {
        self.storages.read().await.iter().any(|s| s.id == id && s.kind == kind)
    }

    /// Open a storage; returns its id. A local path opens as a `LocalFs`
    /// rooted there (the id is that root, `/` or `C:\`); a share URL/token
    /// resolves its key through the session
    /// and connects the share's sources on a warm download transfer; an
    /// archive file opens on its storage's source (id `<storage id>::<key>`).
    /// Idempotent: a storage already open under that id is returned as is —
    /// a share's warm source is kept, not rebuilt.
    pub async fn connect_storage(&self, name: &str, request: StorageConnectRequest) -> Result<String, String> {
        match request {
            StorageConnectRequest::Local { root_path } => {
                let id = root_path.clone();
                if self.is_registered(&id, StorageType::Local).await {
                    return Ok(id);
                }
                let mut fs = LocalFs::new(&root_path);
                fs.connect().await.map_err(|e| e.to_string())?;
                Ok(self
                    .register_storage(StorageEntry {
                        id,
                        name: name.to_string(),
                        kind: StorageType::Local,
                        link_url: None,
                        backend: Backend::Local(Arc::new(RwLock::new(fs))),
                    })
                    .await)
            }
            StorageConnectRequest::Ninja { link_url } => {
                // A bare token names the share without any network: an open
                // one is answered here. A claim fragment yields its token only
                // through resolution — checked again below.
                let (token, url_key) = parse_share_url(&link_url).map_err(|e| e.to_string())?;
                if url_key.is_none() && !token.is_empty() && self.is_registered(&token, StorageType::Ninja).await {
                    return Ok(token);
                }
                let (token, key) = resolve_share_access(&*self.session.read().await, &link_url, None)
                    .await
                    .map_err(|e| e.to_string())?;
                if self.is_registered(&token, StorageType::Ninja).await {
                    return Ok(token);
                }
                self.open_share(token, key, name, Some(link_url)).await
            }
            StorageConnectRequest::Archive { storage_id, path } => {
                let id = format!("{storage_id}::{path}");
                if self.is_registered(&id, StorageType::Archive).await {
                    return Ok(id);
                }
                let base = self.source(&storage_id).await?;
                let size = base
                    .read()
                    .await
                    .get_attrs(&path)
                    .await
                    .map_err(|e| e.to_string())?
                    .size;
                let source = archive::open(base, path, size).await?;
                Ok(self
                    .register_storage(StorageEntry {
                        id,
                        name: name.to_string(),
                        kind: StorageType::Archive,
                        link_url: None,
                        backend: Backend::Archive(source),
                    })
                    .await)
            }
        }
    }

    /// Connect a share's sources on a warm download transfer with its key in
    /// hand and register it as a `Ninja` storage under its token (the id).
    /// `link_url` is what it was opened from, for the selector; an offered
    /// share has none.
    async fn open_share(
        &self,
        token: String,
        key: [u8; 32],
        name: &str,
        link_url: Option<String>,
    ) -> Result<String, String> {
        let mut transfer = TransferDownload::new(self.back.clone());
        transfer
            .connect_source(&token, Some(key))
            .await
            .map_err(|e| storage_error_text(&e))?;
        let source = transfer.source().expect("source set after connect_source");
        Ok(self
            .register_storage(StorageEntry {
                id: token,
                name: name.to_string(),
                kind: StorageType::Ninja,
                link_url,
                backend: Backend::Ninja {
                    transfer: Arc::new(transfer),
                    source,
                },
            })
            .await)
    }

    async fn local_fs(&self, storage_id: &str) -> Result<Arc<RwLock<LocalFs>>, String> {
        let storages = self.storages.read().await;
        let entry = storages
            .iter()
            .find(|s| s.id == storage_id)
            .ok_or_else(|| format!("no storage {storage_id}"))?;
        match &entry.backend {
            Backend::Local(fs) => Ok(fs.clone()),
            Backend::Ninja { .. } => Err("Share.Ninja storage is read-only".to_string()),
            Backend::Archive(_) => Err("an archive is read-only".to_string()),
        }
    }

    /// A storage's read side: the local fs, the share's composed source
    /// (P2P / S3, failing over per range) on its warm transfer, or the
    /// archive source.
    pub(crate) async fn source(&self, storage_id: &str) -> Result<Arc<RwLock<dyn StorageSource>>, String> {
        let storages = self.storages.read().await;
        let entry = storages
            .iter()
            .find(|s| s.id == storage_id)
            .ok_or_else(|| format!("no storage {storage_id}"))?;
        match &entry.backend {
            Backend::Local(fs) => Ok(fs.clone()),
            Backend::Ninja { source, .. } => Ok(source.clone()),
            Backend::Archive(source) => Ok(source.clone()),
        }
    }

    // ── nearby ─────────────────────────────────────────────────────────────

    async fn push_offers(&self) {
        let views: Vec<OfferView> = self
            .offers
            .lock()
            .await
            .iter()
            .map(|(agent_id, o)| OfferView {
                agent_id: agent_id.clone(),
                status: o.status,
                bytes: o.bytes,
                total_bytes: o.total_bytes,
            })
            .collect();
        self.push(UiEvent::NearbyOffers(views));
    }

    /// Offer `roots` (absolute local paths, shared by basename) to a nearby
    /// device — the desktop app's `send_nearby_share`: a fresh share key, a
    /// P2P-only link whose description is `name`, the key sealed to the
    /// device's public key inside the offer, then serve the link's incoming
    /// P2P calls from the selection (`keys` under the panel's local storage).
    /// One offer per device; a new one replaces it.
    pub async fn send_nearby_offer(
        self: &Arc<Self>,
        agent_id: &str,
        storage_id: &str,
        keys: Vec<String>,
        name: String,
    ) -> Result<(), String> {
        let peer_pub: [u8; 32] = {
            let nearby = self.nearby.read().await;
            let dev = nearby
                .iter()
                .find(|d| d.agent_id == agent_id)
                .ok_or_else(|| "this device is no longer nearby".to_string())?;
            dev.device_public_key
                .as_deref()
                .and_then(|k| k.try_into().ok())
                .ok_or_else(|| "this device has no key to seal the share to".to_string())?
        };
        let fs = self.local_fs(storage_id).await?;
        let root = fs.read().await.root().to_path_buf();
        let roots: Vec<PathBuf> = keys.iter().map(|k| root.join(k)).collect();
        {
            let mut offers = self.offers.lock().await;
            if let Some(prev) = offers.get_mut(agent_id)
                && let Some(h) = prev.timer.take()
            {
                h.abort();
            }
            offers.insert(
                agent_id.to_string(),
                OutboundOffer {
                    offer_token: String::new(),
                    url_token: String::new(),
                    status: OfferStatus::Sending,
                    bytes: None,
                    total_bytes: None,
                    timer: None,
                },
            );
        }
        self.push_offers().await;

        // Size the selection through the panel's storage so `estimated_bytes`
        // rides the link create itself (patching after the send would race the
        // peer's link fetch). Best-effort: on a failed walk the offer still
        // goes out, just unsized.
        let estimated_bytes = {
            let mut total: u64 = 0;
            let mut sized = true;
            for key in &keys {
                let mut rx = fs.write().await.get_dir_stats_streamed(key).await;
                let mut last = None;
                while let Some(block) = rx.recv().await {
                    match block {
                        Ok(s) => last = Some(s),
                        Err(e) => {
                            tracing::warn!(key = %key, "offer dir stats failed: {e}");
                            last = None;
                            break;
                        }
                    }
                }
                match last {
                    Some(s) => total += s.total_size,
                    None => {
                        sized = false;
                        break;
                    }
                }
            }
            sized.then_some(total as i64)
        };

        let built = self
            .build_nearby_offer(agent_id, roots, &name, peer_pub, estimated_bytes)
            .await;
        let (url_token, offer_token) = match built {
            Ok(v) => v,
            Err(e) => {
                self.offers.lock().await.remove(agent_id);
                self.push_offers().await;
                return Err(e);
            }
        };
        {
            let mut offers = self.offers.lock().await;
            if let Some(o) = offers.get_mut(agent_id) {
                o.offer_token = offer_token;
                o.url_token = url_token;
                o.status = OfferStatus::Waiting;
                let engine = self.clone();
                let agent = agent_id.to_string();
                let timer = tokio::spawn(async move {
                    tokio::time::sleep(OFFER_TIMEOUT).await;
                    let mut offers = engine.offers.lock().await;
                    if let Some(o) = offers.get_mut(&agent)
                        && o.status == OfferStatus::Waiting
                    {
                        o.status = OfferStatus::NoResponse;
                        o.timer = None;
                        drop(offers);
                        engine.push_offers().await;
                    }
                });
                o.timer = Some(timer.abort_handle());
            }
        }
        self.push_offers().await;
        tracing::info!(agent_id, name = %name, "nearby offer sent");
        Ok(())
    }

    /// The link + offer; on failure the link is deleted again. Returns
    /// `(url_token, offer_token)`.
    async fn build_nearby_offer(
        &self,
        agent_id: &str,
        roots: Vec<PathBuf>,
        name: &str,
        peer_pub: [u8; 32],
        estimated_bytes: Option<i64>,
    ) -> Result<(String, String), String> {
        let key: [u8; 32] = {
            use rand::Rng;
            let mut k = [0u8; 32];
            rand::rng().fill_bytes(&mut k);
            k
        };
        let encrypted_metadata = encrypt_metadata(
            &LinkMetadata {
                description: Some(name.to_string()),
            },
            &key,
        )
        .map_err(|e| format!("encrypt link metadata: {e}"))?;
        let encrypted_share_key = self.session.read().await.wrap_own_share_key(&key).await;
        let link_info = LinkInfo {
            description: None,
            validity: Some(OFFER_LINK_VALIDITY_SECS),
            workspace_id: None,
            cloud_storage: Some(false),
            p2p_storage: Some(true),
            estimated_bytes,
            encrypted_metadata: Some(encrypted_metadata),
            encrypted_share_key,
            notify_on_open: None,
            notify_on_download: None,
        };
        let url_token = self
            .back
            .read()
            .await
            .link(&link_info)
            .await
            .map_err(|e| format!("create link: {e}"))?
            .url_token;
        // The offer is the key channel: the share key sealed to the device
        // key, never a key-store row.
        let enc_share_key =
            hybrid_encrypt(&key, &peer_pub, Some(P2P_HYBRID)).map_err(|e| format!("seal share key: {e}"))?;
        self.served
            .lock()
            .await
            .insert(url_token.clone(), Served { roots, key });
        let offer = self
            .back
            .read()
            .await
            .send_share_offer(agent_id, &url_token, Some(enc_share_key))
            .await;
        match offer {
            Ok(offer_token) => Ok((url_token, offer_token)),
            Err(e) => {
                self.served.lock().await.remove(&url_token);
                if let Err(de) = self.back.read().await.link_delete(&url_token).await {
                    tracing::warn!(token = %url_token, "link_delete after failed offer: {de}");
                }
                Err(format!("send offer: {e}"))
            }
        }
    }

    /// Withdraw an offer: tell the peer, close any P2P serve, delete the
    /// link. Best-effort throughout; the entry goes regardless.
    pub async fn cancel_nearby_offer(&self, agent_id: &str) -> Result<(), String> {
        let Some(mut offer) = self.offers.lock().await.remove(agent_id) else {
            return Err("no offer to this device".to_string());
        };
        if let Some(h) = offer.timer.take() {
            h.abort();
        }
        let back = self.back.read().await;
        if !offer.offer_token.is_empty()
            && let Err(e) = back.send_share_cancel(&offer.offer_token).await
        {
            tracing::debug!("send_share_cancel: {e}");
        }
        if !offer.url_token.is_empty() {
            self.call_server.lock().await.cancel_serve(&offer.url_token).await;
            self.served.lock().await.remove(&offer.url_token);
            if let Err(e) = back.link_delete(&offer.url_token).await {
                tracing::debug!("link_delete on cancel: {e}");
            }
        }
        drop(back);
        self.push_offers().await;
        Ok(())
    }

    /// `share.offered`: unwrap the share key sealed to our device key (the
    /// app's rule — the offer is device-sealed by definition), fetch the link
    /// for what the frame does not carry (the size in the clear, the
    /// description under the offered key), and hold it as the pending
    /// inbound offer for the dialog. The slot is taken before the fetch so a
    /// withdrawal landing meanwhile still matches; a newer offer landing
    /// meanwhile wins (matched on the token after the fetch).
    async fn on_share_offered(&self, data: models::WsShareOffer) {
        let device_priv = self.session.read().await.device.1;
        let key = data
            .encrypted_share_key
            .as_deref()
            .and_then(|enc| hybrid_decrypt(enc, &device_priv, Some(P2P_HYBRID)).ok())
            .and_then(|v| <[u8; 32]>::try_from(v.as_slice()).ok());
        if key.is_none() {
            tracing::warn!(offer = %data.offer_token, "share.offered: key did not unwrap with the device key");
        }
        let mut view = InboundOfferView {
            offer_token: data.offer_token.clone(),
            sender_name: data.sender_name,
            sender_display_name: data.sender_display_name,
            description: String::new(),
            bytes: None,
        };
        *self.inbound.lock().await = Some(InboundOffer {
            offer_token: data.offer_token.clone(),
            url_token: data.url_token.clone(),
            key,
            view: view.clone(),
        });
        match self.back.read().await.link_get(&data.url_token, None).await {
            Ok(link) => {
                view.bytes = link
                    .size
                    .as_ref()
                    .and_then(|s| s.estimated_bytes)
                    .map(|v| v.max(0) as u64);
                view.description = key
                    .and_then(|key| {
                        link.encrypted_metadata
                            .as_deref()
                            .and_then(|em| decrypt_metadata_description(em, &key))
                    })
                    .unwrap_or_default();
            }
            Err(e) => {
                tracing::warn!(token = %data.url_token, "share.offered: link_get failed: {e}");
            }
        }
        {
            let mut inbound = self.inbound.lock().await;
            match inbound.as_mut() {
                Some(o) if o.offer_token == data.offer_token => o.view = view.clone(),
                // Withdrawn or superseded while the link was fetched.
                _ => return,
            }
        }
        tracing::info!(sender = %view.sender_name, offer = %data.offer_token, "nearby offer received");
        self.push(UiEvent::ShareOffered(view));
    }

    /// Accept the pending inbound offer: answer "accept" and open the share
    /// in the panels straight from the offer's key. Returns the storage id
    /// (the url token) and the storage's name: the share's description,
    /// else the sender's device name. An offer whose key did not unwrap is
    /// declined instead — nothing could open it.
    pub async fn accept_inbound_offer(&self) -> Result<(String, String), String> {
        let offer = self
            .inbound
            .lock()
            .await
            .take()
            .ok_or_else(|| "the offer is gone".to_string())?;
        let back = self.back.read().await;
        let Some(key) = offer.key else {
            back.send_share_answer(&offer.offer_token, "reject")
                .await
                .map_err(|e| format!("answer offer: {e}"))?;
            return Err("the offer's key was not sealed to this device — declined".to_string());
        };
        back.send_share_answer(&offer.offer_token, "accept")
            .await
            .map_err(|e| format!("answer offer: {e}"))?;
        drop(back);
        let name = if offer.view.description.is_empty() {
            offer.view.sender_name
        } else {
            offer.view.description
        };
        if self.is_registered(&offer.url_token, StorageType::Ninja).await {
            return Ok((offer.url_token, name));
        }
        let id = self.open_share(offer.url_token, key, &name, None).await?;
        tracing::info!(storage = %id, name = %name, "nearby offer accepted");
        Ok((id, name))
    }

    /// Decline the pending inbound offer (answer "reject"); nothing pending
    /// is fine.
    pub async fn reject_inbound_offer(&self) -> Result<(), String> {
        let Some(offer) = self.inbound.lock().await.take() else {
            return Ok(());
        };
        tracing::info!(offer = %offer.offer_token, "nearby offer declined");
        self.back
            .read()
            .await
            .send_share_answer(&offer.offer_token, "reject")
            .await
            .map_err(|e| format!("answer offer: {e}"))
    }

    /// `share.answered`: accept → the peer will pull; reject → tear the
    /// link down and show `Declined`.
    async fn on_share_answered(&self, offer_token: &str, reject: bool) {
        let url_token = {
            let mut offers = self.offers.lock().await;
            let Some(o) = offers
                .values_mut()
                .find(|o| !o.offer_token.is_empty() && o.offer_token == offer_token)
            else {
                return;
            };
            if let Some(h) = o.timer.take() {
                h.abort();
            }
            o.status = if reject {
                OfferStatus::Declined
            } else {
                OfferStatus::Transferring
            };
            o.url_token.clone()
        };
        if reject {
            self.call_server.lock().await.cancel_serve(&url_token).await;
            self.served.lock().await.remove(&url_token);
            if let Err(e) = self.back.read().await.link_delete(&url_token).await {
                tracing::debug!("link_delete on decline: {e}");
            }
        }
        self.push_offers().await;
    }

    /// A peer's download of one of our offered links: progress, and the
    /// delivery signal — a successful completion at any scope (the peer's
    /// file end) marks the offer `Sent`; a failed session marks it `Failed`.
    async fn on_link_transfer(&self, topic: &str, actor_id: &str, is_download: bool, signal: TransferSignal) {
        let Some(url_token) = topic.strip_prefix("link:").filter(|t| !t.is_empty()) else {
            return;
        };
        if !is_download || actor_id == self.back.read().await.agent_id {
            return;
        }
        let changed = {
            let mut offers = self.offers.lock().await;
            let Some(o) = offers.values_mut().find(|o| o.url_token == url_token) else {
                return;
            };
            match signal {
                TransferSignal::Progress(bytes) => {
                    if matches!(o.status, OfferStatus::Waiting | OfferStatus::Transferring) {
                        o.status = OfferStatus::Transferring;
                        if bytes.is_some() {
                            o.bytes = bytes;
                        }
                        true
                    } else {
                        false
                    }
                }
                TransferSignal::Completed {
                    failed: false, bytes, ..
                } => {
                    if let Some(h) = o.timer.take() {
                        h.abort();
                    }
                    o.status = OfferStatus::Sent;
                    if bytes.is_some() {
                        o.bytes = bytes;
                    }
                    true
                }
                TransferSignal::Completed {
                    failed: true,
                    session: true,
                    ..
                } => {
                    o.status = OfferStatus::Failed;
                    true
                }
                TransferSignal::Completed { .. } => false,
            }
        };
        if changed {
            self.push_offers().await;
        }
    }

    /// `call.incoming` for an offered link: serve `roots` over the relay
    /// WebRTC path (the CLI's `put` / the app's `serve_call`).
    async fn serve_call(&self, ci: CallInInfo) {
        let cs = self.call_server.lock().await;
        if cs.is_call_active(&ci.call_id).await {
            tracing::debug!(call = %ci.call_id, "duplicate P2P call, ignoring");
            return;
        }
        if ci.relay.is_none() {
            tracing::warn!(call = %ci.call_id, "P2P call without relay — not supported");
            return;
        }
        let (roots, key) = match self.served.lock().await.get(&ci.url_token) {
            Some(s) => (s.roots.clone(), s.key),
            None => {
                tracing::info!(token = %ci.url_token, "P2P call: not an offered link");
                return;
            }
        };
        let source: Arc<RwLock<dyn StorageSource + Send + Sync>> = Arc::new(RwLock::new(FsSource::new(roots)));
        if let Err(e) = source.write().await.connect().await {
            tracing::error!("P2P call: source connect failed: {e}");
            return;
        }
        tracing::info!(call = %ci.call_id, peer = ?ci.peer_name, token = %ci.url_token, "serving P2P call");
        if let Err(e) = cs.on_call_with_relay(source, ci, Some(key)).await {
            tracing::error!("P2P call setup failed: {e}");
        }
    }

    // ── viewer reads ───────────────────────────────────────────────────────
    //
    // Range reads straight off the source — the same `get_streamed` the
    // desktop's preview path uses on a warm browse source, no per-file
    // transfer session and no copy. A share file is served by whichever leg
    // the MultiSource picks (P2P `Get{offset,length}` / S3 `Range:`).

    /// The file's size (`get_attrs`).
    pub async fn file_size(&self, storage_id: &str, path: &str) -> Result<u64, String> {
        let source = self.source(storage_id).await?;
        let attrs = source.read().await.get_attrs(path).await.map_err(|e| e.to_string())?;
        Ok(attrs.size)
    }

    /// `[offset, offset+len)` of the file, clamped to its end by the source.
    pub async fn read_range(&self, storage_id: &str, path: &str, offset: u64, len: u64) -> Result<Vec<u8>, String> {
        let source = self.source(storage_id).await?;
        drain_range(&source, path, offset, len).await
    }

    /// First occurrence of `needle` at or after `from`, scanning the file
    /// forward in viewer-block-sized reads (matches straddling reads are
    /// kept by a carry of `needle.len() - 1` bytes). `progress` is told the
    /// offset scanned up to after every read. Aborting the task cancels the
    /// read in flight (its receiver drops).
    pub async fn search_forward(
        &self,
        storage_id: &str,
        path: &str,
        from: u64,
        size: u64,
        needle: &[u8],
        mut progress: impl FnMut(u64) + Send,
    ) -> Result<Option<u64>, String> {
        let chunk = crate::viewer::BLOCK_SIZE;
        if needle.is_empty() {
            return Err("empty search string".to_string());
        }
        let source = self.source(storage_id).await?;
        let keep = needle.len() - 1;
        let mut carry: Vec<u8> = Vec::new();
        let mut off = from;
        while off < size {
            let bytes = drain_range(&source, path, off, chunk).await?;
            if bytes.is_empty() {
                break;
            }
            let base = off - carry.len() as u64;
            let mut window = std::mem::take(&mut carry);
            window.extend_from_slice(&bytes);
            if let Some(i) = window.windows(needle.len()).position(|w| w == needle) {
                return Ok(Some(base + i as u64));
            }
            off += bytes.len() as u64;
            progress(off);
            carry = window[window.len().saturating_sub(keep)..].to_vec();
        }
        Ok(None)
    }

    /// Delete a link (F8 on the Links panel). A share open on it is closed:
    /// its storage leaves the selector.
    pub async fn delete_link(&self, token: &str) -> Result<(), String> {
        self.back
            .read()
            .await
            .link_delete(token)
            .await
            .map_err(|e| e.to_string())?;
        self.storages
            .write()
            .await
            .retain(|s| !(s.kind == StorageType::Ninja && s.id == token));
        self.push(UiEvent::ListStorages(self.list_storages().await));
        Ok(())
    }

    /// The links list: one row per link, named by its decrypted description
    /// (the plaintext field, then the token, as fallbacks); duplicate names
    /// get the token appended. Server order is kept.
    pub async fn list_links(&self) -> Result<Vec<LinkRow>, String> {
        let links = self.back.read().await.links().await.map_err(|e| e.to_string())?;
        // The description rides E2E-encrypted under the share key; our
        // key-store row is sealed to the identity (logged-in shares) or the
        // device key (anonymous ones).
        let root: [u8; 32] = {
            let s = self.session.read().await;
            match &s.identity {
                Some(id) => id.private,
                None => s.device.1,
            }
        };
        let mut seen = std::collections::HashSet::new();
        let mut entries = Vec::with_capacity(links.len());
        for l in &links {
            let description = match (l.encrypted_share_key.as_ref(), l.encrypted_metadata.as_ref()) {
                (Some(esk), Some(em)) => decrypt_link_description(esk, em, &root),
                _ => None,
            }
            .or_else(|| l.description.clone())
            .filter(|d| !d.trim().is_empty());
            let mut name = description.unwrap_or_else(|| l.url_token.clone());
            if !seen.insert(name.clone()) {
                name = format!("{} ({})", name, l.url_token);
                seen.insert(name.clone());
            }
            // `actual_bytes` is 0 until an upload completes; fall back to the estimate.
            let size = l.size.as_ref().map(|sz| {
                if sz.actual_bytes > 0 {
                    sz.actual_bytes
                } else {
                    sz.estimated_bytes.unwrap_or(0)
                }
            });
            entries.push(LinkRow {
                name,
                token: l.url_token.clone(),
                size: u64::try_from(size.unwrap_or(0)).unwrap_or(0),
                created_at: l.created_at,
                invalidate_at: l.invalidate_at,
                cloud: l.cloud,
                p2p: l.p2p,
            });
        }
        Ok(entries)
    }

    /// One directory level of a storage as the source streams it: the
    /// `get_dir` batch receiver, keys as the source names them (the caller
    /// takes the basename), plus a local volume's free/total space under
    /// that directory.
    pub async fn list_dir(
        &self,
        storage_id: &str,
        path: &str,
    ) -> Result<
        (
            mpsc::Receiver<Result<Vec<StorageObjectInfo>, StorageError>>,
            Option<Space>,
        ),
        String,
    > {
        enum Src {
            Local(Arc<RwLock<LocalFs>>),
            Other(Arc<RwLock<dyn StorageSource>>),
        }
        let src = {
            let storages = self.storages.read().await;
            let entry = storages
                .iter()
                .find(|s| s.id == storage_id)
                .ok_or_else(|| format!("no storage {storage_id}"))?;
            match &entry.backend {
                Backend::Local(fs) => Src::Local(fs.clone()),
                Backend::Ninja { source, .. } => Src::Other(source.clone()),
                Backend::Archive(source) => Src::Other(source.clone()),
            }
        };
        let (source, space): (Arc<RwLock<dyn StorageSource>>, Option<Space>) = match src {
            Src::Local(fs) => {
                let dir = fs.read().await.root().join(path.trim_matches('/'));
                let space = tokio::task::spawn_blocking(move || volume_free_capacity(&dir))
                    .await
                    .ok()
                    .and_then(|r| r.ok())
                    .map(|(free, total)| Space { free, total });
                (fs, space)
            }
            Src::Other(source) => (source, None),
        };
        let rx = source.write().await.get_dir(path, false, None, Some(500)).await;
        Ok((rx, space))
    }

    pub async fn mkdir(&self, storage_id: &str, path: &str) -> Result<(), String> {
        let fs = self.local_fs(storage_id).await?;
        fs.read()
            .await
            .put_dir(path, None)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    /// The bytes under directory `path` (F3 on a folder): the source's
    /// streamed dir stats — cumulative running totals, the last one the
    /// answer.
    pub async fn dir_size(
        &self,
        storage_id: &str,
        path: &str,
    ) -> Result<mpsc::Receiver<Result<StorageDirStats, StorageError>>, String> {
        let source = self.source(storage_id).await?;
        Ok(source.write().await.get_dir_stats_streamed(path).await)
    }

    /// One page of a name search on a storage (Alt+F7): the source's
    /// `search`, read-locked for the page only.
    pub async fn search(
        &self,
        storage_id: &str,
        query: &StorageSearchQuery,
        options: &SearchOptions,
    ) -> Result<SearchResult, String> {
        let source = self.source(storage_id).await?;
        let res = source.read().await.search(query, options).await;
        res.map_err(|e| e.to_string())
    }

    /// Rename `from` to `to` (root-relative keys) on a local storage; a
    /// share is read-only.
    pub async fn rename(&self, storage_id: &str, from: &str, to: &str) -> Result<(), String> {
        let fs = self.local_fs(storage_id).await?;
        let root = fs.read().await.root().to_path_buf();
        let (src, dst) = (root.join(from), root.join(to));
        if dst.exists() {
            return Err(format!("{} already exists", to));
        }
        tokio::task::spawn_blocking(move || std::fs::rename(&src, &dst))
            .await
            .map_err(|e| format!("rename join: {e}"))?
            .map_err(|e| e.to_string())
    }

    /// Delete a file, symlink, or a directory tree (leaves first).
    pub async fn delete(&self, storage_id: &str, path: &str) -> Result<(), String> {
        let fs = self.local_fs(storage_id).await?;
        let attrs = fs.read().await.get_attrs(path).await.map_err(|e| e.to_string())?;
        if attrs.file_type != StorageFileType::Dir {
            return fs.read().await.del_file(path).await.map_err(|e| e.to_string());
        }
        let source: Arc<RwLock<dyn StorageSource>> = fs.clone();
        let mut items = drain_dir(&source, path, true).await?;
        // Deepest keys first, so every dir is empty by the time it's removed.
        items.sort_by_key(|s| std::cmp::Reverse(s.key.matches('/').count()));
        let fs = fs.read().await;
        for soi in &items {
            match soi.file_type {
                StorageFileType::Dir => fs.del_dir(&soi.key).await,
                _ => fs.del_file(&soi.key).await,
            }
            .map_err(|e| e.to_string())?;
        }
        fs.del_dir(path).await.map_err(|e| e.to_string())
    }

    // ── transfers ──────────────────────────────────────────────────────────

    /// Start a copy on the transfer engine. Progress, conflicts, errors and
    /// the terminal status arrive as events; one copy at a time.
    pub async fn start_transfer(self: &Arc<Self>, req: StartTransfer) -> Result<(), String> {
        if self.transfer.lock().await.is_some() {
            return Err("a transfer is already running".to_string());
        }
        let dest: Option<Arc<RwLock<dyn StorageDestMultipart>>> = match &req.dest {
            TransferDest::NewShare => None,
            TransferDest::Storage { storage_id, path } => {
                let dest_fs = self.local_fs(storage_id).await.map_err(|e| {
                    if e.contains("read-only") {
                        "copying into a read-only storage is not supported".to_string()
                    } else {
                        e
                    }
                })?;
                let dest_root: PathBuf = {
                    let root = dest_fs.read().await.root().to_path_buf();
                    let rel = path.trim_matches('/');
                    if rel.is_empty() { root } else { root.join(rel) }
                };
                Some(Arc::new(RwLock::new(LocalFs::new(dest_root))))
            }
        };

        let conflict_slot: Arc<Mutex<Option<oneshot::Sender<ConflictDecision>>>> = Arc::new(Mutex::new(None));
        let error_slot: Arc<Mutex<Option<oneshot::Sender<ErrorDecision>>>> = Arc::new(Mutex::new(None));
        let policies = if req.interactive {
            TransferPolicies {
                on_existing: OnExisting::Ask,
                conflict_resolver: Some(Arc::new(DialogConflictResolver {
                    tx: self.tx.clone(),
                    slot: conflict_slot.clone(),
                })),
                on_error: OnError::Ask,
                error_resolver: Some(Arc::new(DialogErrorResolver {
                    tx: self.tx.clone(),
                    slot: error_slot.clone(),
                })),
            }
        } else {
            TransferPolicies {
                on_existing: OnExisting::Overwrite,
                ..Default::default()
            }
        };

        let selection = Some(req.source_paths.clone());
        let source_root = req.source_root.trim_matches('/').to_string();
        enum Src {
            /// A local fs or an archive: a plain source.
            Bare(Arc<RwLock<dyn StorageSource>>),
            Ninja(Arc<TransferDownload>),
        }
        let src = {
            let storages = self.storages.read().await;
            let entry = storages
                .iter()
                .find(|s| s.id == req.source_storage_id)
                .ok_or_else(|| format!("no storage {}", req.source_storage_id))?;
            match &entry.backend {
                Backend::Local(fs) => Src::Bare(fs.clone()),
                Backend::Archive(source) => Src::Bare(source.clone()),
                Backend::Ninja { transfer, .. } => Src::Ninja(transfer.clone()),
            }
        };

        // A local or archive source runs a bare item; a share runs on its
        // warm download transfer (`do_download` brackets the run with the
        // backend's per-file session bookends); a copy onto the links list
        // uploads the selection into a new share.
        let tx = self.tx.clone();
        let (item, run): (
            Arc<TransferItem>,
            tokio::task::JoinHandle<(TransferItemState, Option<StorageError>)>,
        ) = match (src, dest) {
            (src, None) => {
                // A cloud-only link must declare its size before its
                // upload can start (`409 estimate_required`): size the
                // selection first, the same walk F3 runs on a folder.
                let mut estimated_bytes: u64 = 0;
                for key in &req.source_paths {
                    let mut rx = self.dir_size(&req.source_storage_id, key).await?;
                    let mut last = None;
                    while let Some(block) = rx.recv().await {
                        last = Some(block.map_err(|e| format!("size {key}: {e}"))?);
                    }
                    let stats = last.ok_or_else(|| format!("size {key}: no result"))?;
                    estimated_bytes += stats.total_size;
                }
                let source: Arc<RwLock<dyn StorageSource>> = match src {
                    Src::Bare(source) => source,
                    Src::Ninja(transfer) => transfer.source().expect("source set after connect_source"),
                };
                self.start_upload_to_new_share(source, selection, &source_root, estimated_bytes, policies)
                    .await?
            }
            (Src::Bare(source), Some(dest)) => {
                let item = Arc::new(
                    TransferItem::new(source, dest, selection, N_STREAMS)
                        .await
                        .with_policies(policies)
                        .with_source_root(source_root),
                );
                let it = item.clone();
                let run = tokio::spawn(async move {
                    let err = it.start().await.err();
                    (it.current_state().await, err)
                });
                (item, run)
            }
            (Src::Ninja(transfer), Some(dest)) => {
                let item = transfer
                    .start_download_with_root(selection, dest, N_STREAMS, policies, &source_root)
                    .await;
                let it = item.clone();
                let run = tokio::spawn(async move { transfer.do_download(it, true).await });
                (item, run)
            }
        };

        *self.transfer.lock().await = Some(ActiveTransfer {
            item: item.clone(),
            conflict_slot,
            error_slot,
            run_abort: run.abort_handle(),
        });

        // Progress ticks while the item runs; the terminal state once the
        // run returns. Parked states are announced by the resolvers.
        let ticker = {
            let item = item.clone();
            let tx = tx.clone();
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(std::time::Duration::from_millis(250));
                loop {
                    tick.tick().await;
                    if !matches!(
                        item.current_state().await,
                        TransferItemState::Idle | TransferItemState::Preparing | TransferItemState::Running
                    ) {
                        continue;
                    }
                    let _ = tx.send(UiEvent::TransferProgress(item.progress().await));
                }
            })
        };
        let engine = self.clone();
        let started = std::time::Instant::now();
        tokio::spawn(async move {
            let (state, err) = match run.await {
                Ok(r) => r,
                // Aborted by `cancel_transfer` after the grace period.
                Err(e) if e.is_cancelled() => (TransferItemState::Cancelled, None),
                Err(e) => (
                    TransferItemState::Error,
                    Some(StorageError::Other(format!("transfer task: {e}"))),
                ),
            };
            ticker.abort();
            let error = match state {
                TransferItemState::Finished | TransferItemState::Cancelled => None,
                _ => Some(
                    err.map(|e| e.to_string())
                        .unwrap_or_else(|| "Transfer failed".to_string()),
                ),
            };
            *engine.transfer.lock().await = None;
            let progress = item.progress().await;
            let secs = started.elapsed().as_secs_f64();
            let mb = progress.bytes as f64 / 1e6;
            tracing::info!(
                ?state,
                "transfer finished: {:.1} MB in {:.1}s ({:.1} MB/s)",
                mb,
                secs,
                if secs > 0.0 { mb / secs } else { 0.0 }
            );
            engine.push(UiEvent::TransferFinished { state, progress, error });
        });
        Ok(())
    }

    /// Create a cloud share for the selection and upload it — what the CLI's
    /// `put` does: a fresh share key, the description (basenames of
    /// the selection) E2E-encrypted under it, the owner's own key-store row,
    /// the declared size (the backend mints the cloud datasource against
    /// it), then `TransferUpload` over the link's S3 datasource.
    async fn start_upload_to_new_share(
        &self,
        source: Arc<RwLock<dyn StorageSource>>,
        selection: Option<Vec<String>>,
        source_root: &str,
        estimated_bytes: u64,
        policies: TransferPolicies,
    ) -> Result<
        (
            Arc<TransferItem>,
            tokio::task::JoinHandle<(TransferItemState, Option<StorageError>)>,
        ),
        String,
    > {
        let key: [u8; 32] = {
            use rand::Rng;
            let mut k = [0u8; 32];
            rand::rng().fill_bytes(&mut k);
            k
        };
        let names: Vec<String> = selection.iter().flatten().map(|p| basename(p)).collect();
        let description = names.join(", ");
        let encrypted_metadata = encrypt_metadata(
            &LinkMetadata {
                description: Some(description),
            },
            &key,
        )
        .map_err(|e| format!("encrypt link metadata: {e}"))?;
        let encrypted_share_key = self.session.read().await.wrap_own_share_key(&key).await;
        let link_info = LinkInfo {
            description: None,
            validity: Some(NEW_SHARE_VALIDITY_SECS),
            workspace_id: None,
            cloud_storage: Some(true),
            p2p_storage: Some(false),
            estimated_bytes: Some(i64::try_from(estimated_bytes).map_err(|_| "selection too large".to_string())?),
            encrypted_metadata: Some(encrypted_metadata),
            encrypted_share_key,
            notify_on_open: None,
            notify_on_download: None,
        };
        let token = self
            .back
            .read()
            .await
            .link(&link_info)
            .await
            .map_err(|e| format!("create link: {e}"))?
            .url_token;
        tracing::info!(token = %token, "created share for the copy");

        let mut transfer = TransferUpload::new(self.back.clone());
        transfer
            .connect_dest(&token, Some(key))
            .await
            .map_err(|e| format!("connect share storage: {e}"))?;
        let item = transfer
            .start_upload_with_root(source, selection, N_STREAMS, policies, source_root)
            .await;
        let it = item.clone();
        let run = tokio::spawn(async move {
            let state = transfer.do_upload(it, true).await;
            let err = transfer.error.take();
            let msg = err.as_ref().map(|e| e.to_string());
            transfer.finish_transfer(msg.as_deref()).await;
            (state, err)
        });
        Ok((item, run))
    }

    pub async fn cancel_transfer(&self) -> Result<(), String> {
        let guard = self.transfer.lock().await;
        let Some(active) = guard.as_ref() else {
            return Err("no transfer is running".to_string());
        };
        // Drop any parked prompt: its resolver reads the closed channel as
        // Cancel.
        active.conflict_slot.lock().await.take();
        active.error_slot.lock().await.take();
        active.item.cancel(false).await;
        // The flag is only observed between awaits: a source call that never
        // returns (dropped peer) would leave the run — and the dialog — up
        // forever. Abort the task once the grace period passes; a no-op if it
        // has ended by then.
        let abort = active.run_abort.clone();
        tokio::spawn(async move {
            tokio::time::sleep(CANCEL_GRACE).await;
            abort.abort();
        });
        Ok(())
    }

    pub async fn resolve_conflicts(&self, decision: ConflictDecision) {
        let guard = self.transfer.lock().await;
        if let Some(active) = guard.as_ref()
            && let Some(tx) = active.conflict_slot.lock().await.take()
        {
            let _ = tx.send(decision);
        }
    }

    pub async fn resolve_error(&self, decision: ErrorDecision) {
        let guard = self.transfer.lock().await;
        if let Some(active) = guard.as_ref()
            && let Some(tx) = active.error_slot.lock().await.take()
        {
            let _ = tx.send(decision);
        }
    }

    // ── vault (trusted side) ───────────────────────────────────────────────

    /// The trust circle: the unified devices overview, trusted cards only.
    pub async fn vault_devices(&self) -> Result<Vec<models::DeviceCard>, String> {
        use models::device_card_trust::Status;
        let overview = self
            .back
            .read()
            .await
            .devices_overview()
            .await
            .map_err(|e| e.to_string())?;
        Ok(overview
            .items
            .into_iter()
            .filter(|card| card.trust.status == Status::Trusted)
            .collect())
    }

    async fn identity(&self) -> Result<VaultIdentity, String> {
        self.session
            .read()
            .await
            .identity
            .clone()
            .ok_or_else(|| "vault is locked on this device".to_string())
    }

    pub async fn vault_approve_join(&self, agent_id: &str) -> Result<(), String> {
        let identity = self.identity().await?;
        let pubkey = self
            .vault
            .read()
            .await
            .pending_join_requests
            .iter()
            .find(|r| r.agent_id == agent_id)
            .map(|r| r.device_public_key.clone())
            .ok_or_else(|| format!("join request not found for agent {agent_id}"))?;
        let res = vault::approve_join(&self.back, agent_id, &pubkey, &identity).await;
        self.finish_join_decision(agent_id, res).await
    }

    pub async fn vault_reject_join(&self, agent_id: &str) -> Result<(), String> {
        let res = vault::reject_join(&self.back, agent_id).await;
        self.finish_join_decision(agent_id, res).await
    }

    /// A decision landing on an already-decided request is settled, not
    /// failed; either way the request leaves our pending set.
    async fn finish_join_decision(&self, agent_id: &str, res: vault::Result<()>) -> Result<(), String> {
        let outcome = match res {
            Ok(()) => Ok(()),
            Err(vault::VaultError::Back { source, .. }) if vault::join_already_decided(&source) => Ok(()),
            Err(e) => Err(e.to_string()),
        };
        if outcome.is_ok() {
            self.resolve_pending_join(agent_id).await;
        }
        outcome
    }

    pub async fn vault_revoke_device(&self, agent_id: &str) -> Result<(), String> {
        vault::revoke_device(&self.back, agent_id)
            .await
            .map_err(|e| e.to_string())
    }

    /// New recovery words for the unlocked identity (the old phrase is void).
    pub async fn vault_regenerate_recovery(&self) -> Result<Vec<String>, String> {
        let identity = self.identity().await?;
        let words = vault::regenerate_recovery(&self.back, &identity)
            .await
            .map_err(|e| e.to_string())?;
        self.vault.write().await.setup_recovery_words = Some(words.clone());
        self.push_snapshot().await;
        Ok(words)
    }

    pub async fn vault_ack_recovery_words(&self) {
        self.vault.write().await.setup_recovery_words = None;
        self.push_snapshot().await;
    }

    pub async fn vault_ack_revoked(&self) {
        self.vault.write().await.revoked_pending = false;
        self.push_snapshot().await;
    }

    /// Ask to join the trust circle; the fingerprint and the approving
    /// devices show until a trusted device approves.
    pub async fn vault_join_start(&self) -> Result<(), String> {
        let resp = vault::join_start(&self.back).await.map_err(|e| e.to_string())?;
        self.session.write().await.vault_status = Some("pending_approval".to_string());
        self.vault.write().await.pending_join = Some(pending_join(resp));
        self.push_snapshot().await;
        Ok(())
    }

    // ── vault recovery (step-up gated) ─────────────────────────────────────

    /// Recover the vault from the 24 words. `recovery/complete` is step-up
    /// gated: the first call normally parks behind an emailed code
    /// (`ChallengeRequired`) — `vault_step_up_verify` finishes it.
    pub async fn vault_recover(&self, mnemonic: &str) -> Result<StepUpOutcome, String> {
        let (device, identity_pub) = {
            let s = self.session.read().await;
            (s.device, s.identity_public_key_b64().await)
        };
        let (req, identity) = vault::recover_prepare(&self.back, mnemonic, device, identity_pub)
            .await
            .map_err(|e| e.to_string())?;
        self.submit_recover(req, identity, None).await
    }

    async fn submit_recover(
        &self,
        req: models::VaultRecoveryCompleteRequest,
        identity: VaultIdentity,
        challenge: Option<&str>,
    ) -> Result<StepUpOutcome, String> {
        let res = self
            .back
            .read()
            .await
            .vault_recovery_complete(req.clone(), challenge)
            .await;
        match res {
            Ok(_) => {
                {
                    let mut s = self.session.write().await;
                    s.identity = Some(identity);
                    s.vault_status = Some("active".to_string());
                }
                {
                    let mut v = self.vault.write().await;
                    v.pending_join = None;
                    v.revoked_pending = false;
                }
                self.push_snapshot().await;
                Ok(StepUpOutcome::Completed)
            }
            Err(BackError::ChallengeRequired { token, expires_at, factors }) => {
                // The refusal mails the code only for a sole email factor; with
                // a choice on offer ask for it, since the code prompt is email-only.
                if factors.len() > 1
                    && let Err(e) = self.back.read().await.send_step_up_code(&token).await
                {
                    tracing::warn!("step-up: sending the email code failed: {e}");
                }
                *self.pending_step_up.lock().await = Some(PendingRecover { token, req, identity });
                Ok(StepUpOutcome::ChallengeRequired { expires_at })
            }
            Err(e) => Err(e.to_string()),
        }
    }

    /// Verify the emailed code and retry the parked recover with the
    /// confirmed token. A wrong code keeps it parked for another try.
    pub async fn vault_step_up_verify(&self, code: &str) -> Result<StepUpOutcome, String> {
        let token = self
            .pending_step_up
            .lock()
            .await
            .as_ref()
            .map(|p| p.token.clone())
            .ok_or_else(|| "no step-up is pending".to_string())?;
        let confirmed = self
            .back
            .read()
            .await
            .verify_step_up(&token, code.trim())
            .await
            .map_err(|e| e.to_string())?;
        let Some(pending) = self.pending_step_up.lock().await.take() else {
            return Err("no step-up is pending".to_string());
        };
        self.submit_recover(pending.req, pending.identity, Some(&confirmed))
            .await
    }

    pub async fn vault_step_up_cancel(&self) {
        *self.pending_step_up.lock().await = None;
    }
}

// ── helpers ────────────────────────────────────────────────────────────────

pub(crate) fn basename(key: &str) -> String {
    key.rsplit('/').next().unwrap_or(key).to_string()
}

fn drive_space(root: &std::path::Path) -> Option<Space> {
    volume_free_capacity(root)
        .ok()
        .map(|(free, total)| Space { free, total })
}

/// The local volumes: the drive letters on Windows; `/` as "~" plus the
/// mounts under `/Volumes` on macOS (the root's own alias skipped); `/`
/// alone elsewhere.
#[cfg(windows)]
fn enumerate_drives() -> Vec<Drive> {
    use windows_sys::Win32::Storage::FileSystem::{GetDriveTypeW, GetLogicalDrives};
    use windows_sys::Win32::System::WindowsProgramming::{
        DRIVE_CDROM, DRIVE_FIXED, DRIVE_RAMDISK, DRIVE_REMOTE, DRIVE_REMOVABLE,
    };
    let mask = unsafe { GetLogicalDrives() };
    (0..26u32)
        .filter(|bit| mask & (1 << bit) != 0)
        .map(|bit| {
            let letter = (b'A' + bit as u8) as char;
            let root = format!("{}:\\", letter);
            let wide: Vec<u16> = root.encode_utf16().chain(std::iter::once(0)).collect();
            let kind = match unsafe { GetDriveTypeW(wide.as_ptr()) } {
                DRIVE_FIXED => "fixed",
                DRIVE_REMOVABLE => "removable",
                DRIVE_REMOTE => "network",
                DRIVE_CDROM => "cdrom",
                DRIVE_RAMDISK => "ramdisk",
                _ => "unknown",
            };
            let space = drive_space(std::path::Path::new(&root));
            Drive {
                label: format!("{}: {}", letter, kind),
                root,
                space,
            }
        })
        .collect()
}

#[cfg(not(windows))]
fn enumerate_drives() -> Vec<Drive> {
    let mut drives = vec![Drive {
        root: "/".to_string(),
        label: "~".to_string(),
        space: drive_space(std::path::Path::new("/")),
    }];
    if cfg!(target_os = "macos")
        && let Ok(entries) = std::fs::read_dir("/Volumes")
    {
        let root_dev = std::fs::metadata("/").ok().map(|m| {
            use std::os::unix::fs::MetadataExt;
            m.dev()
        });
        let mut mounts: Vec<Drive> = entries
            .flatten()
            .filter_map(|e| {
                let path = e.path();
                // The boot volume appears here too (a symlink to `/`): skip
                // anything on the root's own device.
                let meta = std::fs::metadata(&path).ok()?;
                if !meta.is_dir() {
                    return None;
                }
                use std::os::unix::fs::MetadataExt;
                if Some(meta.dev()) == root_dev {
                    return None;
                }
                let label = path.file_name()?.to_string_lossy().to_string();
                Some(Drive {
                    space: drive_space(&path),
                    root: path.to_string_lossy().to_string(),
                    label,
                })
            })
            .collect();
        mounts.sort_by_key(|d| d.label.to_lowercase());
        drives.extend(mounts);
    }
    drives
}

/// Drain a streamed ranged `get` into one vector.
pub(crate) async fn drain_range(
    source: &Arc<RwLock<dyn StorageSource>>,
    path: &str,
    offset: u64,
    len: u64,
) -> Result<Vec<u8>, String> {
    let mut rx = source.read().await.get_streamed(path, Some(offset), Some(len)).await;
    let mut out = Vec::with_capacity(len as usize);
    while let Some(chunk) = rx.recv().await {
        out.extend_from_slice(&chunk.map_err(|e| e.to_string())?);
    }
    Ok(out)
}

/// Drain a streamed `get_dir` into one vector.
/// The whole tree under `path`, for the local delete's deepest-first order.
async fn drain_dir(
    source: &Arc<RwLock<dyn StorageSource>>,
    path: &str,
    recursive: bool,
) -> Result<Vec<StorageObjectInfo>, String> {
    let mut rx = source.write().await.get_dir(path, recursive, None, Some(500)).await;
    let mut items = Vec::new();
    while let Some(batch) = rx.recv().await {
        items.extend(batch.map_err(|e| e.to_string())?);
    }
    Ok(items)
}

/// Parks the copy on the conflict dialog: sends the batch, waits for the
/// answer (a dropped channel — cancel — reads as `Cancel`).
struct DialogConflictResolver {
    tx: EventSender,
    slot: Arc<Mutex<Option<oneshot::Sender<ConflictDecision>>>>,
}

#[async_trait]
impl ConflictResolver for DialogConflictResolver {
    async fn resolve(&self, conflicts: Vec<Conflict>) -> ConflictDecision {
        let (tx, rx) = oneshot::channel();
        *self.slot.lock().await = Some(tx);
        let _ = self.tx.send(UiEvent::TransferConflicts(conflicts));
        rx.await.unwrap_or(ConflictDecision::Cancel)
    }
}

/// Parks the failing unit on the error dialog.
struct DialogErrorResolver {
    tx: EventSender,
    slot: Arc<Mutex<Option<oneshot::Sender<ErrorDecision>>>>,
}

#[async_trait]
impl ErrorResolver for DialogErrorResolver {
    async fn resolve(&self, key: &str, error: &StorageError) -> ErrorDecision {
        let (tx, rx) = oneshot::channel();
        *self.slot.lock().await = Some(tx);
        let _ = self.tx.send(UiEvent::TransferError {
            key: key.to_string(),
            message: error.to_string(),
        });
        rx.await.unwrap_or(ErrorDecision::Cancel)
    }
}
