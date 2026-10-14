use std::sync::{Arc, Mutex};

use tokio::sync::Mutex as AsyncMutex;

use share_ninja_api::apis::auth_api;
use share_ninja_api::apis::configuration::Configuration;
use share_ninja_api::models;

use super::back::{BackError, BackParams, BackState, StateCallbacks, apply_state_change, fmt_api_error};
use super::debug_middleware::{DebugLoggingMiddleware, PoolRouter, RequestDeadline, RequestIdMiddleware};
use crate::secrets::SecureStore;

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// An empty string is not a usable refresh token — the server answers it with
/// 401 `invalid_token` — but it still reads as "present" to every `is_some()`
/// check. Normalise on both sides of the store so a refresh token is `Some`
/// only when it holds something worth sending.
fn non_empty(token: Option<String>) -> Option<String> {
    token.filter(|t| !t.is_empty())
}

/// The access token and its expiry — the only token state held in memory.
///
/// The refresh token has no copy here. Its one home is the [`SecureStore`]:
/// read at the moment it is needed (a grant, the logout body) and written only
/// when the server just issued one. The store may be shared with sibling
/// processes (the `sninja` CLI and the commander share one keychain item), and
/// the server rotates the token on every grant, so any copy held across
/// requests would go stale and, once sent, be a replay of a rotated-away token.
#[derive(Default)]
struct TokenState {
    access_token: Option<String>,
    access_token_expire_ts: Option<i64>,
}

pub struct AuthManager {
    state: Mutex<TokenState>,
    secure_store: Arc<SecureStore>,
    refresh_gate: AsyncMutex<()>,
    /// The refresh grant's client: `Back`'s pool under its own middleware
    /// stack (no `AuthMiddleware` — the grant must not refresh itself),
    /// reached through the `PoolRouter`, so a renewed pool is picked up
    /// without touching this.
    refresh_config: Configuration,
    response_mode: String,
    shared_state: Arc<Mutex<BackState>>,
    state_callbacks: StateCallbacks,
}

/// The refresh client over `Back`'s pool: request ids, the per-call
/// deadline (a refresh that never gets answered holds `refresh_gate`, and
/// every API call — the register handshake included — queues behind it),
/// the optional wire log, and last the `PoolRouter`.
fn refresh_stack(pool: Arc<std::sync::RwLock<reqwest::Client>>, log_api: bool) -> reqwest_middleware::ClientWithMiddleware {
    let seed = pool.read().unwrap_or_else(|e| e.into_inner()).clone();
    let mut builder = reqwest_middleware::ClientBuilder::new(seed)
        .with(RequestIdMiddleware)
        .with(RequestDeadline);
    if log_api {
        builder = builder.with(DebugLoggingMiddleware);
    }
    builder.with(PoolRouter { pool }).build()
}

impl AuthManager {
    pub fn new(
        params: &BackParams,
        pool: Arc<std::sync::RwLock<reqwest::Client>>,
        secure_store: Arc<SecureStore>,
        shared_state: Arc<Mutex<BackState>>,
        state_callbacks: StateCallbacks,
    ) -> Arc<Self> {
        let refresh_config = Configuration {
            base_path: params.base_url.clone(),
            user_agent: Some(params.user_agent.clone()),
            client: refresh_stack(pool, params.log_api),
            ..Configuration::default()
        };

        Arc::new(Self {
            state: Mutex::new(TokenState::default()),
            secure_store,
            refresh_gate: AsyncMutex::new(()),
            refresh_config,
            response_mode: params.response_mode.clone(),
            shared_state,
            state_callbacks,
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, TokenState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn access_token(&self) -> Option<String> {
        self.lock().access_token.clone()
    }

    /// The refresh token the secure store holds right now.
    pub fn refresh_token(&self) -> Option<String> {
        non_empty(self.secure_store.refresh_token())
    }

    /// Persist a refresh token the server just issued — or `None` when the
    /// session is over (logout, a refused grant, a revoked session). Nothing
    /// else ever writes the store: a token this process did not just receive
    /// from the server may already have been rotated by a sibling process.
    fn store_refresh_token(&self, token: Option<String>) {
        self.secure_store.set_refresh_token(non_empty(token));
    }

    /// Replace the access token (register, OAuth exchange, refresh grant).
    pub fn set_access(&self, access: Option<String>, expire_ts: Option<i64>) {
        let mut s = self.lock();
        s.access_token = access;
        s.access_token_expire_ts = expire_ts;
    }

    /// Replace the access token alone, keeping the current expiry (a
    /// challenge verify answers with a token but no lifetime).
    pub fn set_access_token(&self, access: Option<String>) {
        self.lock().access_token = access;
    }

    /// The server issued a refresh token with the access token (register of a
    /// new session, OAuth exchange, refresh grant): take both.
    pub fn apply_issued(&self, access: Option<String>, expire_ts: Option<i64>, refresh: Option<String>) {
        self.set_access(access, expire_ts);
        if let Some(refresh) = non_empty(refresh) {
            self.store_refresh_token(Some(refresh));
        }
    }

    /// End the session: access token gone, stored refresh token cleared.
    pub fn clear(&self) {
        self.set_access(None, None);
        self.store_refresh_token(None);
    }

    pub async fn ensure_fresh(&self) -> Result<(), BackError> {
        let need = {
            let s = self.lock();
            s.access_token.is_none() || s.access_token_expire_ts.is_some_and(|ts| ts < now_ms())
        };
        if need && self.refresh_token().is_some() {
            self.refresh().await
        } else {
            Ok(())
        }
    }

    pub async fn refresh(&self) -> Result<(), BackError> {
        let _gate = self.refresh_gate.lock().await;

        {
            let s = self.lock();
            if s.access_token.is_some() && s.access_token_expire_ts.is_some_and(|ts| ts >= now_ms()) {
                return Ok(());
            }
        }

        self.do_refresh().await
    }

    /// 401-driven refresh: the server just rejected `failed_token`, so the
    /// wall-clock freshness check is moot — the token is dead however fresh its
    /// expiry timestamp looks. Refresh unless another caller already replaced
    /// the token while we waited on the gate (then the retry can just use it).
    pub async fn refresh_after_401(&self, failed_token: Option<&str>) -> Result<(), BackError> {
        let _gate = self.refresh_gate.lock().await;

        {
            let s = self.lock();
            if s.access_token.as_deref() != failed_token {
                return Ok(());
            }
        }

        self.do_refresh().await
    }

    /// The actual token-grant round trip. Callers hold `refresh_gate` and have
    /// decided a refresh is genuinely needed. The grant is sent with the token
    /// the store holds at this moment — whichever process rotated it last. A
    /// token the server issues back is stored; a grant answered without one
    /// (a sibling rotated this token first and holds the new one) changes
    /// nothing in the store. A grant the server refuses (401/403) ends the
    /// session — access token gone, stored refresh token cleared — and drops
    /// the state to `NoConnection`; transient errors keep everything so an
    /// outage doesn't log the user out.
    async fn do_refresh(&self) -> Result<(), BackError> {
        let refresh_token = self
            .refresh_token()
            .ok_or_else(|| BackError::Other("No refresh token available".to_string()))?;

        tracing::debug!("Refreshing access token");
        let request = models::TokenRequest::RefreshToken(Box::new(models::RefreshTokenGrant::new(
            models::refresh_token_grant::GrantType::RefreshToken,
            refresh_token,
        )));

        let result = auth_api::auth_token(&self.refresh_config, request, Some(&self.response_mode)).await;

        match result {
            Ok(response) => {
                tracing::debug!("Token refresh successful");
                self.apply_issued(
                    Some(response.access_token),
                    Some(response.expires_in as i64 * 1000 + now_ms()),
                    response.refresh_token,
                );
                Ok(())
            }
            Err(e) => {
                tracing::debug!("Token refresh failed: {}", e);
                let is_auth_error = matches!(
                    &e,
                    share_ninja_api::apis::Error::ResponseError(resp)
                        if resp.status.as_u16() == 401 || resp.status.as_u16() == 403
                );
                if is_auth_error {
                    self.clear();
                    self.fail_offline();
                }
                Err(BackError::Api {
                    code: "token_refresh_failed".to_string(),
                    message: fmt_api_error(&e),
                })
            }
        }
    }

    /// Drop the connection state to `NoConnection`, which fires the state
    /// callback that spawns the reconnect loop — i.e. schedules a full
    /// re-register. Also the middleware's escape hatch for a 401 it cannot
    /// recover in-band (no refresh token to retry with).
    pub(super) fn fail_offline(&self) {
        apply_state_change(&self.shared_state, &self.state_callbacks, BackState::NoConnection);
    }

    /// The server declared the session dead (a 401 with `session_revoked` /
    /// `refresh_token_reused`): a refresh grant can only be refused, so skip
    /// the doomed round trip and perform the same teardown a rejected grant
    /// does — end the session and drop to `NoConnection` for a full
    /// re-register.
    pub(super) fn drop_revoked_session(&self) {
        self.clear();
        self.fail_offline();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::secrets::MemorySecretStore;
    use tokio::sync::RwLock;

    fn params() -> BackParams {
        BackParams {
            base_url: "https://example.invalid/api".to_string(),
            ws_url: "wss://example.invalid/api".to_string(),
            oauth_url: None,
            user_agent: "test".to_string(),
            device_name: None,
            product_id: "test".to_string(),
            product_version: "0".to_string(),
            os: "test".to_string(),
            os_version: "0".to_string(),
            lang: "en".to_string(),
            response_mode: "token".to_string(),
            device_public_key: Vec::new(),
            device_serial: None,
            device_model: None,
            device_form_factor: None,
            log_api: false,
            nearby_visibility: None,
            instance_id: None,
            connect_override: None,
        }
    }

    fn manager(store: Arc<SecureStore>) -> Arc<AuthManager> {
        // The reqwest client is built eagerly; with `rustls-no-provider`
        // that needs the process-default provider the binaries install
        // (`sninja` main.rs does the same).
        rustls::crypto::ring::default_provider().install_default().ok();
        AuthManager::new(
            &params(),
            Arc::new(std::sync::RwLock::new(reqwest::Client::new())),
            store,
            Arc::new(Mutex::new(BackState::NoConnection)),
            Arc::new(RwLock::new(Vec::new())),
        )
    }

    fn store_with(token: Option<&str>) -> Arc<SecureStore> {
        let store = Arc::new(SecureStore::new(Arc::new(MemorySecretStore::default())));
        store.set_refresh_token(token.map(str::to_string));
        store
    }

    /// An empty stored token must read as absent, or `refresh()` sends
    /// `{"refresh_token":""}` and the server 401s on every cold start.
    #[test]
    fn empty_stored_refresh_token_reads_as_absent() {
        assert_eq!(manager(store_with(Some(""))).refresh_token(), None);
        assert_eq!(
            manager(store_with(Some("real"))).refresh_token(),
            Some("real".to_string())
        );
        assert_eq!(manager(store_with(None)).refresh_token(), None);
    }

    #[test]
    fn issued_empty_refresh_token_is_not_stored() {
        let auth = manager(store_with(Some("real")));

        auth.apply_issued(None, None, Some(String::new()));
        assert_eq!(auth.refresh_token(), Some("real".to_string()));

        auth.apply_issued(None, None, Some("rotated".to_string()));
        assert_eq!(auth.refresh_token(), Some("rotated".to_string()));

        auth.clear();
        assert_eq!(auth.refresh_token(), None);
    }

    /// The store is the token's only home: a rotation persisted by one
    /// `AuthManager` (a sibling process) is what the other one sends next.
    #[test]
    fn sibling_rotation_is_visible_without_a_copy() {
        let backend = Arc::new(MemorySecretStore::default());
        let a = manager(Arc::new(SecureStore::new(backend.clone())));
        let b = manager(Arc::new(SecureStore::new(backend)));

        a.apply_issued(None, None, Some("rt1".to_string()));
        assert_eq!(b.refresh_token(), Some("rt1".to_string()));

        b.apply_issued(None, None, Some("rt2".to_string()));
        assert_eq!(a.refresh_token(), Some("rt2".to_string()));
    }

    /// `ensure_fresh` gates on "does the store hold a refresh token" — an
    /// empty one must not trigger a doomed refresh round trip.
    #[tokio::test]
    async fn ensure_fresh_is_a_noop_without_a_usable_token() {
        assert!(manager(store_with(Some(""))).ensure_fresh().await.is_ok());
    }
}
