//! CallServer - handles incoming P2P calls and serves files

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

use crate::back::{Back, CallInInfo};
use crate::transfers::StorageSource;

use super::data_channel::DataChannelWrapper;
use super::file_server::FileServer;
use super::relay_transport::RelayConfig;
use super::webrtc_connector::{IceCandidate, WebRTCConnector};

/// Share configuration for CallServer
#[derive(Debug, Clone)]
pub struct CallServerShare {
    pub link_token: String,
    pub paths: Vec<String>,
}

/// Active P2P serves, keyed by link `url_token` -> (`call_id` -> connector). One
/// link can be pulled by several callers at once, hence the inner map. Mirrors the
/// TS `CallServer.activeServes` (`Map<url_token, Set<WebRTCConnector>>`); we key the
/// inner map by `call_id` so close-driven untracking removes the right entry without
/// connector-identity comparison.
type ActiveServes = Arc<Mutex<HashMap<String, HashMap<String, Arc<WebRTCConnector>>>>>;

/// CallServer handles incoming P2P calls and serves files to peers
pub struct CallServer {
    back: Option<Arc<RwLock<Back>>>,
    active_serves: ActiveServes,
}

impl CallServer {
    pub fn new() -> Self {
        Self {
            back: None,
            active_serves: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Register an in-flight serve so the sender can abort it midflight via
    /// [`cancel_serve`](Self::cancel_serve). Mirrors TS `trackServe`.
    async fn track_serve(&self, url_token: &str, call_id: &str, wrtc: Arc<WebRTCConnector>) {
        self.active_serves
            .lock()
            .await
            .entry(url_token.to_string())
            .or_default()
            .insert(call_id.to_string(), wrtc);
    }

    /// Is a call with this `call_id` already being served? Used to dedup duplicate
    /// `call.incoming` events before we do any setup work.
    pub async fn is_call_active(&self, call_id: &str) -> bool {
        self.active_serves
            .lock()
            .await
            .values()
            .any(|m| m.contains_key(call_id))
    }

    /// Abort all in-flight P2P serves for a link (sender-initiated midflight cancel).
    /// Mirrors TS `cancelServe`: drop the map entry first, then close each connector —
    /// `close()` drives the `on_close` callback which re-enters the untrack path and
    /// finds nothing, so we never mutate the map we're draining.
    pub async fn cancel_serve(&self, url_token: &str) {
        let entries = self.active_serves.lock().await.remove(url_token);
        let Some(entries) = entries else { return };
        tracing::debug!(token = %url_token, count = entries.len(), "cancel_serve: closing connection(s)");
        for (_call_id, wrtc) in entries {
            wrtc.close().await;
        }
    }

    /// Set the backend for WebSocket communication
    pub fn set_back(&mut self, back: Arc<RwLock<Back>>) {
        self.back = Some(back);
    }

    /// Set shares that this server will serve
    pub fn set_shares(&mut self, _shares: Vec<CallServerShare>) {
        // Store shares for access control
    }

    /// Create a WebRTC connector for an incoming call (returns immediately)
    /// Call setup_call() after registering the peermsg handler
    pub fn create_call(&self, ci: &CallInInfo) -> Arc<Mutex<WebRTCConnector>> {
        tracing::debug!("Creating WebRTC connector for call {}", ci.call_id);
        Arc::new(Mutex::new(WebRTCConnector::new()))
    }

    /// Set up the call after the wrtc is created and peermsg handler is registered
    pub async fn setup_call(
        &self,
        wrtc: Arc<Mutex<WebRTCConnector>>,
        source: Arc<tokio::sync::RwLock<dyn StorageSource + Send + Sync>>,
        ci: CallInInfo,
        encryption_key: Option<[u8; 32]>,
    ) -> Result<(), String> {
        tracing::debug!(
            "Setting up call {} (encrypted: {})",
            ci.call_id,
            encryption_key.is_some()
        );

        let call_id = ci.call_id.clone();
        let mut wrtc_guard = wrtc.lock().await;

        // Derive session key from master key and call_id if encryption is enabled
        if let Some(master_key) = encryption_key {
            let session_key = super::data_channel::derive_session_key(&master_key, &call_id)
                .map_err(|e| format!("Failed to derive session key: {}", e))?;
            tracing::debug!("Derived session key for call {}", call_id);
            wrtc_guard.set_encryption_key(session_key).await;
        }

        // Create FileServer for per-request channel handling
        let file_server = Arc::new(FileServer::new(source.clone()));

        // Set up WebRTC callbacks
        let back_clone = self.back.clone();
        let call_id_clone = call_id.clone();
        wrtc_guard.on_answer_create = Some(Arc::new(move |sdp: String| {
            tracing::debug!("Got SDP answer, sending over WS");
            if let Some(ref back) = back_clone {
                let back = back.clone();
                let call_id = call_id_clone.clone();
                tokio::spawn(async move {
                    let msg = serde_json::json!({
                        "call_id": call_id,
                        "message": {
                            "type": "answer",
                            "sdp": sdp,
                            "from": "server"
                        }
                    });
                    if let Err(e) = back.read().await.send_ws(msg).await {
                        tracing::error!("Failed to send answer over WS: {}", e);
                    }
                });
            }
        }));

        let back_clone = self.back.clone();
        let call_id_clone = call_id.clone();
        wrtc_guard.on_candidate_change = Some(Arc::new(move |candidate: IceCandidate| {
            tracing::debug!("Got local candidate change, sending over WS");
            if let Some(ref back) = back_clone {
                let back = back.clone();
                let call_id = call_id_clone.clone();
                tokio::spawn(async move {
                    let msg = serde_json::json!({
                        "call_id": call_id,
                        "message": {
                            "type": "candidate",
                            "sdp": candidate.candidate,
                            "mid": candidate.sdp_mid,
                            "mline": candidate.sdp_m_line_index,
                            "from": "server"
                        }
                    });
                    if let Err(e) = back.read().await.send_ws(msg).await {
                        tracing::error!("Failed to send candidate over WS: {}", e);
                    }
                });
            }
        }));

        wrtc_guard.on_connect = Some(Arc::new(|| {
            tracing::debug!("FileServer is ready");
        }));

        // Set up per-request channel handler
        let file_server_clone = file_server.clone();
        wrtc_guard.on_incoming_channel = Some(Arc::new(move |channel: Arc<DataChannelWrapper>| {
            tracing::debug!("Incoming channel: {}", channel.label());
            file_server_clone.handle_channel(channel);
        }));

        wrtc_guard.on_error = Some(Arc::new(move |msg: String| {
            tracing::error!("Error in WebRTC connection: {}", msg);
        }));

        wrtc_guard.on_close = Some(Arc::new(move || {
            tracing::debug!("Data channel closed");
        }));

        // Connect as server (not initiator)
        wrtc_guard.connect(false, &ci.ice_servers).await?;

        Ok(())
    }

    /// Handle an incoming call (legacy method - combines create and setup)
    pub async fn on_call(
        &self,
        source: Arc<tokio::sync::RwLock<dyn StorageSource + Send + Sync>>,
        ci: CallInInfo,
    ) -> Result<Arc<Mutex<WebRTCConnector>>, String> {
        tracing::debug!("Handling incoming call {}", ci.call_id);

        let mut wrtc = WebRTCConnector::new();
        let call_id = ci.call_id.clone();

        // Create FileServer for per-request channel handling
        let file_server = Arc::new(FileServer::new(source.clone()));

        // Set up WebRTC callbacks
        let back_clone = self.back.clone();
        let call_id_clone = call_id.clone();
        wrtc.on_answer_create = Some(Arc::new(move |sdp: String| {
            tracing::debug!("Got SDP answer, sending over WS");
            if let Some(ref back) = back_clone {
                let back = back.clone();
                let call_id = call_id_clone.clone();
                tokio::spawn(async move {
                    let msg = serde_json::json!({
                        "call_id": call_id,
                        "message": {
                            "type": "answer",
                            "sdp": sdp,
                            "from": "server"
                        }
                    });
                    if let Err(e) = back.read().await.send_ws(msg).await {
                        tracing::error!("Failed to send answer over WS: {}", e);
                    }
                });
            }
        }));

        let back_clone = self.back.clone();
        let call_id_clone = call_id.clone();
        wrtc.on_candidate_change = Some(Arc::new(move |candidate: IceCandidate| {
            tracing::debug!("Got local candidate change, sending over WS");
            if let Some(ref back) = back_clone {
                let back = back.clone();
                let call_id = call_id_clone.clone();
                tokio::spawn(async move {
                    let msg = serde_json::json!({
                        "call_id": call_id,
                        "message": {
                            "type": "candidate",
                            "sdp": candidate.candidate,
                            "mid": candidate.sdp_mid,
                            "mline": candidate.sdp_m_line_index,
                            "from": "server"
                        }
                    });
                    if let Err(e) = back.read().await.send_ws(msg).await {
                        tracing::error!("Failed to send candidate over WS: {}", e);
                    }
                });
            }
        }));

        wrtc.on_connect = Some(Arc::new(|| {
            tracing::debug!("FileServer is ready");
        }));

        // Set up per-request channel handler
        let file_server_clone = file_server.clone();
        wrtc.on_incoming_channel = Some(Arc::new(move |channel: Arc<DataChannelWrapper>| {
            tracing::debug!("Incoming channel: {}", channel.label());
            file_server_clone.handle_channel(channel);
        }));

        wrtc.on_error = Some(Arc::new(move |msg: String| {
            tracing::error!("Error in WebRTC connection: {}", msg);
        }));

        wrtc.on_close = Some(Arc::new(move || {
            tracing::debug!("Data channel closed");
        }));

        // Connect as server (not initiator)
        wrtc.connect(false, &ci.ice_servers).await?;

        // Wrap in Arc<Mutex> for sharing
        let wrtc = Arc::new(Mutex::new(wrtc));

        Ok(wrtc)
    }

    /// Handle an incoming call using relay-based signaling (new architecture)
    ///
    /// This method uses the relay probe channel for WebRTC signaling instead of
    /// WebSocket handlers. The relay must be configured in the CallInInfo.
    pub async fn on_call_with_relay(
        &self,
        source: Arc<tokio::sync::RwLock<dyn StorageSource + Send + Sync>>,
        ci: CallInInfo,
        encryption_key: Option<[u8; 32]>,
    ) -> Result<Arc<WebRTCConnector>, String> {
        tracing::debug!("Handling incoming call {} with relay", ci.call_id);

        // Get relay config from call info
        let relay_config = ci
            .relay
            .as_ref()
            .map(|r| RelayConfig {
                send_url: r.send_url.clone(),
                receive_url: r.receive_url.clone(),
            })
            .ok_or("No relay config in call info")?;

        let mut wrtc = WebRTCConnector::with_relay(Some(relay_config));
        let call_id = ci.call_id.clone();

        // Derive session key from master key and call_id if encryption is enabled
        if let Some(master_key) = encryption_key {
            let session_key = super::data_channel::derive_session_key(&master_key, &call_id)
                .map_err(|e| format!("Failed to derive session key: {}", e))?;
            tracing::debug!("Derived session key for call {}", call_id);
            wrtc.set_encryption_key(session_key).await;
        }

        // Create FileServer for per-request channel handling
        let file_server = Arc::new(FileServer::new(source.clone()));

        // Set up per-request channel handler
        let file_server_clone = file_server.clone();
        wrtc.on_incoming_channel = Some(Arc::new(move |channel: Arc<DataChannelWrapper>| {
            tracing::debug!("Incoming channel: {}", channel.label());
            file_server_clone.handle_channel(channel);
        }));

        wrtc.on_error = Some(Arc::new(move |msg: String| {
            tracing::error!("Error in WebRTC connection: {}", msg);
        }));

        // Untrack on close so a finished/failed serve drops out of active_serves
        // (mirrors TS `wrtc.onConnectionClose -> untrackServe`). Fire-and-forget:
        // on_close is a sync callback, the map lock is async.
        let serves = self.active_serves.clone();
        let untrack_token = ci.url_token.clone();
        let untrack_call = call_id.clone();
        wrtc.on_close = Some(Arc::new(move || {
            tracing::debug!("Connection closed");
            let serves = serves.clone();
            let url_token = untrack_token.clone();
            let call_id = untrack_call.clone();
            tokio::spawn(async move {
                let mut map = serves.lock().await;
                if let Some(inner) = map.get_mut(&url_token) {
                    inner.remove(&call_id);
                    if inner.is_empty() {
                        map.remove(&url_token);
                    }
                }
            });
        }));

        // Connect as callee (server). Pass ICE servers so setup_webrtc runs on the
        // answerer, mirroring TS CallServer.ts: `new WebRTCConnector(false, ci.ice_servers, relay)`.
        // The offer/answer/ICE exchange then happens over the relay probe loop, and the
        // relay stays as the fallback transport if WebRTC negotiation fails.
        wrtc.connect_as_callee(&ci.ice_servers).await?;

        // Share directly (no outer Mutex; the connector's request-serving methods
        // take `&self`).
        let wrtc = Arc::new(wrtc);

        // Track so the sender can abort midflight via cancel_serve(url_token).
        self.track_serve(&ci.url_token, &call_id, wrtc.clone()).await;

        Ok(wrtc)
    }
}

impl Default for CallServer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::p2p::webrtc_connector::WebRTCState;

    #[tokio::test]
    async fn tracked_serves_are_active_until_cancelled() {
        let cs = CallServer::new();
        let w1 = Arc::new(WebRTCConnector::new());
        let w2 = Arc::new(WebRTCConnector::new());
        cs.track_serve("tok", "call-1", w1.clone()).await;
        cs.track_serve("tok", "call-2", w2.clone()).await;

        assert!(cs.is_call_active("call-1").await);
        assert!(cs.is_call_active("call-2").await);
        assert!(!cs.is_call_active("call-3").await);

        cs.cancel_serve("tok").await;
        assert!(!cs.is_call_active("call-1").await);
        assert!(!cs.is_call_active("call-2").await);
        assert_eq!(w1.state().await, WebRTCState::Closed, "cancel closes the connector");
        assert_eq!(w2.state().await, WebRTCState::Closed);
        assert!(cs.active_serves.lock().await.is_empty());
    }

    #[tokio::test]
    async fn cancel_serve_on_unknown_token_is_a_noop() {
        let cs = CallServer::new();
        let w = Arc::new(WebRTCConnector::new());
        cs.track_serve("tok", "call-1", w.clone()).await;

        cs.cancel_serve("other-token").await;
        assert!(cs.is_call_active("call-1").await, "unrelated serve untouched");
        assert_eq!(w.state().await, WebRTCState::New);
    }
}
