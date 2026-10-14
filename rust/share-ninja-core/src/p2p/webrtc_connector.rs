//! WebRTC connection management using webrtc-rs
//!
//! Supports both WebRTC and relay-based signaling/transport.
//! Relay is used for signaling and as a fallback transport when WebRTC fails.

use std::sync::Arc;

use bytes::BytesMut;
use tokio::sync::{Mutex, RwLock};
use webrtc::data_channel::{DataChannel, DataChannelEvent};
use webrtc::peer_connection::{
    PeerConnection, PeerConnectionBuilder, PeerConnectionEventHandler, RTCConfigurationBuilder, RTCIceCandidateInit,
    RTCIceGatheringState, RTCIceServer, RTCPeerConnectionIceEvent, RTCPeerConnectionState, RTCSessionDescription,
    SettingEngineBuilder,
};

use crate::back::IceServer;

use super::bulk_gate::BulkGate;
use super::data_channel::{DataChannelWrapper, EncryptionConfig};
use super::relay_transport::{RelayConfig, WebSocketTransport, connect_relay};

const BUFFERED_MAX: usize = 10_000_000;

/// WebRTC connection state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WebRTCState {
    New,
    Connecting,
    Connected,
    Disconnected,
    Failed,
    Closed,
}

/// ICE candidate information
#[derive(Debug, Clone)]
pub struct IceCandidate {
    pub candidate: String,
    pub sdp_mid: Option<String>,
    pub sdp_m_line_index: Option<u16>,
}

/// SDP offer/answer
#[derive(Debug, Clone)]
pub struct SessionDescription {
    pub sdp_type: String,
    pub sdp: String,
}

/// Callback types for WebRTC events
pub type OnOfferCreate = Arc<dyn Fn(String) + Send + Sync>;
pub type OnAnswerCreate = Arc<dyn Fn(String) + Send + Sync>;
pub type OnCandidateChange = Arc<dyn Fn(IceCandidate) + Send + Sync>;
pub type OnConnect = Arc<dyn Fn() + Send + Sync>;
pub type OnMessage = Arc<dyn Fn(Vec<u8>) + Send + Sync>;
pub type OnError = Arc<dyn Fn(String) + Send + Sync>;
pub type OnClose = Arc<dyn Fn() + Send + Sync>;

/// Callback for incoming channels (server side)
pub type OnIncomingChannel = Arc<dyn Fn(Arc<DataChannelWrapper>) + Send + Sync>;

const MAX_POOL_SIZE: usize = 10;
const RELAY_CONNECT_TIMEOUT_MS: u64 = 10000;

/// Local UDP bind addresses for ICE candidate gathering. The wildcards are
/// expanded by `PeerConnectionBuilder::with_udp_addrs` to one socket per
/// interface address of that family, skipping loopback and link-local — which
/// is the filtering our old `SettingEngine::set_ip_filter` did by hand (IPv4
/// 169.254/16 e.g. VMware VMnet, IPv6 fe80::/10 which can't even bind without a
/// scope id). WSL/Hyper-V live in 172.16/12 and are NOT cut — indistinguishable
/// by IP from a real private LAN. Port 0 = ephemeral per socket.
///
/// The Windows `WSAECONNRESET` quirk (os error 10054 on the recv after an ICMP
/// port-unreachable from a failed srflx/relay check) that used to permanently
/// deafen a candidate's socket is handled in webrtc-rs 0.20's socket driver:
/// `ConnectionReset`/`ConnectionRefused` are classified transient and the recv
/// loop re-arms. We no longer vendor `webrtc-ice` for it.
const ICE_UDP_BIND_ADDRS: [&str; 2] = ["0.0.0.0:0", "[::]:0"];

/// Convert backend ICE servers to webrtc-rs ones.
///
/// Use STUN only; drop TURN relay candidates. When both peers are on the
/// same host, a browser's mDNS (.local) candidate resolves to loopback and
/// webrtc-rs pairs the TURN relay with 127.0.0.1, which coturn rejects with
/// "403 Forbidden IP". That error stalls the ICE agent so it never nominates
/// the valid host pair, and the connection fails after ~30s. Hard-NAT peers
/// fall back to the app-level relay channels instead of TURN.
fn stun_only_ice_servers(ice_servers: &[IceServer]) -> Vec<RTCIceServer> {
    ice_servers
        .iter()
        .filter(|s| s.uri.starts_with("stun"))
        .map(|s| RTCIceServer {
            urls: vec![s.uri.clone()],
            username: s.username.clone(),
            credential: s.password.clone(),
        })
        .collect()
}

/// Build a peer connection with the given event handler and start its driver.
async fn build_peer_connection(
    ice_servers: &[IceServer],
    handler: Arc<dyn PeerConnectionEventHandler>,
) -> Result<Arc<dyn PeerConnection>, String> {
    let config = RTCConfigurationBuilder::default()
        .with_ice_servers(stun_only_ice_servers(ice_servers))
        .build();
    // EXPERIMENT: SCTP receive window (a_rwnd) 5 MiB, parity with libwebrtc
    // dcSCTP (max_receiver_window_buffer_size); rtc default is 1 MiB.
    let setting_engine = SettingEngineBuilder::new()
        .with_sctp_max_receive_buffer_size(5 * 1024 * 1024)
        .build();
    let pc = PeerConnectionBuilder::new()
        .with_configuration(config)
        .with_setting_engine(setting_engine)
        .with_handler(handler)
        .with_udp_addrs(ICE_UDP_BIND_ADDRS.to_vec())
        .build()
        .await
        .map_err(|e| e.to_string())?;
    Ok(Arc::new(pc))
}

/// End-of-candidates marker, sent once gathering completes. 0.17 signalled this
/// with `on_ice_candidate(None)`; 0.20 has no such event, so it is derived from
/// `on_ice_gathering_state_change(Complete)`.
fn end_of_candidates() -> IceCandidate {
    IceCandidate {
        candidate: String::new(),
        sdp_mid: Some("0".to_string()),
        sdp_m_line_index: Some(0),
    }
}

/// Peer-connection events for the relay-signalled path (`setup_webrtc`):
/// candidates go out over the relay probe channel, incoming channels are
/// wrapped and handed to `on_incoming_channel`.
struct RelaySignaledHandler {
    probe_channel: Arc<Mutex<Option<Arc<WebSocketTransport>>>>,
    state: Arc<RwLock<WebRTCState>>,
    pc_state: Arc<Mutex<RTCPeerConnectionState>>,
    webrtc_connected: Arc<Mutex<bool>>,
    relay_connected: Arc<Mutex<bool>>,
    on_error: Option<OnError>,
    on_close: Option<OnClose>,
    on_incoming_channel: Option<OnIncomingChannel>,
    encryption_key: Arc<Mutex<Option<[u8; 32]>>>,
    bulk_gate: Arc<BulkGate>,
}

impl RelaySignaledHandler {
    async fn send_candidate_signal(&self, candidate: IceCandidate) {
        let signal = serde_json::json!({
            "type": "candidate",
            "sdp": candidate.candidate,
            "mid": candidate.sdp_mid,
            "mline": candidate.sdp_m_line_index
        });
        let probe = self.probe_channel.lock().await;
        if let Some(ref channel) = *probe {
            let data = serde_json::to_vec(&signal).unwrap_or_default();
            match channel.send(&data).await {
                Ok(_) => tracing::debug!("on_ice_candidate: sent {} bytes via probe", data.len()),
                Err(e) => tracing::warn!("on_ice_candidate: probe send failed: {}", e),
            }
        } else {
            tracing::warn!("on_ice_candidate: probe channel not available, dropping candidate");
        }
    }
}

#[async_trait::async_trait]
impl PeerConnectionEventHandler for RelaySignaledHandler {
    async fn on_ice_candidate(&self, event: RTCPeerConnectionIceEvent) {
        match event.candidate.to_json() {
            Ok(json) => {
                tracing::debug!("on_ice_candidate: {}", json.candidate);
                self.send_candidate_signal(IceCandidate {
                    candidate: json.candidate,
                    sdp_mid: json.sdp_mid,
                    sdp_m_line_index: json.sdp_mline_index,
                })
                .await;
            }
            Err(e) => {
                tracing::warn!("on_ice_candidate: to_json failed: {}", e);
            }
        }
    }

    async fn on_ice_gathering_state_change(&self, state: RTCIceGatheringState) {
        if state == RTCIceGatheringState::Complete {
            tracing::debug!("on_ice_candidate: end-of-candidates");
            self.send_candidate_signal(end_of_candidates()).await;
        }
    }

    async fn on_connection_state_change(&self, s: RTCPeerConnectionState) {
        tracing::debug!("Peer connection state: {:?}", s);
        *self.pc_state.lock().await = s;
        match s {
            RTCPeerConnectionState::Connected => {
                *self.state.write().await = WebRTCState::Connected;
                *self.webrtc_connected.lock().await = true;
                tracing::debug!("WebRTC connected, will prefer WebRTC channels");
            }
            RTCPeerConnectionState::Failed => {
                *self.webrtc_connected.lock().await = false;
                if !*self.relay_connected.lock().await {
                    *self.state.write().await = WebRTCState::Failed;
                    if let Some(ref callback) = self.on_error {
                        callback("Connection failed".to_string());
                    }
                } else {
                    tracing::debug!("WebRTC failed, falling back to relay");
                }
            }
            RTCPeerConnectionState::Disconnected => {
                *self.webrtc_connected.lock().await = false;
                tracing::debug!("WebRTC disconnected");
            }
            RTCPeerConnectionState::Closed => {
                *self.webrtc_connected.lock().await = false;
                if !*self.relay_connected.lock().await {
                    *self.state.write().await = WebRTCState::Closed;
                    if let Some(ref callback) = self.on_close {
                        callback();
                    }
                }
            }
            _ => {}
        }
    }

    // Server side: handle incoming WebRTC data channels
    async fn on_data_channel(&self, dc: Arc<dyn DataChannel>) {
        tracing::debug!("Incoming WebRTC channel: {:?}", dc.label().await);
        if let Some(ref callback) = self.on_incoming_channel {
            let enc_key = *self.encryption_key.lock().await;
            let enc_config = enc_key.map(|key| EncryptionConfig { key, is_client: false });
            match DataChannelWrapper::new(Arc::new(dc), enc_config, Some(self.bulk_gate.clone())).await {
                Ok(wrapper) => callback(Arc::new(wrapper)),
                Err(e) => tracing::error!("Incoming WebRTC channel: wrapper failed: {}", e),
            }
        }
    }
}

/// Peer-connection events for the backend-signalled path (`connect`):
/// candidates go to `on_candidate_change`; incoming channels (responder only)
/// either go to `on_incoming_channel` (per-channel architecture) or, legacy,
/// become the single `data_channel` driving `on_message`/`on_connect`.
struct BackendSignaledHandler {
    is_initiator: bool,
    state: Arc<RwLock<WebRTCState>>,
    pc_state: Arc<Mutex<RTCPeerConnectionState>>,
    data_channel: Arc<Mutex<Option<Arc<dyn DataChannel>>>>,
    encryption_key: Arc<Mutex<Option<[u8; 32]>>>,
    bulk_gate: Arc<BulkGate>,
    on_error: Option<OnError>,
    on_close: Option<OnClose>,
    on_incoming_channel: Option<OnIncomingChannel>,
    on_candidate_change: Option<OnCandidateChange>,
    on_connect: Option<OnConnect>,
    on_message: Option<OnMessage>,
}

#[async_trait::async_trait]
impl PeerConnectionEventHandler for BackendSignaledHandler {
    async fn on_ice_candidate(&self, event: RTCPeerConnectionIceEvent) {
        tracing::debug!("Got local ICE candidate");
        if let Some(ref callback) = self.on_candidate_change {
            let json = event.candidate.to_json();
            let ice_candidate = IceCandidate {
                candidate: json.as_ref().map(|j| j.candidate.clone()).unwrap_or_default(),
                sdp_mid: json.as_ref().ok().and_then(|j| j.sdp_mid.clone()),
                sdp_m_line_index: json.as_ref().ok().and_then(|j| j.sdp_mline_index),
            };
            callback(ice_candidate);
        }
    }

    async fn on_ice_gathering_state_change(&self, state: RTCIceGatheringState) {
        if state == RTCIceGatheringState::Complete {
            tracing::debug!("ICE gathering complete");
            // Send end-of-candidates
            if let Some(ref callback) = self.on_candidate_change {
                callback(end_of_candidates());
            }
        }
    }

    async fn on_connection_state_change(&self, s: RTCPeerConnectionState) {
        tracing::debug!("Peer connection state: {:?}", s);
        *self.pc_state.lock().await = s;
        match s {
            RTCPeerConnectionState::Connected => {
                *self.state.write().await = WebRTCState::Connected;
            }
            RTCPeerConnectionState::Disconnected => {
                *self.state.write().await = WebRTCState::Disconnected;
            }
            RTCPeerConnectionState::Failed => {
                *self.state.write().await = WebRTCState::Failed;
                if let Some(ref callback) = self.on_error {
                    callback("Connection failed".to_string());
                }
            }
            RTCPeerConnectionState::Closed => {
                *self.state.write().await = WebRTCState::Closed;
                if let Some(ref callback) = self.on_close {
                    callback();
                }
            }
            _ => {}
        }
    }

    async fn on_data_channel(&self, dc: Arc<dyn DataChannel>) {
        // Only the responder waits for the remote's channel (the initiator creates its own).
        if self.is_initiator {
            return;
        }
        tracing::debug!("Incoming data channel: {:?}", dc.label().await);

        // If we have on_incoming_channel callback, use new per-channel architecture
        if let Some(ref callback) = self.on_incoming_channel {
            let enc_key = *self.encryption_key.lock().await;
            let enc_config = enc_key.map(|key| EncryptionConfig {
                key,
                is_client: false, // Server side
            });

            // Create wrapper immediately - this starts the event poll task, which
            // queues messages even before we call the callback.
            // Call handler immediately - FileServerV2 will wait for messages.
            // Don't wait for open since messages might arrive before open state.
            match DataChannelWrapper::new(Arc::new(dc), enc_config, Some(self.bulk_gate.clone())).await {
                Ok(wrapper) => callback(Arc::new(wrapper)),
                Err(e) => tracing::error!("Incoming data channel: wrapper failed: {}", e),
            }
            return;
        }

        // Legacy: single channel mode
        *self.data_channel.lock().await = Some(dc.clone());
        spawn_legacy_data_channel_events(
            dc,
            self.state.clone(),
            self.on_message.clone(),
            self.on_connect.clone(),
            self.on_close.clone(),
            self.on_error.clone(),
        );
    }
}

/// Legacy single-channel mode: drive the channel's events into the connector's
/// flat callbacks. 0.17 registered `on_message`/`on_open`/`on_close`/`on_error`
/// callbacks; 0.20 delivers the same through `DataChannel::poll()`.
fn spawn_legacy_data_channel_events(
    dc: Arc<dyn DataChannel>,
    state: Arc<RwLock<WebRTCState>>,
    on_message: Option<OnMessage>,
    on_connect: Option<OnConnect>,
    on_close: Option<OnClose>,
    on_error: Option<OnError>,
) {
    tokio::spawn(async move {
        while let Some(event) = dc.poll().await {
            match event {
                DataChannelEvent::OnMessage(msg) => {
                    if !msg.data.is_empty()
                        && let Some(ref callback) = on_message
                    {
                        callback(msg.data.to_vec());
                    }
                }
                DataChannelEvent::OnOpen => {
                    tracing::debug!("Data channel open");
                    *state.write().await = WebRTCState::Connected;
                    if let Some(ref callback) = on_connect {
                        callback();
                    }
                }
                DataChannelEvent::OnClose => {
                    tracing::debug!("Data channel closed");
                    if let Some(ref callback) = on_close {
                        callback();
                    }
                    break;
                }
                DataChannelEvent::OnError => {
                    tracing::error!("Data channel error");
                    if let Some(ref callback) = on_error {
                        callback("Data channel error".to_string());
                    }
                }
                DataChannelEvent::OnClosing
                | DataChannelEvent::OnBufferedAmountLow
                | DataChannelEvent::OnBufferedAmountHigh => {}
                // `DataChannelEvent` is non-exhaustive since 0.21.
                _ => {}
            }
        }
    });
}

/// WebRTC connector for peer-to-peer connections
///
/// Supports both WebRTC and relay-based transport. When relay is configured,
/// signaling (offers, answers, ICE candidates) goes through the relay probe channel.
pub struct WebRTCConnector {
    state: Arc<RwLock<WebRTCState>>,
    is_initiator: bool,
    // Interior-mutable so the request-serving methods (and `close`) only need `&self`.
    // That lets the connector be shared as a bare `Arc<WebRTCConnector>` instead of
    // `Arc<Mutex<WebRTCConnector>>` — the outer Mutex used to be held across
    // `acquire_channel`'s up-to-10s channel open, serializing every concurrent request.
    // Written only during setup (before the connector is shared) and taken in `close`.
    peer_connection: Mutex<Option<Arc<dyn PeerConnection>>>,
    /// Last `on_connection_state_change` value. 0.20's `PeerConnection` has no
    /// `connection_state()` getter, so it is tracked from the event.
    pc_state: Arc<Mutex<RTCPeerConnectionState>>,
    data_channel: Arc<Mutex<Option<Arc<dyn DataChannel>>>>,
    send_queue: Arc<Mutex<Vec<Vec<u8>>>>,

    // Channel pooling
    channel_id: Arc<Mutex<u32>>,
    channel_pool: Arc<Mutex<Vec<Arc<DataChannelWrapper>>>>,
    /// One bulk gate per connection (see `BulkGate`), given to every WebRTC
    /// channel this connector wraps, own or incoming.
    bulk_gate: Arc<BulkGate>,

    // Relay state
    relay_config: Option<RelayConfig>,
    relay_channel_pool: Arc<Mutex<Vec<Arc<DataChannelWrapper>>>>,
    relay_channel_id: Arc<Mutex<u32>>,
    relay_connected: Arc<Mutex<bool>>,
    webrtc_connected: Arc<Mutex<bool>>,
    probe_channel: Arc<Mutex<Option<Arc<WebSocketTransport>>>>,
    probe_ever_connected: Arc<Mutex<bool>>,
    closed: Arc<Mutex<bool>>,

    // Encryption
    encryption_key: Arc<Mutex<Option<[u8; 32]>>>,

    // Pending ICE candidates (before remote description is set)
    pending_candidates: Arc<Mutex<Vec<IceCandidate>>>,
    has_remote_description: Arc<Mutex<bool>>,

    // Callbacks
    pub on_error: Option<OnError>,
    pub on_close: Option<OnClose>,
    pub on_incoming_channel: Option<OnIncomingChannel>,

    // Legacy callbacks (for backward compatibility)
    pub on_offer_create: Option<OnOfferCreate>,
    pub on_answer_create: Option<OnAnswerCreate>,
    pub on_candidate_change: Option<OnCandidateChange>,
    pub on_connect: Option<OnConnect>,
    pub on_message: Option<OnMessage>,
}

impl WebRTCConnector {
    pub fn new() -> Self {
        Self::with_relay(None)
    }

    pub fn with_relay(relay_config: Option<RelayConfig>) -> Self {
        Self {
            state: Arc::new(RwLock::new(WebRTCState::New)),
            is_initiator: false,
            peer_connection: Mutex::new(None),
            pc_state: Arc::new(Mutex::new(RTCPeerConnectionState::default())),
            data_channel: Arc::new(Mutex::new(None)),
            send_queue: Arc::new(Mutex::new(Vec::new())),
            channel_id: Arc::new(Mutex::new(0)),
            channel_pool: Arc::new(Mutex::new(Vec::new())),
            bulk_gate: Arc::new(BulkGate::new()),
            relay_config,
            relay_channel_pool: Arc::new(Mutex::new(Vec::new())),
            relay_channel_id: Arc::new(Mutex::new(0)),
            relay_connected: Arc::new(Mutex::new(false)),
            webrtc_connected: Arc::new(Mutex::new(false)),
            probe_channel: Arc::new(Mutex::new(None)),
            probe_ever_connected: Arc::new(Mutex::new(false)),
            closed: Arc::new(Mutex::new(false)),
            encryption_key: Arc::new(Mutex::new(None)),
            pending_candidates: Arc::new(Mutex::new(Vec::new())),
            has_remote_description: Arc::new(Mutex::new(false)),
            on_error: None,
            on_close: None,
            on_incoming_channel: None,
            on_offer_create: None,
            on_answer_create: None,
            on_candidate_change: None,
            on_connect: None,
            on_message: None,
        }
    }

    /// Get the role identifier for logging
    fn who(&self) -> &'static str {
        if self.is_initiator { "client" } else { "server" }
    }

    /// Snapshot the peer connection (clones the `Arc` out of the interior mutex so
    /// callers never hold the `peer_connection` lock across `await`).
    async fn pc(&self) -> Option<Arc<dyn PeerConnection>> {
        self.peer_connection.lock().await.clone()
    }

    /// Check if a channel is a relay channel (by label prefix)
    fn is_relay_channel(channel: &DataChannelWrapper) -> bool {
        channel.label().starts_with("relay-")
    }

    /// Send a signal message over the probe channel
    async fn send_signal(&self, msg: &serde_json::Value) {
        let probe = self.probe_channel.lock().await;
        if let Some(ref channel) = *probe {
            if channel.is_open().await {
                let data = serde_json::to_vec(msg).unwrap_or_default();
                tracing::debug!("{} sending signal: {} bytes", self.who(), data.len());
                if let Err(e) = channel.send(&data).await {
                    tracing::error!("Failed to send signal: {}", e);
                } else {
                    tracing::debug!("{} signal sent successfully", self.who());
                }
            } else {
                tracing::error!("Cannot send signal: probe channel not open");
            }
        } else {
            tracing::error!("Cannot send signal: no probe channel");
        }
    }

    /// Start the relay probe channel and begin listening for signals
    pub async fn start_relay(&self) -> Result<(), String> {
        let relay_config = self.relay_config.as_ref().ok_or("No relay config")?;

        let url = if self.is_initiator {
            &relay_config.send_url
        } else {
            &relay_config.receive_url
        };

        tracing::debug!("{} connecting relay probe", self.who());

        let transport = connect_relay(url, "relay-probe", RELAY_CONNECT_TIMEOUT_MS).await?;

        *self.probe_channel.lock().await = Some(Arc::new(transport));
        *self.relay_connected.lock().await = true;
        *self.probe_ever_connected.lock().await = true;

        tracing::debug!("{} relay connected", self.who());

        // NOTE: probe loop is started by the caller (connect_as_caller / connect_as_callee)
        // AFTER setup_webrtc, so the closure captures the populated peer_connection.

        Ok(())
    }

    /// Start background task to handle incoming probe messages
    async fn start_probe_loop(&self) {
        let probe_channel = self.probe_channel.clone();
        let closed = self.closed.clone();
        let relay_connected = self.relay_connected.clone();
        let webrtc_connected = self.webrtc_connected.clone();
        let on_close = self.on_close.clone();
        let relay_config = self.relay_config.clone();
        let on_incoming_channel = self.on_incoming_channel.clone();
        let encryption_key = self.encryption_key.clone();
        let is_initiator = self.is_initiator;

        // Clone fields needed for signal handling. Snapshot the peer connection now —
        // start_probe_loop runs after setup_webrtc, so it's already populated.
        let peer_connection = self.peer_connection.lock().await.clone();
        let has_remote_description = self.has_remote_description.clone();
        let pending_candidates = self.pending_candidates.clone();
        let probe_channel_for_signal = self.probe_channel.clone();

        tokio::spawn(async move {
            loop {
                // Get channel reference without holding lock during recv
                let channel = {
                    let probe = probe_channel.lock().await;
                    probe.clone()
                };

                let data = if let Some(ref ch) = channel {
                    ch.recv().await
                } else {
                    break;
                };

                if *closed.lock().await {
                    break;
                }

                match data {
                    Some(data) => {
                        if let Ok(msg) = serde_json::from_slice::<serde_json::Value>(&data) {
                            let msg_type = msg["type"].as_str().unwrap_or("");
                            tracing::debug!("probe msg: {}", msg_type);

                            match msg_type {
                                "offer" => {
                                    if let Some(ref pc) = peer_connection
                                        && let Some(sdp) = msg["sdp"].as_str()
                                    {
                                        tracing::debug!("Probe received SDP offer (remote):\n{}", sdp);
                                        if let Ok(offer) = RTCSessionDescription::offer(sdp.to_string()) {
                                            if let Err(e) = pc.set_remote_description(offer).await {
                                                tracing::error!("Failed to set remote description: {}", e);
                                                continue;
                                            }
                                            *has_remote_description.lock().await = true;

                                            // Flush pending candidates
                                            let candidates: Vec<IceCandidate> = {
                                                let mut pending = pending_candidates.lock().await;
                                                std::mem::take(&mut *pending)
                                            };
                                            for candidate in candidates {
                                                let rtc_candidate = RTCIceCandidateInit {
                                                    candidate: candidate.candidate,
                                                    sdp_mid: candidate.sdp_mid,
                                                    sdp_mline_index: candidate.sdp_m_line_index,
                                                    ..Default::default()
                                                };
                                                let _ = pc.add_ice_candidate(rtc_candidate).await;
                                            }

                                            // Create answer
                                            if let Ok(answer) = pc.create_answer(None).await {
                                                if let Err(e) = pc.set_local_description(answer.clone()).await {
                                                    tracing::error!("Failed to set local description: {}", e);
                                                    continue;
                                                }
                                                tracing::debug!("Probe created SDP answer (local):\n{}", answer.sdp);
                                                let signal = serde_json::json!({
                                                    "type": "answer",
                                                    "sdp": answer.sdp
                                                });
                                                let probe = probe_channel_for_signal.lock().await;
                                                if let Some(ref channel) = *probe {
                                                    let data = serde_json::to_vec(&signal).unwrap_or_default();
                                                    let _ = channel.send(&data).await;
                                                }
                                            }
                                        }
                                    }
                                }
                                "answer" => {
                                    if let Some(ref pc) = peer_connection
                                        && let Some(sdp) = msg["sdp"].as_str()
                                    {
                                        tracing::debug!("Probe received SDP answer (remote):\n{}", sdp);
                                        if let Ok(answer) = RTCSessionDescription::answer(sdp.to_string()) {
                                            match pc.set_remote_description(answer).await {
                                                Ok(_) => tracing::debug!("set_remote_description(answer) ok"),
                                                Err(e) => {
                                                    tracing::error!("Failed to set remote description: {}", e);
                                                }
                                            }
                                            *has_remote_description.lock().await = true;

                                            // Flush pending candidates
                                            let candidates: Vec<IceCandidate> = {
                                                let mut pending = pending_candidates.lock().await;
                                                std::mem::take(&mut *pending)
                                            };
                                            tracing::debug!("flushing {} pending candidates", candidates.len());
                                            for candidate in candidates {
                                                let sdp_log = candidate.candidate.clone();
                                                let rtc_candidate = RTCIceCandidateInit {
                                                    candidate: candidate.candidate,
                                                    sdp_mid: candidate.sdp_mid,
                                                    sdp_mline_index: candidate.sdp_m_line_index,
                                                    ..Default::default()
                                                };
                                                match pc.add_ice_candidate(rtc_candidate).await {
                                                    Ok(_) => {
                                                        tracing::debug!("add_ice_candidate (pending) ok: {}", sdp_log)
                                                    }
                                                    Err(e) => tracing::warn!(
                                                        "add_ice_candidate (pending) failed: {} sdp={}",
                                                        e,
                                                        sdp_log
                                                    ),
                                                }
                                            }
                                        }
                                    }
                                }
                                "candidate" => {
                                    if let Some(ref pc) = peer_connection {
                                        let sdp = msg["sdp"].as_str().unwrap_or("");
                                        if sdp.is_empty() {
                                            tracing::debug!("Received end-of-candidates");
                                            continue;
                                        }
                                        let candidate = IceCandidate {
                                            candidate: sdp.to_string(),
                                            sdp_mid: msg["mid"].as_str().map(|s| s.to_string()),
                                            sdp_m_line_index: msg["mline"].as_u64().map(|n| n as u16),
                                        };

                                        if !*has_remote_description.lock().await {
                                            tracing::debug!(
                                                "queueing remote candidate (no remote desc yet): {}",
                                                candidate.candidate
                                            );
                                            pending_candidates.lock().await.push(candidate);
                                        } else {
                                            let sdp_log = candidate.candidate.clone();
                                            let rtc_candidate = RTCIceCandidateInit {
                                                candidate: candidate.candidate,
                                                sdp_mid: candidate.sdp_mid,
                                                sdp_mline_index: candidate.sdp_m_line_index,
                                                ..Default::default()
                                            };
                                            match pc.add_ice_candidate(rtc_candidate).await {
                                                Ok(_) => tracing::debug!("add_ice_candidate ok: {}", sdp_log),
                                                Err(e) => {
                                                    tracing::warn!("add_ice_candidate failed: {} sdp={}", e, sdp_log)
                                                }
                                            }
                                        }
                                    }
                                }
                                "new-channel" => {
                                    if let Some(ch) = msg["ch"].as_str()
                                        && let Some(ref config) = relay_config
                                    {
                                        let url = if is_initiator {
                                            config.send_url.clone()
                                        } else {
                                            config.receive_url.clone()
                                        };
                                        let ch = ch.to_string();
                                        let who = if is_initiator { "client" } else { "server" };
                                        tracing::debug!("{} opening relay channel on request: {}", who, ch);

                                        // Spawn task to connect relay channel without blocking probe loop
                                        let encryption_key = encryption_key.clone();
                                        let on_incoming_channel = on_incoming_channel.clone();
                                        tokio::spawn(async move {
                                            match connect_relay(&url, &ch, RELAY_CONNECT_TIMEOUT_MS).await {
                                                Ok(transport) => {
                                                    let enc_key = *encryption_key.lock().await;
                                                    let enc_config = enc_key.map(|key| EncryptionConfig {
                                                        key,
                                                        is_client: is_initiator,
                                                    });
                                                    let wrapper = Arc::new(
                                                        DataChannelWrapper::from_relay(transport, enc_config).await,
                                                    );
                                                    tracing::debug!("{} relay channel opened: {}", who, ch);
                                                    if let Some(ref callback) = on_incoming_channel {
                                                        callback(wrapper);
                                                    }
                                                }
                                                Err(e) => {
                                                    tracing::error!("{} relay channel failed: {} - {}", who, ch, e);
                                                }
                                            }
                                        });
                                    }
                                }
                                _ => {
                                    tracing::error!("Unknown probe message type: {}", msg_type);
                                }
                            }
                        }
                    }
                    None => {
                        tracing::debug!("probe channel closed");
                        *relay_connected.lock().await = false;
                        if !*webrtc_connected.lock().await
                            && let Some(ref callback) = on_close
                        {
                            callback();
                        }
                        break;
                    }
                }
            }
        });
    }

    /// Connect as caller (client) - relay-based
    pub async fn connect_as_caller(&mut self, ice_servers: &[IceServer]) -> Result<(), String> {
        tracing::debug!("client starting connection");
        self.is_initiator = true;
        *self.state.write().await = WebRTCState::Connecting;

        // Start relay first (needed for signaling)
        self.start_relay().await?;

        // Set up WebRTC if ICE servers provided
        if !ice_servers.is_empty() {
            self.setup_webrtc(ice_servers).await?;
        }

        // Start probe loop AFTER setup_webrtc so it captures the live peer_connection.
        self.start_probe_loop().await;

        if !ice_servers.is_empty()
            && let Some(pc) = self.pc().await
        {
            // Create initial channel to trigger ICE gathering
            let channel_label = {
                let mut id = self.channel_id.lock().await;
                let label = format!("ch-{}", *id);
                *id += 1;
                label
            };

            let _dc = pc
                .create_data_channel(&channel_label, None)
                .await
                .map_err(|e| e.to_string())?;

            // Create offer
            let offer = pc.create_offer(None).await.map_err(|e| e.to_string())?;
            pc.set_local_description(offer.clone())
                .await
                .map_err(|e| e.to_string())?;

            tracing::debug!("Client created offer; WebRTC SDP offer (local):\n{}", offer.sdp);

            // Send offer via relay
            let signal = serde_json::json!({
                "type": "offer",
                "sdp": offer.sdp
            });
            self.send_signal(&signal).await;
        }

        Ok(())
    }

    /// Connect as callee (server) - relay-based
    pub async fn connect_as_callee(&mut self, ice_servers: &[IceServer]) -> Result<(), String> {
        tracing::debug!("server starting connection");
        self.is_initiator = false;
        *self.state.write().await = WebRTCState::Connecting;

        // Start relay first (needed for signaling)
        self.start_relay().await?;

        // Set up WebRTC if ICE servers provided
        if !ice_servers.is_empty() {
            self.setup_webrtc(ice_servers).await?;
        }

        // Start probe loop AFTER setup_webrtc so it captures the live peer_connection.
        self.start_probe_loop().await;

        // WebRTC negotiation happens via probe loop (offers/answers arrive there)
        Ok(())
    }

    /// Set up WebRTC peer connection
    async fn setup_webrtc(&mut self, ice_servers: &[IceServer]) -> Result<(), String> {
        let uris: Vec<&str> = ice_servers.iter().map(|s| s.uri.as_str()).collect();
        tracing::debug!("setup_webrtc ice_servers={:?}", uris);

        let handler = Arc::new(RelaySignaledHandler {
            probe_channel: self.probe_channel.clone(),
            state: self.state.clone(),
            pc_state: self.pc_state.clone(),
            webrtc_connected: self.webrtc_connected.clone(),
            relay_connected: self.relay_connected.clone(),
            on_error: self.on_error.clone(),
            on_close: self.on_close.clone(),
            on_incoming_channel: self.on_incoming_channel.clone(),
            encryption_key: self.encryption_key.clone(),
            bulk_gate: self.bulk_gate.clone(),
        });

        let pc = build_peer_connection(ice_servers, handler).await?;
        *self.peer_connection.lock().await = Some(pc);

        Ok(())
    }

    /// Set encryption key for all channels
    pub async fn set_encryption_key(&self, key: [u8; 32]) {
        *self.encryption_key.lock().await = Some(key);
    }

    /// Check if encryption is enabled
    pub async fn is_encrypted(&self) -> bool {
        self.encryption_key.lock().await.is_some()
    }

    /// Acquire a DataChannel - prefers WebRTC, falls back to relay
    pub async fn acquire_channel(&self) -> Result<Arc<DataChannelWrapper>, String> {
        tracing::debug!("{} acquire_channel", self.who());
        if *self.closed.lock().await {
            return Err("Connection is closed".to_string());
        }

        let webrtc_connected = *self.webrtc_connected.lock().await;
        let relay_connected = *self.relay_connected.lock().await;
        tracing::debug!(
            "{} acquire_channel: webrtc={}, relay={}",
            self.who(),
            webrtc_connected,
            relay_connected
        );

        // Prefer WebRTC if connected
        if webrtc_connected {
            if let Some(ch) = self.acquire_from_pool(&self.channel_pool).await {
                return Ok(ch);
            }
            return self.create_webrtc_channel().await;
        }

        // Fall back to relay
        if relay_connected && self.relay_config.is_some() {
            if let Some(ch) = self.acquire_from_pool(&self.relay_channel_pool).await {
                return Ok(ch);
            }
            tracing::debug!("{} creating new relay channel", self.who());
            return self.create_relay_channel().await;
        }

        // Neither available - check if WebRTC is still negotiating.
        // If the probe was up and is now gone, signaling is dead — don't
        // optimistically try a WebRTC channel that can never complete
        // negotiation; fail fast so upper layers don't hang.
        let probe_ever_connected = *self.probe_ever_connected.lock().await;
        let signaling_dead = probe_ever_connected && !relay_connected;
        if self.pc().await.is_some() && !signaling_dead {
            let state = *self.pc_state.lock().await;
            if state != RTCPeerConnectionState::Closed && state != RTCPeerConnectionState::Failed {
                return self.create_webrtc_channel().await;
            }
        }

        Err("No transport available".to_string())
    }

    /// Try to acquire a channel from a pool
    async fn acquire_from_pool(
        &self,
        pool: &Arc<Mutex<Vec<Arc<DataChannelWrapper>>>>,
    ) -> Option<Arc<DataChannelWrapper>> {
        let mut pool = pool.lock().await;
        while let Some(ch) = pool.pop() {
            if ch.is_open().await {
                tracing::debug!("{} reusing pooled channel: {}", self.who(), ch.label());
                return Some(ch);
            }
            // Channel closed while in pool, discard
        }
        None
    }

    /// Create a new WebRTC data channel
    async fn create_webrtc_channel(&self) -> Result<Arc<DataChannelWrapper>, String> {
        let pc = self.pc().await.ok_or("WebRTC not available")?;

        let channel_label = {
            let mut id = self.channel_id.lock().await;
            let label = format!("ch-{}", *id);
            *id += 1;
            label
        };

        let enc_key = *self.encryption_key.lock().await;
        let enc_config = enc_key.map(|key| EncryptionConfig {
            key,
            is_client: self.is_initiator,
        });

        tracing::debug!(
            "{} creating WebRTC channel: {} (encrypted: {})",
            self.who(),
            channel_label,
            enc_config.is_some()
        );

        let dc = pc
            .create_data_channel(&channel_label, None)
            .await
            .map_err(|e| e.to_string())?;

        let wrapper = Arc::new(DataChannelWrapper::new(Arc::new(dc), enc_config, Some(self.bulk_gate.clone())).await?);
        wrapper.wait_for_open(10000).await?;

        Ok(wrapper)
    }

    /// Create a new relay data channel
    async fn create_relay_channel(&self) -> Result<Arc<DataChannelWrapper>, String> {
        let relay_config = self.relay_config.as_ref().ok_or("No relay config")?;

        let url = if self.is_initiator {
            &relay_config.send_url
        } else {
            &relay_config.receive_url
        };

        let label = {
            let mut id = self.relay_channel_id.lock().await;
            let label = format!("relay-{}", *id);
            *id += 1;
            label
        };

        tracing::debug!("{} creating relay channel: {}", self.who(), label);

        // Tell the other side to open a matching relay channel
        let signal = serde_json::json!({
            "type": "new-channel",
            "ch": label
        });
        self.send_signal(&signal).await;

        tracing::debug!("{} signal sent, connecting relay: {}", self.who(), label);

        // Connect to relay — server blocks until both sides connect
        let transport = connect_relay(url, &label, RELAY_CONNECT_TIMEOUT_MS).await?;

        tracing::debug!("{} relay channel connected: {}", self.who(), label);

        let enc_key = *self.encryption_key.lock().await;
        let enc_config = enc_key.map(|key| EncryptionConfig {
            key,
            is_client: self.is_initiator,
        });

        let wrapper = Arc::new(DataChannelWrapper::from_relay(transport, enc_config).await);

        Ok(wrapper)
    }

    /// Release a channel back to the pool, or close it if pool is full.
    /// If WebRTC is now available and this is a relay channel, discard it.
    pub async fn release_channel(&self, channel: Arc<DataChannelWrapper>) {
        if !channel.is_open().await {
            return; // Already closed
        }

        // Check encryption matches
        let is_encrypted = self.is_encrypted().await;
        if channel.is_encrypted() != is_encrypted {
            tracing::debug!("encrypted != encrypted, closing channel");
            channel.close().await;
            return;
        }

        let is_relay = Self::is_relay_channel(&channel);
        let webrtc_connected = *self.webrtc_connected.lock().await;

        // If WebRTC is up and this is a relay channel, don't re-pool it
        if is_relay && webrtc_connected {
            tracing::debug!(
                "{} discarding relay channel (WebRTC available): {}",
                self.who(),
                channel.label()
            );
            channel.close().await;
            return;
        }

        let pool = if is_relay {
            &self.relay_channel_pool
        } else {
            &self.channel_pool
        };

        let mut pool = pool.lock().await;
        if pool.len() < MAX_POOL_SIZE {
            tracing::debug!("{} returning channel to pool: {}", self.who(), channel.label());
            pool.push(channel);
        } else {
            tracing::debug!("{} pool full, closing channel: {}", self.who(), channel.label());
            channel.close().await;
        }
    }

    /// Connect as initiator (client) or responder (server)
    pub async fn connect(&mut self, is_initiator: bool, ice_servers: &[IceServer]) -> Result<(), String> {
        self.is_initiator = is_initiator;
        *self.state.write().await = WebRTCState::Connecting;

        tracing::debug!(
            "WebRTC connect, initiator: {}, ice_servers: {}",
            is_initiator,
            ice_servers.len()
        );

        let handler = Arc::new(BackendSignaledHandler {
            is_initiator,
            state: self.state.clone(),
            pc_state: self.pc_state.clone(),
            data_channel: self.data_channel.clone(),
            encryption_key: self.encryption_key.clone(),
            bulk_gate: self.bulk_gate.clone(),
            on_error: self.on_error.clone(),
            on_close: self.on_close.clone(),
            on_incoming_channel: self.on_incoming_channel.clone(),
            on_candidate_change: self.on_candidate_change.clone(),
            on_connect: self.on_connect.clone(),
            on_message: self.on_message.clone(),
        });

        let pc = build_peer_connection(ice_servers, handler).await?;
        *self.peer_connection.lock().await = Some(pc.clone());

        if is_initiator {
            // Create data channel as initiator
            let dc = pc.create_data_channel("data", None).await.map_err(|e| e.to_string())?;

            self.setup_data_channel(dc).await;

            // Create offer
            let offer = pc.create_offer(None).await.map_err(|e| e.to_string())?;
            pc.set_local_description(offer.clone())
                .await
                .map_err(|e| e.to_string())?;

            tracing::debug!("WebRTC SDP offer (local):\n{}", offer.sdp);

            if let Some(ref callback) = self.on_offer_create {
                callback(offer.sdp);
            }
        }
        // Responder: the remote's data channel arrives via the handler's on_data_channel.

        Ok(())
    }

    async fn setup_data_channel(&mut self, dc: Arc<dyn DataChannel>) {
        *self.data_channel.lock().await = Some(dc.clone());
        spawn_legacy_data_channel_events(
            dc,
            self.state.clone(),
            self.on_message.clone(),
            self.on_connect.clone(),
            self.on_close.clone(),
            self.on_error.clone(),
        );
    }

    /// Apply a remote SDP offer (as responder)
    pub async fn apply_offer(&mut self, offer: &SessionDescription) -> Result<(), String> {
        tracing::debug!("Applying remote offer; WebRTC SDP offer (remote):\n{}", offer.sdp);

        let pc = self.pc().await.ok_or("No peer connection")?;

        let rtc_offer = RTCSessionDescription::offer(offer.sdp.clone()).map_err(|e| e.to_string())?;

        pc.set_remote_description(rtc_offer).await.map_err(|e| e.to_string())?;

        // Mark remote description as set and flush pending candidates
        *self.has_remote_description.lock().await = true;
        self.flush_pending_candidates().await?;

        // Create answer
        let answer = pc.create_answer(None).await.map_err(|e| e.to_string())?;
        pc.set_local_description(answer.clone())
            .await
            .map_err(|e| e.to_string())?;

        tracing::debug!("WebRTC SDP answer (local):\n{}", answer.sdp);

        if let Some(ref callback) = self.on_answer_create {
            callback(answer.sdp);
        }

        Ok(())
    }

    /// Apply a remote SDP answer (as initiator)
    pub async fn apply_answer(&mut self, answer: &SessionDescription) -> Result<(), String> {
        tracing::debug!("Applying remote answer; WebRTC SDP answer (remote):\n{}", answer.sdp);

        let pc = self.pc().await.ok_or("No peer connection")?;

        let rtc_answer = RTCSessionDescription::answer(answer.sdp.clone()).map_err(|e| e.to_string())?;

        pc.set_remote_description(rtc_answer).await.map_err(|e| e.to_string())?;

        // Mark remote description as set and flush pending candidates
        *self.has_remote_description.lock().await = true;
        self.flush_pending_candidates().await?;

        Ok(())
    }

    /// Add a remote ICE candidate
    pub async fn add_candidate(&self, candidate: &IceCandidate) -> Result<(), String> {
        if candidate.candidate.is_empty() {
            tracing::debug!("Received end-of-candidates");
            return Ok(());
        }

        // Queue candidate if remote description not yet set
        if !*self.has_remote_description.lock().await {
            tracing::debug!(
                "Queuing ICE candidate (no remote description yet): {}",
                candidate.candidate
            );
            self.pending_candidates.lock().await.push(candidate.clone());
            return Ok(());
        }

        tracing::debug!("Adding ICE candidate: {}", candidate.candidate);

        let pc = self.pc().await.ok_or("No peer connection")?;

        let rtc_candidate = RTCIceCandidateInit {
            candidate: candidate.candidate.clone(),
            sdp_mid: candidate.sdp_mid.clone(),
            sdp_mline_index: candidate.sdp_m_line_index,
            ..Default::default()
        };

        pc.add_ice_candidate(rtc_candidate).await.map_err(|e| e.to_string())?;

        Ok(())
    }

    /// Flush pending ICE candidates after remote description is set
    async fn flush_pending_candidates(&self) -> Result<(), String> {
        let candidates: Vec<IceCandidate> = {
            let mut pending = self.pending_candidates.lock().await;
            std::mem::take(&mut *pending)
        };

        if !candidates.is_empty() {
            tracing::debug!("Flushing {} pending ICE candidates", candidates.len());
        }

        let pc = self.pc().await.ok_or("No peer connection")?;

        for candidate in candidates {
            let rtc_candidate = RTCIceCandidateInit {
                candidate: candidate.candidate.clone(),
                sdp_mid: candidate.sdp_mid.clone(),
                sdp_mline_index: candidate.sdp_m_line_index,
                ..Default::default()
            };

            pc.add_ice_candidate(rtc_candidate).await.map_err(|e| e.to_string())?;
        }

        Ok(())
    }

    /// Check if ready to send (buffer not full)
    pub async fn is_ready_to_send(&self) -> bool {
        if let Some(dc) = self.data_channel.lock().await.as_ref() {
            // `Err` = channel gone from the core: not ready.
            dc.outstanding_bytes().await.map(|n| n < BUFFERED_MAX).unwrap_or(false)
        } else {
            false
        }
    }

    /// Send data over the data channel
    pub async fn send(&self, data: &[u8]) -> Result<(), String> {
        let state = *self.state.read().await;
        if state != WebRTCState::Connected {
            return Err("Not connected".to_string());
        }

        let dc = self.data_channel.lock().await;
        let dc = dc.as_ref().ok_or("No data channel")?;

        dc.send(BytesMut::from(data)).await.map_err(|e| e.to_string())?;

        Ok(())
    }

    /// Queue data for sending (handles backpressure)
    pub async fn send_queued(&self, data: &[u8]) -> Result<(), String> {
        // If buffer is not full and queue is empty, send directly
        if self.is_ready_to_send().await {
            let queue = self.send_queue.lock().await;
            if queue.is_empty() {
                drop(queue);
                return self.send(data).await;
            }
        }

        // Otherwise queue it
        self.send_queue.lock().await.push(data.to_vec());

        // Try to flush queue
        self.flush_queue().await
    }

    /// Flush the send queue
    async fn flush_queue(&self) -> Result<(), String> {
        while self.is_ready_to_send().await {
            let mut queue = self.send_queue.lock().await;
            if queue.is_empty() {
                break;
            }
            let data = queue.remove(0);
            drop(queue);
            self.send(&data).await?;
        }
        Ok(())
    }

    /// Close the connection and clean up all resources
    pub async fn close(&self) {
        tracing::debug!("{} closing connection", self.who());
        *self.closed.lock().await = true;

        // Close all pooled WebRTC channels
        {
            let mut pool = self.channel_pool.lock().await;
            for ch in pool.drain(..) {
                ch.close().await;
            }
        }

        // Close all pooled relay channels
        {
            let mut pool = self.relay_channel_pool.lock().await;
            for ch in pool.drain(..) {
                ch.close().await;
            }
        }

        // Close probe channel
        {
            let mut probe = self.probe_channel.lock().await;
            if let Some(ref channel) = *probe {
                channel.close().await;
            }
            *probe = None;
        }

        *self.relay_connected.lock().await = false;
        *self.webrtc_connected.lock().await = false;

        // Close WebRTC peer connection
        if let Some(pc) = self.peer_connection.lock().await.take() {
            let _ = pc.close().await;
        }

        *self.state.write().await = WebRTCState::Closed;

        if let Some(ref callback) = self.on_close {
            callback();
        }
    }

    pub async fn state(&self) -> WebRTCState {
        *self.state.read().await
    }

    pub async fn is_connected(&self) -> bool {
        *self.state.read().await == WebRTCState::Connected
    }
}

impl Default for WebRTCConnector {
    fn default() -> Self {
        Self::new()
    }
}

/// Helper to parse SDP from JSON message
#[allow(dead_code)]
pub fn parse_sdp_from_json(msg: &serde_json::Value) -> Option<SessionDescription> {
    Some(SessionDescription {
        sdp_type: msg["type"].as_str()?.to_string(),
        sdp: msg["sdp"].as_str()?.to_string(),
    })
}

/// Helper to parse ICE candidate from JSON message
#[allow(dead_code)]
pub fn parse_candidate_from_json(msg: &serde_json::Value) -> Option<IceCandidate> {
    Some(IceCandidate {
        candidate: msg["sdp"].as_str().or(msg["candidate"].as_str())?.to_string(),
        sdp_mid: msg["mid"].as_str().or(msg["sdpMid"].as_str()).map(|s| s.to_string()),
        sdp_m_line_index: msg["mline"]
            .as_u64()
            .or(msg["sdpMLineIndex"].as_u64())
            .map(|n| n as u16),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::p2p::test_support::FakeDataChannel;

    fn ice(uri: &str) -> IceServer {
        IceServer {
            uri: uri.into(),
            username: "u".into(),
            password: "p".into(),
        }
    }

    /// TURN entries are dropped (the coturn 403-on-loopback stall); STUN stays.
    #[test]
    fn stun_only_filters_out_turn() {
        let servers = vec![
            ice("stun:stun.example.com:3478"),
            ice("turn:turn.example.com:3478?transport=udp"),
            ice("turns:turn.example.com:5349"),
            ice("stuns:stun.example.com:5349"),
        ];
        let out = stun_only_ice_servers(&servers);
        let urls: Vec<&str> = out.iter().flat_map(|s| s.urls.iter().map(String::as_str)).collect();
        assert_eq!(urls, ["stun:stun.example.com:3478", "stuns:stun.example.com:5349"]);
    }

    #[test]
    fn parse_helpers_accept_both_key_styles() {
        let sdp = parse_sdp_from_json(&serde_json::json!({"type": "offer", "sdp": "v=0"})).unwrap();
        assert_eq!(sdp.sdp_type, "offer");
        assert_eq!(sdp.sdp, "v=0");
        assert!(parse_sdp_from_json(&serde_json::json!({"type": "offer"})).is_none());

        let relay_style =
            parse_candidate_from_json(&serde_json::json!({"sdp": "cand", "mid": "0", "mline": 1})).unwrap();
        assert_eq!(relay_style.candidate, "cand");
        assert_eq!(relay_style.sdp_mid.as_deref(), Some("0"));
        assert_eq!(relay_style.sdp_m_line_index, Some(1));

        let browser_style =
            parse_candidate_from_json(&serde_json::json!({"candidate": "cand2", "sdpMid": "1", "sdpMLineIndex": 2}))
                .unwrap();
        assert_eq!(browser_style.candidate, "cand2");
        assert_eq!(browser_style.sdp_mid.as_deref(), Some("1"));
        assert_eq!(browser_style.sdp_m_line_index, Some(2));
    }

    async fn wrapper(label: &str) -> Arc<DataChannelWrapper> {
        Arc::new(
            DataChannelWrapper::new(FakeDataChannel::single(label), None, None)
                .await
                .unwrap(),
        )
    }

    #[tokio::test]
    async fn release_drops_closed_channel() {
        let conn = WebRTCConnector::new();
        let ch = wrapper("ch-0").await;
        ch.close().await;
        conn.release_channel(ch).await;
        assert!(conn.channel_pool.lock().await.is_empty());
    }

    /// A channel whose encryption doesn't match the connector's current key
    /// state is closed, never pooled (it would decrypt nothing / leak plaintext).
    #[tokio::test]
    async fn release_closes_on_encryption_mismatch() {
        let conn = WebRTCConnector::new(); // no key
        let enc = EncryptionConfig {
            key: [5u8; 32],
            is_client: true,
        };
        let ch = Arc::new(
            DataChannelWrapper::new(FakeDataChannel::single("ch-0"), Some(enc), None)
                .await
                .unwrap(),
        );
        conn.release_channel(ch.clone()).await;
        assert!(conn.channel_pool.lock().await.is_empty());
        assert!(!ch.is_open().await, "mismatched channel must be closed");
    }

    /// A relay channel is only worth pooling while relay is the best transport:
    /// once WebRTC is up it is discarded; before that it is pooled.
    #[tokio::test]
    async fn release_discards_relay_channel_once_webrtc_is_up() {
        let conn = WebRTCConnector::new();

        let ch = wrapper("relay-0").await;
        conn.release_channel(ch.clone()).await;
        assert_eq!(conn.relay_channel_pool.lock().await.len(), 1, "pooled while relay-only");

        let ch2 = wrapper("relay-1").await;
        *conn.webrtc_connected.lock().await = true;
        conn.release_channel(ch2.clone()).await;
        assert_eq!(
            conn.relay_channel_pool.lock().await.len(),
            1,
            "not pooled with WebRTC up"
        );
        assert!(!ch2.is_open().await);
    }

    #[tokio::test]
    async fn release_pool_cap_closes_overflow() {
        let conn = WebRTCConnector::new();
        for i in 0..MAX_POOL_SIZE {
            conn.release_channel(wrapper(&format!("ch-{i}")).await).await;
        }
        assert_eq!(conn.channel_pool.lock().await.len(), MAX_POOL_SIZE);

        let overflow = wrapper("ch-10").await;
        conn.release_channel(overflow.clone()).await;
        assert_eq!(conn.channel_pool.lock().await.len(), MAX_POOL_SIZE);
        assert!(!overflow.is_open().await, "overflow channel must be closed");
    }

    #[tokio::test]
    async fn acquire_reuses_pooled_channel_and_skips_dead_ones() {
        let conn = WebRTCConnector::new();
        *conn.webrtc_connected.lock().await = true;

        let live = wrapper("ch-0").await;
        let dead = wrapper("ch-1").await;
        dead.close().await;
        // pop() takes from the end: the dead one is tried first and skipped.
        conn.channel_pool.lock().await.push(live.clone());
        conn.channel_pool.lock().await.push(dead);

        let got = conn.acquire_channel().await.unwrap();
        assert_eq!(got.label(), "ch-0");
        assert!(
            conn.channel_pool.lock().await.is_empty(),
            "dead channel discarded, live one taken"
        );
    }

    #[tokio::test]
    async fn acquire_fails_when_closed_or_signaling_dead() {
        let conn = WebRTCConnector::new();
        *conn.closed.lock().await = true;
        assert_eq!(conn.acquire_channel().await.err().unwrap(), "Connection is closed");

        // Probe was up once and is gone, WebRTC never connected: fail fast
        // instead of hanging upper layers on a channel that can't negotiate.
        let conn = WebRTCConnector::new();
        *conn.probe_ever_connected.lock().await = true;
        assert_eq!(conn.acquire_channel().await.err().unwrap(), "No transport available");
    }
}
