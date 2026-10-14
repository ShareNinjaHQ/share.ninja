//! WebSocket connection handling for backend communication
//!
//! Core does **no** typed dispatch of upstream frames: it forwards every frame
//! verbatim to the `raw_message_handler`, and each subscriber parses the
//! `share_ninja_api::WsMessage` envelope itself (`serde_json::from_str::<WsMessage>`)
//! and acts on the variants it cares about. The only state we surface directly
//! is the connection lifecycle (`state_change_handler`), which is not a
//! `WsMessage` and is owned by the connection, not the server.

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use serde::Serialize;
use tokio::net::TcpStream;
use tokio::sync::{RwLock, mpsc};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

/// Interval between client-side keepalive pings.
const PING_INTERVAL: Duration = Duration::from_secs(15);

/// Host part of a ws(s) URL, without port ("wss://h.example:443/ws?x" → "h.example").
fn url_host(url: &str) -> Option<&str> {
    let rest = url.split("://").nth(1)?;
    let authority = rest.split('/').next()?;
    Some(authority.split(':').next().unwrap_or(authority))
}
/// If no message (including pong) is received within this duration, consider the connection dead.
const PONG_TIMEOUT: Duration = Duration::from_secs(30);

/// Bound on the whole dial (TCP + TLS + HTTP upgrade). A peer that accepts
/// the socket and never answers fails here instead of parking the register
/// handshake at `Connecting` for good (docs/app/reconnect.md, change set 5).
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);

use super::BackState;

/// Connection-state transitions (notably `NoConnection` on socket close /
/// read error / keepalive timeout).
pub type StateChangeHandler = Arc<dyn Fn(BackState) + Send + Sync>;
/// Fires for every upstream WS frame, with the raw text the server sent.
/// The subscriber parses the `share_ninja_api::WsMessage` envelope itself.
pub type RawMessageHandler = Arc<dyn Fn(&str) + Send + Sync>;

/// WebSocket event handlers
#[derive(Default, Clone)]
pub struct WsHandlers {
    pub state_change_handler: Option<StateChangeHandler>,
    pub raw_message_handler: Option<RawMessageHandler>,
}

impl WsHandlers {
    pub fn new() -> Self {
        Self::default()
    }
}

/// WebSocket connection manager
pub struct WsConnection {
    sender: mpsc::Sender<Message>,
    /// The read loop, aborted on drop: a replaced (or abandoned) socket must
    /// not report its own death as ours — `NoConnection` is only ever the
    /// current socket's verdict.
    read_task: tokio::task::JoinHandle<()>,
}

impl Drop for WsConnection {
    fn drop(&mut self) {
        self.read_task.abort();
    }
}

impl WsConnection {
    /// Connect to WebSocket server. `connect_override` is the harness hook
    /// from [`super::BackParams`]: dial that address for the URL's host while
    /// TLS keeps validating the real hostname.
    pub async fn connect(
        base_url: &str,
        ticket: &str,
        handlers: Arc<RwLock<WsHandlers>>,
        connect_override: Option<&(String, std::net::SocketAddr)>,
    ) -> Result<Self, WsError> {
        let ws_base = base_url
            .replacen("https://", "wss://", 1)
            .replacen("http://", "ws://", 1);
        let url = format!("{}/ws/connect?ticket={}", ws_base, ticket);
        // The ticket is a bearer for the socket: the log gets the URL without it.
        tracing::info!("Backend WS: connecting to {}/ws/connect", ws_base);

        let dial = async {
            match connect_override {
                Some((host, addr)) if url_host(&url) == Some(host.as_str()) => {
                    let tcp = TcpStream::connect(addr)
                        .await
                        .map_err(|e| WsError::Connection(e.to_string()))?;
                    tokio_tungstenite::client_async_tls(&url, tcp)
                        .await
                        .map_err(WsError::from_handshake)
                }
                _ => connect_async(&url).await.map_err(WsError::from_handshake),
            }
        };
        let (ws_stream, _response) = tokio::time::timeout(CONNECT_TIMEOUT, dial)
            .await
            .map_err(|_| WsError::Connection(format!("connect timed out after {}s", CONNECT_TIMEOUT.as_secs())))??;

        tracing::info!("Backend WS: connected");

        let (write, read) = ws_stream.split();
        let (tx, rx) = mpsc::channel::<Message>(100);

        let ping_tx = tx.clone();

        // Spawn write task
        tokio::spawn(Self::write_loop(write, rx));

        // Spawn read task with keepalive ping sender
        let read_task = tokio::spawn(Self::read_loop(read, handlers, ping_tx));

        // NOTE: we deliberately do NOT transition Back state here. Mirroring
        // TS (`ws.onopen` only logs "Connected to server"), the connection
        // state machine is owned entirely by `run_register`, which sets
        // Online / OnlineAndLoggedIn after the WS + `GET /me` complete. The
        // WS only reports disconnects (NoConnection on close, below).

        Ok(Self { sender: tx, read_task })
    }

    /// Send a message over WebSocket
    pub async fn send(&self, msg: &impl Serialize) -> Result<(), WsError> {
        let json = serde_json::to_string(msg).map_err(|e| WsError::Serialization(e.to_string()))?;
        tracing::debug!("Sending WS message: {}", json);
        self.sender
            .send(Message::Text(json.into()))
            .await
            .map_err(|e| WsError::Send(e.to_string()))
    }

    /// Send raw JSON value
    pub async fn send_json(&self, value: serde_json::Value) -> Result<(), WsError> {
        let json = serde_json::to_string(&value).map_err(|e| WsError::Serialization(e.to_string()))?;
        tracing::debug!("Sending WS message: {}", json);
        self.sender
            .send(Message::Text(json.into()))
            .await
            .map_err(|e| WsError::Send(e.to_string()))
    }

    async fn write_loop(
        mut write: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        mut rx: mpsc::Receiver<Message>,
    ) {
        while let Some(msg) = rx.recv().await {
            if let Err(e) = write.send(msg).await {
                tracing::error!("Backend WS: write error: {}", e);
                break;
            }
        }
        tracing::info!("Backend WS: write loop ended");
    }

    async fn read_loop(
        mut read: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
        handlers: Arc<RwLock<WsHandlers>>,
        ping_tx: mpsc::Sender<Message>,
    ) {
        let mut last_activity = Instant::now();
        let mut ping_interval = tokio::time::interval(PING_INTERVAL);
        ping_interval.tick().await; // consume the immediate first tick

        loop {
            tokio::select! {
                msg_opt = read.next() => {
                    match msg_opt {
                        Some(Ok(Message::Text(text))) => {
                            last_activity = Instant::now();
                            Self::handle_message(&text, &handlers).await;
                        }
                        Some(Ok(Message::Binary(data))) => {
                            last_activity = Instant::now();
                            if let Ok(text) = String::from_utf8(data.to_vec()) {
                                Self::handle_message(&text, &handlers).await;
                            }
                        }
                        Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => {
                            last_activity = Instant::now();
                        }
                        Some(Ok(Message::Close(_))) => {
                            tracing::warn!("Backend WS: closed by server");
                            break;
                        }
                        Some(Ok(Message::Frame(_))) => {
                            last_activity = Instant::now();
                        }
                        Some(Err(e)) => {
                            tracing::error!("Backend WS: read error: {}", e);
                            break;
                        }
                        None => {
                            tracing::warn!("Backend WS: stream ended");
                            break;
                        }
                    }
                }
                _ = ping_interval.tick() => {
                    if last_activity.elapsed() > PONG_TIMEOUT {
                        tracing::error!(
                            "Backend WS: keepalive timeout ({:.0}s without activity)",
                            last_activity.elapsed().as_secs_f64(),
                        );
                        break;
                    }
                    if ping_tx.send(Message::Ping(vec![].into())).await.is_err() {
                        tracing::warn!("Backend WS: ping send failed (write loop closed)");
                        break;
                    }
                }
            }
        }

        tracing::info!("Backend WS: read loop ended");

        // Notify state change to NoConnection
        let h = handlers.read().await;
        if let Some(ref handler) = h.state_change_handler {
            handler(BackState::NoConnection);
        }
    }

    /// Forward the raw frame to the subscriber. Subscribers parse the
    /// `share_ninja_api::WsMessage` envelope themselves and dispatch on the
    /// variants they care about — core does no typed dispatch.
    async fn handle_message(text: &str, handlers: &Arc<RwLock<WsHandlers>>) {
        tracing::debug!("Backend WS: received message: {}", text);
        let h = handlers.read().await;
        if let Some(ref handler) = h.raw_message_handler {
            handler(text);
        }
    }
}

/// WebSocket errors
#[derive(Debug, thiserror::Error)]
pub enum WsError {
    #[error("Connection error: {0}")]
    Connection(String),

    /// The handshake answered 429; the backend's `Retry-After` when it parsed.
    #[error("Connection error: HTTP error: 429 Too Many Requests")]
    RateLimited(Option<std::time::Duration>),

    #[error("Send error: {0}")]
    Send(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Not connected")]
    NotConnected,
}

impl WsError {
    /// A failed WS handshake: a 429 keeps its `Retry-After` for the reconnect
    /// loop, anything else is the connection error it was.
    fn from_handshake(e: tokio_tungstenite::tungstenite::Error) -> Self {
        if let tokio_tungstenite::tungstenite::Error::Http(resp) = &e
            && resp.status() == http::StatusCode::TOO_MANY_REQUESTS
        {
            return WsError::RateLimited(super::debug_middleware::parse_retry_after(
                resp.headers().get(http::header::RETRY_AFTER),
            ));
        }
        WsError::Connection(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use share_ninja_api::models::{WsMessage, WsNearby};

    #[test]
    fn test_ws_nearby_direct_parse() {
        // Direct struct parse works - the type field is present
        let json = r#"{"type":"nearby.updated","id":"evt_19e3984e6bf000069a13704","topic":"presence:agt_x","timestamp":"2026-05-18T09:48:28.485Z","nearby":[]}"#;
        let result = serde_json::from_str::<WsNearby>(json);
        assert!(result.is_ok(), "Direct parse failed: {}", result.unwrap_err());
    }

    #[test]
    fn test_ws_nearby_via_enum() {
        // Via tagged enum - serde strips the tag, the #22018 patch repopulates it via Default
        let json = r#"{"type":"nearby.updated","id":"evt_19e3984e6bf000069a13704","topic":"presence:agt_x","timestamp":"2026-05-18T09:48:28.485Z","nearby":[]}"#;
        let result = serde_json::from_str::<WsMessage>(json);
        assert!(result.is_ok(), "Enum parse failed: {}", result.unwrap_err());
    }

    #[test]
    fn test_ws_event_link_transfer_started() {
        // Flat envelope: type is the dotted discriminator, fields live directly on the variant.
        let json = r#"{"type":"link.transfer_started","id":"evt_19e3984e6bf000069a13704","topic":"link:67r3452q","timestamp":"2026-05-18T09:48:28.485Z","transfer_id":"tr_xyz789","direction":"upload","actor_id":"agt_sender01","actor":{"agent_id":"agt_sender01","display_name":"Tester","device":{"os":"mac","model":"MacBookPro18,1"}},"link":{"url_token":"67r3452q","created_at":"2026-05-18T09:48:28.485Z","invalidation_at":"2026-05-25T09:48:28.485Z","estimated_bytes":0}}"#;
        let result = serde_json::from_str::<WsMessage>(json);
        assert!(result.is_ok(), "Event parse failed: {}", result.unwrap_err());
    }

    #[test]
    fn test_ws_event_link_p2p_online() {
        let json = r#"{"type":"link.p2p_online","id":"evt_19e3984e6bf000069a13704","topic":"link:67r3452q","timestamp":"2026-05-18T09:48:28.485Z","agent_id":"agt_x","online":true,"online_agents":["agt_x"]}"#;
        let result = serde_json::from_str::<WsMessage>(json);
        assert!(result.is_ok(), "P2P online parse failed: {}", result.unwrap_err());
    }

    #[test]
    fn test_ws_share_answer_reject_preserves_offer_token() {
        // The reported bug: rejecting a share offer surfaces an EMPTY offer_token
        // to the sender. Prove whether the internally-tagged WsMessage enum drops
        // offer_token while buffering the Content to read the `type` tag.
        let json = r#"{"type":"share.answered","id":"evt_19e3984e6bf000069a13704","topic":"agent:agt_sender01","timestamp":"2026-05-18T09:48:28.485Z","offer_token":"shr_nae0kePa1234zoo2Fahf5678","answer":"reject"}"#;
        let result = serde_json::from_str::<WsMessage>(json);
        assert!(result.is_ok(), "share.answered parse failed: {}", result.unwrap_err());
        match result.unwrap() {
            WsMessage::ShareAnswered(data) => {
                assert_eq!(
                    data.offer_token, "shr_nae0kePa1234zoo2Fahf5678",
                    "offer_token was lost through the tagged-enum Content buffer"
                );
            }
            other => panic!("expected ShareAnswered, got {:?}", other),
        }
    }
}
