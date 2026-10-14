//! Relay transport - WebSocket-based transport for relay server connections
//!
//! This module provides a WebSocket transport that connects to a relay server,
//! allowing P2P communication when direct WebRTC connections fail.

use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore, mpsc};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

// Byte budget for outstanding (queued + in-flight) send payload before `send()`
// back-pressures. Mirrors TS Transport.ts's `bufferedAmount` watermark (TS bounds
// the relay by `BUFFERED_MAX` bytes). tokio-tungstenite exposes no `bufferedAmount`,
// so we account payload bytes ourselves with a semaphore — the faithful adaptation
// the merge plan calls for, byte-based rather than message-count-based.
const BUFFERED_MAX: usize = 15_000_000;

/// Relay configuration containing send and receive URLs
#[derive(Debug, Clone)]
pub struct RelayConfig {
    pub send_url: String,
    pub receive_url: String,
}

/// WebSocket transport state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportState {
    Connecting,
    Open,
    Closing,
    Closed,
}

/// WebSocket transport wrapper for relay connections
///
/// Wraps a WebSocket connection to look like a data channel transport.
/// One WebSocketTransport = one logical channel.
pub struct WebSocketTransport {
    label: String,
    state: Arc<Mutex<TransportState>>,
    // Each queued message carries the byte permit it reserved; the writer task drops
    // it once the frame is flushed, releasing the bytes. Unbounded by message count —
    // entry is gated by `send_permits` (bytes), so the queue holds <= BUFFERED_MAX of payload.
    send_tx: mpsc::UnboundedSender<(Vec<u8>, OwnedSemaphorePermit)>,
    recv_rx: Arc<Mutex<mpsc::Receiver<Vec<u8>>>>,
    close_tx: mpsc::Sender<()>,
    // Byte-based send backpressure: `BUFFERED_MAX` permits == bytes of buffer budget.
    send_permits: Arc<Semaphore>,
}

impl WebSocketTransport {
    /// Get the label for this transport
    pub fn label(&self) -> &str {
        &self.label
    }

    /// Get the current state
    pub async fn state(&self) -> TransportState {
        *self.state.lock().await
    }

    /// Check if the transport is open
    pub async fn is_open(&self) -> bool {
        *self.state.lock().await == TransportState::Open
    }

    /// Send data over the transport
    pub async fn send(&self, data: &[u8]) -> Result<(), String> {
        if *self.state.lock().await != TransportState::Open {
            return Err("Transport not open".to_string());
        }

        // Byte-based backpressure: reserve `data.len()` bytes of buffer budget before
        // queuing. This awaits here once outstanding payload reaches BUFFERED_MAX,
        // mirroring TS's `bufferedAmount` watermark. Clamp so a single message larger
        // than the whole budget can still make progress instead of deadlocking. The
        // writer task releases the bytes (drops the permit) after it flushes the frame.
        let n = data.len().min(BUFFERED_MAX) as u32;
        let permit = self
            .send_permits
            .clone()
            .acquire_many_owned(n)
            .await
            .map_err(|_| "Transport closed".to_string())?;

        self.send_tx
            .send((data.to_vec(), permit))
            .map_err(|e| format!("Send failed: {}", e))
    }

    /// Receive data from the transport
    pub async fn recv(&self) -> Option<Vec<u8>> {
        self.recv_rx.lock().await.recv().await
    }

    /// Receive data with timeout
    pub async fn recv_timeout(&self, timeout_ms: u64) -> Result<Vec<u8>, String> {
        use tokio::time::{Duration, timeout};

        let mut rx = self.recv_rx.lock().await;
        timeout(Duration::from_millis(timeout_ms), rx.recv())
            .await
            .map_err(|_| "Receive timeout".to_string())?
            .ok_or_else(|| "Channel closed".to_string())
    }

    /// Close the transport
    pub async fn close(&self) {
        let _ = self.close_tx.send(()).await;
        *self.state.lock().await = TransportState::Closed;
        // Wake any sender blocked acquiring buffer permits so it errors out.
        self.send_permits.close();
    }
}

/// Connect to a relay server and return a WebSocketTransport
///
/// The URL is constructed as: `{base_url}/{label}?{query}`
pub async fn connect_relay(url: &str, label: &str, timeout_ms: u64) -> Result<WebSocketTransport, String> {
    // Parse URL and append label
    let (base, query) = url.split_once('?').unwrap_or((url, ""));
    let full_url = if query.is_empty() {
        format!("{}/{}", base, urlencoding::encode(label))
    } else {
        format!("{}/{}?{}", base, urlencoding::encode(label), query)
    };

    tracing::debug!("Connecting relay: {} ({})", label, full_url);

    // Connect with timeout
    let connect_future = connect_async(&full_url);
    let (ws_stream, _response) = tokio::time::timeout(tokio::time::Duration::from_millis(timeout_ms), connect_future)
        .await
        .map_err(|_| format!("Relay connect timeout: {}", label))?
        .map_err(|e| format!("Relay connect failed: {}: {}", label, e))?;

    tracing::debug!("Relay connected: {}", label);

    let (write, read) = ws_stream.split();

    // Create channels for send/recv. Send is unbounded by message count but gated by
    // `send_permits` (bytes), so the queue holds at most BUFFERED_MAX of payload.
    let (send_tx, mut send_rx) = mpsc::unbounded_channel::<(Vec<u8>, OwnedSemaphorePermit)>();
    let (recv_tx, recv_rx) = mpsc::channel::<Vec<u8>>(100);
    let (close_tx, mut close_rx) = mpsc::channel::<()>(1);

    let send_permits = Arc::new(Semaphore::new(BUFFERED_MAX));
    let send_permits_writer = send_permits.clone();

    let state = Arc::new(Mutex::new(TransportState::Open));
    let state_clone = state.clone();
    let label_clone = label.to_string();

    // Spawn task to handle WebSocket I/O
    tokio::spawn(async move {
        use futures_util::stream::SplitSink;
        use futures_util::stream::SplitStream;

        let mut write: SplitSink<WsStream, Message> = write;
        let mut read: SplitStream<WsStream> = read;

        loop {
            tokio::select! {
                // Handle outgoing messages
                Some((data, permit)) = send_rx.recv() => {
                    let result = write.send(Message::Binary(data.into())).await;
                    // Release the reserved bytes now the frame is flushed to the socket.
                    drop(permit);
                    if let Err(e) = result {
                        tracing::error!("Relay send error on {}: {}", label_clone, e);
                        break;
                    }
                }

                // Handle incoming messages
                msg = read.next() => {
                    match msg {
                        Some(Ok(Message::Binary(data))) => {
                            if recv_tx.send(data.to_vec()).await.is_err() {
                                tracing::debug!("Relay recv channel closed: {}", label_clone);
                                break;
                            }
                        }
                        Some(Ok(Message::Close(_))) => {
                            tracing::debug!("Relay close received: {}", label_clone);
                            break;
                        }
                        Some(Ok(Message::Ping(data))) => {
                            let _ = write.send(Message::Pong(data)).await;
                        }
                        Some(Ok(_)) => {
                            // Ignore text, pong, etc.
                        }
                        Some(Err(e)) => {
                            tracing::error!("Relay read error on {}: {}", label_clone, e);
                            break;
                        }
                        None => {
                            tracing::debug!("Relay stream ended: {}", label_clone);
                            break;
                        }
                    }
                }

                // Handle close request
                _ = close_rx.recv() => {
                    tracing::debug!("Relay close requested: {}", label_clone);
                    let _ = write.send(Message::Close(None)).await;
                    break;
                }
            }
        }

        *state_clone.lock().await = TransportState::Closed;
        // Wake any sender blocked acquiring buffer permits so it errors out.
        send_permits_writer.close();
        tracing::debug!("Relay transport closed: {}", label_clone);
    });

    Ok(WebSocketTransport {
        label: label.to_string(),
        state,
        send_tx,
        recv_rx: Arc::new(Mutex::new(recv_rx)),
        close_tx,
        send_permits,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_relay_config() {
        let config = RelayConfig {
            send_url: "wss://relay.example.com/send".to_string(),
            receive_url: "wss://relay.example.com/recv".to_string(),
        };
        assert!(!config.send_url.is_empty());
        assert!(!config.receive_url.is_empty());
    }
}
