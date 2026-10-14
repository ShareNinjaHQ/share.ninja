//! DataChannelWrapper - Per-request DataChannel abstraction with encryption support
//!
//! Each request gets its own DataChannel. The channel itself is the request context.
//! Cancellation: just close the channel.
//!
//! Supports both WebRTC data channels and relay WebSocket transports.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use aes_gcm::{
    Aes256Gcm, Nonce,
    aead::{Aead, KeyInit},
};
use async_trait::async_trait;
use bytes::BytesMut;
use tokio::sync::{Mutex, Notify, mpsc};
use webrtc::data_channel::{DataChannel, DataChannelEvent, RTCDataChannelState};
use webrtc::error::Result as RtcResult;

use super::bulk_gate::BulkGate;
use super::relay_transport::WebSocketTransport;

const BUFFERED_MAX: usize = 10_000_000;
// Low watermark: the channel's `on_buffered_amount_low` fires when the SCTP send
// buffer drains below this, waking a sender blocked because `buffered_amount`
// reached `BUFFERED_MAX`. Mirrors TS DataChannel's `bufferedAmountLowThreshold`.
const BUFFERED_MIN: usize = 5_000_000;
const NONCE_SIZE: usize = 12;
const TAG_SIZE: usize = 16;
/// Default WebRTC data-channel max message size (bytes). webrtc-rs's default
/// `SctpMaxMessageSize` is `Bounded(64K)` (RFC 8841's value when no
/// `a=max-message-size` is present), so that is the negotiated SCTP limit on both
/// ends of a webrtc-rs↔webrtc-rs call. Sending more fails with SCTP
/// `ErrOutboundPacketTooLarge`. The connector may raise this via
/// `set_max_message_size` if a larger value is negotiated. Mirrors the browser's
/// `RTCSctpTransport.maxMessageSize` default.
pub(crate) const WEBRTC_DEFAULT_MAX_MESSAGE_SIZE: u64 = 65535;

// Nonce channel discriminator (bytes 8-11): one domain byte + 24-bit index.
// Every channel in a call shares ONE AES-GCM key (derived once from the call id),
// so the GCM nonce must be unique per channel as well as per message — otherwise
// two channels both start their counter at 0 and produce an identical (key, nonce)
// pair on different plaintext, breaking confidentiality and authenticity. Mirrors
// TS DataChannel's CHANNEL_DOMAIN_* / CHANNEL_INDEX_MAX.
const CHANNEL_DOMAIN_WEBRTC: u8 = 0;
const CHANNEL_DOMAIN_RELAY: u8 = 1;
const CHANNEL_INDEX_MAX: u32 = 0xFF_FFFF;

/// Derive the per-channel nonce discriminator `(domain, index)` from the channel
/// label. The label (`ch-N` / `relay-N`) is the only identifier both peers agree
/// on — the receiver derives the same discriminator from it (relay channels are
/// mirrored under the same label via the `new-channel` signal). Returns an error
/// for an underivable label rather than silently aliasing channel 0, which would
/// reintroduce nonce reuse. Mirrors TS `channelNonceDiscriminator`.
pub fn channel_nonce_discriminator(label: &str) -> Result<(u8, u32), String> {
    let underivable = || format!("Cannot derive nonce discriminator from channel label: \"{}\"", label);
    let (domain, rest) = if let Some(rest) = label.strip_prefix("ch-") {
        (CHANNEL_DOMAIN_WEBRTC, rest)
    } else if let Some(rest) = label.strip_prefix("relay-") {
        (CHANNEL_DOMAIN_RELAY, rest)
    } else {
        return Err(underivable());
    };
    // Require a pure-digit index: an empty or non-digit rest (e.g. `relay-probe`,
    // `ch-`, `ch-1.5`, `ch--1`) would otherwise alias channel 0 and reuse a nonce.
    if rest.is_empty() || !rest.bytes().all(|b| b.is_ascii_digit()) {
        return Err(underivable());
    }
    let index: u32 = rest.parse().map_err(|_| {
        format!(
            "Channel index out of nonce range (0..{}): \"{}\"",
            CHANNEL_INDEX_MAX, label
        )
    })?;
    if index > CHANNEL_INDEX_MAX {
        return Err(format!(
            "Channel index out of nonce range (0..{}): \"{}\"",
            CHANNEL_INDEX_MAX, label
        ));
    }
    Ok((domain, index))
}

/// Resolve the stored nonce discriminator at channel construction: `None` when
/// unencrypted; `Some` when encrypted and the label parses; `None` plus a loud
/// error when encrypted but the label is underivable (so `send()` refuses to send
/// rather than reuse a nonce). Mirrors TS's "derive once at construction".
fn derive_nonce_discriminator(label: &str, encrypted: bool) -> Option<(u8, u32)> {
    if !encrypted {
        return None;
    }
    match channel_nonce_discriminator(label) {
        Ok(d) => Some(d),
        Err(e) => {
            tracing::error!("DataChannel '{}': {} — encrypted channel will refuse to send", label, e);
            None
        }
    }
}

/// Encryption configuration for a channel
#[derive(Clone)]
pub struct EncryptionConfig {
    pub key: [u8; 32],
    pub is_client: bool,
}

/// The part of webrtc-rs's [`DataChannel`] the wrapper drives, with the same
/// signatures. webrtc-rs sealed `DataChannel` in 0.21, so the in-memory channel
/// the p2p tests run on (`test_support::FakeDataChannel`) can no longer
/// implement it; the wrapper talks to this trait instead and the real channel
/// is adapted below.
#[async_trait]
pub trait RtcChannel: Send + Sync + 'static {
    async fn label(&self) -> RtcResult<String>;
    async fn ready_state(&self) -> RtcResult<RTCDataChannelState>;
    async fn set_buffered_amount_low_threshold(&self, threshold: u32) -> RtcResult<()>;
    async fn outstanding_bytes(&self) -> RtcResult<usize>;
    async fn send(&self, data: BytesMut) -> RtcResult<()>;
    async fn poll(&self) -> Option<DataChannelEvent>;
    async fn close(&self) -> RtcResult<()>;
}

#[async_trait]
impl RtcChannel for Arc<dyn DataChannel> {
    async fn label(&self) -> RtcResult<String> {
        (**self).label().await
    }
    async fn ready_state(&self) -> RtcResult<RTCDataChannelState> {
        (**self).ready_state().await
    }
    async fn set_buffered_amount_low_threshold(&self, threshold: u32) -> RtcResult<()> {
        (**self).set_buffered_amount_low_threshold(threshold).await
    }
    async fn outstanding_bytes(&self) -> RtcResult<usize> {
        (**self).outstanding_bytes().await
    }
    async fn send(&self, data: BytesMut) -> RtcResult<()> {
        (**self).send(data).await
    }
    async fn poll(&self) -> Option<DataChannelEvent> {
        (**self).poll().await
    }
    async fn close(&self) -> RtcResult<()> {
        (**self).close().await
    }
}

/// Transport type - either WebRTC or Relay
enum TransportKind {
    WebRTC(Arc<dyn RtcChannel>),
    // `WebSocketTransport` is internally synchronized (all methods take `&self` and
    // route through channels), so it needs no outer `Mutex`. Wrapping it in one was
    // actively harmful: the background recv loop held the lock across a 100ms
    // `recv_timeout`, blocking every `send` behind it (and risking a bidirectional
    // backpressure deadlock). A bare `Arc` lets send and recv proceed concurrently.
    Relay(Arc<WebSocketTransport>),
}

/// Inbound queue. WebRTC channels use an UNBOUNDED queue on purpose: webrtc-rs
/// 0.20's driver drains SCTP into a fixed 256-event per-channel queue with
/// `try_send` and DROPS the message when it is full (no back-pressure reaches the
/// SCTP receive window any more — 0.17's awaited `on_message` callback used to
/// stall the reader and throttle the sender). Our transfer protocol streams a
/// whole range after one request with no app-level credit window, so a consumer
/// slower than the network (disk stall) would silently lose blocks. The poll task
/// therefore never blocks: it drains the driver's queue into this unbounded one,
/// trading bounded memory for loss-free delivery — the same position a browser's
/// `onmessage` is in. Relay keeps its bounded queue: the WebSocket transport
/// back-pressures properly.
enum RecvQueue {
    Bounded(mpsc::Receiver<Vec<u8>>),
    Unbounded(mpsc::UnboundedReceiver<Vec<u8>>),
}

impl RecvQueue {
    async fn recv(&mut self) -> Option<Vec<u8>> {
        match self {
            RecvQueue::Bounded(rx) => rx.recv().await,
            RecvQueue::Unbounded(rx) => rx.recv().await,
        }
    }
}

/// DataChannelWrapper provides send/recv abstraction over a WebRTC DataChannel or Relay
pub struct DataChannelWrapper {
    transport: TransportKind,
    label: String,
    closed: Arc<Mutex<bool>>,
    close_error: Arc<Mutex<Option<String>>>,
    /// Woken when the channel closes (or errors), so a `recv` parked on an
    /// indefinite wait observes the close instead of hanging forever. The sender
    /// half of the inbound queue lives in the background poll/recv task, which may
    /// outlive the remote close, so `rx.recv()` alone is not a reliable close
    /// signal; this is what lets `recv` give up. `notify_one` (not
    /// `notify_waiters`) so a close racing the gap between `recv`'s flag-check and
    /// its `notified()` is not lost.
    close_notify: Arc<Notify>,
    /// Stops the WebRTC event poll task from `close()`. The task normally ends on
    /// the channel's `OnClose`/end-of-events, but if the peer connection goes away
    /// without emitting one the task would otherwise park on `poll()` forever,
    /// pinning the `Arc<dyn DataChannel>` it holds. Unused for relay.
    poll_stop: Arc<Notify>,

    // Encryption
    encryption: Option<EncryptionConfig>,
    send_counter: Arc<Mutex<u64>>,
    /// Per-channel nonce discriminator `(domain, index)` derived once from the
    /// label. `Some` iff the channel is encrypted AND the label was derivable;
    /// `None` either when unencrypted or when an encrypted channel's label could
    /// not be parsed — in the latter case `send()` refuses to send rather than
    /// reuse a nonce. Mirrors TS DataChannel's `channelDomain`/`channelIndex`.
    nonce_discriminator: Option<(u8, u32)>,

    // Receive queue
    recv_rx: Arc<Mutex<RecvQueue>>,

    // WebRTC send backpressure: signalled by the channel's `OnBufferedAmountLow`
    // event when the SCTP buffer drains below `BUFFERED_MIN`, waking a sender
    // blocked because `outstanding_bytes` reached `BUFFERED_MAX`. Relay channels
    // back-pressure via a byte-bounded send semaphore instead and leave this idle.
    buffer_low_notify: Arc<Notify>,

    /// Largest single message (bytes, post-encryption) this transport accepts.
    /// WebRTC channels are bounded by the negotiated SCTP max message size (the
    /// connector sets it; default 65535) — sending more fails with SCTP
    /// `ErrOutboundPacketTooLarge`. Relay channels (WebSocket) are unbounded
    /// (`u64::MAX`). Senders that frame their own payloads (FileServer) clamp to
    /// `max_plaintext_size()`. Mirrors TS DataChannel's `maxMessageSize`.
    max_message_size: AtomicU64,

    /// The connection's bulk gate (see `BulkGate`): `send_bulk` waits on it,
    /// `send` never does. `None` for relay channels and in tests.
    gate: Option<Arc<BulkGate>>,
}

impl DataChannelWrapper {
    /// Create a wrapper for a WebRTC data channel. Spawns the event poll task that
    /// drains the channel's events (messages, open/close/error, buffer low-water).
    pub async fn new(
        channel: Arc<dyn RtcChannel>,
        encryption: Option<EncryptionConfig>,
        gate: Option<Arc<BulkGate>>,
    ) -> Result<Self, String> {
        if let Some(gate) = &gate {
            gate.register(channel.clone()).await;
        }
        let label = channel.label().await.map_err(|e| e.to_string())?;
        tracing::debug!(
            "Creating DataChannelWrapper for WebRTC channel '{}', state: {:?}, encrypted: {}",
            label,
            channel.ready_state().await,
            encryption.is_some()
        );
        let closed = Arc::new(Mutex::new(false));
        let close_error = Arc::new(Mutex::new(None));
        let close_notify = Arc::new(Notify::new());
        let poll_stop = Arc::new(Notify::new());
        let send_counter = Arc::new(Mutex::new(0u64));
        let buffer_low_notify = Arc::new(Notify::new());

        // Unbounded on purpose — see `RecvQueue`.
        let (recv_tx, recv_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let recv_rx = Arc::new(Mutex::new(RecvQueue::Unbounded(recv_rx)));

        let nonce_discriminator = derive_nonce_discriminator(&label, encryption.is_some());

        // Event-driven send backpressure (replaces the old 1ms busy-poll): wake a
        // blocked sender when the SCTP buffer drains below BUFFERED_MIN. Mirrors TS
        // DataChannel's `bufferedAmountLowThreshold = BUFFERED_MIN` + `onbufferedamountlow`.
        // Set before the poll task starts so no low-water event is missed.
        // The low event is edge-triggered (fires only on crossing the
        // threshold downward). Under a gate the connection's total is capped
        // far below BUFFERED_MIN, so a channel would never cross it and the
        // gate's waiters would never wake. Threshold 0 = "this channel
        // drained": while the connection sits at the cap some channel holds
        // data with its sender parked, so it does drain and wakes them all.
        let low_threshold = if gate.is_some() { 0 } else { BUFFERED_MIN as u32 };
        channel
            .set_buffered_amount_low_threshold(low_threshold)
            .await
            .map_err(|e| e.to_string())?;

        let wrapper = Self {
            transport: TransportKind::WebRTC(channel.clone()),
            label: label.clone(),
            closed: closed.clone(),
            close_error: close_error.clone(),
            close_notify: close_notify.clone(),
            poll_stop: poll_stop.clone(),
            encryption: encryption.clone(),
            send_counter,
            nonce_discriminator,
            recv_rx,
            buffer_low_notify: buffer_low_notify.clone(),
            // WebRTC channel: bounded by the negotiated SCTP max message size.
            // Default 65535; the connector may raise it via set_max_message_size.
            max_message_size: AtomicU64::new(WEBRTC_DEFAULT_MAX_MESSAGE_SIZE),
            gate: gate.clone(),
        };

        // Event poll task. webrtc-rs 0.20 delivers per-channel events through
        // `DataChannel::poll()` instead of callbacks; this task is the only consumer.
        // It must never block on downstream (unbounded queue) — see `RecvQueue`.
        let enc = encryption;
        tokio::spawn(async move {
            let mark_closed = |reason: String| {
                let closed = closed.clone();
                let close_error = close_error.clone();
                let buffer_low_notify = buffer_low_notify.clone();
                let close_notify = close_notify.clone();
                let gate = gate.clone();
                async move {
                    *closed.lock().await = true;
                    let mut err = close_error.lock().await;
                    if err.is_none() {
                        *err = Some(reason);
                    }
                    // Wake a sender blocked on backpressure so it observes the close.
                    buffer_low_notify.notify_one();
                    if let Some(gate) = &gate {
                        gate.wake();
                    }
                    // Wake a receiver parked on an indefinite recv so it observes the close.
                    close_notify.notify_one();
                }
            };
            loop {
                let event = tokio::select! {
                    biased;
                    _ = poll_stop.notified() => break,
                    ev = channel.poll() => ev,
                };
                match event {
                    Some(DataChannelEvent::OnMessage(msg)) => {
                        if msg.data.is_empty() {
                            tracing::debug!("DataChannel '{}' received empty message, ignoring", label);
                            continue;
                        }
                        let data = msg.data;
                        // Decrypt if encryption is enabled
                        let plaintext = if let Some(ref enc_config) = enc {
                            match decrypt_message(&data, enc_config) {
                                Ok(pt) => pt,
                                Err(e) => {
                                    // Mirror TS DataChannel: on decrypt failure, log and DROP the
                                    // message. Never deliver raw ciphertext as plaintext (it would be
                                    // parsed as a protocol message and corrupt the stream).
                                    tracing::error!(
                                        "DataChannel '{}' decrypt error: {} ({} bytes) - dropping message",
                                        label,
                                        e,
                                        data.len()
                                    );
                                    continue;
                                }
                            }
                        } else {
                            data.to_vec()
                        };
                        if let Err(e) = recv_tx.send(plaintext) {
                            tracing::error!("DataChannel '{}' failed to queue received message: {}", label, e);
                        }
                    }
                    Some(DataChannelEvent::OnBufferedAmountLow) => {
                        buffer_low_notify.notify_one();
                        if let Some(gate) = &gate {
                            gate.wake();
                        }
                    }
                    Some(DataChannelEvent::OnOpen) => {
                        tracing::debug!("DataChannel '{}' open", label);
                    }
                    Some(DataChannelEvent::OnClosing) | Some(DataChannelEvent::OnBufferedAmountHigh) => {}
                    Some(DataChannelEvent::OnError) => {
                        // 0.20's event carries no payload.
                        tracing::error!("DataChannel '{}' error", label);
                        mark_closed("Channel error".to_string()).await;
                    }
                    Some(DataChannelEvent::OnClose) => {
                        tracing::debug!("DataChannel '{}' closed", label);
                        mark_closed("Channel closed".to_string()).await;
                        break;
                    }
                    // `DataChannelEvent` is non-exhaustive since 0.21.
                    Some(_) => {}
                    None => {
                        // Event stream ended without OnClose: the channel is gone.
                        tracing::debug!("DataChannel '{}' event stream ended", label);
                        mark_closed("Channel closed".to_string()).await;
                        break;
                    }
                }
            }
        });

        Ok(wrapper)
    }

    /// Create a wrapper for a relay WebSocket transport
    pub async fn from_relay(transport: WebSocketTransport, encryption: Option<EncryptionConfig>) -> Self {
        let label = transport.label().to_string();
        tracing::debug!(
            "Creating DataChannelWrapper for relay channel '{}', encrypted: {}",
            label,
            encryption.is_some()
        );

        let closed = Arc::new(Mutex::new(false));
        let close_error = Arc::new(Mutex::new(None));
        let close_notify = Arc::new(Notify::new());
        let send_counter = Arc::new(Mutex::new(0u64));

        let (recv_tx, recv_rx) = mpsc::channel::<Vec<u8>>(100);
        let recv_rx = Arc::new(Mutex::new(RecvQueue::Bounded(recv_rx)));

        let transport = Arc::new(transport);
        let nonce_discriminator = derive_nonce_discriminator(&label, encryption.is_some());

        let wrapper = Self {
            transport: TransportKind::Relay(transport.clone()),
            label: label.clone(),
            closed: closed.clone(),
            close_error: close_error.clone(),
            close_notify: close_notify.clone(),
            // Relay has no poll task; unused.
            poll_stop: Arc::new(Notify::new()),
            encryption: encryption.clone(),
            send_counter,
            nonce_discriminator,
            recv_rx,
            // Relay back-pressures via the transport's byte-bounded send semaphore;
            // this Notify is unused for relay channels.
            buffer_low_notify: Arc::new(Notify::new()),
            // Relay (WebSocket) has no SCTP per-message limit — leave unbounded.
            max_message_size: AtomicU64::new(u64::MAX),
            gate: None,
        };

        // Spawn background task to receive from relay and forward to recv channel
        let enc = encryption;
        let closed_recv = closed.clone();
        let close_error_recv = close_error.clone();
        let label_recv = label.clone();

        tokio::spawn(async move {
            loop {
                if *closed_recv.lock().await {
                    break;
                }

                // No outer lock: the transport is internally synchronized, so recv and
                // send proceed concurrently. The short timeout keeps the loop checking
                // the `closed` flag.
                let result = transport.recv_timeout(100).await;

                match result {
                    Ok(data) => {
                        if data.is_empty() {
                            continue;
                        }

                        //tracing::debug!("Relay '{}' received {} bytes, encrypted: {}", label_recv, data.len(), enc.is_some());

                        let plaintext = if let Some(ref enc_config) = enc {
                            match decrypt_message(&data, enc_config) {
                                Ok(pt) => pt,
                                Err(e) => {
                                    // Mirror TS: drop the message on decrypt failure rather
                                    // than delivering raw ciphertext as plaintext.
                                    tracing::error!(
                                        "Relay '{}' decrypt error: {} ({} bytes) - dropping message",
                                        label_recv,
                                        e,
                                        data.len()
                                    );
                                    continue;
                                }
                            }
                        } else {
                            data
                        };

                        if recv_tx.send(plaintext).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        // Timeout is expected, just continue. Only break on actual close.
                        if e == "Receive timeout" {
                            continue;
                        }
                        tracing::debug!("Relay '{}' closed: {}", label_recv, e);
                        *closed_recv.lock().await = true;
                        *close_error_recv.lock().await = Some(e);
                        break;
                    }
                }
            }
        });

        wrapper
    }

    pub fn label(&self) -> &str {
        &self.label
    }

    /// Set the negotiated max message size (bytes). Called by the connector for
    /// WebRTC channels when the SCTP limit is known/updated. Mirrors the TS
    /// `wrapper.maxMessageSize = ...` assignment.
    pub fn set_max_message_size(&self, n: u64) {
        self.max_message_size.store(n, Ordering::Relaxed);
    }

    /// Largest plaintext (bytes) `send()` accepts without the post-encryption
    /// message exceeding the transport's max message size. Senders that frame
    /// their own payloads (FileServer) clamp block sizes to this. `usize::MAX`
    /// when unbounded (relay). Mirrors TS `DataChannel.maxPlaintextSize`.
    pub fn max_plaintext_size(&self) -> usize {
        let max = self.max_message_size.load(Ordering::Relaxed);
        if max == u64::MAX {
            return usize::MAX;
        }
        let overhead = if self.encryption.is_some() {
            (NONCE_SIZE + TAG_SIZE) as u64
        } else {
            0
        };
        max.saturating_sub(overhead).min(usize::MAX as u64) as usize
    }

    pub async fn is_open(&self) -> bool {
        !*self.closed.lock().await
    }

    pub fn is_encrypted(&self) -> bool {
        self.encryption.is_some()
    }

    /// Wait for the channel to be open
    pub async fn wait_for_open(&self, timeout_ms: u64) -> Result<(), String> {
        use tokio::time::{Duration, timeout};

        match &self.transport {
            TransportKind::WebRTC(channel) => {
                let channel = channel.clone();
                let check = async {
                    loop {
                        // `Err` means the core no longer knows the channel — gone.
                        let state = channel.ready_state().await.map_err(|e| e.to_string())?;
                        match state {
                            RTCDataChannelState::Open => {
                                return Ok(());
                            }
                            RTCDataChannelState::Closed => {
                                return Err("Channel closed".to_string());
                            }
                            _ => {
                                tokio::time::sleep(Duration::from_millis(10)).await;
                            }
                        }
                    }
                };

                timeout(Duration::from_millis(timeout_ms), check)
                    .await
                    .map_err(|_| format!("Channel {} open timeout", self.label))?
            }
            TransportKind::Relay(transport) => {
                let check = async {
                    loop {
                        if transport.is_open().await {
                            return Ok(());
                        }
                        if *self.closed.lock().await {
                            return Err("Channel closed".to_string());
                        }
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                };

                timeout(Duration::from_millis(timeout_ms), check)
                    .await
                    .map_err(|_| format!("Channel {} open timeout", self.label))?
            }
        }
    }

    /// Send data over the channel
    pub async fn send(&self, plaintext: &[u8]) -> Result<(), String> {
        if *self.closed.lock().await {
            return Err("Channel is closed".to_string());
        }

        // Encrypt if encryption is enabled
        let data = if let Some(ref enc) = self.encryption {
            // Refuse to send on an encrypted channel whose label was underivable:
            // sending with a zero/aliased discriminator would reuse a nonce under
            // the shared key. (In practice labels are always `ch-N`/`relay-N`.)
            let (channel_domain, channel_index) = self.nonce_discriminator.ok_or_else(|| {
                format!(
                    "Channel '{}' has no nonce discriminator (underivable label); refusing encrypted send",
                    self.label
                )
            })?;
            let counter = {
                let mut c = self.send_counter.lock().await;
                let val = *c;
                *c += 1;
                val
            };
            encrypt_message(plaintext, enc, counter, channel_domain, channel_index)?
        } else {
            plaintext.to_vec()
        };

        match &self.transport {
            TransportKind::WebRTC(channel) => {
                let state = channel.ready_state().await.map_err(|e| e.to_string())?;
                if state != RTCDataChannelState::Open {
                    tracing::warn!("WebRTC '{}' send: channel not open, state={:?}", self.label, state);
                    return Err("Channel is not open".to_string());
                }

                // Wait for buffer space — event-driven, no polling. OnBufferedAmountLow
                // fires when the SCTP buffer drains below BUFFERED_MIN and signals
                // buffer_low_notify. There is only ever one sender per channel; Notify's
                // stored-permit semantics make this safe against a wake that lands between
                // the outstanding_bytes check and the await (re-checking outstanding_bytes,
                // the source of truth, on each iteration).
                while channel.outstanding_bytes().await.map_err(|e| e.to_string())? >= BUFFERED_MAX {
                    if *self.closed.lock().await {
                        return Err("Channel closed while waiting to send".to_string());
                    }
                    self.buffer_low_notify.notified().await;
                }
                let result = channel.send(BytesMut::from(data.as_slice())).await;
                if let Err(e) = &result {
                    tracing::warn!("WebRTC '{}' send failed: {}", self.label, e);
                }
                result.map(|_| ()).map_err(|e| e.to_string())
            }
            TransportKind::Relay(transport) => {
                //tracing::debug!("Relay '{}' sending {} bytes", self.label, data.len());
                let result = transport.send(&data).await;
                if !result.is_ok() {
                    tracing::debug!("Relay '{}' send failed: {:?}", self.label, result);
                }
                result
            }
        }
    }

    /// Send a bulk file block: waits until the connection's queued bytes are
    /// under the gate's cap (see `BulkGate`), then sends as `send` does. A
    /// control response goes through `send` and never waits here.
    pub async fn send_bulk(&self, plaintext: &[u8]) -> Result<(), String> {
        if let Some(gate) = &self.gate {
            loop {
                // Register for the next wake before checking, so a wake
                // between the check and the await is not lost.
                let wake = gate.notified();
                tokio::pin!(wake);
                wake.as_mut().enable();
                if gate.has_room().await {
                    break;
                }
                if *self.closed.lock().await {
                    return Err("Channel closed while waiting for bulk room".to_string());
                }
                wake.await;
            }
        }
        self.send(plaintext).await
    }

    /// Receive next message with timeout
    pub async fn recv(&self, timeout_ms: u64) -> Result<Vec<u8>, String> {
        // Check for close error
        if let Some(ref err) = *self.close_error.lock().await {
            return Err(err.clone());
        }

        if *self.closed.lock().await {
            return Err("Channel closed".to_string());
        }

        let mut rx = self.recv_rx.lock().await;

        // Race the incoming queue against the close signal. The queue's sender lives
        // in the background poll/recv task and may outlive a remote close, so
        // `rx.recv()` alone could block forever — `close_notify` is what lets an
        // indefinite wait give up. `biased` prefers draining an already-queued
        // message over reporting the close.
        let recv_fut = async {
            tokio::select! {
                biased;
                msg = rx.recv() => msg.ok_or_else(|| "Channel closed".to_string()),
                _ = self.close_notify.notified() => Err("Channel closed".to_string()),
            }
        };

        if timeout_ms == 0 {
            // Wait indefinitely (until a message arrives or the channel closes).
            recv_fut.await
        } else {
            use tokio::time::{Duration, timeout};
            timeout(Duration::from_millis(timeout_ms), recv_fut)
                .await
                .map_err(|_| "Receive timeout".to_string())?
        }
    }

    /// Close the channel
    pub async fn close(&self) {
        *self.closed.lock().await = true;
        // Wake a sender blocked on WebRTC backpressure so it observes the close.
        self.buffer_low_notify.notify_one();
        if let Some(gate) = &self.gate {
            gate.wake();
        }
        // Wake a receiver parked on an indefinite recv so it observes the close.
        self.close_notify.notify_one();
        match &self.transport {
            TransportKind::WebRTC(channel) => {
                let _ = channel.close().await;
                // Stop the poll task even if the core never emits OnClose.
                self.poll_stop.notify_one();
            }
            TransportKind::Relay(transport) => {
                transport.close().await;
            }
        }
    }
}

/// Create a 12-byte nonce: direction bit + 63-bit counter (bytes 0-7, big-endian)
/// and the per-channel discriminator (bytes 8-11). Must match TS `makeNonce`
/// byte-for-byte — a TS peer and a Rust peer share one DataChannel and key.
fn make_nonce(counter: u64, is_client: bool, channel_domain: u8, channel_index: u32) -> [u8; NONCE_SIZE] {
    let mut nonce = [0u8; NONCE_SIZE];
    let direction_bit: u32 = if is_client { 0 } else { 0x80000000 };
    let high = ((counter >> 32) as u32) & 0x7FFFFFFF;

    nonce[0..4].copy_from_slice(&(direction_bit | high).to_be_bytes());
    nonce[4..8].copy_from_slice(&(counter as u32).to_be_bytes());
    // Bytes 8-11: channel discriminator — domain byte (webrtc/relay) in the high
    // byte, 24-bit channel index in the low 3 bytes. Makes the nonce unique across
    // the channels that share this call's single GCM key.
    let discriminator = ((channel_domain as u32) << 24) | (channel_index & CHANNEL_INDEX_MAX);
    nonce[8..12].copy_from_slice(&discriminator.to_be_bytes());

    nonce
}

/// Encrypt a message with AES-256-GCM
fn encrypt_message(
    plaintext: &[u8],
    enc: &EncryptionConfig,
    counter: u64,
    channel_domain: u8,
    channel_index: u32,
) -> Result<Vec<u8>, String> {
    let cipher = Aes256Gcm::new_from_slice(&enc.key).map_err(|e| format!("Failed to create cipher: {}", e))?;

    let nonce_bytes = make_nonce(counter, enc.is_client, channel_domain, channel_index);
    let nonce = Nonce::try_from(&nonce_bytes[..]).map_err(|_| "Invalid nonce length".to_string())?;

    let ciphertext = cipher
        .encrypt(&nonce, plaintext)
        .map_err(|e| format!("Encryption failed: {}", e))?;

    // Prepend nonce to ciphertext
    let mut result = Vec::with_capacity(NONCE_SIZE + ciphertext.len());
    result.extend_from_slice(&nonce_bytes);
    result.extend_from_slice(&ciphertext);

    Ok(result)
}

/// Decrypt a message with AES-256-GCM
fn decrypt_message(data: &[u8], enc: &EncryptionConfig) -> Result<Vec<u8>, String> {
    if data.len() < NONCE_SIZE + TAG_SIZE {
        return Err("Ciphertext too short".to_string());
    }

    let cipher = Aes256Gcm::new_from_slice(&enc.key).map_err(|e| format!("Failed to create cipher: {}", e))?;

    let nonce = Nonce::try_from(&data[..NONCE_SIZE]).map_err(|_| "Invalid nonce length".to_string())?;
    let ciphertext = &data[NONCE_SIZE..];

    cipher
        .decrypt(&nonce, ciphertext)
        .map_err(|e| format!("Decryption failed: {}", e))
}

/// Derive a session key from master key and call ID using HKDF
pub fn derive_session_key(master_key: &[u8], call_id: &str) -> Result<[u8; 32], String> {
    use hkdf::Hkdf;
    use sha2::Sha256;

    let salt = call_id.as_bytes();
    let info = b"p2p-aes-gcm-v1";

    let hk = Hkdf::<Sha256>::new(Some(salt), master_key);
    let mut okm = [0u8; 32];
    hk.expand(info, &mut okm)
        .map_err(|e| format!("HKDF expand failed: {}", e))?;

    Ok(okm)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Mirrors the packing in make_nonce bytes 8-11 (and TS DataChannelNonce.test).
    fn discriminator_word(label: &str) -> u32 {
        let (domain, index) = channel_nonce_discriminator(label).unwrap();
        ((domain as u32) << 24) | (index & CHANNEL_INDEX_MAX)
    }

    #[test]
    fn discriminator_parses_webrtc_and_relay() {
        assert_eq!(channel_nonce_discriminator("ch-0").unwrap(), (0, 0));
        assert_eq!(channel_nonce_discriminator("ch-7").unwrap(), (0, 7));
        assert_eq!(channel_nonce_discriminator("relay-0").unwrap(), (1, 0));
        assert_eq!(channel_nonce_discriminator("relay-42").unwrap(), (1, 42));
    }

    #[test]
    fn discriminator_distinguishes_webrtc_from_relay_same_index() {
        // A ch-3 and a relay-3 can both be live under the same call key.
        assert_ne!(discriminator_word("ch-3"), discriminator_word("relay-3"));
    }

    #[test]
    fn discriminator_distinct_per_channel() {
        let labels = ["ch-0", "ch-1", "ch-2", "relay-0", "relay-1", "relay-2"];
        let mut words: Vec<u32> = labels.iter().map(|l| discriminator_word(l)).collect();
        words.sort_unstable();
        words.dedup();
        assert_eq!(words.len(), labels.len());
    }

    #[test]
    fn discriminator_rejects_underivable_labels() {
        for bad in ["relay-probe", "probe", "ch-", "", "ch-1.5", "ch--1"] {
            assert!(
                channel_nonce_discriminator(bad).is_err(),
                "expected error for {:?}",
                bad
            );
        }
    }

    #[test]
    fn discriminator_rejects_24bit_overflow() {
        assert!(channel_nonce_discriminator("ch-16777215").is_ok()); // 0xFFFFFF, max
        assert!(channel_nonce_discriminator("ch-16777216").is_err()); // 0x1000000, overflow
    }

    // Golden vectors pinning the exact 12-byte layout — must match TS makeNonce
    // byte-for-byte, since a TS peer and a Rust peer share one channel + key.
    #[test]
    fn nonce_golden_vectors_match_ts_layout() {
        // ch-0, counter 0, client → all zero.
        assert_eq!(make_nonce(0, true, 0, 0), [0u8; 12]);
        // server (direction bit set), counter 1, ch-7.
        assert_eq!(make_nonce(1, false, 0, 7), [0x80, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 7]);
        // relay-3, counter 0, client → discriminator 0x01000003 in bytes 8-11.
        assert_eq!(
            make_nonce(0, true, 1, 3),
            [0, 0, 0, 0, 0, 0, 0, 0, 0x01, 0x00, 0x00, 0x03]
        );
        // 64-bit counter split: high word bytes 0-3 (top bit = direction), low 4-7.
        let counter = 0x0000_0001_0000_0002u64;
        assert_eq!(
            make_nonce(counter, false, 0, 0),
            [0x80, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 0]
        );
    }

    #[test]
    fn nonce_differs_across_channels_at_same_counter() {
        // The anti-reuse property: same key+counter, different channel → different nonce.
        assert_ne!(make_nonce(0, true, 0, 0), make_nonce(0, true, 0, 1));
        assert_ne!(make_nonce(0, true, 0, 3), make_nonce(0, true, 1, 3));
    }

    #[test]
    fn encrypt_decrypt_round_trips_with_nonce_on_wire() {
        let enc = EncryptionConfig {
            key: [7u8; 32],
            is_client: true,
        };
        let pt = b"hello channel";
        let ct = encrypt_message(pt, &enc, 0, 0, 5).unwrap();
        // Nonce is prepended in cleartext and equals make_nonce.
        assert_eq!(&ct[..NONCE_SIZE], &make_nonce(0, true, 0, 5));
        // Decrypt reads the nonce off the wire (no discriminator needed).
        assert_eq!(decrypt_message(&ct, &enc).unwrap(), pt);
    }

    // ── wrapper runtime behavior (over the in-memory fake channel) ─────────

    use crate::p2p::test_support::{FakeDataChannel, wrapper_pair};

    #[tokio::test]
    async fn encrypted_round_trip_between_wrappers() {
        let (client, server, fa, _fb) = wrapper_pair("ch-0", Some([3u8; 32])).await;

        client.send(b"from client").await.unwrap();
        assert_eq!(server.recv(2000).await.unwrap(), b"from client");
        server.send(b"from server").await.unwrap();
        assert_eq!(client.recv(2000).await.unwrap(), b"from server");

        // On the wire: nonce-prefixed ciphertext, never the plaintext.
        let wire = fa.sent_snapshot();
        assert_eq!(wire.len(), 1);
        assert_eq!(wire[0].len(), b"from client".len() + NONCE_SIZE + TAG_SIZE);
        assert_eq!(&wire[0][..NONCE_SIZE], &make_nonce(0, true, 0, 0));
    }

    #[tokio::test]
    async fn unencrypted_wrapper_passes_bytes_through() {
        let (client, server, fa, _fb) = wrapper_pair("ch-1", None).await;
        client.send(b"plain").await.unwrap();
        assert_eq!(server.recv(2000).await.unwrap(), b"plain");
        assert_eq!(fa.sent_snapshot(), vec![b"plain".to_vec()]);
    }

    /// Security-relevant: a message that fails to decrypt is DROPPED — never
    /// delivered as plaintext — and the channel keeps working for later
    /// well-formed messages.
    #[tokio::test]
    async fn decrypt_failure_drops_message_and_stream_survives() {
        let (client, server, _fa, fb) = wrapper_pair("ch-2", Some([4u8; 32])).await;

        // Garbage that is long enough to reach the cipher and fail the tag check.
        fb.inject_message(&[0xAAu8; 64]);
        assert_eq!(
            server.recv(100).await.unwrap_err(),
            "Receive timeout",
            "garbage must be dropped, not delivered"
        );

        client.send(b"still fine").await.unwrap();
        assert_eq!(server.recv(2000).await.unwrap(), b"still fine");
    }

    #[tokio::test]
    async fn empty_messages_are_ignored() {
        let (client, server, _fa, fb) = wrapper_pair("ch-3", None).await;
        fb.inject_message(b"");
        client.send(b"real").await.unwrap();
        assert_eq!(server.recv(2000).await.unwrap(), b"real");
    }

    /// An indefinite recv (timeout 0) parked on a quiet channel observes the
    /// remote close instead of hanging forever.
    #[tokio::test]
    async fn recv_wakes_on_remote_close() {
        let (client, server, _fa, _fb) = wrapper_pair("ch-4", None).await;
        let parked = tokio::spawn(async move { server.recv(0).await });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        client.close().await;
        let res = tokio::time::timeout(std::time::Duration::from_secs(2), parked)
            .await
            .expect("recv must wake on remote close")
            .unwrap();
        assert_eq!(res.unwrap_err(), "Channel closed");
    }

    #[tokio::test]
    async fn recv_times_out_when_quiet() {
        let (_client, server, _fa, _fb) = wrapper_pair("ch-5", None).await;
        assert_eq!(server.recv(50).await.unwrap_err(), "Receive timeout");
    }

    #[tokio::test]
    async fn send_after_close_errors() {
        let (client, _server, _fa, _fb) = wrapper_pair("ch-6", None).await;
        client.close().await;
        assert!(client.send(b"x").await.is_err());
        assert!(!client.is_open().await);
    }

    /// Bulk gate (`BulkGate`): a bulk send waits while the CONNECTION's queue
    /// (sum over its channels) is over the cap; a control send on its own
    /// channel goes out at once; any channel's buffer-low releases the wait.
    #[tokio::test]
    async fn bulk_send_waits_for_connection_room_control_does_not() {
        use super::super::bulk_gate::{BULK_QUEUE_MAX, BulkGate};
        let gate = Arc::new(BulkGate::new());
        let fa = FakeDataChannel::single("ch-1");
        let fb = FakeDataChannel::single("ch-2");
        let fc = FakeDataChannel::single("ch-3");
        let _a = DataChannelWrapper::new(fa.clone(), None, Some(gate.clone())).await.unwrap();
        let b = Arc::new(DataChannelWrapper::new(fb.clone(), None, Some(gate.clone())).await.unwrap());
        let control = DataChannelWrapper::new(fc.clone(), None, Some(gate.clone())).await.unwrap();

        // The low event is edge-triggered and the gate caps the connection
        // far below BUFFERED_MIN: a gated channel must ask for it at zero
        // ("this channel drained") or the gate's waiters never wake. An
        // ungated one keeps the per-channel low-water mark.
        assert_eq!(fa.low_threshold(), 0);
        let ungated = FakeDataChannel::single("ch-9");
        let _u = DataChannelWrapper::new(ungated.clone(), None, None).await.unwrap();
        assert_eq!(ungated.low_threshold(), BUFFERED_MIN as u32);

        // Channel A alone fills the connection's budget.
        fa.set_outstanding(BULK_QUEUE_MAX);
        assert!(!gate.has_room().await);

        let parked = tokio::spawn({
            let b = b.clone();
            async move { b.send_bulk(b"block").await }
        });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(!parked.is_finished(), "bulk send must park over the cap");
        assert!(fb.sent_snapshot().is_empty());

        // The listing answer on its own channel goes out at once.
        control.send(b"listing").await.unwrap();
        assert_eq!(fc.sent_snapshot().len(), 1);

        // A's SCTP buffer drains below the cap: B's block is released.
        fa.set_outstanding(0);
        fa.inject(webrtc::data_channel::DataChannelEvent::OnBufferedAmountLow);
        tokio::time::timeout(std::time::Duration::from_secs(2), parked)
            .await
            .expect("bulk send must resume on buffer-low")
            .unwrap()
            .unwrap();
        assert_eq!(fb.sent_snapshot().len(), 1);
    }

    /// A bulk sender whose own channel closes while parked fails instead of
    /// hanging, and the closed channel's bytes leave the sum.
    #[tokio::test]
    async fn parked_bulk_sender_fails_on_own_close() {
        use super::super::bulk_gate::{BULK_QUEUE_MAX, BulkGate};
        let gate = Arc::new(BulkGate::new());
        let fa = FakeDataChannel::single("ch-1");
        let fb = FakeDataChannel::single("ch-2");
        let _a = DataChannelWrapper::new(fa.clone(), None, Some(gate.clone())).await.unwrap();
        let b = Arc::new(DataChannelWrapper::new(fb.clone(), None, Some(gate.clone())).await.unwrap());
        fa.set_outstanding(BULK_QUEUE_MAX);

        let parked = tokio::spawn({
            let b = b.clone();
            async move { b.send_bulk(b"block").await }
        });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(!parked.is_finished());

        b.close().await;
        let res = tokio::time::timeout(std::time::Duration::from_secs(2), parked)
            .await
            .expect("parked bulk send must wake on close")
            .unwrap();
        assert!(res.unwrap_err().contains("closed"));
        assert!(fb.sent_snapshot().is_empty());
        // B is closed: it no longer counts, A alone is still over the cap.
        assert_eq!(gate.queued().await, BULK_QUEUE_MAX);
    }

    /// A sender parked on SCTP backpressure (outstanding >= BUFFERED_MAX) is
    /// woken by close and errors out instead of hanging.
    #[tokio::test]
    async fn blocked_sender_wakes_on_close() {
        let (fa, _fb) = FakeDataChannel::pair("ch-7");
        fa.set_outstanding(BUFFERED_MAX);
        let client = Arc::new(DataChannelWrapper::new(fa.clone(), None, None).await.unwrap());

        let sender = tokio::spawn({
            let client = client.clone();
            async move { client.send(b"stuck").await }
        });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(!sender.is_finished(), "send must park on backpressure");

        client.close().await;
        let res = tokio::time::timeout(std::time::Duration::from_secs(2), sender)
            .await
            .expect("blocked send must wake on close")
            .unwrap();
        assert!(res.unwrap_err().contains("closed"));
    }

    /// The buffer-low event resumes a parked sender once outstanding drains.
    #[tokio::test]
    async fn blocked_sender_resumes_on_buffer_low() {
        let (fa, fb) = FakeDataChannel::pair("ch-8");
        fa.set_outstanding(BUFFERED_MAX);
        let client = Arc::new(DataChannelWrapper::new(fa.clone(), None, None).await.unwrap());
        let server = Arc::new(DataChannelWrapper::new(fb.clone(), None, None).await.unwrap());

        let sender = tokio::spawn({
            let client = client.clone();
            async move { client.send(b"finally").await }
        });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(!sender.is_finished());

        fa.set_outstanding(0);
        fa.inject(webrtc::data_channel::DataChannelEvent::OnBufferedAmountLow);

        tokio::time::timeout(std::time::Duration::from_secs(2), sender)
            .await
            .expect("send must resume after buffer-low")
            .unwrap()
            .unwrap();
        assert_eq!(server.recv(2000).await.unwrap(), b"finally");
    }

    #[tokio::test]
    async fn max_plaintext_size_accounts_for_encryption_overhead() {
        let (enc, _es, _ef1, _ef2) = wrapper_pair("ch-9", Some([1u8; 32])).await;
        let (plain, _ps, _pf1, _pf2) = wrapper_pair("ch-10", None).await;
        // Default SCTP limit minus nonce+tag when encrypted; verbatim when not.
        assert_eq!(
            enc.max_plaintext_size(),
            (WEBRTC_DEFAULT_MAX_MESSAGE_SIZE as usize) - NONCE_SIZE - TAG_SIZE
        );
        assert_eq!(plain.max_plaintext_size(), WEBRTC_DEFAULT_MAX_MESSAGE_SIZE as usize);

        // Raised negotiated limit follows; u64::MAX (relay) means unbounded.
        enc.set_max_message_size(10_000);
        assert_eq!(enc.max_plaintext_size(), 10_000 - NONCE_SIZE - TAG_SIZE);
        enc.set_max_message_size(u64::MAX);
        assert_eq!(enc.max_plaintext_size(), usize::MAX);
    }

    /// An encrypted channel whose label can't derive a nonce discriminator
    /// refuses to send rather than risk nonce reuse.
    #[tokio::test]
    async fn encrypted_send_refuses_underivable_label() {
        let fa = FakeDataChannel::single("probe");
        let enc = EncryptionConfig {
            key: [2u8; 32],
            is_client: true,
        };
        let w = DataChannelWrapper::new(fa, Some(enc), None).await.unwrap();
        let err = w.send(b"data").await.unwrap_err();
        assert!(err.contains("nonce discriminator"), "got: {err}");
    }
}
