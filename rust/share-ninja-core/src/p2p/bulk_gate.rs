//! One gate per connection: a bulk file block is handed to its channel only
//! while the bytes queued across ALL of the connection's channels are under
//! `BULK_QUEUE_MAX`; control responses (listing, stats, attrs, previews)
//! never wait here. Every channel shares one SCTP association with one send
//! FIFO, so without this a 1 KB listing answer sat behind
//! channels × BUFFERED_MAX of file data (docs/app/EngineIssues.md #13). The
//! cap only has to cover the bandwidth-delay product, so throughput is
//! unaffected. Mirrors TS `BulkGate`.

use std::sync::Arc;

use tokio::sync::{Mutex, Notify};
use webrtc::data_channel::RTCDataChannelState;

use super::data_channel::RtcChannel;

pub const BULK_QUEUE_MAX: usize = 4_000_000;

#[derive(Default)]
pub struct BulkGate {
    channels: Mutex<Vec<Arc<dyn RtcChannel>>>,
    notify: Notify,
}

impl BulkGate {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn register(&self, channel: Arc<dyn RtcChannel>) {
        self.channels.lock().await.push(channel);
    }

    /// Bytes queued on the connection's open channels; a closed one drops out.
    pub async fn queued(&self) -> usize {
        let mut channels = self.channels.lock().await;
        let mut total = 0;
        let mut open = Vec::with_capacity(channels.len());
        for ch in channels.drain(..) {
            if matches!(ch.ready_state().await, Ok(RTCDataChannelState::Open)) {
                total += ch.outstanding_bytes().await.unwrap_or(0);
                open.push(ch);
            }
        }
        *channels = open;
        total
    }

    pub async fn has_room(&self) -> bool {
        self.queued().await < BULK_QUEUE_MAX
    }

    /// Something changed (a buffer drained, a channel closed): re-check.
    pub fn wake(&self) {
        self.notify.notify_waiters();
    }

    /// The next `wake`, as a future the caller pins and `enable`s BEFORE its
    /// re-check, so a wake landing between the check and the await is kept.
    pub fn notified(&self) -> tokio::sync::futures::Notified<'_> {
        self.notify.notified()
    }
}
