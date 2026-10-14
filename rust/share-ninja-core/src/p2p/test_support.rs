//! Shared fakes for p2p tests.
//!
//! The centerpiece is [`FakeDataChannel`] — an in-memory implementation of the
//! wrapper's [`RtcChannel`] trait. Two of them wired back-to-back carry real
//! [`DataChannelWrapper`]s (encryption included), which is enough to test the
//! wrapper's runtime behavior, the [`super::FileServer`] wire protocol, and the
//! connector's pooling rules without any network.
//!
//! Also here: a couple of scripted `Storage` fakes the FileServer tests need
//! beyond what `crate::transfers::test_support` provides (typed dest errors,
//! search pages, a stats walk that yields nothing).

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

use async_trait::async_trait;
use bytes::BytesMut;
use tokio::sync::{Mutex, mpsc};
use webrtc::data_channel::{DataChannelEvent, RTCDataChannelMessage, RTCDataChannelState};
use webrtc::error::{Error as RtcError, Result as RtcResult};

use super::data_channel::{DataChannelWrapper, EncryptionConfig, RtcChannel};
use crate::transfers::{
    SearchOptions, SearchResult, Storage, StorageChange, StorageDest, StorageError, StorageFileAttrs, StorageMetaState,
    StorageObjectInfo, StoragePutConditions, StoragePutResult, StorageSearchQuery, StorageSource,
};

/// In-memory [`RtcChannel`]: events the wrapper polls come from an injectable
/// queue; sends are recorded and, when paired, delivered to the peer channel's
/// queue as `OnMessage`.
pub(crate) struct FakeDataChannel {
    label: String,
    state: StdMutex<RTCDataChannelState>,
    events: Mutex<mpsc::UnboundedReceiver<DataChannelEvent>>,
    self_tx: mpsc::UnboundedSender<DataChannelEvent>,
    peer_tx: StdMutex<Option<mpsc::UnboundedSender<DataChannelEvent>>>,
    outstanding: AtomicUsize,
    low_threshold: AtomicU32,
    /// Raw wire bytes this end sent (post-encryption).
    sent: StdMutex<Vec<Vec<u8>>>,
}

impl FakeDataChannel {
    fn new(label: &str) -> (Arc<Self>, mpsc::UnboundedSender<DataChannelEvent>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let ch = Arc::new(Self {
            label: label.to_string(),
            state: StdMutex::new(RTCDataChannelState::Open),
            events: Mutex::new(rx),
            self_tx: tx.clone(),
            peer_tx: StdMutex::new(None),
            outstanding: AtomicUsize::new(0),
            low_threshold: AtomicU32::new(0),
            sent: StdMutex::new(Vec::new()),
        });
        (ch, tx)
    }

    /// A lone channel: the test injects events via `inject*` and reads what the
    /// wrapper sent via `sent_snapshot`.
    pub(crate) fn single(label: &str) -> Arc<Self> {
        Self::new(label).0
    }

    /// Two channels wired back-to-back: a `send` on one arrives as `OnMessage`
    /// on the other; a `close` on one closes both ends.
    pub(crate) fn pair(label: &str) -> (Arc<Self>, Arc<Self>) {
        let (a, a_tx) = Self::new(label);
        let (b, b_tx) = Self::new(label);
        *a.peer_tx.lock().unwrap() = Some(b_tx);
        *b.peer_tx.lock().unwrap() = Some(a_tx);
        (a, b)
    }

    pub(crate) fn inject(&self, ev: DataChannelEvent) {
        let _ = self.self_tx.send(ev);
    }

    /// Inject a raw inbound message (bypasses any peer/encryption — what the
    /// wrapper's poll task sees on the wire).
    pub(crate) fn inject_message(&self, data: &[u8]) {
        self.inject(DataChannelEvent::OnMessage(RTCDataChannelMessage {
            is_string: false,
            data: BytesMut::from(data),
        }));
    }

    pub(crate) fn sent_snapshot(&self) -> Vec<Vec<u8>> {
        self.sent.lock().unwrap().clone()
    }

    /// Value reported by `outstanding_bytes` — drives the wrapper's send
    /// backpressure loop.
    pub(crate) fn set_outstanding(&self, n: usize) {
        self.outstanding.store(n, Ordering::SeqCst);
    }

    /// The low-water threshold the wrapper configured on this channel.
    pub(crate) fn low_threshold(&self) -> u32 {
        self.low_threshold.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl RtcChannel for FakeDataChannel {
    async fn label(&self) -> RtcResult<String> {
        Ok(self.label.clone())
    }
    async fn ready_state(&self) -> RtcResult<RTCDataChannelState> {
        Ok(*self.state.lock().unwrap())
    }
    async fn set_buffered_amount_low_threshold(&self, threshold: u32) -> RtcResult<()> {
        self.low_threshold.store(threshold, Ordering::SeqCst);
        Ok(())
    }
    async fn outstanding_bytes(&self) -> RtcResult<usize> {
        Ok(self.outstanding.load(Ordering::SeqCst))
    }
    async fn send(&self, data: BytesMut) -> RtcResult<()> {
        if *self.state.lock().unwrap() != RTCDataChannelState::Open {
            return Err(RtcError::ErrDataChannelClosed);
        }
        self.sent.lock().unwrap().push(data.to_vec());
        let peer = self.peer_tx.lock().unwrap().clone();
        if let Some(tx) = peer {
            let _ = tx.send(DataChannelEvent::OnMessage(RTCDataChannelMessage {
                is_string: false,
                data,
            }));
        }
        Ok(())
    }
    async fn poll(&self) -> Option<DataChannelEvent> {
        self.events.lock().await.recv().await
    }
    async fn close(&self) -> RtcResult<()> {
        {
            let mut st = self.state.lock().unwrap();
            if *st == RTCDataChannelState::Closed {
                return Ok(());
            }
            *st = RTCDataChannelState::Closed;
        }
        let _ = self.self_tx.send(DataChannelEvent::OnClose);
        if let Some(tx) = self.peer_tx.lock().unwrap().clone() {
            let _ = tx.send(DataChannelEvent::OnClose);
        }
        Ok(())
    }
}

/// Client/server [`EncryptionConfig`]s over one shared key.
pub(crate) fn enc_pair(key: [u8; 32]) -> (EncryptionConfig, EncryptionConfig) {
    (
        EncryptionConfig { key, is_client: true },
        EncryptionConfig { key, is_client: false },
    )
}

/// Real wrapper pair over an in-memory channel pair. `key` = `Some` for an
/// encrypted call (first wrapper is the client). Returns the fakes too, for
/// wire-level assertions/injection.
pub(crate) async fn wrapper_pair(
    label: &str,
    key: Option<[u8; 32]>,
) -> (
    Arc<DataChannelWrapper>,
    Arc<DataChannelWrapper>,
    Arc<FakeDataChannel>,
    Arc<FakeDataChannel>,
) {
    let (fa, fb) = FakeDataChannel::pair(label);
    let (ea, eb) = match key {
        Some(k) => {
            let (c, s) = enc_pair(k);
            (Some(c), Some(s))
        }
        None => (None, None),
    };
    let a = Arc::new(DataChannelWrapper::new(fa.clone(), ea, None).await.unwrap());
    let b = Arc::new(DataChannelWrapper::new(fb.clone(), eb, None).await.unwrap());
    (a, b, fa, fb)
}

/// Which typed error a [`ScriptedDest`] operation raises.
#[derive(Clone, Copy)]
pub(crate) enum DestErrKind {
    Precondition,
    NoSuchKey,
    NotImplemented,
    Other,
}

impl DestErrKind {
    fn to_error(self, key: &str) -> StorageError {
        match self {
            DestErrKind::Precondition => StorageError::PreconditionFailed(key.to_string()),
            DestErrKind::NoSuchKey => StorageError::NoSuchKey(key.to_string()),
            DestErrKind::NotImplemented => StorageError::NotImplemented(key.to_string()),
            DestErrKind::Other => StorageError::Other(key.to_string()),
        }
    }
}

/// Dest whose put/delete fail with a scripted typed error per key — for the
/// FileServer's wire status mapping. Successful puts store bytes and return a
/// fixed etag.
pub(crate) struct ScriptedDest {
    files: Mutex<HashMap<String, Vec<u8>>>,
    errors: StdMutex<HashMap<String, DestErrKind>>,
    data_source_id: Option<String>,
}

impl ScriptedDest {
    pub(crate) fn new() -> Self {
        Self {
            files: Mutex::new(HashMap::new()),
            errors: StdMutex::new(HashMap::new()),
            data_source_id: None,
        }
    }

    pub(crate) fn fail_key(&self, key: &str, kind: DestErrKind) {
        self.errors.lock().unwrap().insert(key.to_string(), kind);
    }

    pub(crate) async fn snapshot(&self) -> HashMap<String, Vec<u8>> {
        self.files.lock().await.clone()
    }

    fn error_for(&self, key: &str) -> Option<StorageError> {
        self.errors.lock().unwrap().get(key).map(|k| k.to_error(key))
    }
}

#[async_trait]
impl Storage for ScriptedDest {
    async fn connect(&mut self) -> Result<StorageMetaState, StorageError> {
        Ok(StorageMetaState::Unknown)
    }
    async fn cancel(&self) -> Result<(), StorageError> {
        Ok(())
    }
    fn data_source_id(&self) -> Option<&str> {
        self.data_source_id.as_deref()
    }
    fn set_data_source_id(&mut self, id: String) {
        self.data_source_id = Some(id);
    }
}

#[async_trait]
impl StorageDest for ScriptedDest {
    fn is_sequential(&self) -> bool {
        false
    }
    async fn put_file(
        &self,
        k: &str,
        d: &[u8],
        _: Option<&StorageFileAttrs>,
        _: Option<&StoragePutConditions>,
    ) -> Result<StoragePutResult, StorageError> {
        if let Some(e) = self.error_for(k) {
            return Err(e);
        }
        self.files.lock().await.insert(k.into(), d.to_vec());
        Ok(StoragePutResult {
            etag: Some(format!("etag-{k}")),
            crc32: None,
            sha256: None,
            storage_key: k.into(),
        })
    }
    async fn put_dir(&self, k: &str, _: Option<&StorageFileAttrs>) -> Result<StoragePutResult, StorageError> {
        if let Some(e) = self.error_for(k) {
            return Err(e);
        }
        Ok(StoragePutResult {
            etag: None,
            crc32: None,
            sha256: None,
            storage_key: k.into(),
        })
    }
    async fn put_symlink(
        &self,
        k: &str,
        _: &str,
        _: Option<&StorageFileAttrs>,
    ) -> Result<StoragePutResult, StorageError> {
        Ok(StoragePutResult {
            etag: None,
            crc32: None,
            sha256: None,
            storage_key: k.into(),
        })
    }
    async fn del_file(&self, k: &str) -> Result<(), StorageError> {
        if let Some(e) = self.error_for(k) {
            return Err(e);
        }
        self.files.lock().await.remove(k);
        Ok(())
    }
    async fn del_dir(&self, _: &str) -> Result<(), StorageError> {
        Ok(())
    }
    async fn update_stats(&self, _: &crate::transfers::StorageStatInfo) -> Result<(), StorageError> {
        Ok(())
    }
    async fn close(&self) -> Result<(), StorageError> {
        Ok(())
    }
}

/// Source whose `search` answers from a scripted page queue (one page per
/// request) and whose dir-stats walk yields nothing at all — the two source
/// behaviors the FileServer tests need beyond the transfers fakes.
pub(crate) struct ScriptedSource {
    pub(crate) search_pages: StdMutex<VecDeque<SearchResult>>,
    data_source_id: Option<String>,
}

impl ScriptedSource {
    pub(crate) fn new() -> Self {
        Self {
            search_pages: StdMutex::new(VecDeque::new()),
            data_source_id: None,
        }
    }

    pub(crate) fn push_search_page(&self, page: SearchResult) {
        self.search_pages.lock().unwrap().push_back(page);
    }
}

#[async_trait]
impl Storage for ScriptedSource {
    async fn connect(&mut self) -> Result<StorageMetaState, StorageError> {
        Ok(StorageMetaState::Complete)
    }
    async fn cancel(&self) -> Result<(), StorageError> {
        Ok(())
    }
    async fn update_meta(&self) -> Result<Vec<StorageChange>, StorageError> {
        Ok(Vec::new())
    }
    /// Yields no blocks at all — the "source that owes the client a terminal
    /// block anyway" case for `process_dir_stats`.
    async fn get_dir_stats_streamed(
        &self,
        _: &str,
    ) -> mpsc::Receiver<Result<crate::transfers::StorageDirStats, StorageError>> {
        let (_tx, rx) = mpsc::channel(1);
        rx
    }
    async fn search(
        &self,
        _query: &StorageSearchQuery,
        _options: &SearchOptions,
    ) -> Result<SearchResult, StorageError> {
        self.search_pages
            .lock()
            .unwrap()
            .pop_front()
            .ok_or_else(|| StorageError::Other("no scripted search page".into()))
    }
    fn data_source_id(&self) -> Option<&str> {
        self.data_source_id.as_deref()
    }
    fn set_data_source_id(&mut self, id: String) {
        self.data_source_id = Some(id);
    }
}

#[async_trait]
impl StorageSource for ScriptedSource {
    fn source_name(&self) -> &str {
        "Scripted"
    }
    async fn get_streamed(
        &self,
        object: &str,
        _offset: Option<u64>,
        _length: Option<u64>,
    ) -> mpsc::Receiver<Result<Vec<u8>, StorageError>> {
        let (tx, rx) = mpsc::channel(1);
        let _ = tx.send(Err(StorageError::NoSuchKey(object.to_string()))).await;
        rx
    }
    async fn get_symlink(&self, o: &str) -> Result<String, StorageError> {
        Err(StorageError::NoSuchKey(o.to_string()))
    }
}

/// `StorageObjectInfo` regular-file entry of `size` zero-filled bytes worth of
/// metadata — for search/list payload-size tests.
pub(crate) fn soi_file(key: &str, size: u64) -> StorageObjectInfo {
    let mut soi = StorageObjectInfo::new(key);
    soi.size = size;
    soi
}
