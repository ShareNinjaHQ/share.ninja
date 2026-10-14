//! MultipartReorderDest — proxy that owns per-multipart part ordering.
//!
//! Wraps a [`StorageDestMultipart`] that wants sequential parts per multipart
//! (`is_sequential_multipart() == true`). Forwards all other Storage methods
//! to the inner dest.
//!
//! `put_part(N)` for an out-of-order part returns a future that resolves once
//! part N's turn comes around. Sequential drain after the current part
//! guarantees the inner dest's `put_part` sees parts in strict order.

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock, mpsc, oneshot};

use super::{
    Storage, StorageDest, StorageDestMultipart, StorageDirStats, StorageError, StorageFileAttrs, StorageMetaState,
    StorageMultipartUpload, StorageObjectInfo, StoragePartInfo, StoragePutConditions, StoragePutResult,
    StorageStatInfo,
};

#[allow(unused_imports)]
use StorageObjectInfo as _SoiUsed;

struct BufferedPart {
    data: Vec<u8>,
    offset: u64,
    is_last: bool,
    deferred: oneshot::Sender<Result<StoragePartInfo, StorageError>>,
}

struct UploadState {
    /// Expected next part number (1-based).
    current_part: u32,
    buffer: HashMap<u32, BufferedPart>,
}

/// Proxy that owns per-multipart part ordering. Wraps an inner
/// [`StorageDestMultipart`] (typically a Class 1 sequential dest like
/// `ZipDest` / `StreamDest`, or — eventually — a Class 2 cloud dest with
/// per-multipart ordering requirements).
///
/// Holds the inner behind `Arc<RwLock<...>>` to match the existing dest
/// plumbing (which needs `&mut self` access to `meta`/`update_meta`/`connect`).
/// Per-call methods (`put_part`, `put_file`, etc.) use a read guard so
/// multiple concurrent calls don't serialize on the lock.
pub struct MultipartReorderDest {
    inner: Arc<RwLock<dyn StorageDestMultipart>>,
    upload_states: Mutex<HashMap<String, UploadState>>,
    cancelled: std::sync::atomic::AtomicBool,
    /// Last value successfully read out of `inner` by the sync
    /// `bytes_sent_progress`. See there for why it is cached.
    last_sent_progress: std::sync::Mutex<Option<u64>>,
}

impl MultipartReorderDest {
    pub fn new(inner: Arc<RwLock<dyn StorageDestMultipart>>) -> Self {
        Self {
            inner,
            upload_states: Mutex::new(HashMap::new()),
            cancelled: std::sync::atomic::AtomicBool::new(false),
            last_sent_progress: std::sync::Mutex::new(None),
        }
    }

    /// Reference to the wrapped dest. Useful for tests and for the contiguous
    /// scheduling path that needs `inner.is_sequential_multipart()` or similar.
    pub fn inner(&self) -> &Arc<RwLock<dyn StorageDestMultipart>> {
        &self.inner
    }

    /// The next part number this proxy expects to forward for the given
    /// upload. Returns `StorageError::Other` if the uploadID is not tracked
    /// (i.e. `begin_multipart` was never called for it).
    pub async fn get_current_part(&self, upload_id: &str) -> Result<u32, StorageError> {
        let states = self.upload_states.lock().await;
        states.get(upload_id).map(|s| s.current_part).ok_or_else(|| {
            StorageError::Other(format!(
                "MultipartReorderDest::get_current_part: no state for upload {}",
                upload_id
            ))
        })
    }

    fn reject_buffered(state: &mut UploadState, msg: &str) {
        for (_, part) in state.buffer.drain() {
            let _ = part.deferred.send(Err(StorageError::Other(msg.to_string())));
        }
    }
}

#[async_trait]
impl Storage for MultipartReorderDest {
    async fn connect(&mut self) -> Result<StorageMetaState, StorageError> {
        self.inner.write().await.connect().await
    }

    async fn cancel(&self) -> Result<(), StorageError> {
        self.cancelled.store(true, std::sync::atomic::Ordering::Relaxed);

        // Reject any pending out-of-order parts so their submitting tasks
        // unpark (and release their limiter slots) instead of hanging forever.
        let mut states = self.upload_states.lock().await;
        for state in states.values_mut() {
            Self::reject_buffered(state, "MultipartReorderDest cancelled");
        }
        states.clear();
        drop(states);

        self.inner.read().await.cancel().await
    }

    async fn get_dir(
        &self,
        path: &str,
        recursive: bool,
        start_after: Option<&str>,
        max_batch_size: Option<usize>,
    ) -> mpsc::Receiver<Result<Vec<StorageObjectInfo>, StorageError>> {
        self.inner
            .write()
            .await
            .get_dir(path, recursive, start_after, max_batch_size)
            .await
    }

    async fn get_attrs(&self, path: &str) -> Result<StorageObjectInfo, StorageError> {
        self.inner.read().await.get_attrs(path).await
    }

    async fn get_dir_stats_streamed(&self, path: &str) -> mpsc::Receiver<Result<StorageDirStats, StorageError>> {
        self.inner.read().await.get_dir_stats_streamed(path).await
    }

    async fn meta_stats(&self) -> Result<StorageStatInfo, StorageError> {
        self.inner.read().await.meta_stats().await
    }

    /// The wrapped dest's wire progress, seen through the proxy. Parts parked
    /// here are not on any wire and do not count.
    ///
    /// Sync, so the inner lock can only be `try_read`. On contention we serve
    /// the last value we did read: tokio's `RwLock` is write-preferring, so a
    /// queued writer (`connect`, `get_dir`, `get_dir_stats`) blocks readers,
    /// and the counter is monotonic — a value a poll stale is merely late.
    fn bytes_sent_progress(&self) -> Option<u64> {
        let mut last = self.last_sent_progress.lock().unwrap();
        match self.inner.try_read() {
            Ok(g) => {
                *last = g.bytes_sent_progress();
                *last
            }
            Err(_) => *last,
        }
    }

    fn data_source_id(&self) -> Option<&str> {
        None
    }

    fn set_data_source_id(&mut self, _id: String) {}
}

#[async_trait]
impl StorageDest for MultipartReorderDest {
    fn is_sequential(&self) -> bool {
        // Avoid blocking on the lock for these read-only metadata checks.
        // The inner's flags don't change at runtime, so try_read is enough;
        // if temporarily contended, fall back to a conservative true (sequential)
        // which matches our typical wrapping target.
        match self.inner.try_read() {
            Ok(g) => g.is_sequential(),
            Err(_) => true,
        }
    }

    fn is_sequential_multipart(&self) -> bool {
        match self.inner.try_read() {
            Ok(g) => g.is_sequential_multipart(),
            Err(_) => true,
        }
    }

    async fn put_file(
        &self,
        object: &str,
        data: &[u8],
        attrs: Option<&StorageFileAttrs>,
        conditions: Option<&StoragePutConditions>,
    ) -> Result<StoragePutResult, StorageError> {
        self.inner.read().await.put_file(object, data, attrs, conditions).await
    }

    async fn put_dir(&self, object: &str, attrs: Option<&StorageFileAttrs>) -> Result<StoragePutResult, StorageError> {
        self.inner.read().await.put_dir(object, attrs).await
    }

    async fn put_symlink(
        &self,
        object: &str,
        target: &str,
        attrs: Option<&StorageFileAttrs>,
    ) -> Result<StoragePutResult, StorageError> {
        self.inner.read().await.put_symlink(object, target, attrs).await
    }

    async fn del_file(&self, object: &str) -> Result<(), StorageError> {
        self.inner.read().await.del_file(object).await
    }

    async fn del_dir(&self, object: &str) -> Result<(), StorageError> {
        self.inner.read().await.del_dir(object).await
    }

    async fn update_stats(&self, stats: &StorageStatInfo) -> Result<(), StorageError> {
        self.inner.read().await.update_stats(stats).await
    }

    async fn flush(&self) -> Result<(), StorageError> {
        self.inner.read().await.flush().await
    }

    async fn close(&self) -> Result<(), StorageError> {
        self.inner.read().await.close().await
    }

    async fn finalize(&self) -> Result<(), StorageError> {
        self.inner.read().await.finalize().await
    }
}

#[async_trait]
impl StorageDestMultipart for MultipartReorderDest {
    async fn list_multiparts_all(&self) -> Result<Vec<StorageMultipartUpload>, StorageError> {
        self.inner.read().await.list_multiparts_all().await
    }

    async fn list_parts_all(&self, upload: &StorageMultipartUpload) -> Result<Vec<StoragePartInfo>, StorageError> {
        self.inner.read().await.list_parts_all(upload).await
    }

    async fn begin_multipart(
        &self,
        object: &str,
        size: u64,
        attrs: Option<&StorageFileAttrs>,
    ) -> Result<StorageMultipartUpload, StorageError> {
        let upload = self.inner.read().await.begin_multipart(object, size, attrs).await?;
        let id = upload
            .upload_id
            .as_ref()
            .ok_or_else(|| StorageError::Other("begin_multipart returned no upload_id".into()))?
            .clone();
        self.upload_states.lock().await.insert(
            id,
            UploadState {
                current_part: 1,
                buffer: HashMap::new(),
            },
        );
        Ok(upload)
    }

    async fn put_part(
        &self,
        part_number: u32,
        upload: &StorageMultipartUpload,
        offset: u64,
        data: &[u8],
        is_last_part: bool,
    ) -> Result<StoragePartInfo, StorageError> {
        if self.cancelled.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(StorageError::Cancelled);
        }

        let upload_id = upload
            .upload_id
            .as_ref()
            .ok_or_else(|| StorageError::Other("put_part: no upload_id".into()))?
            .clone();

        // First lock: classify (forward now, or buffer with deferred) and bookkeep.
        enum Action {
            Forward,
            Buffer(oneshot::Receiver<Result<StoragePartInfo, StorageError>>),
        }

        let action = {
            let mut states = self.upload_states.lock().await;
            let state = states.get_mut(&upload_id).ok_or_else(|| {
                StorageError::Other(format!(
                    "MultipartReorderDest::put_part: no state for upload {} (begin_multipart not called)",
                    upload_id
                ))
            })?;

            if part_number < state.current_part {
                return Err(StorageError::Other(format!(
                    "MultipartReorderDest::put_part: part {} already past current_part {}",
                    part_number, state.current_part
                )));
            }

            if part_number != state.current_part {
                if state.buffer.contains_key(&part_number) {
                    return Err(StorageError::Other(format!(
                        "MultipartReorderDest::put_part: duplicate part {}",
                        part_number
                    )));
                }
                let (tx, rx) = oneshot::channel();
                state.buffer.insert(
                    part_number,
                    BufferedPart {
                        data: data.to_vec(),
                        offset,
                        is_last: is_last_part,
                        deferred: tx,
                    },
                );
                tracing::debug!(
                    "buffered part {}#{} (expected {})",
                    upload_id,
                    part_number,
                    state.current_part
                );
                Action::Buffer(rx)
            } else {
                Action::Forward
            }
        };

        match action {
            Action::Buffer(rx) => match rx.await {
                Ok(result) => result,
                Err(_) => Err(StorageError::Cancelled),
            },
            Action::Forward => {
                self.forward_and_drain(&upload_id, upload, part_number, offset, data, is_last_part)
                    .await
            }
        }
    }

    async fn cancel_multipart(&self, upload: &StorageMultipartUpload) -> Result<(), StorageError> {
        let upload_id_opt = upload.upload_id.clone();
        if let Some(ref id) = upload_id_opt {
            let mut states = self.upload_states.lock().await;
            if let Some(mut state) = states.remove(id) {
                Self::reject_buffered(&mut state, "cancel_multipart");
            }
        }
        self.inner.read().await.cancel_multipart(upload).await
    }

    async fn complete_multipart(
        &self,
        upload: &StorageMultipartUpload,
        parts: &[StoragePartInfo],
    ) -> Result<String, StorageError> {
        if let Some(ref id) = upload.upload_id {
            let mut states = self.upload_states.lock().await;
            if let Some(state) = states.get(id)
                && !state.buffer.is_empty()
            {
                return Err(StorageError::Other(format!(
                    "complete_multipart: buffer not empty for {} ({} parts)",
                    id,
                    state.buffer.len()
                )));
            }
            states.remove(id);
        }
        self.inner.read().await.complete_multipart(upload, parts).await
    }
}

impl MultipartReorderDest {
    /// Write one part to the inner dest.
    async fn put_inner(
        &self,
        part_number: u32,
        upload: &StorageMultipartUpload,
        offset: u64,
        data: &[u8],
        is_last_part: bool,
    ) -> Result<StoragePartInfo, StorageError> {
        self.inner
            .read()
            .await
            .put_part(part_number, upload, offset, data, is_last_part)
            .await
    }

    /// Forward the current part to inner, then drain any consecutive buffered
    /// followers (each await happens with the state mutex dropped, so we
    /// re-acquire briefly between iterations to advance `current_part` and
    /// pull the next buffered entry).
    async fn forward_and_drain(
        &self,
        upload_id: &str,
        upload: &StorageMultipartUpload,
        part_number: u32,
        offset: u64,
        data: &[u8],
        is_last_part: bool,
    ) -> Result<StoragePartInfo, StorageError> {
        tracing::debug!("forward part {}#{}", upload_id, part_number);
        let result = self.put_inner(part_number, upload, offset, data, is_last_part).await;

        // Advance current_part regardless of result; on error, also reject buffered.
        match result {
            Ok(info) => {
                {
                    let mut states = self.upload_states.lock().await;
                    if let Some(state) = states.get_mut(upload_id) {
                        state.current_part += 1;
                    }
                }
                self.drain(upload_id, upload).await?;
                Ok(info)
            }
            Err(e) => {
                let mut states = self.upload_states.lock().await;
                if let Some(state) = states.get_mut(upload_id) {
                    Self::reject_buffered(state, "forward failed");
                }
                Err(e)
            }
        }
    }

    /// Drain buffered parts whose number now matches `current_part`, in strict
    /// order. Releases the state lock around each `inner.put_part` await.
    async fn drain(&self, upload_id: &str, upload: &StorageMultipartUpload) -> Result<(), StorageError> {
        loop {
            let next = {
                let mut states = self.upload_states.lock().await;
                let Some(state) = states.get_mut(upload_id) else {
                    return Ok(());
                };
                let n = state.current_part;
                state.buffer.remove(&n).map(|bp| (n, bp))
            };

            let Some((n, bp)) = next else {
                return Ok(());
            };

            tracing::debug!("drain part {}#{}", upload_id, n);
            let r = self.put_inner(n, upload, bp.offset, &bp.data, bp.is_last).await;

            match r {
                Ok(info) => {
                    let _ = bp.deferred.send(Ok(info));
                    let mut states = self.upload_states.lock().await;
                    if let Some(state) = states.get_mut(upload_id) {
                        state.current_part += 1;
                    }
                }
                Err(e) => {
                    let err_msg = format!("{}", e);
                    let _ = bp.deferred.send(Err(e));
                    // Reject remaining buffered parts; they can't proceed.
                    let mut states = self.upload_states.lock().await;
                    if let Some(state) = states.get_mut(upload_id) {
                        Self::reject_buffered(state, &err_msg);
                    }
                    return Err(StorageError::Other(err_msg));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transfers::{StorageDest, StorageDestMultipart};
    use std::sync::atomic::{AtomicU32, Ordering};

    /// Test dest that records the order parts arrive at and asserts they're in-order.
    /// Uses shared Arc-of-state so an outside observer can inspect after the
    /// dest is moved into the proxy's RwLock.
    struct StrictOrderDest {
        sequence: Arc<Mutex<Vec<u32>>>,
        next_expected: Arc<AtomicU32>,
        /// Held by `put_part` for the duration of each write. Unlocked by
        /// default; a test clones the Arc and locks it to freeze a write.
        gate: Arc<Mutex<()>>,
        /// What this dest reports as its own `bytes_sent_progress`.
        sent_progress: Arc<std::sync::atomic::AtomicU64>,
    }

    impl StrictOrderDest {
        fn new() -> (Self, Arc<Mutex<Vec<u32>>>) {
            let sequence = Arc::new(Mutex::new(Vec::new()));
            let next_expected = Arc::new(AtomicU32::new(1));
            (
                Self {
                    sequence: sequence.clone(),
                    next_expected,
                    gate: Arc::new(Mutex::new(())),
                    sent_progress: Arc::new(std::sync::atomic::AtomicU64::new(0)),
                },
                sequence,
            )
        }
    }

    #[async_trait]
    impl Storage for StrictOrderDest {
        async fn connect(&mut self) -> Result<StorageMetaState, StorageError> {
            Ok(StorageMetaState::Unknown)
        }
        async fn cancel(&self) -> Result<(), StorageError> {
            Ok(())
        }
        fn bytes_sent_progress(&self) -> Option<u64> {
            Some(self.sent_progress.load(Ordering::Relaxed))
        }
        fn data_source_id(&self) -> Option<&str> {
            None
        }
        fn set_data_source_id(&mut self, _: String) {}
    }

    #[async_trait]
    impl StorageDest for StrictOrderDest {
        fn is_sequential(&self) -> bool {
            true
        }
        fn is_sequential_multipart(&self) -> bool {
            true
        }
        async fn put_file(
            &self,
            k: &str,
            _: &[u8],
            _: Option<&StorageFileAttrs>,
            _: Option<&StoragePutConditions>,
        ) -> Result<StoragePutResult, StorageError> {
            Ok(StoragePutResult {
                etag: None,
                crc32: None,
                sha256: None,
                storage_key: k.into(),
            })
        }
        async fn put_dir(&self, k: &str, _: Option<&StorageFileAttrs>) -> Result<StoragePutResult, StorageError> {
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
        async fn del_file(&self, _: &str) -> Result<(), StorageError> {
            Ok(())
        }
        async fn del_dir(&self, _: &str) -> Result<(), StorageError> {
            Ok(())
        }
        async fn update_stats(&self, _: &StorageStatInfo) -> Result<(), StorageError> {
            Ok(())
        }
        async fn close(&self) -> Result<(), StorageError> {
            Ok(())
        }
    }

    #[async_trait]
    impl StorageDestMultipart for StrictOrderDest {
        async fn list_multiparts_all(&self) -> Result<Vec<StorageMultipartUpload>, StorageError> {
            Ok(vec![])
        }
        async fn list_parts_all(&self, _: &StorageMultipartUpload) -> Result<Vec<StoragePartInfo>, StorageError> {
            Ok(vec![])
        }
        async fn begin_multipart(
            &self,
            object: &str,
            _: u64,
            _: Option<&StorageFileAttrs>,
        ) -> Result<StorageMultipartUpload, StorageError> {
            Ok(StorageMultipartUpload {
                plain_key: Some(object.into()),
                upload_id: Some(object.into()),
                ..Default::default()
            })
        }
        async fn put_part(
            &self,
            part_number: u32,
            _upload: &StorageMultipartUpload,
            _offset: u64,
            data: &[u8],
            _is_last_part: bool,
        ) -> Result<StoragePartInfo, StorageError> {
            let _gate = self.gate.lock().await;
            let expected = self.next_expected.fetch_add(1, Ordering::Relaxed);
            assert_eq!(part_number, expected, "parts arrived out of order at inner dest");
            self.sequence.lock().await.push(part_number);
            Ok(StoragePartInfo {
                part_number: Some(part_number),
                size: Some(data.len() as u64),
                ..Default::default()
            })
        }
        async fn cancel_multipart(&self, _: &StorageMultipartUpload) -> Result<(), StorageError> {
            Ok(())
        }
        async fn complete_multipart(
            &self,
            _: &StorageMultipartUpload,
            _: &[StoragePartInfo],
        ) -> Result<String, StorageError> {
            Ok(String::new())
        }
    }

    fn wrap(inner: StrictOrderDest) -> Arc<RwLock<dyn StorageDestMultipart>> {
        Arc::new(RwLock::new(inner))
    }

    #[tokio::test]
    async fn forwards_in_order_immediately() {
        let (inner, sequence) = StrictOrderDest::new();
        let proxy = MultipartReorderDest::new(wrap(inner));
        let up = proxy.begin_multipart("k", 0, None).await.unwrap();

        for n in 1..=5 {
            proxy.put_part(n, &up, 0, &[0; 16], n == 5).await.unwrap();
        }
        assert_eq!(*sequence.lock().await, vec![1, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn reorders_out_of_order_arrivals() {
        let (inner, sequence) = StrictOrderDest::new();
        let proxy = Arc::new(MultipartReorderDest::new(wrap(inner)));
        let up = proxy.begin_multipart("k", 0, None).await.unwrap();

        // Submit parts 3 and 2 first (they should buffer), then 1 which drains everything.
        let p3 = {
            let proxy = proxy.clone();
            let up = up.clone();
            tokio::spawn(async move { proxy.put_part(3, &up, 0, &[0; 16], false).await })
        };
        let p2 = {
            let proxy = proxy.clone();
            let up = up.clone();
            tokio::spawn(async move { proxy.put_part(2, &up, 0, &[0; 16], false).await })
        };

        // Brief pause to let parts park in the buffer.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(sequence.lock().await.is_empty(), "no parts should have arrived yet");

        // Submit part 1: forwards 1, drains 2 and 3.
        proxy.put_part(1, &up, 0, &[0; 16], false).await.unwrap();
        p2.await.unwrap().unwrap();
        p3.await.unwrap().unwrap();

        assert_eq!(*sequence.lock().await, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn cancel_rejects_buffered_parts() {
        let (inner, _sequence) = StrictOrderDest::new();
        let proxy = Arc::new(MultipartReorderDest::new(wrap(inner)));
        let up = proxy.begin_multipart("k", 0, None).await.unwrap();

        let p2 = {
            let proxy = proxy.clone();
            let up = up.clone();
            tokio::spawn(async move { proxy.put_part(2, &up, 0, &[0; 16], false).await })
        };

        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        proxy.cancel().await.unwrap();

        let r = p2.await.unwrap();
        assert!(r.is_err(), "buffered part should reject on cancel");
    }

    // A busy inner lock must not read as "no progress" — the getter serves
    // the last successfully read value instead.
    #[tokio::test]
    async fn inner_progress_falls_back_to_last_known() {
        let (inner, _sequence) = StrictOrderDest::new();
        let progress = inner.sent_progress.clone();
        let proxy = Arc::new(MultipartReorderDest::new(wrap(inner)));

        progress.store(500, Ordering::Relaxed);
        assert_eq!(proxy.bytes_sent_progress(), Some(500), "read through and cached");

        let writer = proxy.inner().write().await;
        assert_eq!(
            proxy.bytes_sent_progress(),
            Some(500),
            "lock busy: last known, not None"
        );
        drop(writer);

        assert_eq!(proxy.bytes_sent_progress(), Some(500));
    }
}
