//! Shared in-memory Storage fakes for transfers tests.
//!
//! Factored out of `transfer_item.rs`'s test module so every transfers test
//! can drive the engine against the same fakes:
//!
//! - [`InMemDest`] — Class 3 (non-sequential) dest with recording hooks
//!   (put counts, completed/cancelled multiparts) and failure injection
//!   (per-key put failures of a chosen error kind, per-part failures,
//!   full-disk `check_space`).
//! - [`InMemDestProxy`] — forwards to a shared `Arc<InMemDest>` so a test can
//!   inspect the dest after the transfer completes.
//! - [`InMemSource`] — complete in-memory source: regular files, dirs,
//!   symlinks, per-key mtimes, incomplete-with-parts entries, and an optional
//!   [`GetGate`] that parks selected gets mid-flight.
//! - [`LiveSource`] — becomes complete only after `update_meta()`, driving the
//!   engine's multi-pass outer loop.
//! - [`GatedStatsSource`] — parks its dir-stats walk after the first partial
//!   block so a test can observe the mid-walk `progress()` snapshot.

use async_trait::async_trait;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::sync::{Mutex, Notify, mpsc};

use super::{
    PreviewImage, SearchOptions, SearchResult, Storage, StorageChange, StorageDest, StorageDestMultipart, StorageDirStats,
    StorageError, StorageFileAttrs, StorageFileType, StorageMeta, StorageMetaState, StorageMultipartUpload,
    StorageObjectInfo, StoragePartInfo, StoragePutConditions, StoragePutResult, StorageSearchQuery, StorageSource,
    StorageStatInfo,
};

/// `[("a.txt", b"hello"), ...]` → the `files` map the fakes take.
pub(crate) fn files(pairs: &[(&str, &[u8])]) -> HashMap<String, Vec<u8>> {
    pairs.iter().map(|(k, v)| (k.to_string(), v.to_vec())).collect()
}

/// Which error a scheduled put failure raises.
#[derive(Debug, Clone, Copy)]
pub(crate) enum PutFailKind {
    Io,
    NoSpace,
    SessionExpired,
}

impl PutFailKind {
    fn to_error(self, key: &str) -> StorageError {
        match self {
            PutFailKind::Io => StorageError::Io(std::io::Error::other(format!("simulated put failure for {key}"))),
            PutFailKind::NoSpace => StorageError::NoSpace { required: 1, free: 0 },
            PutFailKind::SessionExpired => StorageError::SessionExpired(format!("simulated session expiry for {key}")),
        }
    }
}

/// Gate that parks matching `get_streamed` calls until released. Matching is
/// by key or by request offset; every parked (and late-arriving) get resumes
/// after `release_all()`. Each gated get announces itself on the `entered`
/// channel before parking, so tests can act at a known point mid-transfer.
pub(crate) struct GetGate {
    keys: HashSet<String>,
    offsets: HashSet<u64>,
    released: AtomicBool,
    notify: Notify,
    entered_tx: mpsc::UnboundedSender<(String, Option<u64>)>,
}

impl GetGate {
    pub(crate) fn for_keys<'a>(
        keys: impl IntoIterator<Item = &'a str>,
    ) -> (Arc<Self>, mpsc::UnboundedReceiver<(String, Option<u64>)>) {
        Self::new(keys.into_iter().map(String::from).collect(), HashSet::new())
    }

    pub(crate) fn for_offsets(
        offsets: impl IntoIterator<Item = u64>,
    ) -> (Arc<Self>, mpsc::UnboundedReceiver<(String, Option<u64>)>) {
        Self::new(HashSet::new(), offsets.into_iter().collect())
    }

    fn new(
        keys: HashSet<String>,
        offsets: HashSet<u64>,
    ) -> (Arc<Self>, mpsc::UnboundedReceiver<(String, Option<u64>)>) {
        let (entered_tx, entered_rx) = mpsc::unbounded_channel();
        let gate = Arc::new(Self {
            keys,
            offsets,
            released: AtomicBool::new(false),
            notify: Notify::new(),
            entered_tx,
        });
        (gate, entered_rx)
    }

    fn matches(&self, key: &str, offset: Option<u64>) -> bool {
        self.keys.contains(key) || offset.is_some_and(|o| self.offsets.contains(&o))
    }

    pub(crate) fn release_all(&self) {
        self.released.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    async fn enter_and_wait(&self, key: &str, offset: Option<u64>) {
        let _ = self.entered_tx.send((key.to_string(), offset));
        while !self.released.load(Ordering::SeqCst) {
            let notified = self.notify.notified();
            if self.released.load(Ordering::SeqCst) {
                break;
            }
            notified.await;
        }
    }
}

/// Tiny in-memory dest: stores all puts into hashmaps. Class 3 (non-sequential).
pub(crate) struct InMemDest {
    files: Mutex<HashMap<String, Vec<u8>>>,
    /// dir key → `put_dir` count (a preloaded dir starts at 0).
    dirs: Mutex<HashMap<String, u32>>,
    symlinks: Mutex<HashMap<String, String>>,
    mtimes: Mutex<HashMap<String, chrono::DateTime<chrono::Utc>>>,
    puts: Mutex<HashMap<String, u32>>,
    /// Preloaded in-progress multiparts (key → parts), served by
    /// `list_multiparts_all` / `list_parts_all` to simulate a prior run.
    preset_multiparts: Mutex<HashMap<String, Vec<StoragePartInfo>>>,
    /// `check_space` reports a full disk.
    no_space: bool,
    /// key → (how many more `put_file`s on it fail, with which error).
    fail_puts: Mutex<HashMap<String, (u32, PutFailKind)>>,
    /// (key, part_number) → how many more `put_part`s on it fail (IO error).
    fail_part_puts: Mutex<HashMap<(String, u32), u32>>,
    completed_multiparts: Mutex<Vec<String>>,
    cancelled_multiparts: Mutex<Vec<String>>,
    /// What `bytes_sent_progress` reports: `None` (a local dest, the
    /// default) or a counter a test drives (for `moved_bytes` tests). Set
    /// before wrapping in an `Arc`.
    pub(crate) sent_progress: Option<AtomicU64>,
    data_source_id: Option<String>,
}

impl InMemDest {
    pub(crate) fn new() -> Self {
        Self {
            files: Mutex::new(HashMap::new()),
            dirs: Mutex::new(HashMap::new()),
            symlinks: Mutex::new(HashMap::new()),
            mtimes: Mutex::new(HashMap::new()),
            puts: Mutex::new(HashMap::new()),
            preset_multiparts: Mutex::new(HashMap::new()),
            no_space: false,
            fail_puts: Mutex::new(HashMap::new()),
            fail_part_puts: Mutex::new(HashMap::new()),
            completed_multiparts: Mutex::new(Vec::new()),
            cancelled_multiparts: Mutex::new(Vec::new()),
            sent_progress: None,
            data_source_id: None,
        }
    }

    pub(crate) fn with_no_space() -> Self {
        Self {
            no_space: true,
            ..Self::new()
        }
    }

    pub(crate) async fn fail_next_puts(&self, key: &str, times: u32) {
        self.fail_next_puts_with(key, times, PutFailKind::Io).await;
    }

    pub(crate) async fn fail_next_puts_with(&self, key: &str, times: u32, kind: PutFailKind) {
        self.fail_puts.lock().await.insert(key.into(), (times, kind));
    }

    pub(crate) async fn fail_next_part_puts(&self, key: &str, part_number: u32, times: u32) {
        self.fail_part_puts
            .lock()
            .await
            .insert((key.into(), part_number), times);
    }

    pub(crate) async fn snapshot(&self) -> HashMap<String, Vec<u8>> {
        self.files.lock().await.clone()
    }

    pub(crate) async fn preload(&self, key: &str, data: &[u8]) {
        self.files.lock().await.insert(key.into(), data.to_vec());
    }

    pub(crate) async fn preload_with_mtime(&self, key: &str, data: &[u8], mtime: chrono::DateTime<chrono::Utc>) {
        self.preload(key, data).await;
        self.mtimes.lock().await.insert(key.into(), mtime);
    }

    pub(crate) async fn preload_dir(&self, key: &str) {
        self.dirs.lock().await.insert(key.into(), 0);
    }

    pub(crate) async fn preload_multipart(&self, key: &str, parts: Vec<StoragePartInfo>) {
        self.preset_multiparts.lock().await.insert(key.into(), parts);
    }

    pub(crate) async fn put_count(&self, key: &str) -> u32 {
        self.puts.lock().await.get(key).copied().unwrap_or(0)
    }

    pub(crate) async fn dir_put_count(&self, key: &str) -> u32 {
        self.dirs.lock().await.get(key).copied().unwrap_or(0)
    }

    pub(crate) async fn symlinks_snapshot(&self) -> HashMap<String, String> {
        self.symlinks.lock().await.clone()
    }

    pub(crate) async fn completed_multiparts(&self) -> Vec<String> {
        self.completed_multiparts.lock().await.clone()
    }

    pub(crate) async fn cancelled_multiparts(&self) -> Vec<String> {
        self.cancelled_multiparts.lock().await.clone()
    }
}

#[async_trait]
impl Storage for InMemDest {
    async fn connect(&mut self) -> Result<StorageMetaState, StorageError> {
        Ok(StorageMetaState::Unknown)
    }
    async fn cancel(&self) -> Result<(), StorageError> {
        Ok(())
    }
    async fn update_meta(&self) -> Result<Vec<StorageChange>, StorageError> {
        Ok(Vec::new())
    }
    async fn get_attrs(&self, path: &str) -> Result<StorageObjectInfo, StorageError> {
        if let Some(v) = self.files.lock().await.get(path) {
            let mut soi = StorageObjectInfo::new(path);
            soi.size = v.len() as u64;
            soi.attrs.mtime = self.mtimes.lock().await.get(path).copied();
            return Ok(soi);
        }
        if self.dirs.lock().await.contains_key(path) {
            let mut soi = StorageObjectInfo::new(path);
            soi.file_type = StorageFileType::Dir;
            return Ok(soi);
        }
        if let Some(target) = self.symlinks.lock().await.get(path) {
            let mut soi = StorageObjectInfo::new(path);
            soi.file_type = StorageFileType::Symlink;
            soi.size = target.len() as u64;
            return Ok(soi);
        }
        Err(StorageError::NoSuchKey(path.to_string()))
    }
    fn bytes_sent_progress(&self) -> Option<u64> {
        self.sent_progress.as_ref().map(|p| p.load(Ordering::Relaxed))
    }
    fn data_source_id(&self) -> Option<&str> {
        self.data_source_id.as_deref()
    }
    fn set_data_source_id(&mut self, id: String) {
        self.data_source_id = Some(id);
    }
}

#[async_trait]
impl StorageDest for InMemDest {
    fn is_sequential(&self) -> bool {
        false
    }
    fn is_sequential_multipart(&self) -> bool {
        false
    }
    async fn check_space(&self, required_total: u64) -> Result<(), StorageError> {
        if self.no_space {
            return Err(StorageError::NoSpace {
                required: required_total,
                free: 0,
            });
        }
        Ok(())
    }
    async fn put_file(
        &self,
        k: &str,
        d: &[u8],
        _: Option<&StorageFileAttrs>,
        _: Option<&StoragePutConditions>,
    ) -> Result<StoragePutResult, StorageError> {
        *self.puts.lock().await.entry(k.into()).or_insert(0) += 1;
        {
            let mut fails = self.fail_puts.lock().await;
            if let Some((n, kind)) = fails.get_mut(k)
                && *n > 0
            {
                *n -= 1;
                let kind = *kind;
                return Err(kind.to_error(k));
            }
        }
        self.files.lock().await.insert(k.into(), d.to_vec());
        Ok(StoragePutResult {
            etag: None,
            crc32: None,
            sha256: None,
            storage_key: k.into(),
        })
    }
    async fn put_dir(&self, k: &str, _: Option<&StorageFileAttrs>) -> Result<StoragePutResult, StorageError> {
        *self.dirs.lock().await.entry(k.into()).or_insert(0) += 1;
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
        target: &str,
        _: Option<&StorageFileAttrs>,
    ) -> Result<StoragePutResult, StorageError> {
        *self.puts.lock().await.entry(k.into()).or_insert(0) += 1;
        self.symlinks.lock().await.insert(k.into(), target.to_string());
        Ok(StoragePutResult {
            etag: None,
            crc32: None,
            sha256: None,
            storage_key: k.into(),
        })
    }
    async fn del_file(&self, k: &str) -> Result<(), StorageError> {
        self.files.lock().await.remove(k);
        self.symlinks.lock().await.remove(k);
        self.mtimes.lock().await.remove(k);
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
impl StorageDestMultipart for InMemDest {
    async fn list_multiparts_all(&self) -> Result<Vec<StorageMultipartUpload>, StorageError> {
        let preset = self.preset_multiparts.lock().await;
        Ok(preset
            .keys()
            .map(|k| StorageMultipartUpload {
                plain_key: Some(k.clone()),
                encrypted_key: Some(k.clone()),
                upload_id: Some(k.clone()),
                ..Default::default()
            })
            .collect())
    }
    async fn list_parts_all(&self, u: &StorageMultipartUpload) -> Result<Vec<StoragePartInfo>, StorageError> {
        let preset = self.preset_multiparts.lock().await;
        Ok(u.upload_id
            .as_ref()
            .and_then(|id| preset.get(id))
            .cloned()
            .unwrap_or_default())
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
            encrypted_key: Some(object.into()),
            ..Default::default()
        })
    }
    async fn put_part(
        &self,
        n: u32,
        upload: &StorageMultipartUpload,
        offset: u64,
        data: &[u8],
        is_last: bool,
    ) -> Result<StoragePartInfo, StorageError> {
        let key = upload.plain_key.as_ref().unwrap().clone();
        {
            let mut fails = self.fail_part_puts.lock().await;
            if let Some(times) = fails.get_mut(&(key.clone(), n))
                && *times > 0
            {
                *times -= 1;
                return Err(StorageError::Io(std::io::Error::other(format!(
                    "simulated part put failure for {key}#{n}"
                ))));
            }
        }
        let mut files = self.files.lock().await;
        let entry = files.entry(key).or_insert_with(Vec::new);
        let end = offset as usize + data.len();
        if entry.len() < end {
            entry.resize(end, 0);
        }
        entry[offset as usize..end].copy_from_slice(data);
        let info = StoragePartInfo {
            part_number: Some(n),
            offset: Some(offset),
            size: Some(data.len() as u64),
            is_last: Some(is_last),
            ..Default::default()
        };
        // A landed part is listed from now on, as on a real dest.
        if let Some(id) = upload.upload_id.as_ref() {
            self.preset_multiparts
                .lock()
                .await
                .entry(id.clone())
                .or_default()
                .push(info.clone());
        }
        Ok(info)
    }
    async fn cancel_multipart(&self, u: &StorageMultipartUpload) -> Result<(), StorageError> {
        let key = u.plain_key.clone().or_else(|| u.upload_id.clone()).unwrap_or_default();
        self.cancelled_multiparts.lock().await.push(key);
        if let Some(id) = u.upload_id.as_ref() {
            self.preset_multiparts.lock().await.remove(id);
        }
        Ok(())
    }
    async fn complete_multipart(
        &self,
        u: &StorageMultipartUpload,
        _: &[StoragePartInfo],
    ) -> Result<String, StorageError> {
        let key = u.plain_key.clone().or_else(|| u.upload_id.clone()).unwrap_or_default();
        self.completed_multiparts.lock().await.push(key);
        Ok(String::new())
    }
}

/// Wrapper that forwards to a shared `Arc<InMemDest>` so the test can
/// inspect the dest after the transfer completes.
pub(crate) struct InMemDestProxy(pub(crate) Arc<InMemDest>);

#[async_trait]
impl Storage for InMemDestProxy {
    async fn connect(&mut self) -> Result<StorageMetaState, StorageError> {
        Ok(StorageMetaState::Unknown)
    }
    async fn cancel(&self) -> Result<(), StorageError> {
        self.0.cancel().await
    }
    async fn update_meta(&self) -> Result<Vec<StorageChange>, StorageError> {
        Ok(Vec::new())
    }
    async fn get_attrs(&self, path: &str) -> Result<StorageObjectInfo, StorageError> {
        self.0.get_attrs(path).await
    }
    fn bytes_sent_progress(&self) -> Option<u64> {
        self.0.bytes_sent_progress()
    }
    fn data_source_id(&self) -> Option<&str> {
        None
    }
    fn set_data_source_id(&mut self, _: String) {}
}

#[async_trait]
impl StorageDest for InMemDestProxy {
    fn is_sequential(&self) -> bool {
        self.0.is_sequential()
    }
    fn is_sequential_multipart(&self) -> bool {
        self.0.is_sequential_multipart()
    }
    async fn check_space(&self, required_total: u64) -> Result<(), StorageError> {
        self.0.check_space(required_total).await
    }
    async fn put_file(
        &self,
        k: &str,
        d: &[u8],
        attrs: Option<&StorageFileAttrs>,
        c: Option<&StoragePutConditions>,
    ) -> Result<StoragePutResult, StorageError> {
        self.0.put_file(k, d, attrs, c).await
    }
    async fn put_dir(&self, k: &str, attrs: Option<&StorageFileAttrs>) -> Result<StoragePutResult, StorageError> {
        self.0.put_dir(k, attrs).await
    }
    async fn put_symlink(
        &self,
        k: &str,
        t: &str,
        attrs: Option<&StorageFileAttrs>,
    ) -> Result<StoragePutResult, StorageError> {
        self.0.put_symlink(k, t, attrs).await
    }
    async fn del_file(&self, k: &str) -> Result<(), StorageError> {
        self.0.del_file(k).await
    }
    async fn del_dir(&self, k: &str) -> Result<(), StorageError> {
        self.0.del_dir(k).await
    }
    async fn update_stats(&self, s: &StorageStatInfo) -> Result<(), StorageError> {
        self.0.update_stats(s).await
    }
    async fn close(&self) -> Result<(), StorageError> {
        self.0.close().await
    }
}

#[async_trait]
impl StorageDestMultipart for InMemDestProxy {
    async fn list_multiparts_all(&self) -> Result<Vec<StorageMultipartUpload>, StorageError> {
        self.0.list_multiparts_all().await
    }
    async fn list_parts_all(&self, u: &StorageMultipartUpload) -> Result<Vec<StoragePartInfo>, StorageError> {
        self.0.list_parts_all(u).await
    }
    async fn begin_multipart(
        &self,
        o: &str,
        size: u64,
        attrs: Option<&StorageFileAttrs>,
    ) -> Result<StorageMultipartUpload, StorageError> {
        self.0.begin_multipart(o, size, attrs).await
    }
    async fn put_part(
        &self,
        n: u32,
        u: &StorageMultipartUpload,
        off: u64,
        d: &[u8],
        last: bool,
    ) -> Result<StoragePartInfo, StorageError> {
        self.0.put_part(n, u, off, d, last).await
    }
    async fn cancel_multipart(&self, u: &StorageMultipartUpload) -> Result<(), StorageError> {
        self.0.cancel_multipart(u).await
    }
    async fn complete_multipart(
        &self,
        u: &StorageMultipartUpload,
        p: &[StoragePartInfo],
    ) -> Result<String, StorageError> {
        self.0.complete_multipart(u, p).await
    }
}

/// Tiny in-memory source with complete meta: regular files plus optional
/// dirs, symlinks, per-key mtimes and incomplete-with-parts markers. Fill the
/// `pub(crate)` fields after `new` for anything beyond plain files.
pub(crate) struct InMemSource {
    pub(crate) files: HashMap<String, Vec<u8>>,
    pub(crate) dirs: Vec<String>,
    /// key → link target.
    pub(crate) symlinks: HashMap<String, String>,
    pub(crate) mtimes: HashMap<String, chrono::DateTime<chrono::Utc>>,
    /// Keys listed here enumerate as incomplete regulars carrying these parts.
    pub(crate) incomplete_parts: HashMap<String, Vec<StoragePartInfo>>,
    /// Keys whose bytes the source can't serve yet: `get_streamed` answers
    /// `NoPartError` while `get_attrs` still lists them (a live share's part
    /// the uploader hasn't produced).
    pub(crate) unavailable: HashSet<String>,
    pub(crate) gate: Option<Arc<GetGate>>,
    /// Serve `get_previews`: a file's preview is its own bytes as `image/png`
    /// with etag `v-<len>` (`*.bin` has none, `*.err` fails), answered in
    /// REVERSE request order.
    pub(crate) previews: bool,
    pub(crate) data_source_id: Option<String>,
}

impl InMemSource {
    pub(crate) fn new(files: HashMap<String, Vec<u8>>) -> Self {
        Self {
            files,
            dirs: Vec::new(),
            symlinks: HashMap::new(),
            mtimes: HashMap::new(),
            incomplete_parts: HashMap::new(),
            unavailable: HashSet::new(),
            gate: None,
            previews: false,
            data_source_id: None,
        }
    }

    fn meta(&self) -> StorageMeta {
        let mut m = StorageMeta::default();
        for (k, v) in &self.files {
            let mut soi = StorageObjectInfo::new(k);
            soi.file_type = StorageFileType::Regular;
            soi.size = v.len() as u64;
            soi.is_complete = true;
            soi.attrs.mtime = self.mtimes.get(k).copied();
            if let Some(parts) = self.incomplete_parts.get(k) {
                soi.is_complete = false;
                soi.parts = Some(parts.clone());
            }
            m.update(soi);
        }
        for d in &self.dirs {
            let mut soi = StorageObjectInfo::new(d);
            soi.file_type = StorageFileType::Dir;
            m.update(soi);
        }
        for (k, target) in &self.symlinks {
            let mut soi = StorageObjectInfo::new(k);
            soi.file_type = StorageFileType::Symlink;
            soi.size = target.len() as u64;
            m.update(soi);
        }
        m.set_is_complete_meta(true);
        m
    }
}

#[async_trait]
impl Storage for InMemSource {
    async fn connect(&mut self) -> Result<StorageMetaState, StorageError> {
        Ok(StorageMetaState::Complete)
    }
    async fn cancel(&self) -> Result<(), StorageError> {
        Ok(())
    }
    async fn get_dir(
        &self,
        path: &str,
        recursive: bool,
        _start_after: Option<&str>,
        max_batch_size: Option<usize>,
    ) -> mpsc::Receiver<Result<Vec<StorageObjectInfo>, StorageError>> {
        self.meta().get_dir(path, recursive, max_batch_size)
    }
    async fn update_meta(&self) -> Result<Vec<StorageChange>, StorageError> {
        Ok(Vec::new())
    }
    async fn get_attrs(&self, path: &str) -> Result<StorageObjectInfo, StorageError> {
        self.meta()
            .get_attrs(path)
            .ok_or_else(|| StorageError::NoSuchKey(path.to_string()))
    }
    /// The meta's paged search, as a ninja source answers it.
    async fn search(&self, query: &StorageSearchQuery, options: &SearchOptions) -> Result<SearchResult, StorageError> {
        self.meta().search(query, options)
    }
    fn data_source_id(&self) -> Option<&str> {
        self.data_source_id.as_deref()
    }
    fn set_data_source_id(&mut self, id: String) {
        self.data_source_id = Some(id);
    }
}

#[async_trait]
impl StorageSource for InMemSource {
    fn source_name(&self) -> &str {
        "InMem"
    }
    async fn get_streamed(
        &self,
        object: &str,
        offset: Option<u64>,
        length: Option<u64>,
    ) -> mpsc::Receiver<Result<Vec<u8>, StorageError>> {
        let (tx, rx) = mpsc::channel(1);
        let item = match self.files.get(object) {
            Some(_) if self.unavailable.contains(object) => {
                Err(StorageError::NoPartError(format!("unavailable {object}")))
            }
            Some(data) => {
                let start = offset.unwrap_or(0) as usize;
                let end = length.map(|l| start + l as usize).unwrap_or(data.len()).min(data.len());
                Ok(data[start..end].to_vec())
            }
            None => Err(StorageError::NoSuchKey(object.to_string())),
        };
        let gate = self.gate.as_ref().filter(|g| g.matches(object, offset)).cloned();
        let key = object.to_string();
        tokio::spawn(async move {
            if let Some(g) = gate {
                g.enter_and_wait(&key, offset).await;
            }
            let _ = tx.send(item).await;
        });
        rx
    }
    async fn get_symlink(&self, object: &str) -> Result<String, StorageError> {
        self.symlinks
            .get(object)
            .cloned()
            .ok_or_else(|| StorageError::NoSuchKey(object.to_string()))
    }
    fn serves_previews(&self) -> bool {
        self.previews
    }
    async fn get_previews(
        &self,
        objects: Vec<String>,
        _px: u32,
    ) -> Result<mpsc::Receiver<(usize, Result<PreviewImage, StorageError>)>, StorageError> {
        if !self.previews {
            return Err(StorageError::NotImplemented("get_previews".into()));
        }
        let mut answers = Vec::new();
        for (index, object) in objects.iter().enumerate() {
            let result = match self.files.get(object) {
                _ if self.symlinks.contains_key(object) => Err(StorageError::NoPreview(object.clone())),
                None => Err(StorageError::NoSuchKey(object.clone())),
                Some(_) if object.ends_with(".bin") => Err(StorageError::NoPreview(object.clone())),
                Some(_) if object.ends_with(".err") => Err(StorageError::Other("decoder blew up".into())),
                Some(bytes) => Ok(PreviewImage {
                    bytes: bytes.clone(),
                    mime_type: "image/png".into(),
                    etag: Some(format!("v-{}", bytes.len())),
                }),
            };
            answers.push((index, result));
        }
        let (tx, rx) = mpsc::channel(4);
        tokio::spawn(async move {
            for answer in answers.into_iter().rev() {
                if tx.send(answer).await.is_err() {
                    return;
                }
            }
        });
        Ok(rx)
    }
}

/// Source that becomes complete only after `update_meta()`: pass 1 sees the
/// initial files with Incomplete meta; `update_meta()` adds the late files
/// and flips to Complete — driving the engine's multi-pass outer loop.
pub(crate) struct LiveSource {
    // Interior mutability: `update_meta` takes `&self` (it runs under the
    // engine's read guard) and is what moves `late` into `files`.
    files: std::sync::Mutex<HashMap<String, Vec<u8>>>,
    late: std::sync::Mutex<HashMap<String, Vec<u8>>>,
    complete: std::sync::atomic::AtomicBool,
    /// Answer the dir-stats walk with `Incomplete` while the source is —
    /// the still-uploading cloud share with no peer (MultiSource's rule).
    deferred_sizing: bool,
    /// Keys whose stream ends cleanly one byte short of the listed size —
    /// a connection cut behind a clean close.
    truncated: HashSet<String>,
    data_source_id: Option<String>,
}

impl LiveSource {
    pub(crate) fn new(files: HashMap<String, Vec<u8>>, late: HashMap<String, Vec<u8>>) -> Self {
        let complete = late.is_empty();
        Self {
            files: std::sync::Mutex::new(files),
            late: std::sync::Mutex::new(late),
            complete: std::sync::atomic::AtomicBool::new(complete),
            deferred_sizing: false,
            truncated: HashSet::new(),
            data_source_id: None,
        }
    }

    pub(crate) fn with_truncated(mut self, key: &str) -> Self {
        self.truncated.insert(key.to_string());
        self
    }

    pub(crate) fn with_deferred_sizing(mut self) -> Self {
        self.deferred_sizing = true;
        self
    }

    fn build_meta(&self) -> StorageMeta {
        let mut m = StorageMeta::default();
        for (k, v) in self.files.lock().unwrap().iter() {
            let mut soi = StorageObjectInfo::new(k);
            soi.file_type = StorageFileType::Regular;
            soi.size = v.len() as u64;
            m.update(soi);
        }
        m
    }
}

#[async_trait]
impl Storage for LiveSource {
    async fn connect(&mut self) -> Result<StorageMetaState, StorageError> {
        Ok(StorageMetaState::Incomplete)
    }
    async fn cancel(&self) -> Result<(), StorageError> {
        Ok(())
    }
    async fn meta_stats(&self) -> Result<StorageStatInfo, StorageError> {
        Ok(StorageStatInfo {
            meta_state: if self.complete.load(std::sync::atomic::Ordering::Relaxed) {
                StorageMetaState::Complete
            } else {
                StorageMetaState::Incomplete
            },
            total_size: None,
        })
    }
    async fn update_meta(&self) -> Result<Vec<StorageChange>, StorageError> {
        // let pass-1's spawned put tasks drain before pass 2 enumerates
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        self.complete.store(true, std::sync::atomic::Ordering::Relaxed);
        let late = std::mem::take(&mut *self.late.lock().unwrap());
        self.files.lock().unwrap().extend(late);
        Ok(Vec::new())
    }
    async fn get_dir(
        &self,
        path: &str,
        recursive: bool,
        _start_after: Option<&str>,
        max_batch_size: Option<usize>,
    ) -> mpsc::Receiver<Result<Vec<StorageObjectInfo>, StorageError>> {
        self.build_meta().get_dir(path, recursive, max_batch_size)
    }
    async fn get_dir_stats_streamed(&self, path: &str) -> mpsc::Receiver<Result<StorageDirStats, StorageError>> {
        if self.deferred_sizing && !self.complete.load(std::sync::atomic::Ordering::Relaxed) {
            let (tx, rx) = mpsc::channel(1);
            let _ = tx.send(Err(StorageError::Incomplete(path.to_string()))).await;
            return rx;
        }
        let (tx, rx) = mpsc::channel(1);
        let _ = tx.send(Ok(self.build_meta().get_dir_stats(path))).await;
        rx
    }
    async fn get_attrs(&self, path: &str) -> Result<StorageObjectInfo, StorageError> {
        self.build_meta()
            .get_attrs(path)
            .ok_or_else(|| StorageError::NoSuchKey(path.to_string()))
    }
    fn data_source_id(&self) -> Option<&str> {
        self.data_source_id.as_deref()
    }
    fn set_data_source_id(&mut self, id: String) {
        self.data_source_id = Some(id);
    }
}

#[async_trait]
impl StorageSource for LiveSource {
    fn source_name(&self) -> &str {
        "LiveSource"
    }
    async fn get_streamed(
        &self,
        object: &str,
        offset: Option<u64>,
        length: Option<u64>,
    ) -> mpsc::Receiver<Result<Vec<u8>, StorageError>> {
        let (tx, rx) = mpsc::channel(1);
        let item = match self.files.lock().unwrap().get(object) {
            Some(data) => {
                let start = offset.unwrap_or(0) as usize;
                let mut end = length.map(|l| start + l as usize).unwrap_or(data.len()).min(data.len());
                if self.truncated.contains(object) && end > start {
                    end -= 1;
                }
                Ok(data[start..end].to_vec())
            }
            None => Err(StorageError::NoSuchKey(object.to_string())),
        };
        let _ = tx.send(item).await;
        rx
    }
    async fn get_symlink(&self, _: &str) -> Result<String, StorageError> {
        Err(StorageError::NotImplemented("symlink".into()))
    }
}

/// Source whose dir-stats walk parks on a gate after its first partial
/// block, so a test can observe the mid-walk progress() snapshot.
pub(crate) struct GatedStatsSource {
    pub(crate) files: HashMap<String, Vec<u8>>,
    pub(crate) release: Arc<Notify>,
    pub(crate) data_source_id: Option<String>,
}

#[async_trait]
impl Storage for GatedStatsSource {
    async fn connect(&mut self) -> Result<StorageMetaState, StorageError> {
        Ok(StorageMetaState::Complete)
    }
    async fn cancel(&self) -> Result<(), StorageError> {
        Ok(())
    }
    async fn meta_stats(&self) -> Result<StorageStatInfo, StorageError> {
        // total_size unknown — forces the sizing walk
        Ok(StorageStatInfo {
            meta_state: StorageMetaState::Complete,
            total_size: None,
        })
    }
    async fn get_dir(
        &self,
        path: &str,
        recursive: bool,
        _start_after: Option<&str>,
        max_batch_size: Option<usize>,
    ) -> mpsc::Receiver<Result<Vec<StorageObjectInfo>, StorageError>> {
        let mut m = StorageMeta::default();
        for (k, v) in &self.files {
            let mut soi = StorageObjectInfo::new(k);
            soi.file_type = StorageFileType::Regular;
            soi.size = v.len() as u64;
            soi.is_complete = true;
            m.update(soi);
        }
        m.set_is_complete_meta(true);
        m.get_dir(path, recursive, max_batch_size)
    }
    async fn update_meta(&self) -> Result<Vec<StorageChange>, StorageError> {
        Ok(Vec::new())
    }
    async fn get_dir_stats_streamed(&self, _: &str) -> mpsc::Receiver<Result<StorageDirStats, StorageError>> {
        let total: u64 = self.files.values().map(|v| v.len() as u64).sum();
        let count = self.files.len() as u32;
        let first = self.files.values().next().map(|v| v.len() as u64).unwrap_or(0);
        let release = self.release.clone();
        let (tx, rx) = mpsc::channel(2);
        tokio::spawn(async move {
            let _ = tx
                .send(Ok(StorageDirStats {
                    number_of_files: 1,
                    number_of_folders: 0,
                    number_of_symlinks: 0,
                    total_size: first,
                }))
                .await;
            release.notified().await;
            let _ = tx
                .send(Ok(StorageDirStats {
                    number_of_files: count,
                    number_of_folders: 0,
                    number_of_symlinks: 0,
                    total_size: total,
                }))
                .await;
        });
        rx
    }
    fn data_source_id(&self) -> Option<&str> {
        self.data_source_id.as_deref()
    }
    fn set_data_source_id(&mut self, id: String) {
        self.data_source_id = Some(id);
    }
}

#[async_trait]
impl StorageSource for GatedStatsSource {
    fn source_name(&self) -> &str {
        "GatedStats"
    }
    async fn get_streamed(
        &self,
        object: &str,
        offset: Option<u64>,
        length: Option<u64>,
    ) -> mpsc::Receiver<Result<Vec<u8>, StorageError>> {
        let (tx, rx) = mpsc::channel(1);
        let item = match self.files.get(object) {
            Some(data) => {
                let start = offset.unwrap_or(0) as usize;
                let end = length.map(|l| start + l as usize).unwrap_or(data.len()).min(data.len());
                Ok(data[start..end].to_vec())
            }
            None => Err(StorageError::NoSuchKey(object.to_string())),
        };
        let _ = tx.send(item).await;
        rx
    }
    async fn get_symlink(&self, _: &str) -> Result<String, StorageError> {
        Err(StorageError::NotImplemented("symlink".into()))
    }
}
