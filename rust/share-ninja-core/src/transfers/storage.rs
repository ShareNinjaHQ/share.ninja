//! Core storage types and traits

use async_trait::async_trait;
use thiserror::Error;
use tokio::sync::mpsc;

/// Storage file types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageFileType {
    Regular,
    Dir,
    Part,
    LastPart,
    Symlink,
    Info,
    ZipBatch,
}

/// Storage metadata state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StorageMetaState {
    #[default]
    Unknown = 0,
    Incomplete = 1,
    Complete = 2,
}

impl StorageMetaState {
    /// Numeric encoding matching the TS `StorageMetaState` enum
    /// (Unknown=0, Incomplete=1, Complete=2) — used in the `storage#info` JSON.
    pub fn as_i32(self) -> i32 {
        self as i32
    }

    pub fn from_i32(v: i32) -> StorageMetaState {
        match v {
            2 => StorageMetaState::Complete,
            1 => StorageMetaState::Incomplete,
            _ => StorageMetaState::Unknown,
        }
    }
}

/// Storage source type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageSourceType {
    S3,
    P2P,
    FileSystem,
    IMAP,
    NinjaV2,
}

/// Part information for multipart uploads
#[derive(Debug, Clone, Default)]
pub struct StoragePartInfo {
    pub etag: Option<String>,
    pub part_number: Option<u32>,
    pub crc32: Option<Vec<u8>>,
    pub sha256: Option<Vec<u8>>,
    pub offset: Option<u64>,
    pub size: Option<u64>,
    pub is_last: Option<bool>,
}

/// File attributes (mirrors proto FileAttrs / TS StorageFileAttrs)
#[derive(Debug, Clone, Default)]
pub struct StorageFileAttrs {
    pub mtime: Option<chrono::DateTime<chrono::Utc>>,
    pub posix_permissions: Option<i64>,
    pub email_subject: Option<String>,
    pub gdrive_file_id: Option<String>,
    pub gdrive_mime_type: Option<String>,
}

/// Base object information — the generic surface only, mirroring the TS
/// `StorageObjectInfo`. Backend-private payload (ninja blob keys, seeds, zip
/// entry names) lives inside the owning backend, never on this shared type.
#[derive(Debug, Clone)]
pub struct StorageObjectInfo {
    pub key: String,
    pub file_type: StorageFileType,
    pub size: u64,
    pub is_complete: bool,
    pub parts: Option<Vec<StoragePartInfo>>,
    pub attrs: StorageFileAttrs,
    /// Backend version tag of the object, opaque: only ever compared for
    /// equality against `StoragePutResult::etag` / `StoragePutConditions::if_match`.
    /// `None` where the backend has no notion of one.
    pub etag: Option<String>,
}

impl StorageObjectInfo {
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            file_type: StorageFileType::Regular,
            size: 0,
            is_complete: true,
            parts: None,
            attrs: StorageFileAttrs::default(),
            etag: None,
        }
    }
}

/// Per-call preconditions for `StorageDest::put_file`. Deliberately NOT part
/// of `StorageFileAttrs`: attrs describe the file and get persisted by dests;
/// a precondition is consumed by the one call and never stored or echoed.
///
/// - `if_none_match`: the put succeeds only if the key does not exist yet
///   (S3 `If-None-Match: *`, FS `O_EXCL`).
/// - `if_match`: the put succeeds only if the current object's etag equals
///   this value (S3 `If-Match`) — the compare-and-swap.
///
/// A violated precondition is `StorageError::PreconditionFailed`. A dest that
/// cannot honour a requested condition must fail with `NotImplemented`, never
/// silently ignore it — see `reject_conditions`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StoragePutConditions {
    pub if_match: Option<String>,
    pub if_none_match: bool,
}

impl StoragePutConditions {
    pub fn if_none_match() -> Self {
        Self {
            if_match: None,
            if_none_match: true,
        }
    }

    pub fn if_match(etag: impl Into<String>) -> Self {
        Self {
            if_match: Some(etag.into()),
            if_none_match: false,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.if_match.is_none() && !self.if_none_match
    }
}

/// For dests without conditional-write support: `Ok` when no condition was
/// asked for, `NotImplemented` otherwise. Call it first thing in `put_file`.
pub fn reject_conditions(conditions: Option<&StoragePutConditions>) -> Result<(), StorageError> {
    match conditions {
        Some(c) if !c.is_empty() => Err(StorageError::NotImplemented("conditional put".into())),
        _ => Ok(()),
    }
}

/// Multipart upload tracking
#[derive(Debug, Clone, Default)]
pub struct StorageMultipartUpload {
    pub plain_key: Option<String>,
    pub encrypted_key: Option<String>,
    pub upload_id: Option<String>,
    pub rand: Option<Vec<u8>>,
}

/// Result of a put operation
#[derive(Debug, Clone)]
pub struct StoragePutResult {
    pub etag: Option<String>,
    pub crc32: Option<Vec<u8>>,
    pub sha256: Option<Vec<u8>>,
    pub storage_key: String,
}

/// Storage statistics
#[derive(Debug, Clone, Default)]
pub struct StorageStatInfo {
    pub meta_state: StorageMetaState, // mirrors TS StorageStatInfo.metaState
    pub total_size: Option<u64>,      // total size of all files on source, actual may be less if not complete
}

impl StorageStatInfo {
    /// Convenience: the metadata is fully known/complete.
    pub fn is_complete(&self) -> bool {
        self.meta_state == StorageMetaState::Complete
    }
}

/// How often a dir-stats walk publishes its running totals. The P2P
/// per-message receive timeout restarts on every block, so emitting this often
/// keeps a long walk from ever tripping it, with wide headroom.
pub const DIR_STATS_PROGRESS_INTERVAL: std::time::Duration = std::time::Duration::from_millis(500);

/// Directory statistics
#[derive(Debug, Clone, Copy, Default)]
pub struct StorageDirStats {
    pub number_of_files: u32,
    pub number_of_folders: u32,
    pub number_of_symlinks: u32,
    pub total_size: u64,
}

/// Storage info metadata
#[derive(Debug, Clone, Default)]
pub struct StorageInfo {
    pub version: Option<u32>,
    pub stats: StorageStatInfo,
    pub ninja_parts: Option<bool>,
    pub zip_batches: Option<bool>,
    pub is_encrypted: Option<bool>,
    pub last_journal_num: Option<u32>,
}

/// Storage errors
#[derive(Error, Debug)]
pub enum StorageError {
    #[error("No such key: {0}")]
    NoSuchKey(String),

    #[error("No part available: {0}")]
    NoPartError(String),

    #[error("Incomplete: {0}")]
    Incomplete(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("S3 error: {0}")]
    S3(String),

    #[error("P2P error: {0}")]
    P2P(String),

    #[error("Cancelled")]
    Cancelled,

    /// The wire under a call died — connect/read timeout, reset, DNS — or
    /// the backend was not reachable to begin with (`BackError::Offline`).
    /// Says nothing about the session or the transfer: the holder waits for
    /// connectivity and retries (docs/app/reconnect.md). Never a per-file
    /// choice and no user code — nothing shows it.
    #[error("transport error: {0}")]
    Transport(String),

    /// The backend transfer session behind this source lapsed (stale-cleanup
    /// timed it out, backend restarted, …). Recoverable: the holder reconnects
    /// with a fresh `download_start` and retries — see the app-layer reopen.
    #[error("Transfer session expired: {0}")]
    SessionExpired(String),

    #[error("Not implemented: {0}")]
    NotImplemented(String),

    /// A `StoragePutConditions` precondition did not hold: the key already
    /// exists (`if_none_match`) or its etag moved (`if_match`). The caller
    /// re-reads and retries; this is the expected outcome of a lost
    /// compare-and-swap, not a fault.
    #[error("Precondition failed: {0}")]
    PreconditionFailed(String),

    #[error("not enough disk space: need {:.1} GB free, have {:.1} GB", *required as f64 / 1e9, *free as f64 / 1e9)]
    NoSpace { required: u64, free: u64 },

    /// The backend refused a storage-metered call with the tier quota
    /// rejection (`409 storage_quota_exceeded`) — the link owner's cloud
    /// storage cap is reached. Not recoverable by retrying the transfer.
    #[error("cloud storage quota exceeded: {0}")]
    CloudQuotaExceeded(String),

    /// The destination filesystem refuses to create symlinks (Android's
    /// FUSE-backed shared storage denies `symlink()` outright; some mounts
    /// behave the same). Carries the object path for the log.
    #[error("destination does not support symlinks: {0}")]
    SymlinkUnsupported(String),

    /// Selection roots the source can never deliver: its meta is `Complete`
    /// yet these keys still resolve `NoSuchKey` (deleted, renamed, or a typo).
    /// Comma-joined list of the missing roots; the user clears it by
    /// deselecting them (docs/core/PartialDownloads.md).
    #[error("selection roots missing from source: {0}")]
    SelectionRootsMissing(String),

    /// The ninja `storage#info` declares a layout version this build does not
    /// read (anything but 2). Carries the declared version; the user's way
    /// out is updating the app.
    #[error("unsupported storage version: {0}")]
    UnsupportedVersion(String),

    /// `get_previews`: this file has no preview (its type, a symlink, a
    /// directory). A fact about the file — the one preview failure a receiver
    /// may cache (docs/core/previews.md).
    #[error("no preview: {0}")]
    NoPreview(String),

    #[error("Other error: {0}")]
    Other(String),

    /// A read ended cleanly with fewer bytes than the meta says the object
    /// has (a connection cut behind a clean stream close, a truncated blob):
    /// a failed read, never a shorter file. Parts report the same as
    /// `NoPartError` (all-or-nothing, retried next pass).
    #[error("short read of {key}: expected {expected} bytes, got {got}")]
    ShortRead { key: String, expected: u64, got: u64 },

    /// A per-key task of a transfer run (file read/put, dir, symlink,
    /// multipart part) failed on `key` with `source`. The message is the
    /// source's, so wire bookkeeping reads as before; `user_code` delegates
    /// to the source, and the app's generic fallback names the key. Mirrors
    /// the TS `TransferKeyError`.
    #[error("{source}")]
    AtKey { key: String, source: Box<StorageError> },
}

impl StorageError {
    /// The key a run failed on, when the failure was a per-key task's.
    pub fn failed_key(&self) -> Option<&str> {
        match self {
            StorageError::AtKey { key, .. } => Some(key),
            _ => None,
        }
    }

    /// Stable machine code + params for errors the user can act on — the UI
    /// translates these via its locale files (`errors.<code>`); everything
    /// else falls back to a generic per-direction wrapper carrying the raw
    /// message. Byte quantities travel as raw byte strings; the UI formats
    /// them for the locale. Mirrors the TS `transferErrorCode`.
    pub fn user_code(&self) -> Option<(&'static str, Vec<(&'static str, String)>)> {
        match self {
            StorageError::NoSpace { required, free } => Some((
                "noSpace",
                vec![("need", required.to_string()), ("have", free.to_string())],
            )),
            StorageError::SelectionRootsMissing(roots) => Some(("selectionMissing", vec![("roots", roots.clone())])),
            // Both mean the read path ran out of sources: `NoPartError` after
            // every source was exhausted, `P2P` when the peer connection
            // failed or timed out (raised only by the receiver-side
            // `p2p_storage`). The user-facing story for each is "the source
            // isn't reachable right now".
            StorageError::NoPartError(_) | StorageError::P2P(_) => Some(("sourceUnavailable", vec![])),
            StorageError::SymlinkUnsupported(_) => Some(("symlinksUnsupported", vec![])),
            StorageError::CloudQuotaExceeded(_) => Some(("storageQuotaExceeded", vec![])),
            StorageError::UnsupportedVersion(v) => Some(("storageVersionUnsupported", vec![("version", v.clone())])),
            StorageError::AtKey { source, .. } => source.user_code(),
            _ => None,
        }
    }
}

/// Storage metadata container — mirrors the TS `StorageMeta`: the entry list plus
/// a single `stat`, from which `is_complete_meta` is derived (no separate flag).
#[derive(Debug, Clone, Default)]
pub struct StorageMeta {
    items: Vec<StorageObjectInfo>,
    stat: StorageStatInfo,
}

impl StorageMeta {
    /// `new StorageMeta(isComplete)` — `metaState` is `Complete` when complete,
    /// else `Unknown`.
    pub fn with_completeness(is_complete: bool) -> Self {
        Self {
            items: Vec::new(),
            stat: StorageStatInfo {
                meta_state: if is_complete {
                    StorageMetaState::Complete
                } else {
                    StorageMetaState::Unknown
                },
                ..Default::default()
            },
        }
    }

    /// `new StorageMeta(_, stat)` — seed from an existing stat (TS passes the
    /// `storage#info` stats here).
    pub fn with_stat(stat: StorageStatInfo) -> Self {
        Self {
            items: Vec::new(),
            stat,
        }
    }

    /// Read-only view of the entries. (TS keeps `_meta` private and reads via
    /// `getDir`/`getAttrs`/`search`; Rust keeps a slice accessor for iteration.)
    pub fn items(&self) -> &[StorageObjectInfo] {
        &self.items
    }

    pub fn stat(&self) -> &StorageStatInfo {
        &self.stat
    }

    /// Mirrors TS `setIsCompleteMeta`: flips `stat.metaState` Complete/Incomplete.
    pub fn set_is_complete_meta(&mut self, is_complete: bool) {
        self.stat.meta_state = if is_complete {
            StorageMetaState::Complete
        } else {
            StorageMetaState::Incomplete
        };
    }

    /// Mirrors TS `isCompleteMeta`: derived from `stat.metaState`.
    pub fn is_complete_meta(&self) -> bool {
        self.stat.meta_state == StorageMetaState::Complete
    }
}

/// Normalize a directory `path` into a key prefix: empty for the root, otherwise
/// guaranteed to end with `/`. Used by `StorageMeta::get_dir`.
fn dir_prefix(path: &str) -> String {
    if path.is_empty() {
        String::new()
    } else if path.ends_with('/') {
        path.to_string()
    } else {
        format!("{}/", path)
    }
}

impl StorageMeta {
    /// In-memory `getDir` over this meta's entries under `path` — the analog of
    /// the TS `StorageMeta.getDir` generator. Clones matched entries into batches
    /// (`&self`: a `StorageMeta` is long-lived and queried repeatedly, unlike the
    /// streaming producers in `Storage::get_dir`). `max_batch_size` caps each batch
    /// (`None`/`0` ⇒ a single batch, matching the TS `maxBatchSize ?? items.length`).
    /// Dropping the receiver cancels the producer.
    pub fn get_dir(
        &self,
        path: &str,
        recursive: bool,
        max_batch_size: Option<usize>,
    ) -> mpsc::Receiver<Result<Vec<StorageObjectInfo>, StorageError>> {
        let prefix = dir_prefix(path);
        let limit = max_batch_size.filter(|n| *n > 0).unwrap_or(usize::MAX);
        let matched: Vec<StorageObjectInfo> = self
            .items
            .iter()
            .filter(|soi| {
                if !soi.key.starts_with(&prefix) {
                    return false;
                }
                let subpath = &soi.key[prefix.len()..];
                !subpath.is_empty() && (recursive || !subpath.contains('/'))
            })
            .cloned()
            .collect();
        let (tx, rx) = mpsc::channel(4);
        tokio::spawn(async move {
            let mut batch: Vec<StorageObjectInfo> = Vec::new();
            for soi in matched {
                batch.push(soi);
                if batch.len() >= limit && tx.send(Ok(std::mem::take(&mut batch))).await.is_err() {
                    return; // receiver dropped — cancel
                }
            }
            if !batch.is_empty() {
                let _ = tx.send(Ok(batch)).await;
            }
        });
        rx
    }

    /// Look up a single entry by exact key. Mirrors TS `StorageMeta.getAttrs`,
    /// returning `None` (not an error) when absent.
    pub fn get_attrs(&self, key: &str) -> Option<StorageObjectInfo> {
        self.items.iter().find(|s| s.key == key).cloned()
    }

    /// Filter entries by path-scope / recursion / name-match, mirroring TS
    /// `StorageMeta.search`, paged over the held entries: the page token is
    /// the offset of the next match, so a page never repeats or skips one
    /// while the meta only grows (a live share appends).
    pub fn search(&self, query: &StorageSearchQuery, options: &SearchOptions) -> Result<SearchResult, StorageError> {
        let page_limit = options
            .page_limit
            .filter(|l| *l > 0)
            .map(|l| l as usize)
            .unwrap_or(1000);
        let offset: usize = match options.page_token.as_deref().filter(|t| !t.is_empty()) {
            Some(t) => t
                .parse()
                .map_err(|_| StorageError::Other(format!("bad search page token: {t}")))?,
            None => 0,
        };
        let recursive = options.recursive.unwrap_or(true);
        // `""` is the root, like no scope at all (the P2P server always sends one).
        let path_scope = options.path_scope.as_deref().filter(|p| !p.is_empty());
        let prefix = path_scope.map(|p| format!("{}/", p)).unwrap_or_default();
        let nm = query
            .name_match
            .as_ref()
            .filter(|s| !s.is_empty())
            .map(|s| s.to_lowercase());

        let matches = self
            .items
            .iter()
            .filter(|soi| {
                if let Some(scope) = path_scope
                    && !soi.key.starts_with(&prefix)
                    && soi.key != scope
                {
                    return false;
                }
                if !recursive {
                    let cut = prefix.len().min(soi.key.len());
                    if soi.key[cut..].contains('/') {
                        return false;
                    }
                }
                if let Some(nm) = &nm {
                    let name = soi.key.rsplit('/').next().unwrap_or(&soi.key);
                    if !name.to_lowercase().contains(nm) {
                        return false;
                    }
                }
                true
            })
            .skip(offset);
        // The page is cut at `page_limit` entries or the byte budget (the
        // first entry always ships), the token pointing at the next match.
        let mut items: Vec<StorageObjectInfo> = Vec::new();
        let mut page_bytes = 0usize;
        let mut cut = false;
        for soi in matches {
            let est = search_entry_wire_estimate(&soi.key);
            let over_bytes = options.max_response_bytes.is_some_and(|max| page_bytes + est > max);
            if items.len() >= page_limit || (!items.is_empty() && over_bytes) {
                cut = true;
                break;
            }
            page_bytes += est;
            items.push(soi.clone());
        }
        Ok(SearchResult {
            next_page_token: cut.then(|| (offset + items.len()).to_string()),
            items,
        })
    }

    /// Directory stats computed directly over the held entries — mirrors the TS
    /// `StorageMeta.getDirStats` (raw `key.startsWith(path)` prefix match; every
    /// matched entry contributes its `size` to `total_size`).
    pub fn get_dir_stats(&self, path: &str) -> StorageDirStats {
        let mut stats = StorageDirStats::default();
        for soi in &self.items {
            if soi.key.starts_with(path) {
                stats.total_size += soi.size;
                match soi.file_type {
                    StorageFileType::Dir => stats.number_of_folders += 1,
                    StorageFileType::Symlink => stats.number_of_symlinks += 1,
                    StorageFileType::Regular => stats.number_of_files += 1,
                    _ => {}
                }
            }
        }
        stats
    }

    /// Upsert an entry by key — mirrors TS `StorageMeta.update`. (Rust takes the
    /// `soi` by value, so the TS "clone to avoid external mutation" concern is moot.)
    pub fn update(&mut self, soi: StorageObjectInfo) {
        match self.items.iter().position(|e| e.key == soi.key) {
            Some(idx) => self.items[idx] = soi,
            None => self.items.push(soi),
        }
    }

    /// Remove an entry by key — mirrors TS `StorageMeta.delete`.
    pub fn delete(&mut self, key: &str) {
        if let Some(idx) = self.items.iter().position(|e| e.key == key) {
            self.items.remove(idx);
        }
    }
}

/// Change type for listUpdates
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeType {
    Add,
    Modify,
    Delete,
}

/// A single change event from listUpdates
#[derive(Debug, Clone)]
pub struct StorageChange {
    pub change_type: ChangeType,
    pub item: StorageObjectInfo,
}

/// Options for listUpdates
#[derive(Debug, Clone, Default)]
pub struct ListUpdatesOptions {
    pub timeout_ms: Option<i32>,
    pub poll_interval_ms: Option<i32>,
    pub close: Option<bool>,
}

/// Result from listUpdates
#[derive(Debug, Clone)]
pub struct ListUpdatesResult {
    pub changes: Vec<StorageChange>,
    pub cursor: String,
    pub has_more: bool,
}

/// Query for search
#[derive(Debug, Clone, Default)]
pub struct StorageSearchQuery {
    pub name_match: Option<String>,
}

/// Options for search
#[derive(Debug, Clone, Default)]
pub struct SearchOptions {
    pub path_scope: Option<String>,
    pub recursive: Option<bool>,
    /// Entries per page. A page that fills comes back with a
    /// `next_page_token`; the walk goes on from there on the next call.
    pub page_limit: Option<i32>,
    pub page_token: Option<String>,
    /// Time budget for this call. A source that can page (`FsSource`) returns
    /// whatever it has when the budget runs out — possibly an empty page —
    /// with a `next_page_token` that resumes the same walk; sources that
    /// answer from memory ignore it. Serving-side only (the P2P file server
    /// sets it so every response lands inside the client's per-message
    /// receive timeout); never on the wire.
    pub deadline: Option<std::time::Duration>,
    /// Approximate wire-size budget for the returned page — every source
    /// cuts the page (with a resume token) before exceeding it, measuring
    /// entries with `search_entry_wire_estimate`, so a served page always
    /// fits one message. Serving-side only.
    pub max_response_bytes: Option<usize>,
}

/// One page of a search. `next_page_token` means the search goes on past
/// this page: the next call with it continues; `None` means it ran out.
#[derive(Debug, Clone)]
pub struct SearchResult {
    pub items: Vec<StorageObjectInfo>,
    pub next_page_token: Option<String>,
}

/// An entry's estimated size on the wire, for `SearchOptions::
/// max_response_bytes`: the key plus a deliberate overestimate of the fixed
/// per-entry wire fields + framing, so an assembled page never exceeds the
/// real message cap.
pub fn search_entry_wire_estimate(key: &str) -> usize {
    key.len() + 96
}

/// Options threaded through `get_v2` / `get_streamed_v2`. Mirrors the TS
/// `StorageGetOptions`. `wait_for_parts=true` tells the source to hard-wait for
/// not-yet-available parts (live incomplete download) instead of failing fast
/// with `NoPartError`; the default (`false`) preserves fail-fast semantics.
/// `wait_cancel` ends that wait early: once it fires the request answers
/// `NoPartError` for the range it is still waiting on (a pause: land what the
/// source can serve now, never wait for the uploader). The default token never
/// fires.
#[derive(Debug, Clone, Default)]
pub struct StorageGetOptions {
    pub wait_for_parts: bool,
    pub wait_cancel: tokio_util::sync::CancellationToken,
}

/// One thumbnail (docs/core/previews.md). `mime_type` is `image/jpeg` or
/// `image/png`, the generator's choice. `etag` is the version of the file it
/// was rendered from — the listing's `StorageObjectInfo.etag` — stated by the
/// source that serves it; a generator leaves it `None`.
#[derive(Debug, Clone)]
pub struct PreviewImage {
    pub bytes: Vec<u8>,
    pub mime_type: String,
    pub etag: Option<String>,
}

/// The platform thumbnailer, injected by the shell — core renders nothing
/// itself. Local sources (`FsSource`, `FsDest`) serve `get_previews` through it.
#[async_trait]
pub trait PreviewGenerator: Send + Sync {
    /// Largest `px` this generator renders; a larger request is capped to it.
    fn max_px(&self) -> u32;

    /// A thumbnail of `file` bounded by `px` on its longer side. `file` is the
    /// locator the FS layer itself opens: a path, or a `content://` URI on
    /// Android. `Ok(None)` = no preview for this file.
    async fn generate(&self, file: &str, px: u32) -> Result<Option<PreviewImage>, String>;
}

/// Core storage trait
#[async_trait]
pub trait Storage: Send + Sync {
    fn cleanup(&mut self) {}

    async fn connect(&mut self) -> Result<StorageMetaState, StorageError>;
    async fn cancel(&self) -> Result<(), StorageError>;

    /// Advance metadata and return the change set since the last call. Mirrors the
    /// TS `Storage.updateMeta` which now returns `StorageChange[]` (was a bool);
    /// the default reports no changes.
    /// `&self`: the transfer engine calls it between passes while spawned part
    /// tasks hold read guards on the same `RwLock`, and `MultiSource` paces the
    /// refresh with a sleep of up to `META_REFRESH_MS` — under a write guard
    /// that sleep stalled every in-flight read and the progress poller.
    async fn update_meta(&self) -> Result<Vec<StorageChange>, StorageError> {
        Ok(Vec::new())
    }

    /// Mint (and keep) GET URLs for `objects` ahead of the reads that will
    /// need them — a hint from a layer that knows what it is about to read
    /// (`NinjaStorageV2` streaming a part span). Only a storage whose reads
    /// go through per-object URLs (`S3Storage`) has anything to do;
    /// the default is a no-op. Never on the critical path: the read's own URL
    /// resolution stays the truth, this only front-loads it. Mirrors TS
    /// `Storage.presignUrls`.
    async fn presign_urls(&self, _objects: &[String]) -> Result<(), StorageError> {
        Ok(())
    }

    async fn meta_stats(&self) -> Result<StorageStatInfo, StorageError> {
        Ok(StorageStatInfo {
            meta_state: StorageMetaState::Complete,
            total_size: None,
        })
    }

    /// Set a callback invoked when the underlying connection is lost.
    /// Only meaningful for connection-based storages (e.g. P2P).
    fn set_on_disconnect(&mut self, _callback: std::sync::Arc<dyn Fn() + Send + Sync>) {
        // Default no-op
    }

    /// List directory contents as a stream of batches — the Rust analog of the
    /// TS async-generator `Storage.getDir`. Each channel item is one batch (or an
    /// `Err`, which ends the stream); `max_batch_size` caps each batch, `None`
    /// emits a single batch. Dropping the receiver cancels the producer.
    ///
    /// The default yields nothing — backends that can list (FsSource, S3Storage,
    /// P2PStorage, NinjaStorageV2) override this. Mirrors the TS `Storage.getDir`
    /// default empty generator.
    /// - path: "" for top-level, or "dir" or "dir/subdir"
    /// - recursive: if true, include all nested items; if false, only immediate children
    /// - start_after: resume cursor — only keys lexicographically AFTER this full
    ///   key (exclusive), in key order. `None` = from the beginning, in whatever
    ///   order the backend lists. Backends that cannot honour it must end the
    ///   stream with `NotImplemented`, never silently list from the start.
    async fn get_dir(
        &self,
        _path: &str,
        _recursive: bool,
        _start_after: Option<&str>,
        _max_batch_size: Option<usize>,
    ) -> mpsc::Receiver<Result<Vec<StorageObjectInfo>, StorageError>> {
        let (_tx, rx) = mpsc::channel(1);
        rx // closed immediately — no batches
    }

    /// Get attributes for a specific path. Default is unimplemented (mirrors the TS
    /// `Storage.getAttrs` which throws "NOT IMPL"); listing backends override it.
    /// `&self`: a read-only status probe — the transfer engine calls it per key
    /// while spawned part tasks hold read guards on the same `RwLock`, so a `&mut`
    /// receiver would serialize enumeration against in-flight uploads.
    async fn get_attrs(&self, _path: &str) -> Result<StorageObjectInfo, StorageError> {
        Err(StorageError::NotImplemented("getAttrs".into()))
    }

    /// Directory statistics for a path, published progressively: each item is
    /// the running total so far and the LAST item is the complete answer. A
    /// caller that only wants the answer drains the channel and keeps the last
    /// item — there is no buffered variant, because a recursive stat of a large
    /// tree outruns the P2P per-message receive timeout and a source that
    /// answers only at the end cannot be served over the wire.
    ///
    /// Items are CUMULATIVE, not deltas. Dropping the receiver cancels the walk.
    ///
    /// The default streams `get_dir(path, true)` and tallies it, mirroring the
    /// TS `Storage.getDirStatsStreamed`: regular files and symlinks contribute
    /// `size`, directories are counted only; totals are published as batches
    /// arrive. Sources with a cheaper answer (in-memory meta) or their own walk
    /// override it.
    async fn get_dir_stats_streamed(&self, path: &str) -> mpsc::Receiver<Result<StorageDirStats, StorageError>> {
        let mut src_rx = self.get_dir(path, true, None, None).await;
        let (tx, rx) = mpsc::channel(4);
        tokio::spawn(async move {
            let mut stats = StorageDirStats::default();
            // Publishing is driven by the CLOCK, not by batch arrival: a walk
            // that stalls (slow disk, deep directory, a peer mid-listing) is
            // exactly the case this exists for, and it produces no batches to
            // hang a check on. A tick with unchanged totals is a keepalive and
            // is meant to go out.
            let mut ticker = tokio::time::interval(DIR_STATS_PROGRESS_INTERVAL);
            ticker.tick().await; // the first tick resolves immediately
            loop {
                tokio::select! {
                    // Consumer dropped → cancel the walk at once rather than at
                    // the next publish.
                    _ = tx.closed() => return,
                    _ = ticker.tick() => {
                        if tx.send(Ok(stats)).await.is_err() {
                            return;
                        }
                    }
                    batch = src_rx.recv() => {
                        let Some(batch) = batch else {
                            break; // listing exhausted
                        };
                        let batch = match batch {
                            Ok(b) => b,
                            Err(e) => {
                                let _ = tx.send(Err(e)).await;
                                return;
                            }
                        };
                        for soi in batch {
                            match soi.file_type {
                                StorageFileType::Regular => {
                                    stats.number_of_files += 1;
                                    stats.total_size += soi.size;
                                }
                                StorageFileType::Symlink => {
                                    stats.number_of_symlinks += 1;
                                    stats.total_size += soi.size;
                                }
                                StorageFileType::Dir => {
                                    stats.number_of_folders += 1;
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
            // The final total always goes out, even when the walk finished
            // before the first tick was due.
            let _ = tx.send(Ok(stats)).await;
        });
        rx
    }

    /// Begin watching for file-system changes at `path`. Returns a cursor.
    async fn list_updates_begin(&self, _path: &str) -> Result<String, StorageError> {
        Err(StorageError::Other("listUpdatesBegin not implemented".into()))
    }

    /// Poll for changes since `cursor`. Supports long-polling via `options.timeout_ms`.
    async fn list_updates(
        &self,
        cursor: &str,
        options: &ListUpdatesOptions,
    ) -> Result<ListUpdatesResult, StorageError> {
        // Handle close request immediately
        if options.close == Some(true) {
            return self.list_updates_imm(cursor, true).await;
        }

        let timeout_ms = options.timeout_ms.unwrap_or(30000) as u64;
        let poll_interval_ms = options.poll_interval_ms.unwrap_or(1000) as u64;
        let start = std::time::Instant::now();
        let mut cur = cursor.to_string();

        while start.elapsed().as_millis() < timeout_ms as u128 {
            let result = self.list_updates_imm(&cur, false).await?;
            if !result.changes.is_empty() {
                return Ok(result);
            }
            cur = result.cursor;
            tokio::time::sleep(std::time::Duration::from_millis(poll_interval_ms)).await;
        }

        Ok(ListUpdatesResult {
            changes: Vec::new(),
            cursor: cur,
            has_more: false,
        })
    }

    /// Immediate (non-blocking) check for changes since `cursor`.
    /// Subclasses implement this for polling-based change detection.
    async fn list_updates_imm(&self, _cursor: &str, _close: bool) -> Result<ListUpdatesResult, StorageError> {
        Err(StorageError::Other("listUpdatesImm not implemented".into()))
    }

    /// Search for items matching `query`. `&self` so callers can hold a read
    /// lock: an `FsSource` search walks the whole scoped tree, and doing that
    /// under the source's write lock would block every concurrent operation
    /// (preview reads, a superseding search) for the walk's duration.
    async fn search(
        &self,
        _query: &StorageSearchQuery,
        _options: &SearchOptions,
    ) -> Result<SearchResult, StorageError> {
        Err(StorageError::Other("search not implemented".into()))
    }

    fn total_bytes_sent(&self) -> u64 {
        0
    }
    fn total_bytes_recv(&self) -> u64 {
        0
    }
    /// The progress counter of a dest that streams to a wire: cumulative
    /// bytes put on it, advanced per body chunk as it goes out and never
    /// migrated anywhere — it steps back only when a failed PUT's bytes are
    /// rolled back to be sent again. The transfer engine reads the bar off
    /// this when the dest is the slow hop (an upload); `None` — the default —
    /// is a local dest, and the engine counts what it received from the
    /// source instead. Sync: a wrapper over an `RwLock`ed inner uses
    /// `try_read()` and serves its last-known value on contention (the
    /// counter is monotonic, so a stale sample is merely late).
    fn bytes_sent_progress(&self) -> Option<u64> {
        None
    }

    fn data_source_id(&self) -> Option<&str>;
    fn set_data_source_id(&mut self, id: String);
}

/// Storage source trait for reading data
#[async_trait]
pub trait StorageSource: Storage {
    fn source_name(&self) -> &str;
    fn source_type(&self) -> StorageSourceType {
        StorageSourceType::S3
    }

    /// Streaming get — THE required read primitive. Delivers the object body as a
    /// sequence of chunks over an mpsc channel, any error as an `Err` item; dropping
    /// the receiver cancels the producer. Mirrors the post-Megasync TS where
    /// `getStreamed` is the one method each source implements and `get`/`getV2` are
    /// base drains (TS deleted per-source `get`). Sources stream for real
    /// (`S3Storage` streams the HTTP body; `NinjaStorageV2` yields per part;
    /// `MultiSource` fails over) — a source with no natural streaming yields a single
    /// chunk.
    async fn get_streamed(
        &self,
        object: &str,
        offset: Option<u64>,
        length: Option<u64>,
    ) -> mpsc::Receiver<Result<Vec<u8>, StorageError>>;

    /// Options-aware streaming get. Mirrors TS `IStorageSource.getStreamedV2`.
    /// The default ignores `options` and delegates to `get_streamed` — only
    /// sources that honor `wait_for_parts` (MultiSource, NinjaStorageV2) override
    /// it. Dropping the receiver cancels the producer.
    async fn get_streamed_v2(
        &self,
        object: &str,
        _options: &StorageGetOptions,
        offset: Option<u64>,
        length: Option<u64>,
    ) -> mpsc::Receiver<Result<Vec<u8>, StorageError>> {
        self.get_streamed(object, offset, length).await
    }

    /// Options-aware buffering get. Mirrors TS `Storage.getV2`: drains
    /// `get_streamed_v2` into a single buffer. Callers that want the whole object
    /// (e.g. `TransferItem` building a part for `put_part`) use this so the
    /// `wait_for_parts` intent threads down to the source.
    async fn get_v2(
        &self,
        object: &str,
        options: &StorageGetOptions,
        offset: Option<u64>,
        length: Option<u64>,
    ) -> Result<Vec<u8>, StorageError> {
        let mut rx = self.get_streamed_v2(object, options, offset, length).await;
        let mut out: Vec<u8> = Vec::with_capacity(length.unwrap_or(0) as usize);
        while let Some(chunk) = rx.recv().await {
            out.extend_from_slice(&chunk?);
        }
        Ok(out)
    }

    /// Buffering get — base drain of the streaming path (TS `Storage.get` →
    /// `getV2`). Provided once here so no source carries its own `get`; sources
    /// implement only `get_streamed`.
    async fn get(&self, object: &str, offset: Option<u64>, length: Option<u64>) -> Result<Vec<u8>, StorageError> {
        self.get_v2(object, &StorageGetOptions::default(), offset, length).await
    }

    async fn get_symlink(&self, object: &str) -> Result<String, StorageError>;

    /// Whether `get_previews` is served — what a `FileServer` over this source
    /// advertises as `PREVIEW_REQUEST`.
    fn serves_previews(&self) -> bool {
        false
    }

    /// Thumbnails of `objects` bounded by `px` on the longer side, each
    /// answered once as `(index into objects, result)` in completion order;
    /// the channel closes after the last. `Err` = the request as a whole is
    /// refused. See docs/core/previews.md.
    async fn get_previews(
        &self,
        _objects: Vec<String>,
        _px: u32,
    ) -> Result<mpsc::Receiver<(usize, Result<PreviewImage, StorageError>)>, StorageError> {
        Err(StorageError::NotImplemented("get_previews".into()))
    }
}

/// Storage destination trait for writing data
#[async_trait]
pub trait StorageDest: Storage {
    fn is_sequential(&self) -> bool;

    /// Whether this dest requires parts within a single multipart to arrive in
    /// strict order. Defaults to `is_sequential()` for backends that haven't
    /// distinguished the two. Class 1 dests (ZipDest, StreamDest) return true
    /// for both. Class 3 dests (S3, Ninja, FS) return false for both. Class 2
    /// dests (future OneDrive/GDrive/Dropbox) would return false from
    /// `is_sequential` and true from `is_sequential_multipart`.
    fn is_sequential_multipart(&self) -> bool {
        self.is_sequential()
    }

    /// Fail the transfer up-front when the destination can already tell it
    /// won't fit: `required_total` is the transfer's total byte size, the
    /// implementation subtracts what it already holds (resume) and its
    /// platform reserve, and returns [`StorageError::NoSpace`]. Default: no
    /// meaningful notion of free space (cloud dests) — always fits. Mirrors
    /// the TS `IStorageDest.checkSpace`.
    async fn check_space(&self, _required_total: u64) -> Result<(), StorageError> {
        Ok(())
    }

    /// `conditions`: per-call preconditions (see `StoragePutConditions`). Dests
    /// without conditional-write support must `reject_conditions` first.
    async fn put_file(
        &self,
        object: &str,
        data: &[u8],
        attrs: Option<&StorageFileAttrs>,
        conditions: Option<&StoragePutConditions>,
    ) -> Result<StoragePutResult, StorageError>;
    async fn put_dir(&self, object: &str, attrs: Option<&StorageFileAttrs>) -> Result<StoragePutResult, StorageError>;
    async fn put_symlink(
        &self,
        object: &str,
        target: &str,
        attrs: Option<&StorageFileAttrs>,
    ) -> Result<StoragePutResult, StorageError>;
    async fn del_file(&self, object: &str) -> Result<(), StorageError>;
    async fn del_dir(&self, object: &str) -> Result<(), StorageError>;
    async fn update_stats(&self, stats: &StorageStatInfo) -> Result<(), StorageError>;
    async fn flush(&self) -> Result<(), StorageError> {
        Ok(()) // Default no-op implementation
    }
    async fn close(&self) -> Result<(), StorageError>;
    async fn finalize(&self) -> Result<(), StorageError> {
        Ok(()) // Default no-op - mark upload as complete
    }
}

/// Multipart upload support trait
#[async_trait]
pub trait StorageDestMultipart: StorageDest {
    async fn list_multiparts_all(&self) -> Result<Vec<StorageMultipartUpload>, StorageError>;
    async fn list_parts_all(&self, upload: &StorageMultipartUpload) -> Result<Vec<StoragePartInfo>, StorageError>;
    /// `size` is the object's declared full length. Dests that journal
    /// metadata record it so readers never have to derive the length from the
    /// parts seen so far.
    async fn begin_multipart(
        &self,
        object: &str,
        size: u64,
        attrs: Option<&StorageFileAttrs>,
    ) -> Result<StorageMultipartUpload, StorageError>;
    async fn put_part(
        &self,
        part_number: u32,
        upload: &StorageMultipartUpload,
        offset: u64,
        data: &[u8],
        is_last_part: bool,
    ) -> Result<StoragePartInfo, StorageError>;
    async fn cancel_multipart(&self, upload: &StorageMultipartUpload) -> Result<(), StorageError>;
    async fn complete_multipart(
        &self,
        upload: &StorageMultipartUpload,
        parts: &[StoragePartInfo],
    ) -> Result<String, StorageError>;
}

/// Check if an S3 part list is complete
pub fn is_complete_s3_part_list(sorted_parts: &[StoragePartInfo], src_size: u64) -> bool {
    if sorted_parts.is_empty() {
        return false;
    }
    let mut sz = 0u64;
    for (part_number, p) in (1u32..).zip(sorted_parts) {
        if p.part_number != Some(part_number) {
            return false;
        }
        sz += p.size.unwrap_or(0);
    }
    sz == src_size
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn reject_conditions_only_rejects_non_empty() {
        assert!(reject_conditions(None).is_ok());
        assert!(reject_conditions(Some(&StoragePutConditions::default())).is_ok());
        assert!(matches!(
            reject_conditions(Some(&StoragePutConditions::if_none_match())),
            Err(StorageError::NotImplemented(_))
        ));
        assert!(matches!(
            reject_conditions(Some(&StoragePutConditions::if_match("\"x\""))),
            Err(StorageError::NotImplemented(_))
        ));
    }

    #[tokio::test]
    async fn test_get_dir_stats_empty() {
        let mut meta = StorageMeta::default();
        meta.set_is_complete_meta(true);

        let stats = calc_dir_stats(&meta, "");
        assert_eq!(stats.number_of_files, 0);
        assert_eq!(stats.number_of_folders, 0);
        assert_eq!(stats.number_of_symlinks, 0);
        assert_eq!(stats.total_size, 0);
    }

    #[tokio::test]
    async fn test_get_dir_stats_root() {
        let mut meta = StorageMeta::default();

        let mut file1 = StorageObjectInfo::new("file1.txt");
        file1.file_type = StorageFileType::Regular;
        file1.size = 100;
        meta.update(file1);

        let mut file2 = StorageObjectInfo::new("file2.txt");
        file2.file_type = StorageFileType::Regular;
        file2.size = 200;
        meta.update(file2);

        let mut dir1 = StorageObjectInfo::new("subdir");
        dir1.file_type = StorageFileType::Dir;
        meta.update(dir1);

        let mut link1 = StorageObjectInfo::new("link1");
        link1.file_type = StorageFileType::Symlink;
        meta.update(link1);

        meta.set_is_complete_meta(true);

        let stats = calc_dir_stats(&meta, "");
        assert_eq!(stats.number_of_files, 2);
        assert_eq!(stats.number_of_folders, 1);
        assert_eq!(stats.number_of_symlinks, 1);
        assert_eq!(stats.total_size, 300);
    }

    #[tokio::test]
    async fn test_get_dir_stats_subdir() {
        let mut meta = StorageMeta::default();

        let mut file1 = StorageObjectInfo::new("file1.txt");
        file1.file_type = StorageFileType::Regular;
        file1.size = 100;
        meta.update(file1);

        let mut subdir = StorageObjectInfo::new("subdir");
        subdir.file_type = StorageFileType::Dir;
        meta.update(subdir);

        let mut subfile1 = StorageObjectInfo::new("subdir/file1.txt");
        subfile1.file_type = StorageFileType::Regular;
        subfile1.size = 50;
        meta.update(subfile1);

        let mut subfile2 = StorageObjectInfo::new("subdir/file2.txt");
        subfile2.file_type = StorageFileType::Regular;
        subfile2.size = 75;
        meta.update(subfile2);

        meta.set_is_complete_meta(true);

        // Stats for root - should include all
        let stats = calc_dir_stats(&meta, "");
        assert_eq!(stats.number_of_files, 3);
        assert_eq!(stats.number_of_folders, 1);
        assert_eq!(stats.total_size, 225);

        // Stats for subdir only
        let stats = calc_dir_stats(&meta, "subdir");
        assert_eq!(stats.number_of_files, 2);
        assert_eq!(stats.number_of_folders, 0);
        assert_eq!(stats.total_size, 125);
    }

    /// Helper function to calculate dir stats from StorageMeta (mirrors the trait method)
    fn calc_dir_stats(meta: &StorageMeta, path: &str) -> StorageDirStats {
        let prefix = if path.is_empty() {
            String::new()
        } else if path.ends_with('/') {
            path.to_string()
        } else {
            format!("{}/", path)
        };

        let mut stats = StorageDirStats::default();
        for soi in meta.items() {
            let matches = if prefix.is_empty() {
                true
            } else {
                soi.key.starts_with(&prefix)
            };

            if matches {
                match soi.file_type {
                    StorageFileType::Regular => {
                        stats.number_of_files += 1;
                        stats.total_size += soi.size;
                    }
                    StorageFileType::Dir => {
                        stats.number_of_folders += 1;
                    }
                    StorageFileType::Symlink => {
                        stats.number_of_symlinks += 1;
                    }
                    _ => {}
                }
            }
        }
        stats
    }

    // ── get_dir_stats_streamed (trait default) ───────────────────────────────

    /// Source whose `get_dir` emits scripted batches, optionally pausing before
    /// each one, and counts how many it actually produced (so a cancelled walk
    /// is observable).
    struct ScriptedSource {
        batches: Vec<Vec<StorageObjectInfo>>,
        /// Pause before emitting batch `i` (index-aligned with `batches`).
        delays: Vec<Duration>,
        /// Yielded as the item after the last batch, ending the stream.
        error_at_end: Option<String>,
        produced: Arc<std::sync::atomic::AtomicUsize>,
    }

    fn file(key: &str, size: u64) -> StorageObjectInfo {
        let mut soi = StorageObjectInfo::new(key);
        soi.file_type = StorageFileType::Regular;
        soi.size = size;
        soi
    }

    fn dir(key: &str) -> StorageObjectInfo {
        let mut soi = StorageObjectInfo::new(key);
        soi.file_type = StorageFileType::Dir;
        soi
    }

    fn symlink(key: &str, size: u64) -> StorageObjectInfo {
        let mut soi = StorageObjectInfo::new(key);
        soi.file_type = StorageFileType::Symlink;
        soi.size = size;
        soi
    }

    #[async_trait]
    impl Storage for ScriptedSource {
        async fn connect(&mut self) -> Result<StorageMetaState, StorageError> {
            Ok(StorageMetaState::Complete)
        }
        async fn cancel(&self) -> Result<(), StorageError> {
            Ok(())
        }
        fn data_source_id(&self) -> Option<&str> {
            None
        }
        fn set_data_source_id(&mut self, _id: String) {}

        async fn get_dir(
            &self,
            _path: &str,
            _recursive: bool,
            _start_after: Option<&str>,
            _max_batch_size: Option<usize>,
        ) -> mpsc::Receiver<Result<Vec<StorageObjectInfo>, StorageError>> {
            let batches = self.batches.clone();
            let delays = self.delays.clone();
            let error_at_end = self.error_at_end.clone();
            let produced = self.produced.clone();
            let (tx, rx) = mpsc::channel(1);
            tokio::spawn(async move {
                for (i, batch) in batches.into_iter().enumerate() {
                    if let Some(d) = delays.get(i) {
                        tokio::time::sleep(*d).await;
                    }
                    if tx.send(Ok(batch)).await.is_err() {
                        return; // consumer dropped — stop producing
                    }
                    produced.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                if let Some(e) = error_at_end {
                    let _ = tx.send(Err(StorageError::Other(e))).await;
                }
            });
            rx
        }
    }

    impl ScriptedSource {
        fn new(batches: Vec<Vec<StorageObjectInfo>>) -> Self {
            Self {
                batches,
                delays: Vec::new(),
                error_at_end: None,
                produced: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            }
        }
    }

    async fn drain(
        rx: &mut mpsc::Receiver<Result<StorageDirStats, StorageError>>,
    ) -> (Vec<StorageDirStats>, Option<StorageError>) {
        let mut items = Vec::new();
        let mut err = None;
        while let Some(block) = rx.recv().await {
            match block {
                Ok(s) => items.push(s),
                Err(e) => {
                    err = Some(e);
                    break;
                }
            }
        }
        (items, err)
    }

    /// The final block is the complete tally, counted per type exactly like the
    /// buffered walk this replaced.
    #[tokio::test]
    async fn streamed_dir_stats_final_block_is_the_complete_tally() {
        let src = ScriptedSource::new(vec![vec![
            file("a.txt", 100),
            file("b.txt", 200),
            dir("sub"),
            symlink("link", 7),
        ]]);

        let mut rx = src.get_dir_stats_streamed("").await;
        let (items, err) = drain(&mut rx).await;

        assert!(err.is_none());
        let last = items.last().expect("at least the final block");
        assert_eq!(last.number_of_files, 2);
        assert_eq!(last.number_of_folders, 1);
        assert_eq!(last.number_of_symlinks, 1);
        assert_eq!(last.total_size, 307, "files + symlink size; dirs contribute none");
    }

    /// A walk that finishes before the first partial is due still yields its
    /// total — a consumer must never get an empty stream.
    #[tokio::test]
    async fn streamed_dir_stats_fast_walk_still_yields_a_block() {
        let src = ScriptedSource::new(vec![vec![file("a.txt", 1)]]);
        let mut rx = src.get_dir_stats_streamed("").await;
        let (items, err) = drain(&mut rx).await;
        assert!(err.is_none());
        assert_eq!(items.len(), 1, "exactly the final block");
        assert_eq!(items[0].number_of_files, 1);
    }

    /// An empty listing still terminates with a zeroed block.
    #[tokio::test]
    async fn streamed_dir_stats_empty_listing_yields_zero_block() {
        let src = ScriptedSource::new(vec![]);
        let mut rx = src.get_dir_stats_streamed("").await;
        let (items, err) = drain(&mut rx).await;
        assert!(err.is_none());
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].number_of_files, 0);
        assert_eq!(items[0].total_size, 0);
    }

    /// A walk that outlives the publish interval emits partials, and every
    /// block is a CUMULATIVE running total (never a delta) ending at the full
    /// tally. This is what keeps the peer's per-message receive timeout from
    /// firing on a long walk.
    #[tokio::test]
    async fn streamed_dir_stats_emits_cumulative_partials() {
        let pause = DIR_STATS_PROGRESS_INTERVAL + Duration::from_millis(100);
        let mut src = ScriptedSource::new(vec![
            vec![file("a.txt", 10), file("b.txt", 10)],
            vec![file("c.txt", 10), file("d.txt", 10)],
        ]);
        // No pause before the first batch; long pause before the second, so the
        // interval elapses and the first batch's total is published.
        src.delays = vec![Duration::ZERO, pause];

        let mut rx = src.get_dir_stats_streamed("").await;
        let (items, err) = drain(&mut rx).await;

        assert!(err.is_none());
        assert!(
            items.len() >= 2,
            "expected a partial before the final, got {}",
            items.len()
        );
        assert_eq!(items[0].number_of_files, 2, "partial = totals so far, not a delta");
        assert_eq!(items[0].total_size, 20);
        let last = items.last().unwrap();
        assert_eq!(last.number_of_files, 4, "final = full tally");
        assert_eq!(last.total_size, 40);
        // Cumulative ⇒ monotonically non-decreasing.
        for pair in items.windows(2) {
            assert!(
                pair[1].number_of_files >= pair[0].number_of_files && pair[1].total_size >= pair[0].total_size,
                "totals must never go backwards"
            );
        }
    }

    /// A STALLED walk still publishes. This is the whole point of streaming:
    /// the peer's per-message receive timeout restarts on every block, so a
    /// walk that produces nothing for a long stretch (slow disk, deep
    /// directory, a peer mid-listing) must still keep the wire busy. Publishing
    /// only when a batch arrives would emit nothing here and the timeout would
    /// fire — the bug this test was written against.
    #[tokio::test]
    async fn streamed_dir_stats_keeps_publishing_while_the_walk_stalls() {
        let stall = DIR_STATS_PROGRESS_INTERVAL * 3;
        let mut src = ScriptedSource::new(vec![vec![file("a.txt", 10)], vec![file("b.txt", 10)]]);
        // Long stall between the two batches, none before the first.
        src.delays = vec![Duration::ZERO, stall];

        let mut rx = src.get_dir_stats_streamed("").await;
        let (items, err) = drain(&mut rx).await;

        assert!(err.is_none());
        // ~3 ticks during the stall (each an unchanged-total keepalive), plus
        // the final block. Without clock-driven publishing there would be 1.
        assert!(
            items.len() >= 3,
            "a {stall:?} stall should publish repeatedly, got {} block(s)",
            items.len()
        );
        // Every block during the stall carries the totals so far.
        assert_eq!(items[0].number_of_files, 1);
        assert_eq!(items.last().unwrap().number_of_files, 2);
    }

    /// The other half of the keepalive guarantee: a walk that streams batches
    /// steadily but runs long must still publish on schedule. A timer restarted
    /// on every batch would keep resetting and a minutes-long walk would
    /// publish nothing until it ended.
    #[tokio::test]
    async fn streamed_dir_stats_keeps_publishing_during_a_steady_long_walk() {
        let batches: Vec<Vec<StorageObjectInfo>> = (0..60).map(|i| vec![file(&format!("f{i}.txt"), 1)]).collect();
        let mut src = ScriptedSource::new(batches);
        // ~1.2s of continuous batches, none of them a stall.
        src.delays = vec![Duration::from_millis(20); 60];

        let mut rx = src.get_dir_stats_streamed("").await;
        let (items, err) = drain(&mut rx).await;

        assert!(err.is_none());
        assert!(
            items.len() >= 2,
            "expected ticks during a long walk, got {}",
            items.len()
        );
        assert_eq!(items.last().unwrap().number_of_files, 60);
    }

    /// A mid-walk source failure surfaces as an `Err` item instead of a
    /// truncated total silently passing for the answer.
    #[tokio::test]
    async fn streamed_dir_stats_propagates_source_error() {
        let mut src = ScriptedSource::new(vec![vec![file("a.txt", 1)]]);
        src.error_at_end = Some("disk on fire".to_string());

        let mut rx = src.get_dir_stats_streamed("").await;
        let (_items, err) = drain(&mut rx).await;

        let err = err.expect("source error must reach the consumer");
        assert!(err.to_string().contains("disk on fire"), "got: {err}");
    }

    /// Dropping the receiver cancels the walk — the producer stops instead of
    /// listing a whole tree nobody is waiting for.
    #[tokio::test]
    async fn streamed_dir_stats_dropping_receiver_cancels_the_walk() {
        let pause = Duration::from_millis(20);
        let batches: Vec<Vec<StorageObjectInfo>> = (0..50).map(|i| vec![file(&format!("f{i}.txt"), 1)]).collect();
        let mut src = ScriptedSource::new(batches);
        src.delays = vec![pause; 50];
        let produced = src.produced.clone();

        let rx = src.get_dir_stats_streamed("").await;
        tokio::time::sleep(pause * 3).await;
        drop(rx);
        let at_drop = produced.load(std::sync::atomic::Ordering::SeqCst);

        // Well past when the remaining 50 batches would have been produced.
        tokio::time::sleep(pause * 20).await;
        let after = produced.load(std::sync::atomic::Ordering::SeqCst);
        assert!(after < 50, "walk kept running after cancel: produced {after}/50");
        assert!(
            after - at_drop <= 2,
            "producer should stop right after the drop (was {at_drop}, now {after})"
        );
    }
}
