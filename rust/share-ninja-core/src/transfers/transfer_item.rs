//! TransferItem — the transfer orchestrator: limiter-bounded parallel parts,
//! per-multipart reordering through [`MultipartReorderDest`], and a
//! multi-pass outer loop for live-incomplete sources.

use async_trait::async_trait;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use tokio::sync::{Mutex, RwLock};
use tokio_util::sync::CancellationToken;

use super::policy::{Conflict, ConflictDecision, ErrorDecision, OnError, OnExisting, TransferPolicies};
use super::{
    ConcurrencyLimiter, MultipartReorderDest, ScopeCompletion, StorageDestMultipart, StorageDirStats, StorageError,
    StorageFileAttrs, StorageFileType, StorageGetOptions, StorageMetaState, StorageMultipartUpload, StorageObjectInfo,
    StoragePartInfo, StoragePutConditions, StorageSource, StorageStatInfo, is_complete_s3_part_list,
};
use crate::utils::speed_calc::SpeedCalc;

fn align_up(x: u64, align: u64) -> u64 {
    x.div_ceil(align) * align
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferItemState {
    Idle,
    Preparing,
    Running,
    /// Parked on `pending_conflicts()` under `OnExisting::Ask`.
    HasConflicts,
    /// Parked on `pending_error()` under `OnError::Ask`; the rest keeps running.
    HasError,
    Error,
    Finished,
    Cancelled,
}

#[derive(Default)]
pub struct TransferItemStats {
    pub total_bytes: AtomicU64,
    pub total_files: AtomicU64,
    pub total_dirs: AtomicU64,
    pub total_symlinks: AtomicU64,
    pub src_bytes_read: AtomicU64,
    /// Bytes taken off the source by `TaskCtx::read`, chunk by chunk, and
    /// not discarded since — the bar's own counter when the dest is local
    /// (see `moved_bytes`). Steps back only for bytes that will be read
    /// again: a part whose put failed, a short read, a hard cancel's drop.
    pub bytes_received: AtomicU64,
    pub bytes_skipped: AtomicU64,
    pub dst_bytes_written: AtomicU64,
    pub dst_files_written: AtomicU64,
    pub dst_dirs_written: AtomicU64,
    pub dst_symlinks_written: AtomicU64,
    /// Entries skipped by the error policy (not resume skips).
    pub files_skipped: AtomicU64,
}

impl TransferItemStats {
    pub fn snapshot(&self) -> TransferItemStatsSnapshot {
        TransferItemStatsSnapshot {
            total_bytes: self.total_bytes.load(Ordering::Relaxed),
            total_files: self.total_files.load(Ordering::Relaxed),
            total_dirs: self.total_dirs.load(Ordering::Relaxed),
            total_symlinks: self.total_symlinks.load(Ordering::Relaxed),
            src_bytes_read: self.src_bytes_read.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
            bytes_skipped: self.bytes_skipped.load(Ordering::Relaxed),
            dst_bytes_written: self.dst_bytes_written.load(Ordering::Relaxed),
            dst_files_written: self.dst_files_written.load(Ordering::Relaxed),
            dst_dirs_written: self.dst_dirs_written.load(Ordering::Relaxed),
            dst_symlinks_written: self.dst_symlinks_written.load(Ordering::Relaxed),
            files_skipped: self.files_skipped.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct TransferItemStatsSnapshot {
    pub total_bytes: u64,
    pub total_files: u64,
    pub total_dirs: u64,
    pub total_symlinks: u64,
    pub src_bytes_read: u64,
    pub bytes_received: u64,
    pub bytes_skipped: u64,
    pub dst_bytes_written: u64,
    pub dst_files_written: u64,
    pub dst_dirs_written: u64,
    pub dst_symlinks_written: u64,
    pub files_skipped: u64,
}

/// Progress snapshot for every UI surface (percent, speed, ETA).
#[derive(Debug, Clone, Default)]
pub struct TransferProgress {
    /// Skipped + committed + in-flight, clamped to `total_bytes`.
    pub bytes: u64,
    /// 0 if not yet known.
    pub total_bytes: u64,
    /// 0..1 (0 when `total_bytes` unknown).
    pub ratio: f64,
    pub bytes_per_sec: f64,
    /// `None` when unknown (no speed / unknown total).
    pub eta_seconds: Option<u64>,
    pub files_written: u64,
    pub dirs_written: u64,
    /// Sizing walk still running: totals are running discovery totals.
    pub preparing: bool,
    pub total_files: u64,
    /// The lowest key with work in flight.
    pub current_file: Option<String>,
    /// Entries skipped by the error policy.
    pub files_skipped: u64,
}

/// Dirs a run's enumeration passed through, per scope (selection root or
/// `""`), dest keys, scope included. A set, so re-enumerating passes are
/// idempotent; with `skipped_keys`/`failed_keys` it is all `dir_completion`
/// needs — on a finished run everything else landed.
#[derive(Default)]
struct DirTracker {
    scopes: std::collections::HashMap<String, std::collections::HashSet<String>>,
}

/// Parent dir of a dest key (`""` for a top-level key).
fn parent_dir(key: &str) -> &str {
    key.rsplit_once('/').map(|(p, _)| p).unwrap_or("")
}

/// Credit `bytes` landed at `dst_key` to every dir above it, `""` included.
fn credit_dir_bytes(map: &std::sync::Mutex<std::collections::HashMap<String, u64>>, dst_key: &str, bytes: u64) {
    if bytes == 0 {
        return;
    }
    let mut m = map.lock().unwrap();
    let mut dir = parent_dir(dst_key);
    loop {
        *m.entry(dir.to_string()).or_insert(0) += bytes;
        if dir.is_empty() {
            break;
        }
        dir = parent_dir(dir);
    }
}

impl DirTracker {
    fn open_scope(&mut self, scope: &str) {
        self.scopes
            .entry(scope.to_string())
            .or_default()
            .insert(scope.to_string());
    }

    /// Stops at the first ancestor already known — amortized O(1) per entry.
    fn note_entry(&mut self, scope: &str, key: &str, is_dir: bool) {
        let dirs = self.scopes.entry(scope.to_string()).or_default();
        if is_dir && !dirs.insert(key.to_string()) {
            return;
        }
        let mut cur = key;
        while cur != scope {
            let parent = parent_dir(cur);
            if !dirs.insert(parent.to_string()) {
                break;
            }
            cur = parent;
        }
    }

    /// Per-scope outcome given the dest keys that did not land: the maximal
    /// complete dirs (a complete subtree collapses to its top).
    fn completion(&self, not_landed: &std::collections::HashSet<String>) -> Vec<ScopeCompletion> {
        let mut out = Vec::with_capacity(self.scopes.len());
        for (scope, dirs) in &self.scopes {
            let mut incomplete: std::collections::HashSet<&str> = std::collections::HashSet::new();
            for key in not_landed {
                if !super::selection::root_covers(scope, key) {
                    continue;
                }
                if dirs.contains(key) {
                    incomplete.insert(key.as_str());
                }
                let parent = parent_dir(key);
                if let Some(p) = dirs.get(parent) {
                    incomplete.insert(p.as_str());
                }
            }
            // Reverse-sorted: every child before its parent.
            let mut sorted: Vec<&str> = dirs.iter().map(|d| d.as_str()).collect();
            sorted.sort_unstable();
            for d in sorted.iter().rev() {
                if *d != scope.as_str() && incomplete.contains(d) {
                    incomplete.insert(parent_dir(d));
                }
            }
            let maximal: Vec<String> = sorted
                .iter()
                .filter(|d| !incomplete.contains(*d))
                .filter(|d| {
                    **d == scope.as_str() || !dirs.contains(parent_dir(d)) || incomplete.contains(parent_dir(d))
                })
                .map(|d| d.to_string())
                .collect();
            out.push(ScopeCompletion {
                scope: scope.clone(),
                complete: !incomplete.contains(scope.as_str()),
                dirs: maximal,
            });
        }
        out.sort_by(|a, b| a.scope.cmp(&b.scope));
        out
    }
}

/// One outer-loop pass's outcome for a single selection root.
enum RootPassOutcome {
    Done,
    /// Absent on the source — unsatisfiable once the source meta turns `Complete`.
    RootMissing,
    /// A transient miss inside the root's tree — retry next pass.
    Retry,
}

/// How a run was told to stop. `Pause` (a flushing cancel) lets parts whose
/// read already completed land before the run ends; `Cancel` drops them.
/// Once set it never goes back to `Running` — a new run is a new item.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum StopState {
    Running = 0,
    Pause = 1,
    Cancel = 2,
}

impl StopState {
    fn load(cell: &AtomicU8) -> Self {
        match cell.load(Ordering::Relaxed) {
            1 => StopState::Pause,
            2 => StopState::Cancel,
            _ => StopState::Running,
        }
    }
}

pub struct TransferItem {
    source: Arc<RwLock<dyn StorageSource>>,
    /// The raw dest, or a [`MultipartReorderDest`] over it for sequential-multipart dests.
    dest: Arc<RwLock<dyn StorageDestMultipart>>,
    /// The un-proxied dest, for `is_sequential*()` / `dest_status` / `check_space`.
    raw_dest: Arc<RwLock<dyn StorageDestMultipart>>,

    dest_min_chunk_size: u64,
    dest_multiparts: Arc<Mutex<Option<Vec<StorageMultipartUpload>>>>,

    limiter: ConcurrencyLimiter,
    /// Normalized include-list (`None` = whole share, empty = nothing).
    /// Immutable per run — selection edits restart the item.
    selection: Option<Vec<String>>,
    /// Source-key prefix stripped off every dest key (`""` = none). Every key
    /// the run touches must lie strictly under it; all per-key bookkeeping is
    /// in dest keys.
    source_root: String,
    /// Sized total per selection root; a root missing on a live source joins
    /// when it first resolves.
    root_totals: std::sync::Mutex<std::collections::HashMap<String, u64>>,
    /// Ancestor dirs already materialized on the dest this run (selection roots only).
    ensured_ancestors: std::sync::Mutex<std::collections::HashSet<String>>,
    /// Dirs the enumeration passed through, per scope — `dir_completion`'s input.
    dir_tracker: std::sync::Mutex<DirTracker>,
    /// Bytes landed this run (written + resume-credited) per dir above the
    /// landing key. Shared with the spawned tasks.
    dir_bytes: Arc<std::sync::Mutex<std::collections::HashMap<String, u64>>>,
    /// Per-key in-flight task counts — backs `active_keys()`.
    active_key_counts: Arc<std::sync::Mutex<std::collections::HashMap<String, u32>>>,

    /// Shared with the spawned tasks; see [`StopState`].
    stop: Arc<AtomicU8>,
    /// Aborts the tasks' in-flight reads on a hard cancel (never on a pause —
    /// a read in flight then finishes and lands, see [`StopState`]). Dropping
    /// a read's receiver cancels just that request's producer; the shared
    /// source itself is never touched.
    read_cancel: CancellationToken,
    /// Ends the tasks' `wait_for_parts` waits on a pause: a read streams what
    /// the source can serve now and answers `NoPartError` for a range the
    /// uploader hasn't produced yet — a pause never waits for the uploader.
    /// Per request, like `read_cancel`; the shared source is never touched.
    wait_cancel: CancellationToken,

    /// First error raised by any spawned task: the limiter cancel that follows
    /// makes every other task observe a bare `Cancelled`, which would mask it.
    root_error: Arc<Mutex<Option<StorageError>>>,

    stats: Arc<TransferItemStats>,
    /// The dest's `bytes_sent_progress` when this item was created — a warm
    /// dest reused across runs keeps counting; this item's share starts here.
    dest_progress_base: u64,
    /// Dest keys already resume-credited or started this run — the
    /// once-per-key guard across outer-loop passes.
    credited_keys: std::sync::Mutex<std::collections::HashSet<String>>,
    state: Arc<Mutex<TransferItemState>>,
    policies: TransferPolicies,
    /// Per-key answers from an `Ask` conflict pass.
    decisions: std::sync::Mutex<std::collections::HashMap<String, OnExisting>>,
    /// The `All(x)` answer of an `Ask` pass — what unlisted (late) keys get.
    ask_default: std::sync::Mutex<OnExisting>,
    pending_conflicts: Arc<Mutex<Vec<Conflict>>>,
    pending_error: Arc<Mutex<Option<(String, String)>>>,
    skipped_keys: Arc<std::sync::Mutex<Vec<String>>>,
    /// Files whose multipart the error policy abandoned: no further parts
    /// are submitted or reported.
    failed_keys: Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
    /// Serializes `OnError::Ask` prompts across concurrently failing units.
    ask_lock: Arc<Mutex<()>>,
    /// 5 s sliding-window speed sampler over `moved_bytes()`.
    dst_speed: Arc<std::sync::Mutex<SpeedCalc>>,
}

impl TransferItem {
    pub async fn new(
        source: Arc<RwLock<dyn StorageSource>>,
        dest: Arc<RwLock<dyn StorageDestMultipart>>,
        selection: Option<Vec<String>>,
        n_streams: usize,
    ) -> Self {
        let want_proxy = dest.read().await.is_sequential_multipart();
        let dest_progress_base = dest.read().await.bytes_sent_progress().unwrap_or(0);

        // Class 1 dests (ZipDest/StreamDest) need parts in order: out-of-order
        // parts buffer in the proxy until their turn.
        let effective_dest: Arc<RwLock<dyn StorageDestMultipart>> = if want_proxy {
            let proxy = Arc::new(MultipartReorderDest::new(dest.clone()));
            Arc::new(RwLock::new(ProxyHandle { inner: proxy }))
        } else {
            dest.clone()
        };

        Self {
            source,
            raw_dest: dest,
            dest: effective_dest,
            dest_min_chunk_size: 10 * 1024 * 1024,
            dest_multiparts: Arc::new(Mutex::new(None)),
            limiter: ConcurrencyLimiter::new(n_streams.max(1)),
            selection,
            source_root: String::new(),
            root_totals: std::sync::Mutex::new(std::collections::HashMap::new()),
            ensured_ancestors: std::sync::Mutex::new(std::collections::HashSet::new()),
            dir_tracker: std::sync::Mutex::new(DirTracker::default()),
            dir_bytes: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            active_key_counts: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            stop: Arc::new(AtomicU8::new(StopState::Running as u8)),
            read_cancel: CancellationToken::new(),
            wait_cancel: CancellationToken::new(),
            root_error: Arc::new(Mutex::new(None)),
            stats: Arc::new(TransferItemStats::default()),
            dest_progress_base,
            credited_keys: std::sync::Mutex::new(std::collections::HashSet::new()),
            state: Arc::new(Mutex::new(TransferItemState::Idle)),
            policies: TransferPolicies::default(),
            decisions: std::sync::Mutex::new(std::collections::HashMap::new()),
            ask_default: std::sync::Mutex::new(OnExisting::Skip),
            pending_conflicts: Arc::new(Mutex::new(Vec::new())),
            pending_error: Arc::new(Mutex::new(None)),
            skipped_keys: Arc::new(std::sync::Mutex::new(Vec::new())),
            failed_keys: Arc::new(std::sync::Mutex::new(std::collections::HashSet::new())),
            ask_lock: Arc::new(Mutex::new(())),
            dst_speed: Arc::new(std::sync::Mutex::new(SpeedCalc::new(5000))),
        }
    }

    pub fn with_policies(mut self, policies: TransferPolicies) -> Self {
        self.policies = policies;
        self
    }

    /// `root` is a source key without leading/trailing slash; set before `start`.
    pub fn with_source_root(mut self, root: impl Into<String>) -> Self {
        self.source_root = root.into();
        self
    }

    /// A key not strictly under `source_root` is a caller error — fail loud.
    fn dst_key(&self, src_key: &str) -> Result<String, StorageError> {
        if self.source_root.is_empty() {
            return Ok(src_key.to_string());
        }
        match src_key
            .strip_prefix(self.source_root.as_str())
            .and_then(|rest| rest.strip_prefix('/'))
        {
            Some(rest) if !rest.is_empty() => Ok(rest.to_string()),
            _ => Err(StorageError::Other(format!(
                "key {} is not under source root {}",
                src_key, self.source_root
            ))),
        }
    }

    /// Inverse of `dst_key` — the source key a dest key came from.
    fn src_key(&self, dst_key: &str) -> String {
        if self.source_root.is_empty() {
            dst_key.to_string()
        } else {
            format!("{}/{}", self.source_root, dst_key)
        }
    }

    /// `dst_key` for a scope: the `source_root` itself maps to the dest root.
    fn dst_scope(&self, scope_path: &str) -> Result<String, StorageError> {
        if scope_path == self.source_root {
            return Ok(String::new());
        }
        self.dst_key(scope_path)
    }

    /// Dirs the run landed whole, per scope (dest keys). Only a `Finished`
    /// run vouches — every enumerated entry landed unless the error policy
    /// skipped it; any other state answers nothing. A file/symlink root is a
    /// scope of its own, complete iff it landed.
    pub async fn dir_completion(&self) -> Vec<ScopeCompletion> {
        if *self.state.lock().await != TransferItemState::Finished {
            return Vec::new();
        }
        let mut not_landed: std::collections::HashSet<String> =
            self.skipped_keys.lock().unwrap().iter().cloned().collect();
        not_landed.extend(self.failed_keys.lock().unwrap().iter().cloned());
        self.dir_tracker.lock().unwrap().completion(&not_landed)
    }

    pub fn limiter(&self) -> &ConcurrencyLimiter {
        &self.limiter
    }

    pub fn stats(&self) -> &Arc<TransferItemStats> {
        &self.stats
    }

    /// The effective dest (reorder proxy if wrapped) — the one to close/flush.
    pub fn get_dest(&self) -> Arc<RwLock<dyn StorageDestMultipart>> {
        self.dest.clone()
    }

    /// Keys with in-flight work right now.
    pub fn active_keys(&self) -> std::collections::HashSet<String> {
        self.active_key_counts.lock().unwrap().keys().cloned().collect()
    }

    /// Sized byte total per selection root (selection runs only).
    pub fn root_totals(&self) -> std::collections::HashMap<String, u64> {
        self.root_totals.lock().unwrap().clone()
    }

    /// Bytes this run has landed under dir `key` (written + resume-credited).
    pub fn dir_bytes_for(&self, key: &str) -> u64 {
        self.dir_bytes.lock().unwrap().get(key).copied().unwrap_or(0)
    }

    /// The bar: resume credit plus one counter that advances per chunk at
    /// the slow hop and never migrates between buckets. A dest that streams
    /// to a wire owns it (`bytes_sent_progress`, less what it had already
    /// sent when this item was created); for a local dest it is what this
    /// item has received from the source (`bytes_received`). Nothing is
    /// summed across owners, so no sample can catch a byte between two of
    /// them — the bar steps back only for bytes that will move again.
    /// Clamped to `total_bytes`.
    pub async fn moved_bytes(&self) -> u64 {
        let s = self.stats.snapshot();
        let sent = self.dest.read().await.bytes_sent_progress();
        let progress = match sent {
            Some(p) => p.saturating_sub(self.dest_progress_base),
            None => s.bytes_received,
        };
        let moved = s.bytes_skipped + progress;
        if s.total_bytes > 0 {
            moved.min(s.total_bytes)
        } else {
            moved
        }
    }

    /// Call on a regular cadence (~1 Hz) so the 5 s window accumulates samples.
    pub async fn current_bytes_per_sec(&self) -> f64 {
        let moved = self.moved_bytes().await;
        let mut sc = self.dst_speed.lock().unwrap();
        sc.update_total(moved);
        sc.avg_bytes_per_sec()
    }

    pub async fn progress(&self) -> TransferProgress {
        let s = self.stats.snapshot();
        let bytes = self.moved_bytes().await;
        let total_bytes = s.total_bytes;
        let bytes_per_sec = self.current_bytes_per_sec().await;
        let preparing = *self.state.lock().await == TransferItemState::Preparing;
        let eta_seconds = if total_bytes > 0 && bytes_per_sec > 0.0 {
            Some((total_bytes.saturating_sub(bytes) as f64 / bytes_per_sec).ceil() as u64)
        } else {
            None
        };
        TransferProgress {
            bytes,
            total_bytes,
            ratio: if total_bytes > 0 {
                bytes as f64 / total_bytes as f64
            } else {
                0.0
            },
            bytes_per_sec,
            eta_seconds,
            files_written: s.dst_files_written,
            dirs_written: s.dst_dirs_written,
            preparing,
            total_files: s.total_files,
            current_file: self.active_keys().into_iter().min(),
            files_skipped: s.files_skipped,
        }
    }

    /// The conflicts the item is parked on while `HasConflicts`.
    pub async fn pending_conflicts(&self) -> Vec<Conflict> {
        self.pending_conflicts.lock().await.clone()
    }

    /// `(key, error)` the item is parked on while `HasError`.
    pub async fn pending_error(&self) -> Option<(String, String)> {
        self.pending_error.lock().await.clone()
    }

    /// Entries the error policy skipped this run.
    pub fn skipped_keys(&self) -> Vec<String> {
        self.skipped_keys.lock().unwrap().clone()
    }

    pub async fn current_state(&self) -> TransferItemState {
        *self.state.lock().await
    }

    /// The run was told to stop (pause or cancel): nothing new is submitted.
    pub fn is_cancelled(&self) -> bool {
        StopState::load(&self.stop) != StopState::Running
    }

    /// Cancelling the dest (the proxy, when wrapped) rejects buffered parts so
    /// their tasks can release limiter slots.
    pub async fn cancel(&self, flush: bool) {
        let want = if flush { StopState::Pause } else { StopState::Cancel };
        if self
            .stop
            .compare_exchange(
                StopState::Running as u8,
                want as u8,
                Ordering::Relaxed,
                Ordering::Relaxed,
            )
            .is_err()
        {
            return;
        }
        tracing::debug!("TransferItem cancel (flush={})", flush);
        // Pause: the in-flight tasks finish and land their parts (a part whose
        // read already completed is written, not thrown away), then the dest
        // flushes so progress persists. What a live source cannot serve yet is
        // not waited for: `wait_cancel` turns every pending `wait_for_parts`
        // wait into `NoPartError` (the part is retried after resume). Cancel:
        // the in-flight reads are aborted at their next chunk — nothing is
        // waited for.
        if flush {
            self.wait_cancel.cancel();
            let _ = self.wait_and_flush().await;
        } else {
            self.read_cancel.cancel();
        }
        self.limiter.cancel(Some("transfer cancelled".into())).await;
        let _ = self.dest.read().await.cancel().await;
    }

    /// Through `raw_dest` — the reorder proxy doesn't forward `get_attrs`.
    async fn dest_status(&self, dst_key: &str) -> Result<Option<StorageObjectInfo>, StorageError> {
        match self.raw_dest.read().await.get_attrs(dst_key).await {
            Ok(soi) => Ok(Some(soi)),
            Err(StorageError::NoSuchKey(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Once per dest key for the whole run.
    fn credit_skipped(&self, dst_key: &str, size: u64) {
        let mut credited = self.credited_keys.lock().unwrap();
        if credited.insert(dst_key.to_string()) {
            self.stats.bytes_skipped.fetch_add(size, Ordering::Relaxed);
            credit_dir_bytes(&self.dir_bytes, dst_key, size);
        }
    }

    async fn dst_multiparts(&self) -> Result<Vec<StorageMultipartUpload>, StorageError> {
        {
            let lock = self.dest_multiparts.lock().await;
            if let Some(ref ms) = *lock {
                return Ok(ms.clone());
            }
        }
        let ms = self.dest.read().await.list_multiparts_all().await?;
        *self.dest_multiparts.lock().await = Some(ms.clone());
        Ok(ms)
    }

    /// Drains the source's streamed dir-stats walk to its last block.
    async fn dir_stats_total(&self, scope_path: &str) -> Result<StorageDirStats, StorageError> {
        // Sizing walks run one at a time and nothing else writes the total_*
        // counters, so earlier roots' totals are stable bases to stack on.
        let base_bytes = self.stats.total_bytes.load(Ordering::Relaxed);
        let base_files = self.stats.total_files.load(Ordering::Relaxed);
        let base_dirs = self.stats.total_dirs.load(Ordering::Relaxed);
        let base_symlinks = self.stats.total_symlinks.load(Ordering::Relaxed);
        let mut rx = self.source.read().await.get_dir_stats_streamed(scope_path).await;
        let mut last: Option<StorageDirStats> = None;
        while let Some(block) = rx.recv().await {
            let s = block?;
            // Published per block so progress() shows the walk live.
            self.stats
                .total_bytes
                .store(base_bytes + s.total_size, Ordering::Relaxed);
            self.stats
                .total_files
                .store(base_files + s.number_of_files as u64, Ordering::Relaxed);
            self.stats
                .total_dirs
                .store(base_dirs + s.number_of_folders as u64, Ordering::Relaxed);
            self.stats
                .total_symlinks
                .store(base_symlinks + s.number_of_symlinks as u64, Ordering::Relaxed);
            last = Some(s);
        }
        last.ok_or_else(|| StorageError::Other("getDirStats: no result".to_string()))
    }

    /// Once per root; `NoSuchKey` propagates so the caller can record the
    /// root as still-missing on a live share.
    async fn size_root(&self, root: &str) -> Result<(), StorageError> {
        let obj = self.source.read().await.get_attrs(root).await?;
        let contributed = match obj.file_type {
            StorageFileType::Regular => {
                self.stats.total_bytes.fetch_add(obj.size, Ordering::Relaxed);
                self.stats.total_files.fetch_add(1, Ordering::Relaxed);
                obj.size
            }
            StorageFileType::Dir => {
                tracing::debug!("calculating size of selection root {}...", root);
                let dir_stats = self.dir_stats_total(root).await?;
                dir_stats.total_size
            }
            StorageFileType::Symlink => {
                self.stats.total_bytes.fetch_add(obj.size, Ordering::Relaxed);
                self.stats.total_symlinks.fetch_add(1, Ordering::Relaxed);
                obj.size
            }
            other => {
                return Err(StorageError::Other(format!(
                    "can't transfer {} - not file, dir or symlink ({:?})",
                    root, other
                )));
            }
        };
        self.root_totals.lock().unwrap().insert(root.to_string(), contributed);
        Ok(())
    }

    /// A selected deep entry never enumerates its parents, so materialize
    /// them. Existence goes through `dest_status` (path map, never raw disk):
    /// an unrelated pre-existing dir must hit the collision policy, not be
    /// adopted. Walks the *dest* key's parents, so nothing inside
    /// `source_root` is created.
    async fn ensure_ancestor_dirs(&self, root: &str) -> Result<(), StorageError> {
        let dst_root = self.dst_key(root)?;
        let Some((parents, _leaf)) = dst_root.rsplit_once('/') else {
            return Ok(());
        };
        let mut prefix = String::new();
        for seg in parents.split('/') {
            if !prefix.is_empty() {
                prefix.push('/');
            }
            prefix.push_str(seg);
            if self.ensured_ancestors.lock().unwrap().contains(&prefix) {
                continue;
            }
            match self.dest_status(&prefix).await? {
                Some(d) if d.file_type != StorageFileType::Dir => {
                    return Err(StorageError::Other(format!(
                        "file type mismatch for ancestor {}: dest {:?} vs source Dir",
                        prefix, d.file_type
                    )));
                }
                Some(_) => {}
                None => {
                    let src_prefix = self.src_key(&prefix);
                    let attrs = match self.source.read().await.get_attrs(&src_prefix).await {
                        Ok(o) => Some(o.attrs),
                        Err(_) => None,
                    };
                    self.dest.read().await.put_dir(&prefix, attrs.as_ref()).await?;
                    self.stats.dst_dirs_written.fetch_add(1, Ordering::Relaxed);
                }
            }
            self.ensured_ancestors.lock().unwrap().insert(prefix.clone());
        }
        Ok(())
    }

    /// Fatal errors propagate; per-root misses map into [`RootPassOutcome`]
    /// so the other roots keep progressing. A root the source can't size yet
    /// (`Incomplete`: a still-uploading cloud share with no peer) is still
    /// transferred this pass and sized on a later one — no partial total is
    /// ever recorded, the pass just asks for another round.
    async fn run_root_pass(&self, root: &str) -> Result<RootPassOutcome, StorageError> {
        let sized = if self.root_totals.lock().unwrap().contains_key(root) {
            true
        } else {
            match self.size_root(root).await {
                Ok(()) => true,
                Err(StorageError::NoSuchKey(_)) => return Ok(RootPassOutcome::RootMissing),
                Err(StorageError::Incomplete(_)) => {
                    tracing::debug!("selection root {} not sizable yet (source incomplete)", root);
                    false
                }
                Err(e) => return Err(e),
            }
        };
        self.ensure_ancestor_dirs(root).await?;
        let obj = match self.source.read().await.get_attrs(root).await {
            Ok(o) => o,
            Err(StorageError::NoSuchKey(_)) => return Ok(RootPassOutcome::RootMissing),
            Err(e) => return Err(e),
        };
        let res = if obj.file_type == StorageFileType::Dir {
            match self.transfer_item(&obj).await {
                Ok(()) => self.run_directory_pass(root).await,
                Err(e) => Err(e),
            }
        } else {
            // A file/symlink root is a scope of its own: it reports itself
            // complete when it landed (a skipped/failed key makes it
            // incomplete), so a landed file selection reads as landed
            // exactly like a dir one.
            self.dir_tracker.lock().unwrap().open_scope(&self.dst_scope(root)?);
            self.transfer_item(&obj).await
        };
        match res {
            Ok(()) if sized => Ok(RootPassOutcome::Done),
            Ok(()) => Ok(RootPassOutcome::Retry),
            Err(e @ (StorageError::NoSuchKey(_) | StorageError::NoPartError(_))) => {
                tracing::debug!("root {} pass error, will refresh meta and retry: {}", root, e);
                Ok(RootPassOutcome::Retry)
            }
            Err(e) => {
                tracing::debug!("fatal pass error on root {}, throwing: {}", root, e);
                Err(e)
            }
        }
    }

    pub async fn start(&self) -> Result<(), StorageError> {
        *self.state.lock().await = TransferItemState::Preparing;

        let outer = self.start_inner().await;

        if self.is_cancelled() {
            // The run is not over while its part tasks are: a pause lands
            // the reads that were in flight, a hard cancel aborts them at
            // their next chunk — either way the caller (the session's
            // `file_end`, the app's landed-bytes read) sees the dest as it
            // is once they are gone. `await_all` answers `Cancelled` on a
            // stopped limiter; that is the verdict already in hand.
            let _ = self.limiter.await_all().await;
            *self.state.lock().await = TransferItemState::Cancelled;
            return Ok(());
        }

        match outer {
            Ok(()) => {
                // A spawned task may have failed without the orchestrator
                // observing its `Err`.
                if let Some(root) = self.root_error.lock().await.take() {
                    *self.state.lock().await = TransferItemState::Error;
                    return Err(root);
                }
                *self.state.lock().await = TransferItemState::Finished;
                Ok(())
            }
            Err(e) => {
                // `e` is usually the limiter's cascade `Cancelled`, not the cause.
                let real = self.root_error.lock().await.take().unwrap_or(e);
                *self.state.lock().await = TransferItemState::Error;
                Err(real)
            }
        }
    }

    async fn start_inner(&self) -> Result<(), StorageError> {
        let src_stats = self.source.read().await.meta_stats().await?;
        tracing::debug!("metaState: {}", src_stats.meta_state.as_i32());

        // ── Sizing ──
        // A whole share the source can't size yet (no declared total and an
        // `Incomplete` walk — a still-uploading cloud share with no peer):
        // the total stays unknown, the passes still move bytes, and the walk
        // is retried each round until the source turns complete. Selection
        // roots get the same treatment inside `run_root_pass`.
        let mut whole_unsized = false;
        match self.selection.as_deref() {
            None => {
                if let Some(total_size) = src_stats.total_size {
                    tracing::debug!("already know size...");
                    self.stats.total_bytes.store(total_size, Ordering::Relaxed);
                } else {
                    tracing::debug!("calculating size...");
                    match self.dir_stats_total("").await {
                        Ok(_) => {}
                        Err(StorageError::Incomplete(_)) => {
                            tracing::debug!("share not sizable yet (source incomplete)");
                            whole_unsized = true;
                        }
                        Err(e) => return Err(e),
                    }
                }
            }
            Some([]) => {
                tracing::debug!("empty selection, nothing to transfer");
                *self.state.lock().await = TransferItemState::Running;
                self.update_stats(true).await?;
                self.dest.read().await.flush().await?;
                return Ok(());
            }
            // A root the live source doesn't know yet, or can't size yet,
            // stays out of `root_totals`; the passes re-size it until it
            // resolves.
            Some(roots) => {
                for root in roots {
                    match self.size_root(root).await {
                        Ok(()) => {}
                        Err(StorageError::NoSuchKey(_)) => {
                            tracing::debug!("selection root {} not on source yet", root);
                        }
                        Err(StorageError::Incomplete(_)) => {
                            tracing::debug!("selection root {} not sizable yet (source incomplete)", root);
                        }
                        Err(e) => return Err(e),
                    }
                }
            }
        }

        // `OnExisting::Ask` is answered once for the whole batch, before bytes move.
        self.conflict_pass().await?;
        if self.is_cancelled() {
            return Ok(());
        }

        *self.state.lock().await = TransferItemState::Running;

        self.update_stats(false).await?;

        // The space gate needs the whole total; while any part of the
        // sizing is deferred it waits for the pass that completes it.
        let mut space_checked = false;

        // Each pass transfers what's available; `NoSuchKey` / `NoPartError`
        // mean "retry after the source meta advances". Pacing lives in
        // `source.update_meta()` and `get_v2(wait_for_parts)`.
        loop {
            if self.is_cancelled() {
                break;
            }
            let mut needs_another_pass = false;
            let mut missing_roots: Vec<String> = Vec::new();

            if whole_unsized {
                match self.dir_stats_total("").await {
                    Ok(_) => whole_unsized = false,
                    Err(StorageError::Incomplete(_)) => needs_another_pass = true,
                    Err(e) => return Err(e),
                }
            }
            // Through `raw_dest` — the reorder proxy doesn't forward it.
            // Selection roots still unsized re-run this on a later pass
            // (their `Retry` keeps the loop going).
            if !space_checked && !whole_unsized {
                let total = self.stats.total_bytes.load(Ordering::Relaxed);
                if total > 0 {
                    self.raw_dest.read().await.check_space(total).await?;
                    space_checked = true;
                }
            }

            match self.selection.as_deref() {
                None | Some([]) => {
                    if let Err(e) = self.run_directory_pass("").await {
                        match e {
                            StorageError::NoSuchKey(_) | StorageError::NoPartError(_) => {
                                tracing::debug!("pass error, will refresh meta and retry: {}", e);
                                needs_another_pass = true;
                            }
                            _ => {
                                tracing::debug!("fatal pass error, throwing: {}", e);
                                return Err(e);
                            }
                        }
                    }
                }
                Some(roots) => {
                    for root in roots {
                        if self.is_cancelled() {
                            break;
                        }
                        match self.run_root_pass(root).await? {
                            RootPassOutcome::Done => {}
                            RootPassOutcome::RootMissing => {
                                tracing::debug!("selection root {} still missing", root);
                                missing_roots.push(root.clone());
                                needs_another_pass = true;
                            }
                            RootPassOutcome::Retry => {
                                needs_another_pass = true;
                            }
                        }
                    }
                }
            }

            if self.is_cancelled() {
                break;
            }

            let source_complete = self.source_meta_state().await == StorageMetaState::Complete;
            // Source done and these keys never appeared: fail loud rather than
            // spin or silently finish without them.
            if source_complete && !missing_roots.is_empty() {
                return Err(StorageError::SelectionRootsMissing(missing_roots.join(", ")));
            }
            if source_complete && !needs_another_pass {
                tracing::debug!("transfer finished with complete meta");
                break;
            }

            // Timer-gated inside MultiSource, so this loop can't busy-spin.
            // Read guard: the refresh sleeps up to META_REFRESH_MS, and the
            // in-flight part tasks / progress polls read through this lock.
            let changes = self.source.read().await.update_meta().await.unwrap_or_default();
            let source_complete_after = self.source_meta_state().await == StorageMetaState::Complete;
            tracing::debug!(
                "source meta progressed (changes={}, complete={})",
                changes.len(),
                source_complete_after
            );
        }

        if self.is_cancelled() {
            return Ok(());
        }

        // Complete is stamped BEFORE the flush so the ninja dest lands the
        // final journal and the Complete info in one write; the flush error
        // must propagate — it is what makes the upload real.
        self.limiter.await_all().await?;
        tracing::debug!("limiter awaited");
        self.update_stats(true).await?;
        self.dest.read().await.flush().await?;
        tracing::debug!("dest flushed");
        tracing::debug!("transfer finished");
        Ok(())
    }

    /// The pause path's tail: flush without stamping Complete.
    async fn wait_and_flush(&self) -> Result<(), StorageError> {
        self.limiter.await_all().await?;
        tracing::debug!("limiter awaited");
        let _ = self.dest.read().await.flush().await;
        tracing::debug!("dest flushed");
        Ok(())
    }

    /// Dirs/symlinks/complete files transfer as enumerated; incomplete
    /// regulars go last (their parts wait inside `get_v2(wait_for_parts)`).
    async fn run_directory_pass(&self, scope_path: &str) -> Result<(), StorageError> {
        let mut rx = {
            let src = self.source.read().await;
            src.get_dir(scope_path, true, None, None).await
        };

        let dst_scope = self.dst_scope(scope_path)?;
        self.dir_tracker.lock().unwrap().open_scope(&dst_scope);

        let mut pending_incompletes: Vec<StorageObjectInfo> = Vec::new();

        'outer: while let Some(batch) = rx.recv().await {
            for soi in batch? {
                if self.is_cancelled() {
                    break 'outer;
                }

                if soi.file_type != StorageFileType::Info {
                    let dst = self.dst_key(&soi.key)?;
                    self.dir_tracker.lock().unwrap().note_entry(
                        &dst_scope,
                        &dst,
                        soi.file_type == StorageFileType::Dir,
                    );
                }

                let is_incomplete_regular = soi.file_type == StorageFileType::Regular && !soi.is_complete;

                if is_incomplete_regular {
                    let dst_key = self.dst_key(&soi.key)?;
                    match self.dest_status(&dst_key).await? {
                        Some(d) if d.file_type != soi.file_type => {
                            return Err(StorageError::Other(format!(
                                "file type mismatch for {}: dest {:?} vs source {:?}",
                                dst_key, d.file_type, soi.file_type
                            )));
                        }
                        Some(d) if d.is_complete => {
                            if self.resolve_existing(&soi, &dst_key, &d).await? {
                                pending_incompletes.push(soi);
                            }
                        }
                        _ => pending_incompletes.push(soi),
                    }
                } else {
                    self.transfer_item(&soi).await?;
                }
            }
        }

        if !self.is_cancelled() {
            tracing::debug!("processing {} incompletes", pending_incompletes.len());
            for soi in pending_incompletes {
                if self.is_cancelled() {
                    break;
                }
                self.transfer_item(&soi).await?;
            }
        }

        Ok(())
    }

    /// The outer loop's stop gate: Complete whenever P2P is present, else the
    /// ninja journal's completeness.
    async fn source_meta_state(&self) -> StorageMetaState {
        self.source
            .read()
            .await
            .meta_stats()
            .await
            .map(|s| s.meta_state)
            .unwrap_or(StorageMetaState::Unknown)
    }

    async fn transfer_item(&self, src: &StorageObjectInfo) -> Result<(), StorageError> {
        if src.file_type == StorageFileType::Info {
            return Ok(());
        }

        let dst_key = self.dst_key(&src.key)?;
        match self.dest_status(&dst_key).await? {
            Some(d) if d.file_type != src.file_type => {
                return Err(StorageError::Other(format!(
                    "file type mismatch for {}: dest {:?} vs source {:?}",
                    dst_key, d.file_type, src.file_type
                )));
            }
            // An existing dir is merged into, never a conflict.
            Some(d) if d.is_complete && src.file_type == StorageFileType::Dir => return Ok(()),
            // An existing complete file/symlink: the existing-entry policy decides.
            Some(d) if d.is_complete && !self.resolve_existing(src, &dst_key, &d).await? => return Ok(()),
            _ => {}
        }

        // Marked before transferring: a later pass re-sees what this run wrote
        // and must not credit it as resumed.
        let fresh = self.credited_keys.lock().unwrap().insert(dst_key);

        self.transfer_one_item(src, fresh).await
    }

    /// Per-key `Ask` answer, else the `All` answer, else the policy.
    fn action_for(&self, key: &str) -> OnExisting {
        if let Some(a) = self.decisions.lock().unwrap().get(key) {
            return *a;
        }
        match self.policies.on_existing {
            OnExisting::Ask => *self.ask_default.lock().unwrap(),
            a => a,
        }
    }

    /// `true` = transfer (the existing dest entry was deleted); `false` = the
    /// dest entry stays, credited as resume-skipped.
    async fn resolve_existing(
        &self,
        src: &StorageObjectInfo,
        dst_key: &str,
        dest: &StorageObjectInfo,
    ) -> Result<bool, StorageError> {
        let action = match self.action_for(dst_key) {
            OnExisting::KeepNewer => match (src.attrs.mtime, dest.attrs.mtime) {
                (Some(s), Some(d)) if s > d => OnExisting::Overwrite,
                (Some(_), None) => OnExisting::Overwrite,
                _ => OnExisting::Skip,
            },
            OnExisting::KeepLarger => {
                if src.size > dest.size {
                    OnExisting::Overwrite
                } else {
                    OnExisting::Skip
                }
            }
            OnExisting::IfSizeDiffers => {
                if src.size != dest.size {
                    OnExisting::Overwrite
                } else {
                    OnExisting::Skip
                }
            }
            a => a,
        };
        match action {
            OnExisting::Overwrite => {
                tracing::debug!("overwriting existing {}", dst_key);
                self.raw_dest.read().await.del_file(dst_key).await?;
                Ok(true)
            }
            _ => {
                if matches!(src.file_type, StorageFileType::Regular | StorageFileType::Symlink) {
                    self.credit_skipped(dst_key, dest.size);
                }
                Ok(false)
            }
        }
    }

    async fn conflict_pass(&self) -> Result<(), StorageError> {
        if self.policies.on_existing != OnExisting::Ask {
            return Ok(());
        }
        let Some(resolver) = self.policies.conflict_resolver.clone() else {
            return Ok(());
        };
        let roots: Vec<String> = match self.selection.as_deref() {
            None => vec![String::new()],
            Some(roots) => roots.to_vec(),
        };
        let mut conflicts: Vec<Conflict> = Vec::new();
        for root in roots {
            let mut entries: Vec<StorageObjectInfo> = Vec::new();
            let mut walk = root.is_empty();
            if !root.is_empty() {
                match self.source.read().await.get_attrs(&root).await {
                    Ok(soi) => {
                        walk = soi.file_type == StorageFileType::Dir;
                        entries.push(soi);
                    }
                    Err(StorageError::NoSuchKey(_)) => continue,
                    Err(e) => return Err(e),
                }
            }
            if walk {
                let mut rx = self.source.read().await.get_dir(&root, true, None, None).await;
                while let Some(batch) = rx.recv().await {
                    entries.extend(batch?);
                }
            }
            for soi in entries {
                if !matches!(soi.file_type, StorageFileType::Regular | StorageFileType::Symlink) {
                    continue;
                }
                let dst_key = self.dst_key(&soi.key)?;
                if let Some(d) = self.dest_status(&dst_key).await?
                    && d.is_complete
                    && d.file_type == soi.file_type
                {
                    conflicts.push(Conflict {
                        key: dst_key,
                        source: soi,
                        dest: d,
                    });
                }
            }
        }
        if conflicts.is_empty() {
            return Ok(());
        }
        tracing::debug!("{} conflicts, asking", conflicts.len());
        *self.pending_conflicts.lock().await = conflicts.clone();
        *self.state.lock().await = TransferItemState::HasConflicts;
        let decision = resolver.resolve(conflicts).await;
        *self.state.lock().await = TransferItemState::Preparing;
        self.pending_conflicts.lock().await.clear();
        match decision {
            ConflictDecision::Cancel => {
                tracing::debug!("conflicts: cancelled by resolver");
                self.stop.store(StopState::Cancel as u8, Ordering::Relaxed);
            }
            ConflictDecision::All(a) => *self.ask_default.lock().unwrap() = a,
            ConflictDecision::Each(map) => *self.decisions.lock().unwrap() = map,
        }
        Ok(())
    }

    async fn transfer_one_item(&self, src: &StorageObjectInfo, fresh: bool) -> Result<(), StorageError> {
        tracing::debug!("begin transferring {}", src.key);
        match src.file_type {
            StorageFileType::Regular => {
                let dest_seq_multi = self.dest.read().await.is_sequential_multipart();
                if !src.is_complete {
                    if !dest_seq_multi {
                        self.transfer_src_ninja_parts(src, fresh).await?;
                    } else {
                        self.transfer_sequental_into_parts(src, true, fresh).await?;
                    }
                } else if src.size <= self.dest_min_chunk_size {
                    self.transfer_small(src).await?;
                } else {
                    tracing::debug!("have complete {} before S3", src.key);
                    self.transfer_sequental_into_parts(src, dest_seq_multi, fresh).await?;
                    self.stats.dst_files_written.fetch_add(1, Ordering::Relaxed);
                }
            }
            StorageFileType::Dir => {
                self.put_dir(src).await?;
            }
            StorageFileType::Symlink => {
                self.put_symlink(src).await?;
            }
            _ => {
                return Err(StorageError::Other(format!(
                    "Unknown file type {:?} for {}",
                    src.file_type, src.key
                )));
            }
        }
        Ok(())
    }

    async fn transfer_small(&self, src: &StorageObjectInfo) -> Result<(), StorageError> {
        self.limiter.acquire().await?;

        let task = TaskCtx::from(self);
        let src_obj = src.clone();
        let dst_key = self.dst_key(&src.key)?;
        let label = format!("file {}", src.key);

        task.key_begin(&dst_key);
        tokio::spawn(async move {
            loop {
                match task.run_transfer_small(src_obj.clone(), dst_key.clone()).await {
                    Ok(()) => break,
                    Err(e) => {
                        if task.on_unit_error(&dst_key, &label, e).await != ErrorDecision::Retry {
                            break;
                        }
                    }
                }
            }
            task.key_end(&dst_key);
            task.limiter.release().await;
        });

        Ok(())
    }

    async fn put_dir(&self, src: &StorageObjectInfo) -> Result<(), StorageError> {
        self.limiter.acquire().await?;
        let task = TaskCtx::from(self);
        let dst_key = self.dst_key(&src.key)?;
        let src_attrs = src.attrs.clone();
        let label = format!("dir {}", src.key);

        task.key_begin(&dst_key);
        tokio::spawn(async move {
            loop {
                let result: Result<(), StorageError> = async {
                    task.dest.read().await.put_dir(&dst_key, Some(&src_attrs)).await?;
                    task.stats.dst_dirs_written.fetch_add(1, Ordering::Relaxed);
                    Ok(())
                }
                .await;
                match result {
                    Ok(()) => break,
                    Err(e) => {
                        if task.on_unit_error(&dst_key, &label, e).await != ErrorDecision::Retry {
                            break;
                        }
                    }
                }
            }
            task.key_end(&dst_key);
            task.limiter.release().await;
        });
        Ok(())
    }

    async fn put_symlink(&self, src: &StorageObjectInfo) -> Result<(), StorageError> {
        self.limiter.acquire().await?;
        let task = TaskCtx::from(self);
        let src_obj = src.clone();
        let dst_key = self.dst_key(&src.key)?;
        let label = format!("symlink {}", src.key);

        task.key_begin(&dst_key);
        tokio::spawn(async move {
            loop {
                let result: Result<(), StorageError> = async {
                    let target = task.source.read().await.get_symlink(&src_obj.key).await?;
                    task.dest
                        .read()
                        .await
                        .put_symlink(&dst_key, &target, Some(&src_obj.attrs))
                        .await?;
                    // Symlinks weigh their soi size (lstat = target length)
                    // in total_bytes; the same size feeds the written and
                    // received counters so the bar closes.
                    task.stats.dst_bytes_written.fetch_add(src_obj.size, Ordering::Relaxed);
                    task.stats.bytes_received.fetch_add(src_obj.size, Ordering::Relaxed);
                    task.stats.dst_symlinks_written.fetch_add(1, Ordering::Relaxed);
                    credit_dir_bytes(&task.dir_bytes, &dst_key, target.len() as u64);
                    Ok(())
                }
                .await;
                match result {
                    Ok(()) => break,
                    Err(e) => {
                        if task.on_unit_error(&dst_key, &label, e).await != ErrorDecision::Retry {
                            break;
                        }
                    }
                }
            }
            task.key_end(&dst_key);
            task.limiter.release().await;
        });
        Ok(())
    }

    /// Parts are submitted in strict numeric order; `wait_for_parts` makes a
    /// live-incomplete source block on availability instead of failing.
    async fn transfer_sequental_into_parts(
        &self,
        src: &StorageObjectInfo,
        wait_for_parts: bool,
        fresh: bool,
    ) -> Result<(), StorageError> {
        tracing::debug!("begin transfer sequental multipart {}", src.key);

        let dst_key = self.dst_key(&src.key)?;
        let multiparts = self.dst_multiparts().await?;
        let mut upload_info: Option<StorageMultipartUpload> = multiparts
            .iter()
            .find(|e| e.plain_key.as_deref() == Some(dst_key.as_str()))
            .cloned();

        let mut existing_parts: Vec<StoragePartInfo> = Vec::new();
        let mut part_size: Option<u64> = None;

        if let Some(ref upload) = upload_info {
            existing_parts = self.dest.read().await.list_parts_all(upload).await?;
            existing_parts.sort_by_key(|p| p.part_number);
            if !existing_parts.is_empty() {
                part_size = self.guess_part_size(&existing_parts);
            }
            if part_size.is_none() {
                tracing::debug!("cannot guess part size, cancelling and reuploading");
                self.dest.read().await.cancel_multipart(upload).await?;
                let mut mps = self.dest_multiparts.lock().await;
                if let Some(ms) = mps.as_mut() {
                    ms.retain(|m| m.upload_id != upload.upload_id);
                }
                upload_info = None;
                existing_parts.clear();
            }
        }

        // Gated on `fresh`, not `credit_skipped`: `transfer_item` already
        // marked the key.
        if fresh {
            let existing_bytes: u64 = existing_parts.iter().filter_map(|p| p.size).sum();
            self.stats.bytes_skipped.fetch_add(existing_bytes, Ordering::Relaxed);
            credit_dir_bytes(&self.dir_bytes, &dst_key, existing_bytes);
        }

        if self.is_cancelled() {
            return Ok(());
        }

        let chunk_size = if let Some(ps) = part_size {
            ps
        } else {
            let cs = align_up(src.size / 10000, 1024).max(self.dest_min_chunk_size);
            tracing::debug!("chunk size {}", cs);
            cs
        };

        // Fully-sequential dests: a second multipart's part downloads could
        // hold every limiter slot while the first is still draining — deadlock.
        if self.dest.read().await.is_sequential() {
            self.limiter.await_all().await?;
        }

        if upload_info.is_none() {
            let new_upload = self
                .dest
                .read()
                .await
                .begin_multipart(&dst_key, src.size, Some(&src.attrs))
                .await?;
            {
                let mut mps = self.dest_multiparts.lock().await;
                if let Some(ms) = mps.as_mut() {
                    ms.push(new_upload.clone());
                } else {
                    *mps = Some(vec![new_upload.clone()]);
                }
            }
            upload_info = Some(new_upload);
        }

        let upload = upload_info.expect("upload_info set above");

        let mut pos = 0u64;
        let mut part_number = 1u32;
        let mut submitted = 0u32;

        let parts_state: Arc<Mutex<Vec<StoragePartInfo>>> = Arc::new(Mutex::new(existing_parts));
        let src_size = src.size;

        while pos < src_size {
            if self.is_cancelled() || self.failed_keys.lock().unwrap().contains(&dst_key) {
                return Ok(());
            }

            let existing = parts_state
                .lock()
                .await
                .iter()
                .any(|p| p.part_number == Some(part_number));
            if existing {
                pos += chunk_size;
                part_number += 1;
                continue;
            }

            let length = chunk_size.min(src_size - pos);
            let is_last = pos + length >= src_size;

            // Acquire before spawning: the current part is always in flight.
            self.limiter.acquire().await?;
            // A pause may have landed while this acquire waited on a slot:
            // nothing new is submitted after the stop.
            if self.is_cancelled() {
                self.limiter.release().await;
                return Ok(());
            }

            let task = TaskCtx::from(self);
            let upload_info = upload.clone();
            let parts_state = parts_state.clone();
            let src_obj = src.clone();
            let dst_key_inner = dst_key.clone();
            let label = format!("{} part#{} @{}+{}", src.key, part_number, pos, length);

            task.key_begin(&dst_key_inner);
            tokio::spawn(async move {
                loop {
                    let result: Result<(), StorageError> = async {
                        let data = task
                            .read(
                                &src_obj.key,
                                &StorageGetOptions {
                                    wait_for_parts,
                                    wait_cancel: task.wait_cancel.clone(),
                                },
                                Some(pos),
                                Some(length),
                            )
                            .await?;
                        // A part is all-or-nothing: a short read is a source that
                        // couldn't serve the range, never a smaller part to record.
                        if data.len() as u64 != length {
                            task.discard_received(data.len() as u64);
                            return Err(StorageError::NoPartError(format!(
                                "short read for {} @{}+{}: got {}",
                                src_obj.key,
                                pos,
                                length,
                                data.len()
                            )));
                        }
                        task.stats
                            .src_bytes_read
                            .fetch_add(data.len() as u64, Ordering::Relaxed);

                        let data_len = data.len() as u64;
                        // A hard cancel may have landed while this part downloaded:
                        // the dest is (about to be) cancelled, drop the bytes. A
                        // pause writes them — they're paid for.
                        if task.stop() == StopState::Cancel {
                            task.discard_received(data_len);
                            return Ok(());
                        }

                        let put = task
                            .dest
                            .read()
                            .await
                            .put_part(part_number, &upload_info, pos, &data, is_last)
                            .await;
                        let part_info = match put {
                            Ok(info) => info,
                            Err(e) => {
                                task.discard_received(data_len);
                                return Err(e);
                            }
                        };
                        task.stats.dst_bytes_written.fetch_add(data_len, Ordering::Relaxed);
                        credit_dir_bytes(&task.dir_bytes, &dst_key_inner, data_len);

                        let mut info = part_info;
                        if info.size.is_none() {
                            info.size = Some(data_len);
                        }

                        let mut parts = parts_state.lock().await;
                        parts.push(info);
                        parts.sort_by_key(|p| p.part_number);

                        if is_complete_s3_part_list(&parts, src_obj.size) {
                            tracing::debug!("completing S3 multipart {}", dst_key_inner);
                            let parts_snapshot: Vec<StoragePartInfo> = parts.clone();
                            drop(parts);
                            task.dest
                                .read()
                                .await
                                .complete_multipart(&upload_info, &parts_snapshot)
                                .await?;
                            let mut mps = task.dest_multiparts.lock().await;
                            if let Some(ms) = mps.as_mut() {
                                ms.retain(|m| m.upload_id != upload_info.upload_id);
                            }
                        }
                        Ok(())
                    }
                    .await;
                    match result {
                        Ok(()) => break,
                        Err(e) => match task.on_unit_error(&dst_key_inner, &label, e).await {
                            ErrorDecision::Retry => continue,
                            ErrorDecision::Skip => {
                                task.abandon_multipart(&dst_key_inner, &upload_info, src_obj.size).await;
                                break;
                            }
                            ErrorDecision::Cancel => break,
                        },
                    }
                }
                task.key_end(&dst_key_inner);
                task.limiter.release().await;
            });

            pos += length;
            part_number += 1;
            submitted += 1;
        }

        // Completion normally fires inside the task that lands the part making
        // the list complete. When every part was already on the dest (a crash
        // between the last part's record and `complete_multipart`), no task
        // runs and nobody would complete it — the file would stay an open
        // multipart forever while the run reports Finished.
        if submitted == 0 {
            let parts = parts_state.lock().await;
            if is_complete_s3_part_list(&parts, src_size) {
                tracing::debug!("all parts of {} already on dest; completing multipart", dst_key);
                let parts_snapshot: Vec<StoragePartInfo> = parts.clone();
                drop(parts);
                self.dest
                    .read()
                    .await
                    .complete_multipart(&upload, &parts_snapshot)
                    .await?;
                let mut mps = self.dest_multiparts.lock().await;
                if let Some(ms) = mps.as_mut() {
                    ms.retain(|m| m.upload_id != upload.upload_id);
                }
            }
        }

        Ok(())
    }

    /// Class 3 dest: ninja-source parts in any order.
    async fn transfer_src_ninja_parts(&self, src: &StorageObjectInfo, fresh: bool) -> Result<(), StorageError> {
        tracing::debug!("transferring incomplete ninja {}", src.key);
        let dst_key = self.dst_key(&src.key)?;
        let dst_upload = self.find_or_begin_multipart(src).await?;
        let dst_parts = self.dest.read().await.list_parts_all(&dst_upload).await?;

        if fresh {
            let existing_bytes: u64 = dst_parts.iter().filter_map(|p| p.size).sum();
            self.stats.bytes_skipped.fetch_add(existing_bytes, Ordering::Relaxed);
            credit_dir_bytes(&self.dir_bytes, &dst_key, existing_bytes);
        }

        let Some(src_parts) = src.parts.as_ref() else {
            return Ok(());
        };

        for sp in src_parts {
            let pn = sp.part_number;
            let already = dst_parts.iter().any(|dp| dp.part_number == pn);
            if already {
                continue;
            }
            if self.failed_keys.lock().unwrap().contains(&dst_key) {
                return Ok(());
            }
            self.submit_src_ninja_part(src, &dst_upload, sp).await?;
            if self.is_cancelled() {
                return Ok(());
            }
        }
        Ok(())
    }

    async fn find_or_begin_multipart(&self, src: &StorageObjectInfo) -> Result<StorageMultipartUpload, StorageError> {
        let dst_key = self.dst_key(&src.key)?;
        let multiparts = self.dst_multiparts().await?;
        if let Some(existing) = multiparts
            .iter()
            .find(|m| m.plain_key.as_deref() == Some(dst_key.as_str()))
        {
            return Ok(existing.clone());
        }

        let new_upload = self
            .dest
            .read()
            .await
            .begin_multipart(&dst_key, src.size, Some(&src.attrs))
            .await?;
        {
            let mut mps = self.dest_multiparts.lock().await;
            if let Some(ms) = mps.as_mut() {
                ms.push(new_upload.clone());
            } else {
                *mps = Some(vec![new_upload.clone()]);
            }
        }
        Ok(new_upload)
    }

    async fn submit_src_ninja_part(
        &self,
        src: &StorageObjectInfo,
        dst_upload: &StorageMultipartUpload,
        src_part: &StoragePartInfo,
    ) -> Result<(), StorageError> {
        let offset = src_part
            .offset
            .ok_or_else(|| StorageError::Other("missing offset for ninja part".into()))?;
        let size = src_part.size.unwrap_or(0);
        let part_number = src_part
            .part_number
            .ok_or_else(|| StorageError::Other("missing part_number for ninja part".into()))?;
        let is_last = src_part.is_last.unwrap_or(false);

        self.limiter.acquire().await?;
        // Same as the S3-parts path: a pause that landed during the wait
        // submits nothing new.
        if self.is_cancelled() {
            self.limiter.release().await;
            return Ok(());
        }
        let task = TaskCtx::from(self);
        let src_obj = src.clone();
        let upload = dst_upload.clone();
        let label = format!("{} part#{} @{}+{}", src.key, part_number, offset, size);

        let active_key = self.dst_key(&src.key)?;
        task.key_begin(&active_key);
        tokio::spawn(async move {
            loop {
                let result: Result<(), StorageError> = async {
                    let data = task
                        .read(&src_obj.key, &StorageGetOptions::default(), Some(offset), Some(size))
                        .await?;
                    // A source part is all-or-nothing: a short read is a source
                    // that couldn't serve the range, never a smaller part to record.
                    if data.len() as u64 != size {
                        task.discard_received(data.len() as u64);
                        return Err(StorageError::NoPartError(format!(
                            "short read for {} part#{} @{}+{}: got {}",
                            src_obj.key,
                            part_number,
                            offset,
                            size,
                            data.len()
                        )));
                    }
                    task.stats
                        .src_bytes_read
                        .fetch_add(data.len() as u64, Ordering::Relaxed);

                    let data_len = data.len() as u64;
                    // Hard cancel while this part downloaded → drop; a pause
                    // writes the part (see `transfer_sequental_into_parts`).
                    if task.stop() == StopState::Cancel {
                        task.discard_received(data_len);
                        return Ok(());
                    }

                    let put = task
                        .dest
                        .read()
                        .await
                        .put_part(part_number, &upload, offset, &data, is_last)
                        .await;
                    if let Err(e) = put {
                        task.discard_received(data_len);
                        return Err(e);
                    }
                    task.stats.dst_bytes_written.fetch_add(data_len, Ordering::Relaxed);
                    credit_dir_bytes(&task.dir_bytes, &active_key, data_len);
                    Ok(())
                }
                .await;
                match result {
                    Ok(()) => break,
                    Err(e) => match task.on_unit_error(&active_key, &label, e).await {
                        ErrorDecision::Retry => continue,
                        ErrorDecision::Skip => {
                            task.abandon_multipart(&active_key, &upload, src_obj.size).await;
                            break;
                        }
                        ErrorDecision::Cancel => break,
                    },
                }
            }
            task.key_end(&active_key);
            task.limiter.release().await;
        });
        Ok(())
    }

    fn guess_part_size(&self, parts: &[StoragePartInfo]) -> Option<u64> {
        if parts.len() > 1 {
            return parts[0].size;
        }
        if !parts.is_empty() && parts[0].part_number == Some(1) {
            return parts[0].size;
        }
        None
    }

    async fn update_stats(&self, is_complete: bool) -> Result<(), StorageError> {
        let info = StorageStatInfo {
            meta_state: if is_complete {
                StorageMetaState::Complete
            } else {
                StorageMetaState::Incomplete
            },
            total_size: Some(self.stats.total_bytes.load(Ordering::Relaxed)),
        };
        self.dest.read().await.update_stats(&info).await
    }
}

#[derive(Clone)]
struct TaskCtx {
    source: Arc<RwLock<dyn StorageSource>>,
    dest: Arc<RwLock<dyn StorageDestMultipart>>,
    limiter: ConcurrencyLimiter,
    stats: Arc<TransferItemStats>,
    dest_multiparts: Arc<Mutex<Option<Vec<StorageMultipartUpload>>>>,
    root_error: Arc<Mutex<Option<StorageError>>>,
    stop: Arc<AtomicU8>,
    read_cancel: CancellationToken,
    wait_cancel: CancellationToken,
    active_key_counts: Arc<std::sync::Mutex<std::collections::HashMap<String, u32>>>,
    dir_bytes: Arc<std::sync::Mutex<std::collections::HashMap<String, u64>>>,
    policies: TransferPolicies,
    state: Arc<Mutex<TransferItemState>>,
    pending_error: Arc<Mutex<Option<(String, String)>>>,
    skipped_keys: Arc<std::sync::Mutex<Vec<String>>>,
    failed_keys: Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
    ask_lock: Arc<Mutex<()>>,
}

impl TaskCtx {
    fn from(ti: &TransferItem) -> Self {
        Self {
            source: ti.source.clone(),
            dest: ti.dest.clone(),
            limiter: ti.limiter.clone(),
            stats: ti.stats.clone(),
            dest_multiparts: ti.dest_multiparts.clone(),
            root_error: ti.root_error.clone(),
            stop: ti.stop.clone(),
            read_cancel: ti.read_cancel.clone(),
            wait_cancel: ti.wait_cancel.clone(),
            active_key_counts: ti.active_key_counts.clone(),
            dir_bytes: ti.dir_bytes.clone(),
            policies: ti.policies.clone(),
            state: ti.state.clone(),
            pending_error: ti.pending_error.clone(),
            skipped_keys: ti.skipped_keys.clone(),
            failed_keys: ti.failed_keys.clone(),
            ask_lock: ti.ask_lock.clone(),
        }
    }

    /// `Cancel` has already ended the transfer when returned; `Skip` has
    /// recorded the key. `Cancelled`/`NoSpace`/`SessionExpired`/`Transport`
    /// are never per-file choices.
    async fn on_unit_error(&self, key: &str, label: &str, e: StorageError) -> ErrorDecision {
        if self.failed_keys.lock().unwrap().contains(key) {
            return ErrorDecision::Skip;
        }
        let fatal = self.is_cancelled()
            || matches!(
                e,
                StorageError::Cancelled
                    | StorageError::NoSpace { .. }
                    | StorageError::SessionExpired(_)
                    | StorageError::Transport(_)
            );
        let policy = if fatal { OnError::Fail } else { self.policies.on_error };
        let decision = match policy {
            OnError::Fail => ErrorDecision::Cancel,
            OnError::Skip => ErrorDecision::Skip,
            OnError::Ask => match &self.policies.error_resolver {
                None => ErrorDecision::Cancel,
                Some(resolver) => {
                    let _turn = self.ask_lock.lock().await;
                    if self.is_cancelled() {
                        ErrorDecision::Cancel
                    } else {
                        *self.pending_error.lock().await = Some((key.to_string(), e.to_string()));
                        *self.state.lock().await = TransferItemState::HasError;
                        let d = resolver.resolve(key, &e).await;
                        *self.state.lock().await = TransferItemState::Running;
                        *self.pending_error.lock().await = None;
                        d
                    }
                }
            },
        };
        match decision {
            ErrorDecision::Cancel => self.parent_fail(key, label, e).await,
            ErrorDecision::Skip => {
                tracing::warn!("skipping {} after error [{}]: {}", key, label, e);
                self.skipped_keys.lock().unwrap().push(key.to_string());
                self.stats.files_skipped.fetch_add(1, Ordering::Relaxed);
            }
            ErrorDecision::Retry => tracing::info!("retrying [{}] after error: {}", label, e),
        }
        decision
    }

    /// The size is credited so the bar can still close (progress clamps at the total).
    async fn abandon_multipart(&self, key: &str, upload: &StorageMultipartUpload, size: u64) {
        if !self.failed_keys.lock().unwrap().insert(key.to_string()) {
            return;
        }
        // The parts already on the dest were counted as they landed
        // (`dst_bytes_written`) or as resume credit (`bytes_skipped`); only
        // the remainder closes the bar. If the dest can't list them the whole
        // size is credited — the bar overshoots and clamps, as before.
        let on_dest: u64 = match self.dest.read().await.list_parts_all(upload).await {
            Ok(parts) => parts.iter().filter_map(|p| p.size).sum(),
            Err(e) => {
                tracing::debug!("list_parts_all before abandoning {} failed: {}", key, e);
                0
            }
        };
        if let Err(e) = self.dest.read().await.cancel_multipart(upload).await {
            tracing::debug!("cancel_multipart after skip failed for {}: {}", key, e);
        }
        let mut mps = self.dest_multiparts.lock().await;
        if let Some(ms) = mps.as_mut() {
            ms.retain(|m| m.upload_id != upload.upload_id);
        }
        self.stats
            .bytes_skipped
            .fetch_add(size.saturating_sub(on_dest), Ordering::Relaxed);
    }

    fn stop(&self) -> StopState {
        StopState::load(&self.stop)
    }

    /// Received bytes that will not reach the dest and will be read again —
    /// off the bar.
    fn discard_received(&self, n: u64) {
        self.stats.bytes_received.fetch_sub(n, Ordering::Relaxed);
    }

    /// The tasks' one read primitive: `get_streamed_v2` drained chunk by
    /// chunk under `read_cancel`. A hard cancel drops the receiver at the next
    /// chunk — that cancels this request's producer (S3 body fetch, P2P
    /// channel) and nothing else. The source's read guard is held only to
    /// open the stream; producers run in their own tasks.
    async fn read(
        &self,
        key: &str,
        options: &StorageGetOptions,
        offset: Option<u64>,
        length: Option<u64>,
    ) -> Result<Vec<u8>, StorageError> {
        let mut rx = self
            .source
            .read()
            .await
            .get_streamed_v2(key, options, offset, length)
            .await;
        let mut out: Vec<u8> = Vec::with_capacity(length.unwrap_or(0) as usize);
        loop {
            tokio::select! {
                biased;
                _ = self.read_cancel.cancelled() => {
                    self.discard_received(out.len() as u64);
                    return Err(StorageError::Cancelled);
                }
                item = rx.recv() => match item {
                    // Counted the moment it is ours: a chunk on the bar.
                    Some(Ok(chunk)) => {
                        self.stats.bytes_received.fetch_add(chunk.len() as u64, Ordering::Relaxed);
                        out.extend_from_slice(&chunk);
                    }
                    Some(Err(e)) => {
                        self.discard_received(out.len() as u64);
                        return Err(e);
                    }
                    None => {
                        tracing::debug!(
                            key = %key,
                            ?offset,
                            ?length,
                            got = out.len(),
                            all_zero = out.iter().all(|b| *b == 0),
                            "read done"
                        );
                        return Ok(out);
                    }
                },
            }
        }
    }

    fn is_cancelled(&self) -> bool {
        self.stop() != StopState::Running
    }

    /// Called before the spawn so the key reads as active the moment its slot is held.
    fn key_begin(&self, key: &str) {
        *self
            .active_key_counts
            .lock()
            .unwrap()
            .entry(key.to_string())
            .or_insert(0) += 1;
    }

    fn key_end(&self, key: &str) {
        let mut counts = self.active_key_counts.lock().unwrap();
        if let Some(c) = counts.get_mut(key) {
            *c -= 1;
            if *c == 0 {
                counts.remove(key);
            }
        }
    }

    async fn run_transfer_small(&self, src: StorageObjectInfo, dst_key: String) -> Result<(), StorageError> {
        let data_len: u64;
        if src.size > 0 {
            let data = self.read(&src.key, &StorageGetOptions::default(), None, None).await?;
            data_len = data.len() as u64;
            // The meta size is the file: a stream that ended early with no
            // error is a failed read, not a shorter file.
            if data_len != src.size {
                self.discard_received(data_len);
                return Err(StorageError::ShortRead { key: src.key.clone(), expected: src.size, got: data_len });
            }
            self.stats.src_bytes_read.fetch_add(data_len, Ordering::Relaxed);
            let put = self
                .dest
                .read()
                .await
                .put_file(&dst_key, &data, Some(&src.attrs), None)
                .await;
            if let Err(e) = put {
                self.discard_received(data_len);
                return Err(e);
            }
        } else {
            data_len = 0;
            self.dest
                .read()
                .await
                .put_file(&dst_key, &[], Some(&src.attrs), None)
                .await?;
        }
        self.stats.dst_bytes_written.fetch_add(data_len, Ordering::Relaxed);
        self.stats.dst_files_written.fetch_add(1, Ordering::Relaxed);
        credit_dir_bytes(&self.dir_bytes, &dst_key, data_len);
        Ok(())
    }

    /// First failure wins as the root cause, tagged with the key it happened
    /// on (`StorageError::AtKey`); the limiter cancel that follows makes peer
    /// tasks report `Cancelled`.
    async fn parent_fail(&self, key: &str, label: &str, e: StorageError) {
        tracing::error!("task error [{}] on {}: {}", label, key, e);
        let reason = format!("{}: {}", label, e);
        {
            let mut slot = self.root_error.lock().await;
            if slot.is_none() {
                *slot = Some(StorageError::AtKey { key: key.to_string(), source: Box::new(e) });
            }
        }
        self.read_cancel.cancel();
        self.limiter.cancel(Some(reason)).await;
        let _ = self.dest.read().await.cancel().await;
    }
}

/// `StorageDestMultipart` over a shared `Arc<MultipartReorderDest>`; the
/// proxy's methods are `&self`, so the RwLock around it never serializes them.
struct ProxyHandle {
    inner: Arc<MultipartReorderDest>,
}

#[async_trait]
impl super::Storage for ProxyHandle {
    async fn connect(&mut self) -> Result<StorageMetaState, StorageError> {
        // The wrapped dest is connected directly by the caller.
        Ok(StorageMetaState::Unknown)
    }

    async fn cancel(&self) -> Result<(), StorageError> {
        self.inner.cancel().await
    }

    fn data_source_id(&self) -> Option<&str> {
        None
    }

    fn set_data_source_id(&mut self, _id: String) {}
}

#[async_trait]
impl super::StorageDest for ProxyHandle {
    fn is_sequential(&self) -> bool {
        self.inner.is_sequential()
    }
    fn is_sequential_multipart(&self) -> bool {
        self.inner.is_sequential_multipart()
    }
    async fn put_file(
        &self,
        k: &str,
        d: &[u8],
        attrs: Option<&StorageFileAttrs>,
        conditions: Option<&StoragePutConditions>,
    ) -> Result<super::StoragePutResult, StorageError> {
        self.inner.put_file(k, d, attrs, conditions).await
    }
    async fn put_dir(
        &self,
        k: &str,
        attrs: Option<&StorageFileAttrs>,
    ) -> Result<super::StoragePutResult, StorageError> {
        self.inner.put_dir(k, attrs).await
    }
    async fn put_symlink(
        &self,
        k: &str,
        t: &str,
        attrs: Option<&StorageFileAttrs>,
    ) -> Result<super::StoragePutResult, StorageError> {
        self.inner.put_symlink(k, t, attrs).await
    }
    async fn del_file(&self, k: &str) -> Result<(), StorageError> {
        self.inner.del_file(k).await
    }
    async fn del_dir(&self, k: &str) -> Result<(), StorageError> {
        self.inner.del_dir(k).await
    }
    async fn update_stats(&self, s: &StorageStatInfo) -> Result<(), StorageError> {
        self.inner.update_stats(s).await
    }
    async fn flush(&self) -> Result<(), StorageError> {
        self.inner.flush().await
    }
    async fn close(&self) -> Result<(), StorageError> {
        self.inner.close().await
    }
    async fn finalize(&self) -> Result<(), StorageError> {
        self.inner.finalize().await
    }
}

#[async_trait]
impl super::StorageDestMultipart for ProxyHandle {
    async fn list_multiparts_all(&self) -> Result<Vec<StorageMultipartUpload>, StorageError> {
        self.inner.list_multiparts_all().await
    }
    async fn list_parts_all(&self, u: &StorageMultipartUpload) -> Result<Vec<StoragePartInfo>, StorageError> {
        self.inner.list_parts_all(u).await
    }
    async fn begin_multipart(
        &self,
        o: &str,
        size: u64,
        attrs: Option<&StorageFileAttrs>,
    ) -> Result<StorageMultipartUpload, StorageError> {
        self.inner.begin_multipart(o, size, attrs).await
    }
    async fn put_part(
        &self,
        n: u32,
        u: &StorageMultipartUpload,
        off: u64,
        d: &[u8],
        last: bool,
    ) -> Result<StoragePartInfo, StorageError> {
        self.inner.put_part(n, u, off, d, last).await
    }
    async fn cancel_multipart(&self, u: &StorageMultipartUpload) -> Result<(), StorageError> {
        self.inner.cancel_multipart(u).await
    }
    async fn complete_multipart(
        &self,
        u: &StorageMultipartUpload,
        p: &[StoragePartInfo],
    ) -> Result<String, StorageError> {
        self.inner.complete_multipart(u, p).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transfers::test_support::*;

    #[tokio::test]
    async fn multi_pass_does_not_retransfer() {
        let mut initial = std::collections::HashMap::new();
        initial.insert("a.txt".to_string(), b"hello".to_vec());
        let mut late = std::collections::HashMap::new();
        late.insert("b.txt".to_string(), b"world!".to_vec());

        let source: Arc<RwLock<dyn StorageSource>> = Arc::new(RwLock::new(LiveSource::new(initial, late)));
        let dest_concrete = Arc::new(InMemDest::new());
        let dest: Arc<RwLock<dyn StorageDestMultipart>> = Arc::new(RwLock::new(InMemDestProxy(dest_concrete.clone())));

        let ti = TransferItem::new(source, dest, None, 2).await;
        ti.start().await.unwrap();

        assert_eq!(dest_concrete.put_count("a.txt").await, 1, "a.txt written exactly once");
        assert_eq!(dest_concrete.put_count("b.txt").await, 1, "b.txt written exactly once");
        let stats = ti.stats().snapshot();
        assert_eq!(stats.bytes_skipped, 0, "own writes must not read as resume credit");
        assert_eq!(stats.dst_files_written, 2);
    }

    #[tokio::test]
    async fn resume_skips_and_credits() {
        let mut initial = std::collections::HashMap::new();
        initial.insert("a.txt".to_string(), b"hello".to_vec());
        initial.insert("b.txt".to_string(), b"world!".to_vec());

        let source: Arc<RwLock<dyn StorageSource>> =
            Arc::new(RwLock::new(LiveSource::new(initial, std::collections::HashMap::new())));
        let dest_concrete = Arc::new(InMemDest::new());
        dest_concrete.preload("a.txt", b"hello").await;
        let dest: Arc<RwLock<dyn StorageDestMultipart>> = Arc::new(RwLock::new(InMemDestProxy(dest_concrete.clone())));

        let ti = TransferItem::new(source, dest, None, 2).await;
        ti.start().await.unwrap();

        assert_eq!(dest_concrete.put_count("a.txt").await, 0, "complete on dest — skipped");
        assert_eq!(dest_concrete.put_count("b.txt").await, 1);
        let stats = ti.stats().snapshot();
        assert_eq!(stats.bytes_skipped, 5, "pre-existing bytes credited once");
        assert_eq!(stats.dst_files_written, 1);
    }

    #[tokio::test]
    async fn resume_credits_existing_multipart_parts() {
        let mut files = std::collections::HashMap::new();
        files.insert("big.bin".to_string(), b"0123456789AB".to_vec());

        let source: Arc<RwLock<dyn StorageSource>> = Arc::new(RwLock::new(InMemSource::new(files)));
        let dest_concrete = Arc::new(InMemDest::new());
        dest_concrete
            .preload_multipart(
                "big.bin",
                vec![
                    StoragePartInfo {
                        part_number: Some(1),
                        offset: Some(0),
                        size: Some(4),
                        is_last: Some(false),
                        ..Default::default()
                    },
                    StoragePartInfo {
                        part_number: Some(2),
                        offset: Some(4),
                        size: Some(4),
                        is_last: Some(false),
                        ..Default::default()
                    },
                ],
            )
            .await;
        let dest: Arc<RwLock<dyn StorageDestMultipart>> = Arc::new(RwLock::new(InMemDestProxy(dest_concrete.clone())));

        let mut ti = TransferItem::new(source, dest, None, 2).await;
        // Force the multipart path for a 12-byte file (3 parts of 4).
        ti.dest_min_chunk_size = 4;
        ti.start().await.unwrap();

        let stats = ti.stats().snapshot();
        assert_eq!(stats.bytes_skipped, 8, "prior-run parts credited as skipped");
        assert_eq!(stats.dst_bytes_written, 4, "only the missing part uploaded");
        let files = dest_concrete.snapshot().await;
        let content = files.get("big.bin").expect("file written");
        assert_eq!(&content[8..12], b"89AB", "missing part landed at its offset");
    }

    #[tokio::test]
    async fn no_space_fails_before_first_put() {
        let mut files = std::collections::HashMap::new();
        files.insert("a.txt".to_string(), b"hello".to_vec());
        let source: Arc<RwLock<dyn StorageSource>> = Arc::new(RwLock::new(InMemSource::new(files)));
        let dest_concrete = Arc::new(InMemDest::with_no_space());
        let dest: Arc<RwLock<dyn StorageDestMultipart>> = Arc::new(RwLock::new(InMemDestProxy(dest_concrete.clone())));

        let ti = TransferItem::new(source, dest, None, 2).await;
        let err = ti.start().await.expect_err("must fail on the space check");
        assert!(matches!(err, StorageError::NoSpace { .. }), "got: {err}");
        assert_eq!(ti.current_state().await, TransferItemState::Error);
        assert!(dest_concrete.snapshot().await.is_empty(), "no bytes must land");
    }

    #[tokio::test]
    async fn end_to_end_small_files_class3_dest() {
        let mut files = std::collections::HashMap::new();
        files.insert("a.txt".to_string(), b"hello".to_vec());
        files.insert("b.txt".to_string(), b"world!".to_vec());
        let source: Arc<RwLock<dyn StorageSource>> = Arc::new(RwLock::new(InMemSource::new(files)));
        let dest_concrete = Arc::new(InMemDest::new());
        let dest_for_check = dest_concrete.clone();
        let dest: Arc<RwLock<dyn StorageDestMultipart>> = Arc::new(RwLock::new(InMemDestProxy(dest_concrete)));

        let ti = TransferItem::new(source, dest, None, 4).await;
        ti.start().await.unwrap();

        let snapshot = dest_for_check.snapshot().await;
        assert_eq!(snapshot.get("a.txt").map(|v| v.as_slice()), Some(b"hello".as_slice()));
        assert_eq!(snapshot.get("b.txt").map(|v| v.as_slice()), Some(b"world!".as_slice()));
    }

    #[tokio::test]
    async fn get_default_drains_get_streamed() {
        use tokio::sync::mpsc;

        async fn drain(mut rx: mpsc::Receiver<Result<Vec<u8>, StorageError>>) -> Vec<u8> {
            let mut out = Vec::new();
            while let Some(item) = rx.recv().await {
                out.extend_from_slice(&item.expect("unexpected error item"));
            }
            out
        }

        let mut files = std::collections::HashMap::new();
        files.insert("a.txt".to_string(), b"hello world".to_vec());
        let src = InMemSource::new(files);
        let soi = StorageObjectInfo::new("a.txt");

        let streamed = drain(src.get_streamed(&soi.key, None, None).await).await;
        assert_eq!(streamed, b"hello world");
        assert_eq!(streamed, src.get(&soi.key, None, None).await.unwrap());

        let windowed = drain(src.get_streamed(&soi.key, Some(6), Some(5)).await).await;
        assert_eq!(windowed, b"world");

        let missing = StorageObjectInfo::new("missing.txt");
        let mut rx = src.get_streamed(&missing.key, None, None).await;
        assert!(matches!(rx.recv().await, Some(Err(StorageError::NoSuchKey(_)))));
        assert!(rx.recv().await.is_none());
        assert!(matches!(
            src.get(&missing.key, None, None).await,
            Err(StorageError::NoSuchKey(_))
        ));
    }

    #[tokio::test]
    async fn preparing_walk_is_live_in_progress() {
        let mut files = std::collections::HashMap::new();
        files.insert("a.txt".to_string(), b"hello".to_vec());
        files.insert("b.txt".to_string(), b"world!".to_vec());
        let walk_total: u64 = 11;
        let first: u64 = files.values().next().unwrap().len() as u64;

        let release = Arc::new(tokio::sync::Notify::new());
        let source: Arc<RwLock<dyn StorageSource>> = Arc::new(RwLock::new(GatedStatsSource {
            files,
            release: release.clone(),
            data_source_id: None,
        }));
        let dest_concrete = Arc::new(InMemDest::new());
        let dest: Arc<RwLock<dyn StorageDestMultipart>> = Arc::new(RwLock::new(InMemDestProxy(dest_concrete)));

        let ti = Arc::new(TransferItem::new(source, dest, None, 2).await);
        let run = tokio::spawn({
            let ti = ti.clone();
            async move { ti.start().await }
        });

        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let p = ti.progress().await;
                if p.total_files == 1 {
                    assert!(p.preparing, "mid-walk snapshot must report preparing");
                    assert_eq!(p.total_bytes, first, "partial byte total published");
                    assert_eq!(p.bytes, 0, "no bytes moved during the walk");
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("never observed the walk's partial totals");

        release.notify_one();
        run.await.unwrap().unwrap();

        let p = ti.progress().await;
        assert!(!p.preparing, "finished transfer must not read as preparing");
        assert_eq!(p.total_files, 2);
        assert_eq!(p.total_bytes, walk_total);
        assert_eq!(p.bytes, walk_total);
    }

    // ── existing-entry / error policies ────────────────────────────────────

    use super::super::policy::{
        Conflict, ConflictDecision, ConflictResolver, ErrorDecision, ErrorResolver, OnError, OnExisting,
        TransferPolicies,
    };

    async fn run_with(
        src: std::collections::HashMap<String, Vec<u8>>,
        dest_concrete: &Arc<InMemDest>,
        policies: TransferPolicies,
    ) -> (Arc<TransferItem>, Result<(), StorageError>) {
        let source: Arc<RwLock<dyn StorageSource>> =
            Arc::new(RwLock::new(LiveSource::new(src, std::collections::HashMap::new())));
        let dest: Arc<RwLock<dyn StorageDestMultipart>> = Arc::new(RwLock::new(InMemDestProxy(dest_concrete.clone())));
        let ti = Arc::new(TransferItem::new(source, dest, None, 2).await.with_policies(policies));
        let res = ti.start().await;
        (ti, res)
    }

    fn existing(on_existing: OnExisting) -> TransferPolicies {
        TransferPolicies {
            on_existing,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn overwrite_replaces_existing_file() {
        let dest = Arc::new(InMemDest::new());
        dest.preload("a.txt", b"old").await;
        let (ti, res) = run_with(files(&[("a.txt", b"hello")]), &dest, existing(OnExisting::Overwrite)).await;
        res.unwrap();
        assert_eq!(dest.snapshot().await["a.txt"], b"hello");
        assert_eq!(dest.put_count("a.txt").await, 1);
        assert_eq!(ti.stats().snapshot().bytes_skipped, 0);
    }

    #[tokio::test]
    async fn keep_larger_overwrites_only_when_source_is_larger() {
        let dest = Arc::new(InMemDest::new());
        dest.preload("small.txt", b"xy").await; // source larger → overwrite
        dest.preload("big.txt", b"a longer existing file").await; // source smaller → skip
        let src = files(&[("small.txt", b"hello"), ("big.txt", b"hi")]);
        let (_ti, res) = run_with(src, &dest, existing(OnExisting::KeepLarger)).await;
        res.unwrap();
        let snap = dest.snapshot().await;
        assert_eq!(snap["small.txt"], b"hello");
        assert_eq!(snap["big.txt"], b"a longer existing file");
        assert_eq!(dest.put_count("big.txt").await, 0);
    }

    #[tokio::test]
    async fn if_size_differs_skips_same_size() {
        let dest = Arc::new(InMemDest::new());
        dest.preload("same.txt", b"12345").await; // 5 bytes both
        dest.preload("diff.txt", b"1").await;
        let src = files(&[("same.txt", b"hello"), ("diff.txt", b"hello")]);
        let (_ti, res) = run_with(src, &dest, existing(OnExisting::IfSizeDiffers)).await;
        res.unwrap();
        let snap = dest.snapshot().await;
        assert_eq!(snap["same.txt"], b"12345", "same size → kept");
        assert_eq!(snap["diff.txt"], b"hello", "size differs → overwritten");
    }

    /// Records what it was asked and answers per key.
    struct EachResolver {
        seen: std::sync::Mutex<Vec<Conflict>>,
        answer: ConflictDecision,
    }

    #[async_trait]
    impl ConflictResolver for EachResolver {
        async fn resolve(&self, conflicts: Vec<Conflict>) -> ConflictDecision {
            self.seen.lock().unwrap().extend(conflicts);
            self.answer.clone()
        }
    }

    #[tokio::test]
    async fn ask_collects_all_conflicts_once_and_applies_each_answer() {
        let dest = Arc::new(InMemDest::new());
        dest.preload("a.txt", b"old a").await;
        dest.preload("b.txt", b"old b").await;
        let mut each = std::collections::HashMap::new();
        each.insert("a.txt".to_string(), OnExisting::Overwrite);
        each.insert("b.txt".to_string(), OnExisting::Skip);
        let resolver = Arc::new(EachResolver {
            seen: Default::default(),
            answer: ConflictDecision::Each(each),
        });
        let policies = TransferPolicies {
            on_existing: OnExisting::Ask,
            conflict_resolver: Some(resolver.clone()),
            ..Default::default()
        };
        let src = files(&[("a.txt", b"new a"), ("b.txt", b"new b"), ("c.txt", b"new c")]);
        let (ti, res) = run_with(src, &dest, policies).await;
        res.unwrap();

        let (mut keys, first_dest_size) = {
            let seen = resolver.seen.lock().unwrap();
            (
                seen.iter().map(|c| c.key.clone()).collect::<Vec<_>>(),
                seen[0].dest.size,
            )
        };
        keys.sort();
        assert_eq!(keys, vec!["a.txt", "b.txt"], "only existing complete files conflict");
        assert_eq!(first_dest_size, 5);

        let snap = dest.snapshot().await;
        assert_eq!(snap["a.txt"], b"new a");
        assert_eq!(snap["b.txt"], b"old b");
        assert_eq!(snap["c.txt"], b"new c");
        assert!(ti.pending_conflicts().await.is_empty(), "cleared once answered");
        assert_eq!(ti.current_state().await, TransferItemState::Finished);
    }

    #[tokio::test]
    async fn ask_cancel_aborts_before_any_write() {
        let dest = Arc::new(InMemDest::new());
        dest.preload("a.txt", b"old a").await;
        let resolver = Arc::new(EachResolver {
            seen: Default::default(),
            answer: ConflictDecision::Cancel,
        });
        let policies = TransferPolicies {
            on_existing: OnExisting::Ask,
            conflict_resolver: Some(resolver),
            ..Default::default()
        };
        let src = files(&[("a.txt", b"new a"), ("c.txt", b"new c")]);
        let (ti, res) = run_with(src, &dest, policies).await;
        res.unwrap();
        assert_eq!(ti.current_state().await, TransferItemState::Cancelled);
        assert_eq!(dest.put_count("c.txt").await, 0, "nothing written after cancel");
        assert_eq!(dest.snapshot().await["a.txt"], b"old a");
    }

    /// Answers a fixed decision and counts the asks.
    struct FixedErrorResolver {
        asks: std::sync::Mutex<Vec<(String, String)>>,
        answer: ErrorDecision,
    }

    #[async_trait]
    impl ErrorResolver for FixedErrorResolver {
        async fn resolve(&self, key: &str, error: &StorageError) -> ErrorDecision {
            self.asks.lock().unwrap().push((key.to_string(), error.to_string()));
            self.answer
        }
    }

    // ── complete dirs (dir_completion) ─────────────────────────────────────

    fn tree(pairs: &[(&str, &[u8])], dirs: &[&str]) -> InMemSource {
        let mut src = InMemSource::new(files(pairs));
        src.dirs = dirs.iter().map(|d| d.to_string()).collect();
        src
    }

    async fn run_tree(
        src: InMemSource,
        selection: Option<Vec<String>>,
        dest: &Arc<InMemDest>,
        policies: TransferPolicies,
    ) -> (TransferItem, Result<(), StorageError>) {
        let source: Arc<RwLock<dyn StorageSource>> = Arc::new(RwLock::new(src));
        let dest: Arc<RwLock<dyn StorageDestMultipart>> = Arc::new(RwLock::new(InMemDestProxy(dest.clone())));
        let ti = TransferItem::new(source, dest, selection, 2)
            .await
            .with_policies(policies);
        let res = ti.start().await;
        (ti, res)
    }

    fn sel(roots: &[&str]) -> Option<Vec<String>> {
        Some(roots.iter().map(|r| r.to_string()).collect())
    }

    fn scope(scope: &str, complete: bool, dirs: &[&str]) -> ScopeCompletion {
        ScopeCompletion {
            scope: scope.to_string(),
            complete,
            dirs: dirs.iter().map(|d| d.to_string()).collect(),
        }
    }

    #[test]
    fn dir_tracker_reports_maximal_complete_dirs() {
        let mut t = DirTracker::default();
        t.open_scope("");
        t.note_entry("", "a/b/f.txt", false);
        t.note_entry("", "a/c/d/g.txt", false);
        t.note_entry("", "a/c", true);
        t.note_entry("", "z", true);
        t.note_entry("", "top.txt", false);

        let landed_all = std::collections::HashSet::new();
        assert_eq!(t.completion(&landed_all), vec![scope("", true, &[""])]);

        let failed_file: std::collections::HashSet<String> = ["a/b/f.txt".to_string()].into();
        assert_eq!(t.completion(&failed_file), vec![scope("", false, &["a/c", "z"])]);

        // A failed dir is incomplete itself; a complete subtree below still reports.
        let failed_dir: std::collections::HashSet<String> = ["a/c".to_string()].into();
        assert_eq!(
            t.completion(&failed_dir),
            vec![scope("", false, &["a/b", "a/c/d", "z"])]
        );
    }

    #[test]
    fn dir_tracker_stops_at_the_scope() {
        let mut t = DirTracker::default();
        t.open_scope("a/b");
        t.note_entry("a/b", "a/b/c/d.txt", false);
        let want: std::collections::HashSet<String> = ["a/b".to_string(), "a/b/c".to_string()].into_iter().collect();
        assert_eq!(
            t.scopes["a/b"], want,
            "ancestors above the scope are not the scope's business"
        );
        assert_eq!(t.completion(&Default::default()), vec![scope("a/b", true, &["a/b"])]);
    }

    #[tokio::test]
    async fn dir_completion_whole_share_collapses_to_root() {
        let src = tree(
            &[("a/x.txt", b"1"), ("a/b/y.txt", b"22"), ("c.txt", b"333")],
            &["a", "a/b"],
        );
        let dest = Arc::new(InMemDest::new());
        let (ti, res) = run_tree(src, None, &dest, TransferPolicies::default()).await;
        res.unwrap();
        assert_eq!(ti.dir_completion().await, vec![scope("", true, &[""])]);
    }

    #[tokio::test]
    async fn dir_completion_reports_each_selection_root() {
        let src = tree(
            &[
                ("a/x.txt", b"1"),
                ("a/b/y.txt", b"22"),
                ("d/z.txt", b"3"),
                ("e.txt", b"4"),
            ],
            &["a", "a/b", "d"],
        );
        let dest = Arc::new(InMemDest::new());
        let (ti, res) = run_tree(src, sel(&["d", "a"]), &dest, TransferPolicies::default()).await;
        res.unwrap();
        assert_eq!(
            ti.dir_completion().await,
            vec![scope("a", true, &["a"]), scope("d", true, &["d"])]
        );
        assert!(
            !dest.snapshot().await.contains_key("e.txt"),
            "unselected root untouched"
        );
    }

    #[tokio::test]
    async fn dir_completion_file_roots_report_themselves() {
        let src = tree(&[("a/x.txt", b"1"), ("c.txt", b"333")], &["a"]);
        let dest = Arc::new(InMemDest::new());
        let (ti, res) = run_tree(src, sel(&["c.txt"]), &dest, TransferPolicies::default()).await;
        res.unwrap();
        assert_eq!(ti.dir_completion().await, vec![scope("c.txt", true, &["c.txt"])]);

        // A skipped file root is an incomplete scope with nothing under it.
        let src = tree(&[("a/x.txt", b"1"), ("c.txt", b"333")], &["a"]);
        let dest = Arc::new(InMemDest::new());
        dest.fail_next_puts("c.txt", 1).await;
        let policies = TransferPolicies {
            on_error: OnError::Skip,
            ..Default::default()
        };
        let (ti, res) = run_tree(src, sel(&["c.txt"]), &dest, policies).await;
        res.unwrap();
        assert_eq!(ti.skipped_keys(), vec!["c.txt"]);
        assert_eq!(ti.dir_completion().await, vec![scope("c.txt", false, &[])]);
    }

    #[tokio::test]
    async fn dir_completion_skipped_file_breaks_its_ancestors_only() {
        let src = tree(
            &[
                ("a/b/bad.txt", b"x"),
                ("a/c/good.txt", b"y"),
                ("a/ok.txt", b"z"),
                ("top.txt", b"t"),
            ],
            &["a", "a/b", "a/c"],
        );
        let dest = Arc::new(InMemDest::new());
        dest.fail_next_puts("a/b/bad.txt", 1).await;
        let policies = TransferPolicies {
            on_error: OnError::Skip,
            ..Default::default()
        };
        let (ti, res) = run_tree(src, None, &dest, policies).await;
        res.unwrap();
        assert_eq!(ti.skipped_keys(), vec!["a/b/bad.txt"]);
        assert_eq!(ti.dir_completion().await, vec![scope("", false, &["a/c"])]);
    }

    #[tokio::test]
    async fn dir_completion_is_empty_unless_finished() {
        // Error: the failing put ends the run under the default Fail policy.
        let dest = Arc::new(InMemDest::new());
        dest.fail_next_puts("a/x.txt", 1).await;
        let src = tree(&[("a/x.txt", b"1"), ("b/y.txt", b"2")], &["a", "b"]);
        let (ti, res) = run_tree(src, None, &dest, TransferPolicies::default()).await;
        assert!(res.is_err());
        assert_eq!(ti.current_state().await, TransferItemState::Error);
        assert!(
            ti.dir_completion().await.is_empty(),
            "an errored run vouches for nothing"
        );

        // Cancelled: nothing was verified to land.
        let dest = Arc::new(InMemDest::new());
        let src = tree(&[("a/x.txt", b"1")], &["a"]);
        let source: Arc<RwLock<dyn StorageSource>> = Arc::new(RwLock::new(src));
        let dest_dyn: Arc<RwLock<dyn StorageDestMultipart>> = Arc::new(RwLock::new(InMemDestProxy(dest.clone())));
        let ti = TransferItem::new(source, dest_dyn, None, 2).await;
        ti.cancel(false).await;
        ti.start().await.unwrap();
        assert_eq!(ti.current_state().await, TransferItemState::Cancelled);
        assert!(ti.dir_completion().await.is_empty());
    }

    #[tokio::test]
    async fn dir_completion_counts_resume_skips_as_landed() {
        let src = tree(&[("a/x.txt", b"1"), ("a/y.txt", b"2")], &["a"]);
        let dest = Arc::new(InMemDest::new());
        dest.preload_dir("a").await;
        dest.preload("a/x.txt", b"1").await;
        let (ti, res) = run_tree(src, None, &dest, TransferPolicies::default()).await;
        res.unwrap();
        assert_eq!(dest.put_count("a/x.txt").await, 0, "already complete on dest");
        assert_eq!(ti.dir_completion().await, vec![scope("", true, &[""])]);
    }

    #[tokio::test]
    async fn dir_completion_empty_dir_is_complete() {
        let src = tree(&[], &["e"]);
        let dest = Arc::new(InMemDest::new());
        let (ti, res) = run_tree(src, None, &dest, TransferPolicies::default()).await;
        res.unwrap();
        assert_eq!(dest.dir_put_count("e").await, 1);
        assert_eq!(ti.dir_completion().await, vec![scope("", true, &[""])]);
    }

    #[tokio::test]
    async fn dir_completion_survives_multiple_passes() {
        let initial = files(&[("a/x.txt", b"hello")]);
        let late = files(&[("a/b/y.txt", b"world!")]);
        let source: Arc<RwLock<dyn StorageSource>> = Arc::new(RwLock::new(LiveSource::new(initial, late)));
        let dest_concrete = Arc::new(InMemDest::new());
        let dest: Arc<RwLock<dyn StorageDestMultipart>> = Arc::new(RwLock::new(InMemDestProxy(dest_concrete.clone())));
        let ti = TransferItem::new(source, dest, None, 2).await;
        ti.start().await.unwrap();
        assert_eq!(dest_concrete.put_count("a/b/y.txt").await, 1);
        assert_eq!(ti.dir_completion().await, vec![scope("", true, &[""])]);
    }

    // ── per-dir landed bytes (dir_bytes_for) ───────────────────────────────

    #[tokio::test]
    async fn dir_bytes_accumulate_up_the_tree() {
        let src = tree(
            &[("a/x.txt", b"hello"), ("a/b/y.txt", b"world!"), ("c.txt", b"123")],
            &["a", "a/b"],
        );
        let dest = Arc::new(InMemDest::new());
        let (ti, res) = run_tree(src, None, &dest, TransferPolicies::default()).await;
        res.unwrap();
        assert_eq!(ti.dir_bytes_for(""), 14);
        assert_eq!(ti.dir_bytes_for("a"), 11);
        assert_eq!(ti.dir_bytes_for("a/b"), 6);
        assert_eq!(ti.dir_bytes_for("nope"), 0);
        assert_eq!(ti.dir_bytes_for("a/x.txt"), 0, "files are not dirs");
    }

    #[tokio::test]
    async fn dir_bytes_credit_resume_skips_and_symlinks() {
        let mut src = tree(&[("a/x.txt", b"hello"), ("a/y.txt", b"12")], &["a"]);
        src.symlinks.insert("a/l".into(), "target".into());
        let dest = Arc::new(InMemDest::new());
        dest.preload_dir("a").await;
        dest.preload("a/x.txt", b"hello").await;
        let (ti, res) = run_tree(src, None, &dest, TransferPolicies::default()).await;
        res.unwrap();
        assert_eq!(ti.stats().snapshot().bytes_skipped, 5, "x.txt credited, not rewritten");
        assert_eq!(ti.dir_bytes_for("a"), 5 + 2 + 6);
        assert_eq!(ti.dir_bytes_for(""), 13);
    }

    #[tokio::test]
    async fn dir_bytes_count_written_and_resumed_parts() {
        let src = tree(&[("d/big.bin", b"0123456789AB")], &["d"]);
        let dest = Arc::new(InMemDest::new());
        dest.preload_dir("d").await;
        dest.preload_multipart(
            "d/big.bin",
            vec![
                StoragePartInfo {
                    part_number: Some(1),
                    offset: Some(0),
                    size: Some(4),
                    is_last: Some(false),
                    ..Default::default()
                },
                StoragePartInfo {
                    part_number: Some(2),
                    offset: Some(4),
                    size: Some(4),
                    is_last: Some(false),
                    ..Default::default()
                },
            ],
        )
        .await;
        let source: Arc<RwLock<dyn StorageSource>> = Arc::new(RwLock::new(src));
        let dest_dyn: Arc<RwLock<dyn StorageDestMultipart>> = Arc::new(RwLock::new(InMemDestProxy(dest.clone())));
        let mut ti = TransferItem::new(source, dest_dyn, None, 2).await;
        ti.dest_min_chunk_size = 4;
        ti.start().await.unwrap();
        let stats = ti.stats().snapshot();
        assert_eq!(stats.bytes_skipped, 8);
        assert_eq!(stats.dst_bytes_written, 4);
        assert_eq!(ti.dir_bytes_for("d"), 12);
        assert_eq!(ti.dir_bytes_for(""), 12);
    }

    #[tokio::test]
    async fn resume_credits_existing_symlink() {
        let mut src = tree(&[("a/x.txt", b"hello")], &["a"]);
        src.symlinks.insert("a/l".into(), "target".into());
        let dest = Arc::new(InMemDest::new());
        dest.preload_dir("a").await;
        crate::transfers::StorageDest::put_symlink(dest.as_ref(), "a/l", "target", None)
            .await
            .unwrap();
        let (ti, res) = run_tree(src, None, &dest, TransferPolicies::default()).await;
        res.unwrap();
        assert_eq!(dest.put_count("a/l").await, 1, "preload only — not rewritten");
        let stats = ti.stats().snapshot();
        assert_eq!(stats.bytes_skipped, 6);
        assert_eq!(stats.total_bytes, 11);
        assert_eq!(ti.progress().await.bytes, 11, "resumed symlink counts toward the bar");
        assert_eq!(ti.dir_bytes_for("a"), 11);
    }

    #[tokio::test]
    async fn dir_completion_uses_dest_keys_under_source_root() {
        let src = tree(
            &[("docs/a/x.txt", b"1"), ("docs/a/b/y.txt", b"2")],
            &["docs", "docs/a", "docs/a/b"],
        );
        let source: Arc<RwLock<dyn StorageSource>> = Arc::new(RwLock::new(src));
        let dest_concrete = Arc::new(InMemDest::new());
        let dest: Arc<RwLock<dyn StorageDestMultipart>> = Arc::new(RwLock::new(InMemDestProxy(dest_concrete.clone())));
        let ti = TransferItem::new(source, dest, sel(&["docs/a"]), 2)
            .await
            .with_source_root("docs");
        ti.start().await.unwrap();
        assert!(dest_concrete.snapshot().await.contains_key("a/b/y.txt"));
        assert_eq!(ti.dir_completion().await, vec![scope("a", true, &["a"])]);
    }

    /// A stream that ends cleanly short of the meta size is a failed read on
    /// that key, its bytes off the counter, nothing written.
    #[tokio::test]
    async fn short_read_fails_the_file() {
        let dest = Arc::new(InMemDest::new());
        let source: Arc<RwLock<dyn StorageSource>> = Arc::new(RwLock::new(
            LiveSource::new(files(&[("a.txt", b"hello")]), std::collections::HashMap::new()).with_truncated("a.txt"),
        ));
        let dest_dyn: Arc<RwLock<dyn StorageDestMultipart>> = Arc::new(RwLock::new(InMemDestProxy(dest.clone())));
        let ti = Arc::new(TransferItem::new(source, dest_dyn, None, 2).await);
        let res = ti.start().await;
        assert!(
            matches!(&res, Err(StorageError::AtKey { key, source })
                if key == "a.txt" && matches!(**source, StorageError::ShortRead { expected: 5, got: 4, .. })),
            "got {res:?}"
        );
        assert_eq!(ti.current_state().await, TransferItemState::Error);
        let stats = ti.stats().snapshot();
        assert_eq!(stats.bytes_received, 0, "the short read's bytes rolled back");
        assert_eq!(stats.dst_files_written, 0);
    }

    #[tokio::test]
    async fn error_policy_fail_ends_the_transfer() {
        let dest = Arc::new(InMemDest::new());
        dest.fail_next_puts("bad.txt", 1).await;
        let (ti, res) = run_with(files(&[("bad.txt", b"x")]), &dest, TransferPolicies::default()).await;
        // The run's error names the key the task failed on, cause inside.
        assert!(
            matches!(&res, Err(StorageError::AtKey { key, source })
                if key == "bad.txt" && matches!(**source, StorageError::Io(_))),
            "got {res:?}"
        );
        assert_eq!(ti.current_state().await, TransferItemState::Error);
    }

    #[tokio::test]
    async fn error_policy_skip_records_and_continues() {
        let dest = Arc::new(InMemDest::new());
        dest.fail_next_puts("bad.txt", 1).await;
        let policies = TransferPolicies {
            on_error: OnError::Skip,
            ..Default::default()
        };
        let src = files(&[("bad.txt", b"x"), ("good.txt", b"y")]);
        let (ti, res) = run_with(src, &dest, policies).await;
        res.unwrap();
        assert_eq!(ti.skipped_keys(), vec!["bad.txt"]);
        assert_eq!(ti.stats().snapshot().files_skipped, 1);
        assert_eq!(dest.snapshot().await["good.txt"], b"y");
        assert!(!dest.snapshot().await.contains_key("bad.txt"));
        assert_eq!(ti.current_state().await, TransferItemState::Finished);
    }

    #[tokio::test]
    async fn error_policy_ask_retry_reruns_the_unit() {
        let dest = Arc::new(InMemDest::new());
        dest.fail_next_puts("flaky.txt", 1).await;
        let resolver = Arc::new(FixedErrorResolver {
            asks: Default::default(),
            answer: ErrorDecision::Retry,
        });
        let policies = TransferPolicies {
            on_error: OnError::Ask,
            error_resolver: Some(resolver.clone()),
            ..Default::default()
        };
        let (ti, res) = run_with(files(&[("flaky.txt", b"z")]), &dest, policies).await;
        res.unwrap();
        let asks = resolver.asks.lock().unwrap().clone();
        assert_eq!(asks.len(), 1);
        assert_eq!(asks[0].0, "flaky.txt");
        assert_eq!(dest.put_count("flaky.txt").await, 2, "failed once, retried once");
        assert_eq!(dest.snapshot().await["flaky.txt"], b"z");
        assert!(ti.pending_error().await.is_none());
        assert_eq!(ti.skipped_keys().len(), 0);
    }

    #[tokio::test]
    async fn error_policy_ask_cancel_ends_with_the_real_error() {
        let dest = Arc::new(InMemDest::new());
        dest.fail_next_puts("bad.txt", 1).await;
        let resolver = Arc::new(FixedErrorResolver {
            asks: Default::default(),
            answer: ErrorDecision::Cancel,
        });
        let policies = TransferPolicies {
            on_error: OnError::Ask,
            error_resolver: Some(resolver),
            ..Default::default()
        };
        let (ti, res) = run_with(files(&[("bad.txt", b"x")]), &dest, policies).await;
        assert!(
            matches!(&res, Err(StorageError::AtKey { key, source })
                if key == "bad.txt" && matches!(**source, StorageError::Io(_))),
            "got {res:?}"
        );
        assert_eq!(ti.current_state().await, TransferItemState::Error);
    }

    // ── shared plumbing for the sections below ─────────────────────────────

    fn src_arc(s: InMemSource) -> Arc<RwLock<dyn StorageSource>> {
        Arc::new(RwLock::new(s))
    }

    fn dest_arc(d: &Arc<InMemDest>) -> Arc<RwLock<dyn StorageDestMultipart>> {
        Arc::new(RwLock::new(InMemDestProxy(d.clone())))
    }

    fn part(n: u32, offset: u64, size: u64, is_last: bool) -> StoragePartInfo {
        StoragePartInfo {
            part_number: Some(n),
            offset: Some(offset),
            size: Some(size),
            is_last: Some(is_last),
            ..Default::default()
        }
    }

    /// Spawned tasks can outlive `start()` on cancel/error paths.
    async fn wait_idle(ti: &TransferItem) {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !ti.active_keys().is_empty() {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("in-flight tasks never drained");
    }

    // ── selection + source_root ────────────────────────────────────────────

    #[tokio::test]
    async fn selection_transfers_only_selected_roots() {
        let mut src = InMemSource::new(files(&[
            ("docs/a.txt", b"hello"),
            ("pics/1.jpg", b"11111"),
            ("pics/2.jpg", b"22222"),
            ("other/x.txt", b"nope"),
        ]));
        src.dirs = vec!["docs".into(), "pics".into(), "other".into()];
        let dest = Arc::new(InMemDest::new());
        let ti = TransferItem::new(
            src_arc(src),
            dest_arc(&dest),
            Some(vec!["docs/a.txt".into(), "pics".into()]),
            2,
        )
        .await;
        ti.start().await.unwrap();

        let snap = dest.snapshot().await;
        let mut keys: Vec<&String> = snap.keys().collect();
        keys.sort();
        assert_eq!(keys, ["docs/a.txt", "pics/1.jpg", "pics/2.jpg"]);
        assert_eq!(dest.dir_put_count("docs").await, 1, "ancestor of the file root");
        assert_eq!(dest.dir_put_count("pics").await, 1, "the dir root itself");
        assert_eq!(dest.dir_put_count("other").await, 0);
        let totals = ti.root_totals();
        assert_eq!(totals.get("docs/a.txt"), Some(&5));
        assert_eq!(totals.get("pics"), Some(&10));
        assert_eq!(ti.stats().snapshot().total_bytes, 15, "unselected roots are not sized");
        assert_eq!(ti.current_state().await, TransferItemState::Finished);
    }

    #[tokio::test]
    async fn empty_selection_finishes_trivially() {
        let src = InMemSource::new(files(&[("a.txt", b"hello")]));
        let dest = Arc::new(InMemDest::new());
        let ti = TransferItem::new(src_arc(src), dest_arc(&dest), Some(Vec::new()), 2).await;
        ti.start().await.unwrap();
        assert_eq!(ti.current_state().await, TransferItemState::Finished);
        assert!(dest.snapshot().await.is_empty());
    }

    #[tokio::test]
    async fn source_root_strips_prefix_on_dest() {
        let mut src = InMemSource::new(files(&[("docs/report.pdf", b"PDF"), ("docs/other.txt", b"other")]));
        src.dirs = vec!["docs".into()];
        let dest = Arc::new(InMemDest::new());
        let ti = TransferItem::new(src_arc(src), dest_arc(&dest), Some(vec!["docs/report.pdf".into()]), 2)
            .await
            .with_source_root("docs");
        ti.start().await.unwrap();

        let snap = dest.snapshot().await;
        assert_eq!(snap.get("report.pdf").map(|v| v.as_slice()), Some(b"PDF".as_slice()));
        assert_eq!(snap.len(), 1, "only the selected file, under its stripped key");
        assert_eq!(
            dest.dir_put_count("docs").await,
            0,
            "no dirs from inside the source root"
        );
        assert_eq!(ti.current_state().await, TransferItemState::Finished);
    }

    #[tokio::test]
    async fn key_outside_source_root_fails_loud() {
        let src = InMemSource::new(files(&[("other/x.txt", b"x")]));
        let dest = Arc::new(InMemDest::new());
        let ti = TransferItem::new(src_arc(src), dest_arc(&dest), Some(vec!["other/x.txt".into()]), 2)
            .await
            .with_source_root("docs");
        let err = ti.start().await.expect_err("key outside the root must fail");
        assert!(err.to_string().contains("not under source root"), "got: {err}");
        assert_eq!(ti.current_state().await, TransferItemState::Error);
        assert!(dest.snapshot().await.is_empty());
    }

    #[tokio::test]
    async fn missing_root_on_complete_source_errors() {
        let src = InMemSource::new(files(&[("a.txt", b"hello")]));
        let dest = Arc::new(InMemDest::new());
        let ti = TransferItem::new(
            src_arc(src),
            dest_arc(&dest),
            Some(vec!["a.txt".into(), "ghost.txt".into()]),
            2,
        )
        .await;
        let err = ti.start().await.expect_err("unsatisfiable selection must fail");
        assert!(
            matches!(&err, StorageError::SelectionRootsMissing(roots) if roots == "ghost.txt"),
            "got: {err}"
        );
        assert_eq!(ti.current_state().await, TransferItemState::Error);
        wait_idle(&ti).await;
        assert_eq!(dest.put_count("a.txt").await, 1, "present root still transferred");
    }

    #[tokio::test]
    async fn late_root_appears_after_meta_update() {
        let mut initial = std::collections::HashMap::new();
        initial.insert("a.txt".to_string(), b"hello".to_vec());
        let mut late = std::collections::HashMap::new();
        late.insert("late.txt".to_string(), b"world!".to_vec());
        let source: Arc<RwLock<dyn StorageSource>> = Arc::new(RwLock::new(LiveSource::new(initial, late)));
        let dest = Arc::new(InMemDest::new());
        let ti = TransferItem::new(
            source,
            dest_arc(&dest),
            Some(vec!["a.txt".into(), "late.txt".into()]),
            2,
        )
        .await;
        ti.start().await.unwrap();

        // By content, not put count: pass 2 may re-put a.txt while the pass-1
        // write is still in flight (no double credit is guaranteed, not at-most-once puts).
        assert_eq!(
            dest.snapshot().await.get("a.txt").map(|v| v.as_slice()),
            Some(b"hello".as_slice())
        );
        assert_eq!(dest.put_count("late.txt").await, 1);
        assert_eq!(
            ti.stats().snapshot().bytes_skipped,
            0,
            "own pass-1 write must not read as resume credit"
        );
        let totals = ti.root_totals();
        assert_eq!(totals.get("a.txt"), Some(&5));
        assert_eq!(totals.get("late.txt"), Some(&6), "late root sized once it resolves");
        assert_eq!(ti.current_state().await, TransferItemState::Finished);
    }

    #[tokio::test]
    async fn ancestor_dirs_created_once_across_roots() {
        let mut src = InMemSource::new(files(&[("p/a.txt", b"aa"), ("p/b.txt", b"bb")]));
        src.dirs = vec!["p".into()];
        let dest = Arc::new(InMemDest::new());
        let ti = TransferItem::new(
            src_arc(src),
            dest_arc(&dest),
            Some(vec!["p/a.txt".into(), "p/b.txt".into()]),
            2,
        )
        .await;
        ti.start().await.unwrap();
        assert_eq!(dest.dir_put_count("p").await, 1, "shared ancestor created once");
        assert_eq!(ti.stats().snapshot().dst_dirs_written, 1);
        assert_eq!(dest.snapshot().await.len(), 2);
    }

    #[tokio::test]
    async fn ancestor_type_mismatch_fails() {
        let mut src = InMemSource::new(files(&[("p/a.txt", b"aa")]));
        src.dirs = vec!["p".into()];
        let dest = Arc::new(InMemDest::new());
        dest.preload("p", b"a file where a dir must go").await;
        let ti = TransferItem::new(src_arc(src), dest_arc(&dest), Some(vec!["p/a.txt".into()]), 2).await;
        let err = ti.start().await.expect_err("existing file blocking an ancestor dir");
        assert!(err.to_string().contains("ancestor"), "got: {err}");
        assert_eq!(ti.current_state().await, TransferItemState::Error);
    }

    // ── cancellation / pause ───────────────────────────────────────────────

    #[tokio::test]
    async fn cancel_mid_run_sets_cancelled_and_stops_puts() {
        let (gate, mut entered) = GetGate::for_offsets([0]);
        let mut src = InMemSource::new(files(&[("big.bin", b"0123456789AB")]));
        src.gate = Some(gate.clone());
        let dest = Arc::new(InMemDest::new());
        let mut ti = TransferItem::new(src_arc(src), dest_arc(&dest), None, 1).await;
        ti.dest_min_chunk_size = 4;
        let ti = Arc::new(ti);

        let run = tokio::spawn({
            let ti = ti.clone();
            async move { ti.start().await }
        });
        entered.recv().await.expect("first part's get must park on the gate");
        ti.cancel(false).await;

        // The parked read is abandoned by the cancel itself — the task must be
        // gone while the gate is still closed (nothing waits for the bytes).
        run.await.unwrap().unwrap();
        assert_eq!(ti.current_state().await, TransferItemState::Cancelled);
        wait_idle(&ti).await;
        gate.release_all();
        assert!(dest.snapshot().await.is_empty(), "no part may land after cancel");
    }

    /// A flushing pause writes the part whose read completed after the pause
    /// was requested (it is already paid for) and submits nothing further.
    #[tokio::test]
    async fn pause_flushes_inflight_and_keeps_landed_parts() {
        let (gate, mut entered) = GetGate::for_offsets([4]);
        let mut src = InMemSource::new(files(&[("big.bin", b"0123456789AB")]));
        src.gate = Some(gate.clone());
        let dest = Arc::new(InMemDest::new());
        let mut ti = TransferItem::new(src_arc(src), dest_arc(&dest), None, 1).await;
        ti.dest_min_chunk_size = 4;
        let ti = Arc::new(ti);

        let run = tokio::spawn({
            let ti = ti.clone();
            async move { ti.start().await }
        });
        // Part 2's get parking implies part 1 fully landed (n_streams = 1).
        entered.recv().await.expect("second part's get must park on the gate");
        let pause = tokio::spawn({
            let ti = ti.clone();
            async move { ti.cancel(true).await }
        });
        // The flag flips at cancel() entry; wait for it before releasing the
        // gate so parts 2/3 deterministically observe it before their uploads.
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !ti.is_cancelled() {
                tokio::time::sleep(std::time::Duration::from_millis(2)).await;
            }
        })
        .await
        .expect("cancel flag never flipped");
        gate.release_all();

        pause.await.unwrap();
        run.await.unwrap().unwrap();
        assert_eq!(ti.current_state().await, TransferItemState::Cancelled);
        wait_idle(&ti).await;
        assert_eq!(
            dest.snapshot().await.get("big.bin").map(|v| v.as_slice()),
            Some(b"01234567".as_slice()),
            "part 1 landed before the pause, part 2's in-flight read lands too, part 3 is never submitted"
        );
    }

    /// `start()` does not return from a pause before the in-flight parts
    /// have landed: the caller reads the dest after the run and must see
    /// them (the app persists `landed_bytes` off it).
    #[tokio::test]
    async fn pause_run_ends_after_inflight_parts_land() {
        let (gate, mut entered) = GetGate::for_offsets([4]);
        let mut src = InMemSource::new(files(&[("big.bin", b"0123456789AB")]));
        src.gate = Some(gate.clone());
        let dest = Arc::new(InMemDest::new());
        // Two streams: the orchestrator is not itself parked on the limiter
        // behind the gated part, so only the drain keeps the run open.
        let mut ti = TransferItem::new(src_arc(src), dest_arc(&dest), None, 2).await;
        ti.dest_min_chunk_size = 4;
        let ti = Arc::new(ti);

        let run = tokio::spawn({
            let ti = ti.clone();
            async move { ti.start().await }
        });
        entered.recv().await.expect("second part's get must park on the gate");
        let pause = tokio::spawn({
            let ti = ti.clone();
            async move { ti.cancel(true).await }
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !ti.is_cancelled() {
                tokio::time::sleep(std::time::Duration::from_millis(2)).await;
            }
        })
        .await
        .expect("cancel flag never flipped");
        // Part 2's read is still parked: neither the run nor the pause is over.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(!run.is_finished(), "the run must wait for the in-flight part");
        assert!(!pause.is_finished());

        gate.release_all();
        run.await.unwrap().unwrap();
        // Read right after the run returns — no waiting for the tasks here.
        let landed = dest.snapshot().await.get("big.bin").cloned().unwrap_or_default();
        assert!(
            landed.starts_with(b"01234567"),
            "the in-flight part had landed when the run returned: {landed:?}"
        );
        assert!(ti.active_keys().is_empty(), "no task outlives the run");
        pause.await.unwrap();
        assert_eq!(ti.current_state().await, TransferItemState::Cancelled);
    }

    /// EngineIssues #10: a pause during a `wait_for_parts` wait (a live
    /// share's part the uploader hasn't produced) returns at once — the wait
    /// ends in `NoPartError`, the run parks as Cancelled, the part is left for
    /// the resume. Without `wait_cancel` the pause blocked until the source
    /// delivered the bytes (here: never).
    #[tokio::test]
    async fn pause_does_not_wait_for_the_uploader() {
        use crate::transfers::{MultiSource, StreamDest};
        let mut src = InMemSource::new(files(&[("live.bin", b"0123456789AB")]));
        src.incomplete_parts.insert("live.bin".to_string(), Vec::new());
        src.unavailable.insert("live.bin".to_string());
        let source: Arc<RwLock<dyn StorageSource>> =
            Arc::new(RwLock::new(MultiSource::new(Some(src_arc(src)), None, None)));
        source.write().await.connect().await.unwrap();
        // A sequential dest takes an incomplete source down the waiting path.
        let dest: Arc<RwLock<dyn StorageDestMultipart>> = Arc::new(RwLock::new(StreamDest::new(None)));
        let mut ti = TransferItem::new(source, dest, None, 1).await;
        ti.dest_min_chunk_size = 4;
        let ti = Arc::new(ti);

        let run = tokio::spawn({
            let ti = ti.clone();
            async move { ti.start().await }
        });
        // Let the first part's read reach the wait (one refresh interval is 5 s;
        // the wait itself starts within milliseconds).
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        tokio::time::timeout(std::time::Duration::from_secs(2), ti.cancel(true))
            .await
            .expect("pause must not wait for the uploader");
        run.await.unwrap().unwrap();
        assert_eq!(ti.current_state().await, TransferItemState::Cancelled);
        wait_idle(&ti).await;
        assert_eq!(
            ti.stats().snapshot().dst_bytes_written,
            0,
            "nothing was servable, nothing landed"
        );
    }

    // ── multipart edges ────────────────────────────────────────────────────

    #[tokio::test]
    async fn sequential_dest_reorders_parts_end_to_end() {
        use crate::transfers::StreamDest;
        let src = InMemSource::new(files(&[("big.bin", b"0123456789AB")]));
        let stream = Arc::new(RwLock::new(StreamDest::new(None)));
        let dest: Arc<RwLock<dyn StorageDestMultipart>> = stream.clone();
        let mut ti = TransferItem::new(src_arc(src), dest, None, 4).await;
        ti.dest_min_chunk_size = 4;
        ti.start().await.unwrap();
        assert_eq!(ti.current_state().await, TransferItemState::Finished);
        assert_eq!(stream.read().await.output().await, b"0123456789AB");
    }

    /// A source that can't be sized yet answers `Incomplete` to the stats
    /// walk: the total stays unknown (0) while bytes move, and is filled in
    /// on the pass after the source turns complete — never a partial number.
    #[tokio::test]
    async fn whole_share_sizing_deferred_until_source_complete() {
        let initial = files(&[("a.txt", b"hello")]);
        let late = files(&[("b.txt", b"world!")]);
        let source: Arc<RwLock<dyn StorageSource>> =
            Arc::new(RwLock::new(LiveSource::new(initial, late).with_deferred_sizing()));
        let dest = Arc::new(InMemDest::new());
        let ti = Arc::new(TransferItem::new(source, dest_arc(&dest), None, 2).await);

        let run = tokio::spawn({
            let ti = ti.clone();
            async move { ti.start().await }
        });
        // Pass 1 moves a.txt with no total known.
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while ti.stats().snapshot().dst_bytes_written < 5 {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("pass 1 never landed a.txt");
        assert_eq!(
            ti.stats().snapshot().total_bytes,
            0,
            "no partial total while the source is incomplete"
        );
        assert_eq!(ti.progress().await.ratio, 0.0);

        run.await.unwrap().unwrap();
        assert_eq!(ti.current_state().await, TransferItemState::Finished);
        assert_eq!(ti.stats().snapshot().total_bytes, 11, "sized once the source completed");
        assert_eq!(ti.progress().await.bytes, 11);
        assert_eq!(dest.put_count("b.txt").await, 1);
    }

    /// Every part landed in a previous run but `complete_multipart` never ran
    /// (crash between the last part's record and the completion): the resume
    /// must complete it instead of leaving an open multipart behind a
    /// Finished run.
    #[tokio::test]
    async fn resume_with_all_parts_present_completes_multipart() {
        let src = InMemSource::new(files(&[("big.bin", b"0123456789AB")]));
        let dest = Arc::new(InMemDest::new());
        dest.preload_multipart(
            "big.bin",
            vec![part(1, 0, 4, false), part(2, 4, 4, false), part(3, 8, 4, true)],
        )
        .await;
        let mut ti = TransferItem::new(src_arc(src), dest_arc(&dest), None, 2).await;
        ti.dest_min_chunk_size = 4;
        ti.start().await.unwrap();

        assert_eq!(
            dest.completed_multiparts().await,
            vec!["big.bin"],
            "completed without a part task"
        );
        let stats = ti.stats().snapshot();
        assert_eq!(stats.bytes_skipped, 12, "all parts credited as resumed");
        assert_eq!(stats.dst_bytes_written, 0, "nothing re-uploaded");
        assert_eq!(ti.current_state().await, TransferItemState::Finished);
    }

    #[tokio::test]
    async fn unguessable_part_size_restarts_multipart() {
        let src = InMemSource::new(files(&[("big.bin", b"0123456789AB")]));
        let dest = Arc::new(InMemDest::new());
        // A lone part that isn't #1 — no way to infer the prior run's part size.
        dest.preload_multipart("big.bin", vec![part(2, 4, 4, false)]).await;
        let mut ti = TransferItem::new(src_arc(src), dest_arc(&dest), None, 2).await;
        ti.dest_min_chunk_size = 4;
        ti.start().await.unwrap();

        assert_eq!(
            dest.cancelled_multiparts().await,
            vec!["big.bin"],
            "stale upload dropped"
        );
        let stats = ti.stats().snapshot();
        assert_eq!(stats.bytes_skipped, 0, "unusable parts are not resume credit");
        assert_eq!(stats.dst_bytes_written, 12, "whole file re-uploaded");
        assert_eq!(
            dest.snapshot().await.get("big.bin").map(|v| v.as_slice()),
            Some(b"0123456789AB".as_slice())
        );
    }

    #[tokio::test]
    async fn complete_multipart_fires_once() {
        let src = InMemSource::new(files(&[("big.bin", b"0123456789AB")]));
        let dest = Arc::new(InMemDest::new());
        let mut ti = TransferItem::new(src_arc(src), dest_arc(&dest), None, 3).await;
        ti.dest_min_chunk_size = 4;
        ti.start().await.unwrap();
        assert_eq!(dest.completed_multiparts().await, vec!["big.bin"]);
        assert_eq!(
            dest.snapshot().await.get("big.bin").map(|v| v.as_slice()),
            Some(b"0123456789AB".as_slice())
        );
    }

    #[tokio::test]
    async fn ninja_parts_feed_class3_dest() {
        let mut src = InMemSource::new(files(&[("p.bin", b"0123456789AB")]));
        src.incomplete_parts.insert(
            "p.bin".to_string(),
            vec![part(1, 0, 4, false), part(2, 4, 4, false), part(3, 8, 4, true)],
        );
        let dest = Arc::new(InMemDest::new());
        let ti = TransferItem::new(src_arc(src), dest_arc(&dest), None, 2).await;
        ti.start().await.unwrap();
        assert_eq!(
            dest.snapshot().await.get("p.bin").map(|v| v.as_slice()),
            Some(b"0123456789AB".as_slice())
        );
        let stats = ti.stats().snapshot();
        assert_eq!(stats.dst_bytes_written, 12);
        assert_eq!(stats.bytes_skipped, 0);
    }

    #[tokio::test]
    async fn ninja_parts_resume_skips_existing() {
        let mut src = InMemSource::new(files(&[("p.bin", b"0123456789AB")]));
        src.incomplete_parts.insert(
            "p.bin".to_string(),
            vec![part(1, 0, 4, false), part(2, 4, 4, false), part(3, 8, 4, true)],
        );
        let dest = Arc::new(InMemDest::new());
        dest.preload_multipart("p.bin", vec![part(1, 0, 4, false)]).await;
        let ti = TransferItem::new(src_arc(src), dest_arc(&dest), None, 2).await;
        ti.start().await.unwrap();
        let stats = ti.stats().snapshot();
        assert_eq!(stats.bytes_skipped, 4, "prior-run part credited once");
        assert_eq!(stats.src_bytes_read, 8, "only the missing parts fetched");
        let snap = dest.snapshot().await;
        assert_eq!(
            &snap["p.bin"][4..12],
            b"456789AB",
            "missing parts landed at their offsets"
        );
    }

    #[tokio::test]
    async fn part_error_skip_abandons_whole_file() {
        let src = InMemSource::new(files(&[("big.bin", b"01234567"), ("ok.txt", b"fine")]));
        let dest = Arc::new(InMemDest::new());
        dest.fail_next_part_puts("big.bin", 1, 1).await;
        dest.fail_next_part_puts("big.bin", 2, 1).await;
        let policies = TransferPolicies {
            on_error: OnError::Skip,
            ..Default::default()
        };
        let mut ti = TransferItem::new(src_arc(src), dest_arc(&dest), None, 1)
            .await
            .with_policies(policies);
        ti.dest_min_chunk_size = 4;
        ti.start().await.unwrap();

        assert_eq!(ti.skipped_keys(), vec!["big.bin"], "skipped once, not per part");
        let stats = ti.stats().snapshot();
        assert_eq!(stats.files_skipped, 1);
        assert_eq!(
            stats.bytes_skipped, 8,
            "abandoned file's size credited so the bar closes"
        );
        assert_eq!(
            dest.cancelled_multiparts().await,
            vec!["big.bin"],
            "multipart abandoned once"
        );
        assert!(!dest.snapshot().await.contains_key("big.bin"));
        assert_eq!(dest.snapshot().await["ok.txt"], b"fine");
        assert_eq!(ti.current_state().await, TransferItemState::Finished);
    }

    /// Abandoning a file credits only what hasn't been counted yet: the part
    /// that landed is already in `dst_bytes_written`, so the skip credit is
    /// the remainder and the two sum to the file size exactly (no clamp).
    #[tokio::test]
    async fn part_error_skip_credits_only_the_remainder() {
        let src = InMemSource::new(files(&[("big.bin", b"01234567")]));
        let dest = Arc::new(InMemDest::new());
        dest.fail_next_part_puts("big.bin", 2, 1).await;
        let policies = TransferPolicies {
            on_error: OnError::Skip,
            ..Default::default()
        };
        let mut ti = TransferItem::new(src_arc(src), dest_arc(&dest), None, 1)
            .await
            .with_policies(policies);
        ti.dest_min_chunk_size = 4;
        ti.start().await.unwrap();

        assert_eq!(ti.skipped_keys(), vec!["big.bin"]);
        let stats = ti.stats().snapshot();
        assert_eq!(stats.dst_bytes_written, 4, "part 1 landed before the skip");
        assert_eq!(stats.bytes_skipped, 4, "only the unlanded remainder is credited");
        assert_eq!(dest.cancelled_multiparts().await, vec!["big.bin"]);
    }

    // ── policies + entry types ─────────────────────────────────────────────

    #[tokio::test]
    async fn keep_newer_matrix() {
        use chrono::TimeZone;
        let t1 = chrono::Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let t2 = chrono::Utc.with_ymd_and_hms(2026, 1, 2, 0, 0, 0).unwrap();

        let mut src = InMemSource::new(files(&[
            ("newer.txt", b"new"),
            ("older.txt", b"new"),
            ("src_only.txt", b"new"),
            ("none.txt", b"new"),
        ]));
        src.mtimes.insert("newer.txt".into(), t2);
        src.mtimes.insert("older.txt".into(), t1);
        src.mtimes.insert("src_only.txt".into(), t1);

        let dest = Arc::new(InMemDest::new());
        dest.preload_with_mtime("newer.txt", b"old", t1).await;
        dest.preload_with_mtime("older.txt", b"old", t2).await;
        dest.preload("src_only.txt", b"old").await;
        dest.preload("none.txt", b"old").await;

        let ti = TransferItem::new(src_arc(src), dest_arc(&dest), None, 2)
            .await
            .with_policies(existing(OnExisting::KeepNewer));
        ti.start().await.unwrap();

        let snap = dest.snapshot().await;
        assert_eq!(snap["newer.txt"], b"new", "source newer → overwritten");
        assert_eq!(snap["older.txt"], b"old", "dest newer → kept");
        assert_eq!(snap["src_only.txt"], b"new", "dest mtime unknown → overwritten");
        assert_eq!(snap["none.txt"], b"old", "both unknown → kept");
        assert_eq!(ti.stats().snapshot().bytes_skipped, 6, "the two kept files credited");
    }

    #[tokio::test]
    async fn fatal_no_space_overrides_skip_policy() {
        let dest = Arc::new(InMemDest::new());
        dest.fail_next_puts_with("f.txt", 1, PutFailKind::NoSpace).await;
        let policies = TransferPolicies {
            on_error: OnError::Skip,
            ..Default::default()
        };
        let (ti, res) = run_with(files(&[("f.txt", b"x")]), &dest, policies).await;
        assert!(
            matches!(&res, Err(StorageError::AtKey { key, source })
                if key == "f.txt" && matches!(**source, StorageError::NoSpace { .. })),
            "got {res:?}"
        );
        assert_eq!(ti.current_state().await, TransferItemState::Error);
        assert!(ti.skipped_keys().is_empty(), "fatal errors are never skips");
    }

    #[tokio::test]
    async fn fatal_session_expired_bypasses_ask() {
        let dest = Arc::new(InMemDest::new());
        dest.fail_next_puts_with("f.txt", 1, PutFailKind::SessionExpired).await;
        let resolver = Arc::new(FixedErrorResolver {
            asks: Default::default(),
            answer: ErrorDecision::Retry,
        });
        let policies = TransferPolicies {
            on_error: OnError::Ask,
            error_resolver: Some(resolver.clone()),
            ..Default::default()
        };
        let (ti, res) = run_with(files(&[("f.txt", b"x")]), &dest, policies).await;
        assert!(
            matches!(&res, Err(StorageError::AtKey { key, source })
                if key == "f.txt" && matches!(**source, StorageError::SessionExpired(_))),
            "got {res:?}"
        );
        assert!(
            resolver.asks.lock().unwrap().is_empty(),
            "fatal errors never reach the resolver"
        );
        assert_eq!(ti.current_state().await, TransferItemState::Error);
    }

    #[tokio::test]
    async fn existing_dir_merges_without_conflict() {
        let mut src = InMemSource::new(files(&[("d/f.txt", b"inside")]));
        src.dirs = vec!["d".into()];
        let dest = Arc::new(InMemDest::new());
        dest.preload_dir("d").await;
        let ti = TransferItem::new(src_arc(src), dest_arc(&dest), None, 2).await;
        ti.start().await.unwrap();
        assert_eq!(dest.dir_put_count("d").await, 0, "existing dir merged, not re-created");
        assert_eq!(ti.stats().snapshot().dst_dirs_written, 0);
        assert_eq!(dest.snapshot().await["d/f.txt"], b"inside");
        assert_eq!(ti.current_state().await, TransferItemState::Finished);
    }

    #[tokio::test]
    async fn file_type_mismatch_fails_loud() {
        let src = InMemSource::new(files(&[("x", b"file")]));
        let dest = Arc::new(InMemDest::new());
        dest.preload_dir("x").await;
        let ti = TransferItem::new(src_arc(src), dest_arc(&dest), None, 2).await;
        let err = ti.start().await.expect_err("dir on dest where the source has a file");
        assert!(err.to_string().contains("file type mismatch"), "got: {err}");
        assert_eq!(ti.current_state().await, TransferItemState::Error);
    }

    #[tokio::test]
    async fn symlink_transfer_counts_bytes_and_links() {
        let mut src = InMemSource::new(files(&[("a.txt", b"hello")]));
        src.symlinks.insert("link".into(), "a.txt".into());
        let dest = Arc::new(InMemDest::new());
        let ti = TransferItem::new(src_arc(src), dest_arc(&dest), None, 2).await;
        ti.start().await.unwrap();
        assert_eq!(dest.symlinks_snapshot().await.get("link"), Some(&"a.txt".to_string()));
        let stats = ti.stats().snapshot();
        assert_eq!(stats.dst_symlinks_written, 1);
        assert_eq!(stats.dst_bytes_written, 10, "file bytes + symlink target length");
        let p = ti.progress().await;
        assert_eq!(p.total_bytes, 10, "symlink weighs its target length");
        assert_eq!(p.bytes, p.total_bytes, "symlink bytes close the bar");
    }

    // ── progress accounting ────────────────────────────────────────────────

    /// Local dest: the bar is resume credit + what this item received.
    #[tokio::test]
    async fn moved_bytes_local_dest_counts_received_and_clamps() {
        let dest = Arc::new(InMemDest::new());
        let src = InMemSource::new(std::collections::HashMap::new());
        let ti = TransferItem::new(src_arc(src), dest_arc(&dest), None, 1).await;

        let stats = ti.stats();
        stats.bytes_skipped.store(10, Ordering::Relaxed);
        stats.bytes_received.store(25, Ordering::Relaxed);
        // Writes add nothing: they are the received bytes again.
        stats.dst_bytes_written.store(20, Ordering::Relaxed);

        stats.total_bytes.store(100, Ordering::Relaxed);
        assert_eq!(ti.moved_bytes().await, 35);

        stats.total_bytes.store(30, Ordering::Relaxed);
        assert_eq!(ti.moved_bytes().await, 30, "clamped to the total");

        stats.total_bytes.store(0, Ordering::Relaxed);
        assert_eq!(ti.moved_bytes().await, 35, "unknown total → no clamp");
    }

    /// Wire dest: the bar is resume credit + the dest's own sent progress,
    /// less what the dest had already sent before this item existed.
    #[tokio::test]
    async fn moved_bytes_wire_dest_reads_the_dest_progress() {
        let mut dest = InMemDest::new();
        dest.sent_progress = Some(std::sync::atomic::AtomicU64::new(100));
        let dest = Arc::new(dest);
        let src = InMemSource::new(std::collections::HashMap::new());
        let ti = TransferItem::new(src_arc(src), dest_arc(&dest), None, 1).await;

        let stats = ti.stats();
        stats.bytes_skipped.store(10, Ordering::Relaxed);
        // What the engine received is not the bar here — the wire is.
        stats.bytes_received.store(1000, Ordering::Relaxed);
        stats.total_bytes.store(1000, Ordering::Relaxed);
        assert_eq!(ti.moved_bytes().await, 10, "nothing sent since the item was created");

        dest.sent_progress.as_ref().unwrap().store(140, Ordering::Relaxed);
        assert_eq!(ti.moved_bytes().await, 50);
    }

    /// A part whose put failed is read again: its bytes leave the counter
    /// so the retry does not count them twice.
    #[tokio::test]
    async fn retried_put_does_not_double_count_received_bytes() {
        let dest = Arc::new(InMemDest::new());
        dest.fail_next_part_puts("big.bin", 2, 1).await;
        let src = InMemSource::new(files(&[("big.bin", b"0123456789AB")]));
        let resolver = Arc::new(FixedErrorResolver {
            asks: Default::default(),
            answer: ErrorDecision::Retry,
        });
        let policies = TransferPolicies {
            on_error: OnError::Ask,
            error_resolver: Some(resolver),
            ..Default::default()
        };
        let mut ti = TransferItem::new(src_arc(src), dest_arc(&dest), None, 1)
            .await
            .with_policies(policies);
        ti.dest_min_chunk_size = 4;
        ti.start().await.unwrap();
        assert_eq!(ti.current_state().await, TransferItemState::Finished);
        let stats = ti.stats().snapshot();
        assert_eq!(stats.src_bytes_read, 16, "one part read twice");
        assert_eq!(stats.bytes_received, 12, "the failed put's bytes rolled back");
        assert_eq!(ti.moved_bytes().await, 12);
    }

    #[tokio::test]
    async fn current_file_is_lowest_active_key() {
        let (gate, mut entered) = GetGate::for_keys(["a.txt", "b.txt"]);
        let mut src = InMemSource::new(files(&[("a.txt", b"hello"), ("b.txt", b"world")]));
        src.gate = Some(gate.clone());
        let dest = Arc::new(InMemDest::new());
        let ti = Arc::new(TransferItem::new(src_arc(src), dest_arc(&dest), None, 2).await);

        let run = tokio::spawn({
            let ti = ti.clone();
            async move { ti.start().await }
        });
        entered.recv().await.unwrap();
        entered.recv().await.unwrap();

        let active = ti.active_keys();
        assert!(active.contains("a.txt") && active.contains("b.txt"), "got: {active:?}");
        assert_eq!(ti.progress().await.current_file.as_deref(), Some("a.txt"));

        gate.release_all();
        run.await.unwrap().unwrap();
        assert!(
            ti.progress().await.current_file.is_none(),
            "no in-flight work when finished"
        );
    }

    /// Zip end-to-end: what the engine wrote comes back out of the archive.
    #[tokio::test]
    async fn zip_end_to_end_readback() {
        use crate::transfers::ZipDest;
        use std::io::Read;
        let mut src = InMemSource::new(files(&[("d/a.txt", b"alpha"), ("b.txt", b"beta")]));
        src.dirs = vec!["d".into()];
        let zip_dest = Arc::new(RwLock::new(ZipDest::new()));
        let dest: Arc<RwLock<dyn StorageDestMultipart>> = zip_dest.clone();
        let ti = TransferItem::new(src_arc(src), dest, None, 2).await;
        ti.start().await.unwrap();
        assert_eq!(ti.current_state().await, TransferItemState::Finished);

        let bytes = zip_dest.read().await.finish().await.unwrap();
        let mut archive = zip::ZipArchive::new(std::io::Cursor::new(bytes)).unwrap();
        let mut names: Vec<String> = archive.file_names().map(String::from).collect();
        names.sort();
        assert_eq!(names, ["b.txt", "d/", "d/a.txt"]);
        let mut content = String::new();
        archive
            .by_name("d/a.txt")
            .unwrap()
            .read_to_string(&mut content)
            .unwrap();
        assert_eq!(content, "alpha");
    }
}
