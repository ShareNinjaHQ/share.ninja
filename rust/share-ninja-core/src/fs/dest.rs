//! Local filesystem storage destination for the in-process transfer engine.
//!
//! `FsDest` implements the `share-ninja-core` storage destination traits, backed
//! by a per-download persistent metadata store ([`FsMetaStore`]) — the Rust port
//! of Electron's `FSStorageMeta` (`apps/electron/src/node/FSStorageMeta.ts`). The
//! store makes multipart downloads crash-safe/resumable and gives `FsDest`
//! virtual→real path mapping with top-level collision avoidance. See
//! `docs/app/RustFSImps.md`.
//!
//! `FsDest` is also a `StorageSource` over what it landed: the store's
//! `roots` + `renames` are the key→disk mapping, its in-flight records mark
//! what is not yet whole. That is how a download is browsed and previewed
//! locally — in any state, and after the whole share landed from the
//! record's roots and renames alone ([`FsDest::landed`]).

use std::collections::{HashMap, VecDeque};
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use sha2::{Digest, Sha256};
use tokio::fs;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::mpsc;

use crate::transfers::{
    PreviewGenerator, PreviewImage, SearchOptions, SearchResult, StorageFileAttrs, StoragePutConditions,
    StoragePutResult, StorageSearchQuery, StorageSourceType, StorageStatInfo, reject_conditions,
};
use crate::{
    Storage, StorageDest, StorageDestMultipart, StorageError, StorageFileType, StorageMetaState,
    StorageMultipartUpload, StorageObjectInfo, StoragePartInfo, StorageSource,
};

use super::meta_store::{CollisionPolicy, FsEntryType, FsMetaStore, FsPartInfo, NameRule, WriteKind};
use super::preview::spawn_previews;
use super::search::{BatchReceiver, SEARCH_BATCH, SearchFilter, SearchPager, push_filtered};
use super::stream::stream_file;
use super::walk::{
    EntryKind, IncompleteParts, file_etag, meta_parts, reverse_renames, soi_from, spawn_listing, walk_into_sink,
};

/// Restore `mtime`/exec-bits on `path` (Rust port of `applyAttrs`). Best-effort:
/// attribute restoration never fails the transfer (e.g. setting an mtime on a
/// directory, or perms on a read-only FS, is silently skipped). Runs the sync
/// `std::fs` calls on a blocking thread so the runtime isn't stalled.
pub(crate) async fn apply_attrs(path: &Path, attrs: Option<&StorageFileAttrs>) -> Result<(), StorageError> {
    let Some(attrs) = attrs else { return Ok(()) };
    let mtime = attrs.mtime;
    let perm = attrs.posix_permissions;
    if mtime.is_none() && perm.is_none() {
        return Ok(());
    }
    let p = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        if let Some(mt) = mtime {
            let st: std::time::SystemTime = mt.into();
            // Opening for write is required to set times; a directory (or a
            // read-only target) can't be, so skip rather than fail.
            if let Ok(f) = std::fs::OpenOptions::new().write(true).open(&p) {
                let _ = f.set_times(std::fs::FileTimes::new().set_modified(st));
            }
        }
        #[cfg(unix)]
        if let Some(perm) = perm {
            // Match Electron: only restore exec bits, gated on any exec bit set,
            // mirroring the read bits.
            if perm & 0o111 != 0 {
                use std::os::unix::fs::PermissionsExt;
                if let Ok(md) = std::fs::metadata(&p) {
                    let mode = md.permissions().mode();
                    let mut perms = md.permissions();
                    perms.set_mode(mode | ((mode & 0o444) >> 2));
                    let _ = std::fs::set_permissions(&p, perms);
                }
            }
        }
        #[cfg(not(unix))]
        let _ = perm;
    })
    .await
    .map_err(|e| StorageError::Other(format!("apply_attrs join: {e}")))?;
    Ok(())
}

fn hex_lower(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

// ── Resume-time part verifier (parallel, positional reads) ───────────────────

/// One recorded part to check.
struct PartJob {
    number: u32,
    offset: u64,
    size: u64,
    etag: Option<String>,
}

/// One in-progress object and the data file its parts live in.
struct ObjJob {
    key: String,
    path: PathBuf,
    /// Data-file length at gather time; `None` = missing/unreadable.
    len: Option<u64>,
    parts: Vec<PartJob>,
}

/// Per-object outcome of the verify pass.
struct ObjResult {
    key: String,
    total: usize,
    good: Vec<u32>,
    dropped: Vec<(u32, String)>, // (part_number, reason) — for logging
}

enum Verdict {
    Keep,
    Mismatch,
    Unreadable(String),
}

/// Read exactly `buf.len()` bytes at `offset` *without* a shared file cursor —
/// `pread` on unix, offset-`ReadFile` on Windows — so many threads can read
/// disjoint ranges of one file concurrently. A short read surfaces as `Err`
/// (which the verifier treats as a torn part), never SIGBUS.
#[cfg(unix)]
fn pread_exact(file: &std::fs::File, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;
    file.read_exact_at(buf, offset)
}
#[cfg(windows)]
fn pread_exact(file: &std::fs::File, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
    use std::os::windows::fs::FileExt;
    let mut done = 0;
    while done < buf.len() {
        match file.seek_read(&mut buf[done..], offset + done as u64)? {
            0 => return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "early eof")),
            n => done += n,
        }
    }
    Ok(())
}

/// Streaming hex-sha256 of `[offset, offset+size)` of the file at `path`, read
/// positionally in ≤1 MiB chunks through the reused `buf` so memory stays flat
/// even for multi-MB parts. `sha2` uses hardware sha256 (SHA-NI / ARM crypto).
fn pread_hash(path: &Path, offset: u64, size: u64, buf: &mut Vec<u8>) -> std::io::Result<String> {
    const CHUNK: u64 = 1024 * 1024;
    let file = std::fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut remaining = size;
    let mut off = offset;
    while remaining > 0 {
        let want = remaining.min(CHUNK) as usize;
        if buf.len() < want {
            buf.resize(want, 0);
        }
        pread_exact(&file, &mut buf[..want], off)?;
        hasher.update(&buf[..want]);
        off += want as u64;
        remaining -= want as u64;
    }
    Ok(hex_lower(&hasher.finalize()))
}

fn verify_one(obj: &ObjJob, part: &PartJob, buf: &mut Vec<u8>) -> Verdict {
    // No stored hash (legacy/hand-edited doc) — can't verify; keep.
    let Some(expected) = part.etag.as_deref() else {
        return Verdict::Keep;
    };
    let Some(len) = obj.len else {
        return Verdict::Unreadable("data file missing".into());
    };
    // Past-EOF (file shorter than the recorded part) → torn; no read needed.
    match part.offset.checked_add(part.size) {
        Some(end) if end <= len => {}
        _ => return Verdict::Mismatch,
    }
    match pread_hash(&obj.path, part.offset, part.size, buf) {
        Ok(h) if h == expected => Verdict::Keep,
        Ok(_) => Verdict::Mismatch,
        Err(e) => Verdict::Unreadable(e.to_string()),
    }
}

/// Hash every part of every object in parallel — one worker per core pulling
/// jobs off a shared atomic index (work-stealing for the uneven tail), each
/// reading its own ranges via `pread` (no shared cursor). Runs inside
/// `spawn_blocking`; no external deps.
fn hash_jobs(jobs: Vec<ObjJob>) -> Vec<ObjResult> {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let flat: Vec<(usize, usize)> = jobs
        .iter()
        .enumerate()
        .flat_map(|(oi, o)| (0..o.parts.len()).map(move |pi| (oi, pi)))
        .collect();
    let nthreads = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        .min(flat.len().max(1));

    let next = AtomicUsize::new(0);
    let partials: Vec<Vec<(usize, Verdict)>> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..nthreads)
            .map(|_| {
                s.spawn(|| {
                    let mut buf: Vec<u8> = Vec::new();
                    let mut out: Vec<(usize, Verdict)> = Vec::new();
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        if i >= flat.len() {
                            break;
                        }
                        let (oi, pi) = flat[i];
                        out.push((i, verify_one(&jobs[oi], &jobs[oi].parts[pi], &mut buf)));
                    }
                    out
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().expect("verify worker panicked"))
            .collect()
    });

    let mut results: Vec<ObjResult> = jobs
        .iter()
        .map(|o| ObjResult {
            key: o.key.clone(),
            total: o.parts.len(),
            good: Vec::new(),
            dropped: Vec::new(),
        })
        .collect();
    for chunk in partials {
        for (i, verdict) in chunk {
            let (oi, pi) = flat[i];
            let num = jobs[oi].parts[pi].number;
            match verdict {
                Verdict::Keep => results[oi].good.push(num),
                Verdict::Mismatch => results[oi]
                    .dropped
                    .push((num, "data does not match stored hash (torn/lost write)".to_string())),
                Verdict::Unreadable(e) => results[oi].dropped.push((num, format!("unreadable/short read: {e}"))),
            }
        }
    }
    results
}

/// Free and total bytes of the volume holding `path`, resolved on the nearest
/// existing ancestor (a fresh download's base dir may not exist yet). `free`
/// is what an unprivileged writer can actually use (`f_bavail`, not `f_bfree`).
pub fn volume_free_capacity(path: &Path) -> Result<(u64, u64), StorageError> {
    let mut p = path.to_path_buf();
    while !p.exists() {
        if !p.pop() {
            return Err(StorageError::Other("no existing ancestor for space check".into()));
        }
    }

    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        let c = std::ffi::CString::new(p.as_os_str().as_bytes())
            .map_err(|_| StorageError::Other("path not usable for statvfs".into()))?;
        let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
        if unsafe { libc::statvfs(c.as_ptr(), &mut st) } != 0 {
            return Err(StorageError::Io(std::io::Error::last_os_error()));
        }
        let frsize = if st.f_frsize > 0 { st.f_frsize } else { st.f_bsize } as u64;
        Ok((st.f_bavail as u64 * frsize, st.f_blocks as u64 * frsize))
    }

    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        let wide: Vec<u16> = p.as_os_str().encode_wide().chain(std::iter::once(0)).collect();
        let mut free_avail: u64 = 0;
        let mut total: u64 = 0;
        let ok = unsafe {
            windows_sys::Win32::Storage::FileSystem::GetDiskFreeSpaceExW(
                wide.as_ptr(),
                &mut free_avail,
                &mut total,
                std::ptr::null_mut(),
            )
        };
        if ok == 0 {
            return Err(StorageError::Io(std::io::Error::last_os_error()));
        }
        Ok((free_avail, total))
    }
}

/// Filesystem destination: writes downloaded files under a base directory, with
/// crash-safe multipart resume + collision handling via [`FsMetaStore`].
pub struct FsDest {
    base_path: PathBuf,
    meta: FsMetaStore,
    data_source_id: Option<String>,
    /// The parked paged-search walks (see `SearchPager`).
    search_pager: SearchPager,
    /// The shell's thumbnailer; `None` = this dest serves no previews.
    previews: Option<Arc<dyn PreviewGenerator>>,
}

impl FsDest {
    /// Open `FsDest` over `base_path`, with multipart metadata persisted under
    /// `meta_dir`, the given top-level collision `policy` and the name `rule`
    /// of the destination platform ([`NameRule::platform`]).
    ///
    /// When `verify_parts_on_load` is set, every recorded part is re-hashed
    /// against its stored etag once at load (resume) and any whose data didn't
    /// survive a crash is dropped so the engine re-downloads it — see
    /// [`Self::verify_loaded_parts`]. Off ⇒ recorded parts are trusted (faster
    /// resume, but a power-loss-torn part could slip through, as in Electron).
    pub async fn new(
        base_path: PathBuf,
        meta_dir: PathBuf,
        policy: CollisionPolicy,
        rule: NameRule,
        verify_parts_on_load: bool,
    ) -> Result<Self, StorageError> {
        let meta = FsMetaStore::open(meta_dir, policy, rule).await?;
        let dest = Self {
            base_path,
            meta,
            data_source_id: None,
            search_pager: SearchPager::new(),
            previews: None,
        };
        if verify_parts_on_load {
            dest.verify_loaded_parts().await?;
        }
        Ok(dest)
    }

    /// The dest of a download that landed whole, from the roots and renames
    /// its record carries: no meta dir, nothing in flight, reads only (a
    /// write errors).
    pub fn landed(base_path: PathBuf, roots: Vec<String>, renames: HashMap<String, String>, rule: NameRule) -> Self {
        Self {
            base_path,
            meta: FsMetaStore::landed(roots, renames, rule),
            data_source_id: None,
            search_pager: SearchPager::new(),
            previews: None,
        }
    }

    /// Serve `get_previews` of the landed files through the shell's
    /// thumbnailer (`None` = don't).
    pub fn with_preview_generator(mut self, previews: Option<Arc<dyn PreviewGenerator>>) -> Self {
        self.previews = previews;
        self
    }

    /// Snapshot of the in-flight records for a listing overlay.
    async fn incomplete_parts(&self) -> IncompleteParts {
        let mut out = IncompleteParts::new();
        for key in self.meta.list_multiparts().await {
            if let Some(info) = self.meta.read(&key).await {
                out.insert(key, Self::to_part_infos(&info.parts));
            }
        }
        out
    }

    /// The listing producer behind `get_dir` and `search`: `""` walks the
    /// roots in key order (each root is an entry, a dir root descends when
    /// `recursive`); any other `path` resolves through the renames and walks
    /// that dir. Every key and name is virtual — the walk maps renamed disk
    /// names back. A root gone from disk is skipped, a path that resolves to
    /// nothing or to a file lists nothing. In-flight files are listed with
    /// `is_complete = false` and their landed parts.
    async fn open_listing(
        &self,
        path: &str,
        recursive: bool,
        max_batch_size: Option<usize>,
        mut filter: Option<SearchFilter>,
    ) -> BatchReceiver {
        let incomplete = self.incomplete_parts().await;
        let renames = self.meta.renames().await;
        let reverse = reverse_renames(&renames);
        if path.is_empty() {
            let mut roots: Vec<(String, PathBuf)> = self
                .meta
                .roots()
                .await
                .into_iter()
                .map(|key| {
                    let disk = renames.get(&key).unwrap_or(&key);
                    let real = self.base_path.join(disk);
                    (key, real)
                })
                .collect();
            roots.sort();
            return spawn_listing(max_batch_size, move |sink| {
                for (key, root) in &roots {
                    let Ok(md) = std::fs::symlink_metadata(root) else {
                        continue;
                    };
                    let (kind, size, mtime, mode) = meta_parts(root, &md);
                    push_filtered(sink, &mut filter, key, || {
                        let mut soi = soi_from(key, kind, size, mtime, mode);
                        if let Some(parts) = incomplete.get(key) {
                            soi.is_complete = false;
                            soi.parts = Some(parts.clone());
                        }
                        soi
                    })?;
                    if kind == EntryKind::Dir && recursive {
                        walk_into_sink(root, key, recursive, sink, &mut filter, &incomplete, &reverse)?;
                    }
                }
                Ok(())
            });
        }
        let dir = self.meta.resolve_mapped(&self.base_path, path).await;
        let prefix = path.to_string();
        spawn_listing(max_batch_size, move |sink| {
            let Some(dir) = dir else {
                return Ok(());
            };
            if !std::fs::symlink_metadata(&dir)?.is_dir() {
                return Ok(());
            }
            walk_into_sink(&dir, &prefix, recursive, sink, &mut filter, &incomplete, &reverse)
        })
    }

    /// A complete landed file's on-disk path — what the source side streams
    /// and opens in place. `None` for an unmapped key, an in-flight file, or
    /// anything but a plain file.
    async fn complete_file_path(&self, key: &str) -> Option<PathBuf> {
        if self.meta.read(key).await.is_some() {
            return None;
        }
        let path = self.meta.resolve_mapped(&self.base_path, key).await?;
        let md = fs::symlink_metadata(&path).await.ok()?;
        md.is_file().then_some(path)
    }

    /// Resume-time integrity pass: re-hash each recorded part's bytes against its
    /// stored sha256 etag and drop any that don't match (torn/lost write, or a
    /// missing file), so the engine re-downloads them. Runs once at construction;
    /// a no-op for fresh downloads (no recorded parts). Closes the only durability
    /// hole left by not fsync'ing part data — a flushed record whose bytes a power
    /// loss never made durable — by catching it here instead of on the hot path.
    async fn verify_loaded_parts(&self) -> Result<(), StorageError> {
        // Gather (cheap, async): each in-progress object + its parts + the data
        // file's length (for the past-EOF check).
        let mut jobs: Vec<ObjJob> = Vec::new();
        for key in self.meta.list_multiparts().await {
            let Some(info) = self.meta.read(&key).await else {
                continue;
            };
            if info.parts.is_empty() {
                continue;
            }
            let path = self.meta.resolve_existing(&self.base_path, &key).await;
            let len = fs::metadata(&path).await.ok().map(|m| m.len());
            let parts = info
                .parts
                .iter()
                .map(|p| PartJob {
                    number: p.part_number,
                    offset: p.offset,
                    size: p.size,
                    etag: p.etag.clone(),
                })
                .collect();
            jobs.push(ObjJob { key, path, len, parts });
        }
        if jobs.is_empty() {
            return Ok(()); // fresh download — nothing recorded to verify
        }

        // Hash everything in parallel, off the async runtime.
        let results = tokio::task::spawn_blocking(move || hash_jobs(jobs))
            .await
            .map_err(|e| StorageError::Other(format!("verify join: {e}")))?;

        // Apply: drop the failed parts so the engine re-downloads them.
        for r in results {
            if r.good.len() == r.total {
                continue;
            }
            for (num, reason) in &r.dropped {
                tracing::warn!(key = %r.key, part = num, "verify: dropping part — {reason}; re-downloading");
            }
            tracing::warn!(
                key = %r.key,
                "verify: {} of {} recorded parts failed integrity check",
                r.dropped.len(),
                r.total
            );
            self.meta.retain_parts(&r.key, &r.good).await?;
        }
        Ok(())
    }

    /// The top-level virtual names this download has written, for persisting
    /// onto the finished download together with [`Self::renames`].
    pub async fn roots(&self) -> Vec<String> {
        self.meta.roots().await
    }

    /// Virtual path of a component (any level) → its on-disk name, only where
    /// the two differ (a collision rename or a name-rule rewrite), relative to
    /// the dest base — relative so the record survives the platform moving
    /// the app container (iOS re-anchors it on every app update).
    pub async fn renames(&self) -> HashMap<String, String> {
        self.meta.renames().await
    }

    /// Everything this download has landed so far, part-accurate:
    /// `bytes_done` summed over every root. Backs the persisted
    /// `Download::landed_bytes`.
    pub async fn landed_bytes(&self) -> u64 {
        let mut total = 0u64;
        for key in self.meta.roots().await {
            total += self.bytes_done(&key).await;
        }
        total
    }

    /// Physical bytes this download already occupies on disk: lstat walk over
    /// the roots' on-disk targets (symlinks never followed), counting
    /// allocated blocks where the platform reports them — a sparse
    /// in-progress file counts what it actually consumes — falling back to
    /// logical size.
    async fn consumed_bytes(&self) -> Result<u64, StorageError> {
        let renames = self.meta.renames().await;
        let roots: Vec<PathBuf> = self
            .meta
            .roots()
            .await
            .iter()
            .map(|root| self.base_path.join(renames.get(root).unwrap_or(root)))
            .collect();
        if roots.is_empty() {
            return Ok(0);
        }
        tokio::task::spawn_blocking(move || {
            fn file_bytes(md: &std::fs::Metadata) -> u64 {
                #[cfg(unix)]
                {
                    use std::os::unix::fs::MetadataExt;
                    let blocks = md.blocks().saturating_mul(512);
                    if blocks > 0 {
                        return blocks;
                    }
                }
                md.len()
            }
            fn walk(p: &Path, total: &mut u64) {
                let Ok(md) = std::fs::symlink_metadata(p) else {
                    return;
                };
                if md.is_dir() {
                    if let Ok(rd) = std::fs::read_dir(p) {
                        for entry in rd.flatten() {
                            walk(&entry.path(), total);
                        }
                    }
                } else {
                    *total += file_bytes(&md);
                }
            }
            let mut total = 0u64;
            for root in &roots {
                walk(root, &mut total);
            }
            total
        })
        .await
        .map_err(|e| StorageError::Other(format!("space walk join: {e}")))
    }

    fn to_part_infos(parts: &[FsPartInfo]) -> Vec<StoragePartInfo> {
        parts
            .iter()
            .map(|p| StoragePartInfo {
                part_number: Some(p.part_number),
                offset: Some(p.offset),
                size: Some(p.size),
                etag: p.etag.clone(),
                ..Default::default()
            })
            .collect()
    }

    /// The real on-disk location of `key`, resolved strictly through the path
    /// map (foreign files invisible), or `None` when nothing landed there.
    /// Backs the browser's per-entry "Show in Finder".
    pub async fn local_path(&self, key: &str) -> Option<PathBuf> {
        let path = self.meta.resolve_mapped(&self.base_path, key).await?;
        match fs::symlink_metadata(&path).await {
            Ok(_) => Some(path),
            Err(_) => None,
        }
    }

    /// Bytes this download has actually landed under `key` (a file or a dir
    /// subtree), part-accurate: a complete file counts its logical size, an
    /// in-flight multipart counts the sum of its recorded part bytes — never
    /// the raw on-disk length, which positional part writes extend past what
    /// has arrived. Resolution goes strictly through the path map, so foreign
    /// files are invisible. Backs the per-file status API's dir `bytes_done`
    /// (docs/core/PartialDownloads.md).
    pub async fn bytes_done(&self, key: &str) -> u64 {
        // In-flight multipart corrections under this key: recorded part bytes
        // replace the disk length of each in-progress file.
        let mut corrections: Vec<(PathBuf, u64)> = Vec::new();
        for mp_key in self.meta.list_multiparts().await {
            let in_scope = mp_key == key || mp_key.strip_prefix(key).is_some_and(|rest| rest.starts_with('/'));
            if !in_scope {
                continue;
            }
            let Some(info) = self.meta.read(&mp_key).await else {
                continue;
            };
            let parts_sum: u64 = info.parts.iter().map(|p| p.size).sum();
            let path = self.meta.resolve_existing(&self.base_path, &mp_key).await;
            corrections.push((path, parts_sum));
        }

        let Some(root) = self.meta.resolve_mapped(&self.base_path, key).await else {
            return 0;
        };
        tokio::task::spawn_blocking(move || {
            fn walk(p: &Path, corrections: &[(PathBuf, u64)], total: &mut u64) {
                let Ok(md) = std::fs::symlink_metadata(p) else {
                    return;
                };
                if md.is_dir() {
                    if let Ok(rd) = std::fs::read_dir(p) {
                        for entry in rd.flatten() {
                            walk(&entry.path(), corrections, total);
                        }
                    }
                } else if let Some((_, parts_sum)) = corrections.iter().find(|(cp, _)| cp == p) {
                    *total += parts_sum;
                } else {
                    // Symlink sizes normalized like the source's walk, so a
                    // landed symlink tallies against the share's total.
                    *total += super::walk::entry_size(p, &md);
                }
            }
            let mut total = 0u64;
            walk(&root, &corrections, &mut total);
            total
        })
        .await
        .unwrap_or(0)
    }
}

#[async_trait]
impl Storage for FsDest {
    fn cleanup(&mut self) {}

    async fn connect(&mut self) -> Result<StorageMetaState, StorageError> {
        fs::create_dir_all(&self.base_path).await?;
        Ok(StorageMetaState::Unknown)
    }

    async fn cancel(&self) -> Result<(), StorageError> {
        Ok(())
    }

    /// Status of a single dest key, resolved strictly through the path map
    /// (pre-existing foreign files stay invisible — collision avoidance still
    /// applies to them on write) and overlaid with the multipart store for
    /// in-progress downloads. lstat: symlinks reported, never followed.
    /// `""` is the synthetic virtual-root `Dir`, as for every local source.
    async fn get_attrs(&self, object: &str) -> Result<StorageObjectInfo, StorageError> {
        if object.is_empty() {
            let mut soi = StorageObjectInfo::new("");
            soi.file_type = StorageFileType::Dir;
            return Ok(soi);
        }
        let resolved = self.meta.resolve_mapped(&self.base_path, object).await;
        let md = match &resolved {
            Some(p) => fs::symlink_metadata(p).await.ok(),
            None => None,
        };
        let record = self.meta.read(object).await;
        if md.is_none() && record.is_none() {
            return Err(StorageError::NoSuchKey(object.to_string()));
        }

        let mut soi = match (&resolved, md) {
            (Some(p), Some(md)) => {
                let (kind, size, mtime, mode) = meta_parts(p, &md);
                soi_from(object, kind, size, mtime, mode)
            }
            // record without bytes on disk: multipart begun, no parts written yet
            _ => StorageObjectInfo::new(object),
        };
        if let Some(info) = record {
            soi.is_complete = false;
            soi.parts = Some(Self::to_part_infos(&info.parts));
        }
        Ok(soi)
    }

    /// Streamed listing of what landed (see `open_listing`); the walk runs on
    /// a blocking thread as the receiver is consumed. No cursor: a local walk
    /// is in directory order, not key order.
    async fn get_dir(
        &self,
        path: &str,
        recursive: bool,
        start_after: Option<&str>,
        max_batch_size: Option<usize>,
    ) -> mpsc::Receiver<Result<Vec<StorageObjectInfo>, StorageError>> {
        if start_after.is_some() {
            let (tx, rx) = mpsc::channel(1);
            let _ = tx
                .send(Err(StorageError::NotImplemented("FsDest getDir start_after".into())))
                .await;
            return rx;
        }
        self.open_listing(path, recursive, max_batch_size, None).await
    }

    /// Name search over what landed, paged by `SearchPager` like
    /// `FsSource::search`.
    async fn search(&self, query: &StorageSearchQuery, options: &SearchOptions) -> Result<SearchResult, StorageError> {
        let scope = options.path_scope.as_deref().unwrap_or("");
        let recursive = options.recursive.unwrap_or(true);
        let filter = SearchFilter::new(query.name_match.as_deref());
        let rx = self.open_listing(scope, recursive, Some(SEARCH_BATCH), Some(filter)).await;
        self.search_pager.page(options, || (rx, VecDeque::new())).await
    }

    fn data_source_id(&self) -> Option<&str> {
        self.data_source_id.as_deref()
    }

    fn set_data_source_id(&mut self, id: String) {
        self.data_source_id = Some(id);
    }
}

#[async_trait]
impl StorageSource for FsDest {
    fn source_name(&self) -> &str {
        "LandedDownload"
    }

    fn source_type(&self) -> StorageSourceType {
        StorageSourceType::FileSystem
    }

    /// Chunked streaming read of a complete landed file. An in-flight file is
    /// `NoSuchKey`: a partial is never served as the file.
    async fn get_streamed(
        &self,
        object: &str,
        offset: Option<u64>,
        length: Option<u64>,
    ) -> mpsc::Receiver<Result<Vec<u8>, StorageError>> {
        match self.complete_file_path(object).await {
            Some(path) => stream_file(path, offset, length),
            None => {
                let (tx, rx) = mpsc::channel(1);
                let _ = tx.send(Err(StorageError::NoSuchKey(object.to_string()))).await;
                rx
            }
        }
    }

    async fn get_symlink(&self, object: &str) -> Result<String, StorageError> {
        let path = self
            .meta
            .resolve_mapped(&self.base_path, object)
            .await
            .ok_or_else(|| StorageError::NoSuchKey(object.to_string()))?;
        let target = fs::read_link(&path).await?;
        Ok(target.to_string_lossy().to_string())
    }

    fn serves_previews(&self) -> bool {
        self.previews.is_some()
    }

    /// Previews of complete landed files. As for `get_streamed`, an in-flight
    /// file — or anything but a plain file — is `NoSuchKey`.
    async fn get_previews(
        &self,
        objects: Vec<String>,
        px: u32,
    ) -> Result<mpsc::Receiver<(usize, Result<PreviewImage, StorageError>)>, StorageError> {
        let Some(generator) = self.previews.clone() else {
            return Err(StorageError::NotImplemented("get_previews".into()));
        };
        let mut targets = Vec::with_capacity(objects.len());
        for object in &objects {
            // The etag `get_attrs` reports for the same file (`soi_from`).
            let target = match self.complete_file_path(object).await {
                Some(path) => match path.to_str() {
                    Some(p) => {
                        let etag = fs::symlink_metadata(&path)
                            .await
                            .ok()
                            .map(|md| file_etag(md.modified().ok(), md.len()));
                        Ok((p.to_string(), etag))
                    }
                    None => Err(StorageError::NoPreview(object.clone())),
                },
                None => Err(StorageError::NoSuchKey(object.clone())),
            };
            targets.push(target);
        }
        Ok(spawn_previews(generator, targets, px))
    }
}

#[async_trait]
impl StorageDest for FsDest {
    fn is_sequential(&self) -> bool {
        false
    }

    /// Up-front disk-space gate: required = transfer total minus what this
    /// download already holds on disk, keeping 1% of the volume as reserve.
    /// Mirrors the electron `FSStorageMeta.checkSpace`.
    async fn check_space(&self, required_total: u64) -> Result<(), StorageError> {
        let base = self.base_path.clone();
        let (free, capacity) = tokio::task::spawn_blocking(move || volume_free_capacity(&base))
            .await
            .map_err(|e| StorageError::Other(format!("statvfs join: {e}")))??;
        let have = self.consumed_bytes().await?;
        let required = required_total.saturating_sub(have);
        let reserve = capacity / 100;
        if required > free.saturating_sub(reserve) {
            return Err(StorageError::NoSpace { required, free });
        }
        Ok(())
    }

    async fn put_file(
        &self,
        object: &str,
        data: &[u8],
        attrs: Option<&StorageFileAttrs>,
        conditions: Option<&StoragePutConditions>,
    ) -> Result<StoragePutResult, StorageError> {
        // A download destination, not an object store: no CAS semantics here.
        // The agent's V3 root is served by `FsObjectStore` instead.
        reject_conditions(conditions)?;
        self.meta.forget(object).await?; // clear any stale partial record
        let path = self.meta.resolve_path(&self.base_path, object, WriteKind::File).await?;
        let digest = &Sha256::digest(data);
        let etag = hex_lower(digest);
        fs::write(&path, data).await?;
        apply_attrs(&path, attrs).await?;
        Ok(StoragePutResult {
            etag: Some(etag),
            crc32: None,
            sha256: Some(digest.to_vec()),
            storage_key: object.to_string(),
        })
    }

    async fn put_dir(&self, object: &str, attrs: Option<&StorageFileAttrs>) -> Result<StoragePutResult, StorageError> {
        let path = self.meta.resolve_path(&self.base_path, object, WriteKind::Dir).await?;
        apply_attrs(&path, attrs).await?;
        Ok(StoragePutResult {
            etag: None,
            crc32: None,
            sha256: None,
            storage_key: object.to_string(),
        })
    }

    async fn put_symlink(
        &self,
        object: &str,
        target: &str,
        _attrs: Option<&StorageFileAttrs>,
    ) -> Result<StoragePutResult, StorageError> {
        let path = self
            .meta
            .resolve_path(&self.base_path, object, WriteKind::Symlink)
            .await?;
        #[cfg(unix)]
        {
            // Android's FUSE-backed shared storage denies `symlink()` outright
            // (EACCES, regardless of app permissions) — classify that as the
            // destination not supporting symlinks so the user sees that story
            // instead of a raw "Permission denied (os error 13)".
            if let Err(e) = tokio::fs::symlink(target, &path).await {
                if e.kind() == std::io::ErrorKind::PermissionDenied {
                    return Err(StorageError::SymlinkUnsupported(object.to_string()));
                }
                return Err(e.into());
            }
        }
        #[cfg(windows)]
        {
            if tokio::fs::symlink_file(target, &path).await.is_err() {
                fs::write(&path, target).await?;
            }
        }
        // Matching Electron: no attrs applied to symlinks.
        Ok(StoragePutResult {
            etag: None,
            crc32: None,
            sha256: None,
            storage_key: object.to_string(),
        })
    }

    async fn del_file(&self, object: &str) -> Result<(), StorageError> {
        self.meta.forget(object).await?;
        let path = self.meta.resolve_existing(&self.base_path, object).await;
        if fs::symlink_metadata(&path).await.is_ok() {
            let _ = fs::remove_file(&path).await;
        }
        Ok(())
    }

    async fn del_dir(&self, object: &str) -> Result<(), StorageError> {
        self.meta.forget(object).await?;
        let path = self.meta.resolve_existing(&self.base_path, object).await;
        if fs::symlink_metadata(&path).await.is_ok() {
            let _ = fs::remove_dir(&path).await;
        }
        Ok(())
    }

    async fn update_stats(&self, _info: &StorageStatInfo) -> Result<(), StorageError> {
        Ok(())
    }

    async fn close(&self) -> Result<(), StorageError> {
        self.meta.close().await
    }
}

#[async_trait]
impl StorageDestMultipart for FsDest {
    async fn list_multiparts_all(&self) -> Result<Vec<StorageMultipartUpload>, StorageError> {
        Ok(self
            .meta
            .list_multiparts()
            .await
            .into_iter()
            .map(|k| StorageMultipartUpload {
                plain_key: Some(k.clone()),
                encrypted_key: Some(k.clone()),
                upload_id: Some(k),
                ..Default::default()
            })
            .collect())
    }

    async fn list_parts_all(&self, upload: &StorageMultipartUpload) -> Result<Vec<StoragePartInfo>, StorageError> {
        let key = upload
            .plain_key
            .as_ref()
            .ok_or_else(|| StorageError::Other("no key in upload".into()))?;
        Ok(self
            .meta
            .read(key)
            .await
            .map(|i| Self::to_part_infos(&i.parts))
            .unwrap_or_default())
    }

    async fn begin_multipart(
        &self,
        object: &str,
        _size: u64,
        attrs: Option<&StorageFileAttrs>,
    ) -> Result<StorageMultipartUpload, StorageError> {
        let path = self.meta.resolve_path(&self.base_path, object, WriteKind::File).await?;
        self.meta.begin(object, FsEntryType::File, attrs).await?;
        // Record first, file second: the record is what marks the file
        // incomplete, so it must be durable before anything lands on disk
        // (a crash between the two leaves a record without bytes, never a
        // bare 0-byte file that would list as complete). From here the name
        // is taken on disk before any part lands — `resolve_path` of a
        // sibling that maps to the same spelling sees it. No truncate: a
        // resume keeps its bytes.
        fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .await?;
        Ok(StorageMultipartUpload {
            plain_key: Some(object.to_string()),
            encrypted_key: Some(object.to_string()),
            upload_id: Some(object.to_string()),
            ..Default::default()
        })
    }

    async fn put_part(
        &self,
        part_number: u32,
        upload: &StorageMultipartUpload,
        offset: u64,
        data: &[u8],
        _is_last_part: bool,
    ) -> Result<StoragePartInfo, StorageError> {
        if !(1..=10000).contains(&part_number) {
            return Err(StorageError::Other(format!("bad part number {part_number}")));
        }
        let key = upload
            .plain_key
            .as_ref()
            .ok_or_else(|| StorageError::Other("no key in upload".into()))?;
        let path = self.meta.resolve_path(&self.base_path, key, WriteKind::File).await?;

        {
            // Explicitly no truncate: this writes one part at `offset` into a
            // file other parts of which may already be on disk.
            let mut f = fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(false)
                .open(&path)
                .await?;
            f.seek(std::io::SeekFrom::Start(offset)).await?;
            f.write_all(data).await?;
            // tokio's `write_all` returns once the bytes are handed to the
            // blocking pool; without `flush` the write can still be in flight
            // when the part is recorded landed in `multiparts.json` below.
            f.flush().await?;
        }

        let digest = Sha256::digest(data);
        let etag = hex_lower(&digest);
        tracing::debug!(
            key = %key,
            part = part_number,
            offset,
            len = data.len(),
            all_zero = data.iter().all(|b| *b == 0),
            etag = %&etag[..12],
            "put_part landed"
        );
        self.meta
            .put_part(
                key,
                FsPartInfo {
                    part_number,
                    offset,
                    size: data.len() as u64,
                    etag: Some(etag.clone()),
                },
            )
            .await?;

        Ok(StoragePartInfo {
            part_number: Some(part_number),
            offset: Some(offset),
            size: Some(data.len() as u64),
            etag: Some(etag),
            sha256: Some(digest.to_vec()),
            ..Default::default()
        })
    }

    async fn cancel_multipart(&self, upload: &StorageMultipartUpload) -> Result<(), StorageError> {
        let key = upload
            .plain_key
            .as_ref()
            .ok_or_else(|| StorageError::Other("no key in upload".into()))?;
        let path = self.meta.resolve_existing(&self.base_path, key).await;
        self.meta.cancel(key).await?;
        let _ = fs::remove_file(&path).await;
        Ok(())
    }

    async fn complete_multipart(
        &self,
        upload: &StorageMultipartUpload,
        _parts: &[StoragePartInfo],
    ) -> Result<String, StorageError> {
        let key = upload
            .plain_key
            .as_ref()
            .ok_or_else(|| StorageError::Other("no key in upload".into()))?;
        // Verifies contiguity, removes the record, flushes.
        let info = self.meta.complete(key).await?;
        let path = self.meta.resolve_existing(&self.base_path, key).await;
        let attrs = info.attrs.as_ref().map(|a| a.to_storage());
        apply_attrs(&path, attrs.as_ref()).await?;
        Ok(key.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TMP_SEQ: AtomicU64 = AtomicU64::new(0);

    struct TmpDir(PathBuf);
    impl TmpDir {
        fn new() -> Self {
            let n = TMP_SEQ.fetch_add(1, Ordering::Relaxed);
            let mut p = std::env::temp_dir();
            p.push(format!("fsdest_test_{}_{}", std::process::id(), n));
            std::fs::create_dir_all(&p).unwrap();
            TmpDir(p)
        }
        fn path(&self) -> &Path {
            &self.0
        }
    }
    impl Drop for TmpDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    async fn new_dest(base: &Path, meta: &Path, policy: CollisionPolicy) -> FsDest {
        FsDest::new(base.to_path_buf(), meta.to_path_buf(), policy, NameRule::Posix, true)
            .await
            .unwrap()
    }

    /// A dest under the FAT rule, the Android downloads case.
    async fn new_fat_dest(base: &Path, meta: &Path) -> FsDest {
        FsDest::new(
            base.to_path_buf(),
            meta.to_path_buf(),
            CollisionPolicy::Avoid,
            NameRule::Fat,
            true,
        )
        .await
        .unwrap()
    }

    /// begin + parts 1-3, close, reopen on the same meta_dir, resume parts 4-5,
    /// complete — file bytes match and the record is gone.
    #[tokio::test]
    async fn resume_across_restart() {
        let tmp = TmpDir::new();
        let base = tmp.path().join("dl");
        let meta_dir = tmp.path().join("meta");
        let key = "big.bin";
        let mk = |i: u8| vec![i; 4];

        {
            let dest = new_dest(&base, &meta_dir, CollisionPolicy::Avoid).await;
            let up = dest.begin_multipart(key, 0, None).await.unwrap();
            for i in 1u8..=3 {
                dest.put_part(i as u32, &up, (i as u64 - 1) * 4, &mk(i), false)
                    .await
                    .unwrap();
            }
            dest.close().await.unwrap(); // graceful flush
        }

        let dest = new_dest(&base, &meta_dir, CollisionPolicy::Avoid).await;
        let ups = dest.list_multiparts_all().await.unwrap();
        assert_eq!(ups.len(), 1);
        let up = ups[0].clone();
        assert_eq!(dest.list_parts_all(&up).await.unwrap().len(), 3, "parts 1-3 persisted");

        let soi = dest.get_attrs(key).await.expect("incomplete soi present");
        assert!(!soi.is_complete, "resumed file is incomplete");
        assert_eq!(soi.parts.as_ref().map(|p| p.len()), Some(3));

        for i in 4u8..=5 {
            dest.put_part(i as u32, &up, (i as u64 - 1) * 4, &mk(i), i == 5)
                .await
                .unwrap();
        }
        dest.complete_multipart(&up, &[]).await.unwrap();

        let written = std::fs::read(base.join(key)).unwrap();
        let expected: Vec<u8> = (1u8..=5).flat_map(|i| vec![i; 4]).collect();
        assert_eq!(written, expected, "all 5 parts concatenated");
        assert!(
            dest.list_multiparts_all().await.unwrap().is_empty(),
            "record removed on complete"
        );
    }

    async fn drain(mut rx: mpsc::Receiver<Result<Vec<StorageObjectInfo>, StorageError>>) -> Vec<StorageObjectInfo> {
        let mut out = Vec::new();
        while let Some(b) = rx.recv().await {
            out.extend(b.unwrap());
        }
        out
    }

    async fn drain_bytes(mut rx: mpsc::Receiver<Result<Vec<u8>, StorageError>>) -> Result<Vec<u8>, StorageError> {
        let mut out = Vec::new();
        while let Some(c) = rx.recv().await {
            out.extend(c?);
        }
        Ok(out)
    }

    /// The dest as a source: a landed tree lists through the path map (real,
    /// collision-renamed names never leak as keys), a file streams by range,
    /// a dir root descends, an in-flight file lists incomplete and refuses to
    /// stream.
    #[tokio::test]
    async fn source_over_landed_and_inflight() {
        let tmp = TmpDir::new();
        let base = tmp.path().join("dl");
        let meta_dir = tmp.path().join("meta");
        std::fs::create_dir_all(&base).unwrap();
        // A foreign `docs` forces the collision rename of the landed one.
        std::fs::create_dir_all(base.join("docs")).unwrap();
        let dest = new_dest(&base, &meta_dir, CollisionPolicy::Avoid).await;
        dest.put_dir("docs", None).await.unwrap();
        dest.put_file("docs/a.txt", b"hello world", None, None)
            .await
            .unwrap();
        dest.put_file("top.bin", &[7u8; 10], None, None).await.unwrap();
        let up = dest.begin_multipart("big.bin", 8, None).await.unwrap();
        dest.put_part(1, &up, 0, &[1u8; 4], false).await.unwrap();

        let root = drain(dest.get_dir("", false, None, None).await).await;
        let keys: Vec<&str> = root.iter().map(|s| s.key.as_str()).collect();
        assert_eq!(keys, ["big.bin", "docs", "top.bin"], "path-map keys, key order");
        let big = root.iter().find(|s| s.key == "big.bin").unwrap();
        assert!(!big.is_complete);
        assert_eq!(big.parts.as_ref().map(|p| p.len()), Some(1));
        assert!(root.iter().find(|s| s.key == "docs").unwrap().file_type == StorageFileType::Dir);

        let under = drain(dest.get_dir("docs", true, None, None).await).await;
        assert_eq!(under.iter().map(|s| s.key.as_str()).collect::<Vec<_>>(), ["docs/a.txt"]);
        let all = drain(dest.get_dir("", true, None, Some(2)).await).await;
        assert_eq!(all.len(), 4, "roots + the nested file, batched by 2");

        let bytes = drain_bytes(dest.get_streamed("docs/a.txt", Some(6), Some(5)).await)
            .await
            .unwrap();
        assert_eq!(bytes, b"world");
        assert!(matches!(
            drain_bytes(dest.get_streamed("big.bin", None, None).await).await,
            Err(StorageError::NoSuchKey(_))
        ), "an in-flight file never streams");
        assert!(matches!(
            drain_bytes(dest.get_streamed("docs", None, None).await).await,
            Err(StorageError::NoSuchKey(_))
        ), "a dir never streams");

        let q = StorageSearchQuery {
            name_match: Some("A.TXT".into()),
        };
        let found = dest.search(&q, &SearchOptions::default()).await.unwrap();
        assert_eq!(found.items.iter().map(|s| s.key.as_str()).collect::<Vec<_>>(), ["docs/a.txt"]);
        assert!(dest.get_attrs("").await.unwrap().file_type == StorageFileType::Dir);
    }

    /// A whole-landed download's dest from the record's roots + renames: no
    /// meta dir, lists and streams the same tree, and any write is an error.
    #[tokio::test]
    async fn landed_dest_reads_from_roots_and_renames_only() {
        let tmp = TmpDir::new();
        let base = tmp.path().join("dl");
        let meta_dir = tmp.path().join("meta");
        let (roots, renames) = {
            let dest = new_dest(&base, &meta_dir, CollisionPolicy::Avoid).await;
            dest.put_dir("docs", None).await.unwrap();
            dest.put_file("docs/a.txt", b"hello", None, None).await.unwrap();
            (dest.roots().await, dest.renames().await)
        };
        std::fs::remove_dir_all(&meta_dir).unwrap();

        let dest = FsDest::landed(base.clone(), roots, renames, NameRule::Posix);
        assert!(!meta_dir.exists(), "a landed dest never recreates the meta dir");
        let all = drain(dest.get_dir("", true, None, None).await).await;
        assert_eq!(all.iter().map(|s| s.key.as_str()).collect::<Vec<_>>(), ["docs", "docs/a.txt"]);
        assert!(all.iter().all(|s| s.is_complete));
        assert_eq!(
            drain_bytes(dest.get_streamed("docs/a.txt", None, None).await).await.unwrap(),
            b"hello"
        );
        assert_eq!(dest.local_path("docs/a.txt").await, Some(base.join("docs/a.txt")));
        assert_eq!(dest.bytes_done("docs").await, 5);

        assert!(dest.put_file("new.txt", b"x", None, None).await.is_err());
        assert!(dest.begin_multipart("docs/b.bin", 4, None).await.is_err());
        assert!(!base.join("new.txt").exists(), "a refused write leaves nothing behind");
        assert!(!meta_dir.exists());
    }

    /// A landed dest previews its complete files through the injected
    /// generator; like `get_streamed`, anything else is `NoSuchKey`.
    #[tokio::test]
    async fn landed_dest_previews_complete_files() {
        struct EchoGenerator;
        #[async_trait]
        impl PreviewGenerator for EchoGenerator {
            fn max_px(&self) -> u32 {
                256
            }
            async fn generate(&self, file: &str, px: u32) -> Result<Option<PreviewImage>, String> {
                Ok(Some(PreviewImage {
                    bytes: format!("{file}@{px}").into_bytes(),
                    mime_type: "image/jpeg".into(),
                    etag: None,
                }))
            }
        }

        let tmp = TmpDir::new();
        let base = tmp.path().join("dl");
        let meta_dir = tmp.path().join("meta");
        let (roots, renames) = {
            let dest = new_dest(&base, &meta_dir, CollisionPolicy::Avoid).await;
            dest.put_dir("docs", None).await.unwrap();
            dest.put_file("docs/a.txt", b"hello", None, None).await.unwrap();
            (dest.roots().await, dest.renames().await)
        };

        let bare = FsDest::landed(base.clone(), roots.clone(), renames.clone(), NameRule::Posix);
        assert!(!bare.serves_previews());
        assert!(matches!(
            bare.get_previews(vec!["docs/a.txt".into()], 64).await,
            Err(StorageError::NotImplemented(_))
        ));

        let dest = FsDest::landed(base.clone(), roots, renames, NameRule::Posix)
            .with_preview_generator(Some(Arc::new(EchoGenerator)));
        assert!(dest.serves_previews());
        let objects = vec!["docs/a.txt".to_string(), "docs".to_string(), "nope.txt".to_string()];
        let mut rx = dest.get_previews(objects, 64).await.unwrap();
        let mut got: Vec<Option<Result<PreviewImage, StorageError>>> = vec![None, None, None];
        while let Some((index, result)) = rx.recv().await {
            got[index] = Some(result);
        }
        let image = got[0].take().unwrap().expect("a landed file previews");
        assert_eq!(image.bytes, format!("{}@64", base.join("docs/a.txt").display()).into_bytes());
        assert!(matches!(got[1], Some(Err(StorageError::NoSuchKey(_)))), "a dir is not a plain file");
        assert!(matches!(got[2], Some(Err(StorageError::NoSuchKey(_)))));
    }

    /// A renamed nested in-flight file (`Fat` rule, `:` → `_`): the listing
    /// yields the virtual key with `is_complete = false` and its parts,
    /// search finds it by the virtual name only, `bytes_done` counts its
    /// recorded parts, and once complete it streams and locates by the
    /// virtual key.
    #[tokio::test]
    async fn renamed_nested_inflight_file_lists_by_virtual_key() {
        let tmp = TmpDir::new();
        let base = tmp.path().join("dl");
        let meta_dir = tmp.path().join("meta");
        let key = "Telegram/IMAGE 02:51:27.jpg";
        let dest = new_fat_dest(&base, &meta_dir).await;
        dest.put_dir("Telegram", None).await.unwrap();
        let up = dest.begin_multipart(key, 8, None).await.unwrap();
        dest.put_part(1, &up, 0, &[1u8; 4], false).await.unwrap();
        assert!(base.join("Telegram/IMAGE 02_51_27.jpg").is_file(), "landed under the rewritten name");

        let all = drain(dest.get_dir("", true, None, None).await).await;
        assert_eq!(all.iter().map(|s| s.key.as_str()).collect::<Vec<_>>(), ["Telegram", key]);
        let file = all.iter().find(|s| s.key == key).unwrap();
        assert!(!file.is_complete);
        assert_eq!(file.parts.as_ref().map(|p| p.len()), Some(1));
        let under = drain(dest.get_dir("Telegram", false, None, None).await).await;
        assert_eq!(under.iter().map(|s| s.key.as_str()).collect::<Vec<_>>(), [key]);
        assert!(!under[0].is_complete);

        let by_virtual = StorageSearchQuery {
            name_match: Some("02:51".into()),
        };
        let found = dest.search(&by_virtual, &SearchOptions::default()).await.unwrap();
        assert_eq!(found.items.iter().map(|s| s.key.as_str()).collect::<Vec<_>>(), [key]);
        assert!(!found.items[0].is_complete);
        let by_disk = StorageSearchQuery {
            name_match: Some("02_51".into()),
        };
        let found = dest.search(&by_disk, &SearchOptions::default()).await.unwrap();
        assert!(found.items.is_empty(), "the disk spelling is not a name");

        assert_eq!(dest.bytes_done(key).await, 4, "recorded parts, not the disk length");
        assert_eq!(dest.bytes_done("Telegram").await, 4);
        assert!(!dest.get_attrs(key).await.unwrap().is_complete);
        assert!(matches!(
            drain_bytes(dest.get_streamed(key, None, None).await).await,
            Err(StorageError::NoSuchKey(_))
        ));

        dest.put_part(2, &up, 4, &[2u8; 4], true).await.unwrap();
        dest.complete_multipart(&up, &[]).await.unwrap();
        let bytes = drain_bytes(dest.get_streamed(key, None, None).await).await.unwrap();
        assert_eq!(bytes, [[1u8; 4], [2u8; 4]].concat());
        assert_eq!(dest.local_path(key).await, Some(base.join("Telegram/IMAGE 02_51_27.jpg")));
        assert_eq!(dest.bytes_done("Telegram").await, 8);
        assert!(dest.get_attrs(key).await.unwrap().is_complete);
    }

    /// A whole-landed download with renamed entries at both levels, rebuilt
    /// from the record's `roots` + `renames`: lists virtual keys, streams
    /// and locates the renamed nested file, and counts its bytes.
    #[tokio::test]
    async fn landed_dest_serves_renamed_entries_from_roots_and_renames() {
        let tmp = TmpDir::new();
        let base = tmp.path().join("dl");
        let meta_dir = tmp.path().join("meta");
        let (roots, renames) = {
            let dest = new_fat_dest(&base, &meta_dir).await;
            dest.put_dir("d", None).await.unwrap();
            dest.put_file("d/a:b.jpg", b"hello", None, None).await.unwrap();
            dest.put_file("top:x.txt", b"top", None, None).await.unwrap();
            (dest.roots().await, dest.renames().await)
        };
        std::fs::remove_dir_all(&meta_dir).unwrap();
        assert_eq!(roots, vec!["d".to_string(), "top:x.txt".to_string()]);
        assert_eq!(
            renames,
            HashMap::from([
                ("d/a:b.jpg".to_string(), "a_b.jpg".to_string()),
                ("top:x.txt".to_string(), "top_x.txt".to_string()),
            ])
        );

        let dest = FsDest::landed(base.clone(), roots, renames, NameRule::Fat);
        let all = drain(dest.get_dir("", true, None, None).await).await;
        assert_eq!(
            all.iter().map(|s| s.key.as_str()).collect::<Vec<_>>(),
            ["d", "d/a:b.jpg", "top:x.txt"]
        );
        assert!(all.iter().all(|s| s.is_complete));
        assert_eq!(
            drain_bytes(dest.get_streamed("d/a:b.jpg", None, None).await).await.unwrap(),
            b"hello"
        );
        assert_eq!(dest.local_path("d/a:b.jpg").await, Some(base.join("d/a_b.jpg")));
        assert_eq!(dest.local_path("top:x.txt").await, Some(base.join("top_x.txt")));
        assert_eq!(dest.local_path("top_x.txt").await, None, "the disk spelling is not a key");
        assert_eq!(dest.bytes_done("d").await, 5);
        assert_eq!(dest.landed_bytes().await, 8);
        assert!(dest.consumed_bytes().await.unwrap() >= 8);
        let q = StorageSearchQuery {
            name_match: Some("top:x".into()),
        };
        let found = dest.search(&q, &SearchOptions::default()).await.unwrap();
        assert_eq!(found.items.iter().map(|s| s.key.as_str()).collect::<Vec<_>>(), ["top:x.txt"]);
    }

    /// `landed_bytes` / `consumed_bytes` over `roots` with a collision-renamed
    /// root: the download's own copy counts, the foreign original does not.
    #[tokio::test]
    async fn byte_counts_follow_a_renamed_root() {
        let tmp = TmpDir::new();
        let base = tmp.path().join("dl");
        std::fs::create_dir_all(&base).unwrap();
        // The foreign original is far bigger than the download's own copy,
        // so a consumed count that included it would show.
        std::fs::write(base.join("Foo.txt"), vec![0u8; 4 * 1024 * 1024]).unwrap();
        let dest = new_dest(&base, &tmp.path().join("meta"), CollisionPolicy::Avoid).await;
        dest.put_file("Foo.txt", &[7u8; 10], None, None).await.unwrap();
        assert_eq!(dest.roots().await, vec!["Foo.txt".to_string()]);
        assert_eq!(
            dest.renames().await,
            HashMap::from([("Foo.txt".to_string(), "Foo 1.txt".to_string())])
        );
        assert_eq!(dest.landed_bytes().await, 10);
        let consumed = dest.consumed_bytes().await.unwrap();
        assert!((10..1024 * 1024).contains(&consumed), "own copy only: {consumed}");
        assert_eq!(dest.local_path("Foo.txt").await, Some(base.join("Foo 1.txt")));
    }

    /// A part whose on-disk bytes were corrupted after its record was flushed:
    /// `verify_parts_on_load=true` drops it on reopen (→ re-download); `false`
    /// trusts the record (Electron-style).
    #[tokio::test]
    async fn verify_drops_corrupted_part_on_load() {
        let tmp = TmpDir::new();
        let base = tmp.path().join("dl");
        let meta_dir = tmp.path().join("meta");
        let key = "big.bin";

        {
            let dest = new_dest(&base, &meta_dir, CollisionPolicy::Avoid).await;
            let up = dest.begin_multipart(key, 0, None).await.unwrap();
            for i in 1u8..=3 {
                dest.put_part(i as u32, &up, (i as u64 - 1) * 4, &[i; 4], false)
                    .await
                    .unwrap();
            }
            dest.close().await.unwrap();
        }

        // Corrupt part 2's bytes on disk (offset 4..8) so its sha256 won't match.
        {
            use std::io::{Seek, SeekFrom, Write};
            let mut f = std::fs::OpenOptions::new().write(true).open(base.join(key)).unwrap();
            f.seek(SeekFrom::Start(4)).unwrap();
            f.write_all(&[9u8; 4]).unwrap();
        }

        let nums = |parts: &[StoragePartInfo]| {
            let mut n: Vec<u32> = parts.iter().filter_map(|p| p.part_number).collect();
            n.sort();
            n
        };

        // verify OFF → torn part is trusted, still listed.
        {
            let dest = FsDest::new(base.clone(), meta_dir.clone(), CollisionPolicy::Avoid, NameRule::Posix, false)
                .await
                .unwrap();
            let up = dest.list_multiparts_all().await.unwrap()[0].clone();
            assert_eq!(
                nums(&dest.list_parts_all(&up).await.unwrap()),
                vec![1, 2, 3],
                "verify off keeps the torn part"
            );
        }

        // verify ON → torn part 2 dropped (re-downloaded), 1 and 3 kept.
        {
            let dest = FsDest::new(base.clone(), meta_dir.clone(), CollisionPolicy::Avoid, NameRule::Posix, true)
                .await
                .unwrap();
            let up = dest.list_multiparts_all().await.unwrap()[0].clone();
            assert_eq!(
                nums(&dest.list_parts_all(&up).await.unwrap()),
                vec![1, 3],
                "verify on drops the corrupted part"
            );
        }
    }

    /// Parallel verify across multiple objects: only the corrupted part of one
    /// object is dropped; the other object is untouched.
    #[tokio::test]
    async fn verify_parallel_multi_object() {
        let tmp = TmpDir::new();
        let base = tmp.path().join("dl");
        let meta_dir = tmp.path().join("meta");

        {
            let dest = new_dest(&base, &meta_dir, CollisionPolicy::Avoid).await;
            for key in ["a.bin", "b.bin"] {
                let up = dest.begin_multipart(key, 0, None).await.unwrap();
                for i in 1u8..=3 {
                    dest.put_part(i as u32, &up, (i as u64 - 1) * 4, &[i; 4], false)
                        .await
                        .unwrap();
                }
            }
            dest.close().await.unwrap();
        }
        // Corrupt only a.bin's part 2.
        {
            use std::io::{Seek, SeekFrom, Write};
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .open(base.join("a.bin"))
                .unwrap();
            f.seek(SeekFrom::Start(4)).unwrap();
            f.write_all(&[0u8; 4]).unwrap();
        }

        let dest = new_dest(&base, &meta_dir, CollisionPolicy::Avoid).await; // verify on
        let nums = |parts: &[StoragePartInfo]| {
            let mut n: Vec<u32> = parts.iter().filter_map(|p| p.part_number).collect();
            n.sort();
            n
        };
        for up in dest.list_multiparts_all().await.unwrap() {
            let key = up.plain_key.clone().unwrap();
            let got = nums(&dest.list_parts_all(&up).await.unwrap());
            if key == "a.bin" {
                assert_eq!(got, vec![1, 3], "a.bin drops its corrupted part 2");
            } else {
                assert_eq!(got, vec![1, 2, 3], "b.bin untouched");
            }
        }
    }

    #[tokio::test]
    async fn collision_top_level_avoid_keeps_existing() {
        let tmp = TmpDir::new();
        let base = tmp.path().join("dl");
        std::fs::create_dir_all(&base).unwrap();
        std::fs::write(base.join("Foo.txt"), b"old").unwrap();

        let dest = new_dest(&base, &tmp.path().join("meta"), CollisionPolicy::Avoid).await;
        dest.put_file("Foo.txt", b"new", None, None).await.unwrap();

        assert_eq!(
            std::fs::read(base.join("Foo.txt")).unwrap(),
            b"old",
            "original untouched"
        );
        assert_eq!(
            std::fs::read(base.join("Foo 1.txt")).unwrap(),
            b"new",
            "renamed copy written"
        );
    }

    #[tokio::test]
    async fn collision_top_level_strict_errors() {
        let tmp = TmpDir::new();
        let base = tmp.path().join("dl");
        std::fs::create_dir_all(&base).unwrap();
        std::fs::write(base.join("Foo.txt"), b"old").unwrap();

        let dest = new_dest(&base, &tmp.path().join("meta"), CollisionPolicy::Strict).await;
        assert!(dest.put_file("Foo.txt", b"new", None, None).await.is_err());
        assert!(dest.begin_multipart("Foo.txt", 0, None).await.is_err());
        assert_eq!(
            std::fs::read(base.join("Foo.txt")).unwrap(),
            b"old",
            "untouched on strict error"
        );
    }

    #[tokio::test]
    async fn duplicate_begin_errors() {
        let tmp = TmpDir::new();
        let dest = new_dest(&tmp.path().join("dl"), &tmp.path().join("meta"), CollisionPolicy::Avoid).await;
        dest.begin_multipart("x.bin", 0, None).await.unwrap();
        assert!(
            dest.begin_multipart("x.bin", 0, None).await.is_err(),
            "second begin must error"
        );
    }

    #[tokio::test]
    async fn contiguity_guard_on_complete() {
        let tmp = TmpDir::new();
        let dest = new_dest(&tmp.path().join("dl"), &tmp.path().join("meta"), CollisionPolicy::Avoid).await;
        let up = dest.begin_multipart("y.bin", 0, None).await.unwrap();
        dest.put_part(1, &up, 0, b"aaaa", false).await.unwrap();
        dest.put_part(3, &up, 8, b"cccc", false).await.unwrap(); // gap at #2
        assert!(
            dest.complete_multipart(&up, &[]).await.is_err(),
            "missing middle part fails"
        );
    }

    /// check_space: an absurd requirement fails with NoSpace, a tiny one passes,
    /// and bytes already on disk (resume) count against the requirement.
    #[tokio::test]
    async fn check_space_gates_on_free_volume_space() {
        let base = TmpDir::new();
        let meta = TmpDir::new();
        let dest = new_dest(base.path(), meta.path(), CollisionPolicy::Avoid).await;

        dest.check_space(1024).await.expect("1 KiB must fit");
        let err = dest
            .check_space(u64::MAX / 4)
            .await
            .expect_err("absurd requirement must fail");
        assert!(matches!(err, StorageError::NoSpace { .. }), "got: {err}");

        // Resume credit: bytes already written under the path map offset the
        // requirement. Pick a requirement that only fits WITH the credit:
        // free − reserve + consumed/2 — over the line by consumed/2 without
        // the credit, under it by consumed/2 with it (tolerates ~2 MB of
        // free-space drift between the two statvfs calls).
        dest.put_file("have.bin", &vec![0u8; 4 * 1024 * 1024], None, None)
            .await
            .unwrap();
        let (free, capacity) = volume_free_capacity(base.path()).unwrap();
        let consumed = dest.consumed_bytes().await.unwrap();
        assert!(consumed >= 4 * 1024 * 1024, "written file must count: {consumed}");
        let reserve = capacity / 100;
        dest.check_space(free.saturating_sub(reserve) + consumed / 2)
            .await
            .expect("already-held bytes must offset the requirement");
    }
}
