//! Local filesystem storage source for the in-process transfer engine.
//!
//! `FsSource` implements the `share-ninja-core` storage source traits.

use std::collections::{HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::fs;
use tokio::sync::mpsc;

use crate::transfers::{
    PreviewGenerator, PreviewImage, SearchOptions, SearchResult, StorageDirStats, StorageSearchQuery,
    StorageSourceType, StorageStatInfo,
};
use crate::{Storage, StorageError, StorageFileType, StorageMetaState, StorageObjectInfo, StorageSource};

#[cfg(target_os = "android")]
use super::search::basename_contains;
use super::preview::spawn_previews;
use super::search::{BatchReceiver, BatchSink, SEARCH_BATCH, SearchFilter, SearchPager, push_filtered};
#[cfg(target_os = "android")]
use super::stream::pump_file;
use super::stream::stream_file;
use super::walk::{
    EntryKind, IncompleteParts, WalkRenames, file_etag, meta_parts, soi_from, spawn_listing, system_time_to_chrono,
    walk_into_sink, walk_tree,
};

/// Reject `.`/`..` path components (Rust port of Electron `FSStorage.checkPath`),
/// so a peer-supplied key can't escape a shared root via traversal.
fn key_has_traversal(key: &str) -> bool {
    key.split('/').any(|c| c == "." || c == "..")
}

/// How often the walk publishes its running totals. A recursive stat of a big
/// tree runs far longer than the P2P per-message receive timeout, and that
/// timeout restarts on every message — emitting this often keeps it from ever
/// firing, with 60x headroom against the 30s deadline.
const STATS_PROGRESS_INTERVAL: std::time::Duration = std::time::Duration::from_millis(500);
/// Entries walked between clock reads. `Instant::now()` is cheap but not free,
/// and a million-entry walk would pay it a million times.
const STATS_PROGRESS_CHECK_EVERY: u32 = 4096;
/// Stat each root (the `get_dir_stats` body): a directory has its contents
/// walked, a file/symlink is counted directly. `strict` propagates an lstat
/// error on a resolved single path; for the virtual-root set (`path==""`) an
/// unreadable root is skipped. Synchronous — runs on one blocking thread.
///
/// `progress` is called with the running totals at most every
/// `STATS_PROGRESS_INTERVAL`; returning `Err` from it (receiver dropped)
/// aborts the walk.
pub(super) fn stat_roots(
    roots: &[PathBuf],
    strict: bool,
    progress: &mut dyn FnMut(&StorageDirStats) -> Result<(), StorageError>,
) -> Result<StorageDirStats, StorageError> {
    let mut stats = StorageDirStats::default();
    let mut since_check: u32 = 0;
    let mut last_emit = std::time::Instant::now();
    for root in roots {
        let md = match std::fs::symlink_metadata(root) {
            Ok(m) => m,
            Err(e) => {
                if strict {
                    return Err(e.into());
                }
                continue;
            }
        };
        let (root_kind, root_size, _, _) = meta_parts(root, &md);
        match root_kind {
            EntryKind::Dir => {
                walk_tree(root, "", true, false, false, &WalkRenames::new(), &mut |_name, _pfx, kind, size, _mtime, _mode| {
                    match kind {
                        EntryKind::Dir => stats.number_of_folders += 1,
                        EntryKind::Symlink => {
                            stats.number_of_symlinks += 1;
                            stats.total_size += size;
                        }
                        EntryKind::File => {
                            stats.number_of_files += 1;
                            stats.total_size += size;
                        }
                    }
                    since_check += 1;
                    if since_check >= STATS_PROGRESS_CHECK_EVERY {
                        since_check = 0;
                        if last_emit.elapsed() >= STATS_PROGRESS_INTERVAL {
                            last_emit = std::time::Instant::now();
                            progress(&stats)?;
                        }
                    }
                    Ok(())
                })?;
            }
            EntryKind::Symlink => {
                stats.number_of_symlinks += 1;
                stats.total_size += root_size;
            }
            EntryKind::File => {
                stats.number_of_files += 1;
                stats.total_size += root_size;
            }
        }
    }
    Ok(stats)
}

/// Resolve `path` against the configured roots and stream it into `sink` — the
/// shared body of `meta()`/`scan_paths` and `get_dir`. `""` lists the virtual
/// root across all entries. lstat throughout: symlinks are not resolved.
/// Synchronous (runs on the blocking thread); the walk goes through `walk_tree`.
fn produce_dir(
    entries: &[PathBuf],
    root_keys: &[Option<String>],
    path: &str,
    recursive: bool,
    sink: &mut BatchSink,
    mut filter: Option<SearchFilter>,
) -> Result<(), StorageError> {
    if path.is_empty() {
        for (entry, root_key) in entries.iter().zip(root_keys) {
            let md = match std::fs::symlink_metadata(entry) {
                Ok(m) => m,
                Err(_) => continue, // skip an unreadable root
            };
            let (kind, size, mtime, mode) = meta_parts(entry, &md);
            let name = root_key.clone().unwrap_or_else(|| {
                if kind == EntryKind::Dir {
                    "dir".into()
                } else {
                    "file".into()
                }
            });
            push_filtered(sink, &mut filter, &name, || soi_from(&name, kind, size, mtime, mode))?;
            if kind == EntryKind::Dir && recursive {
                walk_into_sink(entry, &name, recursive, sink, &mut filter, &IncompleteParts::new(), &WalkRenames::new())?;
            }
        }
        return Ok(());
    }

    let fspath = match find_file_path_in_keyed(entries, root_keys, path) {
        Some(p) => p,
        None => return Ok(()),
    };
    let md = std::fs::symlink_metadata(&fspath)?;
    if !md.is_dir() {
        return Ok(()); // can't descend into a file
    }
    walk_into_sink(&fspath, path, recursive, sink, &mut filter, &IncompleteParts::new(), &WalkRenames::new())
}

/// The directory walk as a batch stream — the single producer behind
/// `get_dir` (returns it), `meta()`/`scan_paths` (drain it) and `search`
/// (drains it filtered). Dropping the receiver cancels the walk.
fn stream_entries(
    entries: Vec<PathBuf>,
    root_keys: Vec<Option<String>>,
    path: String,
    recursive: bool,
    max_batch_size: Option<usize>,
    filter: Option<SearchFilter>,
) -> mpsc::Receiver<Result<Vec<StorageObjectInfo>, StorageError>> {
    spawn_listing(max_batch_size, move |sink| {
        produce_dir(&entries, &root_keys, &path, recursive, sink, filter)
    })
}

/// Claim `name` in `taken`, disambiguating a collision with the repo's
/// collision-name convention (`"<stem> <n><ext>"`, 1-based — `"Foo 1.txt"`,
/// same as `meta_store::generate_new_name`). Any parent prefix in `name`
/// (Android `content://` display names may carry one) is preserved.
fn unique_key(name: &str, taken: &mut HashSet<String>) -> String {
    if taken.insert(name.to_string()) {
        return name.to_string();
    }
    let p = Path::new(name);
    let parent = p
        .parent()
        .map(|d| d.to_string_lossy().into_owned())
        .filter(|d| !d.is_empty());
    let stem = p
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| name.to_string());
    let ext = p
        .extension()
        .map(|e| format!(".{}", e.to_string_lossy()))
        .unwrap_or_default();
    let mut num = 1u32;
    loop {
        let base = format!("{stem} {num}{ext}");
        let candidate = match &parent {
            Some(d) => format!("{d}/{base}"),
            None => base,
        };
        if taken.insert(candidate.clone()) {
            return candidate;
        }
        num += 1;
    }
}

/// Assign each root its virtual key: the basename, made unique across the
/// ordered root list (`unique_key`) so two picked roots sharing a basename —
/// e.g. two iOS bookmarks to different same-named files — don't collide on one
/// key. Deterministic for a given root order, so the listing (`produce_dir`)
/// and resolution (`find_file_path_in_keyed`) always agree. `None` for a root
/// without a basename (keeps `produce_dir`'s kind-based fallback name).
pub(crate) fn unique_root_keys(entries: &[PathBuf]) -> Vec<Option<String>> {
    let mut taken: HashSet<String> = HashSet::new();
    entries
        .iter()
        .map(|e| e.file_name().map(|n| unique_key(&n.to_string_lossy(), &mut taken)))
        .collect()
}

/// Resolve a virtual `key` to an on-disk path against the configured roots.
/// Free fn so the spawned `get_dir` producer can use it without borrowing
/// `&self` (the `FsSource::find_file_path` method wraps it).
/// `root_keys` is index-aligned with `entries` (the
/// `unique_root_keys` assignment, or `FsSource`'s stored copy — which on
/// Android also excludes names claimed by the `content://` overlay). A file
/// root matches its key exactly; a dir root matches its key as the first path
/// segment and joins the remainder.
pub(crate) fn find_file_path_in_keyed(entries: &[PathBuf], root_keys: &[Option<String>], key: &str) -> Option<PathBuf> {
    if key_has_traversal(key) {
        return None;
    }
    for (entry, root_key) in entries.iter().zip(root_keys) {
        let Some(root_key) = root_key else {
            continue;
        };
        if entry.is_file() {
            if root_key == key {
                return Some(entry.clone());
            }
        } else if entry.is_dir() {
            if key == root_key {
                return Some(entry.clone());
            }
            if let Some(rel) = key.strip_prefix(root_key.as_str()).and_then(|r| r.strip_prefix('/'))
                && !rel.is_empty()
            {
                let full = entry.join(rel);
                if full.exists() {
                    return Some(full);
                }
            }
        }
    }
    None
}

/// Resolves a security-scoped bookmark (iOS/macOS sandbox) and holds the access
/// open until dropped. iOS picker URLs / macOS app-scoped bookmarks aren't real
/// paths we can `open()` until the scope is started; this brackets that.
#[cfg(any(target_os = "macos", target_os = "ios"))]
mod scoped {
    use core::ffi::c_void;
    use objc2::rc::Retained;
    use objc2::runtime::Bool;
    use objc2_foundation::{NSData, NSURL, NSURLBookmarkResolutionOptions};

    /// A started security-scoped `NSURL`; stops access on drop. `NSURL` isn't
    /// `Send`/`Sync` in objc2, but security-scoped access is process-global and may
    /// be released from any thread, so the guard is safe to move across threads
    /// (it must be, since `FsSource` is used from tokio worker threads).
    pub struct ScopedAccess(Retained<NSURL>);
    unsafe impl Send for ScopedAccess {}
    unsafe impl Sync for ScopedAccess {}

    impl ScopedAccess {
        /// Resolve `bookmark` to a URL and start accessing it. `None` if the
        /// bookmark can't be resolved (file moved/deleted) or access is denied.
        pub fn acquire(bookmark: &[u8]) -> Option<Self> {
            // macOS bookmarks are created `.withSecurityScope`; iOS document-URL
            // bookmarks are implicitly scoped and must resolve *without* it.
            #[cfg(target_os = "macos")]
            let opts = NSURLBookmarkResolutionOptions::WithSecurityScope;
            #[cfg(target_os = "ios")]
            let opts = NSURLBookmarkResolutionOptions(0);
            unsafe {
                let data = NSData::dataWithBytes_length(bookmark.as_ptr() as *const c_void, bookmark.len());
                let mut stale = Bool::new(false);
                let url = NSURL::URLByResolvingBookmarkData_options_relativeToURL_bookmarkDataIsStale_error(
                    &data, opts, None, &mut stale,
                )
                .ok()?;
                if stale.as_bool() {
                    tracing::warn!("FsSource: security-scoped bookmark is stale");
                }
                if url.startAccessingSecurityScopedResource() {
                    Some(ScopedAccess(url))
                } else {
                    None
                }
            }
        }
    }

    impl Drop for ScopedAccess {
        fn drop(&mut self) {
            unsafe { self.0.stopAccessingSecurityScopedResource() }
        }
    }
}

/// Filesystem source: the set of paths to upload (each is a real on-disk path).
pub struct FsSource {
    entries: Vec<PathBuf>,
    /// Virtual key per root, index-aligned with `entries` (`unique_root_keys`;
    /// on Android reassigned by `with_bookmarks` so the `content://` overlay
    /// names share the namespace). Same-named roots get distinct keys.
    root_keys: Vec<Option<String>>,
    data_source_id: Option<String>,
    /// Started security-scoped resources (iOS/macOS sandbox), held for the source's
    /// lifetime so reads under sandboxed roots are permitted; each stops access on
    /// drop. Unused on other platforms.
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    _scopes: Vec<scoped::ScopedAccess>,
    /// Android `content://` file entries (picked via the document picker), read
    /// over JNI without copying. `(display_name, content_uri)`; served as regular
    /// files and kept out of `entries` (no fs path to walk).
    #[cfg(target_os = "android")]
    content: Vec<ContentEntry>,
    /// The parked paged-search walks (see `SearchPager`).
    search_pager: SearchPager,
    /// The shell's thumbnailer; `None` = this source serves no previews.
    previews: Option<Arc<dyn PreviewGenerator>>,
}

#[cfg(target_os = "android")]
struct ContentEntry {
    name: String,
    uri: String,
}

impl FsSource {
    pub fn new(paths: Vec<PathBuf>) -> Self {
        Self {
            root_keys: unique_root_keys(&paths),
            entries: paths,
            data_source_id: None,
            #[cfg(any(target_os = "macos", target_os = "ios"))]
            _scopes: Vec::new(),
            #[cfg(target_os = "android")]
            content: Vec::new(),
            search_pager: SearchPager::new(),
            previews: None,
        }
    }

    /// Serve `get_previews` through the shell's thumbnailer (`None` = don't).
    pub fn with_preview_generator(mut self, previews: Option<Arc<dyn PreviewGenerator>>) -> Self {
        self.previews = previews;
        self
    }

    /// Attach per-path access handles, index-aligned with the paths passed to `new`:
    /// - **iOS/macOS**: security-scoped bookmark bytes — resolve + start the scope,
    ///   held until this source drops (read the path normally within it).
    /// - **Android**: the bytes are the picked `content://` URI (UTF-8) — record it
    ///   as a content entry read over JNI without copying; the corresponding path is
    ///   dropped from the fs walk (it's just a display name, not a real path).
    ///
    /// Empty entries (sandbox-local files: copied photos, received downloads) stay
    /// as plain `entries` paths.
    // `self` is only mutated under the macOS/iOS/Android cfgs below; on other
    // targets neither block compiles, so the `mut` is genuinely unused there.
    #[cfg_attr(
        not(any(target_os = "macos", target_os = "ios", target_os = "android")),
        allow(unused_mut)
    )]
    pub fn with_bookmarks(mut self, bookmarks: Vec<Vec<u8>>) -> Self {
        #[cfg(any(target_os = "macos", target_os = "ios"))]
        for bm in bookmarks.iter().filter(|b| !b.is_empty()) {
            match scoped::ScopedAccess::acquire(bm) {
                Some(guard) => self._scopes.push(guard),
                None => tracing::warn!("FsSource: could not access security-scoped bookmark"),
            }
        }
        #[cfg(target_os = "android")]
        {
            // Reassign keys jointly over the original pick order: overlay
            // display names and fs basenames share one key namespace, so a
            // content pick and an fs root with the same name must not collide.
            let entries = std::mem::take(&mut self.entries);
            let mut kept = Vec::with_capacity(entries.len());
            let mut kept_keys = Vec::with_capacity(entries.len());
            let mut taken: HashSet<String> = HashSet::new();
            for (i, path) in entries.into_iter().enumerate() {
                let uri = bookmarks
                    .get(i)
                    .filter(|b| !b.is_empty())
                    .and_then(|b| std::str::from_utf8(b).ok())
                    .filter(|s| crate::fs::android_content::is_content_uri(s));
                match uri {
                    Some(uri) => self.content.push(ContentEntry {
                        name: unique_key(&path.to_string_lossy(), &mut taken),
                        uri: uri.to_string(),
                    }),
                    None => {
                        kept_keys.push(path.file_name().map(|n| unique_key(&n.to_string_lossy(), &mut taken)));
                        kept.push(path);
                    }
                }
            }
            self.entries = kept;
            self.root_keys = kept_keys;
        }
        #[cfg(not(any(target_os = "macos", target_os = "ios", target_os = "android")))]
        let _ = bookmarks;
        self
    }

    fn find_file_path(&self, key: &str) -> Option<PathBuf> {
        find_file_path_in_keyed(&self.entries, &self.root_keys, key)
    }

    /// Stats contributed by the Android `content://` overlay for `path`: those picks
    /// are regular files with no fs path for `stat_roots` to walk. `""` counts them
    /// all, an exact key counts that one file (as `stat_roots` does for a file root),
    /// a dir scope counts the keys under it.
    #[cfg(target_os = "android")]
    fn content_stats(&self, path: &str) -> StorageDirStats {
        let prefix = if path.is_empty() {
            String::new()
        } else {
            format!("{path}/")
        };
        let mut stats = StorageDirStats::default();
        for c in &self.content {
            if !(path.is_empty() || c.name == path || c.name.starts_with(&prefix)) {
                continue;
            }
            stats.number_of_files += 1;
            stats.total_size += crate::fs::android_content::metadata(&c.uri).map(|m| m.len()).unwrap_or(0);
        }
        stats
    }

    #[cfg(not(target_os = "android"))]
    fn content_stats(&self, _path: &str) -> StorageDirStats {
        StorageDirStats::default()
    }

    /// Resolve what a dir-stats walk of `key` covers: the fs root(s) to walk,
    /// whether an unreadable root is fatal (`strict`: a resolved single path
    /// yes, the virtual-root set no), and the `content://` overlay's own
    /// contribution. Shared by the buffered and streamed stat paths.
    fn stats_walk_plan(&self, key: &str) -> Result<(Vec<PathBuf>, bool, StorageDirStats), StorageError> {
        let content = self.content_stats(key);
        let roots: Vec<PathBuf> = if key.is_empty() {
            self.entries.clone()
        } else {
            match self.find_file_path(key) {
                Some(p) => vec![p],
                // No fs path, but the `content://` overlay covers this key.
                None if content.number_of_files > 0 => Vec::new(),
                None => return Err(StorageError::NoSuchKey(key.to_string())),
            }
        };
        Ok((roots, !key.is_empty(), content))
    }

    /// Open a search walk: the Android `content://` overlay (flat root-level
    /// files, emitted ahead of the walk exactly like `get_dir`) as the
    /// entries to emit first, then the filtered walk producer.
    fn open_search(
        &self,
        query: &StorageSearchQuery,
        options: &SearchOptions,
    ) -> (BatchReceiver, VecDeque<StorageObjectInfo>) {
        let scope = options.path_scope.as_deref().unwrap_or("");
        let recursive = options.recursive.unwrap_or(true);
        let filter = SearchFilter::new(query.name_match.as_deref());

        // Only the Android overlay below seeds `pending`; on other targets the
        // block doesn't compile, so the `mut` is genuinely unused there.
        #[cfg_attr(not(target_os = "android"), allow(unused_mut))]
        let mut pending: VecDeque<StorageObjectInfo> = VecDeque::new();
        #[cfg(target_os = "android")]
        {
            let prefix = if scope.is_empty() {
                String::new()
            } else if scope.ends_with('/') {
                scope.to_string()
            } else {
                format!("{scope}/")
            };
            for c in &self.content {
                if !c.name.starts_with(&prefix) {
                    continue;
                }
                let subpath = &c.name[prefix.len()..];
                if subpath.is_empty() || (!recursive && subpath.contains('/')) {
                    continue;
                }
                let basename = c.name.rsplit('/').next().unwrap_or(&c.name);
                if let Some(n) = &filter.needle {
                    if !basename_contains(basename, n) {
                        continue;
                    }
                }
                let md = crate::fs::android_content::metadata(&c.uri).ok();
                let mut soi = StorageObjectInfo::new(&c.name);
                soi.file_type = StorageFileType::Regular;
                soi.size = md.as_ref().map(|m| m.len()).unwrap_or(0);
                soi.etag = md.as_ref().map(|m| file_etag(m.modified().ok(), m.len()));
                pending.push_back(soi);
            }
        }

        let rx = stream_entries(
            self.entries.clone(),
            self.root_keys.clone(),
            scope.to_string(),
            recursive,
            Some(SEARCH_BATCH),
            Some(filter),
        );
        (rx, pending)
    }
}

#[async_trait]
impl Storage for FsSource {
    fn cleanup(&mut self) {}

    async fn connect(&mut self) -> Result<StorageMetaState, StorageError> {
        Ok(StorageMetaState::Complete)
    }

    async fn cancel(&self) -> Result<(), StorageError> {
        Ok(())
    }

    // `update_meta` uses the base `Storage` default: a filesystem share is
    // `Complete` at connect, so there are never incremental changes to report.
    // Listing is `get_dir` (which already includes the Android `content://` overlay).

    async fn meta_stats(&self) -> Result<StorageStatInfo, StorageError> {
        Ok(StorageStatInfo {
            meta_state: StorageMetaState::Complete,
            total_size: None,
        })
    }

    /// Search the share by name (Rust port of Electron `FSStorage.search`),
    /// paged: the basename predicate runs inside the walk producer (so
    /// non-matching entries die on the walk thread — case-insensitive,
    /// matching `StorageMeta::search`), and `SearchPager` cuts the walk into
    /// pages of `page_limit` entries (or at the `deadline` / byte budget),
    /// parking the live walk under the returned `next_page_token`; the next
    /// call with that token resumes the same walk — nothing is re-walked.
    /// `&self`: callers hold only a read lock, so a long walk doesn't block
    /// the source.
    async fn search(&self, query: &StorageSearchQuery, options: &SearchOptions) -> Result<SearchResult, StorageError> {
        self.search_pager
            .page(options, || self.open_search(query, options))
            .await
    }

    /// On-demand attrs via `lstat` (Rust port of Electron `FSStorage.getAttrs`):
    /// `""` is the synthetic virtual-root `Dir`; otherwise resolve the key and
    /// stat it directly — no full-tree scan, unlike the `meta()`-backed default.
    async fn get_attrs(&self, path: &str) -> Result<StorageObjectInfo, StorageError> {
        if path.is_empty() {
            let mut soi = StorageObjectInfo::new("");
            soi.file_type = StorageFileType::Dir;
            return Ok(soi);
        }
        // Android `content://` picks have no on-disk path to stat — answer from the
        // overlay `get_dir` lists them with, size via the provider fd.
        #[cfg(target_os = "android")]
        if let Some(c) = self.content.iter().find(|c| c.name == path) {
            let md = crate::fs::android_content::metadata(&c.uri)?;
            let mut soi = StorageObjectInfo::new(path);
            soi.file_type = StorageFileType::Regular;
            soi.size = md.len();
            soi.etag = Some(file_etag(md.modified().ok(), md.len()));
            return Ok(soi);
        }
        let fspath = self
            .find_file_path(path)
            .ok_or_else(|| StorageError::NoSuchKey(path.to_string()))?;
        let md = fs::symlink_metadata(&fspath).await?;
        let mut soi = StorageObjectInfo::new(path);
        if md.is_dir() {
            soi.file_type = StorageFileType::Dir;
        } else if md.file_type().is_symlink() {
            soi.file_type = StorageFileType::Symlink;
            // Symlinks carry their size like everywhere else (the dir listing
            // reports it) — omitting it here made a single-symlink meta read
            // as zero bytes.
            soi.size = super::walk::entry_size(&fspath, &md);
        } else {
            soi.file_type = StorageFileType::Regular;
            soi.size = md.len();
            soi.etag = Some(file_etag(md.modified().ok(), md.len()));
        }
        if let Ok(mtime) = md.modified() {
            let dt = system_time_to_chrono(mtime);
            soi.attrs.mtime = dt;
        }
        soi.attrs.posix_permissions = super::walk::mode_bits(&md).map(i64::from);
        Ok(soi)
    }

    /// Native on-demand directory listing (Rust port of Electron
    /// `FSStorage.getDirInternal`): walk the resolved scope lazily and stream
    /// batches. The walk runs as the receiver is consumed (backpressure), so a
    /// subpath query never scans the whole share and an early-dropped receiver
    /// halts it.
    async fn get_dir(
        &self,
        path: &str,
        recursive: bool,
        start_after: Option<&str>,
        max_batch_size: Option<usize>,
    ) -> mpsc::Receiver<Result<Vec<StorageObjectInfo>, StorageError>> {
        if start_after.is_some() {
            // A local walk is in directory order, not key order; a cursor over
            // it would be a lie. Nothing lists a local share with one.
            let (tx, rx) = mpsc::channel(1);
            tokio::spawn(async move {
                let _ = tx
                    .send(Err(StorageError::NotImplemented("FS getDir start_after".into())))
                    .await;
            });
            return rx;
        }
        let inner = stream_entries(
            self.entries.clone(),
            self.root_keys.clone(),
            path.to_string(),
            recursive,
            max_batch_size,
            None,
        );

        // Android `content://` picks have no on-disk path to walk; overlay them as
        // flat root-level regular files so every consumer (the engine's directory
        // pass, search) sees them via `get_dir`.
        #[cfg(target_os = "android")]
        {
            let content: Vec<(String, String)> = self.content.iter().map(|c| (c.name.clone(), c.uri.clone())).collect();
            if content.is_empty() {
                return inner;
            }
            let prefix = if path.is_empty() {
                String::new()
            } else if path.ends_with('/') {
                path.to_string()
            } else {
                format!("{}/", path)
            };
            let (tx, rx) = mpsc::channel(4);
            tokio::spawn(async move {
                // Emit the (filtered) overlay first, then forward the on-disk walk.
                let mut overlay: Vec<StorageObjectInfo> = Vec::new();
                for (name, uri) in content {
                    if !name.starts_with(&prefix) {
                        continue;
                    }
                    let subpath = &name[prefix.len()..];
                    if subpath.is_empty() || (!recursive && subpath.contains('/')) {
                        continue;
                    }
                    let md = crate::fs::android_content::metadata(&uri).ok();
                    let mut soi = StorageObjectInfo::new(&name);
                    soi.file_type = StorageFileType::Regular;
                    soi.size = md.as_ref().map(|m| m.len()).unwrap_or(0);
                    soi.etag = md.as_ref().map(|m| file_etag(m.modified().ok(), m.len()));
                    overlay.push(soi);
                }
                let limit = max_batch_size.filter(|n| *n > 0).unwrap_or(usize::MAX);
                for chunk in overlay.chunks(limit) {
                    if tx.send(Ok(chunk.to_vec())).await.is_err() {
                        return;
                    }
                }
                let mut inner = inner;
                while let Some(batch) = inner.recv().await {
                    if tx.send(batch).await.is_err() {
                        return;
                    }
                }
            });
            return rx;
        }

        #[cfg(not(target_os = "android"))]
        inner
    }

    /// On-demand recursive walk (Rust port of Electron `FSStorage.getDirStats`):
    /// counts the *contents* of the resolved scope, publishing running totals
    /// as it goes (see `stat_roots`). Fixes the `meta()`-backed default's
    /// caveats — no full-tree scan, no double-counting the share-root node, and
    /// symlink sizes are included.
    ///
    /// The whole recursive stat runs on one blocking thread via `std::fs` — no
    /// per-entry `tokio::fs` round-trips — and publishes with `blocking_send`,
    /// so a slow consumer applies real backpressure and a dropped receiver
    /// aborts the walk instead of statting a whole tree nobody is waiting for.
    async fn get_dir_stats_streamed(&self, path: &str) -> mpsc::Receiver<Result<StorageDirStats, StorageError>> {
        let (tx, rx) = mpsc::channel(4);
        let plan = self.stats_walk_plan(path);
        let (roots, strict, content) = match plan {
            Ok(p) => p,
            Err(e) => {
                let _ = tx.send(Err(e)).await;
                return rx;
            }
        };
        tokio::task::spawn_blocking(move || {
            // The `content://` overlay isn't walked — fold it into every
            // published total so partials and the final agree.
            let add_content = |s: &StorageDirStats| StorageDirStats {
                number_of_files: s.number_of_files + content.number_of_files,
                total_size: s.total_size + content.total_size,
                ..*s
            };
            let ptx = tx.clone();
            let result = stat_roots(&roots, strict, &mut |s| {
                ptx.blocking_send(Ok(add_content(s)))
                    .map_err(|_| StorageError::Cancelled)
            });
            // The final block always goes out (partials may have been skipped
            // entirely on a fast walk); a cancelled walk has nobody to tell.
            match result {
                Ok(s) => {
                    let _ = tx.blocking_send(Ok(add_content(&s)));
                }
                Err(StorageError::Cancelled) => {}
                Err(e) => {
                    let _ = tx.blocking_send(Err(e));
                }
            }
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
impl StorageSource for FsSource {
    fn source_name(&self) -> &str {
        "FileSystem"
    }

    fn source_type(&self) -> StorageSourceType {
        StorageSourceType::FileSystem
    }

    /// True chunked streaming (Rust port of Electron `FSStorage.getStreamed`):
    /// open once, seek to `offset`, push fixed-size chunks down the channel
    /// instead of buffering the whole range in RAM (what the trait default does
    /// via `get()`). Dropping the receiver cancels the producer.
    async fn get_streamed(
        &self,
        object: &str,
        offset: Option<u64>,
        length: Option<u64>,
    ) -> mpsc::Receiver<Result<Vec<u8>, StorageError>> {
        // On Android a picked `content://` file is read over JNI as an fd
        // (a couple of JNI calls to open, then the same chunked pump).
        #[cfg(target_os = "android")]
        if let Some(c) = self.content.iter().find(|c| c.name == object) {
            let uri = c.uri.clone();
            let (tx, rx) = mpsc::channel(4);
            tokio::spawn(async move {
                match crate::fs::android_content::open_fd(&uri) {
                    Ok(fd) => pump_file(fs::File::from_std(std::fs::File::from(fd)), offset, length, tx).await,
                    Err(e) => {
                        let _ = tx.send(Err(e.into())).await;
                    }
                }
            });
            return rx;
        }
        match self.find_file_path(object) {
            Some(path) => stream_file(path, offset, length),
            None => {
                let (tx, rx) = mpsc::channel(4);
                let _ = tx.send(Err(StorageError::NoSuchKey(object.to_string()))).await;
                rx
            }
        }
    }

    async fn get_symlink(&self, object: &str) -> Result<String, StorageError> {
        let path = self
            .find_file_path(object)
            .ok_or_else(|| StorageError::NoSuchKey(object.to_string()))?;
        let target = fs::read_link(&path).await?;
        Ok(target.to_string_lossy().to_string())
    }

    fn serves_previews(&self) -> bool {
        self.previews.is_some()
    }

    /// Keys are resolved here, as `get_streamed` resolves them; the
    /// generations run in their own tasks, off the storage guard.
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
            targets.push(self.preview_target(object).await);
        }
        Ok(spawn_previews(generator, targets, px))
    }
}

impl FsSource {
    /// The locator the generator opens for `object` — a picked `content://`
    /// URI on Android, else the path of a regular file — and the etag of the
    /// version behind it, as the listing reports it. lstat — a symlink or a
    /// directory has no preview, and a symlink's target is never touched.
    async fn preview_target(&self, object: &str) -> Result<(String, Option<String>), StorageError> {
        #[cfg(target_os = "android")]
        if let Some(c) = self.content.iter().find(|c| c.name == object) {
            let etag = crate::fs::android_content::metadata(&c.uri)
                .ok()
                .map(|m| file_etag(m.modified().ok(), m.len()));
            return Ok((c.uri.clone(), etag));
        }
        let path = self
            .find_file_path(object)
            .ok_or_else(|| StorageError::NoSuchKey(object.to_string()))?;
        let md = fs::symlink_metadata(&path).await?;
        if !md.is_file() {
            return Err(StorageError::NoPreview(object.to_string()));
        }
        match path.to_str() {
            Some(p) => Ok((p.to_string(), Some(file_etag(md.modified().ok(), md.len())))),
            None => Err(StorageError::NoPreview(object.to_string())),
        }
    }
}

#[cfg(test)]
#[path = "source_test.rs"]
mod source_test;
