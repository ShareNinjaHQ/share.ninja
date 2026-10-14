//! Plain local filesystem storage for file-manager use (the commander's
//! panels): one root, keys are root-relative paths, and the same object is
//! both a source and a destination. No security-scoped bookmarks, no
//! Android content overlay, no multipart meta store, no collision policy —
//! existing entries are the transfer engine's business (`OnExisting`), and a
//! large file is written positionally straight into its final path.

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use tokio::fs;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tokio::sync::{Mutex, mpsc};

use crate::transfers::{
    SearchOptions, SearchResult, StorageDirStats, StorageFileAttrs, StoragePutConditions, StoragePutResult,
    StorageSearchQuery, StorageSourceType, StorageStatInfo, reject_conditions,
};
use crate::{
    Storage, StorageChange, StorageDest, StorageDestMultipart, StorageError, StorageFileType, StorageMetaState,
    StorageMultipartUpload, StorageObjectInfo, StoragePartInfo, StorageSource,
};

use super::dest::{apply_attrs, volume_free_capacity};
use super::search::{BatchReceiver, SEARCH_BATCH, SearchFilter, SearchPager};
use super::source::stat_roots;
use super::stream::stream_file;
use super::walk::{IncompleteParts, WalkRenames, meta_parts, soi_from, spawn_listing, walk_into_sink};

pub struct LocalFs {
    root: PathBuf,
    /// Attrs handed to `begin_multipart`, applied at `complete_multipart`.
    pending_attrs: Mutex<HashMap<String, Option<StorageFileAttrs>>>,
    data_source_id: Option<String>,
    /// The parked paged-search walks (see `SearchPager`).
    search_pager: SearchPager,
}

impl LocalFs {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            pending_attrs: Mutex::new(HashMap::new()),
            data_source_id: None,
            search_pager: SearchPager::new(),
        }
    }

    /// Open a search walk under `path_scope` (`""` = the root): the filtered
    /// walk producer, keys root-relative like `get_dir`'s.
    fn open_search(
        &self,
        query: &StorageSearchQuery,
        options: &SearchOptions,
    ) -> (BatchReceiver, VecDeque<StorageObjectInfo>) {
        let scope = options
            .path_scope
            .as_deref()
            .unwrap_or("")
            .trim_matches('/')
            .to_string();
        let dir = match self.resolve(&scope) {
            Ok(p) => p,
            Err(e) => {
                let (tx, rx) = mpsc::channel(1);
                let _ = tx.try_send(Err(e));
                return (rx, VecDeque::new());
            }
        };
        let recursive = options.recursive.unwrap_or(true);
        let mut filter = Some(SearchFilter::new(query.name_match.as_deref()));
        let rx = spawn_listing(Some(SEARCH_BATCH), move |sink| {
            walk_into_sink(&dir, &scope, recursive, sink, &mut filter, &IncompleteParts::new(), &WalkRenames::new())
        });
        (rx, VecDeque::new())
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// `key` → on-disk path. Keys are root-relative (`""` = the root); `.` /
    /// `..` components are refused so a key can't escape the root.
    fn resolve(&self, key: &str) -> Result<PathBuf, StorageError> {
        let comps: Vec<&str> = key.split('/').filter(|c| !c.is_empty()).collect();
        if comps.iter().any(|c| *c == "." || *c == "..") {
            return Err(StorageError::Other(format!("path traversal in key: {key}")));
        }
        let mut p = self.root.clone();
        for c in comps {
            p.push(c);
        }
        Ok(p)
    }

    fn resolve_or_missing(&self, key: &str) -> Result<PathBuf, StorageError> {
        let p = self.resolve(key)?;
        if std::fs::symlink_metadata(&p).is_err() {
            return Err(StorageError::NoSuchKey(key.to_string()));
        }
        Ok(p)
    }
}

#[async_trait]
impl Storage for LocalFs {
    async fn connect(&mut self) -> Result<StorageMetaState, StorageError> {
        fs::metadata(&self.root).await?;
        Ok(StorageMetaState::Complete)
    }

    async fn cancel(&self) -> Result<(), StorageError> {
        Ok(())
    }

    async fn update_meta(&self) -> Result<Vec<StorageChange>, StorageError> {
        Ok(Vec::new())
    }

    async fn meta_stats(&self) -> Result<StorageStatInfo, StorageError> {
        Ok(StorageStatInfo {
            meta_state: StorageMetaState::Complete,
            total_size: None,
        })
    }

    async fn get_attrs(&self, key: &str) -> Result<StorageObjectInfo, StorageError> {
        if key.is_empty() {
            let mut soi = StorageObjectInfo::new("");
            soi.file_type = StorageFileType::Dir;
            return Ok(soi);
        }
        let path = self.resolve(key)?;
        let md = match fs::symlink_metadata(&path).await {
            Ok(md) => md,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(StorageError::NoSuchKey(key.to_string()));
            }
            Err(e) => return Err(e.into()),
        };
        let (kind, size, mtime, mode) = meta_parts(&path, &md);
        Ok(soi_from(key, kind, size, mtime, mode))
    }

    /// Streamed listing under `key` (the root for `""`), batched; the walk
    /// runs on a blocking thread as the receiver is consumed.
    async fn get_dir(
        &self,
        key: &str,
        recursive: bool,
        start_after: Option<&str>,
        max_batch_size: Option<usize>,
    ) -> mpsc::Receiver<Result<Vec<StorageObjectInfo>, StorageError>> {
        if start_after.is_some() {
            let (tx, rx) = mpsc::channel(1);
            let _ = tx
                .send(Err(StorageError::NotImplemented("LocalFs getDir start_after".into())))
                .await;
            return rx;
        }
        let dir = match self.resolve(key) {
            Ok(p) => p,
            Err(e) => {
                let (tx, rx) = mpsc::channel(1);
                let _ = tx.send(Err(e)).await;
                return rx;
            }
        };
        let prefix = key.trim_matches('/').to_string();
        spawn_listing(max_batch_size, move |sink| {
            walk_into_sink(&dir, &prefix, recursive, sink, &mut None, &IncompleteParts::new(), &WalkRenames::new())
        })
    }

    /// Counts under `key` (the key itself for a file/symlink); one final
    /// snapshot, walked on a blocking thread.
    async fn get_dir_stats_streamed(&self, key: &str) -> mpsc::Receiver<Result<StorageDirStats, StorageError>> {
        let (tx, rx) = mpsc::channel(4);
        let path = match self.resolve(key) {
            Ok(p) => p,
            Err(e) => {
                let _ = tx.send(Err(e)).await;
                return rx;
            }
        };
        // The one resolved path is a strict root: an unreadable one is the
        // error, not a skip. Running totals go out as the walk publishes them.
        tokio::task::spawn_blocking(move || {
            let ptx = tx.clone();
            let result = stat_roots(&[path], true, &mut |s| {
                ptx.blocking_send(Ok(*s)).map_err(|_| StorageError::Cancelled)
            });
            // The final block always goes out (partials may have been skipped
            // entirely on a fast walk); a cancelled walk has nobody to tell.
            match result {
                Ok(s) => {
                    let _ = tx.blocking_send(Ok(s));
                }
                Err(StorageError::Cancelled) => {}
                Err(e) => {
                    let _ = tx.blocking_send(Err(e));
                }
            }
        });
        rx
    }

    /// Name search under `path_scope`, paged by `SearchPager` like
    /// `FsSource::search`: `page_limit` entries per page, the live walk
    /// parked under `next_page_token` between calls.
    async fn search(&self, query: &StorageSearchQuery, options: &SearchOptions) -> Result<SearchResult, StorageError> {
        self.search_pager
            .page(options, || self.open_search(query, options))
            .await
    }

    fn data_source_id(&self) -> Option<&str> {
        self.data_source_id.as_deref()
    }

    fn set_data_source_id(&mut self, id: String) {
        self.data_source_id = Some(id);
    }
}

#[async_trait]
impl StorageSource for LocalFs {
    fn source_name(&self) -> &str {
        "LocalFs"
    }

    fn source_type(&self) -> StorageSourceType {
        StorageSourceType::FileSystem
    }

    /// Chunked streaming read: open once, seek to `offset`, push fixed-size
    /// chunks; dropping the receiver cancels the producer.
    async fn get_streamed(
        &self,
        key: &str,
        offset: Option<u64>,
        length: Option<u64>,
    ) -> mpsc::Receiver<Result<Vec<u8>, StorageError>> {
        match self.resolve_or_missing(key) {
            Ok(path) => stream_file(path, offset, length),
            Err(e) => {
                let (tx, rx) = mpsc::channel(1);
                let _ = tx.send(Err(e)).await;
                rx
            }
        }
    }

    async fn get_symlink(&self, key: &str) -> Result<String, StorageError> {
        let path = self.resolve_or_missing(key)?;
        let target = fs::read_link(&path).await?;
        Ok(target.to_string_lossy().to_string())
    }
}

#[async_trait]
impl StorageDest for LocalFs {
    fn is_sequential(&self) -> bool {
        false
    }

    async fn check_space(&self, required_total: u64) -> Result<(), StorageError> {
        let base = self.root.clone();
        let (free, capacity) = tokio::task::spawn_blocking(move || volume_free_capacity(&base))
            .await
            .map_err(|e| StorageError::Other(format!("statvfs join: {e}")))??;
        let reserve = capacity / 100;
        if required_total > free.saturating_sub(reserve) {
            return Err(StorageError::NoSpace {
                required: required_total,
                free,
            });
        }
        Ok(())
    }

    async fn put_file(
        &self,
        key: &str,
        data: &[u8],
        attrs: Option<&StorageFileAttrs>,
        conditions: Option<&StoragePutConditions>,
    ) -> Result<StoragePutResult, StorageError> {
        reject_conditions(conditions)?;
        let path = self.resolve(key)?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        fs::write(&path, data).await?;
        apply_attrs(&path, attrs).await?;
        Ok(StoragePutResult {
            etag: None,
            crc32: None,
            sha256: None,
            storage_key: key.to_string(),
        })
    }

    async fn put_dir(&self, key: &str, attrs: Option<&StorageFileAttrs>) -> Result<StoragePutResult, StorageError> {
        let path = self.resolve(key)?;
        fs::create_dir_all(&path).await?;
        apply_attrs(&path, attrs).await?;
        Ok(StoragePutResult {
            etag: None,
            crc32: None,
            sha256: None,
            storage_key: key.to_string(),
        })
    }

    async fn put_symlink(
        &self,
        key: &str,
        target: &str,
        _attrs: Option<&StorageFileAttrs>,
    ) -> Result<StoragePutResult, StorageError> {
        let path = self.resolve(key)?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        #[cfg(unix)]
        {
            if let Err(e) = fs::symlink(target, &path).await {
                if e.kind() == std::io::ErrorKind::PermissionDenied {
                    return Err(StorageError::SymlinkUnsupported(key.to_string()));
                }
                return Err(e.into());
            }
        }
        #[cfg(windows)]
        {
            if fs::symlink_file(target, &path).await.is_err() {
                fs::write(&path, target).await?;
            }
        }
        Ok(StoragePutResult {
            etag: None,
            crc32: None,
            sha256: None,
            storage_key: key.to_string(),
        })
    }

    async fn del_file(&self, key: &str) -> Result<(), StorageError> {
        let path = self.resolve(key)?;
        match fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    async fn del_dir(&self, key: &str) -> Result<(), StorageError> {
        let path = self.resolve(key)?;
        match fs::remove_dir(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    async fn update_stats(&self, _info: &StorageStatInfo) -> Result<(), StorageError> {
        Ok(())
    }

    async fn close(&self) -> Result<(), StorageError> {
        Ok(())
    }
}

#[async_trait]
impl StorageDestMultipart for LocalFs {
    /// No records: nothing resumes across runs.
    async fn list_multiparts_all(&self) -> Result<Vec<StorageMultipartUpload>, StorageError> {
        Ok(Vec::new())
    }

    async fn list_parts_all(&self, _upload: &StorageMultipartUpload) -> Result<Vec<StoragePartInfo>, StorageError> {
        Ok(Vec::new())
    }

    /// Create (truncate) the final file; parts are written into it in place.
    async fn begin_multipart(
        &self,
        key: &str,
        _size: u64,
        attrs: Option<&StorageFileAttrs>,
    ) -> Result<StorageMultipartUpload, StorageError> {
        let path = self.resolve(key)?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        fs::File::create(&path).await?;
        self.pending_attrs.lock().await.insert(key.to_string(), attrs.cloned());
        Ok(StorageMultipartUpload {
            plain_key: Some(key.to_string()),
            encrypted_key: Some(key.to_string()),
            upload_id: Some(key.to_string()),
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
        let key = upload
            .plain_key
            .as_deref()
            .ok_or_else(|| StorageError::Other("no key in upload".into()))?;
        let path = self.resolve(key)?;
        // No truncate: one part at `offset` into a file other parts of which
        // are already on disk.
        let mut f = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .await?;
        f.seek(SeekFrom::Start(offset)).await?;
        f.write_all(data).await?;
        // tokio's `write_all` returns once the bytes are handed to the blocking
        // pool; without `flush` the write can still be in flight when the part
        // is reported landed (and `complete_multipart` runs).
        f.flush().await?;
        Ok(StoragePartInfo {
            part_number: Some(part_number),
            offset: Some(offset),
            size: Some(data.len() as u64),
            ..Default::default()
        })
    }

    async fn cancel_multipart(&self, upload: &StorageMultipartUpload) -> Result<(), StorageError> {
        let key = upload
            .plain_key
            .as_deref()
            .ok_or_else(|| StorageError::Other("no key in upload".into()))?;
        self.pending_attrs.lock().await.remove(key);
        self.del_file(key).await
    }

    async fn complete_multipart(
        &self,
        upload: &StorageMultipartUpload,
        _parts: &[StoragePartInfo],
    ) -> Result<String, StorageError> {
        let key = upload
            .plain_key
            .as_deref()
            .ok_or_else(|| StorageError::Other("no key in upload".into()))?;
        let attrs = self.pending_attrs.lock().await.remove(key).flatten();
        let path = self.resolve(key)?;
        apply_attrs(&path, attrs.as_ref()).await?;
        Ok(key.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transfers::TransferItem;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    struct TmpDir(PathBuf);
    impl TmpDir {
        fn new() -> Self {
            let p = std::env::temp_dir().join(format!("localfs-{}-{}", std::process::id(), uuid::Uuid::new_v4()));
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

    async fn keys(fs: &mut LocalFs, key: &str, recursive: bool) -> Vec<String> {
        let mut rx = fs.get_dir(key, recursive, None, Some(2)).await;
        let mut out = Vec::new();
        while let Some(b) = rx.recv().await {
            out.extend(b.unwrap().into_iter().map(|s| s.key));
        }
        out.sort();
        out
    }

    #[tokio::test]
    async fn lists_relative_keys_and_stats() {
        let tmp = TmpDir::new();
        std::fs::create_dir_all(tmp.path().join("d/sub")).unwrap();
        std::fs::write(tmp.path().join("a.txt"), b"hello").unwrap();
        std::fs::write(tmp.path().join("d/b.txt"), b"hi").unwrap();
        std::fs::write(tmp.path().join("d/sub/c.txt"), b"!").unwrap();
        let mut fs = LocalFs::new(tmp.path());

        assert_eq!(keys(&mut fs, "", false).await, vec!["a.txt", "d"]);
        assert_eq!(
            keys(&mut fs, "", true).await,
            vec!["a.txt", "d", "d/b.txt", "d/sub", "d/sub/c.txt"]
        );
        assert_eq!(keys(&mut fs, "d", false).await, vec!["d/b.txt", "d/sub"]);

        let stats = fs.get_dir_stats_streamed("").await.recv().await.unwrap().unwrap();
        assert_eq!(
            (stats.number_of_files, stats.number_of_folders, stats.total_size),
            (3, 2, 8)
        );

        let a = fs.get_attrs("a.txt").await.unwrap();
        assert_eq!((a.file_type, a.size), (StorageFileType::Regular, 5));
        assert!(matches!(fs.get_attrs("nope").await, Err(StorageError::NoSuchKey(_))));
        assert!(fs.resolve("../x").is_err());
    }

    #[tokio::test]
    async fn search_pages_the_walk_under_a_scope() {
        let tmp = TmpDir::new();
        std::fs::create_dir_all(tmp.path().join("d/sub")).unwrap();
        for i in 0..5 {
            std::fs::write(tmp.path().join(format!("hit_{i}.txt")), b"x").unwrap();
            std::fs::write(tmp.path().join("d/sub").join(format!("hit_sub_{i}.txt")), b"x").unwrap();
        }
        std::fs::write(tmp.path().join("d/other.dat"), b"x").unwrap();
        let fs = LocalFs::new(tmp.path());

        let q = StorageSearchQuery {
            name_match: Some("HIT".into()),
        };
        let mut opts = SearchOptions {
            path_scope: Some("d".into()),
            recursive: Some(true),
            page_limit: Some(2),
            ..Default::default()
        };
        let mut all = Vec::new();
        loop {
            let res = fs.search(&q, &opts).await.unwrap();
            assert!(res.items.len() <= 2, "a page is at most page_limit entries");
            all.extend(res.items.into_iter().map(|s| s.key));
            match res.next_page_token {
                Some(t) => opts.page_token = Some(t),
                None => break,
            }
        }
        all.sort();
        let want: Vec<String> = (0..5).map(|i| format!("d/sub/hit_sub_{i}.txt")).collect();
        assert_eq!(
            all, want,
            "scoped under d, root-relative keys, case-insensitive, no repeats"
        );

        let flat = SearchOptions {
            path_scope: Some("d".into()),
            recursive: Some(false),
            ..Default::default()
        };
        let res = fs.search(&q, &flat).await.unwrap();
        assert!(res.items.is_empty(), "non-recursive: d holds no direct match");
        assert!(res.next_page_token.is_none());
    }

    #[tokio::test]
    async fn copies_a_tree_between_two_roots() {
        let src_dir = TmpDir::new();
        let dst_dir = TmpDir::new();
        std::fs::create_dir_all(src_dir.path().join("d")).unwrap();
        std::fs::write(src_dir.path().join("a.txt"), b"hello").unwrap();
        std::fs::write(src_dir.path().join("d/b.txt"), vec![7u8; 3 * 1024 * 1024]).unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink("a.txt", src_dir.path().join("link")).unwrap();

        let source: Arc<RwLock<dyn StorageSource>> = Arc::new(RwLock::new(LocalFs::new(src_dir.path())));
        let dest: Arc<RwLock<dyn StorageDestMultipart>> = Arc::new(RwLock::new(LocalFs::new(dst_dir.path())));
        let ti = TransferItem::new(source, dest, None, 3).await;
        ti.start().await.unwrap();

        assert_eq!(std::fs::read(dst_dir.path().join("a.txt")).unwrap(), b"hello");
        assert_eq!(
            std::fs::read(dst_dir.path().join("d/b.txt")).unwrap().len(),
            3 * 1024 * 1024
        );
        #[cfg(unix)]
        assert_eq!(
            std::fs::read_link(dst_dir.path().join("link")).unwrap(),
            Path::new("a.txt")
        );
    }

    /// File-manager copy: dest = dest dir + basename of the selection, not
    /// the full source path (`TransferItem::with_source_root`).
    #[tokio::test]
    async fn source_root_strips_the_prefix_off_dest_keys() {
        let src_dir = TmpDir::new();
        let dst_dir = TmpDir::new();
        std::fs::create_dir_all(src_dir.path().join("docs/sub")).unwrap();
        std::fs::write(src_dir.path().join("docs/report.pdf"), b"pdf").unwrap();
        std::fs::write(src_dir.path().join("docs/sub/x.txt"), b"x").unwrap();
        std::fs::create_dir_all(src_dir.path().join("docs/deep/er")).unwrap();
        std::fs::write(src_dir.path().join("docs/deep/er/y.txt"), b"yy").unwrap();

        let source: Arc<RwLock<dyn StorageSource>> = Arc::new(RwLock::new(LocalFs::new(src_dir.path())));
        let dest: Arc<RwLock<dyn StorageDestMultipart>> = Arc::new(RwLock::new(LocalFs::new(dst_dir.path())));
        let selection = vec![
            "docs/report.pdf".to_string(),
            "docs/sub".to_string(),
            "docs/deep/er/y.txt".to_string(),
        ];
        let ti = TransferItem::new(source, dest, Some(selection), 2)
            .await
            .with_source_root("docs");
        ti.start().await.unwrap();

        assert_eq!(std::fs::read(dst_dir.path().join("report.pdf")).unwrap(), b"pdf");
        assert_eq!(std::fs::read(dst_dir.path().join("sub/x.txt")).unwrap(), b"x");
        assert_eq!(std::fs::read(dst_dir.path().join("deep/er/y.txt")).unwrap(), b"yy");
        assert!(
            !dst_dir.path().join("docs").exists(),
            "source root must not be recreated on dest"
        );
        let mut top: Vec<_> = std::fs::read_dir(dst_dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        top.sort();
        assert_eq!(top, vec!["deep", "report.pdf", "sub"]);
    }

    /// A selection root not strictly under the source root is a caller
    /// error and fails before anything lands.
    #[tokio::test]
    async fn source_root_rejects_keys_outside_it() {
        let src_dir = TmpDir::new();
        let dst_dir = TmpDir::new();
        std::fs::write(src_dir.path().join("a.txt"), b"a").unwrap();
        let source: Arc<RwLock<dyn StorageSource>> = Arc::new(RwLock::new(LocalFs::new(src_dir.path())));
        let dest: Arc<RwLock<dyn StorageDestMultipart>> = Arc::new(RwLock::new(LocalFs::new(dst_dir.path())));
        let ti = TransferItem::new(source, dest, Some(vec!["a.txt".to_string()]), 2)
            .await
            .with_source_root("docs");
        assert!(matches!(ti.start().await, Err(StorageError::Other(_))));
        assert!(std::fs::read_dir(dst_dir.path()).unwrap().next().is_none());
    }

    #[tokio::test]
    async fn multipart_writes_in_place_and_deletes() {
        let tmp = TmpDir::new();
        let fs = LocalFs::new(tmp.path());
        let up = fs.begin_multipart("big.bin", 0, None).await.unwrap();
        fs.put_part(2, &up, 4, b"WXYZ", true).await.unwrap();
        fs.put_part(1, &up, 0, b"abcd", false).await.unwrap();
        fs.complete_multipart(&up, &[]).await.unwrap();
        assert_eq!(std::fs::read(tmp.path().join("big.bin")).unwrap(), b"abcdWXYZ");

        fs.put_dir("x/y", None).await.unwrap();
        fs.del_dir("x/y").await.unwrap();
        fs.del_dir("x").await.unwrap();
        fs.del_file("big.bin").await.unwrap();
        fs.del_file("big.bin").await.unwrap(); // idempotent
        assert!(std::fs::read_dir(tmp.path()).unwrap().next().is_none());
    }
}
