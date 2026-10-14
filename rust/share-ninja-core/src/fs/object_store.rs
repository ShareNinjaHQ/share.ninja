//! Local-disk object store: the agent-side backend for NinjaStorageV3 when a
//! team hosts its own storage (`docs/…/NinjaStorageV3.md`, "Store Contract" →
//! "P2P"). One root directory, keys map to paths verbatim, and the store knows
//! nothing about what it holds — every object is opaque ciphertext.
//!
//! This is deliberately NOT `FsDest`: that is the download destination, with
//! collision avoidance and a "never overwrite a leaf we don't own" guard. An
//! object store needs the opposite — overwrite when the compare-and-swap says
//! so, map an existing `v3/` as-is on restart — so it gets its own type.
//!
//! What V3 relies on here:
//! - `put_file` with `if_none_match` (atomic create-new via temp + `hard_link`)
//!   and `if_match` (CAS on the etag via temp + `rename` under a lock);
//! - a cheap, change-on-every-write etag (`mtime_ns-size-inode`) on every
//!   put and `get_attrs`;
//! - `get_dir` in key order with an exclusive `start_after` cursor.

// Constructed by the P2P file server once it serves a V3 root (next step);
// until then only the tests build one.
#![allow(dead_code)]

use std::io::SeekFrom;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::mpsc;

use crate::transfers::{StorageFileAttrs, StoragePutConditions, StoragePutResult, StorageSourceType, StorageStatInfo};
use crate::{Storage, StorageDest, StorageError, StorageFileType, StorageMetaState, StorageObjectInfo, StorageSource};

/// Bytes per chunk of a streamed read.
const READ_CHUNK: usize = 4 * 1024 * 1024;

pub struct FsObjectStore {
    root: PathBuf,
    data_source_id: Option<String>,
    /// Serialises `if_match` puts against each other: stat→compare→rename must
    /// not interleave with another CAS. `if_none_match` is atomic on its own
    /// (`link(2)`), unconditional puts need no ordering. One lock for the
    /// store, not per key — the only CAS'd object in practice is the root pointer.
    cas_lock: tokio::sync::Mutex<()>,
}

impl FsObjectStore {
    /// Open the store over `root`, creating it if missing.
    pub async fn open(root: impl Into<PathBuf>) -> Result<Self, StorageError> {
        let root = root.into();
        fs::create_dir_all(&root).await?;
        Ok(Self {
            root,
            data_source_id: None,
            cas_lock: tokio::sync::Mutex::new(()),
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Resolve a key to its on-disk path. Keys are `/`-separated, relative,
    /// and may not climb out of the root; anything else is a bad key, not an
    /// I/O error.
    fn path_for(&self, key: &str) -> Result<PathBuf, StorageError> {
        if key.is_empty() || key.starts_with('/') || key.ends_with('/') {
            return Err(StorageError::Other(format!("bad object key: {key:?}")));
        }
        let mut p = self.root.clone();
        for seg in key.split('/') {
            if seg.is_empty() || seg == "." || seg == ".." || seg.contains('\\') || seg.contains('\0') {
                return Err(StorageError::Other(format!("bad object key: {key:?}")));
            }
            p.push(seg);
        }
        Ok(p)
    }

    /// Directory to list for `path` (`""` = root).
    fn dir_for(&self, path: &str) -> Result<PathBuf, StorageError> {
        let trimmed = path.trim_matches('/');
        if trimmed.is_empty() {
            Ok(self.root.clone())
        } else {
            self.path_for(trimmed)
        }
    }
}

/// Version tag of a file: `mtime_ns-size-inode`. One stat, no read; changes
/// on every write this store performs because conditional puts land through
/// temp + rename/link and therefore a fresh inode. Opaque; equality only.
fn fs_etag(md: &std::fs::Metadata) -> String {
    let mtime_ns = md
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    #[cfg(unix)]
    let ino = {
        use std::os::unix::fs::MetadataExt;
        md.ino()
    };
    #[cfg(not(unix))]
    let ino = 0u64;
    format!("{}-{}-{}", mtime_ns, md.len(), ino)
}

/// Temp sibling for an atomic write of `path` — same directory, so the final
/// rename/link never crosses a filesystem.
fn temp_sibling(path: &Path) -> PathBuf {
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    path.with_file_name(format!(".{}.{}.tmp", name, uuid::Uuid::new_v4().simple()))
}

fn soi_for(key: &str, md: &std::fs::Metadata) -> StorageObjectInfo {
    let mut soi = StorageObjectInfo::new(key);
    if md.is_dir() {
        soi.file_type = StorageFileType::Dir;
    } else {
        soi.file_type = StorageFileType::Regular;
        soi.size = md.len();
        soi.etag = Some(fs_etag(md));
    }
    soi.attrs.mtime = md.modified().ok().map(chrono::DateTime::<chrono::Utc>::from);
    soi
}

/// Collect every entry under `dir` as `(key, metadata)`; `recursive` descends.
/// Temp siblings of in-flight conditional puts are skipped. Blocking walk —
/// callers run it on the blocking pool.
fn collect_entries(
    dir: &Path,
    key_prefix: &str,
    recursive: bool,
    out: &mut Vec<(String, std::fs::Metadata)>,
) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.ends_with(".tmp") && name.starts_with('.') {
            continue;
        }
        let md = entry.metadata()?;
        let key = if key_prefix.is_empty() {
            name
        } else {
            format!("{key_prefix}/{name}")
        };
        if md.is_dir() && recursive {
            collect_entries(&entry.path(), &key, true, out)?;
        } else {
            out.push((key, md));
        }
    }
    Ok(())
}

#[async_trait]
impl Storage for FsObjectStore {
    async fn connect(&mut self) -> Result<StorageMetaState, StorageError> {
        Ok(StorageMetaState::Complete)
    }

    async fn cancel(&self) -> Result<(), StorageError> {
        Ok(())
    }

    /// Entries under `path` in **key order**, so `start_after` (exclusive) is a
    /// true cursor. The directory is read and sorted in memory per call —
    /// which is the cost this backend pays for a listing contract S3 gives
    /// natively; keep big prefixes bounded (GC does for `wal/`).
    async fn get_dir(
        &self,
        path: &str,
        recursive: bool,
        start_after: Option<&str>,
        max_batch_size: Option<usize>,
    ) -> mpsc::Receiver<Result<Vec<StorageObjectInfo>, StorageError>> {
        let (tx, rx) = mpsc::channel(4);
        let dir = match self.dir_for(path) {
            Ok(d) => d,
            Err(e) => {
                let _ = tx.send(Err(e)).await;
                return rx;
            }
        };
        let key_prefix = path.trim_matches('/').to_string();
        let start_after = start_after.map(|s| s.to_string());
        let limit = max_batch_size.filter(|n| *n > 0).unwrap_or(usize::MAX);

        tokio::spawn(async move {
            let listed = tokio::task::spawn_blocking(move || {
                let mut out = Vec::new();
                match collect_entries(&dir, &key_prefix, recursive, &mut out) {
                    Ok(()) => Ok(out),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(out),
                    Err(e) => Err(e),
                }
            })
            .await;
            let mut entries = match listed {
                Ok(Ok(v)) => v,
                Ok(Err(e)) => {
                    let _ = tx.send(Err(e.into())).await;
                    return;
                }
                Err(e) => {
                    let _ = tx.send(Err(StorageError::Other(e.to_string()))).await;
                    return;
                }
            };
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            let mut batch = Vec::new();
            for (key, md) in entries {
                if let Some(after) = &start_after
                    && key.as_str() <= after.as_str()
                {
                    continue;
                }
                batch.push(soi_for(&key, &md));
                if batch.len() >= limit && tx.send(Ok(std::mem::take(&mut batch))).await.is_err() {
                    return;
                }
            }
            if !batch.is_empty() {
                let _ = tx.send(Ok(batch)).await;
            }
        });
        rx
    }

    async fn get_attrs(&self, key: &str) -> Result<StorageObjectInfo, StorageError> {
        let path = self.path_for(key)?;
        match fs::metadata(&path).await {
            Ok(md) => Ok(soi_for(key, &md)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Err(StorageError::NoSuchKey(key.to_string())),
            Err(e) => Err(e.into()),
        }
    }

    fn data_source_id(&self) -> Option<&str> {
        self.data_source_id.as_deref()
    }

    fn set_data_source_id(&mut self, id: String) {
        self.data_source_id = Some(id);
    }
}

#[async_trait]
impl StorageSource for FsObjectStore {
    fn source_name(&self) -> &str {
        "FsObjectStore"
    }

    fn source_type(&self) -> StorageSourceType {
        StorageSourceType::FileSystem
    }

    async fn get_streamed(
        &self,
        key: &str,
        offset: Option<u64>,
        length: Option<u64>,
    ) -> mpsc::Receiver<Result<Vec<u8>, StorageError>> {
        let (tx, rx) = mpsc::channel(4);
        let path = match self.path_for(key) {
            Ok(p) => p,
            Err(e) => {
                let _ = tx.send(Err(e)).await;
                return rx;
            }
        };
        let key = key.to_string();
        tokio::spawn(async move {
            let mut file = match fs::File::open(&path).await {
                Ok(f) => f,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    let _ = tx.send(Err(StorageError::NoSuchKey(key))).await;
                    return;
                }
                Err(e) => {
                    let _ = tx.send(Err(e.into())).await;
                    return;
                }
            };
            if let Some(off) = offset
                && let Err(e) = file.seek(SeekFrom::Start(off)).await
            {
                let _ = tx.send(Err(e.into())).await;
                return;
            }
            let mut remaining = length;
            loop {
                let want = match remaining {
                    Some(0) => break,
                    Some(r) => (r as usize).min(READ_CHUNK),
                    None => READ_CHUNK,
                };
                let mut buf = vec![0u8; want];
                let n = match file.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => n,
                    Err(e) => {
                        let _ = tx.send(Err(e.into())).await;
                        return;
                    }
                };
                buf.truncate(n);
                if let Some(r) = remaining.as_mut() {
                    *r -= n as u64;
                }
                if tx.send(Ok(buf)).await.is_err() {
                    break;
                }
            }
        });
        rx
    }

    async fn get_symlink(&self, key: &str) -> Result<String, StorageError> {
        Err(StorageError::NotImplemented(format!(
            "object store holds no symlinks: {key}"
        )))
    }
}

#[async_trait]
impl StorageDest for FsObjectStore {
    fn is_sequential(&self) -> bool {
        false
    }

    async fn put_file(
        &self,
        key: &str,
        data: &[u8],
        _attrs: Option<&StorageFileAttrs>,
        conditions: Option<&StoragePutConditions>,
    ) -> Result<StoragePutResult, StorageError> {
        let path = self.path_for(key)?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }

        match conditions {
            Some(c) if c.if_none_match => {
                // Create-new: write a temp sibling, then link(2) it to the final
                // name — EEXIST if the key exists, and a crash never leaves a
                // partial object under the key.
                let tmp = temp_sibling(&path);
                fs::write(&tmp, data).await?;
                let linked = fs::hard_link(&tmp, &path).await;
                let _ = fs::remove_file(&tmp).await;
                match linked {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                        return Err(StorageError::PreconditionFailed(format!("{key} exists")));
                    }
                    Err(e) => return Err(e.into()),
                }
            }
            Some(c) if c.if_match.is_some() => {
                let expected = c.if_match.as_deref().unwrap_or_default();
                let _guard = self.cas_lock.lock().await;
                let current = match fs::metadata(&path).await {
                    Ok(md) => fs_etag(&md),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        return Err(StorageError::PreconditionFailed(format!("{key} missing")));
                    }
                    Err(e) => return Err(e.into()),
                };
                if current != expected {
                    return Err(StorageError::PreconditionFailed(format!("{key} etag moved")));
                }
                let tmp = temp_sibling(&path);
                fs::write(&tmp, data).await?;
                if let Err(e) = fs::rename(&tmp, &path).await {
                    let _ = fs::remove_file(&tmp).await;
                    return Err(e.into());
                }
            }
            _ => {
                // Unconditional: still atomic (temp + rename) so a reader never
                // observes a half-written object.
                let tmp = temp_sibling(&path);
                fs::write(&tmp, data).await?;
                if let Err(e) = fs::rename(&tmp, &path).await {
                    let _ = fs::remove_file(&tmp).await;
                    return Err(e.into());
                }
            }
        }

        let etag = Some(fs_etag(&fs::metadata(&path).await?));
        Ok(StoragePutResult {
            etag,
            crc32: None,
            sha256: None,
            storage_key: key.to_string(),
        })
    }

    async fn put_dir(&self, key: &str, _attrs: Option<&StorageFileAttrs>) -> Result<StoragePutResult, StorageError> {
        let path = self.path_for(key)?;
        fs::create_dir_all(&path).await?;
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
        _target: &str,
        _attrs: Option<&StorageFileAttrs>,
    ) -> Result<StoragePutResult, StorageError> {
        Err(StorageError::NotImplemented(format!(
            "object store holds no symlinks: {key}"
        )))
    }

    async fn del_file(&self, key: &str) -> Result<(), StorageError> {
        let path = self.path_for(key)?;
        match fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            // Idempotent: GC may retry a delete that already happened.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    async fn del_dir(&self, key: &str) -> Result<(), StorageError> {
        let path = self.path_for(key)?;
        match fs::remove_dir_all(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    async fn update_stats(&self, _stats: &StorageStatInfo) -> Result<(), StorageError> {
        Ok(())
    }

    async fn close(&self) -> Result<(), StorageError> {
        Ok(())
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
            p.push(format!("fsobj_test_{}_{}", std::process::id(), n));
            std::fs::create_dir_all(&p).unwrap();
            TmpDir(p)
        }
    }
    impl Drop for TmpDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    async fn drain(mut rx: mpsc::Receiver<Result<Vec<StorageObjectInfo>, StorageError>>) -> Vec<StorageObjectInfo> {
        let mut out = Vec::new();
        while let Some(b) = rx.recv().await {
            out.extend(b.unwrap());
        }
        out
    }

    #[tokio::test]
    async fn if_none_match_creates_once() {
        let tmp = TmpDir::new();
        let store = FsObjectStore::open(tmp.0.join("v3")).await.unwrap();
        let cond = StoragePutConditions::if_none_match();

        let r = store
            .put_file("wal/wal-000000000001", b"first", None, Some(&cond))
            .await
            .unwrap();
        assert!(r.etag.is_some());

        let e = store
            .put_file("wal/wal-000000000001", b"second", None, Some(&cond))
            .await
            .unwrap_err();
        assert!(matches!(e, StorageError::PreconditionFailed(_)), "{e:?}");
        assert_eq!(std::fs::read(tmp.0.join("v3/wal/wal-000000000001")).unwrap(), b"first");

        let leftovers: Vec<_> = std::fs::read_dir(tmp.0.join("v3/wal"))
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|n| n.ends_with(".tmp"))
            .collect();
        assert!(leftovers.is_empty(), "{leftovers:?}");
    }

    #[tokio::test]
    async fn if_match_is_compare_and_swap() {
        let tmp = TmpDir::new();
        let store = FsObjectStore::open(tmp.0.join("v3")).await.unwrap();

        let e = store
            .put_file("uberblock", b"x", None, Some(&StoragePutConditions::if_match("0-0-0")))
            .await
            .unwrap_err();
        assert!(matches!(e, StorageError::PreconditionFailed(_)));

        let etag0 = store
            .put_file("uberblock", b"v0", None, None)
            .await
            .unwrap()
            .etag
            .unwrap();
        assert_eq!(
            store.get_attrs("uberblock").await.unwrap().etag.as_deref(),
            Some(etag0.as_str())
        );

        let etag1 = store
            .put_file("uberblock", b"v1", None, Some(&StoragePutConditions::if_match(&etag0)))
            .await
            .unwrap()
            .etag
            .unwrap();
        assert_ne!(etag0, etag1);
        assert_eq!(std::fs::read(tmp.0.join("v3/uberblock")).unwrap(), b"v1");

        let e = store
            .put_file("uberblock", b"v2", None, Some(&StoragePutConditions::if_match(&etag0)))
            .await
            .unwrap_err();
        assert!(matches!(e, StorageError::PreconditionFailed(_)));
        assert_eq!(std::fs::read(tmp.0.join("v3/uberblock")).unwrap(), b"v1");
        assert_eq!(store.get_attrs("uberblock").await.unwrap().etag.unwrap(), etag1);
    }

    #[tokio::test]
    async fn list_is_sorted_with_exclusive_cursor() {
        let tmp = TmpDir::new();
        let store = FsObjectStore::open(tmp.0.join("v3")).await.unwrap();
        for k in [
            "wal/wal-000000000003",
            "wal/wal-000000000001",
            "wal/wal-000000000010",
            "wal/wal-000000000002",
        ] {
            store.put_file(k, b"x", None, None).await.unwrap();
        }
        store.put_file("blobs/a", b"x", None, None).await.unwrap();

        let keys: Vec<_> = drain(store.get_dir("wal", false, None, None).await)
            .await
            .into_iter()
            .map(|s| s.key)
            .collect();
        assert_eq!(
            keys,
            [
                "wal/wal-000000000001",
                "wal/wal-000000000002",
                "wal/wal-000000000003",
                "wal/wal-000000000010"
            ]
        );

        let keys: Vec<_> = drain(store.get_dir("wal", false, Some("wal/wal-000000000002"), Some(1)).await)
            .await
            .into_iter()
            .map(|s| s.key)
            .collect();
        assert_eq!(keys, ["wal/wal-000000000003", "wal/wal-000000000010"]);

        let all: Vec<_> = drain(store.get_dir("", true, None, None).await)
            .await
            .into_iter()
            .map(|s| s.key)
            .collect();
        assert_eq!(all[0], "blobs/a");
        assert_eq!(all.len(), 5);

        let empty = drain(store.get_dir("nope", false, None, None).await).await;
        assert!(empty.is_empty());
    }

    #[tokio::test]
    async fn get_head_delete_roundtrip() {
        let tmp = TmpDir::new();
        let store = FsObjectStore::open(tmp.0.join("v3")).await.unwrap();
        store.put_file("blobs/b", b"0123456789", None, None).await.unwrap();

        let mut rx = store.get_streamed("blobs/b", Some(2), Some(5)).await;
        let mut got = Vec::new();
        while let Some(c) = rx.recv().await {
            got.extend(c.unwrap());
        }
        assert_eq!(got, b"23456");

        let soi = store.get_attrs("blobs/b").await.unwrap();
        assert_eq!(soi.size, 10);
        assert!(soi.etag.is_some());

        store.del_file("blobs/b").await.unwrap();
        store.del_file("blobs/b").await.unwrap(); // idempotent
        assert!(matches!(
            store.get_attrs("blobs/b").await,
            Err(StorageError::NoSuchKey(_))
        ));
        let mut rx = store.get_streamed("blobs/b", None, None).await;
        assert!(matches!(rx.recv().await, Some(Err(StorageError::NoSuchKey(_)))));
    }

    #[tokio::test]
    async fn rejects_escaping_keys() {
        let tmp = TmpDir::new();
        let store = FsObjectStore::open(tmp.0.join("v3")).await.unwrap();
        for k in ["../x", "/abs", "a/../b", "", "a//b", "a/", "a\\b"] {
            assert!(store.put_file(k, b"x", None, None).await.is_err(), "{k:?} accepted");
        }
    }
}
