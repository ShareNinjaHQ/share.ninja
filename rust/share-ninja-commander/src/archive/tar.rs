//! A plain tar archive as a read-only storage. `connect` walks the headers
//! once through a [`StorageReader`] — the `tar` crate seeks past every
//! entry's data, so the walk reads 512 bytes per entry, not the archive —
//! and every entry's bytes sit contiguously at a known offset, so a read is a
//! direct range on the containing storage: nothing is decompressed or copied.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{RwLock, mpsc};

use share_ninja_core::transfers::{SearchOptions, SearchResult, StorageSearchQuery, StorageSourceType};
use share_ninja_core::{Storage, StorageError, StorageFileType, StorageMetaState, StorageObjectInfo, StorageSource};

use crate::storage_reader::StorageReader;

use super::{ArchiveIndex, clean_key};

pub struct TarSource {
    /// The storage the archive file lives on, and its key and size there.
    base: Arc<RwLock<dyn StorageSource>>,
    key: String,
    size: u64,
    index: ArchiveIndex,
    /// Where each regular file's bytes start in the archive, by key.
    data: HashMap<String, u64>,
    /// Symlink targets by key.
    links: HashMap<String, String>,
    data_source_id: Option<String>,
}

/// What the header walk yields.
struct Walked {
    items: Vec<StorageObjectInfo>,
    data: HashMap<String, u64>,
    links: HashMap<String, String>,
}

/// The header walk, on a blocking thread: one `StorageObjectInfo` per
/// regular file / directory / symlink. A hard link is the file it names
/// (same bytes); device nodes, fifos and GNU sparse entries (whose stored
/// bytes are not the file) are skipped.
fn walk(reader: StorageReader) -> Result<Walked, StorageError> {
    let tar_err = |e: std::io::Error| StorageError::Other(format!("tar: {e}"));
    let mut archive = tar::Archive::new(reader);
    let mut items: Vec<StorageObjectInfo> = Vec::new();
    let mut data: HashMap<String, u64> = HashMap::new();
    let mut links: HashMap<String, String> = HashMap::new();
    // (offset, size) of every regular file seen so far, for hard links.
    let mut files: HashMap<String, (u64, u64)> = HashMap::new();
    for entry in archive.entries_with_seek().map_err(tar_err)? {
        let entry = entry.map_err(tar_err)?;
        let header = entry.header();
        let kind = header.entry_type();
        let name = String::from_utf8_lossy(&entry.path_bytes()).into_owned();
        let Some(key) = clean_key(&name) else {
            tracing::warn!(name = %name, "tar: entry skipped (bad name)");
            continue;
        };
        let mut soi = StorageObjectInfo::new(&key);
        soi.attrs.mtime = header
            .mtime()
            .ok()
            .and_then(|t| i64::try_from(t).ok())
            .and_then(|t| chrono::DateTime::from_timestamp(t, 0));
        soi.attrs.posix_permissions = header.mode().ok().map(i64::from);
        if kind.is_dir() {
            soi.file_type = StorageFileType::Dir;
        } else if kind.is_symlink() {
            let target = entry
                .link_name_bytes()
                .map(|b| String::from_utf8_lossy(&b).into_owned())
                .unwrap_or_default();
            soi.file_type = StorageFileType::Symlink;
            soi.size = target.len() as u64;
            links.insert(key, target);
        } else if kind.is_hard_link() {
            let target = entry
                .link_name_bytes()
                .map(|b| String::from_utf8_lossy(&b).into_owned())
                .and_then(|t| clean_key(&t));
            let Some((offset, size)) = target.as_deref().and_then(|t| files.get(t).copied()) else {
                tracing::warn!(name = %name, "tar: hard link to an unknown entry skipped");
                continue;
            };
            soi.file_type = StorageFileType::Regular;
            soi.size = size;
            data.insert(key, offset);
        } else if kind.is_gnu_sparse() {
            tracing::warn!(name = %name, "tar: sparse entry skipped");
            continue;
        } else if kind.is_file() || kind.is_contiguous() {
            let offset = entry.raw_file_position();
            soi.file_type = StorageFileType::Regular;
            soi.size = entry.size();
            files.insert(key.clone(), (offset, soi.size));
            data.insert(key, offset);
        } else {
            continue;
        }
        items.push(soi);
    }
    Ok(Walked { items, data, links })
}

impl TarSource {
    pub fn new(base: Arc<RwLock<dyn StorageSource>>, key: String, size: u64) -> Self {
        Self {
            base,
            key,
            size,
            index: ArchiveIndex::default(),
            data: HashMap::new(),
            links: HashMap::new(),
            data_source_id: None,
        }
    }
}

#[async_trait]
impl Storage for TarSource {
    async fn connect(&mut self) -> Result<StorageMetaState, StorageError> {
        let reader = StorageReader::new(
            self.base.clone(),
            self.key.clone(),
            self.size,
            tokio::runtime::Handle::current(),
        );
        let walked = tokio::task::spawn_blocking(move || walk(reader))
            .await
            .map_err(|e| StorageError::Other(format!("tar walk task: {e}")))??;
        self.index = ArchiveIndex::new(walked.items);
        self.data = walked.data;
        self.links = walked.links;
        Ok(StorageMetaState::Complete)
    }

    async fn cancel(&self) -> Result<(), StorageError> {
        Ok(())
    }

    async fn get_attrs(&self, path: &str) -> Result<StorageObjectInfo, StorageError> {
        if path.is_empty() {
            let mut soi = StorageObjectInfo::new("");
            soi.file_type = StorageFileType::Dir;
            return Ok(soi);
        }
        self.index
            .get(path)
            .cloned()
            .ok_or_else(|| StorageError::NoSuchKey(path.to_string()))
    }

    async fn get_dir(
        &self,
        path: &str,
        recursive: bool,
        start_after: Option<&str>,
        max_batch_size: Option<usize>,
    ) -> mpsc::Receiver<Result<Vec<StorageObjectInfo>, StorageError>> {
        if start_after.is_some() {
            // The index is in archive order, not key order.
            let (tx, rx) = mpsc::channel(1);
            tokio::spawn(async move {
                let _ = tx
                    .send(Err(StorageError::NotImplemented("tar getDir start_after".into())))
                    .await;
            });
            return rx;
        }
        self.index.get_dir(path, recursive, max_batch_size)
    }

    async fn search(&self, query: &StorageSearchQuery, options: &SearchOptions) -> Result<SearchResult, StorageError> {
        self.index.search(query, options)
    }

    fn data_source_id(&self) -> Option<&str> {
        self.data_source_id.as_deref()
    }

    fn set_data_source_id(&mut self, id: String) {
        self.data_source_id = Some(id);
    }
}

#[async_trait]
impl StorageSource for TarSource {
    fn source_name(&self) -> &str {
        "Tar"
    }

    fn source_type(&self) -> StorageSourceType {
        StorageSourceType::FileSystem
    }

    /// `[offset, offset+length)` of the entry = that range of the archive,
    /// shifted to where the entry's bytes start: the base storage's own
    /// stream, passed through.
    async fn get_streamed(
        &self,
        object: &str,
        offset: Option<u64>,
        length: Option<u64>,
    ) -> mpsc::Receiver<Result<Vec<u8>, StorageError>> {
        let entry = self
            .index
            .get(object)
            .filter(|s| s.file_type == StorageFileType::Regular);
        let Some((entry, start)) = entry.zip(self.data.get(object)) else {
            let (tx, rx) = mpsc::channel(1);
            let _ = tx.send(Err(StorageError::NoSuchKey(object.to_string()))).await;
            return rx;
        };
        let off = offset.unwrap_or(0).min(entry.size);
        let want = length.map(|l| l.min(entry.size - off)).unwrap_or(entry.size - off);
        self.base
            .read()
            .await
            .get_streamed(&self.key, Some(start + off), Some(want))
            .await
    }

    async fn get_symlink(&self, object: &str) -> Result<String, StorageError> {
        self.links
            .get(object)
            .cloned()
            .ok_or_else(|| StorageError::NoSuchKey(object.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;

    use tokio::sync::RwLock;

    use share_ninja_core::fs::LocalFs;
    use share_ninja_core::transfers::{SearchOptions, StorageSearchQuery};
    use share_ninja_core::{Storage, StorageFileType, StorageSource};

    /// A tree with the tar cases that matter — a long (PAX) name, a
    /// symlink, a hard link, a nested file bigger than a reader block, an
    /// empty directory — packed by bsdtar into `t.tar` beside it.
    fn build_tar() -> (PathBuf, Vec<u8>) {
        let tmp = std::env::temp_dir().join(format!(
            "sninja-tar-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let src = tmp.join("src");
        std::fs::create_dir_all(src.join("sub/deep")).unwrap();
        std::fs::create_dir_all(src.join("empty")).unwrap();
        std::fs::write(src.join("a.txt"), b"hello").unwrap();
        let big: Vec<u8> = (0..1_500_000u32).map(|i| (i % 251) as u8).collect();
        std::fs::write(src.join("sub/deep/b.bin"), &big).unwrap();
        let long = format!("sub/{}.txt", "n".repeat(150));
        std::fs::write(src.join(&long), b"long").unwrap();
        std::os::unix::fs::symlink("a.txt", src.join("link")).unwrap();
        std::fs::hard_link(src.join("a.txt"), src.join("hard")).unwrap();
        // No AppleDouble `._*` sidecars for the xattrs macOS files carry.
        let status = std::process::Command::new("bsdtar")
            .env("COPYFILE_DISABLE", "1")
            .arg("-cf")
            .arg(tmp.join("t.tar"))
            .arg("-C")
            .arg(&src)
            .arg(".")
            .status()
            .expect("bsdtar");
        assert!(status.success());
        (tmp, big)
    }

    async fn drain(mut rx: tokio::sync::mpsc::Receiver<Result<Vec<u8>, share_ninja_core::StorageError>>) -> Vec<u8> {
        let mut out = Vec::new();
        while let Some(chunk) = rx.recv().await {
            out.extend_from_slice(&chunk.unwrap());
        }
        out
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn tar_over_local_fs() {
        let (tmp, big) = build_tar();
        let mut fs = LocalFs::new(&tmp);
        fs.connect().await.unwrap();
        let base: Arc<RwLock<dyn StorageSource>> = Arc::new(RwLock::new(fs));
        let size = base.read().await.get_attrs("t.tar").await.unwrap().size;
        let source = super::super::open(base, "t.tar".to_string(), size).await.unwrap();

        // The root level, names as a panel shows them.
        let mut rx = source.write().await.get_dir("", false, None, Some(2)).await;
        let mut names: Vec<(String, StorageFileType)> = Vec::new();
        while let Some(batch) = rx.recv().await {
            names.extend(batch.unwrap().into_iter().map(|s| (s.key, s.file_type)));
        }
        names.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            names,
            vec![
                ("a.txt".to_string(), StorageFileType::Regular),
                ("empty".to_string(), StorageFileType::Dir),
                ("hard".to_string(), StorageFileType::Regular),
                ("link".to_string(), StorageFileType::Symlink),
                ("sub".to_string(), StorageFileType::Dir),
            ]
        );

        let src = source.read().await;
        assert_eq!(src.get_attrs("sub/deep/b.bin").await.unwrap().size, big.len() as u64);
        assert!(
            src.get_attrs(&format!("sub/{}.txt", "n".repeat(150))).await.is_ok(),
            "PAX long name"
        );
        assert!(matches!(
            src.get_attrs("nope").await,
            Err(share_ninja_core::StorageError::NoSuchKey(_))
        ));
        assert_eq!(drain(src.get_streamed("a.txt", None, None).await).await, b"hello");
        assert_eq!(
            drain(src.get_streamed("hard", None, None).await).await,
            b"hello",
            "hard link = the file"
        );
        assert_eq!(
            drain(src.get_streamed("sub/deep/b.bin", Some(700_000), Some(100_000)).await).await,
            &big[700_000..800_000]
        );
        assert_eq!(
            drain(
                src.get_streamed("sub/deep/b.bin", Some(1_400_000), Some(1_000_000))
                    .await
            )
            .await,
            &big[1_400_000..]
        );
        assert_eq!(src.get_symlink("link").await.unwrap(), "a.txt");

        let found = src
            .search(
                &StorageSearchQuery {
                    name_match: Some("B.BIN".to_string()),
                },
                &SearchOptions {
                    path_scope: Some("sub".to_string()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(found.items.len(), 1);
        assert_eq!(found.items[0].key, "sub/deep/b.bin");
        drop(src);
        std::fs::remove_dir_all(&tmp).unwrap();
    }
}
