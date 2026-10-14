//! Archives browsed in place as read-only storages: a `.tar`, `.zip` or
//! `.iso` on an open storage (a local root or a share) becomes a storage of its own —
//! its entries listed from an index built once at connect, an entry's bytes
//! read straight off the containing storage's ranges (inflated on the way
//! for a deflated zip entry). Nothing is extracted or copied, and a share's
//! archive is never downloaded whole. [`open`] is the one entry point; the
//! panels tell an archive file by [`is_archive`].

pub mod iso;
pub mod tar;
pub mod zip;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tokio::sync::{RwLock, mpsc};

use share_ninja_core::transfers::{SearchOptions, SearchResult, StorageSearchQuery, search_entry_wire_estimate};
use share_ninja_core::{Storage, StorageError, StorageFileType, StorageObjectInfo, StorageSource};

use self::iso::IsoSource;
use self::tar::TarSource;
use self::zip::ZipSource;

enum Format {
    Tar,
    Zip,
    Iso,
}

/// Compressed tars (`.tgz`, `.tar.xz`, …) are deliberately not here: gzip
/// and xz have no random access, so an entry read would decompress the
/// archive from its start.
fn format_of(name: &str) -> Option<Format> {
    let ext = name.rsplit_once('.')?.1.to_ascii_lowercase();
    match ext.as_str() {
        "tar" => Some(Format::Tar),
        "zip" => Some(Format::Zip),
        "iso" => Some(Format::Iso),
        _ => None,
    }
}

/// Whether Enter on `name` opens it as an archive.
pub fn is_archive(name: &str) -> bool {
    format_of(name).is_some()
}

/// Open the archive at `key` (of `size` bytes) on `base` as a connected
/// read-only source.
pub async fn open(
    base: Arc<RwLock<dyn StorageSource>>,
    key: String,
    size: u64,
) -> Result<Arc<RwLock<dyn StorageSource>>, String> {
    match format_of(&key) {
        Some(Format::Tar) => {
            let mut source = TarSource::new(base, key, size);
            source.connect().await.map_err(|e| e.to_string())?;
            Ok(Arc::new(RwLock::new(source)))
        }
        Some(Format::Zip) => {
            let mut source = ZipSource::new(base, key, size);
            source.connect().await.map_err(|e| e.to_string())?;
            Ok(Arc::new(RwLock::new(source)))
        }
        Some(Format::Iso) => {
            let mut source = IsoSource::new(base, key, size);
            source.connect().await.map_err(|e| e.to_string())?;
            Ok(Arc::new(RwLock::new(source)))
        }
        None => Err(format!("{key}: not an archive")),
    }
}

/// An entry name as the archive stores it → a storage key: `./` and `/`
/// prefixes and a trailing `/` go; `None` for the archive root itself and
/// for names with a `.`/`..` component (they would escape a copy's
/// destination root).
fn clean_key(name: &str) -> Option<String> {
    let mut rest = name;
    loop {
        if let Some(r) = rest.strip_prefix("./") {
            rest = r;
        } else if let Some(r) = rest.strip_prefix('/') {
            rest = r;
        } else {
            break;
        }
    }
    let rest = rest.trim_end_matches('/');
    if rest.is_empty() || rest.split('/').any(|c| c == "." || c == "..") {
        return None;
    }
    Some(rest.to_string())
}

/// An archive's entries, keyed like any storage (`dir/sub/file`), plus the
/// parent directories a format leaves implicit (tar and zip both may).
/// Built once at connect; every listing call answers from it — the storage
/// trait methods a [`TarSource`] / [`ZipSource`] / [`IsoSource`] delegate here.
#[derive(Default)]
pub struct ArchiveIndex {
    /// Shared with the `get_dir` producers, which stream batches off it.
    items: Arc<[StorageObjectInfo]>,
    by_key: HashMap<String, usize>,
}

impl ArchiveIndex {
    /// `items` in archive order; a key listed twice keeps its first entry.
    pub fn new(items: Vec<StorageObjectInfo>) -> Self {
        let mut keys: HashSet<String> = HashSet::with_capacity(items.len());
        let mut kept: Vec<StorageObjectInfo> = Vec::with_capacity(items.len());
        for soi in items {
            if keys.insert(soi.key.clone()) {
                kept.push(soi);
            }
        }
        let mut parents: Vec<StorageObjectInfo> = Vec::new();
        for soi in &kept {
            let mut end = soi.key.len();
            while let Some(pos) = soi.key[..end].rfind('/') {
                let dir = &soi.key[..pos];
                if keys.insert(dir.to_string()) {
                    let mut d = StorageObjectInfo::new(dir);
                    d.file_type = StorageFileType::Dir;
                    parents.push(d);
                }
                end = pos;
            }
        }
        kept.extend(parents);
        let by_key = kept.iter().enumerate().map(|(i, s)| (s.key.clone(), i)).collect();
        Self {
            items: kept.into(),
            by_key,
        }
    }

    pub fn get(&self, key: &str) -> Option<&StorageObjectInfo> {
        self.by_key.get(key).map(|&i| &self.items[i])
    }

    /// The entries under `path` (`""` = the root), streamed in batches of
    /// `max_batch_size` (`None` = one batch); dropping the receiver ends it.
    pub fn get_dir(
        &self,
        path: &str,
        recursive: bool,
        max_batch_size: Option<usize>,
    ) -> mpsc::Receiver<Result<Vec<StorageObjectInfo>, StorageError>> {
        let prefix = dir_prefix(path);
        let limit = max_batch_size.filter(|n| *n > 0).unwrap_or(usize::MAX);
        let items = self.items.clone();
        let (tx, rx) = mpsc::channel(4);
        tokio::spawn(async move {
            let mut batch: Vec<StorageObjectInfo> = Vec::new();
            for soi in items.iter() {
                let Some(sub) = soi.key.strip_prefix(prefix.as_str()) else {
                    continue;
                };
                if sub.is_empty() || (!recursive && sub.contains('/')) {
                    continue;
                }
                batch.push(soi.clone());
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

    /// Name search over the index, paged like `StorageMeta::search`: the
    /// page token is the offset of the next match.
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
        let prefix = dir_prefix(options.path_scope.as_deref().unwrap_or(""));
        let needle = query
            .name_match
            .as_ref()
            .filter(|s| !s.is_empty())
            .map(|s| s.to_lowercase());

        let matches = self
            .items
            .iter()
            .filter(|soi| {
                let Some(sub) = soi.key.strip_prefix(prefix.as_str()) else {
                    return false;
                };
                if sub.is_empty() || (!recursive && sub.contains('/')) {
                    return false;
                }
                match &needle {
                    Some(n) => {
                        let name = soi.key.rsplit('/').next().unwrap_or(&soi.key);
                        name.to_lowercase().contains(n.as_str())
                    }
                    None => true,
                }
            })
            .skip(offset);
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
}

/// `""` for the root, else `path/`.
fn dir_prefix(path: &str) -> String {
    let path = path.trim_matches('/');
    if path.is_empty() {
        String::new()
    } else {
        format!("{path}/")
    }
}
