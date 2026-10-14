//! Per-download persistent metadata store for `FsDest` — the Rust analog of the
//! Electron `MetadataLevel` (the LevelDB store) plus the virtual→real path
//! mapping / collision half of `FSStorageMeta.pathForWriteInt`.
//!
//! State is a single small JSON document (`<dir>/multiparts.json`) holding only
//! the *in-flight* multipart uploads, the top-level `roots` this download has
//! written and the `renames` (virtual path → on-disk name, where the two
//! differ), mirrored by an in-memory cache behind one mutex. Every change —
//! membership, a new root or rename, each landed part — flushes synchronously
//! and atomically (tmp + rename + fsync), so what the file says is what is on
//! disk at any moment: a pause or a crash at any point resumes from the last
//! landed part. See `docs/app/RustFSImps.md` and `docs/app/naming-fix.md`.
//!
//! The document lives only while the download is unfinished. Once the whole
//! share landed the record carries `roots` + `renames` and the dir is removed;
//! a store over such a download is built from those alone
//! ([`FsMetaStore::landed`]) and refuses to write.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

use crate::StorageError;
use crate::transfers::StorageFileAttrs;

/// Top-level collision policy. Governs the resolution of the **first** path
/// component only; nested collisions are always a hard error (see
/// [`FsMetaStore::resolve_path`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CollisionPolicy {
    /// First-component target must not already exist on disk, else error.
    Strict,
    /// First-component target exists → rename via `generate_new_name`
    /// (`"Foo 1"`, `"Foo 2"`, …), keeping the existing file.
    Avoid,
}

/// The names the destination platform's filesystem refuses, and how a
/// virtual path component is respelled on disk to get past that
/// (`docs/app/naming-fix.md`). Per platform, not per filesystem: an exFAT
/// volume on macOS is not covered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NameRule {
    /// macOS, iOS, Linux: nothing is rewritten.
    Posix,
    /// Android's public Downloads collection is served by MediaProvider,
    /// which enforces the FAT character set: control characters
    /// (`0x00–0x1F`, `0x7F`) and `" * : < > ? \ |` each become `_`.
    Fat,
    /// The `Fat` set, plus Win32's reserved device basenames (`CON`, `PRN`,
    /// `AUX`, `NUL`, `COM1–9`, `LPT1–9`, case-insensitive, judged on the
    /// part before the first dot) get `_` appended to that basename, and
    /// every trailing dot or space becomes `_` (Win32 would strip them, so
    /// the name written would not be the name read back).
    Windows,
}

impl NameRule {
    /// The rule of the platform this binary runs on.
    pub fn platform() -> Self {
        #[cfg(target_os = "android")]
        {
            NameRule::Fat
        }
        #[cfg(windows)]
        {
            NameRule::Windows
        }
        #[cfg(not(any(target_os = "android", windows)))]
        {
            NameRule::Posix
        }
    }

    /// The on-disk spelling of one virtual path component.
    pub fn apply(self, name: &str) -> String {
        match self {
            NameRule::Posix => name.to_string(),
            NameRule::Fat => fat_chars(name),
            NameRule::Windows => windows_name(&fat_chars(name)),
        }
    }
}

/// Replace every character outside the FAT set with `_`.
fn fat_chars(name: &str) -> String {
    name.chars()
        .map(|c| {
            if (c as u32) < 0x20 || c == '\x7f' || matches!(c, '"' | '*' | ':' | '<' | '>' | '?' | '\\' | '|') {
                '_'
            } else {
                c
            }
        })
        .collect()
}

/// Win32's extra rules over a name already in the FAT set.
fn windows_name(name: &str) -> String {
    const RESERVED: [&str; 22] = [
        "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9", "LPT1",
        "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
    ];
    let (stem, rest) = match name.find('.') {
        Some(i) => (&name[..i], &name[i..]),
        None => (name, ""),
    };
    // Win32 judges the device name with trailing spaces and dots already
    // stripped: `COM1 .txt` is `COM1`.
    let device = stem.trim_end_matches(['.', ' ']);
    let mut out = if RESERVED.iter().any(|r| r.eq_ignore_ascii_case(device)) {
        format!("{stem}_{rest}")
    } else {
        name.to_string()
    };
    let kept = out.trim_end_matches(['.', ' ']).len();
    let trailing = out.len() - kept;
    out.truncate(kept);
    out.extend(std::iter::repeat_n('_', trailing));
    out
}

/// What kind of entry `resolve_path` is resolving a destination for (Rust port
/// of Electron's `"f"|"d"|"s"` arg to `pathForWriteInt`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteKind {
    File,
    Dir,
    Symlink,
}

/// On-disk entry type for a tracked object (only `File` ever occurs today —
/// multipart records are always files — but kept for fidelity with Electron).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FsEntryType {
    #[serde(rename = "f")]
    File,
    #[serde(rename = "s")]
    Symlink,
    #[serde(rename = "d")]
    Dir,
}

/// Serializable subset of `StorageFileAttrs` we persist (the only fields
/// `apply_attrs` restores). Mirrors Electron's `FSMetaInfo.meta`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FsAttrs {
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub mtime: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub posix_permissions: Option<i64>,
}

impl FsAttrs {
    pub fn from_storage(a: &StorageFileAttrs) -> Self {
        Self {
            mtime: a.mtime,
            posix_permissions: a.posix_permissions,
        }
    }

    pub fn to_storage(&self) -> StorageFileAttrs {
        StorageFileAttrs {
            mtime: self.mtime,
            posix_permissions: self.posix_permissions,
            ..Default::default()
        }
    }
}

/// One recorded part (Rust port of `FSPartInfo`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FsPartInfo {
    pub part_number: u32,
    pub offset: u64,
    pub size: u64,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub etag: Option<String>,
}

/// One in-progress object (Rust port of `FSMetaInfo`). Presence in `objects`
/// == "incomplete multipart"; completion removes the key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FsMetaInfo {
    pub ftype: FsEntryType,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub attrs: Option<FsAttrs>,
    #[serde(default)]
    pub parts: Vec<FsPartInfo>,
}

/// The whole serialized document (`multiparts.json`).
#[derive(Debug, Default, Serialize, Deserialize)]
struct MetaDoc {
    version: u32,
    #[serde(default)]
    roots: Vec<String>,
    #[serde(default)]
    renames: HashMap<String, String>,
    /// Written by documents before `roots` + `renames` (every root, renamed
    /// or not). Accepted on load, never written again.
    #[serde(default, skip_serializing)]
    path_map: HashMap<String, String>,
    #[serde(default)]
    objects: HashMap<String, FsMetaInfo>,
}

/// `roots` + `renames` of a legacy `path_map` (virtual first component →
/// on-disk name, filled for every root): the keys, sorted for a stable
/// order, and the pairs whose two sides differ.
pub fn split_legacy_path_map(path_map: HashMap<String, String>) -> (Vec<String>, HashMap<String, String>) {
    let mut roots: Vec<String> = path_map.keys().cloned().collect();
    roots.sort();
    let renames = path_map.into_iter().filter(|(k, v)| k != v).collect();
    (roots, renames)
}

struct MetaInner {
    /// Live (incomplete) objects, keyed by virtual object key.
    objects: HashMap<String, FsMetaInfo>,
    /// The top-level virtual names this download has written — the set of
    /// what it owns under the dest base (sorted; a `Vec` on disk and on the
    /// record).
    roots: BTreeSet<String>,
    /// Full virtual path of a component, any level → its on-disk name, only
    /// where the two differ: a collision rename (`Foo` → `Foo 1`) or a
    /// name-rule rewrite (`a:b.jpg` → `a_b.jpg`). Names, not paths: joined
    /// under the dest base the callers pass in, so the map stays valid when
    /// the platform moves the app container.
    renames: HashMap<String, String>,
}

impl MetaInner {
    fn to_doc(&self) -> MetaDoc {
        MetaDoc {
            version: 1,
            roots: self.roots.iter().cloned().collect(),
            renames: self.renames.clone(),
            path_map: HashMap::new(),
            objects: self.objects.clone(),
        }
    }

    /// `base/comps` with every component spelled as `renames` says.
    fn join_renamed(&self, base: &Path, comps: &[&str]) -> PathBuf {
        let mut path = base.to_path_buf();
        let mut vpath = String::new();
        for comp in comps {
            vpath = child_path(&vpath, comp);
            path.push(self.renames.get(&vpath).map(String::as_str).unwrap_or(comp));
        }
        path
    }

    /// `disk` under the virtual directory `parent` is what another entry of
    /// this download was renamed to.
    fn renamed_to_by_other(&self, parent: &str, disk: &str, except: &str) -> bool {
        self.renames
            .iter()
            .any(|(vpath, d)| d == disk && vpath != except && parent_of(vpath) == parent)
    }
}

/// `parent/name`, or `name` at the top level.
fn child_path(parent: &str, name: &str) -> String {
    if parent.is_empty() {
        name.to_string()
    } else {
        format!("{parent}/{name}")
    }
}

fn parent_of(vpath: &str) -> &str {
    vpath.rsplit_once('/').map(|(p, _)| p).unwrap_or("")
}

/// Serialize `doc` and atomically replace `<dir>/multiparts.json` (tmp +
/// `sync_all` + rename + best-effort dir fsync).
async fn flush_doc(dir: &Path, doc: &MetaDoc) -> Result<(), StorageError> {
    let bytes =
        serde_json::to_vec_pretty(doc).map_err(|e| StorageError::Other(format!("serialize multiparts.json: {e}")))?;
    let tmp = dir.join("multiparts.json.tmp");
    let final_path = dir.join("multiparts.json");
    {
        let mut f = fs::File::create(&tmp).await?;
        f.write_all(&bytes).await?;
        f.sync_all().await?;
    }
    fs::rename(&tmp, &final_path).await?;
    // Best-effort directory fsync so the rename is durable. Not supported on all
    // platforms (notably Windows); a failure here is non-fatal.
    if let Ok(d) = std::fs::File::open(dir) {
        let _ = d.sync_all();
    }
    Ok(())
}

/// Reject `.`/`..` components (Rust port of Electron's `pathForWriteInt` check),
/// so a peer-supplied key can't escape the download root via traversal.
fn split_key(key: &str) -> Result<Vec<&str>, StorageError> {
    let comps: Vec<&str> = key.split('/').filter(|c| !c.is_empty()).collect();
    if comps.iter().any(|c| *c == "." || *c == "..") {
        return Err(StorageError::Other(format!("path traversal in key: {key}")));
    }
    if comps.is_empty() {
        return Err(StorageError::Other("empty key".into()));
    }
    Ok(comps)
}

/// Pick the first `"<stem> <n><ext>"` (1-based) neither promised to another
/// entry of this download in `renames` (under the virtual directory
/// `parent`, `except` being the entry asking) nor present under the on-disk
/// directory `dir` (Rust port of Electron's `generateNewName`, with its
/// double-dot bug fixed so an extension yields `"movie 1.mp4"`, not
/// `"movie 1..mp4"`).
async fn generate_new_name(inner: &MetaInner, parent: &str, except: &str, dir: &Path, name: &str) -> String {
    let p = Path::new(name);
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
        let candidate = format!("{stem} {num}{ext}");
        if !inner.renamed_to_by_other(parent, &candidate, except)
            && fs::symlink_metadata(dir.join(&candidate)).await.is_err()
        {
            return candidate;
        }
        num += 1;
    }
}

/// Per-download persistent metadata store.
pub struct FsMetaStore {
    /// `None` for a landed download: roots and renames came off the record,
    /// nothing is ever written.
    dir: Option<PathBuf>,
    policy: CollisionPolicy,
    rule: NameRule,
    inner: Mutex<MetaInner>,
}

impl FsMetaStore {
    /// Create `dir` and load `multiparts.json`. Writes are atomic (tmp+rename),
    /// so a torn file can't occur; a parse error therefore means genuine
    /// corruption — log and start empty (safe direction: parts get re-sent,
    /// never falsely completed). A document written before `roots` +
    /// `renames` carries `path_map` instead; both are derived from it.
    pub async fn open(dir: PathBuf, policy: CollisionPolicy, rule: NameRule) -> Result<Self, StorageError> {
        fs::create_dir_all(&dir).await?;
        let path = dir.join("multiparts.json");
        let doc = match fs::read(&path).await {
            Ok(bytes) => match serde_json::from_slice::<MetaDoc>(&bytes) {
                Ok(doc) => doc,
                Err(e) => {
                    tracing::warn!("corrupt {}: {e}; starting empty", path.display());
                    MetaDoc::default()
                }
            },
            Err(_) => MetaDoc::default(),
        };
        let (roots, renames) = if doc.roots.is_empty() && !doc.path_map.is_empty() {
            split_legacy_path_map(doc.path_map)
        } else {
            (doc.roots, doc.renames)
        };
        Ok(Self {
            dir: Some(dir),
            policy,
            rule,
            inner: Mutex::new(MetaInner {
                objects: doc.objects,
                roots: roots.into_iter().collect(),
                renames,
            }),
        })
    }

    /// The store of a download that landed whole: its persisted roots and
    /// renames, no in-progress records, no directory. Read-only — any write
    /// is an error.
    pub fn landed(roots: Vec<String>, renames: HashMap<String, String>, rule: NameRule) -> Self {
        Self {
            dir: None,
            policy: CollisionPolicy::Avoid,
            rule,
            inner: Mutex::new(MetaInner {
                objects: HashMap::new(),
                roots: roots.into_iter().collect(),
                renames,
            }),
        }
    }

    /// Persist `inner` — the one write path; a landed (dir-less) store refuses.
    async fn flush(&self, inner: &MetaInner) -> Result<(), StorageError> {
        let Some(dir) = &self.dir else {
            return Err(StorageError::Other("meta store of a landed download is read-only".into()));
        };
        flush_doc(dir, &inner.to_doc()).await
    }

    /// Live (incomplete) object keys.
    pub async fn list_multiparts(&self) -> Vec<String> {
        self.inner.lock().await.objects.keys().cloned().collect()
    }

    pub async fn read(&self, key: &str) -> Option<FsMetaInfo> {
        self.inner.lock().await.objects.get(key).cloned()
    }

    /// The top-level virtual names this download has written, sorted.
    pub async fn roots(&self) -> Vec<String> {
        self.inner.lock().await.roots.iter().cloned().collect()
    }

    /// Virtual path of a component (any level) → its on-disk name, only where
    /// the two differ (base-relative; join under the dest base).
    pub async fn renames(&self) -> HashMap<String, String> {
        self.inner.lock().await.renames.clone()
    }

    /// Resolve a virtual `key` to its real on-disk destination, one loop over
    /// its components (`docs/app/naming-fix.md`). For component `i` with
    /// virtual path `P_i`, under the already-resolved parent directory:
    /// - `renames` has `P_i` → that name;
    /// - a top-level name already in `roots` (unrenamed) → itself;
    /// - otherwise `disk = rule(name)`. `renames` first, then disk: a `disk`
    ///   another entry of this download was renamed to is our own collision
    ///   → `generate_new_name`, whether or not that entry's file exists yet.
    ///   Else, when `disk` exists on disk: the rule rewrote it →
    ///   `generate_new_name`, whoever owns the entry; unrewritten at the top
    ///   level → the policy (`Avoid` → `generate_new_name`, `Strict` →
    ///   error); unrewritten nested → a directory is reused, a leaf without
    ///   an in-flight record for `key` is the hard error "refusing to
    ///   overwrite existing nested entry".
    ///
    /// Disk is consulted only where it decides something: a new root, a
    /// rule-rewritten name, and a non-directory leaf. An unrewritten
    /// intermediate directory is ours and `create_dir_all` reuses it; our
    /// own in-flight file is known by its record. A chosen name that differs
    /// from `name` is recorded in `renames`; a new top-level name is added
    /// to `roots`; any change flushes. Creates the destination dir (`Dir`)
    /// or the parent dir (`File`/`Symlink`).
    pub async fn resolve_path(&self, base: &Path, key: &str, kind: WriteKind) -> Result<PathBuf, StorageError> {
        let comps = split_key(key)?;
        let last = comps.len() - 1;

        let mut inner = self.inner.lock().await;
        let mut dest = base.to_path_buf();
        let mut parent = String::new();
        let mut changed = false;
        for (i, name) in comps.iter().enumerate() {
            let vpath = child_path(&parent, name);
            let chosen = match inner.renames.get(&vpath) {
                Some(disk) => disk.clone(),
                None if i == 0 && inner.roots.contains(*name) => name.to_string(),
                // Our own in-flight file (`put_part` re-resolving it).
                None if i == last && inner.objects.contains_key(key) => name.to_string(),
                None => {
                    let disk = self.rule.apply(name);
                    let rewritten = disk != *name;
                    let consult_disk = rewritten || i == 0 || (i == last && kind != WriteKind::Dir);
                    if inner.renamed_to_by_other(&parent, &disk, &vpath) {
                        generate_new_name(&inner, &parent, &vpath, &dest, &disk).await
                    } else if consult_disk && fs::symlink_metadata(dest.join(&disk)).await.is_ok() {
                        if rewritten {
                            generate_new_name(&inner, &parent, &vpath, &dest, &disk).await
                        } else if i == 0 {
                            match self.policy {
                                CollisionPolicy::Strict => {
                                    return Err(StorageError::Other(format!(
                                        "destination already exists (strict): {}",
                                        dest.join(&disk).display()
                                    )));
                                }
                                CollisionPolicy::Avoid => generate_new_name(&inner, &parent, &vpath, &dest, &disk).await,
                            }
                        } else {
                            // The only stat left on a nested level is the
                            // non-directory leaf, and a nested leaf that
                            // exists without our in-flight record is foreign.
                            return Err(StorageError::Other(format!(
                                "refusing to overwrite existing nested entry: {key}"
                            )));
                        }
                    } else {
                        disk
                    }
                }
            };
            if chosen != *name && !inner.renames.contains_key(&vpath) {
                inner.renames.insert(vpath.clone(), chosen.clone());
                changed = true;
            }
            if i == 0 && inner.roots.insert(name.to_string()) {
                changed = true;
            }
            dest.push(&chosen);
            parent = vpath;
        }
        if changed {
            self.flush(&inner).await?;
        }

        match kind {
            WriteKind::Dir => {
                fs::create_dir_all(&dest).await?;
            }
            WriteKind::File | WriteKind::Symlink => {
                if let Some(dir) = dest.parent() {
                    fs::create_dir_all(dir).await?;
                }
            }
        }
        Ok(dest)
    }

    /// Read-only resolve of `key` through `roots` + `renames`: `None` when the
    /// top-level component is not a root of this download. No `base/key`
    /// fallback — status probes must not see pre-existing foreign files
    /// (collision avoidance still applies to them on write).
    pub async fn resolve_mapped(&self, base: &Path, key: &str) -> Option<PathBuf> {
        let comps: Vec<&str> = key.split('/').filter(|c| !c.is_empty()).collect();
        let inner = self.inner.lock().await;
        let first = *comps.first()?;
        if !inner.roots.contains(first) {
            return None;
        }
        Some(inner.join_renamed(base, &comps))
    }

    /// Read-only resolve through `renames` (no mutation, no fs side effects)
    /// for delete/cleanup paths.
    pub async fn resolve_existing(&self, base: &Path, key: &str) -> PathBuf {
        let comps: Vec<&str> = key.split('/').filter(|c| !c.is_empty()).collect();
        let inner = self.inner.lock().await;
        inner.join_renamed(base, &comps)
    }

    /// Record a new in-progress multipart. **Errors if `key` already has a
    /// record** (duplicate-begin guard — the engine only calls `begin_multipart`
    /// when `list_multiparts_all` has no match, so a live record here is a logic
    /// error). Flushes synchronously (membership must be durable before the
    /// first byte lands).
    pub async fn begin(
        &self,
        key: &str,
        ftype: FsEntryType,
        attrs: Option<&StorageFileAttrs>,
    ) -> Result<(), StorageError> {
        let mut inner = self.inner.lock().await;
        if inner.objects.contains_key(key) {
            return Err(StorageError::Other(format!("multipart already in progress for {key}")));
        }
        inner.objects.insert(
            key.to_string(),
            FsMetaInfo {
                ftype,
                attrs: attrs.map(FsAttrs::from_storage),
                parts: Vec::new(),
            },
        );
        self.flush(&inner).await
    }

    /// Upsert a part by `part_number` (re-put with matching offset/size is
    /// idempotent; a conflicting offset/size is an error). Flushed before
    /// returning: a landed part is a recorded part.
    pub async fn put_part(&self, key: &str, p: FsPartInfo) -> Result<(), StorageError> {
        let mut inner = self.inner.lock().await;
        let info = inner
            .objects
            .get_mut(key)
            .ok_or_else(|| StorageError::Other(format!("no multipart record for {key}")))?;
        if let Some(existing) = info.parts.iter_mut().find(|e| e.part_number == p.part_number) {
            if existing.offset != p.offset || existing.size != p.size {
                return Err(StorageError::Other(format!(
                    "part {key}#{} already exists with different offset/size",
                    p.part_number
                )));
            }
            *existing = p;
        } else {
            info.parts.push(p);
        }
        info.parts.sort_by_key(|e| e.part_number);

        self.flush(&inner).await
    }

    /// Keep only the parts whose `part_number` is in `keep`; flush if any were
    /// dropped. Used by the resume-time integrity pass to discard parts whose
    /// data didn't survive a crash, so the engine re-downloads them.
    pub async fn retain_parts(&self, key: &str, keep: &[u32]) -> Result<(), StorageError> {
        let mut inner = self.inner.lock().await;
        if let Some(info) = inner.objects.get_mut(key) {
            let before = info.parts.len();
            let keepset: HashSet<u32> = keep.iter().copied().collect();
            info.parts.retain(|p| keepset.contains(&p.part_number));
            if info.parts.len() != before {
                self.flush(&inner).await?;
            }
        }
        Ok(())
    }

    /// Verify the parts are contiguous `1..N` **and tile the file from offset
    /// 0 without gaps** (a part set left by a run against a different file
    /// shape can be `1..N` by number and still leave a hole), then remove the
    /// record and flush. Verification happens **before** removal so a gap
    /// leaves the record intact (the engine can re-send the missing part and
    /// retry).
    pub async fn complete(&self, key: &str) -> Result<FsMetaInfo, StorageError> {
        let mut inner = self.inner.lock().await;
        {
            let info = inner
                .objects
                .get(key)
                .ok_or_else(|| StorageError::Other(format!("no multipart record for {key}")))?;
            // parts are kept sorted by `put_part`.
            let mut next_offset = 0u64;
            for (expected, part) in (1u32..).zip(&info.parts) {
                if part.part_number != expected {
                    return Err(StorageError::Other(format!("missing part #{expected} of {key}")));
                }
                if part.offset != next_offset {
                    return Err(StorageError::Other(format!(
                        "part #{expected} of {key} at offset {} does not follow the previous part (expected {next_offset})",
                        part.offset
                    )));
                }
                next_offset += part.size;
            }
        }
        let info = inner.objects.remove(key).expect("checked present above");
        self.flush(&inner).await?;
        Ok(info)
    }

    /// Remove an in-progress record (cancel). Idempotent — no error if absent.
    pub async fn cancel(&self, key: &str) -> Result<(), StorageError> {
        let mut inner = self.inner.lock().await;
        if inner.objects.remove(key).is_some() {
            self.flush(&inner).await?;
        }
        Ok(())
    }

    /// Drop any stale partial record for `key` (Electron's `delPathIncomplete`),
    /// used before a complete `put_file`/`del_file`.
    pub async fn forget(&self, key: &str) -> Result<(), StorageError> {
        self.cancel(key).await
    }

    /// Final flush (call on `close`).
    pub async fn close(&self) -> Result<(), StorageError> {
        let inner = self.inner.lock().await;
        self.flush(&inner).await
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
            p.push(format!("fsmeta_test_{}_{}", std::process::id(), n));
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

    fn part(n: u32, off: u64, size: u64) -> FsPartInfo {
        FsPartInfo {
            part_number: n,
            offset: off,
            size,
            etag: None,
        }
    }

    #[tokio::test]
    async fn load_roundtrips_objects_and_path_map() {
        let tmp = TmpDir::new();
        let meta_dir = tmp.path().join("meta");
        {
            let store = FsMetaStore::open(meta_dir.clone(), CollisionPolicy::Avoid, NameRule::Posix)
                .await
                .unwrap();
            store.begin("a.bin", FsEntryType::File, None).await.unwrap();
            store.put_part("a.bin", part(1, 0, 4)).await.unwrap();
            store.close().await.unwrap();
        }
        // Reopen and confirm the record + part survived a close()/reopen.
        let store = FsMetaStore::open(meta_dir, CollisionPolicy::Avoid, NameRule::Posix).await.unwrap();
        assert_eq!(store.list_multiparts().await, vec!["a.bin".to_string()]);
        let info = store.read("a.bin").await.unwrap();
        assert_eq!(info.parts.len(), 1);
        assert_eq!(info.parts[0].part_number, 1);
    }

    #[tokio::test]
    async fn put_part_idempotent_and_conflict() {
        let tmp = TmpDir::new();
        let store = FsMetaStore::open(tmp.path().join("m"), CollisionPolicy::Avoid, NameRule::Posix)
            .await
            .unwrap();
        store.begin("k", FsEntryType::File, None).await.unwrap();
        store.put_part("k", part(1, 0, 4)).await.unwrap();
        // Same number, same offset/size → idempotent ok.
        store.put_part("k", part(1, 0, 4)).await.unwrap();
        assert_eq!(store.read("k").await.unwrap().parts.len(), 1);
        // Same number, different size → conflict error.
        let err = store.put_part("k", part(1, 0, 8)).await;
        assert!(err.is_err(), "conflicting offset/size must error");
    }

    #[tokio::test]
    async fn begin_twice_errors() {
        let tmp = TmpDir::new();
        let store = FsMetaStore::open(tmp.path().join("m"), CollisionPolicy::Avoid, NameRule::Posix)
            .await
            .unwrap();
        store.begin("k", FsEntryType::File, None).await.unwrap();
        assert!(
            store.begin("k", FsEntryType::File, None).await.is_err(),
            "duplicate begin must error"
        );
    }

    #[tokio::test]
    async fn complete_requires_contiguous_parts() {
        let tmp = TmpDir::new();
        let store = FsMetaStore::open(tmp.path().join("m"), CollisionPolicy::Avoid, NameRule::Posix)
            .await
            .unwrap();
        store.begin("k", FsEntryType::File, None).await.unwrap();
        store.put_part("k", part(1, 0, 4)).await.unwrap();
        store.put_part("k", part(3, 8, 4)).await.unwrap(); // gap at #2
        assert!(store.complete("k").await.is_err(), "gap must fail completion");
        // Record is retained for retry.
        assert!(store.read("k").await.is_some());
        store.put_part("k", part(2, 4, 4)).await.unwrap();
        assert!(store.complete("k").await.is_ok());
        assert!(store.read("k").await.is_none(), "completed record removed");
    }

    /// Numbers 1..N present but a hole behind a short part: not complete.
    #[tokio::test]
    async fn complete_requires_contiguous_offsets() {
        let tmp = TmpDir::new();
        let store = FsMetaStore::open(tmp.path().join("m"), CollisionPolicy::Avoid, NameRule::Posix)
            .await
            .unwrap();
        store.begin("k", FsEntryType::File, None).await.unwrap();
        store.put_part("k", part(1, 0, 4)).await.unwrap();
        store.put_part("k", part(2, 4, 2)).await.unwrap(); // a short part mid-file
        store.put_part("k", part(3, 8, 4)).await.unwrap(); // leaves 6..8 unwritten
        assert!(
            store.complete("k").await.is_err(),
            "hole behind part 2 must fail completion"
        );
        assert!(store.read("k").await.is_some(), "record retained for retry");
    }

    /// A part is on disk the moment `put_part` returns: a store dropped
    /// without `close()` (a pause that never closes the dest, a crash) reopens
    /// with every landed part.
    #[tokio::test]
    async fn every_part_is_durable_without_close() {
        let tmp = TmpDir::new();
        let meta_dir = tmp.path().join("m");
        {
            let store = FsMetaStore::open(meta_dir.clone(), CollisionPolicy::Avoid, NameRule::Posix)
                .await
                .unwrap();
            store.begin("k", FsEntryType::File, None).await.unwrap();
            store.put_part("k", part(1, 0, 4)).await.unwrap();
            store.put_part("k", part(2, 4, 4)).await.unwrap();
            // drop without close()
        }
        let store = FsMetaStore::open(meta_dir, CollisionPolicy::Avoid, NameRule::Posix).await.unwrap();
        assert_eq!(store.list_multiparts().await, vec!["k".to_string()]);
        let parts = store.read("k").await.unwrap().parts;
        assert_eq!(parts.iter().map(|p| p.part_number).collect::<Vec<_>>(), vec![1, 2]);
    }

    #[tokio::test]
    async fn resolve_top_level_avoid_renames_strict_errors() {
        let tmp = TmpDir::new();
        let base = tmp.path().join("dl");
        std::fs::create_dir_all(&base).unwrap();
        std::fs::write(base.join("Foo.txt"), b"old").unwrap();

        // Avoid → renames to "Foo 1.txt", original kept.
        let avoid = FsMetaStore::open(tmp.path().join("ma"), CollisionPolicy::Avoid, NameRule::Posix)
            .await
            .unwrap();
        let dest = avoid.resolve_path(&base, "Foo.txt", WriteKind::File).await.unwrap();
        assert_eq!(dest, base.join("Foo 1.txt"));
        assert_eq!(avoid.roots().await, vec!["Foo.txt".to_string()]);
        assert_eq!(avoid.renames().await.get("Foo.txt"), Some(&"Foo 1.txt".to_string()));

        // Strict → errors.
        let strict = FsMetaStore::open(tmp.path().join("ms"), CollisionPolicy::Strict, NameRule::Posix)
            .await
            .unwrap();
        assert!(strict.resolve_path(&base, "Foo.txt", WriteKind::File).await.is_err());
    }

    #[tokio::test]
    async fn resolve_nested_overwrite_always_errors() {
        let tmp = TmpDir::new();
        let base = tmp.path().join("dl");
        let store = FsMetaStore::open(tmp.path().join("m"), CollisionPolicy::Avoid, NameRule::Posix)
            .await
            .unwrap();

        // Map "Dir" by writing a first nested file (Dir not yet on disk → no rename).
        let a = store.resolve_path(&base, "Dir/a.bin", WriteKind::File).await.unwrap();
        assert_eq!(a, base.join("Dir/a.bin"));
        // Simulate a stray/foreign nested entry we don't own.
        std::fs::write(base.join("Dir/inner.bin"), b"x").unwrap();
        // Even in Avoid mode, a nested overwrite is a hard error.
        assert!(
            store
                .resolve_path(&base, "Dir/inner.bin", WriteKind::File)
                .await
                .is_err()
        );
    }

    #[test]
    fn name_rules() {
        assert_eq!(NameRule::Posix.apply("IMAGE 2026-09-23 02:51:27.jpg"), "IMAGE 2026-09-23 02:51:27.jpg");
        assert_eq!(NameRule::Fat.apply("IMAGE 2026-09-23 02:51:27.jpg"), "IMAGE 2026-09-23 02_51_27.jpg");
        assert_eq!(NameRule::Fat.apply("a\"b*c<d>e?f\\g|h\x01i\x7fj"), "a_b_c_d_e_f_g_h_i_j");
        assert_eq!(NameRule::Fat.apply("Фото"), "Фото");
        assert_eq!(NameRule::Windows.apply("con.txt"), "con_.txt");
        assert_eq!(NameRule::Windows.apply("CON"), "CON_");
        assert_eq!(NameRule::Windows.apply("COM1"), "COM1_");
        assert_eq!(NameRule::Windows.apply("lpt9.tar.gz"), "lpt9_.tar.gz");
        assert_eq!(NameRule::Windows.apply("COM1 .txt"), "COM1 _.txt");
        assert_eq!(NameRule::Windows.apply("COM1 "), "COM1 _");
        assert_eq!(NameRule::Windows.apply("console.txt"), "console.txt");
        assert_eq!(NameRule::Windows.apply("name."), "name_");
        assert_eq!(NameRule::Windows.apply("name "), "name_");
        assert_eq!(NameRule::Windows.apply("name.. "), "name___");
        assert_eq!(NameRule::Windows.apply("a:b."), "a_b_");
    }

    /// `Fat`: a nested file with `:` lands under `_`, the rewrite is in
    /// `renames` and the root in `roots`; `Posix` writes the name as is and
    /// records no rename.
    #[tokio::test]
    async fn rule_rewrites_nested_name_and_records_it() {
        let tmp = TmpDir::new();
        let base = tmp.path().join("dl");
        let key = "Telegram/IMAGE 2026-09-23 02:51:27.jpg";

        let fat = FsMetaStore::open(tmp.path().join("mf"), CollisionPolicy::Avoid, NameRule::Fat)
            .await
            .unwrap();
        let dest = fat.resolve_path(&base, key, WriteKind::File).await.unwrap();
        assert_eq!(dest, base.join("Telegram/IMAGE 2026-09-23 02_51_27.jpg"));
        assert_eq!(fat.roots().await, vec!["Telegram".to_string()]);
        assert_eq!(
            fat.renames().await,
            HashMap::from([(key.to_string(), "IMAGE 2026-09-23 02_51_27.jpg".to_string())])
        );

        let posix = FsMetaStore::open(tmp.path().join("mp"), CollisionPolicy::Avoid, NameRule::Posix)
            .await
            .unwrap();
        let dest = posix.resolve_path(&tmp.path().join("dp"), key, WriteKind::File).await.unwrap();
        assert_eq!(dest, tmp.path().join("dp").join(key));
        assert_eq!(posix.roots().await, vec!["Telegram".to_string()]);
        assert!(posix.renames().await.is_empty());
    }

    /// The `Windows` rule through a resolve: reserved basenames and trailing
    /// dots at any level.
    #[tokio::test]
    async fn windows_rule_through_resolve() {
        let tmp = TmpDir::new();
        let base = tmp.path().join("dl");
        let store = FsMetaStore::open(tmp.path().join("m"), CollisionPolicy::Avoid, NameRule::Windows)
            .await
            .unwrap();
        let dest = store.resolve_path(&base, "logs./con.txt", WriteKind::File).await.unwrap();
        assert_eq!(dest, base.join("logs_/con_.txt"));
        let dest = store.resolve_path(&base, "COM1", WriteKind::File).await.unwrap();
        assert_eq!(dest, base.join("COM1_"));
        assert_eq!(
            store.renames().await,
            HashMap::from([
                ("logs.".to_string(), "logs_".to_string()),
                ("logs./con.txt".to_string(), "con_.txt".to_string()),
                ("COM1".to_string(), "COM1_".to_string()),
            ])
        );
    }

    /// A directory component with `:` in the middle of a path: rewritten
    /// once, and every path through it resolves the same way afterwards.
    #[tokio::test]
    async fn rewritten_directory_component() {
        let tmp = TmpDir::new();
        let base = tmp.path().join("dl");
        let store = FsMetaStore::open(tmp.path().join("m"), CollisionPolicy::Avoid, NameRule::Fat)
            .await
            .unwrap();
        let dir = store.resolve_path(&base, "Album/Фото 02:51", WriteKind::Dir).await.unwrap();
        assert_eq!(dir, base.join("Album/Фото 02_51"));
        assert!(dir.is_dir());
        let a = store.resolve_path(&base, "Album/Фото 02:51/a.jpg", WriteKind::File).await.unwrap();
        assert_eq!(a, base.join("Album/Фото 02_51/a.jpg"));
        let b = store.resolve_path(&base, "Album/Фото 02:51/b.jpg", WriteKind::File).await.unwrap();
        assert_eq!(b, base.join("Album/Фото 02_51/b.jpg"));
        assert_eq!(
            store.renames().await,
            HashMap::from([("Album/Фото 02:51".to_string(), "Фото 02_51".to_string())])
        );
        assert_eq!(store.resolve_mapped(&base, "Album/Фото 02:51/b.jpg").await, Some(b));
    }

    /// `a:b.jpg` beside `a_b.jpg` in one share: whichever arrives first
    /// keeps `a_b.jpg`, the other lands as `a_b 1.jpg` — files and
    /// directories.
    #[tokio::test]
    async fn rewrite_collides_with_sibling_in_either_order() {
        // Files, rewritten one first.
        {
            let tmp = TmpDir::new();
            let base = tmp.path().join("dl");
            let store = FsMetaStore::open(tmp.path().join("m"), CollisionPolicy::Avoid, NameRule::Fat)
                .await
                .unwrap();
            let first = store.resolve_path(&base, "d/a:b.jpg", WriteKind::File).await.unwrap();
            std::fs::write(&first, b"1").unwrap();
            let second = store.resolve_path(&base, "d/a_b.jpg", WriteKind::File).await.unwrap();
            assert_eq!(first, base.join("d/a_b.jpg"));
            assert_eq!(second, base.join("d/a_b 1.jpg"));
            assert_eq!(store.resolve_mapped(&base, "d/a:b.jpg").await, Some(first));
            assert_eq!(store.resolve_mapped(&base, "d/a_b.jpg").await, Some(second));
        }
        // Files, plain one first.
        {
            let tmp = TmpDir::new();
            let base = tmp.path().join("dl");
            let store = FsMetaStore::open(tmp.path().join("m"), CollisionPolicy::Avoid, NameRule::Fat)
                .await
                .unwrap();
            let first = store.resolve_path(&base, "d/a_b.jpg", WriteKind::File).await.unwrap();
            std::fs::write(&first, b"1").unwrap();
            let second = store.resolve_path(&base, "d/a:b.jpg", WriteKind::File).await.unwrap();
            assert_eq!(first, base.join("d/a_b.jpg"));
            assert_eq!(second, base.join("d/a_b 1.jpg"));
            assert!(!store.renames().await.contains_key("d/a_b.jpg"));
            assert_eq!(store.renames().await.get("d/a:b.jpg"), Some(&"a_b 1.jpg".to_string()));
        }
        // Directories, both orders, at the top level.
        for (first_key, second_key) in [("Фото 02:51", "Фото 02_51"), ("Фото 02_51", "Фото 02:51")] {
            let tmp = TmpDir::new();
            let base = tmp.path().join("dl");
            let store = FsMetaStore::open(tmp.path().join("m"), CollisionPolicy::Avoid, NameRule::Fat)
                .await
                .unwrap();
            let first = store.resolve_path(&base, first_key, WriteKind::Dir).await.unwrap();
            let second = store.resolve_path(&base, second_key, WriteKind::Dir).await.unwrap();
            assert_eq!(first, base.join("Фото 02_51"), "{first_key} first");
            assert_eq!(second, base.join("Фото 02_51 1"), "{first_key} first");
            assert_eq!(store.roots().await, vec!["Фото 02:51".to_string(), "Фото 02_51".to_string()]);
            // Every path through either dir keeps its own spelling.
            let inner = store
                .resolve_path(&base, &format!("{second_key}/x.jpg"), WriteKind::File)
                .await
                .unwrap();
            assert_eq!(inner, base.join("Фото 02_51 1/x.jpg"));
        }
        // Promised, not yet on disk: a sibling resolving before the first
        // entry's file exists still gets its own name — both for a plain
        // rewrite and for a generated one.
        {
            let tmp = TmpDir::new();
            let base = tmp.path().join("dl");
            let store = FsMetaStore::open(tmp.path().join("m"), CollisionPolicy::Avoid, NameRule::Fat)
                .await
                .unwrap();
            let first = store.resolve_path(&base, "d/a:b.jpg", WriteKind::File).await.unwrap();
            assert_eq!(first, base.join("d/a_b.jpg"));
            assert!(!first.exists());
            let second = store.resolve_path(&base, "d/a*b.jpg", WriteKind::File).await.unwrap();
            assert_eq!(second, base.join("d/a_b 1.jpg"));
            assert!(!second.exists());
            let third = store.resolve_path(&base, "d/a?b.jpg", WriteKind::File).await.unwrap();
            assert_eq!(third, base.join("d/a_b 2.jpg"));
            // Re-resolving each keeps its name.
            assert_eq!(store.resolve_path(&base, "d/a*b.jpg", WriteKind::File).await.unwrap(), second);
        }
    }

    /// Top-level `Avoid` / `Strict` unchanged with the two fields: a root
    /// renamed by `Avoid` is in `roots` under its virtual name and in
    /// `renames`; a root written as is has no rename; `Strict` errors.
    #[tokio::test]
    async fn top_level_policy_with_roots_and_renames() {
        let tmp = TmpDir::new();
        let base = tmp.path().join("dl");
        std::fs::create_dir_all(&base).unwrap();
        std::fs::write(base.join("Foo"), b"old").unwrap();

        let avoid = FsMetaStore::open(tmp.path().join("ma"), CollisionPolicy::Avoid, NameRule::Posix)
            .await
            .unwrap();
        assert_eq!(avoid.resolve_path(&base, "Foo", WriteKind::File).await.unwrap(), base.join("Foo 1"));
        assert_eq!(avoid.resolve_path(&base, "Bar", WriteKind::File).await.unwrap(), base.join("Bar"));
        // Our own root, re-resolved (a second write under it): no rename.
        std::fs::write(base.join("Bar"), b"ours").unwrap();
        assert_eq!(avoid.resolve_path(&base, "Bar", WriteKind::File).await.unwrap(), base.join("Bar"));
        assert_eq!(avoid.roots().await, vec!["Bar".to_string(), "Foo".to_string()]);
        assert_eq!(avoid.renames().await, HashMap::from([("Foo".to_string(), "Foo 1".to_string())]));

        let strict = FsMetaStore::open(tmp.path().join("ms"), CollisionPolicy::Strict, NameRule::Posix)
            .await
            .unwrap();
        assert!(strict.resolve_path(&base, "Foo", WriteKind::File).await.is_err());
        assert!(strict.roots().await.is_empty());
    }

    /// A reopen carries `roots` and `renames` — in-flight rewrites resolve to
    /// the same disk path after a restart.
    #[tokio::test]
    async fn reload_preserves_roots_and_renames() {
        let tmp = TmpDir::new();
        let base = tmp.path().join("dl");
        let meta_dir = tmp.path().join("m");
        let (roots, renames) = {
            let store = FsMetaStore::open(meta_dir.clone(), CollisionPolicy::Avoid, NameRule::Fat)
                .await
                .unwrap();
            store.resolve_path(&base, "d/a:b.jpg", WriteKind::File).await.unwrap();
            store.resolve_path(&base, "plain.txt", WriteKind::File).await.unwrap();
            (store.roots().await, store.renames().await)
        };
        let store = FsMetaStore::open(meta_dir, CollisionPolicy::Avoid, NameRule::Fat)
            .await
            .unwrap();
        assert_eq!(store.roots().await, roots);
        assert_eq!(store.renames().await, renames);
        assert_eq!(store.roots().await, vec!["d".to_string(), "plain.txt".to_string()]);
        assert_eq!(
            store.resolve_path(&base, "d/a:b.jpg", WriteKind::File).await.unwrap(),
            base.join("d/a_b.jpg")
        );
    }

    /// The three resolvers land on one disk path for a written key;
    /// `resolve_mapped` of a first component that is not a root is `None`.
    #[tokio::test]
    async fn resolvers_agree_and_mapped_gates_on_roots() {
        let tmp = TmpDir::new();
        let base = tmp.path().join("dl");
        std::fs::create_dir_all(base.join("foreign")).unwrap();
        std::fs::write(base.join("foreign/x.txt"), b"not ours").unwrap();
        let store = FsMetaStore::open(tmp.path().join("m"), CollisionPolicy::Avoid, NameRule::Fat)
            .await
            .unwrap();
        let key = "Фото 02:51/a:b.jpg";
        let written = store.resolve_path(&base, key, WriteKind::File).await.unwrap();
        assert_eq!(written, base.join("Фото 02_51/a_b.jpg"));
        assert_eq!(store.resolve_mapped(&base, key).await, Some(written.clone()));
        assert_eq!(store.resolve_existing(&base, key).await, written);
        assert_eq!(store.resolve_mapped(&base, "foreign/x.txt").await, None);
        assert_eq!(store.resolve_mapped(&base, "nothing").await, None);
    }

    /// A document written before `roots` + `renames` carries `path_map` for
    /// every root: both are derived from it on load, and the next flush
    /// writes only the new fields.
    #[tokio::test]
    async fn legacy_path_map_document_loads_as_roots_and_renames() {
        let tmp = TmpDir::new();
        let meta_dir = tmp.path().join("m");
        std::fs::create_dir_all(&meta_dir).unwrap();
        std::fs::write(
            meta_dir.join("multiparts.json"),
            br#"{
  "version": 1,
  "path_map": { "Docs": "Docs 1", "top.bin": "top.bin", "big.bin": "big.bin" },
  "objects": { "big.bin": { "ftype": "f", "parts": [ { "part_number": 1, "offset": 0, "size": 4 } ] } }
}"#,
        )
        .unwrap();
        let store = FsMetaStore::open(meta_dir.clone(), CollisionPolicy::Avoid, NameRule::Posix)
            .await
            .unwrap();
        assert_eq!(
            store.roots().await,
            vec!["Docs".to_string(), "big.bin".to_string(), "top.bin".to_string()]
        );
        assert_eq!(store.renames().await, HashMap::from([("Docs".to_string(), "Docs 1".to_string())]));
        assert_eq!(store.list_multiparts().await, vec!["big.bin".to_string()]);
        let base = tmp.path().join("dl");
        assert_eq!(store.resolve_mapped(&base, "Docs/a.txt").await, Some(base.join("Docs 1/a.txt")));

        store.close().await.unwrap();
        let raw = std::fs::read_to_string(meta_dir.join("multiparts.json")).unwrap();
        assert!(!raw.contains("path_map"), "legacy field never written again: {raw}");
        assert!(raw.contains("\"roots\"") && raw.contains("\"renames\""));
        let again = FsMetaStore::open(meta_dir, CollisionPolicy::Avoid, NameRule::Posix)
            .await
            .unwrap();
        assert_eq!(again.roots().await, store.roots().await);
        assert_eq!(again.renames().await, store.renames().await);
    }
}
