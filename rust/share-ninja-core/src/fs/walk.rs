//! Shared fast directory walk, reused by `FsSource` (listing / dir-stats) and
//! `FsDest` (path-map stats walk). The whole tree walk runs synchronously
//! (call from `spawn_blocking`) over an explicit stack — no per-entry `tokio::fs`
//! await round-trips, no `Box::pin` recursion, one open directory handle at a
//! time. On macOS/iOS each directory is read via `getattrlistbulk` (type + size
//! [+ mtime + mode] inline, no per-entry `lstat`); other platforms use `readdir`.

use std::collections::HashMap;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use tokio::sync::mpsc;

use super::search::{BatchSink, SearchFilter, push_filtered};
use crate::{StorageError, StorageFileType, StorageObjectInfo, StoragePartInfo};

pub(crate) fn system_time_to_chrono(st: SystemTime) -> Option<chrono::DateTime<chrono::Utc>> {
    st.duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|d| chrono::DateTime::from_timestamp(d.as_secs() as i64, d.subsec_nanos()))
}

/// Entry kind, shared by the dir-stats counter and the dir/meta listers.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum EntryKind {
    Dir,
    Symlink,
    File,
}

/// Per-entry visitor for `scan_dir`: `(name, kind, size, mtime, mode)` —
/// `mode` the POSIX permission bits (`None` where the platform has none).
pub(crate) type EntryVisitor<'a> =
    dyn FnMut(&OsStr, EntryKind, u64, Option<SystemTime>, Option<u32>) -> Result<(), StorageError> + 'a;

/// Per-entry visitor for `walk_tree`: `(virtual name, virtual prefix, kind,
/// size, mtime, mode)`.
pub(crate) type WalkVisitor<'a> =
    dyn FnMut(&str, &str, EntryKind, u64, Option<SystemTime>, Option<u32>) -> Result<(), StorageError> + 'a;

/// Disk → virtual names of the renamed entries a walk may cross:
/// `virtual parent prefix/on-disk name` → virtual name. Plain data, built
/// once from a dest's `renames` by [`reverse_renames`]; empty for a source,
/// whose disk names are its virtual names.
pub(crate) type WalkRenames = HashMap<String, String>;

/// The reverse index of `renames` (virtual path → on-disk name) for a walk.
pub(crate) fn reverse_renames(renames: &HashMap<String, String>) -> WalkRenames {
    renames
        .iter()
        .map(|(vpath, disk)| {
            let (prefix, name) = vpath.rsplit_once('/').unwrap_or(("", vpath));
            let disk_path = if prefix.is_empty() {
                disk.clone()
            } else {
                format!("{prefix}/{disk}")
            };
            (disk_path, name.to_string())
        })
        .collect()
}

/// The POSIX permission bits of an lstat (`None` on Windows).
pub(crate) fn mode_bits(md: &std::fs::Metadata) -> Option<u32> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        Some(md.permissions().mode() & 0o7777)
    }
    #[cfg(not(unix))]
    {
        let _ = md;
        None
    }
}

/// lstat-style entry size with symlinks normalized to their target-string
/// length. Unix lstat already reports that; Windows reports 0 for a reparse
/// point, so the link is read instead — otherwise a symlink's bytes would
/// never tally between the source's totals and the destination's landed count.
pub(crate) fn entry_size(path: &Path, md: &std::fs::Metadata) -> u64 {
    #[cfg(windows)]
    if md.file_type().is_symlink() {
        return std::fs::read_link(path)
            .map(|t| t.as_os_str().len() as u64)
            .unwrap_or_else(|_| md.len());
    }
    #[cfg(not(windows))]
    let _ = path;
    md.len()
}

/// Derive `(kind, size, mtime, mode)` from one `lstat`. Symlinks are reported
/// as `Symlink` with their own (link) size — never followed; see
/// [`entry_size`] for the symlink-size normalization. Directories have no
/// size (0) — a dir's lstat length is filesystem noise, never content.
pub(crate) fn meta_parts(path: &Path, md: &std::fs::Metadata) -> (EntryKind, u64, Option<SystemTime>, Option<u32>) {
    let mode = mode_bits(md);
    if md.is_dir() {
        return (EntryKind::Dir, 0, md.modified().ok(), mode);
    }
    let kind = if md.file_type().is_symlink() {
        EntryKind::Symlink
    } else {
        EntryKind::File
    };
    (kind, entry_size(path, md), md.modified().ok(), mode)
}

/// Version tag of a regular file: `mtime_ns-size` — both already in hand on
/// every listing path (the Darwin bulk walk included), so it costs no syscall.
/// A missing or pre-epoch mtime is 0, so a listing and a `get_attrs` of the
/// same file agree. Opaque; equality only. Unlike `FsObjectStore`'s etag it
/// carries no inode: a miss here is a stale thumbnail (docs/core/previews.md).
pub(crate) fn file_etag(mtime: Option<SystemTime>, size: u64) -> String {
    let mtime_ns = mtime
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{mtime_ns}-{size}")
}

/// Build a `StorageObjectInfo` from resolved entry attrs (a `Dir` node carries
/// no size; symlinks/files carry their lstat size, a regular file its etag;
/// `mode` lands in `attrs.posix_permissions`).
pub(crate) fn soi_from(
    key: &str,
    kind: EntryKind,
    size: u64,
    mtime: Option<SystemTime>,
    mode: Option<u32>,
) -> StorageObjectInfo {
    let mut soi = StorageObjectInfo::new(key);
    match kind {
        EntryKind::Dir => soi.file_type = StorageFileType::Dir,
        EntryKind::Symlink => {
            soi.file_type = StorageFileType::Symlink;
            soi.size = size;
        }
        EntryKind::File => {
            soi.file_type = StorageFileType::Regular;
            soi.size = size;
            soi.etag = Some(file_etag(mtime, size));
        }
    }
    if let Some(mt) = mtime {
        let dt = system_time_to_chrono(mt);
        soi.attrs.mtime = dt;
    }
    soi.attrs.posix_permissions = mode.map(i64::from);
    soi
}

/// Enumerate one directory, invoking `visit(name, kind, size, mtime, mode)` per
/// entry. macOS/iOS use `getattrlistbulk` (type + size [+ mtime + mode] inline,
/// no per-entry `lstat`); other platforms use `readdir` + cached entry
/// metadata. `want_mtime` requests mtimes and modes (the lister needs them,
/// the stats counter doesn't). Symlinks are reported, never followed. `Err`
/// from `visit` stops enumeration.
fn scan_dir(dir: &Path, want_mtime: bool, visit: &mut EntryVisitor<'_>) -> Result<(), StorageError> {
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    {
        darwin_bulk::scan_dir(dir, want_mtime, visit)
    }
    #[cfg(not(any(target_os = "macos", target_os = "ios")))]
    {
        scan_dir_std(dir, want_mtime, visit)
    }
}

/// Portable `readdir` enumerator (also the Darwin fallback). Attrs come from
/// `DirEntry::metadata()` — lstat semantics, but on Windows it's the data
/// `FindNextFileW` already returned (no per-entry file open) and on Unix an
/// `fstatat` against the open dirfd (no full-path re-resolution). A per-entry
/// metadata failure still yields the entry (zero-size file) rather than
/// aborting, matching the lister's tolerance for broken entries.
fn scan_dir_std(dir: &Path, _want_mtime: bool, visit: &mut EntryVisitor<'_>) -> Result<(), StorageError> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let (kind, size, mtime, mode) = match entry.metadata() {
            Ok(md) => meta_parts(&entry.path(), &md),
            Err(_) => (EntryKind::File, 0, None, None),
        };
        visit(&entry.file_name(), kind, size, mtime, mode)?;
    }
    Ok(())
}

/// Walk `root` (real path) under virtual `prefix`, depth-first over an explicit
/// stack (no recursion / deep-stack overflow, one open dir handle at a time),
/// calling `visit(name, prefix, …)` per entry with the entry's **virtual**
/// name (`renames` maps a renamed disk name back; the prefix pushed for a
/// child directory is virtual too); child dirs are descended when
/// `recursive`. Every directory node is visited before any of its descendants.
/// `skip_unreadable`: a descendant directory that can't be opened (permission
/// denied, vanished mid-walk) is skipped instead of failing the walk — its own
/// entry was already visited. The root is never skipped: an unreadable scope
/// is an error for every caller.
/// Synchronous — run on a blocking thread.
pub(crate) fn walk_tree(
    root: &Path,
    prefix: &str,
    recursive: bool,
    want_mtime: bool,
    skip_unreadable: bool,
    renames: &WalkRenames,
    visit: &mut WalkVisitor<'_>,
) -> Result<(), StorageError> {
    let mut stack: Vec<(PathBuf, String)> = vec![(root.to_path_buf(), prefix.to_string())];
    let mut is_root = true;
    while let Some((dir, pfx)) = stack.pop() {
        let skippable = skip_unreadable && !std::mem::take(&mut is_root);
        let scanned = scan_dir(&dir, want_mtime, &mut |disk_name, kind, size, mtime, mode| {
            let disk = disk_name.to_string_lossy();
            let renamed = if renames.is_empty() {
                None
            } else if pfx.is_empty() {
                renames.get(disk.as_ref())
            } else {
                renames.get(&format!("{pfx}/{disk}"))
            };
            let name = renamed.map(String::as_str).unwrap_or(&disk);
            visit(name, &pfx, kind, size, mtime, mode)?;
            if recursive && kind == EntryKind::Dir {
                let child = if pfx.is_empty() {
                    name.to_string()
                } else {
                    format!("{pfx}/{name}")
                };
                stack.push((dir.join(disk_name), child));
            }
            Ok(())
        });
        match scanned {
            Err(StorageError::Io(e)) if skippable && is_unreadable_dir(&e) => {
                tracing::debug!(dir = %dir.display(), error = %e, "walk: skipping unreadable directory");
            }
            other => other?,
        }
    }
    Ok(())
}

/// A directory the walk can step over: no access, or gone since its parent
/// was listed.
fn is_unreadable_dir(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        std::io::ErrorKind::PermissionDenied | std::io::ErrorKind::NotFound
    )
}

/// Spawn a listing producer on a blocking thread and return its batch
/// stream — the single spawn behind every local `get_dir`/`search`. `None`
/// batch size emits everything in one batch. The producer pushes into the
/// sink; `Cancelled` is either a dropped receiver (flush's send then fails,
/// ignored) or a filtered walk that spent its match budget (the buffered
/// tail must still go out) — flush covers both.
pub(crate) fn spawn_listing(
    max_batch_size: Option<usize>,
    produce: impl FnOnce(&mut BatchSink) -> Result<(), StorageError> + Send + 'static,
) -> mpsc::Receiver<Result<Vec<StorageObjectInfo>, StorageError>> {
    let limit = max_batch_size.filter(|n| *n > 0).unwrap_or(usize::MAX);
    let (tx, rx) = mpsc::channel(4);
    tokio::task::spawn_blocking(move || {
        let mut sink = BatchSink {
            tx,
            buf: Vec::new(),
            limit,
        };
        match produce(&mut sink) {
            Ok(()) | Err(StorageError::Cancelled) => sink.flush(),
            Err(e) => sink.send_err(e),
        }
    });
    rx
}

/// In-flight multipart records overlaid on a listing: key → its landed parts.
/// A landed `FsDest` lists its own in-progress files this way
/// (`is_complete = false` + parts); a source has none and passes an empty map.
pub(crate) type IncompleteParts = HashMap<String, Vec<StoragePartInfo>>;

/// Walk `root` (real path) under virtual `prefix` into `sink`: one entry per
/// node, keyed `prefix/name` (virtual names throughout: `renames` maps the
/// renamed disk names back, so the keys, the `incomplete` overlay and the
/// search needle all meet on the virtual key), filtered by `filter` when
/// present, overlaid with `incomplete`. A filtered walk is a search: it
/// steps over unreadable subdirectories (see `walk_tree`) — a listing or a
/// transfer must not silently lose a subtree, a search just finds what it
/// can read. Synchronous — run inside `spawn_listing`.
pub(crate) fn walk_into_sink(
    root: &Path,
    prefix: &str,
    recursive: bool,
    sink: &mut BatchSink,
    filter: &mut Option<SearchFilter>,
    incomplete: &IncompleteParts,
    renames: &WalkRenames,
) -> Result<(), StorageError> {
    let is_search = filter.is_some();
    walk_tree(root, prefix, recursive, true, is_search, renames, &mut |nm, pfx, k, s, m, mode| {
        push_filtered(sink, filter, nm, || {
            let key = if pfx.is_empty() {
                nm.to_string()
            } else {
                format!("{pfx}/{nm}")
            };
            let mut soi = soi_from(&key, k, s, m, mode);
            if let Some(parts) = incomplete.get(&key) {
                soi.is_complete = false;
                soi.parts = Some(parts.clone());
            }
            soi
        })
    })
}

/// macOS/iOS bulk directory enumeration via `getattrlistbulk`: one syscall
/// returns many entries with type + data length [+ mtime] inline, eliminating
/// the per-entry `lstat`. Falls back to `scan_dir_std` on a filesystem that
/// doesn't support the call.
#[cfg(any(target_os = "macos", target_os = "ios"))]
mod darwin_bulk {
    use super::{EntryKind, StorageError, scan_dir_std};
    use std::ffi::{CString, OsStr};
    use std::os::unix::ffi::OsStrExt;
    use std::path::Path;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    // <sys/attr.h>
    const ATTR_BIT_MAP_COUNT: u16 = 5;
    const ATTR_CMN_RETURNED_ATTRS: u32 = 0x8000_0000;
    const ATTR_CMN_NAME: u32 = 0x0000_0001;
    const ATTR_CMN_OBJTYPE: u32 = 0x0000_0008;
    const ATTR_CMN_MODTIME: u32 = 0x0000_0400;
    const ATTR_CMN_ACCESSMASK: u32 = 0x0002_0000;
    const ATTR_FILE_DATALENGTH: u32 = 0x0000_0200;
    const FSOPT_NOFOLLOW: u64 = 0x0000_0008;
    // <sys/vnode.h> enum vtype
    const VDIR: u32 = 2;
    const VLNK: u32 = 5;

    #[repr(C)]
    struct Attrlist {
        bitmapcount: u16,
        reserved: u16,
        commonattr: u32,
        volattr: u32,
        dirattr: u32,
        fileattr: u32,
        forkattr: u32,
    }
    #[repr(C)]
    #[derive(Clone, Copy)]
    struct AttributeSet {
        commonattr: u32,
        volattr: u32,
        dirattr: u32,
        fileattr: u32,
        forkattr: u32,
    }
    #[repr(C)]
    #[derive(Clone, Copy)]
    struct AttrReference {
        attr_dataoffset: i32,
        attr_length: u32,
    }

    unsafe extern "C" {
        fn getattrlistbulk(
            dirfd: libc::c_int,
            attr_list: *mut libc::c_void,
            attr_buf: *mut libc::c_void,
            attr_buf_size: libc::size_t,
            options: u64,
        ) -> libc::c_int;
    }

    /// Closes the dir fd on drop.
    struct FdGuard(libc::c_int);
    impl Drop for FdGuard {
        fn drop(&mut self) {
            unsafe { libc::close(self.0) };
        }
    }

    #[inline]
    unsafe fn read<T: Copy>(p: *const u8) -> T {
        unsafe { (p as *const T).read_unaligned() }
    }

    pub fn scan_dir(dir: &Path, want_mtime: bool, visit: &mut super::EntryVisitor<'_>) -> Result<(), StorageError> {
        let cpath = CString::new(dir.as_os_str().as_bytes())
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "path contains NUL"))?;
        let fd = unsafe { libc::open(cpath.as_ptr(), libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        let _guard = FdGuard(fd);

        let mut commonattr = ATTR_CMN_RETURNED_ATTRS | ATTR_CMN_NAME | ATTR_CMN_OBJTYPE;
        if want_mtime {
            commonattr |= ATTR_CMN_MODTIME | ATTR_CMN_ACCESSMASK;
        }
        let mut attrlist = Attrlist {
            bitmapcount: ATTR_BIT_MAP_COUNT,
            reserved: 0,
            commonattr,
            volattr: 0,
            dirattr: 0,
            fileattr: ATTR_FILE_DATALENGTH,
            forkattr: 0,
        };

        let mut buf = [0u8; 64 * 1024];
        let mut consumed_any = false;
        loop {
            let n = unsafe {
                getattrlistbulk(
                    fd,
                    &mut attrlist as *mut Attrlist as *mut libc::c_void,
                    buf.as_mut_ptr() as *mut libc::c_void,
                    buf.len() as libc::size_t,
                    FSOPT_NOFOLLOW,
                )
            };
            if n < 0 {
                let e = std::io::Error::last_os_error();
                // FS doesn't support the bulk call → fall back to readdir for this
                // dir (only safe before we've consumed any entry).
                if !consumed_any
                    && matches!(
                        e.raw_os_error(),
                        Some(libc::ENOTSUP) | Some(libc::EINVAL) | Some(libc::ENOTDIR)
                    )
                {
                    return scan_dir_std(dir, want_mtime, visit);
                }
                return Err(e.into());
            }
            if n == 0 {
                break; // enumeration complete
            }
            consumed_any = true;

            let mut p: *const u8 = buf.as_ptr();
            for _ in 0..n {
                let entry = p;
                let length = unsafe { read::<u32>(entry) } as usize;
                let mut field = unsafe { entry.add(4) };

                // ATTR_CMN_RETURNED_ATTRS is always first; it says which requested
                // attrs actually follow (canonical order, no gaps).
                let returned: AttributeSet = unsafe { read(field) };
                field = unsafe { field.add(std::mem::size_of::<AttributeSet>()) };

                let mut name: Option<&[u8]> = None;
                let mut objtype: u32 = 0;
                let mut size: u64 = 0;
                let mut mtime: Option<SystemTime> = None;
                let mut mode: Option<u32> = None;

                if returned.commonattr & ATTR_CMN_NAME != 0 {
                    let aref: AttrReference = unsafe { read(field) };
                    let nptr = unsafe { field.offset(aref.attr_dataoffset as isize) };
                    let raw = unsafe { std::slice::from_raw_parts(nptr, aref.attr_length as usize) };
                    name = Some(raw.strip_suffix(&[0]).unwrap_or(raw)); // drop trailing NUL
                    field = unsafe { field.add(std::mem::size_of::<AttrReference>()) };
                }
                if returned.commonattr & ATTR_CMN_OBJTYPE != 0 {
                    objtype = unsafe { read::<u32>(field) };
                    field = unsafe { field.add(std::mem::size_of::<u32>()) };
                }
                if returned.commonattr & ATTR_CMN_MODTIME != 0 {
                    // struct timespec { tv_sec: i64, tv_nsec: i64 } on 64-bit Darwin.
                    let sec = unsafe { read::<i64>(field) };
                    let nsec = unsafe { read::<i64>(field.add(8)) };
                    field = unsafe { field.add(16) };
                    if sec >= 0 {
                        mtime = UNIX_EPOCH.checked_add(Duration::new(sec as u64, nsec.clamp(0, 999_999_999) as u32));
                    }
                }
                if returned.commonattr & ATTR_CMN_ACCESSMASK != 0 {
                    // u_int32_t: the st_mode permission bits.
                    mode = Some(unsafe { read::<u32>(field) } & 0o7777);
                    field = unsafe { field.add(std::mem::size_of::<u32>()) };
                }
                if returned.fileattr & ATTR_FILE_DATALENGTH != 0 {
                    size = unsafe { read::<i64>(field) } as u64;
                }

                let kind = match objtype {
                    VDIR => EntryKind::Dir,
                    VLNK => EntryKind::Symlink,
                    _ => EntryKind::File,
                };
                let nm = name.map(OsStr::from_bytes).unwrap_or_else(|| OsStr::new(""));
                visit(nm, kind, size, mtime, mode)?;

                p = unsafe { entry.add(length) };
            }
        }
        Ok(())
    }
}
