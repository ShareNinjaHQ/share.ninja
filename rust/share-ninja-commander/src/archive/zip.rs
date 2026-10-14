//! A zip archive as a read-only storage, parsed here rather than through
//! the `zip` crate: the crate hands out an entry's metadata only via a
//! `ZipFile`, whose construction reads that entry's local header — one
//! range read per entry, scattered over the whole archive. The central
//! directory at the archive's end already carries everything a listing
//! needs, contiguously, so `connect` reads just that (plus the zip64
//! records when present); an entry's local header — the only place its
//! data offset can be derived from — is read once, lazily, before its first
//! read. A stored entry's bytes are then a direct range on the containing
//! storage; a deflated one is inflated on a blocking thread (deflate has no
//! random access: a read at an offset inflates from the entry's start).

use std::collections::HashMap;
use std::io::Read;
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use tokio::sync::{RwLock, mpsc};

use share_ninja_core::transfers::{SearchOptions, SearchResult, StorageSearchQuery, StorageSourceType};
use share_ninja_core::{Storage, StorageError, StorageFileType, StorageMetaState, StorageObjectInfo, StorageSource};

use crate::storage_reader::StorageReader;

use super::{ArchiveIndex, clean_key};

const SIG_EOCD: u32 = 0x0605_4b50;
const SIG_EOCD64_LOCATOR: u32 = 0x0706_4b50;
const SIG_EOCD64: u32 = 0x0606_4b50;
const SIG_CENTRAL: u32 = 0x0201_4b50;
const SIG_LOCAL: u32 = 0x0403_4b50;
/// Fixed part of the end-of-central-directory record; a comment of up to
/// 64 KiB may follow it.
const EOCD_LEN: u64 = 22;
const MAX_COMMENT: u64 = 0xFFFF;
const LOCAL_HEADER_LEN: u64 = 30;
const METHOD_STORED: u16 = 0;
const METHOD_DEFLATE: u16 = 8;
/// General-purpose flag bits.
const FLAG_ENCRYPTED: u16 = 1;
const FLAG_UTF8: u16 = 1 << 11;
/// Extra field ids.
const EXTRA_ZIP64: u16 = 0x0001;
const EXTRA_EXT_TIMESTAMP: u16 = 0x5455;
/// Unix mode bits in the high half of the external attributes.
const S_IFMT: u32 = 0o170000;
const S_IFDIR: u32 = 0o040000;
const S_IFLNK: u32 = 0o120000;
/// Chunk an inflating read hands down the channel.
const INFLATE_CHUNK: usize = 256 * 1024;

fn bad(what: &str) -> StorageError {
    StorageError::Other(format!("zip: {what}"))
}

/// Little-endian field reads over a byte slice, erroring on a short slice.
struct Fields<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Fields<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn bytes(&mut self, n: usize) -> Result<&'a [u8], StorageError> {
        let end = self
            .pos
            .checked_add(n)
            .filter(|e| *e <= self.buf.len())
            .ok_or_else(|| bad("truncated record"))?;
        let out = &self.buf[self.pos..end];
        self.pos = end;
        Ok(out)
    }

    fn u16(&mut self) -> Result<u16, StorageError> {
        Ok(u16::from_le_bytes(self.bytes(2)?.try_into().expect("2 bytes")))
    }

    fn u32(&mut self) -> Result<u32, StorageError> {
        Ok(u32::from_le_bytes(self.bytes(4)?.try_into().expect("4 bytes")))
    }

    fn u64(&mut self) -> Result<u64, StorageError> {
        Ok(u64::from_le_bytes(self.bytes(8)?.try_into().expect("8 bytes")))
    }

    fn skip(&mut self, n: usize) -> Result<(), StorageError> {
        self.bytes(n).map(|_| ())
    }
}

/// What the central directory says about one entry, beyond its
/// `StorageObjectInfo`.
struct ZipEntry {
    header_offset: u64,
    compressed_size: u64,
    method: u16,
    /// Where the entry's bytes start: derived from the local header on the
    /// first read (its name/extra lengths may differ from the central copy).
    data_start: OnceLock<u64>,
}

/// DOS date/time → a timestamp (the archive carries no zone; taken as UTC).
fn dos_datetime(time: u16, date: u16) -> Option<chrono::DateTime<chrono::Utc>> {
    let (sec, min, hour) = ((time & 0x1F) * 2, (time >> 5) & 0x3F, time >> 11);
    let (day, month, year) = (date & 0x1F, (date >> 5) & 0x0F, 1980 + (date >> 9));
    chrono::NaiveDate::from_ymd_opt(i32::from(year), u32::from(month), u32::from(day))?
        .and_hms_opt(u32::from(hour), u32::from(min), u32::from(sec.min(59)))
        .map(|dt| dt.and_utc())
}

/// The archive's extra fields: zip64 sizes/offset for the fields the
/// 32-bit record could not hold, and the extended-timestamp mtime.
struct Extras {
    uncompressed: Option<u64>,
    compressed: Option<u64>,
    header_offset: Option<u64>,
    mtime: Option<i64>,
}

fn parse_extras(extra: &[u8], uncompressed32: u32, compressed32: u32, offset32: u32) -> Result<Extras, StorageError> {
    let mut out = Extras {
        uncompressed: None,
        compressed: None,
        header_offset: None,
        mtime: None,
    };
    let mut f = Fields::new(extra);
    while f.pos + 4 <= extra.len() {
        let id = f.u16()?;
        let len = usize::from(f.u16()?);
        let body = f.bytes(len)?;
        let mut b = Fields::new(body);
        match id {
            EXTRA_ZIP64 => {
                // Only the fields saturated in the fixed record are present, in this order.
                if uncompressed32 == u32::MAX {
                    out.uncompressed = Some(b.u64()?);
                }
                if compressed32 == u32::MAX {
                    out.compressed = Some(b.u64()?);
                }
                if offset32 == u32::MAX {
                    out.header_offset = Some(b.u64()?);
                }
            }
            EXTRA_EXT_TIMESTAMP => {
                // The central copy carries only the mtime, when flag bit 0 is set.
                let flags = b.bytes(1)?[0];
                if flags & 1 != 0 && body.len() >= 5 {
                    out.mtime = Some(i64::from(b.u32()? as i32));
                }
            }
            _ => {}
        }
    }
    Ok(out)
}

/// Read `[offset, offset+len)` of the archive.
fn read_at(reader: &mut StorageReader, offset: u64, len: u64) -> Result<Vec<u8>, StorageError> {
    use std::io::{Seek, SeekFrom};
    reader.seek(SeekFrom::Start(offset)).map_err(StorageError::Io)?;
    let mut buf = vec![0u8; usize::try_from(len).map_err(|_| bad("record too large"))?];
    reader.read_exact(&mut buf).map_err(StorageError::Io)?;
    Ok(buf)
}

/// Locate and read the central directory: the EOCD record (searched back
/// through a possible comment), the zip64 locator + record when the EOCD's
/// fields are saturated, then the directory itself.
fn read_central_directory(reader: &mut StorageReader) -> Result<Vec<u8>, StorageError> {
    let size = reader.size();
    if size < EOCD_LEN {
        return Err(bad("not a zip (too short)"));
    }
    let tail_len = (EOCD_LEN + MAX_COMMENT).min(size);
    let tail_start = size - tail_len;
    let tail = read_at(reader, tail_start, tail_len)?;
    let eocd_in_tail = (0..=tail.len() - EOCD_LEN as usize)
        .rev()
        .find(|&i| u32::from_le_bytes(tail[i..i + 4].try_into().expect("4 bytes")) == SIG_EOCD)
        .ok_or_else(|| bad("not a zip (no end-of-central-directory record)"))?;
    let eocd_pos = tail_start + eocd_in_tail as u64;
    let mut f = Fields::new(&tail[eocd_in_tail..]);
    f.skip(4)?; // signature
    f.skip(2)?; // this disk
    f.skip(2)?; // disk with the central directory
    f.skip(2)?; // entries on this disk
    let entries32 = f.u16()?;
    let cd_size32 = f.u32()?;
    let cd_offset32 = f.u32()?;

    let (cd_offset, cd_size) = if entries32 == u16::MAX || cd_size32 == u32::MAX || cd_offset32 == u32::MAX {
        let locator_pos = eocd_pos.checked_sub(20).ok_or_else(|| bad("zip64 locator missing"))?;
        let locator = read_at(reader, locator_pos, 20)?;
        let mut l = Fields::new(&locator);
        if l.u32()? != SIG_EOCD64_LOCATOR {
            return Err(bad("zip64 locator missing"));
        }
        l.skip(4)?; // disk with the zip64 EOCD
        let eocd64_pos = l.u64()?;
        let eocd64 = read_at(reader, eocd64_pos, 56)?;
        let mut r = Fields::new(&eocd64);
        if r.u32()? != SIG_EOCD64 {
            return Err(bad("zip64 end-of-central-directory record missing"));
        }
        r.skip(8)?; // record size
        r.skip(2)?; // version made by
        r.skip(2)?; // version needed
        r.skip(4)?; // this disk
        r.skip(4)?; // disk with the central directory
        r.skip(8)?; // entries on this disk
        r.skip(8)?; // total entries
        let cd_size = r.u64()?;
        let cd_offset = r.u64()?;
        (cd_offset, cd_size)
    } else {
        (u64::from(cd_offset32), u64::from(cd_size32))
    };
    if cd_offset.checked_add(cd_size).is_none_or(|end| end > size) {
        return Err(bad("central directory out of bounds"));
    }
    read_at(reader, cd_offset, cd_size)
}

/// One central-directory record → the entry as the panel lists it, plus
/// what a read needs. `None` for records skipped with a warning.
fn parse_record(f: &mut Fields<'_>) -> Result<Option<(StorageObjectInfo, ZipEntry)>, StorageError> {
    if f.u32()? != SIG_CENTRAL {
        return Err(bad("bad central directory record"));
    }
    let version_made_by = f.u16()?;
    f.skip(2)?; // version needed
    let flags = f.u16()?;
    let method = f.u16()?;
    let dos_time = f.u16()?;
    let dos_date = f.u16()?;
    f.skip(4)?; // crc32
    let compressed32 = f.u32()?;
    let uncompressed32 = f.u32()?;
    let name_len = usize::from(f.u16()?);
    let extra_len = usize::from(f.u16()?);
    let comment_len = usize::from(f.u16()?);
    f.skip(2)?; // disk number start
    f.skip(2)?; // internal attributes
    let external_attrs = f.u32()?;
    let offset32 = f.u32()?;
    let name_raw = f.bytes(name_len)?;
    let extra = f.bytes(extra_len)?;
    f.skip(comment_len)?;

    // Names are UTF-8 by flag or by luck; anything else is shown lossily.
    let name = match std::str::from_utf8(name_raw) {
        Ok(s) => s.to_string(),
        Err(_) => {
            if flags & FLAG_UTF8 == 0 {
                tracing::warn!("zip: non-UTF-8 entry name shown lossily");
            }
            String::from_utf8_lossy(name_raw).into_owned()
        }
    };
    let Some(key) = clean_key(&name) else {
        tracing::warn!(name = %name, "zip: entry skipped (bad name)");
        return Ok(None);
    };
    if flags & FLAG_ENCRYPTED != 0 {
        tracing::warn!(name = %name, "zip: encrypted entry skipped");
        return Ok(None);
    }
    let extras = parse_extras(extra, uncompressed32, compressed32, offset32)?;
    let uncompressed = extras.uncompressed.unwrap_or(u64::from(uncompressed32));
    let compressed = extras.compressed.unwrap_or(u64::from(compressed32));
    let header_offset = extras.header_offset.unwrap_or(u64::from(offset32));

    let mut soi = StorageObjectInfo::new(&key);
    soi.size = uncompressed;
    // High byte of "version made by" is the host system: 3 = Unix, whose
    // mode bits sit in the high half of the external attributes.
    let unix_mode = if version_made_by >> 8 == 3 {
        Some(external_attrs >> 16)
    } else {
        None
    };
    soi.file_type = match unix_mode.map(|m| m & S_IFMT) {
        Some(S_IFLNK) => StorageFileType::Symlink,
        Some(S_IFDIR) => StorageFileType::Dir,
        _ if name.ends_with('/') => StorageFileType::Dir,
        _ => StorageFileType::Regular,
    };
    soi.attrs.posix_permissions = unix_mode.map(|m| i64::from(m & 0o7777));
    soi.attrs.mtime = match extras.mtime {
        Some(t) => chrono::DateTime::from_timestamp(t, 0),
        None => dos_datetime(dos_time, dos_date),
    };
    let entry = ZipEntry {
        header_offset,
        compressed_size: compressed,
        method,
        data_start: OnceLock::new(),
    };
    Ok(Some((soi, entry)))
}

fn parse(reader: &mut StorageReader) -> Result<Vec<(StorageObjectInfo, ZipEntry)>, StorageError> {
    let cd = read_central_directory(reader)?;
    let mut f = Fields::new(&cd);
    let mut out = Vec::new();
    while f.pos < cd.len() {
        if let Some(parsed) = parse_record(&mut f)? {
            out.push(parsed);
        }
    }
    Ok(out)
}

pub struct ZipSource {
    /// The storage the archive file lives on, and its key and size there.
    base: Arc<RwLock<dyn StorageSource>>,
    key: String,
    size: u64,
    index: ArchiveIndex,
    entries: HashMap<String, ZipEntry>,
    data_source_id: Option<String>,
}

impl ZipSource {
    pub fn new(base: Arc<RwLock<dyn StorageSource>>, key: String, size: u64) -> Self {
        Self {
            base,
            key,
            size,
            index: ArchiveIndex::default(),
            entries: HashMap::new(),
            data_source_id: None,
        }
    }

    /// `[offset, offset+len)` of the archive, drained.
    async fn read_range(&self, offset: u64, len: u64) -> Result<Vec<u8>, StorageError> {
        let mut rx = self
            .base
            .read()
            .await
            .get_streamed(&self.key, Some(offset), Some(len))
            .await;
        let mut out = Vec::with_capacity(len as usize);
        while let Some(chunk) = rx.recv().await {
            out.extend_from_slice(&chunk?);
        }
        if out.len() as u64 != len {
            return Err(bad("short read of a local header"));
        }
        Ok(out)
    }

    /// Where the entry's bytes start, from its local header (read once).
    async fn data_start(&self, entry: &ZipEntry) -> Result<u64, StorageError> {
        if let Some(start) = entry.data_start.get() {
            return Ok(*start);
        }
        let header = self.read_range(entry.header_offset, LOCAL_HEADER_LEN).await?;
        let mut f = Fields::new(&header);
        if f.u32()? != SIG_LOCAL {
            return Err(bad("bad local header"));
        }
        f.skip(22)?; // version, flags, method, time, date, crc, sizes
        let name_len = u64::from(f.u16()?);
        let extra_len = u64::from(f.u16()?);
        let start = entry.header_offset + LOCAL_HEADER_LEN + name_len + extra_len;
        if start
            .checked_add(entry.compressed_size)
            .is_none_or(|end| end > self.size)
        {
            return Err(bad("entry data out of bounds"));
        }
        Ok(*entry.data_start.get_or_init(|| start))
    }

    /// Inflate the entry on a blocking thread, dropping the first `skip`
    /// bytes and streaming the next `want` down the channel. The receiver
    /// dropping ends it.
    fn stream_deflated(
        &self,
        data_start: u64,
        compressed_size: u64,
        skip: u64,
        want: u64,
    ) -> mpsc::Receiver<Result<Vec<u8>, StorageError>> {
        let (tx, rx) = mpsc::channel(4);
        let mut reader = StorageReader::new(
            self.base.clone(),
            self.key.clone(),
            self.size,
            tokio::runtime::Handle::current(),
        );
        tokio::task::spawn_blocking(move || {
            use std::io::{Seek, SeekFrom};
            if let Err(e) = reader.seek(SeekFrom::Start(data_start)) {
                let _ = tx.blocking_send(Err(e.into()));
                return;
            }
            let mut inflater = flate2::read::DeflateDecoder::new(reader.take(compressed_size));
            let mut buf = vec![0u8; INFLATE_CHUNK];
            let mut to_skip = skip;
            while to_skip > 0 {
                let n = (to_skip as usize).min(buf.len());
                match inflater.read(&mut buf[..n]) {
                    Ok(0) => return, // entry shorter than its offset: nothing to send
                    Ok(n) => to_skip -= n as u64,
                    Err(e) => {
                        let _ = tx.blocking_send(Err(e.into()));
                        return;
                    }
                }
            }
            let mut left = want;
            while left > 0 {
                let n = (left as usize).min(buf.len());
                match inflater.read(&mut buf[..n]) {
                    Ok(0) => return,
                    Ok(n) => {
                        left -= n as u64;
                        if tx.blocking_send(Ok(buf[..n].to_vec())).is_err() {
                            return; // receiver dropped → cancel
                        }
                    }
                    Err(e) => {
                        let _ = tx.blocking_send(Err(e.into()));
                        return;
                    }
                }
            }
        });
        rx
    }
}

#[async_trait]
impl Storage for ZipSource {
    async fn connect(&mut self) -> Result<StorageMetaState, StorageError> {
        let mut reader = StorageReader::new(
            self.base.clone(),
            self.key.clone(),
            self.size,
            tokio::runtime::Handle::current(),
        );
        let parsed = tokio::task::spawn_blocking(move || parse(&mut reader))
            .await
            .map_err(|e| StorageError::Other(format!("zip parse task: {e}")))??;
        let mut items = Vec::with_capacity(parsed.len());
        let mut entries = HashMap::with_capacity(parsed.len());
        for (soi, entry) in parsed {
            // A key listed twice keeps its first entry (as the index does).
            entries.entry(soi.key.clone()).or_insert(entry);
            items.push(soi);
        }
        self.index = ArchiveIndex::new(items);
        self.entries = entries;
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
                    .send(Err(StorageError::NotImplemented("zip getDir start_after".into())))
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
impl StorageSource for ZipSource {
    fn source_name(&self) -> &str {
        "Zip"
    }

    fn source_type(&self) -> StorageSourceType {
        StorageSourceType::FileSystem
    }

    /// `[offset, offset+length)` of the entry: a stored one is that range
    /// of the archive on the base storage's own stream; a deflated one is
    /// inflated from its start up to there. A symlink's bytes are its target.
    async fn get_streamed(
        &self,
        object: &str,
        offset: Option<u64>,
        length: Option<u64>,
    ) -> mpsc::Receiver<Result<Vec<u8>, StorageError>> {
        let fail = |e: StorageError| {
            let (tx, rx) = mpsc::channel(1);
            let _ = tx.try_send(Err(e));
            rx
        };
        let found = self
            .index
            .get(object)
            .filter(|s| s.file_type != StorageFileType::Dir)
            .zip(self.entries.get(object));
        let Some((soi, entry)) = found else {
            return fail(StorageError::NoSuchKey(object.to_string()));
        };
        let off = offset.unwrap_or(0).min(soi.size);
        let want = length.map(|l| l.min(soi.size - off)).unwrap_or(soi.size - off);
        let start = match self.data_start(entry).await {
            Ok(s) => s,
            Err(e) => return fail(e),
        };
        match entry.method {
            METHOD_STORED => {
                self.base
                    .read()
                    .await
                    .get_streamed(&self.key, Some(start + off), Some(want))
                    .await
            }
            METHOD_DEFLATE => self.stream_deflated(start, entry.compressed_size, off, want),
            other => fail(StorageError::NotImplemented(format!("zip compression method {other}"))),
        }
    }

    async fn get_symlink(&self, object: &str) -> Result<String, StorageError> {
        if self
            .index
            .get(object)
            .is_none_or(|s| s.file_type != StorageFileType::Symlink)
        {
            return Err(StorageError::NoSuchKey(object.to_string()));
        }
        let mut rx = self.get_streamed(object, None, None).await;
        let mut target = Vec::new();
        while let Some(chunk) = rx.recv().await {
            target.extend_from_slice(&chunk?);
        }
        Ok(String::from_utf8_lossy(&target).into_owned())
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use tokio::sync::RwLock;

    use share_ninja_core::fs::LocalFs;
    use share_ninja_core::transfers::{SearchOptions, StorageSearchQuery};
    use share_ninja_core::{Storage, StorageError, StorageFileType, StorageSource};

    /// A tree with the zip cases that matter — a deflated file bigger than
    /// a reader block, a stored one, a symlink, a nested directory, an
    /// empty directory — packed by Info-ZIP as `t.zip` (32-bit records)
    /// and `t64.zip` (forced zip64 records) beside it.
    fn build_zips() -> (PathBuf, Vec<u8>) {
        let tmp = std::env::temp_dir().join(format!(
            "sninja-zip-{}-{}",
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
        // Compressible but not trivially: a repeating pattern with a twist.
        let big: Vec<u8> = (0..1_500_000u32)
            .map(|i| ((i * 7) % 251) as u8 ^ (i / 1000) as u8)
            .collect();
        std::fs::write(src.join("sub/deep/b.bin"), &big).unwrap();
        std::fs::write(src.join("stored.bin"), &big[..300_000]).unwrap();
        std::os::unix::fs::symlink("a.txt", src.join("link")).unwrap();
        let zip = |archive: &Path, extra: &[&str]| {
            let status = std::process::Command::new("zip")
                .current_dir(&src)
                .args(["-q", "-r", "-y"])
                .args(extra)
                .arg(archive)
                .arg(".")
                .status()
                .expect("zip");
            assert!(status.success());
            // The stored file, added as is.
            let status = std::process::Command::new("zip")
                .current_dir(&src)
                .args(["-q", "-0"])
                .arg(archive)
                .arg("stored.bin")
                .status()
                .expect("zip -0");
            assert!(status.success());
        };
        zip(&tmp.join("t.zip"), &[]);
        zip(&tmp.join("t64.zip"), &["-fz"]);
        (tmp, big)
    }

    async fn drain(mut rx: tokio::sync::mpsc::Receiver<Result<Vec<u8>, StorageError>>) -> Vec<u8> {
        let mut out = Vec::new();
        while let Some(chunk) = rx.recv().await {
            out.extend_from_slice(&chunk.unwrap());
        }
        out
    }

    async fn check(tmp: &Path, archive: &str, big: &[u8]) {
        let mut fs = LocalFs::new(tmp);
        fs.connect().await.unwrap();
        let base: Arc<RwLock<dyn StorageSource>> = Arc::new(RwLock::new(fs));
        let size = base.read().await.get_attrs(archive).await.unwrap().size;
        let source = super::super::open(base, archive.to_string(), size).await.unwrap();

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
                ("link".to_string(), StorageFileType::Symlink),
                ("stored.bin".to_string(), StorageFileType::Regular),
                ("sub".to_string(), StorageFileType::Dir),
            ],
            "{archive}"
        );

        let src = source.read().await;
        let b = src.get_attrs("sub/deep/b.bin").await.unwrap();
        assert_eq!(b.size, big.len() as u64);
        assert!(
            b.attrs.mtime.is_some(),
            "mtime from the extended timestamp / DOS fields"
        );
        assert!(
            b.attrs.posix_permissions.is_some(),
            "unix mode from the external attributes"
        );
        assert!(matches!(src.get_attrs("nope").await, Err(StorageError::NoSuchKey(_))));
        assert_eq!(drain(src.get_streamed("a.txt", None, None).await).await, b"hello");
        // Deflated: whole, a middle range (inflated from the start, skipped
        // to), and a range past the end (clamped).
        assert_eq!(drain(src.get_streamed("sub/deep/b.bin", None, None).await).await, big);
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
        // Stored: the base storage's own range.
        assert_eq!(
            drain(src.get_streamed("stored.bin", Some(100_000), Some(50_000)).await).await,
            &big[100_000..150_000]
        );
        assert_eq!(
            drain(src.get_streamed("stored.bin", None, None).await).await,
            &big[..300_000]
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
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn zip_over_local_fs() {
        let (tmp, big) = build_zips();
        check(&tmp, "t.zip", &big).await;
        check(&tmp, "t64.zip", &big).await;
        std::fs::remove_dir_all(&tmp).unwrap();
    }

    #[test]
    fn dos_datetime_decodes() {
        // 2024-03-05 14:30:20 → date: (44<<9)|(3<<5)|5, time: (14<<11)|(30<<5)|10
        let dt = super::dos_datetime((14 << 11) | (30 << 5) | 10, (44 << 9) | (3 << 5) | 5).unwrap();
        assert_eq!(dt.to_rfc3339(), "2024-03-05T14:30:20+00:00");
    }
}
