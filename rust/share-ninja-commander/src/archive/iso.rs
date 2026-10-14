//! An ISO 9660 image as a read-only storage. `connect` reads the volume
//! descriptors (sector 16 on) and walks the directory tree through a
//! [`StorageReader`] — directory extents only, never file data — and every
//! file is one or more contiguous extents, so a read is a direct range on
//! the containing storage. Names come from the best source the image has:
//! Rock Ridge `NM` entries, else the Joliet tree (UCS-2 names), else the
//! plain 8.3 names; Rock Ridge `PX`/`TF`/`SL` (mode, mtime, symlink target)
//! always come from the primary tree, matched to a Joliet-named file by its
//! extent (both trees point at the same data). UDF-only images and
//! interleaved files are not opened.

use std::collections::{HashMap, HashSet};
use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{RwLock, mpsc};

use share_ninja_core::transfers::{SearchOptions, SearchResult, StorageSearchQuery, StorageSourceType};
use share_ninja_core::{Storage, StorageError, StorageFileType, StorageMetaState, StorageObjectInfo, StorageSource};

use crate::storage_reader::StorageReader;

use super::{ArchiveIndex, clean_key};

const DESCRIPTOR_START: u64 = 16;
const DESC_PRIMARY: u8 = 1;
const DESC_SUPPLEMENTARY: u8 = 2;
const DESC_TERMINATOR: u8 = 255;
/// Directory record flags.
const FLAG_DIRECTORY: u8 = 1 << 1;
const FLAG_MULTI_EXTENT: u8 = 1 << 7;
/// Unix mode bits (Rock Ridge `PX`).
const S_IFMT: u32 = 0o170000;
const S_IFDIR: u32 = 0o040000;
const S_IFLNK: u32 = 0o120000;
/// Nested `CE` continuation areas followed at most this deep.
const MAX_CONTINUATIONS: u32 = 8;

fn bad(what: &str) -> StorageError {
    StorageError::Other(format!("iso: {what}"))
}

fn u16_le(b: &[u8], at: usize) -> u16 {
    u16::from_le_bytes([b[at], b[at + 1]])
}

fn u32_le(b: &[u8], at: usize) -> u32 {
    u32::from_le_bytes([b[at], b[at + 1], b[at + 2], b[at + 3]])
}

/// The 7-byte directory-record timestamp: years since 1900, month, day,
/// hour, minute, second, zone in quarter hours from GMT (signed).
fn record_time(b: &[u8]) -> Option<chrono::DateTime<chrono::Utc>> {
    if b.len() < 7 || b[1] == 0 {
        return None;
    }
    let local = chrono::NaiveDate::from_ymd_opt(1900 + i32::from(b[0]), u32::from(b[1]), u32::from(b[2]))?
        .and_hms_opt(u32::from(b[3]), u32::from(b[4]), u32::from(b[5].min(59)))?;
    let zone = chrono::Duration::minutes(15 * i64::from(b[6] as i8));
    Some((local - zone).and_utc())
}

/// What the Rock Ridge system-use entries of one record say.
#[derive(Default)]
struct RockRidge {
    name: Option<String>,
    symlink: Option<String>,
    mode: Option<u32>,
    mtime: Option<chrono::DateTime<chrono::Utc>>,
    /// `CL`: the directory's real extent (deep-directory relocation).
    child_link: Option<u32>,
    /// `RE`: a relocated directory's placeholder — hidden here.
    relocated: bool,
}

/// One directory record, as the walk uses it.
struct Record {
    name: Vec<u8>,
    is_dir: bool,
    /// Data extent (after any extended-attribute blocks) and its length.
    lba: u32,
    len: u32,
    multi_extent: bool,
    interleaved: bool,
    mtime: Option<chrono::DateTime<chrono::Utc>>,
    rr: RockRidge,
}

/// A record with its data extents `(lba, len)` — one, or several for a
/// multi-extent file.
type Listed = (Record, Vec<(u32, u32)>);

/// The image's geometry and where its trees start.
struct Volume {
    block: u64,
    root: (u32, u32),
    joliet_root: Option<(u32, u32)>,
    /// Rock Ridge `SP` skip length: bytes to pass over at the start of
    /// every system-use area.
    susp_skip: usize,
}

fn read_at(reader: &mut StorageReader, offset: u64, len: u64) -> Result<Vec<u8>, StorageError> {
    reader.seek(SeekFrom::Start(offset)).map_err(StorageError::Io)?;
    let mut buf = vec![0u8; usize::try_from(len).map_err(|_| bad("extent too large"))?];
    reader.read_exact(&mut buf).map_err(StorageError::Io)?;
    Ok(buf)
}

/// The root record's extent and length off a volume descriptor.
fn root_of(desc: &[u8]) -> (u32, u32) {
    let record = &desc[156..190];
    (u32_le(record, 2) + u32::from(record[1]), u32_le(record, 10))
}

fn read_volume(reader: &mut StorageReader) -> Result<Volume, StorageError> {
    let mut block: u64 = 2048;
    let mut root = None;
    let mut joliet_root = None;
    for i in DESCRIPTOR_START.. {
        let desc = read_at(reader, i * 2048, 2048)?;
        if &desc[1..6] != b"CD001" {
            return Err(bad("not an ISO 9660 image"));
        }
        match desc[0] {
            DESC_PRIMARY => {
                block = u64::from(u16_le(&desc, 128));
                if block == 0 {
                    return Err(bad("zero logical block size"));
                }
                root = Some(root_of(&desc));
            }
            // Joliet: a supplementary descriptor with a UCS-2 escape sequence.
            DESC_SUPPLEMENTARY if matches!(&desc[88..91], b"%/@" | b"%/C" | b"%/E") => {
                joliet_root = Some(root_of(&desc));
            }
            DESC_TERMINATOR => break,
            _ => {}
        }
        if i - DESCRIPTOR_START > 64 {
            return Err(bad("no descriptor terminator"));
        }
    }
    let root = root.ok_or_else(|| bad("no primary volume descriptor"))?;
    Ok(Volume {
        block,
        root,
        joliet_root,
        susp_skip: 0,
    })
}

/// Parse a system-use area: Rock Ridge entries, following `CE`
/// continuations. `SP` (on the root's `.` record) yields the skip length.
fn parse_susp(
    reader: &mut StorageReader,
    block: u64,
    area: &[u8],
    rr: &mut RockRidge,
    skip_out: &mut Option<usize>,
    depth: u32,
) -> Result<(), StorageError> {
    let mut i = 0;
    let mut name_parts: Vec<u8> = Vec::new();
    let mut link_parts: Vec<String> = Vec::new();
    let mut link_continues = false;
    while i + 4 <= area.len() {
        let sig = &area[i..i + 2];
        let len = usize::from(area[i + 2]);
        if len < 4 || i + len > area.len() {
            break;
        }
        let data = &area[i + 4..i + len];
        match sig {
            b"SP" if data.len() >= 3 && data[0] == 0xBE && data[1] == 0xEF => {
                *skip_out = Some(usize::from(data[2]));
            }
            b"NM" if !data.is_empty() => {
                let flags = data[0];
                // Bit 1 = ".", bit 2 = ".." — names the record has anyway.
                if flags & 0b110 == 0 {
                    name_parts.extend_from_slice(&data[1..]);
                    if flags & 1 == 0 {
                        rr.name = Some(String::from_utf8_lossy(&name_parts).into_owned());
                    }
                }
            }
            b"SL" if !data.is_empty() => {
                let mut j = 1;
                while j + 2 <= data.len() {
                    let cflags = data[j];
                    let clen = usize::from(data[j + 1]);
                    let content = data.get(j + 2..j + 2 + clen).unwrap_or(&[]);
                    let piece = match cflags & 0b1110 {
                        0b0010 => ".".to_string(),
                        0b0100 => "..".to_string(),
                        0b1000 => "/".to_string(),
                        _ => String::from_utf8_lossy(content).into_owned(),
                    };
                    // A continued component joins the previous piece; a
                    // fresh one is a new path component.
                    if link_continues {
                        if let Some(last) = link_parts.last_mut() {
                            last.push_str(&piece);
                        }
                    } else {
                        link_parts.push(piece);
                    }
                    link_continues = cflags & 1 != 0;
                    j += 2 + clen;
                }
                if data[0] & 1 == 0 {
                    let mut target = String::new();
                    for (n, part) in link_parts.iter().enumerate() {
                        if part == "/" {
                            target.push('/');
                        } else {
                            if n > 0 && !target.ends_with('/') {
                                target.push('/');
                            }
                            target.push_str(part);
                        }
                    }
                    rr.symlink = Some(target);
                }
            }
            b"PX" if data.len() >= 8 => {
                rr.mode = Some(u32_le(data, 0));
            }
            b"TF" if !data.is_empty() => {
                let flags = data[0];
                let long_form = flags & (1 << 7) != 0;
                let stamp_len = if long_form { 17 } else { 7 };
                let mut at = 1;
                // Creation (bit 0) precedes modify (bit 1).
                if flags & 1 != 0 {
                    at += stamp_len;
                }
                if flags & 2 != 0 && !long_form && at + 7 <= data.len() {
                    rr.mtime = record_time(&data[at..at + 7]);
                }
            }
            b"CL" if data.len() >= 4 => {
                rr.child_link = Some(u32_le(data, 0));
            }
            b"RE" => {
                rr.relocated = true;
            }
            b"CE" if data.len() >= 24 && depth < MAX_CONTINUATIONS => {
                let (ce_block, ce_offset, ce_len) = (u32_le(data, 0), u32_le(data, 8), u32_le(data, 16));
                let cont = read_at(
                    reader,
                    u64::from(ce_block) * block + u64::from(ce_offset),
                    u64::from(ce_len),
                )?;
                parse_susp(reader, block, &cont, rr, skip_out, depth + 1)?;
            }
            _ => {}
        }
        i += len;
    }
    Ok(())
}

/// The records of one directory extent (`.` and `..` dropped), consecutive
/// multi-extent pieces of one file merged into its first record.
fn parse_directory(
    reader: &mut StorageReader,
    vol: &mut Volume,
    lba: u32,
    len: u32,
    is_root: bool,
) -> Result<Vec<Listed>, StorageError> {
    let block = vol.block;
    let dir = read_at(reader, u64::from(lba) * block, u64::from(len))?;
    let mut out: Vec<Listed> = Vec::new();
    let mut i = 0usize;
    let mut first = true;
    while i < dir.len() {
        let rec_len = usize::from(dir[i]);
        if rec_len == 0 {
            // Records never straddle a sector: skip to the next one.
            i = (i / block as usize + 1) * block as usize;
            continue;
        }
        if rec_len < 34 || i + rec_len > dir.len() {
            return Err(bad("bad directory record"));
        }
        let r = &dir[i..i + rec_len];
        i += rec_len;
        let name_len = usize::from(r[32]);
        let name = r[33..33 + name_len].to_vec();
        let su_start = 33 + name_len + usize::from(name_len % 2 == 0);
        let flags = r[25];
        let mut rr = RockRidge::default();
        if su_start < r.len() {
            let mut skip_out = None;
            let area = &r[su_start + vol.susp_skip.min(r.len() - su_start)..];
            parse_susp(reader, block, area, &mut rr, &mut skip_out, 0)?;
            if let Some(skip) = skip_out
                && is_root
                && first
            {
                vol.susp_skip = skip;
            }
        }
        first = false;
        if name == [0] || name == [1] {
            continue;
        }
        let record = Record {
            name,
            is_dir: flags & FLAG_DIRECTORY != 0,
            lba: u32_le(r, 2) + u32::from(r[1]),
            len: u32_le(r, 10),
            multi_extent: flags & FLAG_MULTI_EXTENT != 0,
            interleaved: r[26] != 0,
            mtime: record_time(&r[18..25]),
            rr,
        };
        // A continuation piece of the previous file (same name, that one
        // flagged as not final): another extent of it.
        if let Some((prev, extents)) = out.last_mut()
            && prev.name == record.name
            && !prev.is_dir
            && (prev.multi_extent || extents.len() > 1)
        {
            extents.push((record.lba, record.len));
            prev.multi_extent = record.multi_extent;
            continue;
        }
        let extents = vec![(record.lba, record.len)];
        out.push((record, extents));
    }
    Ok(out)
}

/// A plain ISO 9660 identifier → a name: the `;version` and a trailing `.`
/// (empty extension) go.
fn plain_name(raw: &[u8]) -> String {
    let s = String::from_utf8_lossy(raw);
    let s = s.split(';').next().unwrap_or(&s);
    s.trim_end_matches('.').to_string()
}

/// A Joliet identifier (UCS-2, big-endian) → a name, same trimming.
fn joliet_name(raw: &[u8]) -> String {
    let units: Vec<u16> = raw.as_chunks::<2>().0.iter().map(|c| u16::from_be_bytes(*c)).collect();
    let s = String::from_utf16_lossy(&units);
    let s = s.split(';').next().unwrap_or(&s);
    s.trim_end_matches('.').to_string()
}

/// One walked entry.
struct Node {
    key: String,
    is_dir: bool,
    /// Byte extents of a file's data, in order.
    extents: Vec<(u64, u64)>,
    size: u64,
    mtime: Option<chrono::DateTime<chrono::Utc>>,
    rr: RockRidge,
}

/// Walk a tree from `root`, naming entries per `joliet`; Rock Ridge names
/// win when present. Deep-directory relocations (`CL`/`RE`) are honoured.
fn walk_tree(
    reader: &mut StorageReader,
    vol: &mut Volume,
    root: (u32, u32),
    joliet: bool,
) -> Result<Vec<Node>, StorageError> {
    let mut nodes: Vec<Node> = Vec::new();
    let mut seen: HashSet<u32> = HashSet::new();
    let mut pending: Vec<(u32, u32, String, bool)> = vec![(root.0, root.1, String::new(), true)];
    while let Some((lba, len, prefix, is_root)) = pending.pop() {
        if !seen.insert(lba) {
            continue;
        }
        for (record, extents) in parse_directory(reader, vol, lba, len, is_root)? {
            if record.rr.relocated {
                continue;
            }
            if record.interleaved {
                tracing::warn!("iso: interleaved file skipped");
                continue;
            }
            let name = match &record.rr.name {
                Some(n) => n.clone(),
                None if joliet => joliet_name(&record.name),
                None => plain_name(&record.name),
            };
            let key = if prefix.is_empty() {
                name
            } else {
                format!("{prefix}/{name}")
            };
            let Some(key) = clean_key(&key) else {
                continue;
            };
            let is_dir = record.is_dir;
            if is_dir {
                let dir_lba = record.rr.child_link.unwrap_or(record.lba);
                pending.push((dir_lba, record.len, key.clone(), false));
            }
            let byte_extents: Vec<(u64, u64)> = extents
                .iter()
                .map(|(l, n)| (u64::from(*l) * vol.block, u64::from(*n)))
                .collect();
            let size = byte_extents.iter().map(|(_, n)| n).sum();
            nodes.push(Node {
                key,
                is_dir,
                extents: byte_extents,
                size,
                mtime: record.mtime,
                rr: record.rr,
            });
        }
    }
    Ok(nodes)
}

/// What the walk yields.
struct Walked {
    items: Vec<StorageObjectInfo>,
    data: HashMap<String, Vec<(u64, u64)>>,
    links: HashMap<String, String>,
}

fn walk(mut reader: StorageReader) -> Result<Walked, StorageError> {
    let mut vol = read_volume(&mut reader)?;
    let root = vol.root;
    let primary = walk_tree(&mut reader, &mut vol, root, false)?;
    let has_rr_names = primary.iter().any(|n| n.rr.name.is_some());
    // Names: Rock Ridge if the image has them, else Joliet, else plain.
    // Attributes always from the primary tree, matched by data extent
    // when the names come from Joliet.
    let nodes = match (has_rr_names, vol.joliet_root) {
        (false, Some(jroot)) => {
            let attrs: HashMap<u64, &Node> = primary
                .iter()
                .filter(|n| !n.is_dir)
                .filter_map(|n| n.extents.first().map(|(off, _)| (*off, n)))
                .collect();
            let mut joliet = walk_tree(&mut reader, &mut vol, jroot, true)?;
            for node in joliet.iter_mut().filter(|n| !n.is_dir) {
                if let Some(p) = node.extents.first().and_then(|(off, _)| attrs.get(off)) {
                    node.rr = RockRidge {
                        mode: p.rr.mode,
                        mtime: p.rr.mtime,
                        symlink: p.rr.symlink.clone(),
                        ..RockRidge::default()
                    };
                }
            }
            joliet
        }
        _ => primary,
    };

    let mut items = Vec::with_capacity(nodes.len());
    let mut data = HashMap::new();
    let mut links = HashMap::new();
    for node in nodes {
        let mut soi = StorageObjectInfo::new(&node.key);
        soi.file_type = match node.rr.mode.map(|m| m & S_IFMT) {
            Some(S_IFLNK) => StorageFileType::Symlink,
            Some(S_IFDIR) => StorageFileType::Dir,
            _ if node.is_dir => StorageFileType::Dir,
            _ => StorageFileType::Regular,
        };
        soi.attrs.mtime = node.rr.mtime.or(node.mtime);
        soi.attrs.posix_permissions = node.rr.mode.map(|m| i64::from(m & 0o7777));
        match soi.file_type {
            StorageFileType::Dir => {}
            StorageFileType::Symlink => {
                // An `SL` target, else the link's data is the target.
                match node.rr.symlink {
                    Some(target) => {
                        soi.size = target.len() as u64;
                        links.insert(node.key.clone(), target);
                    }
                    None => {
                        soi.size = node.size;
                        data.insert(node.key.clone(), node.extents);
                    }
                }
            }
            _ => {
                soi.size = node.size;
                data.insert(node.key.clone(), node.extents);
            }
        }
        items.push(soi);
    }
    Ok(Walked { items, data, links })
}

pub struct IsoSource {
    /// The storage the image lives on, and its key and size there.
    base: Arc<RwLock<dyn StorageSource>>,
    key: String,
    size: u64,
    index: ArchiveIndex,
    /// Byte extents of every file's data, by key, in order.
    data: HashMap<String, Vec<(u64, u64)>>,
    /// Rock Ridge symlink targets by key; a symlink absent here carries
    /// its target as its data.
    links: HashMap<String, String>,
    data_source_id: Option<String>,
}

impl IsoSource {
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
impl Storage for IsoSource {
    async fn connect(&mut self) -> Result<StorageMetaState, StorageError> {
        let reader = StorageReader::new(
            self.base.clone(),
            self.key.clone(),
            self.size,
            tokio::runtime::Handle::current(),
        );
        let walked = tokio::task::spawn_blocking(move || walk(reader))
            .await
            .map_err(|e| StorageError::Other(format!("iso walk task: {e}")))??;
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
            // The index is in image order, not key order.
            let (tx, rx) = mpsc::channel(1);
            tokio::spawn(async move {
                let _ = tx
                    .send(Err(StorageError::NotImplemented("iso getDir start_after".into())))
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
impl StorageSource for IsoSource {
    fn source_name(&self) -> &str {
        "ISO"
    }

    fn source_type(&self) -> StorageSourceType {
        StorageSourceType::FileSystem
    }

    /// `[offset, offset+length)` of the entry: the matching range of its
    /// extent on the base storage's own stream, passed through; a range
    /// spanning several extents (a multi-extent file) is forwarded piece by
    /// piece in order.
    async fn get_streamed(
        &self,
        object: &str,
        offset: Option<u64>,
        length: Option<u64>,
    ) -> mpsc::Receiver<Result<Vec<u8>, StorageError>> {
        let found = self.index.get(object).zip(self.data.get(object));
        let Some((soi, extents)) = found else {
            let (tx, rx) = mpsc::channel(1);
            let _ = tx.try_send(Err(StorageError::NoSuchKey(object.to_string())));
            return rx;
        };
        let off = offset.unwrap_or(0).min(soi.size);
        let want = length.map(|l| l.min(soi.size - off)).unwrap_or(soi.size - off);
        // The archive ranges covering `[off, off+want)`.
        let mut pieces: Vec<(u64, u64)> = Vec::new();
        let (mut pos, mut left) = (0u64, want);
        for &(start, len) in extents {
            if left == 0 {
                break;
            }
            let end = pos + len;
            if off < end {
                let from = off.max(pos) - pos;
                let take = (len - from).min(left);
                pieces.push((start + from, take));
                left -= take;
            }
            pos = end;
        }
        if pieces.len() <= 1 {
            let (start, len) = pieces.first().copied().unwrap_or((extents[0].0, 0));
            return self
                .base
                .read()
                .await
                .get_streamed(&self.key, Some(start), Some(len))
                .await;
        }
        let (tx, rx) = mpsc::channel(4);
        let base = self.base.clone();
        let key = self.key.clone();
        tokio::spawn(async move {
            for (start, len) in pieces {
                let mut inner = base.read().await.get_streamed(&key, Some(start), Some(len)).await;
                while let Some(chunk) = inner.recv().await {
                    let failed = chunk.is_err();
                    if tx.send(chunk).await.is_err() || failed {
                        return;
                    }
                }
            }
        });
        rx
    }

    async fn get_symlink(&self, object: &str) -> Result<String, StorageError> {
        if self
            .index
            .get(object)
            .is_none_or(|s| s.file_type != StorageFileType::Symlink)
        {
            return Err(StorageError::NoSuchKey(object.to_string()));
        }
        if let Some(target) = self.links.get(object) {
            return Ok(target.clone());
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

    /// A tree packed by `hdiutil makehybrid` twice: with Joliet (names from
    /// the Joliet tree, mode/mtime/symlink from the primary tree's Rock
    /// Ridge `PX`/`TF` — hdiutil writes no `NM`/`SL`) and plain ISO 9660
    /// only (8.3 upper-case names).
    fn build_isos() -> (PathBuf, Vec<u8>) {
        let tmp = std::env::temp_dir().join(format!(
            "sninja-iso-{}-{}",
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
        let big: Vec<u8> = (0..1_500_000u32).map(|i| ((i * 7) % 251) as u8).collect();
        std::fs::write(src.join("sub/deep/b.bin"), &big).unwrap();
        std::fs::write(src.join(format!("sub/{}.txt", "n".repeat(60))), b"long").unwrap();
        std::os::unix::fs::symlink("a.txt", src.join("link")).unwrap();
        let make = |out: &Path, joliet: bool| {
            let mut cmd = std::process::Command::new("hdiutil");
            cmd.args(["makehybrid", "-quiet", "-iso"]);
            if joliet {
                cmd.arg("-joliet");
            }
            let status = cmd.arg("-o").arg(out).arg(&src).status().expect("hdiutil");
            assert!(status.success());
        };
        make(&tmp.join("t.iso"), true);
        make(&tmp.join("plain.iso"), false);
        (tmp, big)
    }

    async fn drain(mut rx: tokio::sync::mpsc::Receiver<Result<Vec<u8>, StorageError>>) -> Vec<u8> {
        let mut out = Vec::new();
        while let Some(chunk) = rx.recv().await {
            out.extend_from_slice(&chunk.unwrap());
        }
        out
    }

    async fn open(tmp: &Path, image: &str) -> Arc<RwLock<dyn StorageSource>> {
        let mut fs = LocalFs::new(tmp);
        fs.connect().await.unwrap();
        let base: Arc<RwLock<dyn StorageSource>> = Arc::new(RwLock::new(fs));
        let size = base.read().await.get_attrs(image).await.unwrap().size;
        super::super::open(base, image.to_string(), size).await.unwrap()
    }

    async fn root_names(source: &Arc<RwLock<dyn StorageSource>>) -> Vec<(String, StorageFileType)> {
        let mut rx = source.write().await.get_dir("", false, None, Some(2)).await;
        let mut names: Vec<(String, StorageFileType)> = Vec::new();
        while let Some(batch) = rx.recv().await {
            names.extend(batch.unwrap().into_iter().map(|s| (s.key, s.file_type)));
        }
        names.sort_by(|a, b| a.0.cmp(&b.0));
        names
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn iso_over_local_fs() {
        let (tmp, big) = build_isos();

        // Joliet names + Rock Ridge attributes.
        let source = open(&tmp, "t.iso").await;
        assert_eq!(
            root_names(&source).await,
            vec![
                ("a.txt".to_string(), StorageFileType::Regular),
                ("empty".to_string(), StorageFileType::Dir),
                ("link".to_string(), StorageFileType::Symlink),
                ("sub".to_string(), StorageFileType::Dir),
            ]
        );
        let src = source.read().await;
        let b = src.get_attrs("sub/deep/b.bin").await.unwrap();
        assert_eq!(b.size, big.len() as u64);
        assert!(b.attrs.mtime.is_some());
        assert!(b.attrs.posix_permissions.is_some(), "mode from the primary tree's PX");
        let long = src.get_attrs(&format!("sub/{}.txt", "n".repeat(60))).await.unwrap();
        assert_eq!(long.size, 4, "Joliet keeps the long name");
        assert!(matches!(src.get_attrs("nope").await, Err(StorageError::NoSuchKey(_))));
        assert_eq!(drain(src.get_streamed("a.txt", None, None).await).await, b"hello");
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
        assert_eq!(
            src.get_symlink("link").await.unwrap(),
            "a.txt",
            "target = the link's data"
        );
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

        // Plain ISO 9660: identifiers as the image spells them.
        let source = open(&tmp, "plain.iso").await;
        assert_eq!(
            root_names(&source).await,
            vec![
                ("A.TXT".to_string(), StorageFileType::Regular),
                ("EMPTY".to_string(), StorageFileType::Dir),
                ("LINK".to_string(), StorageFileType::Symlink),
                ("SUB".to_string(), StorageFileType::Dir),
            ]
        );
        let src = source.read().await;
        assert_eq!(
            drain(src.get_streamed("SUB/DEEP/B.BIN", Some(10), Some(20)).await).await,
            &big[10..30]
        );
        assert_eq!(src.get_symlink("LINK").await.unwrap(), "a.txt");
        drop(src);

        std::fs::remove_dir_all(&tmp).unwrap();
    }

    #[test]
    fn names_are_trimmed() {
        assert_eq!(super::plain_name(b"A.TXT;1"), "A.TXT");
        assert_eq!(super::plain_name(b"README.;1"), "README");
        assert_eq!(
            super::joliet_name(&[0, b'a', 0, b'.', 0, b't', 0, b';', 0, b'1']),
            "a.t"
        );
    }

    #[test]
    fn record_time_applies_the_zone() {
        // 2024-03-05 14:30:20 at GMT+2 (8 quarter hours) → 12:30:20 UTC.
        let t = super::record_time(&[124, 3, 5, 14, 30, 20, 8]).unwrap();
        assert_eq!(t.to_rfc3339(), "2024-03-05T12:30:20+00:00");
        assert!(super::record_time(&[0, 0, 0, 0, 0, 0, 0]).is_none());
    }
}
