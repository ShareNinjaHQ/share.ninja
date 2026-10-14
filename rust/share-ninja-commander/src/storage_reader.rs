//! A sync `Read + Seek` over one storage key — what a format parser that only
//! speaks `std::io` (symphonia in the player, the tar walker) reads a local
//! file or a share file through. Reads go in viewer-sized blocks, each one
//! `get_streamed` range on the source, with a small recency cache; the range
//! runs on the engine's runtime via `block_on`, so this type lives only on
//! threads allowed to wait — the player's decode thread, a `spawn_blocking`
//! task — never on the UI thread or a runtime worker.

use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;

use tokio::sync::RwLock;

use share_ninja_core::StorageSource;

use crate::engine::drain_range;

/// Fetch granularity — the viewer's block, for the same reason: one block
/// amortizes a P2P round trip, holds ~30 s of mp3, and a parser's small
/// header reads land in the block already held.
const BLOCK_SIZE: u64 = crate::viewer::BLOCK_SIZE;
/// Blocks kept around the read position. A sequential reader needs one; an
/// MP4 container hops between its sample table and the audio data, so keep
/// a few.
const KEEP_BLOCKS: usize = 4;

pub struct StorageReader {
    source: Arc<RwLock<dyn StorageSource>>,
    key: String,
    size: u64,
    rt: tokio::runtime::Handle,
    pos: u64,
    /// (block index, bytes), most recently used last.
    blocks: Vec<(u64, Vec<u8>)>,
}

impl StorageReader {
    /// A reader over `key` on `source`, `size` bytes long (the caller has it
    /// from `get_attrs`), positioned at 0.
    pub fn new(source: Arc<RwLock<dyn StorageSource>>, key: String, size: u64, rt: tokio::runtime::Handle) -> Self {
        Self {
            source,
            key,
            size,
            rt,
            pos: 0,
            blocks: Vec::new(),
        }
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    fn block(&mut self, idx: u64) -> std::io::Result<&[u8]> {
        if let Some(i) = self.blocks.iter().position(|(b, _)| *b == idx) {
            let entry = self.blocks.remove(i);
            self.blocks.push(entry);
        } else {
            let data = self
                .rt
                .block_on(drain_range(&self.source, &self.key, idx * BLOCK_SIZE, BLOCK_SIZE))
                .map_err(std::io::Error::other)?;
            self.blocks.push((idx, data));
            if self.blocks.len() > KEEP_BLOCKS {
                self.blocks.remove(0);
            }
        }
        Ok(&self.blocks.last().expect("pushed above").1)
    }
}

impl Read for StorageReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.pos >= self.size {
            return Ok(0);
        }
        let idx = self.pos / BLOCK_SIZE;
        let off = (self.pos - idx * BLOCK_SIZE) as usize;
        let block = self.block(idx)?;
        if off >= block.len() {
            return Ok(0);
        }
        let n = buf.len().min(block.len() - off);
        buf[..n].copy_from_slice(&block[off..off + n]);
        self.pos += n as u64;
        Ok(n)
    }
}

impl Seek for StorageReader {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let target = match pos {
            SeekFrom::Start(o) => o as i64,
            SeekFrom::End(o) => self.size as i64 + o,
            SeekFrom::Current(o) => self.pos as i64 + o,
        };
        if target < 0 {
            return Err(std::io::Error::other("seek before start"));
        }
        self.pos = target as u64;
        Ok(self.pos)
    }
}
