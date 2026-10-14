//! FileServer - P2P file server for serving files over WebRTC
//!
//! Per-request DataChannel architecture: each incoming channel handles one request.
//! Channel close = request cancelled.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use protobuf::Message;
use tokio::sync::{Mutex, RwLock, mpsc};

use crate::transfers::{
    ChangeType, ListUpdatesOptions, SearchOptions, StorageDest, StorageError, StorageFileType, StorageObjectInfo,
    StoragePutConditions, StorageSearchQuery, StorageSource,
};

use super::data_channel::DataChannelWrapper;
use share_ninja_proto::file_proto::{
    self, CapsResponse, ChangeType as ProtoChangeType, DirEntry as ProtoDirEntry, DirStatsResponse,
    FileAttrs as ProtoFileAttrs, FileType, GetAttrsResponse, GetDirResponse,
    ListUpdatesBeginResponse as ProtoListUpdatesBeginResponse, ListUpdatesResponse as ProtoListUpdatesResponse,
    PreviewChunk, PreviewFailure, PreviewResponse, PutDirResponse as ProtoPutDirResponse,
    PutResponse as ProtoPutResponse, ReadResponse, Request, Response,
    Result as ProtoResult, SearchResponse as ProtoSearchResponse, StorageChange as ProtoStorageChange,
    SupportedRequestType,
};

const MAX_RESPONSE_SIZE: usize = 200 * 1024; // 200KB max response size
const ITERATOR_TIMEOUT_SECS: u64 = 60; // cleanup idle iterators after 1 minute
/// Per-page time budget for SEARCH: the source returns whatever it has by
/// then (possibly an empty page) plus a resume token, so the response always
/// lands well inside the client's 30s per-message receive timeout.
const SEARCH_PAGE_DEADLINE: std::time::Duration = std::time::Duration::from_secs(20);

/// Full wire contribution of one entry to a `repeated` embedded-message field:
/// `compute_size()` returns only the element body, but on the wire each element is
/// prefixed with a field tag + a length varint. Budgeting a block against the
/// channel's max message size must include that framing or many small entries
/// overflow the limit. Uses a 2-byte tag (safe for field numbers up to 2047) and
/// the exact varint length of the body — a true upper bound.
fn entry_framed_size(body: usize) -> usize {
    fn varint_len(mut v: usize) -> usize {
        let mut n = 1;
        while v >= 0x80 {
            v >>= 7;
            n += 1;
        }
        n
    }
    2 + varint_len(body) + body
}

/// Stored iterator for cursor-based pagination: the live `get_dir` stream plus
/// any entries already pulled from a batch but not yet sent. The peer resumes by
/// cursor; we keep pulling from `rx` lazily instead of buffering the whole list.
struct ActiveIterator {
    rx: mpsc::Receiver<Result<Vec<StorageObjectInfo>, StorageError>>,
    pending: VecDeque<StorageObjectInfo>,
    last_access: Instant,
}

/// FileServer - Per-request DataChannel architecture
///
/// Each incoming DataChannel handles one request independently.
/// Channel close = request cancelled.
pub struct FileServer {
    source: Arc<RwLock<dyn StorageSource>>,
    dest: Option<Arc<RwLock<dyn StorageDest>>>,
    active_iterators: Arc<Mutex<HashMap<String, ActiveIterator>>>,
    cursor_counter: AtomicU64,
}

impl FileServer {
    pub fn new(source: Arc<RwLock<dyn StorageSource>>) -> Self {
        Self {
            source,
            dest: None,
            active_iterators: Arc::new(Mutex::new(HashMap::new())),
            cursor_counter: AtomicU64::new(0),
        }
    }

    /// Create a FileServer with both read and write support
    pub fn with_dest(source: Arc<RwLock<dyn StorageSource>>, dest: Arc<RwLock<dyn StorageDest>>) -> Self {
        Self {
            source,
            dest: Some(dest),
            active_iterators: Arc::new(Mutex::new(HashMap::new())),
            cursor_counter: AtomicU64::new(0),
        }
    }

    /// Handle an incoming channel - spawns a task to process requests on this channel
    pub fn handle_channel(&self, channel: Arc<DataChannelWrapper>) {
        let source = self.source.clone();
        let dest = self.dest.clone();
        let active_iterators = self.active_iterators.clone();
        let cursor_counter = &self.cursor_counter;
        // Pre-fetch a block of cursor IDs for this task to avoid sharing the atomic across await
        let cursor_base = cursor_counter.fetch_add(1_000_000, Ordering::Relaxed);
        let cursor_local = Arc::new(AtomicU64::new(cursor_base));

        tokio::spawn(async move {
            tracing::debug!(
                "Handling channel {} (open: {})",
                channel.label(),
                channel.is_open().await
            );

            // Process messages in a loop while channel is open
            while channel.is_open().await {
                // Wait for next message (indefinitely like TypeScript)
                let data = match channel.recv(0).await {
                    Ok(d) => {
                        tracing::trace!("Channel {} received {} bytes", channel.label(), d.len());
                        d
                    }
                    Err(e) => {
                        if !channel.is_open().await {
                            tracing::debug!("Channel {} closed, msg: {}", channel.label(), e);
                        } else {
                            tracing::debug!("Channel {} error: {}", channel.label(), e);
                        }
                        return;
                    }
                };

                // Process the message
                if let Err(e) =
                    Self::process_message(&source, &dest, &channel, &data, &active_iterators, &cursor_local).await
                {
                    tracing::error!("Error processing message: {}", e);
                    let _ = Self::send_error(&channel, &e).await;
                    channel.close().await;
                    return;
                }
            }

            tracing::debug!("Channel {} closed", channel.label());
        });
    }

    async fn send_response(channel: &DataChannelWrapper, resp: Response) -> Result<(), String> {
        let data = resp.write_to_bytes().map_err(|e| e.to_string())?;
        channel.send(&data).await
    }

    /// A file block: waits for room on the connection's bulk gate first, so
    /// control responses (`send_response`) never queue behind file data.
    async fn send_bulk_response(channel: &DataChannelWrapper, resp: Response) -> Result<(), String> {
        let data = resp.write_to_bytes().map_err(|e| e.to_string())?;
        channel.send_bulk(&data).await
    }

    async fn send_error(channel: &DataChannelWrapper, msg: &str) -> Result<(), String> {
        Self::send_status(channel, ProtoResult::RESULT_FAIL, msg).await
    }

    async fn send_status(channel: &DataChannelWrapper, status: ProtoResult, msg: &str) -> Result<(), String> {
        let mut resp = Response::new();
        resp.status = status.into();
        resp.status_string = Some(msg.to_string());
        Self::send_response(channel, resp).await
    }

    /// Wire status for a dest/source error: the typed ones a V3 client acts on
    /// get their own code, everything else is a generic failure with the message.
    fn status_for(e: &StorageError) -> ProtoResult {
        match e {
            StorageError::PreconditionFailed(_) => ProtoResult::RESULT_PRECONDITION_FAILED,
            StorageError::NoSuchKey(_) => ProtoResult::RESULT_NO_SUCH_FILE,
            StorageError::NotImplemented(_) => ProtoResult::RESULT_NOT_IMPL,
            StorageError::NoPreview(_) => ProtoResult::RESULT_UNSUPPORTED,
            _ => ProtoResult::RESULT_FAIL,
        }
    }

    async fn process_message(
        source: &Arc<RwLock<dyn StorageSource>>,
        dest: &Option<Arc<RwLock<dyn StorageDest>>>,
        channel: &DataChannelWrapper,
        data: &[u8],
        active_iterators: &Arc<Mutex<HashMap<String, ActiveIterator>>>,
        cursor_counter: &AtomicU64,
    ) -> Result<(), String> {
        //tracing::debug!("Raw data ({} bytes): {:02x?}", data.len(), &data[..data.len().min(64)]);
        let msg = Request::parse_from_bytes(data).map_err(|e| {
            tracing::error!(
                "Protobuf parse error: {} - first bytes: {:02x?}",
                e,
                &data[..data.len().min(16)]
            );
            e.to_string()
        })?;

        match &msg.msg {
            Some(file_proto::request::Msg::Caps(_caps)) => {
                Self::process_caps(source, channel).await?;
            }
            Some(file_proto::request::Msg::Preview(preview)) => {
                Self::process_preview(source, channel, preview).await?;
            }
            Some(file_proto::request::Msg::Get(get)) => {
                Self::process_get(source, channel, get).await?;
            }
            Some(file_proto::request::Msg::GetDir(get_dir)) => {
                Self::process_get_dir(source, channel, get_dir, active_iterators, cursor_counter).await?;
            }
            Some(file_proto::request::Msg::GetAttrs(get_attrs)) => {
                Self::process_get_attrs(source, channel, get_attrs).await?;
            }
            Some(file_proto::request::Msg::DirStats(dir_stats)) => {
                Self::process_dir_stats(source, channel, dir_stats).await?;
            }
            Some(file_proto::request::Msg::Put(put)) => {
                Self::process_put(dest, channel, put).await?;
            }
            Some(file_proto::request::Msg::PutDir(put_dir)) => {
                Self::process_put_dir(dest, channel, put_dir).await?;
            }
            Some(file_proto::request::Msg::Delete(del)) => {
                Self::process_delete(dest, channel, del).await?;
            }
            Some(file_proto::request::Msg::ListUpdatesBegin(req)) => {
                Self::process_list_updates_begin(source, channel, req).await?;
            }
            Some(file_proto::request::Msg::ListUpdates(req)) => {
                Self::process_list_updates(source, channel, req).await?;
            }
            Some(file_proto::request::Msg::Search(req)) => {
                Self::process_search(source, channel, req).await?;
            }
            _ => {
                tracing::debug!("Unhandled message type");
                Self::send_error(channel, "Not implemented").await?;
            }
        }

        Ok(())
    }

    async fn process_caps(
        source: &Arc<RwLock<dyn StorageSource>>,
        channel: &DataChannelWrapper,
    ) -> Result<(), String> {
        tracing::debug!("Processing CAPS request");

        let mut caps_resp = CapsResponse::new();
        caps_resp.server_supported_messages = vec![
            SupportedRequestType::GET_REQUEST as i64,
            SupportedRequestType::CONDITIONAL_PUT as i64,
            SupportedRequestType::DELETE_REQUEST as i64,
            SupportedRequestType::GETDIR_START_AFTER as i64,
        ];
        if source.read().await.serves_previews() {
            caps_resp
                .server_supported_messages
                .push(SupportedRequestType::PREVIEW_REQUEST as i64);
        }

        let mut resp = Response::new();
        resp.status = ProtoResult::RESULT_OK.into();
        resp.msg = Some(file_proto::response::Msg::CapsResp(caps_resp));

        Self::send_response(channel, resp).await
    }

    /// Stream `[offset, offset+length)` of `path` to the client in blocks that
    /// fit the channel's message limit.
    ///
    /// One `get_streamed` for the whole range, opened under the source's read
    /// guard and drained with it dropped: the source's producer runs in its
    /// own task, so a GetDir/DirStats writer queued behind this request no
    /// longer waits for the file to finish (and, tokio's `RwLock` being
    /// write-preferring, no longer blocks every other Get while it waits),
    /// and the file is opened once instead of once per ~64 KB block.
    async fn process_get(
        source: &Arc<RwLock<dyn StorageSource>>,
        channel: &DataChannelWrapper,
        get: &file_proto::Get,
    ) -> Result<(), String> {
        let path = &get.path;
        let offset = get.offset.unwrap_or(0);
        let length = get.length;

        tracing::info!(
            "[FileServer] Serving file: {} (offset={}, length={:?})",
            path,
            offset,
            length
        );

        // Block size must fit the channel's max message size (the negotiated SCTP
        // limit for WebRTC, ~64K; unbounded for relay) so the post-encryption
        // ReadResponse doesn't exceed it and trip SCTP `ErrOutboundPacketTooLarge`.
        // Reserve 1024 for protobuf/Response framing. Mirrors TS FileServer.maxSize.
        let block_len = std::cmp::min(MAX_RESPONSE_SIZE, channel.max_plaintext_size())
            .saturating_sub(1024)
            .max(1);

        let mut rx = {
            let source = source.read().await;
            source.get_streamed(path, Some(offset), length).await
        };

        let mut curr_offset = offset;
        // Bytes received from the source and not yet sent. A block goes out
        // only when more than a block's worth is buffered, or at end of
        // stream — so the final block can be flagged without a trailing empty
        // one (an empty file still answers with one empty last block).
        let mut pending: Vec<u8> = Vec::new();
        loop {
            let eof = match rx.recv().await {
                Some(Ok(chunk)) => {
                    if pending.is_empty() {
                        pending = chunk;
                    } else {
                        pending.extend_from_slice(&chunk);
                    }
                    false
                }
                Some(Err(e)) => return Err(e.to_string()),
                None => true,
            };

            loop {
                let take = if pending.len() > block_len {
                    block_len
                } else if eof {
                    pending.len()
                } else {
                    break;
                };
                let is_last = eof && take == pending.len();
                let bytes: Vec<u8> = if take == pending.len() {
                    std::mem::take(&mut pending)
                } else {
                    pending.drain(..take).collect()
                };

                if !channel.is_open().await {
                    return Err("Channel closed".to_string());
                }

                let mut read_resp = ReadResponse::new();
                read_resp.offset = curr_offset;
                read_resp.bytes = bytes;
                read_resp.is_last_block = is_last;

                let mut resp = Response::new();
                resp.status = ProtoResult::RESULT_OK.into();
                resp.msg = Some(file_proto::response::Msg::ReadResp(read_resp));

                Self::send_bulk_response(channel, resp).await?;
                curr_offset += take as u64;

                if is_last {
                    tracing::info!("[FileServer] Completed: {} ({} bytes sent)", path, curr_offset);
                    return Ok(());
                }
            }
        }
    }

    /// Thumbnails of a batch (docs/core/previews.md): each item goes out as the
    /// source completes it — its chunk(s), or one failure — then the bodyless
    /// closing block. A refused request is the top-level status alone.
    async fn process_preview(
        source: &Arc<RwLock<dyn StorageSource>>,
        channel: &DataChannelWrapper,
        preview: &file_proto::Preview,
    ) -> Result<(), String> {
        tracing::debug!("Processing PREVIEW request: {} paths, px={}", preview.paths.len(), preview.px);

        let mut rx = {
            let source = source.read().await;
            match source.get_previews(preview.paths.clone(), preview.px).await {
                Ok(rx) => rx,
                Err(e) => return Self::send_status(channel, Self::status_for(&e), &e.to_string()).await,
            }
        };

        // Same budget as `process_get`: the chunk must fit the channel's message.
        let block_len = std::cmp::min(MAX_RESPONSE_SIZE, channel.max_plaintext_size())
            .saturating_sub(1024)
            .max(1);

        while let Some((index, result)) = rx.recv().await {
            let image = match result {
                Ok(image) if !image.bytes.is_empty() => image,
                Ok(_) => {
                    Self::send_preview_failure(channel, index, ProtoResult::RESULT_FAIL).await?;
                    continue;
                }
                Err(e) => {
                    Self::send_preview_failure(channel, index, Self::status_for(&e)).await?;
                    continue;
                }
            };
            let mut blocks = image.bytes.chunks(block_len).peekable();
            // The item's first chunk names its mime type and version.
            let mut head = Some((image.mime_type.clone(), image.etag.clone()));
            while let Some(bytes) = blocks.next() {
                let mut chunk = PreviewChunk::new();
                chunk.index = index as u32;
                chunk.bytes = bytes.to_vec();
                if let Some((mime_type, etag)) = head.take() {
                    chunk.mime_type = Some(mime_type);
                    chunk.etag = etag;
                }
                chunk.item_done = blocks.peek().is_none();

                let mut body = PreviewResponse::new();
                body.body = Some(file_proto::preview_response::Body::Chunk(chunk));
                Self::send_preview_response(channel, body).await?;
            }
        }

        let mut closing = PreviewResponse::new();
        closing.is_last_block = true;
        Self::send_preview_response(channel, closing).await
    }

    async fn send_preview_failure(
        channel: &DataChannelWrapper,
        index: usize,
        status: ProtoResult,
    ) -> Result<(), String> {
        let mut failure = PreviewFailure::new();
        failure.index = index as u32;
        failure.status = status.into();

        let mut body = PreviewResponse::new();
        body.body = Some(file_proto::preview_response::Body::Failure(failure));
        Self::send_preview_response(channel, body).await
    }

    async fn send_preview_response(channel: &DataChannelWrapper, body: PreviewResponse) -> Result<(), String> {
        let mut resp = Response::new();
        resp.status = ProtoResult::RESULT_OK.into();
        resp.msg = Some(file_proto::response::Msg::Preview(body));
        Self::send_response(channel, resp).await
    }

    fn soi_to_dir_entry(soi: &StorageObjectInfo) -> ProtoDirEntry {
        let mut entry = ProtoDirEntry::new();
        entry.name = soi.key.clone();
        entry.file_type = match soi.file_type {
            StorageFileType::Regular => FileType::FILE_TYPE_REGULAR,
            StorageFileType::Dir => FileType::FILE_TYPE_DIRECTORY,
            StorageFileType::Symlink => FileType::FILE_TYPE_SYMBOLIC_LINK,
            _ => FileType::FILE_TYPE_UNKNOWN,
        }
        .into();
        entry.file_size = Some(soi.size);
        entry.etag = soi.etag.clone();

        let mut attrs = ProtoFileAttrs::new();
        let a = &soi.attrs;
        if let Some(mtime) = a.mtime {
            attrs.modification_date = Some(mtime.timestamp_micros());
        }
        attrs.posix_permissions = a.posix_permissions;
        attrs.email_subject = a.email_subject.clone();
        attrs.gdrive_file_id = a.gdrive_file_id.clone();
        attrs.gdrive_mime_type = a.gdrive_mime_type.clone();
        if attrs != ProtoFileAttrs::new() {
            entry.attrs = protobuf::MessageField::some(attrs);
        }

        entry
    }

    fn cleanup_stale_iterators(iterators: &mut HashMap<String, ActiveIterator>) {
        let now = Instant::now();
        iterators.retain(|cursor, iter| {
            let stale = now.duration_since(iter.last_access).as_secs() > ITERATOR_TIMEOUT_SECS;
            if stale {
                tracing::debug!("Cleaning up stale iterator: {}", cursor);
            }
            !stale
        });
    }

    async fn process_get_dir(
        source: &Arc<RwLock<dyn StorageSource>>,
        channel: &DataChannelWrapper,
        get_dir: &file_proto::GetDir,
        active_iterators: &Arc<Mutex<HashMap<String, ActiveIterator>>>,
        cursor_counter: &AtomicU64,
    ) -> Result<(), String> {
        let path = &get_dir.path;
        let max_entries = if get_dir.max_entries > 0 {
            get_dir.max_entries as usize
        } else {
            1000
        };
        let recursive = get_dir.recursive.unwrap_or(true);

        tracing::debug!(
            "Processing GETDIR request: path={}, max_entries={}, recursive={}, cursor={:?}",
            path,
            max_entries,
            recursive,
            get_dir.cursor
        );

        {
            let mut iters = active_iterators.lock().await;
            Self::cleanup_stale_iterators(&mut iters);
        }

        // Resume the live stream by cursor, or start a fresh one from the source.
        // The source `get_dir` is lazy — we pull entries as we page, never
        // buffering the whole listing.
        let resumed = match get_dir.cursor {
            Some(ref cursor) => active_iterators.lock().await.remove(cursor),
            None => None,
        };
        let mut iter = match resumed {
            Some(iter) => iter,
            None => {
                let rx = {
                    let source = source.read().await;
                    source
                        .get_dir(path, recursive, get_dir.start_after.as_deref(), Some(max_entries))
                        .await
                };
                ActiveIterator {
                    rx,
                    pending: VecDeque::new(),
                    last_access: Instant::now(),
                }
            }
        };

        // Cap each block to BOTH the protocol limit AND the channel's max message
        // size. On WebRTC the negotiated SCTP limit is ~64K, so a block sized only
        // against MAX_RESPONSE_SIZE (200K) overflows and the send fails with
        // `ErrOutboundPacketTooLarge`. Reserve 1024 for the Response/GetDirResponse
        // framing (matches process_get). Relay is unbounded so this stays at 200K.
        let max_block = std::cmp::min(MAX_RESPONSE_SIZE, channel.max_plaintext_size())
            .saturating_sub(1024)
            .max(1);

        let mut total_sent = 0usize;
        loop {
            let mut entries: Vec<ProtoDirEntry> = Vec::new();
            let mut current_size = 50usize; // base overhead for GetDirResponse wrapper
            let mut exhausted = false;

            while total_sent + entries.len() < max_entries {
                let soi = match Self::next_dir_item(&mut iter).await? {
                    Some(soi) => soi,
                    None => {
                        exhausted = true;
                        break;
                    }
                };
                let entry = Self::soi_to_dir_entry(&soi);
                // Account for the entry's FULL contribution to the encoded
                // GetDirResponse, not just its own body: a repeated embedded-message
                // field prefixes each element with a tag + length varint that
                // compute_size() does not include. Undercounting that framing is what
                // let a block of many small entries still overflow the 64K SCTP limit.
                let framed = entry_framed_size(entry.compute_size() as usize);

                if current_size + framed > max_block && !entries.is_empty() {
                    iter.pending.push_front(soi); // doesn't fit this block — keep for the next
                    break;
                }

                current_size += framed;
                entries.push(entry);
            }

            total_sent += entries.len();

            if !channel.is_open().await {
                tracing::debug!("Channel closed, cancelling getDir");
                return Ok(());
            }

            // Page full but more remains → stash the live iterator under a cursor
            // and send the final block; the peer resumes with that cursor.
            if !exhausted && total_sent >= max_entries {
                let c = cursor_counter.fetch_add(1, Ordering::Relaxed).to_string();
                iter.last_access = Instant::now();
                active_iterators.lock().await.insert(c.clone(), iter);
                tracing::debug!(
                    "getDir block: {} entries, {} total, cursor={}",
                    entries.len(),
                    total_sent,
                    c
                );
                Self::send_dir_block(channel, entries, true, Some(c)).await?;
                return Ok(());
            }

            // Otherwise: exhausted (last block) or a max_block split mid-page.
            tracing::debug!(
                "getDir block: {} entries, {} total, last={}",
                entries.len(),
                total_sent,
                exhausted
            );
            Self::send_dir_block(channel, entries, exhausted, None).await?;
            if exhausted {
                tracing::debug!("getDir done");
                return Ok(());
            }
        }
    }

    /// Pull the next entry from a paginating iterator: drain `pending`, else pull
    /// the next batch off the live stream. `None` ⇒ the walk is exhausted.
    async fn next_dir_item(iter: &mut ActiveIterator) -> Result<Option<StorageObjectInfo>, String> {
        loop {
            if let Some(soi) = iter.pending.pop_front() {
                return Ok(Some(soi));
            }
            match iter.rx.recv().await {
                Some(Ok(batch)) => iter.pending.extend(batch),
                Some(Err(e)) => return Err(e.to_string()),
                None => return Ok(None),
            }
        }
    }

    async fn send_dir_block(
        channel: &DataChannelWrapper,
        entries: Vec<ProtoDirEntry>,
        is_last: bool,
        cursor: Option<String>,
    ) -> Result<(), String> {
        let mut dir_resp = GetDirResponse::new();
        dir_resp.entries = entries;
        dir_resp.is_last_block = is_last;
        dir_resp.cursor = cursor;

        let mut resp = Response::new();
        resp.status = ProtoResult::RESULT_OK.into();
        resp.msg = Some(file_proto::response::Msg::GetDirResp(dir_resp));

        Self::send_response(channel, resp).await
    }

    async fn process_get_attrs(
        source: &Arc<RwLock<dyn StorageSource>>,
        channel: &DataChannelWrapper,
        get_attrs: &file_proto::GetAttrs,
    ) -> Result<(), String> {
        let path = &get_attrs.path;
        tracing::debug!("Processing GETATTRS request: path={}", path);

        let source = source.read().await;

        match source.get_attrs(path).await {
            Ok(item) => {
                let mut entry = Self::soi_to_dir_entry(&item);

                if item.file_type == StorageFileType::Symlink
                    && let Ok(target) = source.get_symlink(path).await
                {
                    entry.symlink_target = Some(target);
                }

                let mut attrs_resp = GetAttrsResponse::new();
                attrs_resp.dir_entry = protobuf::MessageField::some(entry);

                let mut resp = Response::new();
                resp.status = ProtoResult::RESULT_OK.into();
                resp.msg = Some(file_proto::response::Msg::GetAttrsResp(attrs_resp));

                Self::send_response(channel, resp).await
            }
            Err(_) => {
                let mut resp = Response::new();
                resp.status = ProtoResult::RESULT_NO_SUCH_FILE.into();
                resp.status_string = Some("File not found".to_string());
                Self::send_response(channel, resp).await
            }
        }
    }

    async fn process_dir_stats(
        source: &Arc<RwLock<dyn StorageSource>>,
        channel: &DataChannelWrapper,
        dir_stats: &file_proto::DirStats,
    ) -> Result<(), String> {
        let path = &dir_stats.path;
        tracing::debug!("Processing DIRSTATS request: path={}", path);

        // Stream the walk: a recursive stat of a large tree runs far longer than
        // the client's per-message receive timeout, so its running totals go out
        // as they accumulate (each block cumulative, `is_last_block` on the
        // final one) and the client's deadline restarts on every block.
        let mut rx = source.read().await.get_dir_stats_streamed(path).await;
        let mut pending: Option<crate::transfers::StorageDirStats> = None;
        while let Some(item) = rx.recv().await {
            let stats = match item {
                Ok(s) => s,
                Err(e) => {
                    let mut resp = Response::new();
                    resp.status = ProtoResult::RESULT_FAIL.into();
                    resp.status_string = Some(format!("Failed to get dir stats: {}", e));
                    return Self::send_response(channel, resp).await;
                }
            };
            // Hold each block until the next one arrives — the last block the
            // source produces is the complete answer, and only it may carry
            // `is_last_block`.
            if let Some(prev) = pending.replace(stats) {
                if !channel.is_open().await {
                    tracing::debug!("Channel closed, cancelling dirStats");
                    return Ok(());
                }
                Self::send_dir_stats(channel, &prev, false).await?;
            }
        }

        match pending {
            Some(final_stats) => Self::send_dir_stats(channel, &final_stats, true).await,
            // A source that yielded nothing at all still owes the client a
            // terminal block, or it would wait out its receive timeout.
            None => Self::send_dir_stats(channel, &crate::transfers::StorageDirStats::default(), true).await,
        }
    }

    async fn send_dir_stats(
        channel: &DataChannelWrapper,
        stats: &crate::transfers::StorageDirStats,
        is_last: bool,
    ) -> Result<(), String> {
        let mut stats_resp = DirStatsResponse::new();
        stats_resp.number_of_files = stats.number_of_files as i32;
        stats_resp.number_of_folders = stats.number_of_folders as i32;
        stats_resp.number_of_symlinks = stats.number_of_symlinks as i32;
        stats_resp.total_data_size = stats.total_size as i64;
        stats_resp.is_last_block = is_last;

        let mut resp = Response::new();
        resp.status = ProtoResult::RESULT_OK.into();
        resp.msg = Some(file_proto::response::Msg::DirStatsResp(stats_resp));

        Self::send_response(channel, resp).await
    }

    async fn process_list_updates_begin(
        source: &Arc<RwLock<dyn StorageSource>>,
        channel: &DataChannelWrapper,
        req: &file_proto::ListUpdatesBegin,
    ) -> Result<(), String> {
        tracing::debug!("Processing LISTUPDATESBEGIN request: path={}", req.path);

        let source = source.read().await;
        match source.list_updates_begin(&req.path).await {
            Ok(cursor) => {
                let mut begin_resp = ProtoListUpdatesBeginResponse::new();
                begin_resp.cursor = cursor;

                let mut resp = Response::new();
                resp.status = ProtoResult::RESULT_OK.into();
                resp.msg = Some(file_proto::response::Msg::ListUpdatesBeginResp(begin_resp));

                Self::send_response(channel, resp).await
            }
            Err(e) => Self::send_error(channel, &format!("ListUpdatesBegin failed: {}", e)).await,
        }
    }

    async fn process_list_updates(
        source: &Arc<RwLock<dyn StorageSource>>,
        channel: &DataChannelWrapper,
        req: &file_proto::ListUpdates,
    ) -> Result<(), String> {
        tracing::debug!(
            "Processing LISTUPDATES request: cursor={} timeout={:?}",
            req.cursor,
            req.timeout_ms
        );

        let options = ListUpdatesOptions {
            timeout_ms: req.timeout_ms,
            close: req.close,
            ..Default::default()
        };

        let source = source.read().await;
        match source.list_updates(&req.cursor, &options).await {
            Ok(result) => {
                let changes: Vec<ProtoStorageChange> = result
                    .changes
                    .iter()
                    .map(|c| {
                        let mut sc = ProtoStorageChange::new();
                        sc.change_type = match c.change_type {
                            ChangeType::Add => ProtoChangeType::CHANGE_ADD,
                            ChangeType::Modify => ProtoChangeType::CHANGE_MODIFY,
                            ChangeType::Delete => ProtoChangeType::CHANGE_DELETE,
                        }
                        .into();
                        sc.item = protobuf::MessageField::some(Self::soi_to_dir_entry(&c.item));
                        sc
                    })
                    .collect();

                let mut updates_resp = ProtoListUpdatesResponse::new();
                updates_resp.changes = changes;
                updates_resp.cursor = result.cursor;
                updates_resp.has_more = result.has_more;

                let mut resp = Response::new();
                resp.status = ProtoResult::RESULT_OK.into();
                resp.msg = Some(file_proto::response::Msg::ListUpdatesResp(updates_resp));

                Self::send_response(channel, resp).await
            }
            Err(e) => Self::send_error(channel, &format!("ListUpdates failed: {}", e)).await,
        }
    }

    async fn process_search(
        source: &Arc<RwLock<dyn StorageSource>>,
        channel: &DataChannelWrapper,
        req: &file_proto::Search,
    ) -> Result<(), String> {
        tracing::debug!(
            "Processing SEARCH request: path={} name_match={:?} recursive={:?} limit={:?}",
            req.path,
            req.name_match,
            req.recursive,
            req.limit
        );

        // The response size cap is the source's page byte budget: every
        // source cuts its page under it, so a page always fits one message.
        let max_block = std::cmp::min(MAX_RESPONSE_SIZE, channel.max_plaintext_size())
            .saturating_sub(1024)
            .max(1);

        let query = StorageSearchQuery {
            name_match: req.name_match.clone(),
        };
        let options = SearchOptions {
            path_scope: Some(req.path.clone()),
            recursive: req.recursive,
            page_limit: req.limit,
            page_token: req.page_token.clone(),
            deadline: Some(SEARCH_PAGE_DEADLINE),
            max_response_bytes: Some(max_block),
        };

        // Read lock: `search` is `&self`, and the walk it does on an fs source
        // must not block the source's other operations for its duration.
        let source = source.read().await;
        match source.search(&query, &options).await {
            Ok(result) => {
                let mut search_resp = ProtoSearchResponse::new();
                search_resp.items = result.items.iter().map(Self::soi_to_dir_entry).collect();
                // The wire keeps the field for older peers; the token says it all.
                search_resp.has_more = result.next_page_token.is_some();
                search_resp.next_page_token = result.next_page_token;

                let mut resp = Response::new();
                resp.status = ProtoResult::RESULT_OK.into();
                resp.msg = Some(file_proto::response::Msg::SearchResp(search_resp));

                Self::send_response(channel, resp).await
            }
            Err(e) => Self::send_error(channel, &format!("Search failed: {}", e)).await,
        }
    }

    async fn process_put(
        dest: &Option<Arc<RwLock<dyn StorageDest>>>,
        channel: &DataChannelWrapper,
        put: &file_proto::Put,
    ) -> Result<(), String> {
        tracing::debug!("Processing PUT request: path={} size={}", put.path, put.size);

        let dest = match dest {
            Some(d) => d,
            None => return Self::send_error(channel, "Write not supported").await,
        };

        let conditions = StoragePutConditions {
            if_match: put.if_match.clone(),
            if_none_match: put.if_none_match.unwrap_or(false),
        };
        let conditions = (!conditions.is_empty()).then_some(&conditions);

        let dest = dest.write().await;
        match dest.put_file(&put.path, &put.bytes, None, conditions).await {
            Ok(result) => {
                let mut put_resp = ProtoPutResponse::new();
                // The stored version when the dest has one; the path otherwise,
                // as before, so legacy clients see the same shape.
                put_resp.etag = result.etag.unwrap_or_else(|| put.path.clone());

                let mut resp = Response::new();
                resp.status = ProtoResult::RESULT_OK.into();
                resp.msg = Some(file_proto::response::Msg::PutResp(put_resp));

                Self::send_response(channel, resp).await
            }
            Err(e) => Self::send_status(channel, Self::status_for(&e), &format!("Put failed: {}", e)).await,
        }
    }

    async fn process_delete(
        dest: &Option<Arc<RwLock<dyn StorageDest>>>,
        channel: &DataChannelWrapper,
        del: &file_proto::Delete,
    ) -> Result<(), String> {
        tracing::debug!("Processing DELETE request: path={}", del.path);

        let dest = match dest {
            Some(d) => d,
            None => return Self::send_error(channel, "Write not supported").await,
        };

        let dest = dest.write().await;
        match dest.del_file(&del.path).await {
            Ok(()) => {
                let mut resp = Response::new();
                resp.status = ProtoResult::RESULT_OK.into();
                Self::send_response(channel, resp).await
            }
            Err(e) => Self::send_status(channel, Self::status_for(&e), &format!("Delete failed: {}", e)).await,
        }
    }

    async fn process_put_dir(
        dest: &Option<Arc<RwLock<dyn StorageDest>>>,
        channel: &DataChannelWrapper,
        put_dir: &file_proto::PutDir,
    ) -> Result<(), String> {
        tracing::debug!("Processing PUTDIR request: path={}", put_dir.path);

        let dest = match dest {
            Some(d) => d,
            None => return Self::send_error(channel, "Write not supported").await,
        };

        let dest = dest.write().await;
        match dest.put_dir(&put_dir.path, None).await {
            Ok(_result) => {
                let mut put_dir_resp = ProtoPutDirResponse::new();
                put_dir_resp.etag = put_dir.path.clone();

                let mut resp = Response::new();
                resp.status = ProtoResult::RESULT_OK.into();
                resp.msg = Some(file_proto::response::Msg::PutDirResp(put_dir_resp));

                Self::send_response(channel, resp).await
            }
            Err(e) => Self::send_error(channel, &format!("PutDir failed: {}", e)).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::p2p::data_channel::DataChannelWrapper;
    use crate::p2p::test_support::{
        DestErrKind, FakeDataChannel, ScriptedDest, ScriptedSource, soi_file, wrapper_pair,
    };
    use crate::transfers::test_support::{GatedStatsSource, InMemSource, files};
    use crate::transfers::{SearchResult, StorageMetaState, StorageSource};
    use share_ninja_proto::file_proto::Caps;
    use share_ninja_proto::file_proto::request::Msg;

    const KEY: [u8; 32] = [9u8; 32];

    struct TestClient {
        ch: Arc<DataChannelWrapper>,
        /// The server side's fake channel — its `sent_snapshot` is the raw wire
        /// bytes the server sent (post-encryption).
        wire: Arc<FakeDataChannel>,
    }

    impl TestClient {
        async fn send_req(&self, msg: Msg) {
            let mut req = Request::new();
            req.msg = Some(msg);
            self.ch.send(&req.write_to_bytes().unwrap()).await.unwrap();
        }

        async fn recv_resp(&self) -> Response {
            let data = self.ch.recv(5000).await.expect("response expected");
            Response::parse_from_bytes(&data).unwrap()
        }

        async fn request(&self, msg: Msg) -> Response {
            self.send_req(msg).await;
            self.recv_resp().await
        }
    }

    /// Open one request channel against the server, optionally clamping the
    /// server side's max message size (the negotiated SCTP limit).
    async fn open_channel(fs: &FileServer, label: &str, max_msg: Option<u64>) -> TestClient {
        let (client_w, server_w, _cf, sf) = wrapper_pair(label, Some(KEY)).await;
        if let Some(m) = max_msg {
            server_w.set_max_message_size(m);
        }
        fs.handle_channel(server_w);
        TestClient { ch: client_w, wire: sf }
    }

    fn src_arc(s: impl StorageSource + 'static) -> Arc<RwLock<dyn StorageSource>> {
        Arc::new(RwLock::new(s))
    }

    fn get_msg(path: &str, offset: Option<u64>, length: Option<u64>) -> Msg {
        let mut g = file_proto::Get::new();
        g.path = path.to_string();
        g.offset = offset;
        g.length = length;
        Msg::Get(g)
    }

    fn read_resp(resp: Response) -> ReadResponse {
        assert_eq!(
            resp.status,
            ProtoResult::RESULT_OK.into(),
            "got: {:?}",
            resp.status_string
        );
        match resp.msg {
            Some(file_proto::response::Msg::ReadResp(r)) => r,
            other => panic!("expected ReadResp, got {other:?}"),
        }
    }

    fn dir_resp(resp: Response) -> GetDirResponse {
        assert_eq!(
            resp.status,
            ProtoResult::RESULT_OK.into(),
            "got: {:?}",
            resp.status_string
        );
        match resp.msg {
            Some(file_proto::response::Msg::GetDirResp(r)) => r,
            other => panic!("expected GetDirResp, got {other:?}"),
        }
    }

    #[test]
    fn entry_framed_size_includes_tag_and_length_varint() {
        assert_eq!(entry_framed_size(0), 3); // 2-byte tag + 1-byte varint + empty body
        assert_eq!(entry_framed_size(127), 2 + 1 + 127);
        assert_eq!(entry_framed_size(128), 2 + 2 + 128); // varint grows at 128
    }

    #[tokio::test]
    async fn caps_lists_supported_requests() {
        let fs = FileServer::new(src_arc(InMemSource::new(files(&[]))));
        let client = open_channel(&fs, "ch-0", None).await;

        let resp = client.request(Msg::Caps(Caps::default())).await;
        assert_eq!(resp.status, ProtoResult::RESULT_OK.into());
        let Some(file_proto::response::Msg::CapsResp(caps)) = resp.msg else {
            panic!("expected CapsResp");
        };
        for feature in [
            SupportedRequestType::GET_REQUEST,
            SupportedRequestType::CONDITIONAL_PUT,
            SupportedRequestType::DELETE_REQUEST,
            SupportedRequestType::GETDIR_START_AFTER,
        ] {
            assert!(
                caps.server_supported_messages.contains(&(feature as i64)),
                "missing {feature:?}"
            );
        }
    }

    fn preview_msg(paths: &[&str], px: u32) -> Msg {
        let mut p = file_proto::Preview::new();
        p.paths = paths.iter().map(|s| s.to_string()).collect();
        p.px = px;
        Msg::Preview(p)
    }

    fn preview_resp(resp: Response) -> PreviewResponse {
        assert_eq!(
            resp.status,
            ProtoResult::RESULT_OK.into(),
            "got: {:?}",
            resp.status_string
        );
        match resp.msg {
            Some(file_proto::response::Msg::Preview(r)) => r,
            other => panic!("expected Preview, got {other:?}"),
        }
    }

    /// PREVIEW_REQUEST is advertised exactly when the source serves previews.
    #[tokio::test]
    async fn caps_advertise_previews_only_when_served() {
        for served in [false, true] {
            let mut src = InMemSource::new(files(&[]));
            src.previews = served;
            let fs = FileServer::new(src_arc(src));
            let client = open_channel(&fs, "ch-0", None).await;

            let resp = client.request(Msg::Caps(Caps::default())).await;
            let Some(file_proto::response::Msg::CapsResp(caps)) = resp.msg else {
                panic!("expected CapsResp");
            };
            let advertised = caps
                .server_supported_messages
                .contains(&(SupportedRequestType::PREVIEW_REQUEST as i64));
            assert_eq!(advertised, served);
        }
    }

    /// Every outcome of docs/core/previews.md on the wire: a one-message item,
    /// an item over the message budget (contiguous chunks, mime on the first,
    /// `item_done` on the last), each failure status, then the bodyless closing
    /// block — in the source's completion order, every message inside the limit.
    #[tokio::test]
    async fn preview_streams_items_failures_and_a_closing_block() {
        let big: Vec<u8> = (0..20_000u32).map(|i| (i % 251) as u8).collect();
        let mut src = InMemSource::new(files(&[
            ("small.png", b"tiny"),
            ("big.png", &big),
            ("x.bin", b"\0"),
            ("y.err", b"\0"),
        ]));
        src.previews = true;
        let fs = FileServer::new(src_arc(src));
        let client = open_channel(&fs, "ch-0", Some(8 * 1024)).await;

        client
            .send_req(preview_msg(&["small.png", "big.png", "x.bin", "y.err", "missing.png"], 128))
            .await;

        let mut order: Vec<u32> = Vec::new();
        let mut images: HashMap<u32, (Option<String>, Vec<u8>)> = HashMap::new();
        let mut failures: HashMap<u32, ProtoResult> = HashMap::new();
        let mut open_item: Option<u32> = None;
        loop {
            let r = preview_resp(client.recv_resp().await);
            if r.is_last_block {
                assert!(r.body.is_none(), "the closing block carries no body");
                break;
            }
            match r.body {
                Some(file_proto::preview_response::Body::Chunk(c)) => {
                    match open_item {
                        Some(open) => assert_eq!(open, c.index, "chunks of one item are contiguous"),
                        None => {
                            assert!(c.mime_type.is_some(), "the first chunk names the mime type");
                            assert!(c.etag.is_some(), "the first chunk names the version it was rendered from");
                            order.push(c.index);
                        }
                    }
                    let entry = images.entry(c.index).or_insert((None, Vec::new()));
                    if c.mime_type.is_some() {
                        entry.0 = c.mime_type.clone();
                    }
                    entry.1.extend_from_slice(&c.bytes);
                    open_item = if c.item_done { None } else { Some(c.index) };
                }
                Some(file_proto::preview_response::Body::Failure(f)) => {
                    assert!(open_item.is_none(), "a failure never splits an item");
                    order.push(f.index);
                    failures.insert(f.index, f.status.enum_value().unwrap());
                }
                other => panic!("bodyless block before the end: {other:?}"),
            }
        }

        assert_eq!(order, [4, 3, 2, 1, 0], "items go out as the source completes them");
        assert_eq!(images[&0], (Some("image/png".into()), b"tiny".to_vec()));
        assert_eq!(images[&1].1, big, "a multi-chunk item reassembles byte-exact");
        assert_eq!(failures[&2], ProtoResult::RESULT_UNSUPPORTED);
        assert_eq!(failures[&3], ProtoResult::RESULT_FAIL);
        assert_eq!(failures[&4], ProtoResult::RESULT_NO_SUCH_FILE);
        for wire in client.wire.sent_snapshot() {
            assert!(wire.len() <= 8 * 1024, "wire message of {} bytes exceeds the limit", wire.len());
        }
    }

    /// An empty batch is the closing block alone; a source that serves no
    /// previews refuses the request with the top-level status and the channel
    /// stays usable.
    #[tokio::test]
    async fn preview_empty_batch_and_refusal() {
        let mut src = InMemSource::new(files(&[("a.png", b"a")]));
        src.previews = true;
        let fs = FileServer::new(src_arc(src));
        let client = open_channel(&fs, "ch-0", None).await;
        let r = preview_resp(client.request(preview_msg(&[], 64)).await);
        assert!(r.is_last_block && r.body.is_none());

        let fs = FileServer::new(src_arc(InMemSource::new(files(&[("a.png", b"a")]))));
        let client = open_channel(&fs, "ch-1", None).await;
        let resp = client.request(preview_msg(&["a.png"], 64)).await;
        assert_eq!(resp.status, ProtoResult::RESULT_NOT_IMPL.into());
        assert!(resp.msg.is_none());
        let again = client.request(Msg::Caps(Caps::default())).await;
        assert_eq!(again.status, ProtoResult::RESULT_OK.into(), "a refusal does not close the channel");
    }

    /// A get larger than the channel's message budget streams in multiple
    /// blocks: sequential offsets, last block flagged, byte-exact reassembly,
    /// and every wire message inside the negotiated limit.
    #[tokio::test]
    async fn get_streams_blocks_within_message_limit() {
        let data: Vec<u8> = (0..10_000u32).map(|i| (i % 251) as u8).collect();
        let fs = FileServer::new(src_arc(InMemSource::new(files(&[("big.bin", &data)]))));
        let client = open_channel(&fs, "ch-0", Some(4096)).await;

        client.send_req(get_msg("big.bin", None, None)).await;
        let mut got: Vec<u8> = Vec::new();
        let mut blocks = 0;
        loop {
            let r = read_resp(client.recv_resp().await);
            assert_eq!(r.offset, got.len() as u64, "blocks must arrive in order");
            got.extend_from_slice(&r.bytes);
            blocks += 1;
            if r.is_last_block {
                break;
            }
        }
        assert!(blocks > 1, "10KB over a 4KB channel must chunk (got {blocks} block)");
        assert_eq!(got, data);
        for msg in client.wire.sent_snapshot() {
            assert!(
                msg.len() <= 4096,
                "wire message of {} bytes exceeds the limit",
                msg.len()
            );
        }
    }

    /// A Get whose source producer is parked mid-stream must not hold the
    /// source lock: a GetDir on another channel (a write-guard call) has to
    /// complete while the Get is still streaming.
    #[tokio::test]
    async fn get_dir_is_not_blocked_by_a_streaming_get() {
        use crate::transfers::test_support::GetGate;
        let (gate, mut entered) = GetGate::for_keys(["big.bin"]);
        let mut src = InMemSource::new(files(&[("big.bin", b"0123456789"), ("a.txt", b"x")]));
        src.gate = Some(gate.clone());
        let fs = FileServer::new(src_arc(src));

        let get_client = open_channel(&fs, "ch-0", None).await;
        get_client.send_req(get_msg("big.bin", None, None)).await;
        entered.recv().await.expect("the get must park on the gate");

        let dir_client = open_channel(&fs, "ch-1", None).await;
        let mut gd = file_proto::GetDir::new();
        gd.path = String::new();
        gd.max_entries = 100;
        gd.recursive = Some(true);
        let listing = tokio::time::timeout(std::time::Duration::from_secs(2), dir_client.request(Msg::GetDir(gd)))
            .await
            .expect("GetDir must not wait for the streaming Get");
        let names: Vec<String> = dir_resp(listing).entries.iter().map(|e| e.name.clone()).collect();
        assert!(names.contains(&"a.txt".to_string()), "got {names:?}");

        gate.release_all();
        let r = read_resp(get_client.recv_resp().await);
        assert_eq!(r.bytes, b"0123456789");
        assert!(r.is_last_block);
    }

    #[tokio::test]
    async fn get_honors_offset_and_length() {
        let fs = FileServer::new(src_arc(InMemSource::new(files(&[("f.txt", b"hello world")]))));
        let client = open_channel(&fs, "ch-0", None).await;

        client.send_req(get_msg("f.txt", Some(6), Some(5))).await;
        let r = read_resp(client.recv_resp().await);
        assert_eq!(r.bytes, b"world");
        assert_eq!(r.offset, 6);
        assert!(r.is_last_block);
    }

    /// max_entries caps a page: the server ends the page with a cursor, and the
    /// stashed live iterator resumes from it — across separate request channels,
    /// as the per-request architecture does it.
    #[tokio::test]
    async fn get_dir_paginates_and_resumes_by_cursor() {
        let names: Vec<String> = (0..10).map(|i| format!("f{i:02}.txt")).collect();
        let pairs: Vec<(&str, &[u8])> = names.iter().map(|n| (n.as_str(), b"x".as_slice())).collect();
        let fs = FileServer::new(src_arc(InMemSource::new(files(&pairs))));

        let mut seen: Vec<String> = Vec::new();
        let mut cursor: Option<String> = None;
        let mut requests = 0;
        loop {
            let client = open_channel(&fs, &format!("ch-{requests}"), None).await;
            let mut gd = file_proto::GetDir::new();
            gd.path = String::new();
            gd.max_entries = 3;
            gd.recursive = Some(true);
            gd.cursor = cursor.clone();
            let r = dir_resp(client.request(Msg::GetDir(gd)).await);
            assert!(r.is_last_block, "each page's final block is flagged");
            assert!(r.entries.len() <= 3, "page exceeds max_entries");
            seen.extend(r.entries.iter().map(|e| e.name.clone()));
            requests += 1;
            cursor = r.cursor;
            if cursor.is_none() {
                break;
            }
        }
        assert!(requests >= 4, "10 entries at 3 per page need at least 4 pages");
        seen.sort();
        assert_eq!(seen, names, "every entry exactly once, none lost at page seams");
    }

    /// Within one request, a listing bigger than the message budget splits into
    /// several non-final blocks — each within the wire limit — before the final
    /// flagged one.
    #[tokio::test]
    async fn get_dir_blocks_respect_message_limit() {
        let names: Vec<String> = (0..60).map(|i| format!("{}-{i:03}", "d".repeat(100))).collect();
        let pairs: Vec<(&str, &[u8])> = names.iter().map(|n| (n.as_str(), b"x".as_slice())).collect();
        let fs = FileServer::new(src_arc(InMemSource::new(files(&pairs))));
        let client = open_channel(&fs, "ch-0", Some(2048)).await;

        let mut gd = file_proto::GetDir::new();
        gd.max_entries = 1000;
        gd.recursive = Some(true);
        client.send_req(Msg::GetDir(gd)).await;

        let mut seen: Vec<String> = Vec::new();
        let mut blocks = 0;
        loop {
            let r = dir_resp(client.recv_resp().await);
            seen.extend(r.entries.iter().map(|e| e.name.clone()));
            blocks += 1;
            if r.is_last_block {
                assert!(r.cursor.is_none(), "exhausted listing carries no cursor");
                break;
            }
        }
        assert!(
            blocks > 1,
            "60 long names over a 2KB channel must split (got {blocks} block)"
        );
        for msg in client.wire.sent_snapshot() {
            assert!(
                msg.len() <= 2048,
                "wire message of {} bytes exceeds the limit",
                msg.len()
            );
        }
        seen.sort();
        let mut expected = names;
        expected.sort();
        assert_eq!(seen, expected);
    }

    #[tokio::test]
    async fn get_attrs_answers_entry_symlink_and_missing() {
        let mut src = InMemSource::new(files(&[("f.txt", b"hello")]));
        src.symlinks.insert("link".into(), "f.txt".into());
        let fs = FileServer::new(src_arc(src));
        let client = open_channel(&fs, "ch-0", None).await;

        let ga = |path: &str| {
            let mut m = file_proto::GetAttrs::new();
            m.path = path.to_string();
            Msg::GetAttrs(m)
        };

        let resp = client.request(ga("f.txt")).await;
        assert_eq!(resp.status, ProtoResult::RESULT_OK.into());
        let Some(file_proto::response::Msg::GetAttrsResp(a)) = resp.msg else {
            panic!("expected GetAttrsResp");
        };
        let entry = a.dir_entry.unwrap();
        assert_eq!(entry.name, "f.txt");
        assert_eq!(entry.file_size, Some(5));

        let resp = client.request(ga("link")).await;
        let Some(file_proto::response::Msg::GetAttrsResp(a)) = resp.msg else {
            panic!("expected GetAttrsResp");
        };
        assert_eq!(a.dir_entry.unwrap().symlink_target.as_deref(), Some("f.txt"));

        let resp = client.request(ga("missing")).await;
        assert_eq!(resp.status, ProtoResult::RESULT_NO_SUCH_FILE.into());
    }

    /// A slow stats walk streams its running totals (non-final blocks) and only
    /// the complete tally carries `is_last_block`.
    #[tokio::test]
    async fn dir_stats_streams_partials_then_terminal_total() {
        let release = Arc::new(tokio::sync::Notify::new());
        let fs = FileServer::new(src_arc(GatedStatsSource {
            files: files(&[("a.txt", b"hello".as_slice()), ("b.txt", b"world!".as_slice())]),
            release: release.clone(),
            data_source_id: None,
        }));
        let client = open_channel(&fs, "ch-0", None).await;

        let mut ds = file_proto::DirStats::new();
        ds.path = String::new();
        client.send_req(Msg::DirStats(ds)).await;

        release.notify_one();
        let mut partials = 0;
        loop {
            let resp = client.recv_resp().await;
            let Some(file_proto::response::Msg::DirStatsResp(s)) = resp.msg else {
                panic!("expected DirStatsResp");
            };
            if s.is_last_block {
                assert_eq!(s.number_of_files, 2);
                assert_eq!(s.total_data_size, 11);
                break;
            }
            assert_eq!(s.number_of_files, 1, "partial block carries the running total");
            partials += 1;
        }
        assert_eq!(partials, 1, "the gated walk's first block must arrive as a partial");
    }

    /// A walk that yields nothing still owes the client one terminal block —
    /// otherwise the client waits out its receive timeout.
    #[tokio::test]
    async fn dir_stats_empty_walk_sends_terminal_block() {
        let fs = FileServer::new(src_arc(ScriptedSource::new()));
        let client = open_channel(&fs, "ch-0", None).await;

        let mut ds = file_proto::DirStats::new();
        ds.path = String::new();
        let resp = client.request(Msg::DirStats(ds)).await;
        let Some(file_proto::response::Msg::DirStatsResp(s)) = resp.msg else {
            panic!("expected DirStatsResp");
        };
        assert!(s.is_last_block);
        assert_eq!(s.number_of_files, 0);
        assert_eq!(s.total_data_size, 0);
    }

    fn put_msg(path: &str, bytes: &[u8]) -> Msg {
        let mut p = file_proto::Put::new();
        p.path = path.to_string();
        p.size = bytes.len() as u64;
        p.bytes = bytes.to_vec();
        p.is_last_block = true;
        Msg::Put(p)
    }

    fn delete_msg(path: &str) -> Msg {
        let mut d = file_proto::Delete::new();
        d.path = path.to_string();
        Msg::Delete(d)
    }

    #[tokio::test]
    async fn put_and_delete_map_typed_errors_to_wire_status() {
        let dest = Arc::new(ScriptedDest::new());
        dest.fail_key("locked.txt", DestErrKind::Precondition);
        dest.fail_key("ghost.txt", DestErrKind::NoSuchKey);
        dest.fail_key("old.txt", DestErrKind::NotImplemented);
        dest.fail_key("broken.txt", DestErrKind::Other);

        struct DestProxy(Arc<ScriptedDest>);
        #[async_trait::async_trait]
        impl crate::transfers::Storage for DestProxy {
            async fn connect(&mut self) -> Result<StorageMetaState, StorageError> {
                Ok(StorageMetaState::Unknown)
            }
            async fn cancel(&self) -> Result<(), StorageError> {
                Ok(())
            }
            fn data_source_id(&self) -> Option<&str> {
                None
            }
            fn set_data_source_id(&mut self, _: String) {}
        }
        #[async_trait::async_trait]
        impl StorageDest for DestProxy {
            fn is_sequential(&self) -> bool {
                false
            }
            async fn put_file(
                &self,
                k: &str,
                d: &[u8],
                a: Option<&crate::transfers::StorageFileAttrs>,
                c: Option<&StoragePutConditions>,
            ) -> Result<crate::transfers::StoragePutResult, StorageError> {
                self.0.put_file(k, d, a, c).await
            }
            async fn put_dir(
                &self,
                k: &str,
                a: Option<&crate::transfers::StorageFileAttrs>,
            ) -> Result<crate::transfers::StoragePutResult, StorageError> {
                self.0.put_dir(k, a).await
            }
            async fn put_symlink(
                &self,
                k: &str,
                t: &str,
                a: Option<&crate::transfers::StorageFileAttrs>,
            ) -> Result<crate::transfers::StoragePutResult, StorageError> {
                self.0.put_symlink(k, t, a).await
            }
            async fn del_file(&self, k: &str) -> Result<(), StorageError> {
                self.0.del_file(k).await
            }
            async fn del_dir(&self, k: &str) -> Result<(), StorageError> {
                self.0.del_dir(k).await
            }
            async fn update_stats(&self, s: &crate::transfers::StorageStatInfo) -> Result<(), StorageError> {
                self.0.update_stats(s).await
            }
            async fn close(&self) -> Result<(), StorageError> {
                Ok(())
            }
        }

        let fs = FileServer::with_dest(
            src_arc(InMemSource::new(files(&[]))),
            Arc::new(RwLock::new(DestProxy(dest.clone()))),
        );
        let client = open_channel(&fs, "ch-0", None).await;

        // Success: bytes stored, dest's etag echoed.
        let resp = client.request(put_msg("ok.txt", b"payload")).await;
        assert_eq!(resp.status, ProtoResult::RESULT_OK.into());
        let Some(file_proto::response::Msg::PutResp(p)) = resp.msg else {
            panic!("expected PutResp");
        };
        assert_eq!(p.etag, "etag-ok.txt");
        assert_eq!(dest.snapshot().await["ok.txt"], b"payload");

        // Typed errors keep their identity on the wire.
        let resp = client.request(put_msg("locked.txt", b"x")).await;
        assert_eq!(resp.status, ProtoResult::RESULT_PRECONDITION_FAILED.into());
        let resp = client.request(delete_msg("ghost.txt")).await;
        assert_eq!(resp.status, ProtoResult::RESULT_NO_SUCH_FILE.into());
        let resp = client.request(put_msg("old.txt", b"x")).await;
        assert_eq!(resp.status, ProtoResult::RESULT_NOT_IMPL.into());
        let resp = client.request(put_msg("broken.txt", b"x")).await;
        assert_eq!(
            resp.status,
            ProtoResult::RESULT_FAIL.into(),
            "untyped errors are generic FAIL"
        );

        // Delete success.
        let resp = client.request(delete_msg("ok.txt")).await;
        assert_eq!(resp.status, ProtoResult::RESULT_OK.into());
        assert!(!dest.snapshot().await.contains_key("ok.txt"));
    }

    #[tokio::test]
    async fn writes_without_dest_are_refused() {
        let fs = FileServer::new(src_arc(InMemSource::new(files(&[]))));
        let client = open_channel(&fs, "ch-0", None).await;
        let resp = client.request(put_msg("f.txt", b"x")).await;
        assert_eq!(resp.status, ProtoResult::RESULT_FAIL.into());
        assert!(resp.status_string.unwrap().contains("Write not supported"));
    }

    fn search_msg(limit: Option<i32>) -> Msg {
        let mut s = file_proto::Search::new();
        s.path = String::new();
        s.name_match = Some("f".to_string());
        s.limit = limit;
        Msg::Search(s)
    }

    #[tokio::test]
    async fn search_passes_page_token_through_when_it_fits() {
        let src = ScriptedSource::new();
        src.push_search_page(SearchResult {
            items: vec![soi_file("f1.txt", 1), soi_file("f2.txt", 2)],
            next_page_token: Some("tok-2".into()),
        });
        let fs = FileServer::new(src_arc(src));
        let client = open_channel(&fs, "ch-0", None).await;

        let resp = client.request(search_msg(Some(10))).await;
        let Some(file_proto::response::Msg::SearchResp(s)) = resp.msg else {
            panic!("expected SearchResp");
        };
        assert_eq!(s.items.len(), 2);
        assert_eq!(s.next_page_token.as_deref(), Some("tok-2"));
    }

    /// A meta-backed source (no walk to park) pages under the channel's
    /// message limit by itself: every wire message fits, the offset token
    /// resumes exactly where the page was cut.
    #[tokio::test]
    async fn search_over_meta_pages_under_the_message_limit() {
        let names: Vec<String> = (0..50).map(|i| format!("{}-{i:03}", "n".repeat(100))).collect();
        let total = names.len();
        let pairs: Vec<(&str, &[u8])> = names.iter().map(|n| (n.as_str(), &b"x"[..])).collect();
        let fs = FileServer::new(src_arc(InMemSource::new(files(&pairs))));
        let client = open_channel(&fs, "ch-0", Some(2048)).await;

        let mut got = 0;
        let mut token: Option<String> = None;
        let mut pages = 0;
        loop {
            let mut s = file_proto::Search::new();
            s.path = String::new();
            s.name_match = Some("n".to_string());
            s.limit = Some(1000);
            s.page_token = token.clone();
            let resp = client.request(Msg::Search(s)).await;
            let Some(file_proto::response::Msg::SearchResp(s)) = resp.msg else {
                panic!("expected SearchResp");
            };
            pages += 1;
            assert!(pages <= total, "paging must terminate");
            assert!(!s.items.is_empty());
            got += s.items.len();
            token = s.next_page_token;
            if token.is_none() {
                break;
            }
        }
        assert_eq!(got, total, "every entry arrives exactly once across the pages");
        assert!(pages > 1, "the message limit must have cut the result into pages");
        for msg in client.wire.sent_snapshot() {
            assert!(
                msg.len() <= 2048,
                "wire message of {} bytes exceeds the limit",
                msg.len()
            );
        }
    }

    /// An unrecognized request answers Not-implemented and leaves the channel
    /// usable for the next request.
    #[tokio::test]
    async fn unknown_request_answers_not_implemented_and_channel_survives() {
        let fs = FileServer::new(src_arc(InMemSource::new(files(&[]))));
        let client = open_channel(&fs, "ch-0", None).await;

        let resp = client.request(Msg::Close(file_proto::Close::new())).await;
        assert_eq!(resp.status, ProtoResult::RESULT_FAIL.into());
        assert!(resp.status_string.unwrap().contains("Not implemented"));

        let resp = client.request(Msg::Caps(Caps::default())).await;
        assert_eq!(resp.status, ProtoResult::RESULT_OK.into(), "channel must survive");
    }
}
