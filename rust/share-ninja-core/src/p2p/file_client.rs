//! FileClient - P2P file client for requesting files over WebRTC
//!
//! Per-request DataChannel architecture: each request gets its own channel.
//! Cancellation: just close the channel.

use protobuf::Message;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};

use super::webrtc_connector::WebRTCConnector;
use crate::transfers::StorageFileType;
use crate::transfers::{StorageError, StoragePutConditions};
use share_ninja_proto::file_proto::{
    self, Caps, Close, Delete, DirStats, FileType, Get, GetAttrs, GetDir, ListUpdates, ListUpdatesBegin, Open,
    OpenFlags, OpenMode, Put, PutDir, Read, Request, Response, Result as ProtoResult, Search, SupportedRequestType,
};

/// Typed failure of an object-store request (`put_file`, `delete`,
/// `get_attrs`): the codes a NinjaStorageV3 client branches on keep their
/// identity across the wire; everything else is `Other`. The legacy
/// `String`-erroring methods are unchanged.
#[derive(Debug, Clone)]
pub enum FileClientError {
    /// `Put.if_match` / `if_none_match` did not hold — the expected outcome of
    /// a lost compare-and-swap.
    PreconditionFailed(String),
    NoSuchFile(String),
    NotImplemented(String),
    Other(String),
}

impl From<String> for FileClientError {
    fn from(s: String) -> Self {
        FileClientError::Other(s)
    }
}

impl From<FileClientError> for StorageError {
    fn from(e: FileClientError) -> Self {
        match e {
            FileClientError::PreconditionFailed(m) => StorageError::PreconditionFailed(m),
            FileClientError::NoSuchFile(m) => StorageError::NoSuchKey(m),
            FileClientError::NotImplemented(m) => StorageError::NotImplemented(m),
            FileClientError::Other(m) => StorageError::P2P(m),
        }
    }
}

impl std::fmt::Display for FileClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FileClientError::PreconditionFailed(m) => write!(f, "precondition failed: {m}"),
            FileClientError::NoSuchFile(m) => write!(f, "no such file: {m}"),
            FileClientError::NotImplemented(m) => write!(f, "not implemented: {m}"),
            FileClientError::Other(m) => f.write_str(m),
        }
    }
}

/// Map a non-OK response to the typed error.
fn status_error(label: &str, path: &str, resp: &Response) -> FileClientError {
    let msg = format!("{} {} failed: {:?}", label, path, resp.status_string);
    match resp.status.enum_value_or_default() {
        ProtoResult::RESULT_PRECONDITION_FAILED => FileClientError::PreconditionFailed(msg),
        ProtoResult::RESULT_NO_SUCH_FILE => FileClientError::NoSuchFile(msg),
        ProtoResult::RESULT_NOT_IMPL | ProtoResult::RESULT_UNSUPPORTED => FileClientError::NotImplemented(msg),
        _ => FileClientError::Other(msg),
    }
}

/// File attributes from the server. Mirrors every field of proto `FileAttrs`;
/// keep in sync when the proto gains fields. Conversions in both directions live
/// in the `From` impls below so the field list is maintained in exactly one place.
#[derive(Debug, Clone, Default)]
pub struct FileAttrs {
    pub creation_date: Option<chrono::DateTime<chrono::Utc>>,
    pub modification_date: Option<chrono::DateTime<chrono::Utc>>,
    pub posix_permissions: Option<i64>,
    pub email_subject: Option<String>,
    pub gdrive_file_id: Option<String>,
    pub gdrive_mime_type: Option<String>,
}

impl From<file_proto::FileAttrs> for FileAttrs {
    fn from(a: file_proto::FileAttrs) -> Self {
        FileAttrs {
            // Proto carries these as microseconds since the Unix epoch.
            creation_date: a.creation_date.and_then(chrono::DateTime::from_timestamp_micros),
            modification_date: a.modification_date.and_then(chrono::DateTime::from_timestamp_micros),
            posix_permissions: a.posix_permissions,
            email_subject: a.email_subject,
            gdrive_file_id: a.gdrive_file_id,
            gdrive_mime_type: a.gdrive_mime_type,
        }
    }
}

impl From<&FileAttrs> for file_proto::FileAttrs {
    fn from(a: &FileAttrs) -> Self {
        let mut fa = file_proto::FileAttrs::new();
        fa.creation_date = a.creation_date.map(|d| d.timestamp_micros());
        fa.modification_date = a.modification_date.map(|d| d.timestamp_micros());
        fa.posix_permissions = a.posix_permissions;
        fa.email_subject = a.email_subject.clone();
        fa.gdrive_file_id = a.gdrive_file_id.clone();
        fa.gdrive_mime_type = a.gdrive_mime_type.clone();
        fa
    }
}

/// Directory entry from the server. Mirrors every field of proto `DirEntry`;
/// keep in sync when the proto gains fields.
#[derive(Debug, Clone)]
pub struct DirEntry {
    pub name: String,
    pub file_type: FileType,
    pub file_size: Option<u64>,
    pub symlink_target: Option<String>,
    pub symlink_target_is_dir: Option<bool>,
    pub attrs: Option<FileAttrs>,
    pub number_of_files: Option<u64>,
    pub number_of_folders: Option<u64>,
    pub is_read_only: Option<bool>,
    pub share_name: Option<String>,
    pub etag: Option<String>,
}

impl From<file_proto::DirEntry> for DirEntry {
    fn from(e: file_proto::DirEntry) -> Self {
        let file_type = e.file_type.enum_value_or_default();
        DirEntry {
            name: e.name,
            file_type,
            file_size: e.file_size,
            symlink_target: e.symlink_target,
            symlink_target_is_dir: e.symlink_target_is_dir,
            attrs: e.attrs.into_option().map(FileAttrs::from),
            number_of_files: e.number_of_files,
            number_of_folders: e.number_of_folders,
            is_read_only: e.is_read_only,
            share_name: e.share_name,
            etag: e.etag,
        }
    }
}

impl DirEntry {
    /// Convert the proto FileType to StorageFileType
    pub fn to_storage_file_type(&self) -> StorageFileType {
        match self.file_type {
            FileType::FILE_TYPE_REGULAR => StorageFileType::Regular,
            FileType::FILE_TYPE_DIRECTORY => StorageFileType::Dir,
            FileType::FILE_TYPE_SYMBOLIC_LINK => StorageFileType::Symlink,
            _ => StorageFileType::Regular,
        }
    }
}

#[derive(Debug, Clone)]
pub struct GetResponse {
    pub offset: u64,
    pub bytes: Vec<u8>,
    pub is_last_block: bool,
}

#[derive(Debug, Clone)]
pub struct GetDirResponse {
    pub entries: Vec<DirEntry>,
    pub cursor: Option<String>,
    pub is_last_block: bool,
}

/// Streaming response block — the loop stops after the block whose `is_last_block`
/// is set (mirrors TS `streamingRequest`'s `T extends { isLastBlock?: boolean }`).
trait StreamBlock {
    fn is_last_block(&self) -> bool;
}

impl StreamBlock for GetResponse {
    fn is_last_block(&self) -> bool {
        self.is_last_block
    }
}

impl StreamBlock for GetDirResponse {
    fn is_last_block(&self) -> bool {
        self.is_last_block
    }
}

/// One block of a `Preview` response (docs/core/previews.md): a chunk of a
/// successful item, one failed item, or the bodyless end of the batch. Indices
/// are into the request's paths; items arrive in completion order, the chunks
/// of one item contiguous.
#[derive(Debug)]
pub enum PreviewBlock {
    Chunk {
        index: usize,
        bytes: Vec<u8>,
        /// On the item's first chunk.
        mime_type: Option<String>,
        /// On the item's first chunk: the version of the file it was rendered from.
        etag: Option<String>,
        item_done: bool,
    },
    Failure {
        index: usize,
        status: ProtoResult,
    },
    End,
}

impl StreamBlock for PreviewBlock {
    fn is_last_block(&self) -> bool {
        matches!(self, PreviewBlock::End)
    }
}

/// Most paths one `Preview` request carries. A request cannot be cancelled (see
/// `streaming_request`): an abandoned one is generated and sent in full, so
/// this bounds the waste. Temporary — it goes once a request can be closed.
const PREVIEW_MAX_PATHS: usize = 32;

/// Most path bytes one `Preview` request carries: the request is one message,
/// and the smallest message a channel accepts is 65535 bytes.
const PREVIEW_MAX_PATH_BYTES: usize = 48 * 1024;

/// Split `paths` into consecutive `Preview` requests, each within the path
/// count and the message size above.
pub fn preview_request_groups(paths: &[String]) -> Vec<std::ops::Range<usize>> {
    let mut groups = Vec::new();
    let mut start = 0;
    let mut bytes = 0;
    for (i, path) in paths.iter().enumerate() {
        let full = i - start >= PREVIEW_MAX_PATHS || bytes + path.len() > PREVIEW_MAX_PATH_BYTES;
        if full && i > start {
            groups.push(start..i);
            start = i;
            bytes = 0;
        }
        bytes += path.len();
    }
    if start < paths.len() {
        groups.push(start..paths.len());
    }
    groups
}

#[derive(Debug, Clone, Default)]
pub struct DirStatsResponse {
    pub number_of_files: u32,
    pub number_of_folders: u32,
    pub number_of_symlinks: u32,
    pub total_size: u64,
    /// Set on the walk's final block. Earlier blocks are running totals — the
    /// walk keeps the wire busy so the per-message receive timeout can't fire.
    pub is_last_block: bool,
}

impl StreamBlock for DirStatsResponse {
    fn is_last_block(&self) -> bool {
        self.is_last_block
    }
}

#[derive(Debug, Clone)]
pub struct GetAttrsResponse {
    pub dir_entry: Option<DirEntry>,
}

#[derive(Debug, Clone)]
pub struct CapsResponse {
    pub server_supported_messages: Vec<i64>,
}

#[derive(Debug, Clone)]
pub struct PutFileResponse {
    pub etag: String,
}

#[derive(Debug, Clone)]
pub struct PutDirResponse {
    pub etag: String,
}

#[derive(Debug, Clone)]
pub struct OpenResponse {
    pub handle: u32,
    pub file_size: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct ListUpdatesBeginResponse {
    pub cursor: String,
}

/// A storage change from a listUpdates response
#[derive(Debug, Clone)]
pub struct StorageChange {
    pub change_type: i32,
    pub item: Option<DirEntry>,
}

#[derive(Debug, Clone)]
pub struct ListUpdatesResponse {
    pub changes: Vec<StorageChange>,
    pub cursor: String,
    pub has_more: bool,
}

#[derive(Debug, Clone)]
pub struct SearchResponse {
    pub items: Vec<DirEntry>,
    pub has_more: bool,
    pub next_page_token: Option<String>,
}

/// FileClient - Per-request DataChannel architecture
///
/// Each request acquires its own channel from the pool, sends the request,
/// receives the response, and releases the channel back to the pool.
pub struct FileClient {
    // Bare `Arc` (not `Arc<Mutex<…>>`): `acquire_channel`/`release_channel` take
    // `&self`, so the old outer Mutex only served to serialize every request through
    // one lock — held across `acquire_channel`'s up-to-10s channel open. Sharing the
    // connector directly lets requests acquire channels concurrently.
    connector: Arc<WebRTCConnector>,
    server_caps: Vec<i64>,
    #[allow(dead_code)]
    connection_error: Arc<Mutex<Option<String>>>,
}

impl FileClient {
    pub fn new(connector: Arc<WebRTCConnector>) -> Self {
        Self {
            connector,
            server_caps: Vec::new(),
            connection_error: Arc::new(Mutex::new(None)),
        }
    }

    fn check_connection(&self) -> Result<(), String> {
        if let Ok(err) = self.connection_error.try_lock()
            && let Some(ref e) = *err
        {
            return Err(e.clone());
        }
        Ok(())
    }

    /// Connect and get server capabilities
    pub async fn connect(&mut self) -> Result<(), String> {
        tracing::debug!("Connecting to server, requesting caps");
        let caps = self.get_caps().await?;
        self.server_caps = caps.server_supported_messages;
        tracing::debug!("Connected, server caps: {:?}", self.server_caps);
        Ok(())
    }

    /// Single request-response pattern using per-request channel
    async fn single_request(&self, msg: Request) -> Result<Response, String> {
        self.check_connection()?;

        let channel = self.connector.acquire_channel().await?;

        // Run the request, then settle the channel. On ANY error we close it rather
        // than drop it: dropping the wrapper does not close the underlying transport,
        // which would strand the server's per-request task waiting on a recv that can
        // never complete (and leave a half-used channel that can't be pooled).
        let result = async {
            let data = msg.write_to_bytes().map_err(|e| e.to_string())?;
            channel.send(&data).await?;
            let resp_data = channel.recv(30000).await?;
            Response::parse_from_bytes(&resp_data).map_err(|e| e.to_string())
        }
        .await;

        match result {
            Ok(resp) => {
                self.connector.release_channel(channel).await;
                Ok(resp)
            }
            Err(e) => {
                channel.close().await;
                Err(e)
            }
        }
    }

    /// Streaming response pattern (mirrors TS `streamingRequest`): send `req`, then push
    /// each parsed block over an mpsc channel as it arrives. On normal completion (a block
    /// with `is_last_block` set) the channel is released back to the pool; if the consumer
    /// drops the receiver early, the channel is closed instead — cancelling the server
    /// (TS's premature-close). Errors are delivered as an `Err` item.
    async fn streaming_request<T, F>(
        &self,
        req: Request,
        label: &'static str,
        parse: F,
    ) -> Result<mpsc::Receiver<Result<T, String>>, String>
    where
        T: StreamBlock + Send + 'static,
        F: Fn(Response) -> Result<T, String> + Send + 'static,
    {
        self.check_connection()?;

        let channel = self.connector.acquire_channel().await?;

        // Send the request on the caller's task so request errors surface synchronously.
        let data = req.write_to_bytes().map_err(|e| e.to_string())?;
        channel.send(&data).await?;

        let (tx, rx) = mpsc::channel(4);
        let connector = self.connector.clone();

        tokio::spawn(async move {
            let mut completed = false;
            loop {
                let resp_data = tokio::select! {
                    // Consumer dropped the receiver → premature close.
                    _ = tx.closed() => break,
                    r = channel.recv(30000) => match r {
                        Ok(d) => d,
                        Err(e) => { let _ = tx.send(Err(e)).await; break; }
                    }
                };

                let resp = match Response::parse_from_bytes(&resp_data) {
                    Ok(r) => r,
                    Err(e) => {
                        let _ = tx.send(Err(e.to_string())).await;
                        break;
                    }
                };

                if resp.status != ProtoResult::RESULT_OK.into() {
                    let _ = tx
                        .send(Err(format!("{} failed: {:?}", label, resp.status_string)))
                        .await;
                    break;
                }

                let block = match parse(resp) {
                    Ok(b) => b,
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        break;
                    }
                };
                let is_last = block.is_last_block();

                if tx.send(Ok(block)).await.is_err() {
                    // Consumer dropped between recv and send → premature close.
                    break;
                }
                if is_last {
                    completed = true;
                    break;
                }
            }

            if completed {
                connector.release_channel(channel).await;
            } else {
                channel.close().await;
            }
        });

        Ok(rx)
    }

    /// Get file data, streaming each block over an mpsc channel as it arrives.
    /// Dropping the receiver cancels the request (closes the channel / stops the server).
    pub async fn get_streamed(
        &self,
        path: &str,
        offset: Option<u64>,
        length: Option<u64>,
    ) -> Result<mpsc::Receiver<Result<GetResponse, String>>, String> {
        let mut get_msg = Get::new();
        get_msg.path = path.to_string();
        if let Some(off) = offset {
            get_msg.offset = Some(off);
        }
        if let Some(len) = length {
            tracing::debug!("getstreamed {} length", len);
            get_msg.length = Some(len);
        } else {
            tracing::debug!("getstreamed");
        }

        let mut req = Request::new();
        req.msg = Some(file_proto::request::Msg::Get(get_msg));

        self.streaming_request(req, "Get", |resp| {
            if let Some(file_proto::response::Msg::ReadResp(read_resp)) = resp.msg {
                Ok(GetResponse {
                    offset: read_resp.offset,
                    bytes: read_resp.bytes,
                    is_last_block: read_resp.is_last_block,
                })
            } else {
                Err("Unexpected response type".to_string())
            }
        })
        .await
    }

    /// Get directory listing, streaming each block over an mpsc channel as it arrives.
    /// Dropping the receiver cancels the request (closes the channel / stops the server).
    /// `start_after` (exclusive key) requires a peer advertising
    /// `GETDIR_START_AFTER` — check `supports` first; an older peer would
    /// silently ignore the field and list from the beginning.
    pub async fn get_dir_streamed(
        &self,
        path: &str,
        recursive: bool,
        max_entries: i32,
        cursor: Option<&str>,
        start_after: Option<&str>,
    ) -> Result<mpsc::Receiver<Result<GetDirResponse, String>>, String> {
        let mut get_dir_msg = GetDir::new();
        get_dir_msg.path = path.to_string();
        get_dir_msg.max_entries = max_entries;
        get_dir_msg.recursive = Some(recursive);
        if let Some(c) = cursor {
            get_dir_msg.cursor = Some(c.to_string());
        }
        if let Some(a) = start_after {
            get_dir_msg.start_after = Some(a.to_string());
        }

        let mut req = Request::new();
        req.msg = Some(file_proto::request::Msg::GetDir(get_dir_msg));

        tracing::debug!(
            "Sending GETDIR request: path={}, max_entries={}, recursive={}, cursor={:?}",
            path,
            max_entries,
            recursive,
            cursor
        );

        self.streaming_request(req, "GetDir", |resp| {
            if let Some(file_proto::response::Msg::GetDirResp(dir_resp)) = resp.msg {
                let entries = dir_resp.entries.into_iter().map(DirEntry::from).collect();

                Ok(GetDirResponse {
                    entries,
                    cursor: dir_resp.cursor,
                    is_last_block: dir_resp.is_last_block,
                })
            } else {
                Err("Unexpected response type".to_string())
            }
        })
        .await
    }

    /// Get directory statistics, streaming the peer's running totals as its
    /// walk proceeds (last block = the answer). Streamed rather than one-shot
    /// because a recursive stat of a big tree runs longer than the per-message
    /// receive timeout, which restarts on every block that arrives.
    /// Dropping the receiver cancels the request (closes the channel / stops the server).
    pub async fn dir_stats_streamed(
        &self,
        path: &str,
    ) -> Result<mpsc::Receiver<Result<DirStatsResponse, String>>, String> {
        let mut dir_stats_msg = DirStats::new();
        dir_stats_msg.path = path.to_string();

        let mut req = Request::new();
        req.msg = Some(file_proto::request::Msg::DirStats(dir_stats_msg));

        tracing::debug!("Sending DIRSTATS request: path={}", path);

        self.streaming_request(req, "DirStats", |resp| {
            if let Some(file_proto::response::Msg::DirStatsResp(stats_resp)) = resp.msg {
                Ok(DirStatsResponse {
                    number_of_files: stats_resp.number_of_files as u32,
                    number_of_folders: stats_resp.number_of_folders as u32,
                    number_of_symlinks: stats_resp.number_of_symlinks as u32,
                    total_size: stats_resp.total_data_size as u64,
                    is_last_block: stats_resp.is_last_block,
                })
            } else {
                Err("Unexpected response type".to_string())
            }
        })
        .await
    }

    /// One `Preview` request: thumbnails of `paths` bounded by `px`, streamed as
    /// the peer completes them. The caller keeps `paths` within one
    /// `preview_request_groups` group. Dropping the receiver cancels the
    /// request (closes the channel / stops the server).
    pub async fn get_previews(
        &self,
        paths: Vec<String>,
        px: u32,
    ) -> Result<mpsc::Receiver<Result<PreviewBlock, String>>, String> {
        tracing::debug!("Sending PREVIEW request: {} paths, px={}", paths.len(), px);

        let mut preview_msg = file_proto::Preview::new();
        preview_msg.paths = paths;
        preview_msg.px = px;

        let mut req = Request::new();
        req.msg = Some(file_proto::request::Msg::Preview(preview_msg));

        self.streaming_request(req, "Preview", |resp| {
            let Some(file_proto::response::Msg::Preview(preview_resp)) = resp.msg else {
                return Err("Unexpected response type".to_string());
            };
            if preview_resp.is_last_block {
                return Ok(PreviewBlock::End);
            }
            match preview_resp.body {
                Some(file_proto::preview_response::Body::Chunk(chunk)) => Ok(PreviewBlock::Chunk {
                    index: chunk.index as usize,
                    bytes: chunk.bytes,
                    mime_type: chunk.mime_type,
                    etag: chunk.etag,
                    item_done: chunk.item_done,
                }),
                Some(file_proto::preview_response::Body::Failure(failure)) => Ok(PreviewBlock::Failure {
                    index: failure.index as usize,
                    status: failure.status.enum_value_or(ProtoResult::RESULT_FAIL),
                }),
                _ => Err("Preview block without a body".to_string()),
            }
        })
        .await
    }

    /// Buffered `dir_stats_streamed`: drain the walk and keep the final block.
    pub async fn dir_stats(&self, path: &str) -> Result<DirStatsResponse, String> {
        let mut rx = self.dir_stats_streamed(path).await?;
        let mut last: Option<DirStatsResponse> = None;
        while let Some(block) = rx.recv().await {
            last = Some(block?);
        }
        last.ok_or_else(|| "DirStats: no response".to_string())
    }

    /// Get file/directory attributes
    pub async fn get_attrs(&self, path: &str) -> Result<GetAttrsResponse, FileClientError> {
        let mut get_attrs_msg = GetAttrs::new();
        get_attrs_msg.path = path.to_string();

        let mut req = Request::new();
        req.msg = Some(file_proto::request::Msg::GetAttrs(get_attrs_msg));

        let resp = self.single_request(req).await?;

        if resp.status != ProtoResult::RESULT_OK.into() {
            return Err(status_error("GetAttrs", path, &resp));
        }

        if let Some(file_proto::response::Msg::GetAttrsResp(attrs_resp)) = resp.msg {
            let dir_entry = attrs_resp.dir_entry.into_option().map(DirEntry::from);

            Ok(GetAttrsResponse { dir_entry })
        } else {
            Err(FileClientError::Other("Unexpected response type".to_string()))
        }
    }

    /// Whether the peer advertised a feature in its `Caps`.
    pub fn supports(&self, feature: SupportedRequestType) -> bool {
        self.server_caps.contains(&(feature as i64))
    }

    /// Put (upload) a file. `conditions` require a peer advertising
    /// `CONDITIONAL_PUT` — refused locally otherwise, since an older peer would
    /// ignore the fields and overwrite.
    pub async fn put_file(
        &self,
        path: &str,
        data: &[u8],
        attrs: Option<&FileAttrs>,
        conditions: Option<&StoragePutConditions>,
    ) -> Result<PutFileResponse, FileClientError> {
        let mut put_msg = Put::new();
        put_msg.path = path.to_string();
        put_msg.size = data.len() as u64;
        put_msg.bytes = data.to_vec();
        put_msg.is_last_block = true;
        if let Some(a) = attrs {
            put_msg.attrs = protobuf::MessageField::some(file_proto::FileAttrs::from(a));
        }
        if let Some(c) = conditions
            && !c.is_empty()
        {
            if !self.supports(SupportedRequestType::CONDITIONAL_PUT) {
                return Err(FileClientError::NotImplemented(format!(
                    "peer has no conditional put for {path}"
                )));
            }
            put_msg.if_match = c.if_match.clone();
            put_msg.if_none_match = c.if_none_match.then_some(true);
        }

        let mut req = Request::new();
        req.msg = Some(file_proto::request::Msg::Put(put_msg));

        let resp = self.single_request(req).await?;

        if resp.status != ProtoResult::RESULT_OK.into() {
            return Err(status_error("PutFile", path, &resp));
        }

        if let Some(file_proto::response::Msg::PutResp(put_resp)) = resp.msg {
            Ok(PutFileResponse { etag: put_resp.etag })
        } else {
            Err(FileClientError::Other("Unexpected response type".to_string()))
        }
    }

    /// Delete a file. Requires a peer advertising `DELETE_REQUEST`.
    pub async fn delete(&self, path: &str) -> Result<(), FileClientError> {
        if !self.supports(SupportedRequestType::DELETE_REQUEST) {
            return Err(FileClientError::NotImplemented(format!(
                "peer has no delete for {path}"
            )));
        }
        let mut del = Delete::new();
        del.path = path.to_string();

        let mut req = Request::new();
        req.msg = Some(file_proto::request::Msg::Delete(del));

        let resp = self.single_request(req).await?;
        if resp.status != ProtoResult::RESULT_OK.into() {
            return Err(status_error("Delete", path, &resp));
        }
        Ok(())
    }

    /// Create a directory
    pub async fn put_dir(&self, path: &str, attrs: Option<&FileAttrs>) -> Result<PutDirResponse, String> {
        let mut put_dir_msg = PutDir::new();
        put_dir_msg.path = path.to_string();
        if let Some(a) = attrs {
            put_dir_msg.attrs = protobuf::MessageField::some(file_proto::FileAttrs::from(a));
        }

        let mut req = Request::new();
        req.msg = Some(file_proto::request::Msg::PutDir(put_dir_msg));

        let resp = self.single_request(req).await?;

        if resp.status != ProtoResult::RESULT_OK.into() {
            return Err(format!("PutDir failed: {:?}", resp.status_string));
        }

        if let Some(file_proto::response::Msg::PutDirResp(dir_resp)) = resp.msg {
            Ok(PutDirResponse { etag: dir_resp.etag })
        } else {
            Err("Unexpected response type".to_string())
        }
    }

    /// Open a file handle
    pub async fn open(&self, path: &str, flags: OpenFlags, mode: OpenMode) -> Result<OpenResponse, String> {
        let mut open_msg = Open::new();
        open_msg.path = path.to_string();
        open_msg.open_flags = protobuf::EnumOrUnknown::new(flags);
        open_msg.open_mode = protobuf::EnumOrUnknown::new(mode);

        let mut req = Request::new();
        req.msg = Some(file_proto::request::Msg::Open(open_msg));

        let resp = self.single_request(req).await?;

        if resp.status != ProtoResult::RESULT_OK.into() {
            return Err(format!("Open failed: {:?}", resp.status_string));
        }

        if let Some(file_proto::response::Msg::OpenResp(open_resp)) = resp.msg {
            Ok(OpenResponse {
                handle: open_resp.handle,
                file_size: open_resp.file_size,
            })
        } else {
            Err("Unexpected response type".to_string())
        }
    }

    /// Close a file handle
    pub async fn close_handle(&self, handle: u32) -> Result<(), String> {
        let mut close_msg = Close::new();
        close_msg.handle = handle;

        let mut req = Request::new();
        req.msg = Some(file_proto::request::Msg::Close(close_msg));

        let resp = self.single_request(req).await?;

        if resp.status != ProtoResult::RESULT_OK.into() {
            return Err(format!("Close failed: {:?}", resp.status_string));
        }

        Ok(())
    }

    /// Read from an open file handle, streaming each block over an mpsc channel.
    /// Dropping the receiver cancels the request (closes the channel / stops the server).
    pub async fn read_streamed(
        &self,
        handle: u32,
        offset: Option<u64>,
        length: Option<u64>,
    ) -> Result<mpsc::Receiver<Result<GetResponse, String>>, String> {
        let mut read_msg = Read::new();
        read_msg.handle = handle;
        if let Some(off) = offset {
            read_msg.offset = Some(off);
        }
        if let Some(len) = length {
            read_msg.length = Some(len);
        }

        let mut req = Request::new();
        req.msg = Some(file_proto::request::Msg::Read(read_msg));

        self.streaming_request(req, "Read", |resp| {
            if let Some(file_proto::response::Msg::ReadResp(read_resp)) = resp.msg {
                Ok(GetResponse {
                    offset: read_resp.offset,
                    bytes: read_resp.bytes,
                    is_last_block: read_resp.is_last_block,
                })
            } else {
                Err("Unexpected response type".to_string())
            }
        })
        .await
    }

    /// Begin listening for file-system changes
    pub async fn list_updates_begin(&self, path: &str) -> Result<ListUpdatesBeginResponse, String> {
        let mut msg = ListUpdatesBegin::new();
        msg.path = path.to_string();

        let mut req = Request::new();
        req.msg = Some(file_proto::request::Msg::ListUpdatesBegin(msg));

        let resp = self.single_request(req).await?;

        if resp.status != ProtoResult::RESULT_OK.into() {
            return Err(format!("ListUpdatesBegin failed: {:?}", resp.status_string));
        }

        if let Some(file_proto::response::Msg::ListUpdatesBeginResp(r)) = resp.msg {
            Ok(ListUpdatesBeginResponse { cursor: r.cursor })
        } else {
            Err("Unexpected response type".to_string())
        }
    }

    /// Poll for file-system changes
    pub async fn list_updates(
        &self,
        cursor: &str,
        timeout_ms: Option<i32>,
        close: Option<bool>,
    ) -> Result<ListUpdatesResponse, String> {
        let mut msg = ListUpdates::new();
        msg.cursor = cursor.to_string();
        msg.timeout_ms = timeout_ms;
        msg.close = close;

        let mut req = Request::new();
        req.msg = Some(file_proto::request::Msg::ListUpdates(msg));

        let resp = self.single_request(req).await?;

        if resp.status != ProtoResult::RESULT_OK.into() {
            return Err(format!("ListUpdates failed: {:?}", resp.status_string));
        }

        if let Some(file_proto::response::Msg::ListUpdatesResp(r)) = resp.msg {
            let changes = r
                .changes
                .into_iter()
                .map(|c| {
                    let item = c.item.into_option().map(DirEntry::from);
                    StorageChange {
                        change_type: c.change_type.value(),
                        item,
                    }
                })
                .collect();
            Ok(ListUpdatesResponse {
                changes,
                cursor: r.cursor,
                has_more: r.has_more,
            })
        } else {
            Err("Unexpected response type".to_string())
        }
    }

    /// Search for files by name
    pub async fn search(
        &self,
        path: &str,
        name_match: Option<&str>,
        recursive: Option<bool>,
        limit: Option<i32>,
        page_token: Option<&str>,
    ) -> Result<SearchResponse, String> {
        let mut msg = Search::new();
        msg.path = path.to_string();
        msg.name_match = name_match.map(|s| s.to_string());
        msg.recursive = recursive;
        msg.limit = limit;
        msg.page_token = page_token.map(|s| s.to_string());

        let mut req = Request::new();
        req.msg = Some(file_proto::request::Msg::Search(msg));

        let resp = self.single_request(req).await?;

        if resp.status != ProtoResult::RESULT_OK.into() {
            return Err(format!("Search failed: {:?}", resp.status_string));
        }

        if let Some(file_proto::response::Msg::SearchResp(r)) = resp.msg {
            let items = r.items.into_iter().map(DirEntry::from).collect();
            Ok(SearchResponse {
                items,
                has_more: r.has_more,
                next_page_token: r.next_page_token,
            })
        } else {
            Err("Unexpected response type".to_string())
        }
    }

    /// Get server capabilities
    async fn get_caps(&self) -> Result<CapsResponse, String> {
        let mut req = Request::new();
        req.msg = Some(file_proto::request::Msg::Caps(Caps {
            client_supported_messages: vec![],
            ..Default::default()
        }));

        let resp = self.single_request(req).await?;

        if let Some(file_proto::response::Msg::CapsResp(caps)) = resp.msg {
            Ok(CapsResponse {
                server_supported_messages: caps.server_supported_messages,
            })
        } else {
            Err("Unexpected response type".to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::p2p::file_server::FileServer;
    use crate::p2p::webrtc_connector::{IceCandidate, SessionDescription};
    use crate::transfers::test_support::{InMemSource, files};
    use tokio::sync::RwLock;

    /// Consecutive `Preview` requests: bounded by path count and by path
    /// bytes, covering every path exactly once, in order.
    #[test]
    fn preview_request_groups_bound_count_and_bytes() {
        assert!(preview_request_groups(&[]).is_empty());

        let many: Vec<String> = (0..70).map(|i| format!("f{i}.png")).collect();
        assert_eq!(preview_request_groups(&many), [0..32, 32..64, 64..70]);

        let long = "p".repeat(20 * 1024);
        let paths = vec![long.clone(), long.clone(), long.clone(), "short".to_string()];
        assert_eq!(preview_request_groups(&paths), [0..2, 2..4], "48 KiB of path bytes per request");

        let huge = vec!["h".repeat(60 * 1024), "next".to_string()];
        assert_eq!(preview_request_groups(&huge), [0..1, 1..2], "an oversized path still gets its own request");
    }

    /// Full in-process P2P stack, no fakes: two real `WebRTCConnector`s
    /// negotiate over cross-wired backend-style signaling (host candidates on
    /// loopback, no STUN), then an encrypted `FileClient` pulls data from a
    /// real `FileServer` — connector, wrapper, client and server all live.
    ///
    /// This is the only coverage of FileClient's request paths (channel
    /// acquire/release/settle, streaming loop) without adding seams.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn loopback_end_to_end_encrypted_transfer() {
        let key = [11u8; 32];
        let data: Vec<u8> = (0..200_000u32).map(|i| (i % 241) as u8).collect();
        let huge = vec![7u8; 8 * 1024 * 1024];
        let mut src = InMemSource::new(files(&[("big.bin", &data), ("huge.bin", &huge), ("small.txt", b"hello")]));
        src.symlinks.insert("link".into(), "small.txt".into());
        src.previews = true;
        let server_fs = Arc::new(FileServer::new(
            Arc::new(RwLock::new(src)) as Arc<RwLock<dyn crate::transfers::StorageSource>>
        ));

        // Cross-wired signaling: what one side emits, the test applies on the other.
        let (offer_tx, mut offer_rx) = mpsc::unbounded_channel::<String>();
        let (answer_tx, mut answer_rx) = mpsc::unbounded_channel::<String>();
        let (c2s_tx, mut c2s_rx) = mpsc::unbounded_channel::<IceCandidate>();
        let (s2c_tx, mut s2c_rx) = mpsc::unbounded_channel::<IceCandidate>();

        let mut server = WebRTCConnector::new();
        server.set_encryption_key(key).await;
        {
            let fs = server_fs.clone();
            server.on_incoming_channel = Some(Arc::new(move |ch| fs.handle_channel(ch)));
        }
        server.on_answer_create = Some(Arc::new(move |sdp| {
            let _ = answer_tx.send(sdp);
        }));
        server.on_candidate_change = Some(Arc::new(move |c| {
            let _ = s2c_tx.send(c);
        }));
        server.connect(false, &[]).await.unwrap();

        let mut client = WebRTCConnector::new();
        client.set_encryption_key(key).await;
        client.on_offer_create = Some(Arc::new(move |sdp| {
            let _ = offer_tx.send(sdp);
        }));
        client.on_candidate_change = Some(Arc::new(move |c| {
            let _ = c2s_tx.send(c);
        }));
        client.connect(true, &[]).await.unwrap();

        // Offer/answer exchange (candidates arriving early queue internally).
        let offer = tokio::time::timeout(std::time::Duration::from_secs(10), offer_rx.recv())
            .await
            .expect("no offer within 10s")
            .unwrap();
        server
            .apply_offer(&SessionDescription {
                sdp_type: "offer".into(),
                sdp: offer,
            })
            .await
            .unwrap();
        let answer = tokio::time::timeout(std::time::Duration::from_secs(10), answer_rx.recv())
            .await
            .expect("no answer within 10s")
            .unwrap();
        client
            .apply_answer(&SessionDescription {
                sdp_type: "answer".into(),
                sdp: answer,
            })
            .await
            .unwrap();

        // Candidate pumps, both directions, for the rest of the test.
        let server = Arc::new(server);
        let client = Arc::new(client);
        tokio::spawn({
            let server = server.clone();
            async move {
                while let Some(c) = c2s_rx.recv().await {
                    let _ = server.add_candidate(&c).await;
                }
            }
        });
        tokio::spawn({
            let client = client.clone();
            async move {
                while let Some(c) = s2c_rx.recv().await {
                    let _ = client.add_candidate(&c).await;
                }
            }
        });

        tokio::time::timeout(std::time::Duration::from_secs(15), async {
            while !(client.is_connected().await && server.is_connected().await) {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("peers never connected over loopback");

        let mut fc = FileClient::new(client.clone());
        fc.connect().await.unwrap();
        assert!(fc.supports(SupportedRequestType::GET_REQUEST), "caps exchanged");

        // Attrs (single-request path) — incl. a symlink's target.
        let attrs = fc.get_attrs("big.bin").await.unwrap();
        let entry = attrs.dir_entry.unwrap();
        assert_eq!(entry.file_size, Some(data.len() as u64));
        let link = fc.get_attrs("link").await.unwrap().dir_entry.unwrap();
        assert_eq!(link.symlink_target.as_deref(), Some("small.txt"));

        // Streamed get (multi-block: 200KB over the ~64K SCTP message limit).
        let mut rx = fc.get_streamed("big.bin", None, None).await.unwrap();
        let mut got: Vec<u8> = Vec::new();
        let mut blocks = 0;
        while let Some(item) = rx.recv().await {
            let block = item.unwrap();
            assert_eq!(block.offset, got.len() as u64);
            got.extend_from_slice(&block.bytes);
            blocks += 1;
            if block.is_last_block {
                break;
            }
        }
        assert!(blocks > 1, "200KB must stream in several blocks (got {blocks})");
        assert_eq!(got, data, "byte-exact through encryption and reassembly");

        // Windowed get.
        let mut rx = fc.get_streamed("small.txt", Some(1), Some(3)).await.unwrap();
        let block = rx.recv().await.unwrap().unwrap();
        assert_eq!(block.bytes, b"ell");

        // Directory listing (streaming path).
        let mut rx = fc.get_dir_streamed("", true, 1000, None, None).await.unwrap();
        let mut names: Vec<String> = Vec::new();
        while let Some(item) = rx.recv().await {
            let resp = item.unwrap();
            names.extend(resp.entries.iter().map(|e| e.name.clone()));
            if resp.is_last_block {
                break;
            }
        }
        names.sort();
        assert_eq!(names, ["big.bin", "huge.bin", "link", "small.txt"]);

        // Previews (streaming path): chunks reassemble per index, a failure
        // keeps its status, the bodyless closing block ends the stream.
        assert!(fc.supports(SupportedRequestType::PREVIEW_REQUEST), "the source serves previews");
        let mut rx = fc
            .get_previews(vec!["big.bin".into(), "small.txt".into(), "missing".into()], 128)
            .await
            .unwrap();
        let mut images: std::collections::HashMap<usize, Vec<u8>> = std::collections::HashMap::new();
        let mut failed = Vec::new();
        let mut ended = false;
        while let Some(item) = rx.recv().await {
            match item.unwrap() {
                PreviewBlock::Chunk { index, bytes, .. } => images.entry(index).or_default().extend_from_slice(&bytes),
                PreviewBlock::Failure { index, status } => failed.push((index, status)),
                PreviewBlock::End => ended = true,
            }
        }
        assert!(ended, "the stream ends with the closing block");
        assert_eq!(images[&1], b"hello");
        // The fake answers in reverse order and has no preview for `*.bin`.
        assert_eq!(
            failed,
            [(2, ProtoResult::RESULT_NO_SUCH_FILE), (0, ProtoResult::RESULT_UNSUPPORTED)]
        );

        // A missing key comes back as the typed NoSuchFile error.
        let err = fc.get_attrs("missing").await.unwrap_err();
        assert!(matches!(err, FileClientError::NoSuchFile(_)), "got {err:?}");

        // Cancel mid-stream: dropping the receiver closes the channel under megabytes the
        // server still has queued for it. The SCTP stream reset must take only that
        // channel down — the association, and every later request on it, lives on.
        for round in 0..3 {
            let mut rx = fc.get_streamed("huge.bin", None, None).await.unwrap();
            let first = rx.recv().await.unwrap().unwrap();
            assert!(!first.is_last_block, "round {round}: the cancel must land mid-stream");
            drop(rx);

            let mut rx = fc.get_streamed("big.bin", None, None).await.unwrap();
            let mut got: Vec<u8> = Vec::new();
            while let Some(item) = rx.recv().await {
                let block = item.unwrap();
                got.extend_from_slice(&block.bytes);
                if block.is_last_block {
                    break;
                }
            }
            assert_eq!(got, data, "round {round}: a get after a cancelled one");
        }
        assert!(client.is_connected().await && server.is_connected().await);

        client.close().await;
        server.close().await;
    }
}
