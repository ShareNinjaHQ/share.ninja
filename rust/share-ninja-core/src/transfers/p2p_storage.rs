//! P2P Storage implementation using WebRTC

use async_trait::async_trait;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::sync::{RwLock, mpsc};

use crate::back::{Back, CallOutInfo};
use crate::p2p::{FileClient, P2PDirEntry, PreviewBlock, WebRTCConnector, derive_session_key, preview_request_groups};
use crate::utils::result::Result as SharonResult;

use super::{
    PreviewImage, SearchOptions, SearchResult, Storage, StorageChange, StorageDest, StorageError, StorageFileAttrs,
    StorageMetaState, StorageObjectInfo, StoragePutConditions, StoragePutResult, StorageSearchQuery, StorageSource,
    StorageSourceType, StorageStatInfo,
};
use share_ninja_proto::file_proto::{Result as ProtoResult, SupportedRequestType};

/// A peer's per-item preview failure back as the error the source raised
/// (the inverse of `FileServer::status_for`).
fn preview_error(status: ProtoResult) -> StorageError {
    match status {
        ProtoResult::RESULT_UNSUPPORTED => StorageError::NoPreview("peer".into()),
        ProtoResult::RESULT_NO_SUCH_FILE => StorageError::NoSuchKey("peer".into()),
        other => StorageError::Other(format!("preview failed on the peer: {other:?}")),
    }
}

/// P2P Storage for WebRTC-based file transfers
pub struct P2PStorage {
    back: Arc<RwLock<Back>>,
    call_info: Option<CallOutInfo>,
    target_agent: String,
    transfer_id: String,
    total_sent: u64,
    total_recv: Arc<AtomicU64>,
    error: Option<StorageError>,
    data_source_id: Option<String>,
    is_connected: AtomicBool,
    wrtc: Option<Arc<WebRTCConnector>>,
    /// Connected before it is shared: every request method is `&self`, so
    /// requests run concurrently — a mutex here serialized them on channel
    /// open (a listing waited for a bulk get's channel handshake).
    file_client: Option<Arc<FileClient>>,
    encryption_key: Option<[u8; 32]>,
    /// Callback invoked when the P2P connection is lost
    on_disconnect: Option<Arc<dyn Fn() + Send + Sync>>,
    /// Bumped whenever the current connector is dropped (`cleanup`). A
    /// connector's `on_close` captures the generation it was dialed under
    /// and reports only while it is still the current one: a late close
    /// from a replaced connector, and our own close, never mark the new
    /// connection dead (`WebRTCConnector::close` invokes `on_close` itself).
    generation: Arc<AtomicU64>,
}

impl P2PStorage {
    /// A reopened backend session (docs/app/reconnect.md): dial the peer
    /// again under the new `transfer_id`. The old WebRTC connector is dropped
    /// first — the session it was called under is gone.
    pub async fn reconnect(&mut self, transfer_id: String) -> Result<(), StorageError> {
        self.transfer_id = transfer_id;
        // `connect` drops the previous connector before dialing.
        self.connect().await.map(|_| ())
    }

    pub fn new(
        back: Arc<RwLock<Back>>,
        target_agent: String,
        transfer_id: String,
        encryption_key: Option<[u8; 32]>,
    ) -> Self {
        Self {
            back,
            call_info: None,
            target_agent,
            transfer_id,
            total_sent: 0,
            total_recv: Arc::new(AtomicU64::new(0)),
            error: None,
            data_source_id: None,
            is_connected: AtomicBool::new(false),
            wrtc: None,
            file_client: None,
            encryption_key,
            on_disconnect: None,
            generation: Arc::new(AtomicU64::new(0)),
        }
    }
}

#[async_trait]
impl Storage for P2PStorage {
    fn set_on_disconnect(&mut self, callback: std::sync::Arc<dyn Fn() + Send + Sync>) {
        self.on_disconnect = Some(callback);
    }

    fn cleanup(&mut self) {
        // Drop the current connector: forget it first and move the
        // generation on, so the close it fires on its way out is a stale
        // one, then close it off the lock.
        self.generation.fetch_add(1, Ordering::Relaxed);
        self.is_connected.store(false, Ordering::Relaxed);
        self.file_client = None;
        if let Some(wrtc) = self.wrtc.take() {
            tokio::spawn(async move {
                wrtc.close().await;
            });
        }
    }

    async fn connect(&mut self) -> Result<StorageMetaState, StorageError> {
        tracing::debug!("P2PStorage connect to agent {}", self.target_agent);

        // The one place a connector is replaced closes the previous one: a
        // redial after a drop, a session reopen. Without this every redial
        // left the old peer connection and its tasks behind.
        if self.wrtc.is_some() {
            self.cleanup();
        }

        // The call goes through the backend: wait for it to be there rather
        // than spend a reconnect attempt on a fail-fast `Offline`
        // (docs/app/reconnect.md). The wait ends with the task that runs this
        // connect — MultiSource aborts its reconnect timer on cancel/cleanup.
        let mut state = self.back.read().await.state_watch();
        let _ = state.wait_for(|s| s.is_online()).await;

        // Get call info from backend
        let back = self.back.read().await;
        let call_result = back.call(&self.target_agent, &self.transfer_id).await;
        drop(back);

        let call_info = match call_result {
            Ok((result, call_info)) => {
                if result != SharonResult::OK {
                    return Err(StorageError::P2P(format!(
                        "Failed to call {}: {:?}",
                        self.target_agent, result
                    )));
                }
                call_info.ok_or_else(|| StorageError::P2P("No call info returned".into()))?
            }
            Err(e) if e.is_session_expired() => return Err(StorageError::SessionExpired(e.to_string())),
            Err(e) if e.is_transport() => return Err(StorageError::Transport(e.to_string())),
            Err(e) => return Err(StorageError::P2P(e.to_string())),
        };

        tracing::info!(
            "Starting P2P call to {}, call_id: {}",
            self.target_agent,
            call_info.call_id
        );
        self.call_info = Some(call_info.clone());

        // Create WebRTC connector with relay if available
        let relay = call_info.relay.as_ref().map(|r| crate::p2p::RelayConfig {
            send_url: r.send_url.clone(),
            receive_url: r.receive_url.clone(),
        });
        let has_relay = relay.is_some();
        let mut wrtc = WebRTCConnector::with_relay(relay);

        // Set up encryption if key provided
        if let Some(master_key) = self.encryption_key {
            tracing::debug!("Setting up P2P encryption (client side)");
            let session_key = derive_session_key(&master_key, &call_info.call_id)
                .map_err(|e| StorageError::P2P(format!("Failed to derive session key: {}", e)))?;
            wrtc.set_encryption_key(session_key).await;
        }

        wrtc.on_error = Some(Arc::new(move |msg: String| {
            tracing::error!("WebRTC error: {}", msg);
        }));

        let on_disconnect = self.on_disconnect.clone();
        let generation = self.generation.clone();
        let dialed_under = generation.load(Ordering::Relaxed);
        wrtc.on_close = Some(Arc::new(move || {
            if generation.load(Ordering::Relaxed) != dialed_under {
                tracing::debug!("close event from a replaced P2P connection, ignored");
                return;
            }
            tracing::debug!("P2P data channel closed");
            if let Some(ref cb) = on_disconnect {
                cb();
            }
        }));

        tracing::debug!("Connecting as caller (client), has_relay: {}", has_relay);
        wrtc.connect_as_caller(&call_info.ice_servers)
            .await
            .map_err(StorageError::P2P)?;

        let wrtc = Arc::new(wrtc);
        self.wrtc = Some(wrtc.clone());

        // Create FileClient with the connector; it is shared only once
        // connected (connect is the one `&mut` call).
        let mut file_client = FileClient::new(wrtc.clone());

        tracing::debug!("P2P relay connected, initing FileClient");
        // Initialize FileClient with overall connection timeout
        const P2P_CONNECT_TIMEOUT_SECS: u64 = 15;
        let connect_result =
            tokio::time::timeout(std::time::Duration::from_secs(P2P_CONNECT_TIMEOUT_SECS), file_client.connect()).await;

        match connect_result {
            Ok(Ok(())) => {
                self.file_client = Some(Arc::new(file_client));
                self.is_connected.store(true, Ordering::Relaxed);
                tracing::info!("P2P connection established");
                Ok(StorageMetaState::Complete)
            }
            Ok(Err(e)) => Err(StorageError::P2P(e)),
            Err(_) => Err(StorageError::P2P(format!(
                "P2P connection timeout ({}s)",
                P2P_CONNECT_TIMEOUT_SECS
            ))),
        }
    }

    async fn cancel(&self) -> Result<(), StorageError> {
        tracing::debug!("Cancel P2P");
        self.is_connected.store(false, Ordering::Relaxed);
        Ok(())
    }

    async fn update_meta(&self) -> Result<Vec<StorageChange>, StorageError> {
        Ok(Vec::new())
    }

    fn total_bytes_sent(&self) -> u64 {
        self.total_sent
    }

    fn total_bytes_recv(&self) -> u64 {
        self.total_recv.load(Ordering::Relaxed)
    }

    fn data_source_id(&self) -> Option<&str> {
        self.data_source_id.as_deref()
    }

    fn set_data_source_id(&mut self, id: String) {
        self.data_source_id = Some(id);
    }

    async fn get_dir_stats_streamed(
        &self,
        path: &str,
    ) -> mpsc::Receiver<Result<super::StorageDirStats, StorageError>> {
        let (tx, rx) = mpsc::channel(4);

        let fail = |e: StorageError| {
            let tx = tx.clone();
            async move {
                let _ = tx.send(Err(e)).await;
            }
        };
        if let Some(ref err) = self.error {
            fail(StorageError::Other(err.to_string())).await;
            return rx;
        }
        if !self.is_connected.load(Ordering::Relaxed) {
            fail(StorageError::P2P("Not connected".into())).await;
            return rx;
        }
        let file_client = match self.file_client.as_ref() {
            Some(fc) => fc.clone(),
            None => {
                fail(StorageError::P2P("Not connected".into())).await;
                return rx;
            }
        };

        let mut inner_rx = {
            let fc = file_client.as_ref();
            match fc.dir_stats_streamed(path).await {
                Ok(r) => r,
                Err(e) => {
                    fail(StorageError::P2P(e)).await;
                    return rx;
                }
            }
        };

        tokio::spawn(async move {
            while let Some(block) = inner_rx.recv().await {
                let mapped = block
                    .map(|resp| super::StorageDirStats {
                        number_of_files: resp.number_of_files,
                        number_of_folders: resp.number_of_folders,
                        number_of_symlinks: resp.number_of_symlinks,
                        total_size: resp.total_size,
                    })
                    .map_err(StorageError::P2P);
                let failed = mapped.is_err();
                if tx.send(mapped).await.is_err() || failed {
                    break; // consumer dropped (cancels the request) or stream errored
                }
            }
        });
        rx
    }

    /// Stream the listing live from the peer (cursor pagination, path scope +
    /// recursive pushed down). Mirrors the post-refactor TS `P2PStorage.getDir`
    /// which always hits the peer — no client-side meta cache.
    async fn get_dir(
        &self,
        path: &str,
        recursive: bool,
        start_after: Option<&str>,
        max_batch_size: Option<usize>,
    ) -> mpsc::Receiver<Result<Vec<StorageObjectInfo>, StorageError>> {
        if let Some(ref err) = self.error {
            let (tx, rx) = mpsc::channel(1);
            let _ = tx.send(Err(StorageError::Other(err.to_string()))).await;
            return rx;
        }
        self.get_dir_int(path, recursive, start_after, max_batch_size)
    }

    /// Single-stat: always ask the peer directly. Mirrors the post-refactor TS
    /// `P2PStorage.getAttrs` (no client-side meta cache).
    async fn get_attrs(&self, path: &str) -> Result<StorageObjectInfo, StorageError> {
        if let Some(ref err) = self.error {
            return Err(StorageError::Other(err.to_string()));
        }
        let file_client = self
            .file_client
            .as_ref()
            .ok_or_else(|| StorageError::P2P("Not connected".into()))?;
        let resp = {
            let fc = file_client.as_ref();
            fc.get_attrs(path).await?
        };
        match resp.dir_entry {
            Some(entry) => Ok(entry_to_soi(&entry)),
            None => Err(StorageError::NoSuchKey(path.to_string())),
        }
    }

    /// Push the search down to the peer (SEARCH wire message) — the sender's
    /// source does the walk, we just map the wire entries back. The peer
    /// time-boxes each request and may answer with a partial (possibly empty)
    /// page plus a resume token, so page here until the limit is met or the
    /// walk ends: every round trip stays well inside the per-message receive
    /// timeout no matter how long the peer's total walk takes. Callers see a
    /// single assembled `SearchResult`; the paging never leaks upward.
    async fn search(&self, query: &StorageSearchQuery, options: &SearchOptions) -> Result<SearchResult, StorageError> {
        if let Some(ref err) = self.error {
            return Err(StorageError::Other(err.to_string()));
        }
        let file_client = self
            .file_client
            .as_ref()
            .ok_or_else(|| StorageError::P2P("Not connected".into()))?;

        // One peer page per call, the peer's token passed through: the
        // peer's walk is parked under it, and the caller pages at its pace.
        let resp = {
            let fc = file_client.as_ref();
            fc.search(
                options.path_scope.as_deref().unwrap_or(""),
                query.name_match.as_deref(),
                options.recursive,
                options.page_limit,
                options.page_token.as_deref(),
            )
            .await
            .map_err(StorageError::P2P)?
        };
        Ok(SearchResult {
            items: resp.items.iter().map(entry_to_soi).collect(),
            next_page_token: resp.next_page_token,
        })
    }
}

/// Convert a P2P `DirEntry` into a `StorageObjectInfo`.
fn entry_to_soi(entry: &P2PDirEntry) -> StorageObjectInfo {
    let mut soi = StorageObjectInfo::new(entry.name.clone());
    soi.file_type = entry.to_storage_file_type();
    if let Some(size) = entry.file_size {
        soi.size = size;
    }
    if let Some(ref attrs) = entry.attrs {
        if let Some(mtime) = attrs.modification_date {
            soi.attrs.mtime = Some(mtime);
        }
        soi.attrs.posix_permissions = attrs.posix_permissions;
        soi.attrs.email_subject = attrs.email_subject.clone();
        soi.attrs.gdrive_file_id = attrs.gdrive_file_id.clone();
        soi.attrs.gdrive_mime_type = attrs.gdrive_mime_type.clone();
    }
    soi.etag = entry.etag.clone();
    soi
}

impl P2PStorage {
    /// Stream a directory listing straight from the peer with cursor pagination —
    /// the `getDirInt` analog. Lazy: pages are fetched and converted as the
    /// receiver is consumed, never buffering the whole tree. Drives `get_dir`.
    /// Dropping the receiver cancels.
    fn get_dir_int(
        &self,
        path: &str,
        recursive: bool,
        start_after: Option<&str>,
        max_batch_size: Option<usize>,
    ) -> mpsc::Receiver<Result<Vec<StorageObjectInfo>, StorageError>> {
        let file_client = self.file_client.clone();
        let path = path.to_string();
        let max_b = max_batch_size.filter(|n| *n > 0).unwrap_or(1000);
        let (tx, rx) = mpsc::channel(4);
        let start_after = start_after.map(|s| s.to_string());
        tokio::spawn(async move {
            let Some(file_client) = file_client else {
                let _ = tx.send(Err(StorageError::P2P("Not connected".into()))).await;
                return;
            };
            if start_after.is_some()
                && !file_client.supports(SupportedRequestType::GETDIR_START_AFTER)
            {
                // An older peer would ignore the field and list from the
                // beginning; a cursor that is silently a no-op is worse than none.
                let _ = tx
                    .send(Err(StorageError::NotImplemented(
                        "peer has no getDir start_after".into(),
                    )))
                    .await;
                return;
            }
            let mut batch: Vec<StorageObjectInfo> = Vec::new();
            let mut cursor: Option<String> = None;
            loop {
                let page_result = {
                    let fc = file_client.as_ref();
                    // start_after seeds the first request only; later pages follow the cursor.
                    let after = if cursor.is_none() { start_after.as_deref() } else { None };
                    fc.get_dir_streamed(&path, recursive, max_b as i32, cursor.as_deref(), after)
                        .await
                }; // drop the FileClient guard before handling the result
                let mut page_rx = match page_result {
                    Ok(page_rx) => page_rx,
                    Err(e) => {
                        let _ = tx.send(Err(StorageError::P2P(e))).await;
                        return;
                    }
                };

                let mut is_last_page_block = false;
                while let Some(item) = page_rx.recv().await {
                    let resp = match item {
                        Ok(resp) => resp,
                        Err(e) => {
                            let _ = tx.send(Err(StorageError::P2P(e))).await;
                            return;
                        }
                    };
                    for entry in &resp.entries {
                        batch.push(entry_to_soi(entry));
                        if batch.len() >= max_b && tx.send(Ok(std::mem::take(&mut batch))).await.is_err() {
                            return; // receiver dropped — cancel
                        }
                    }
                    cursor = resp.cursor;
                    if resp.is_last_block {
                        is_last_page_block = true;
                    }
                }

                if is_last_page_block && cursor.is_none() {
                    break;
                }
            }
            if !batch.is_empty() {
                let _ = tx.send(Ok(batch)).await;
            }
        });
        rx
    }
}

#[async_trait]
impl StorageSource for P2PStorage {
    fn source_name(&self) -> &str {
        "P2P"
    }

    fn source_type(&self) -> StorageSourceType {
        StorageSourceType::P2P
    }

    async fn get_streamed(
        &self,
        object: &str,
        offset: Option<u64>,
        length: Option<u64>,
    ) -> mpsc::Receiver<Result<Vec<u8>, StorageError>> {
        let (tx, rx) = mpsc::channel(4);

        // Early-out paths deliver a single Err item, mirroring get().
        if let Some(ref err) = self.error {
            let _ = tx.send(Err(StorageError::Other(err.to_string()))).await;
            return rx;
        }
        if !self.is_connected.load(Ordering::Relaxed) {
            let _ = tx.send(Err(StorageError::P2P("Not connected".into()))).await;
            return rx;
        }
        let file_client = match self.file_client.as_ref() {
            Some(fc) => fc.clone(),
            None => {
                let _ = tx.send(Err(StorageError::P2P("Not connected".into()))).await;
                return rx;
            }
        };

        let key = object.to_string();
        tracing::debug!("P2P get_streamed {} offset {:?} len {:?}", key, offset, length);

        // Kick off the streaming request (acquires a channel, sends, spawns the pump).
        let mut inner_rx = {
            let fc = file_client.as_ref();
            match fc.get_streamed(&key, offset, length).await {
                Ok(r) => r,
                Err(e) => {
                    let _ = tx.send(Err(StorageError::P2P(e))).await;
                    return rx;
                }
            }
        };

        // Adapter: forward each block's bytes, mapping errors. Dropping the consumer's
        // receiver makes our send fail and we drop inner_rx; FileClient's pump then
        // drains the rest of the response and pools the channel (never a mid-stream
        // close — see `FileClient::streaming_request`).
        let total_recv = self.total_recv.clone();
        tokio::spawn(async move {
            while let Some(item) = inner_rx.recv().await {
                match item {
                    Ok(resp) => {
                        total_recv.fetch_add(resp.bytes.len() as u64, Ordering::Relaxed);
                        if tx.send(Ok(resp.bytes)).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(StorageError::P2P(e))).await;
                        break;
                    }
                }
            }
        });

        rx
    }

    /// Thumbnails from the peer (docs/core/previews.md). `objects` of any
    /// length go out as consecutive wire requests (`preview_request_groups`);
    /// once the consumer is gone the running one drains and no further one is
    /// sent. A wire that dies mid-way just ends the stream — the indices never
    /// answered are the caller's failures.
    async fn get_previews(
        &self,
        objects: Vec<String>,
        px: u32,
    ) -> Result<mpsc::Receiver<(usize, Result<PreviewImage, StorageError>)>, StorageError> {
        if let Some(ref err) = self.error {
            return Err(StorageError::Other(err.to_string()));
        }
        if !self.is_connected.load(Ordering::Relaxed) {
            return Err(StorageError::P2P("Not connected".into()));
        }
        let file_client = self
            .file_client
            .as_ref()
            .ok_or_else(|| StorageError::P2P("Not connected".into()))?
            .clone();
        if !file_client.supports(SupportedRequestType::PREVIEW_REQUEST) {
            return Err(StorageError::NotImplemented("the peer serves no previews".into()));
        }

        let (tx, rx) = mpsc::channel(4);
        tokio::spawn(async move {
            for group in preview_request_groups(&objects) {
                if tx.is_closed() {
                    return;
                }
                let paths = objects[group.clone()].to_vec();
                let blocks = {
                    let fc = file_client.as_ref();
                    fc.get_previews(paths, px).await
                };
                let mut blocks = match blocks {
                    Ok(blocks) => blocks,
                    Err(e) => {
                        tracing::debug!("P2P get_previews: request failed: {e}");
                        return;
                    }
                };
                // The item being assembled: its chunks are contiguous.
                let mut item: Option<(usize, PreviewImage)> = None;
                while let Some(block) = blocks.recv().await {
                    let answer = match block {
                        Ok(PreviewBlock::Chunk {
                            index,
                            bytes,
                            mime_type,
                            etag,
                            item_done,
                        }) => {
                            match &mut item {
                                Some((current, image)) if *current == index => image.bytes.extend_from_slice(&bytes),
                                _ => {
                                    item = Some((
                                        index,
                                        PreviewImage {
                                            bytes,
                                            mime_type: mime_type.unwrap_or_default(),
                                            etag,
                                        },
                                    ))
                                }
                            }
                            if !item_done {
                                continue;
                            }
                            let (index, image) = item.take().expect("set above");
                            (index, Ok(image))
                        }
                        Ok(PreviewBlock::Failure { index, status }) => (index, Err(preview_error(status))),
                        Ok(PreviewBlock::End) => break,
                        Err(e) => {
                            tracing::debug!("P2P get_previews: stream broke: {e}");
                            return;
                        }
                    };
                    let (index, result) = answer;
                    if index >= group.len() {
                        tracing::debug!("P2P get_previews: index {index} outside the request");
                        continue;
                    }
                    if tx.send((group.start + index, result)).await.is_err() {
                        return;
                    }
                }
            }
        });
        Ok(rx)
    }

    async fn get_symlink(&self, object: &str) -> Result<String, StorageError> {
        if let Some(ref err) = self.error {
            return Err(StorageError::Other(err.to_string()));
        }

        if !self.is_connected.load(Ordering::Relaxed) {
            return Err(StorageError::P2P("Not connected".into()));
        }

        let file_client = self
            .file_client
            .as_ref()
            .ok_or_else(|| StorageError::P2P("Not connected".into()))?;

        tracing::debug!("P2P getSymlink {}", object);

        // Use getAttrs to fetch symlink target
        let fc = file_client.as_ref();
        let resp = fc.get_attrs(object).await?;

        if let Some(entry) = resp.dir_entry
            && let Some(target) = entry.symlink_target
        {
            return Ok(target);
        }

        Err(StorageError::NoSuchKey(format!("Symlink not found: {}", object)))
    }
}

fn wire_attrs(a: &StorageFileAttrs) -> crate::p2p::P2PFileAttrs {
    crate::p2p::P2PFileAttrs {
        creation_date: None,
        modification_date: a.mtime,
        posix_permissions: a.posix_permissions,
        email_subject: a.email_subject.clone(),
        gdrive_file_id: a.gdrive_file_id.clone(),
        gdrive_mime_type: a.gdrive_mime_type.clone(),
    }
}

/// Write side: the peer's `FileServer` with a dest — a team's agent serving a
/// NinjaStorageV3 root (`FsObjectStore`). Conditional puts and deletes are
/// refused locally when the peer's `Caps` did not advertise them, so an old
/// agent can never be mistaken for an object store.
#[async_trait]
impl StorageDest for P2PStorage {
    fn is_sequential(&self) -> bool {
        false
    }

    async fn put_file(
        &self,
        object: &str,
        data: &[u8],
        attrs: Option<&StorageFileAttrs>,
        conditions: Option<&StoragePutConditions>,
    ) -> Result<StoragePutResult, StorageError> {
        if let Some(ref err) = self.error {
            return Err(StorageError::Other(err.to_string()));
        }
        let file_client = self
            .file_client
            .as_ref()
            .ok_or_else(|| StorageError::P2P("Not connected".into()))?;
        let wire_attrs = attrs.map(wire_attrs);
        let resp = {
            let fc = file_client.as_ref();
            fc.put_file(object, data, wire_attrs.as_ref(), conditions).await?
        };
        Ok(StoragePutResult {
            etag: Some(resp.etag).filter(|e| !e.is_empty()),
            crc32: None,
            sha256: None,
            storage_key: object.to_string(),
        })
    }

    async fn put_dir(&self, object: &str, attrs: Option<&StorageFileAttrs>) -> Result<StoragePutResult, StorageError> {
        let file_client = self
            .file_client
            .as_ref()
            .ok_or_else(|| StorageError::P2P("Not connected".into()))?;
        let wire_attrs = attrs.map(wire_attrs);
        {
            let fc = file_client.as_ref();
            fc.put_dir(object, wire_attrs.as_ref())
                .await
                .map_err(StorageError::P2P)?;
        }
        Ok(StoragePutResult {
            etag: None,
            crc32: None,
            sha256: None,
            storage_key: object.to_string(),
        })
    }

    async fn put_symlink(
        &self,
        object: &str,
        _target: &str,
        _attrs: Option<&StorageFileAttrs>,
    ) -> Result<StoragePutResult, StorageError> {
        Err(StorageError::NotImplemented(format!("P2P put_symlink {object}")))
    }

    async fn del_file(&self, object: &str) -> Result<(), StorageError> {
        let file_client = self
            .file_client
            .as_ref()
            .ok_or_else(|| StorageError::P2P("Not connected".into()))?;
        let fc = file_client.as_ref();
        Ok(fc.delete(object).await?)
    }

    async fn del_dir(&self, object: &str) -> Result<(), StorageError> {
        Err(StorageError::NotImplemented(format!("P2P del_dir {object}")))
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
    use crate::back::BackParams;
    use crate::p2p::P2PFileAttrs;
    use chrono::TimeZone;
    use share_ninja_proto::file_proto::FileType;

    fn entry(name: &str, file_type: FileType) -> P2PDirEntry {
        P2PDirEntry {
            name: name.to_string(),
            file_type,
            file_size: None,
            symlink_target: None,
            symlink_target_is_dir: None,
            attrs: None,
            number_of_files: None,
            number_of_folders: None,
            is_read_only: None,
            share_name: None,
            etag: None,
        }
    }

    #[test]
    fn entry_to_soi_maps_fields_and_syncs_mtime() {
        let mtime = chrono::Utc.with_ymd_and_hms(2026, 3, 1, 12, 0, 0).unwrap();
        let mut e = entry("d/f.txt", FileType::FILE_TYPE_REGULAR);
        e.file_size = Some(42);
        e.etag = Some("e1".into());
        e.attrs = Some(P2PFileAttrs {
            creation_date: None,
            modification_date: Some(mtime),
            posix_permissions: Some(0o644),
            email_subject: Some("subj".into()),
            gdrive_file_id: None,
            gdrive_mime_type: None,
        });

        let soi = entry_to_soi(&e);
        assert_eq!(soi.key, "d/f.txt");
        assert_eq!(soi.size, 42);
        assert_eq!(soi.file_type, crate::transfers::StorageFileType::Regular);
        assert_eq!(soi.etag.as_deref(), Some("e1"));
        assert_eq!(soi.attrs.mtime, Some(mtime));
        assert_eq!(soi.attrs.posix_permissions, Some(0o644));
        assert_eq!(soi.attrs.email_subject.as_deref(), Some("subj"));
    }

    #[test]
    fn entry_to_soi_maps_file_types() {
        assert_eq!(
            entry_to_soi(&entry("d", FileType::FILE_TYPE_DIRECTORY)).file_type,
            crate::transfers::StorageFileType::Dir
        );
        assert_eq!(
            entry_to_soi(&entry("l", FileType::FILE_TYPE_SYMBOLIC_LINK)).file_type,
            crate::transfers::StorageFileType::Symlink
        );
    }

    #[test]
    fn wire_attrs_maps_mtime_and_metadata() {
        let mtime = chrono::Utc.with_ymd_and_hms(2026, 3, 1, 12, 0, 0).unwrap();
        let a = StorageFileAttrs {
            mtime: Some(mtime),
            posix_permissions: Some(0o600),
            email_subject: None,
            gdrive_file_id: Some("gid".into()),
            gdrive_mime_type: None,
        };
        let w = wire_attrs(&a);
        assert_eq!(w.modification_date, Some(mtime));
        assert_eq!(w.creation_date, None);
        assert_eq!(w.posix_permissions, Some(0o600));
        assert_eq!(w.gdrive_file_id.as_deref(), Some("gid"));
    }

    fn dummy_back() -> Arc<RwLock<Back>> {
        // `Back::new` builds a reqwest client eagerly; with `rustls-no-provider`
        // that needs the process-default provider the binaries install.
        rustls::crypto::ring::default_provider().install_default().ok();
        let params = BackParams {
            base_url: "http://localhost:1".into(),
            ws_url: "ws://localhost:1".into(),
            oauth_url: None,
            user_agent: "test".into(),
            device_name: None,
            product_id: "test".into(),
            product_version: "0".into(),
            os: "test".into(),
            os_version: "0".into(),
            lang: "en".into(),
            response_mode: "token".into(),
            device_public_key: Vec::new(),
            device_serial: None,
            device_model: None,
            device_form_factor: None,
            log_api: false,
            nearby_visibility: None,
            instance_id: None,
            connect_override: None,
        };
        let secrets = Arc::new(crate::secrets::SecureStore::new(Arc::new(
            crate::secrets::MemorySecretStore::default(),
        )));
        Arc::new(RwLock::new(Back::new(params, secrets)))
    }

    /// Every operation on a storage that never connected fails loud with a
    /// P2P "Not connected" error instead of hanging or panicking.
    #[tokio::test]
    async fn operations_before_connect_fail_loud() {
        let ps = P2PStorage::new(dummy_back(), "agent".into(), "t1".into(), None);
        assert_eq!(ps.source_name(), "P2P");
        assert_eq!(ps.source_type(), StorageSourceType::P2P);

        let err = ps.get_attrs("f.txt").await.unwrap_err();
        assert!(matches!(err, StorageError::P2P(_)), "got {err:?}");

        let mut rx = ps.get_streamed("f.txt", None, None).await;
        assert!(matches!(rx.recv().await, Some(Err(StorageError::P2P(_)))));

        let err = ps.get_symlink("l").await.unwrap_err();
        assert!(matches!(err, StorageError::P2P(_)), "got {err:?}");

        let err = ps.put_file("f.txt", b"x", None, None).await.unwrap_err();
        assert!(matches!(err, StorageError::P2P(_)), "got {err:?}");

        let mut rx = ps.get_dir("", true, None, None).await;
        assert!(matches!(rx.recv().await, Some(Err(StorageError::P2P(_)))));

        let mut rx = ps.get_dir_stats_streamed("").await;
        assert!(matches!(rx.recv().await, Some(Err(StorageError::P2P(_)))));
    }
}
