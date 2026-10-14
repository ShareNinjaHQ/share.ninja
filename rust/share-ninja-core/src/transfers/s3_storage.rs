//! S3 storage implementation
//!
//! Ports the TypeScript `S3Storage` (sharon-lib
//! `transfers/S3Storage.ts`). An S3 storage that never holds S3
//! credentials: every byte operation is a plain HTTP request against a URL the
//! backend presigned for one object (or one multipart part). Reads GET the
//! presigned URL (streamed); writes PUT it. Listing/attrs come from the backend's
//! live server-side ListObjectsV2 (`list_objects`, paginated by cursor). A
//! presigned GET URL signs the whole object (Range is a plain header), so one
//! mint serves every later read of that object — a media preview asks for
//! hundreds of ranges of one blob: GET URLs are kept in `presigned_files` (the
//! download manifest the backend seeds at start; a miss is minted once and
//! joins it), and a kept URL that has aged out (403) is dropped and re-minted
//! once, transparently, so a warm source that outlived the signatures keeps
//! serving. PUT URLs are single-shot: minted per operation, never kept. A
//! fresh mint failing with `transfer_not_found` surfaces as
//! `StorageError::SessionExpired` for the app-layer reopen.
//!
//! Object keys ("object" args) are relative to the link prefix; the backend
//! prepends the prefix when it signs, so keys are passed through unchanged.

use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::StreamExt;
use http_body::{Frame, SizeHint};
use tokio::sync::{RwLock, mpsc};

use crate::back::{Back, MultipartCompletedPart, PresignRequest, PresignedFile, error_chain};

use super::{
    Storage, StorageDest, StorageDestMultipart, StorageError, StorageFileAttrs, StorageMetaState,
    StorageMultipartUpload, StorageObjectInfo, StoragePartInfo, StoragePutConditions, StoragePutResult, StorageSource,
    StorageSourceType, StorageStatInfo,
};

/// Chunk size the PUT body is sliced into. Small enough that the in-flight
/// counter tracks socket write progress, large enough to stay cheap.
const PUT_CHUNK_SIZE: usize = 64 * 1024;

/// A read or a send that makes no progress for this long is a dead wire
/// (docs/app/reconnect.md). The pool carries no read timeout: a part takes
/// as long as the wire gives, so a GET is bound per received chunk and a PUT
/// per chunk handed to the socket — never as a whole.
const STALL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// An in-memory PUT body with an *exact* `size_hint`, so hyper emits a real
/// `Content-Length` (presigned S3 PUT rejects `Transfer-Encoding: chunked` with
/// 501) while each polled frame advances the storage's cumulative
/// `sent_progress` — the per-chunk feed the transfer engine draws the bar
/// from (`bytes_sent_progress`), the Rust analog of the TS
/// `xhr.upload.onprogress`. The bytes stay counted once sent; only a
/// request that fails rolls its own contribution back ([`SentRollback`]).
struct CountingBody {
    buf: Bytes,
    pos: usize,
    progress: Arc<AtomicU64>,
    added: Arc<AtomicU64>,
}

impl CountingBody {
    fn new(buf: Bytes, progress: Arc<AtomicU64>, added: Arc<AtomicU64>) -> Self {
        Self {
            buf,
            pos: 0,
            progress,
            added,
        }
    }
}

impl http_body::Body for CountingBody {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        if this.pos >= this.buf.len() {
            return Poll::Ready(None);
        }
        let end = (this.pos + PUT_CHUNK_SIZE).min(this.buf.len());
        let chunk = this.buf.slice(this.pos..end);
        let n = chunk.len() as u64;
        this.pos = end;
        this.progress.fetch_add(n, Ordering::Relaxed);
        this.added.fetch_add(n, Ordering::Relaxed);
        Poll::Ready(Some(Ok(Frame::data(chunk))))
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact((self.buf.len() - self.pos) as u64)
    }
}

/// Rolls a [`CountingBody`]'s counted bytes back out of `sent_progress` if
/// the request does not succeed — an error, or the request future being
/// dropped: those bytes go out again on the retry. `put_via_http` disarms it
/// once the response says the object landed.
struct SentRollback {
    progress: Arc<AtomicU64>,
    added: Arc<AtomicU64>,
    armed: bool,
}

impl Drop for SentRollback {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.progress
            .fetch_sub(self.added.load(Ordering::Relaxed), Ordering::Relaxed);
    }
}

/// Presigned S3 storage — implements source, dest, and multipart-dest.
///
/// Holds no HTTP client of its own: every request goes over `Back`'s pool
/// (`Back::http`), taken per request, so a network path change that renews
/// the pool reaches the next request here too.
pub struct S3Storage {
    back: Arc<RwLock<Back>>,
    urltoken: String,
    /// The kept GET URLs — the backend's download manifest returned at start
    /// plus every URL minted on demand since. Mirrors TS `lds.presigned_files`.
    /// Behind a mutex so a stale entry (403 on a warm source that outlived the
    /// signature) can be dropped and re-minted from `&self`.
    presigned_files: Mutex<Option<Vec<PresignedFile>>>,

    is_cancelled: Arc<AtomicBool>,
    total_sent: AtomicU64,
    // recv accounting is Arc-shared so the streaming get_streamed() pump task can
    // account bytes from a spawned context (mirrors S3Storage).
    total_recv: Arc<AtomicU64>,
    /// Cumulative bytes put on the wire, per body chunk (`CountingBody`);
    /// rolled back only for a PUT that failed. `bytes_sent_progress`.
    sent_progress: Arc<AtomicU64>,
    data_source_id: Option<String>,
}

/// A `Back` failure as the storage layer reports it: the expired session and
/// the quota refusal keep their classes, a dead wire is `Transport`, every
/// other backend answer is an `S3` error.
fn back_err(e: crate::back::BackError) -> StorageError {
    if e.is_session_expired() {
        StorageError::SessionExpired(e.to_string())
    } else if e.is_quota_exceeded() {
        StorageError::CloudQuotaExceeded(e.to_string())
    } else if e.is_transport() {
        StorageError::Transport(e.to_string())
    } else {
        StorageError::S3(e.to_string())
    }
}

impl S3Storage {
    pub fn new(back: Arc<RwLock<Back>>, urltoken: String, presigned_files: Option<Vec<PresignedFile>>) -> Self {
        Self {
            back,
            urltoken,
            presigned_files: Mutex::new(presigned_files),
            is_cancelled: Arc::new(AtomicBool::new(false)),
            total_sent: AtomicU64::new(0),
            total_recv: Arc::new(AtomicU64::new(0)),
            sent_progress: Arc::new(AtomicU64::new(0)),
            data_source_id: None,
        }
    }

    fn add_sent_bytes(&self, n: u64) {
        self.total_sent.fetch_add(n, Ordering::Relaxed);
    }

    /// Kept GET URL for `object`, if any. Mirrors TS `manifestUrl`.
    fn manifest_url(&self, object: &str) -> Option<String> {
        self.presigned_files
            .lock()
            .unwrap()
            .as_ref()?
            .iter()
            .find(|f| f.path == object)
            .map(|f| f.url.clone())
    }

    /// Forget a manifest URL whose signature aged out (S3 answered 403), so
    /// later reads of the key mint fresh single-shot URLs instead of replaying
    /// the stale one.
    /// A reopened backend session minted a fresh manifest
    /// (docs/app/reconnect.md): replace ours, the object stays.
    pub fn set_presigned_files(&self, files: Option<Vec<PresignedFile>>) {
        *self.presigned_files.lock().unwrap() = files;
    }

    fn drop_manifest_url(&self, object: &str) {
        if let Some(files) = self.presigned_files.lock().unwrap().as_mut() {
            files.retain(|f| f.path != object);
        }
    }

    /// Kept URL for `object`, or one fresh mint that joins the kept list for
    /// every later read of the object. Mirrors TS `getUrl` minus its in-flight
    /// dedupe: the engine's concurrent reads are of distinct parts, so two
    /// overlapping mints of one object are rare and the later one simply
    /// replaces the earlier entry.
    async fn get_url(&self, object: &str) -> Result<String, StorageError> {
        if let Some(url) = self.manifest_url(object) {
            return Ok(url);
        }
        let files = {
            let back = self.back.read().await;
            back.download_presign(&self.urltoken, &[PresignRequest::plain(object)])
                .await
                .map_err(back_err)?
        };
        let file = files
            .into_iter()
            .next()
            .ok_or_else(|| StorageError::S3(format!("no presigned GET url for {}", object)))?;
        let url = file.url.clone();
        let mut kept = self.presigned_files.lock().unwrap();
        let kept = kept.get_or_insert_with(Vec::new);
        kept.retain(|f| f.path != object);
        kept.push(file);
        Ok(url)
    }

    /// Mint a fresh single-shot PUT URL for `object`, with any preconditions
    /// signed in; returns the URL and the headers that must accompany the PUT.
    /// `size` is the exact byte count the PUT will send (the encrypted blob is
    /// already in hand) — declared for quota admission and signed into the URL
    /// as Content-Length. Mirrors TS `putUrl`.
    async fn put_url(
        &self,
        object: &str,
        size: u64,
        conditions: Option<&StoragePutConditions>,
    ) -> Result<PresignedFile, StorageError> {
        let mut req = PresignRequest::plain(object);
        req.size = Some(size);
        if let Some(c) = conditions {
            req.if_match = c.if_match.clone();
            req.if_none_match = c.if_none_match;
        }
        let files = {
            let back = self.back.read().await;
            back.upload_presign(&self.urltoken, &[req]).await.map_err(back_err)?
        };
        files
            .into_iter()
            .next()
            .ok_or_else(|| StorageError::S3(format!("no presigned PUT url for {}", object)))
    }

    /// Issue the ranged GET for a presigned URL and return the response with
    /// its body unread (the caller streams it).
    async fn send_get(&self, url: &str, range: &Option<String>) -> Result<reqwest::Response, StorageError> {
        let http = self.back.read().await.http();
        let mut req = http.get(url);
        if let Some(r) = range {
            req = req.header(reqwest::header::RANGE, r.clone());
        }
        // The answer's headers are the first read: bound like every other.
        match tokio::time::timeout(STALL_TIMEOUT, req.send()).await {
            Ok(sent) => sent.map_err(|e| StorageError::Transport(format!("GET network error: {}", error_chain(&e)))),
            Err(_) => Err(StorageError::Transport(format!(
                "GET stalled: no answer for {}s",
                STALL_TIMEOUT.as_secs()
            ))),
        }
    }

    /// PUT `data` to a presigned URL, returning the object's ETag. Reports upload
    /// progress into `sent_progress` via `CountingBody`. `headers` are the signed
    /// precondition headers the presign handed back; a 412 (or the 409 some
    /// providers use for `If-None-Match`) is `PreconditionFailed`. Mirrors TS `putViaXhr`.
    async fn put_via_http(
        &self,
        url: &str,
        headers: &[(String, String)],
        data: &[u8],
    ) -> Result<Option<String>, StorageError> {
        let added = Arc::new(AtomicU64::new(0));
        let mut rollback = SentRollback {
            progress: self.sent_progress.clone(),
            added: added.clone(),
            armed: true,
        };
        let body = CountingBody::new(Bytes::copy_from_slice(data), self.sent_progress.clone(), added.clone());
        let http = self.back.read().await.http();
        let mut req = http.put(url).body(reqwest::Body::wrap(body));
        for (name, value) in headers {
            req = req.header(name.as_str(), value.as_str());
        }
        // The send is bound per chunk handed to the socket: `added` moves
        // with each one, and 30 s without a move — the wait for S3's answer
        // after the last chunk included — is a dead wire. Never as a whole:
        // a part takes as long as the uplink takes.
        let send = req.send();
        tokio::pin!(send);
        let response = loop {
            let seen = added.load(Ordering::Relaxed);
            tokio::select! {
                sent = &mut send => {
                    break sent.map_err(|e| StorageError::Transport(format!("PUT network error: {}", error_chain(&e))))?;
                }
                _ = tokio::time::sleep(STALL_TIMEOUT) => {
                    if added.load(Ordering::Relaxed) == seen {
                        return Err(StorageError::Transport(format!(
                            "PUT stalled: no send progress for {}s",
                            STALL_TIMEOUT.as_secs()
                        )));
                    }
                }
            }
        };

        let status = response.status();
        if status == reqwest::StatusCode::PRECONDITION_FAILED
            || (status == reqwest::StatusCode::CONFLICT && !headers.is_empty())
        {
            return Err(StorageError::PreconditionFailed(format!("PUT {}", status.as_u16())));
        }
        if !status.is_success() {
            let text = response.text().await.unwrap_or_default();
            return Err(StorageError::S3(format!("PUT failed {} {}", status.as_u16(), text)));
        }

        // Landed: the counted bytes stay counted.
        rollback.armed = false;

        let etag = response
            .headers()
            .get(reqwest::header::ETAG)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        Ok(etag)
    }

    /// The path an open multipart upload was begun under. Both keys are seeded to
    /// the object at `begin_multipart`, so either resolves it.
    fn upload_path(upload: &StorageMultipartUpload) -> Result<String, StorageError> {
        upload
            .encrypted_key
            .as_ref()
            .or(upload.plain_key.as_ref())
            .cloned()
            .ok_or_else(|| StorageError::S3("multipart upload has no key".into()))
    }
}

#[async_trait]
impl Storage for S3Storage {
    async fn connect(&mut self) -> Result<StorageMetaState, StorageError> {
        tracing::debug!("S3Storage connect");
        Ok(StorageMetaState::Unknown)
    }

    async fn cancel(&self) -> Result<(), StorageError> {
        tracing::debug!("cancel S3");
        self.is_cancelled.store(true, Ordering::Relaxed);
        Ok(())
    }

    /// Live listing via the backend's server-side ListObjectsV2 (one page per
    /// call, paginated by cursor) — a flat recursive list under the prefix, with
    /// non-recursive grouping filtered client-side. Mirrors TS `getDir`.
    async fn get_dir(
        &self,
        path: &str,
        recursive: bool,
        start_after: Option<&str>,
        max_batch_size: Option<usize>,
    ) -> mpsc::Receiver<Result<Vec<StorageObjectInfo>, StorageError>> {
        let (tx, rx) = mpsc::channel(4);
        let back = self.back.clone();
        let urltoken = self.urltoken.clone();
        let start_after = start_after.map(|s| s.to_string());
        let mut list_prefix = path.to_string();
        if !path.is_empty() && !path.ends_with('/') {
            list_prefix.push('/');
        }
        let limit = max_batch_size.filter(|n| *n > 0);

        tokio::spawn(async move {
            let mut cursor: Option<String> = None;
            loop {
                let page = {
                    let back = back.read().await;
                    // start_after applies to the first page only; later pages follow the cursor.
                    let after = if cursor.is_none() { start_after.as_deref() } else { None };
                    back.list_objects(&urltoken, &list_prefix, cursor.as_deref(), after)
                        .await
                };
                let (objects, next_cursor) = match page {
                    Ok(p) => p,
                    Err(e) => {
                        let _ = tx.send(Err(back_err(e))).await;
                        return;
                    }
                };

                let mut page_items: Vec<StorageObjectInfo> = Vec::new();
                for o in objects {
                    let rel = &o.path[list_prefix.len().min(o.path.len())..];
                    if rel.is_empty() {
                        continue;
                    }
                    if !recursive && rel.contains('/') {
                        continue;
                    }
                    let mut soi = StorageObjectInfo::new(&o.path);
                    if let Some(sz) = o.size_bytes {
                        soi.size = sz;
                    }
                    soi.etag = o.etag;
                    soi.attrs.mtime = o.last_modified;
                    page_items.push(soi);
                }

                match limit {
                    Some(l) => {
                        for chunk in page_items.chunks(l) {
                            if tx.send(Ok(chunk.to_vec())).await.is_err() {
                                return;
                            }
                        }
                    }
                    None => {
                        if !page_items.is_empty() && tx.send(Ok(page_items)).await.is_err() {
                            return;
                        }
                    }
                }

                match next_cursor {
                    Some(c) => cursor = Some(c),
                    None => break,
                }
            }
        });

        rx
    }

    /// Single-object attrs via the smallest live query — a one-object list (no
    /// presigned HEAD). Mirrors TS `getAttrs`.
    async fn get_attrs(&self, path: &str) -> Result<StorageObjectInfo, StorageError> {
        let (objects, _) = {
            let back = self.back.read().await;
            back.list_objects(&self.urltoken, path, None, None)
                .await
                .map_err(back_err)?
        };
        let o = objects
            .into_iter()
            .find(|e| e.path == path)
            .ok_or_else(|| StorageError::NoSuchKey(path.to_string()))?;
        let mut soi = StorageObjectInfo::new(path);
        if let Some(sz) = o.size_bytes {
            soi.size = sz;
        }
        soi.etag = o.etag;
        soi.attrs.mtime = o.last_modified;
        Ok(soi)
    }

    fn total_bytes_sent(&self) -> u64 {
        self.total_sent.load(Ordering::Relaxed)
    }

    fn total_bytes_recv(&self) -> u64 {
        self.total_recv.load(Ordering::Relaxed)
    }

    /// One mint for every object not yet kept; each joins the kept list.
    /// Mirrors TS `presignUrls`.
    async fn presign_urls(&self, objects: &[String]) -> Result<(), StorageError> {
        let missing: Vec<PresignRequest> = objects
            .iter()
            .filter(|o| self.manifest_url(o).is_none())
            .map(|o| PresignRequest::plain(o.as_str()))
            .collect();
        if missing.is_empty() {
            return Ok(());
        }
        let files = {
            let back = self.back.read().await;
            back.download_presign(&self.urltoken, &missing)
                .await
                .map_err(back_err)?
        };
        let mut kept = self.presigned_files.lock().unwrap();
        let kept = kept.get_or_insert_with(Vec::new);
        for file in files {
            kept.retain(|f| f.path != file.path);
            kept.push(file);
        }
        Ok(())
    }

    fn bytes_sent_progress(&self) -> Option<u64> {
        Some(self.sent_progress.load(Ordering::Relaxed))
    }

    fn data_source_id(&self) -> Option<&str> {
        self.data_source_id.as_deref()
    }

    fn set_data_source_id(&mut self, id: String) {
        self.data_source_id = Some(id);
    }
}

#[async_trait]
impl StorageSource for S3Storage {
    fn source_name(&self) -> &str {
        "S3"
    }

    fn source_type(&self) -> StorageSourceType {
        StorageSourceType::S3
    }

    async fn get_streamed(
        &self,
        object: &str,
        offset: Option<u64>,
        length: Option<u64>,
    ) -> mpsc::Receiver<Result<Vec<u8>, StorageError>> {
        tracing::debug!("getStreamedS3 {} offset {:?} len {:?}", object, offset, length);
        let (tx, rx) = mpsc::channel(4);

        // Range header (CORS-allowed by the bucket, as the SDK path already
        // relied on); no other request headers, matching the TS reference.
        let range = offset.map(|off| match length {
            Some(len) => format!("bytes={}-{}", off, off + len - 1),
            None => format!("bytes={}-", off),
        });

        // Resolve the URL and the response headers up front (needs &self for the
        // manifest and re-mint paths); only the body pump runs in the spawned
        // task. On failure, deliver the error as the single channel item,
        // mirroring S3Storage's error path.
        let from_manifest = self.manifest_url(object).is_some();
        let url = match self.get_url(object).await {
            Ok(u) => u,
            Err(e) => {
                let _ = tx.send(Err(e)).await;
                return rx;
            }
        };
        let mut response = match self.send_get(&url, &range).await {
            Ok(resp) => resp,
            Err(e) => {
                let _ = tx.send(Err(e)).await;
                return rx;
            }
        };

        // A 403 on a manifest URL means its signature (minted at
        // `download_start`) aged out on this warm source — drop the stale entry
        // and retry once with a freshly minted URL. A 403 on a fresh mint is a
        // real failure and falls through to the status check below.
        if response.status().as_u16() == 403 && from_manifest {
            tracing::info!(object = %object, "presigned manifest URL expired (403); re-minting");
            self.drop_manifest_url(object);
            let fresh = match self.get_url(object).await {
                Ok(u) => u,
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                    return rx;
                }
            };
            response = match self.send_get(&fresh, &range).await {
                Ok(resp) => resp,
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                    return rx;
                }
            };
        }

        let status = response.status();
        if status.as_u16() == 404 {
            let _ = tx.send(Err(StorageError::NoSuchKey(object.to_string()))).await;
            return rx;
        }
        if !status.is_success() {
            let _ = tx
                .send(Err(StorageError::S3(format!(
                    "GET failed {} for {}",
                    status.as_u16(),
                    object
                ))))
                .await;
            return rx;
        }

        let total_recv = self.total_recv.clone();
        let is_cancelled = self.is_cancelled.clone();

        tokio::spawn(async move {
            let mut body = response.bytes_stream();
            loop {
                // Bound per chunk, not per body: a part takes as long as the
                // wire gives, a wire that gives nothing for 30 s is dead.
                let next = match tokio::time::timeout(STALL_TIMEOUT, body.next()).await {
                    Ok(next) => next,
                    Err(_) => {
                        let _ = tx
                            .send(Err(StorageError::Transport(format!(
                                "GET stalled: no data for {}s",
                                STALL_TIMEOUT.as_secs()
                            ))))
                            .await;
                        break;
                    }
                };
                match next {
                    Some(Ok(chunk)) => {
                        // Mirror TS getStreamed: check cancel after each read.
                        if is_cancelled.load(Ordering::Relaxed) {
                            let _ = tx.send(Err(StorageError::Cancelled)).await;
                            break;
                        }
                        total_recv.fetch_add(chunk.len() as u64, Ordering::Relaxed);
                        // Receiver dropped → stop reading, which drops the stream
                        // and aborts the HTTP body fetch.
                        if tx.send(Ok(chunk.to_vec())).await.is_err() {
                            break;
                        }
                    }
                    Some(Err(e)) => {
                        // The body stream died under us — the wire, not S3.
                        let _ = tx.send(Err(StorageError::Transport(error_chain(&e)))).await;
                        break;
                    }
                    None => break,
                }
            }
        });

        rx
    }

    async fn get_symlink(&self, object: &str) -> Result<String, StorageError> {
        let data = self.get(object, None, None).await?;
        String::from_utf8(data).map_err(|e| StorageError::Other(e.to_string()))
    }
}

#[async_trait]
impl StorageDest for S3Storage {
    fn is_sequential(&self) -> bool {
        false
    }

    async fn put_file(
        &self,
        object: &str,
        data: &[u8],
        _attrs: Option<&StorageFileAttrs>,
        conditions: Option<&StoragePutConditions>,
    ) -> Result<StoragePutResult, StorageError> {
        if self.is_cancelled.load(Ordering::Relaxed) {
            return Err(StorageError::Cancelled);
        }
        let presigned = self.put_url(object, data.len() as u64, conditions).await?;
        let etag = self.put_via_http(&presigned.url, &presigned.headers, data).await?;
        // Counted here, not just in `put_part`: the ninja layer writes every blob
        // (including its own multipart parts) through `put_file`, so this is the
        // only site that sees upload bytes on the presigned path.
        self.add_sent_bytes(data.len() as u64);
        Ok(StoragePutResult {
            etag,
            crc32: None,
            sha256: None,
            storage_key: object.to_string(),
        })
    }

    async fn put_dir(&self, object: &str, attrs: Option<&StorageFileAttrs>) -> Result<StoragePutResult, StorageError> {
        tracing::debug!("putDir {}", object);
        self.put_file(object, &[], attrs, None).await
    }

    async fn put_symlink(
        &self,
        object: &str,
        target: &str,
        attrs: Option<&StorageFileAttrs>,
    ) -> Result<StoragePutResult, StorageError> {
        self.put_file(object, target.as_bytes(), attrs, None).await
    }

    /// No client-signed delete; removal goes through the backend, which calls S3
    /// DeleteObject (idempotent server-side). Mirrors TS `delFile`.
    async fn del_file(&self, object: &str) -> Result<(), StorageError> {
        tracing::debug!("delFile {}", object);
        let back = self.back.read().await;
        back.delete_object(&self.urltoken, object).await.map_err(back_err)
    }

    async fn del_dir(&self, _object: &str) -> Result<(), StorageError> {
        Err(StorageError::NotImplemented("delDir is not supported on S3".into()))
    }

    async fn update_stats(&self, info: &StorageStatInfo) -> Result<(), StorageError> {
        tracing::debug!("updateStats {:?}", info.meta_state);
        Ok(())
    }

    async fn close(&self) -> Result<(), StorageError> {
        tracing::debug!("close S3");
        Ok(())
    }
}

#[async_trait]
impl StorageDestMultipart for S3Storage {
    async fn list_multiparts_all(&self) -> Result<Vec<StorageMultipartUpload>, StorageError> {
        let uploads = {
            let back = self.back.read().await;
            back.multipart_list_uploads(&self.urltoken).await.map_err(back_err)?
        };
        Ok(uploads
            .into_iter()
            .map(|u| StorageMultipartUpload {
                plain_key: Some(u.path.clone()),
                encrypted_key: Some(u.path),
                upload_id: Some(u.upload_id),
                rand: None,
            })
            .collect())
    }

    async fn list_parts_all(&self, upload: &StorageMultipartUpload) -> Result<Vec<StoragePartInfo>, StorageError> {
        let path = Self::upload_path(upload)?;
        let upload_id = upload
            .upload_id
            .as_ref()
            .ok_or_else(|| StorageError::S3("No upload ID".into()))?;
        let parts = {
            let back = self.back.read().await;
            back.multipart_list_parts(&self.urltoken, &path, upload_id)
                .await
                .map_err(back_err)?
        };
        Ok(parts
            .into_iter()
            .map(|p| StoragePartInfo {
                etag: Some(p.etag),
                part_number: Some(p.part_number),
                size: Some(p.size_bytes),
                ..Default::default()
            })
            .collect())
    }

    async fn begin_multipart(
        &self,
        object: &str,
        _size: u64,
        _attrs: Option<&StorageFileAttrs>,
    ) -> Result<StorageMultipartUpload, StorageError> {
        let upload_id = {
            let back = self.back.read().await;
            back.multipart_begin(&self.urltoken, object).await.map_err(back_err)?
        };
        Ok(StorageMultipartUpload {
            plain_key: Some(object.to_string()),
            encrypted_key: Some(object.to_string()),
            upload_id: Some(upload_id),
            rand: None,
        })
    }

    async fn put_part(
        &self,
        part_number: u32,
        upload: &StorageMultipartUpload,
        offset: u64,
        data: &[u8],
        is_last_part: bool,
    ) -> Result<StoragePartInfo, StorageError> {
        let path = Self::upload_path(upload)?;
        let upload_id = upload
            .upload_id
            .as_ref()
            .ok_or_else(|| StorageError::S3("No upload ID".into()))?;
        let data_len = data.len() as u64;

        let url =
            {
                let parts = {
                    let back = self.back.read().await;
                    back.multipart_parts(&self.urltoken, &path, upload_id, &[part_number])
                        .await
                        .map_err(back_err)?
                };
                parts.into_iter().next().map(|p| p.url).ok_or_else(|| {
                    StorageError::S3(format!("no presigned part url for {} part {}", path, part_number))
                })?
            };

        let etag = self.put_via_http(&url, &[], data).await?;
        self.add_sent_bytes(data_len);

        Ok(StoragePartInfo {
            etag,
            part_number: Some(part_number),
            offset: Some(offset),
            size: Some(data_len),
            is_last: Some(is_last_part),
            ..Default::default()
        })
    }

    async fn cancel_multipart(&self, upload: &StorageMultipartUpload) -> Result<(), StorageError> {
        let path = Self::upload_path(upload)?;
        let upload_id = upload
            .upload_id
            .as_ref()
            .ok_or_else(|| StorageError::S3("No upload ID".into()))?;
        let back = self.back.read().await;
        back.multipart_abort(&self.urltoken, &path, upload_id)
            .await
            .map_err(back_err)
    }

    /// Completes via the backend, which calls S3 CompleteMultipartUpload and
    /// returns the assembled object's ETag. Mirrors TS `completeMultipart`.
    async fn complete_multipart(
        &self,
        upload: &StorageMultipartUpload,
        parts: &[StoragePartInfo],
    ) -> Result<String, StorageError> {
        let path = Self::upload_path(upload)?;
        let upload_id = upload
            .upload_id
            .as_ref()
            .ok_or_else(|| StorageError::S3("No upload ID".into()))?;

        let cmpl_parts: Vec<MultipartCompletedPart> = parts
            .iter()
            .map(|p| {
                let part_number = p
                    .part_number
                    .ok_or_else(|| StorageError::S3("part missing partNumber".into()))?;
                let etag = p
                    .etag
                    .clone()
                    .ok_or_else(|| StorageError::S3("part missing ETag".into()))?;
                Ok(MultipartCompletedPart { part_number, etag })
            })
            .collect::<Result<_, StorageError>>()?;

        let back = self.back.read().await;
        back.multipart_complete(&self.urltoken, &path, upload_id, &cmpl_parts)
            .await
            .map_err(back_err)
    }
}

/// The per-read timeout (docs/app/reconnect.md, change set 5): a socket that
/// died in suspend fails in ~30 s, while a streamed body that keeps trickling
/// is never cut — the bound is per read, not per request.
#[cfg(test)]
mod timeout_tests {
    use super::*;
    use crate::back::PresignedFile;
    use crate::back::test_support::{Answer, MockBackend, advance, settle};

    fn s3_for(mock: &MockBackend) -> S3Storage {
        S3Storage::new(
            mock.back(),
            "tok".to_string(),
            Some(vec![PresignedFile {
                path: "f".into(),
                url: mock.s3_url("f"),
                headers: Vec::new(),
            }]),
        )
    }

    #[tokio::test(start_paused = true)]
    async fn read_timeout_fails_a_frozen_get() {
        let mock = MockBackend::start().await;
        mock.script("s3/get", &[Answer::Hang]);
        let s3 = Arc::new(s3_for(&mock));
        let task = {
            let s3 = s3.clone();
            tokio::spawn(async move { s3.get("f", None, None).await })
        };
        settle().await;
        assert_eq!(mock.count("s3/get"), 1, "the GET reached the server and hangs");
        assert!(!task.is_finished());
        advance(35).await;
        let err = task.await.unwrap().unwrap_err();
        assert!(matches!(err, StorageError::Transport(_)), "got {err:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn trickling_body_does_not_time_out() {
        let mock = MockBackend::start().await;
        mock.script("s3/get", &[Answer::Trickle]);
        let s3 = Arc::new(s3_for(&mock));
        let task = {
            let s3 = s3.clone();
            tokio::spawn(async move { s3.get("f", None, None).await })
        };
        settle().await;
        // Five 1-byte chunks, 20 virtual seconds apart: 80 s wall, every
        // read gap under the 30 s bound.
        advance(120).await;
        let got = task.await.unwrap().unwrap();
        assert_eq!(got, b"abcde");
    }

    /// A PUT whose send stops moving fails on the stall watch: the socket
    /// took what fits in its buffer, nothing more goes out, no answer comes.
    /// The counted bytes roll back with the failure.
    #[tokio::test(start_paused = true)]
    async fn stalled_put_fails() {
        let mock = MockBackend::start().await;
        mock.script("s3/put", &[Answer::Hang]);
        let s3 = Arc::new(s3_for(&mock));
        let url = mock.s3_url("f");
        let task = {
            let s3 = s3.clone();
            tokio::spawn(async move { s3.put_via_http(&url, &[], &vec![7u8; 64 * 1024]).await })
        };
        settle().await;
        assert_eq!(mock.count("s3/put"), 1, "the PUT reached the server and hangs");
        assert!(!task.is_finished());
        advance(35).await;
        let err = task.await.unwrap().unwrap_err();
        assert!(matches!(err, StorageError::Transport(_)), "got {err:?}");
        assert_eq!(s3.sent_progress.load(Ordering::Relaxed), 0, "a failed PUT rolls its bytes back");
    }

    /// A PUT that keeps moving is never cut, however long it takes in all:
    /// 32 MiB drained 4 MiB per 20 virtual seconds is 160 s, far past any
    /// whole-request bound, with every gap under the stall watch. This is
    /// the slow uplink — a part must land at whatever rate the wire gives.
    #[tokio::test(start_paused = true)]
    async fn slow_put_does_not_time_out() {
        let mock = MockBackend::start().await;
        mock.script("s3/put", &[Answer::Trickle]);
        let s3 = Arc::new(s3_for(&mock));
        let url = mock.s3_url("f");
        let size = 32 * 1024 * 1024;
        let task = {
            let s3 = s3.clone();
            tokio::spawn(async move { s3.put_via_http(&url, &[], &vec![7u8; size]).await })
        };
        settle().await;
        advance(200).await;
        let etag = task.await.unwrap().unwrap();
        assert_eq!(etag.as_deref(), Some("\"trickled\""));
        assert_eq!(s3.sent_progress.load(Ordering::Relaxed), size as u64);
    }
}
