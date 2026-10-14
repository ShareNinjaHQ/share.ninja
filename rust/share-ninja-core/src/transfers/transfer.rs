//! Transfer - High-level transfer orchestration
//!
//! Ports the TypeScript `TransferDownload` / `TransferUpload` (sharon-lib
//! `transfers/Transfer.ts`). Each struct is single-directional: `TransferDownload`
//! owns sources and writes into a caller-provided dest; `TransferUpload` owns a
//! dest and reads from a caller-provided source. Both manage connection setup,
//! periodic stats updates, and cleanup.
//!
//! Note on `cryptoPool`: the TS classes own a `CryptoWorkerPool` and thread it
//! into every `NinjaStorageV2`/`P2PStorage`. There is no such abstraction in the
//! Rust core — crypto runs inline (e.g. `P2PStorage` derives its session key and
//! encrypts/decrypts directly), so there is nothing to create, pass, or close.

use super::policy::TransferPolicies;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::RwLock;
use tokio::time::Duration;

use crate::back::{Back, LinkDataSource, TransferDSStat, TransferStats as BackTransferStats};

use super::multi_source::SessionParams;
use super::transfer_item::{TransferItem, TransferItemState, TransferProgress};
use super::{MultiSource, MultiSourceParams, S3Storage, Storage, StorageDestMultipart, StorageError, StorageSource};
use crate::ninja::{NinjaStorageV2, NinjaStorageV2Params, StorageSourceAndDest};

/// Transfer stats for the backend, shared by both directions. The byte total
/// is the same smoothed moved-bytes number the progress bar shows (resumed +
/// committed + in-flight, see [`TransferItem::moved_bytes`]) so server-side
/// progress matches what the user sees — a deliberate divergence from the TS
/// per-class `getTransferStats`, which reported only this run's
/// `dstBytesWritten`. Files/dirs/symlinks stay the dest-written counters, plus
/// the elapsed wall-clock time in milliseconds — the wire field is
/// `duration_ms`.
async fn compute_transfer_stats(items: &[Arc<TransferItem>], start_ts: i64) -> BackTransferStats {
    let now = chrono::Utc::now().timestamp_millis();
    let mut stats = BackTransferStats {
        bytes: 0,
        files: 0,
        dirs: 0,
        symlinks: 0,
        duration: (now - start_ts).max(0) as u64,
    };

    for item in items {
        let s = item.stats().snapshot();
        stats.bytes += item.moved_bytes().await;
        stats.files += s.dst_files_written;
        stats.dirs += s.dst_dirs_written;
        stats.symlinks += s.dst_symlinks_written;
    }

    stats
}

/// Session update cadence while a file transfer is open (progress).
const UPDATE_INTERVAL: Duration = Duration::from_secs(5);
/// Session update cadence with no file transfer open (keepalive): the backend
/// abandons a session after 5 min without a session-scoped call
/// (docs/app/reconnect.md); a fifth of that tolerates four lost pings.
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(60);

fn ninja_params_with_key(
    override_params: &Option<NinjaStorageV2Params>,
    master_key: Option<[u8; 32]>,
) -> NinjaStorageV2Params {
    let mut params = override_params.clone().unwrap_or_default();
    if let Some(key) = master_key {
        params.master_key = Some(key.to_vec());
    }
    params
}

// =====================================================================
// TransferDownload
// =====================================================================

pub struct TransferDownload {
    back: Arc<RwLock<Back>>,

    pub start_ts: i64,

    items: Arc<Mutex<Vec<Arc<TransferItem>>>>,
    source: Option<Arc<RwLock<dyn StorageSource>>>,
    /// The same object as `source`, typed: the backend session (`transfer_id`,
    /// reopen on a lapse) is the MultiSource's own. `None` for an attached
    /// local source, which has no session.
    multi: Option<Arc<RwLock<MultiSource>>>,

    pub urltoken: Option<String>,
    is_cancelled: Arc<AtomicBool>,
    /// Set once `do_download` has closed/flushed the dest — everything is
    /// really on disk. Until then `progress()` caps the ratio at 0.99 so the
    /// bar can't claim 100% while finalization still runs.
    finalized: Arc<AtomicBool>,

    /// The session loop (`start_update_timer`), connect → close.
    timer_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    ninja_params_override: Option<NinjaStorageV2Params>,
    multi_params: MultiSourceParams,
}

impl TransferDownload {
    pub fn new(back: Arc<RwLock<Back>>) -> Self {
        Self {
            back,
            start_ts: 0,
            items: Arc::new(Mutex::new(Vec::new())),
            source: None,
            multi: None,
            urltoken: None,
            is_cancelled: Arc::new(AtomicBool::new(false)),
            finalized: Arc::new(AtomicBool::new(false)),
            timer_handle: Arc::new(Mutex::new(None)),
            ninja_params_override: None,
            multi_params: MultiSourceParams::default(),
        }
    }

    /// The current backend session id (a new one after every reopen);
    /// `None` for an attached local source.
    pub async fn transfer_id(&self) -> Option<String> {
        match &self.multi {
            Some(m) => m.read().await.transfer_id(),
            None => None,
        }
    }

    /// The session is known gone and not yet reopened.
    pub async fn session_expired(&self) -> bool {
        match &self.multi {
            Some(m) => m.read().await.is_expired(),
            None => false,
        }
    }

    pub fn with_ninja_params(mut self, params: NinjaStorageV2Params) -> Self {
        self.ninja_params_override = Some(params);
        self
    }

    pub fn with_multi_params(mut self, params: MultiSourceParams) -> Self {
        self.multi_params = params;
        self
    }

    pub fn source(&self) -> Option<Arc<RwLock<dyn StorageSource>>> {
        self.source.clone()
    }

    /// Connect to download source(s). Mirrors TS `connectSource`.
    pub async fn connect_source(
        &mut self,
        urltoken: &str,
        src_encryption_key: Option<[u8; 32]>,
    ) -> Result<(), StorageError> {
        tracing::debug!("download item start {}", urltoken);
        self.urltoken = Some(urltoken.to_string());
        self.start_ts = chrono::Utc::now().timestamp_millis();

        match self.connect_source_inner(urltoken, src_encryption_key).await {
            Ok(_source) => Ok(()),
            Err(e) => {
                tracing::error!("downloadStart failed {}", e);
                let msg = e.to_string();
                if self.multi.is_some() {
                    self.finish_transfer(Some(&msg)).await;
                }
                Err(e)
            }
        }
    }

    async fn connect_source_inner(
        &mut self,
        urltoken: &str,
        src_encryption_key: Option<[u8; 32]>,
    ) -> Result<Arc<RwLock<dyn StorageSource>>, StorageError> {
        // The session and the members built from it are the MultiSource's —
        // it opens the session now and opens it again on a lapse.
        let multi = MultiSource::open(
            SessionParams {
                back: self.back.clone(),
                urltoken: urltoken.to_string(),
                key: src_encryption_key,
                ninja_params: ninja_params_with_key(&self.ninja_params_override, src_encryption_key),
            },
            Some(self.multi_params.clone()),
        )
        .await?;
        let multi = Arc::new(RwLock::new(multi));
        self.multi = Some(multi.clone());
        self.source = Some(multi);

        let source = self.source.as_ref().unwrap().clone();
        {
            let mut src = source.write().await;
            src.connect().await?;
        }
        tracing::info!("source connected");
        // The session loop lives with the session, not with a file transfer:
        // a warm source with no file transfer open makes no backend call at
        // all otherwise, and the backend abandons a silent session. Its
        // cadence follows `items` — progress while one runs, keepalive between.
        self.start_update_timer();
        Ok(source)
    }

    /// Attach an already-built local source (e.g. the app layer's `FsSource`
    /// over a finished download's on-disk artifacts) instead of connecting the
    /// link's remote datasources. The transfer then serves listings/searches/
    /// reads exactly like a connected one, with no backend transfer session —
    /// `transfer_id` stays `None`, so the session bookends (`transfer_end`,
    /// stats) are naturally skipped.
    pub async fn attach_source(
        &mut self,
        urltoken: &str,
        source: Arc<RwLock<dyn StorageSource>>,
    ) -> Result<(), StorageError> {
        self.urltoken = Some(urltoken.to_string());
        self.start_ts = chrono::Utc::now().timestamp_millis();
        source.write().await.connect().await?;
        self.source = Some(source);
        Ok(())
    }

    /// `selection`: normalized include-list of share-relative keys — `None` =
    /// whole share, empty = nothing (docs/core/PartialDownloads.md).
    pub async fn start_download(
        &self,
        selection: Option<Vec<String>>,
        dest: Arc<RwLock<dyn StorageDestMultipart>>,
        n_streams: usize,
    ) -> Arc<TransferItem> {
        self.start_download_with(selection, dest, n_streams, TransferPolicies::default())
            .await
    }

    /// `start_download` under explicit existing-entry / error policies.
    pub async fn start_download_with(
        &self,
        selection: Option<Vec<String>>,
        dest: Arc<RwLock<dyn StorageDestMultipart>>,
        n_streams: usize,
        policies: TransferPolicies,
    ) -> Arc<TransferItem> {
        self.start_download_with_root(selection, dest, n_streams, policies, "")
            .await
    }

    /// `start_download_with` with the item's `source_root` (see
    /// `TransferItem::with_source_root`): file-manager copies land the
    /// selection under the dest by basename, not by full share path.
    pub async fn start_download_with_root(
        &self,
        selection: Option<Vec<String>>,
        dest: Arc<RwLock<dyn StorageDestMultipart>>,
        n_streams: usize,
        policies: TransferPolicies,
        source_root: &str,
    ) -> Arc<TransferItem> {
        // A new item is starting on this (possibly warm, reused) transfer:
        // un-latch the pause/cancel flag from the previous run. It never
        // resets otherwise, so a resumed download's next pause() would hit
        // the already-cancelled early-return and leave the live item running
        // behind an idle row.
        self.is_cancelled.store(false, Ordering::SeqCst);
        let source = self.source.clone().expect("no source!");
        let item = Arc::new(
            TransferItem::new(source, dest, selection, n_streams)
                .await
                .with_policies(policies)
                .with_source_root(source_root),
        );
        self.items.lock().unwrap().push(item.clone());
        item
    }

    pub async fn do_download(
        &self,
        item: Arc<TransferItem>,
        close_dest: bool,
    ) -> (TransferItemState, Option<StorageError>) {
        let res = self.run_download(item.clone(), close_dest).await;
        // The run is over — retire the item from the shared list. A warm
        // transfer outlives its runs, and a dead item left behind keeps
        // counting its bytes into progress() and the session stats ticker
        // on the next (resumed) run.
        self.items.lock().unwrap().retain(|i| !Arc::ptr_eq(i, &item));
        res
    }

    async fn run_download(
        &self,
        item: Arc<TransferItem>,
        close_dest: bool,
    ) -> (TransferItemState, Option<StorageError>) {
        let urltoken = self.urltoken.clone().unwrap_or_default();
        // An item is (re)starting — its finalization hasn't happened yet.
        self.finalized.store(false, Ordering::SeqCst);
        // The dest is owned by the item (mirrors TS `ti.getDest()`); close/flush it here.
        let dest = item.get_dest();

        // A session the loop already knows is gone is reopened before the
        // file transfer is opened on it — not discovered by `file_start`.
        if let Some(m) = &self.multi
            && let Err(e) = m.read().await.ensure_session().await
        {
            if close_dest && let Err(ce) = dest.read().await.close().await {
                tracing::error!("dest close failed during error handling: {}", ce);
            }
            return (TransferItemState::Error, Some(e));
        }
        let transfer_id = self.transfer_id().await.unwrap_or_default();

        // Open the per-file transfer before running the item. `encrypted_metadata`
        // is a placeholder matching the current TS stub — to become the real
        // share-key-encrypted `{path, estimated_bytes}` blob.
        let start_res = {
            let back = self.back.read().await;
            back.file_start(true, &urltoken, &transfer_id, b"Ymxh".to_vec(), None, None)
                .await
        };
        let file_transfer_id = match start_res {
            Ok(id) => id,
            Err(e) => {
                tracing::error!("file_start failed: {}", e);
                let err = if e.is_session_expired() {
                    StorageError::SessionExpired(e.to_string())
                } else if e.is_transport() {
                    StorageError::Transport(e.to_string())
                } else {
                    StorageError::Other(e.to_string())
                };
                if close_dest && let Err(ce) = dest.read().await.close().await {
                    tracing::error!("dest close failed during error handling: {}", ce);
                }
                return (TransferItemState::Error, Some(err));
            }
        };

        let result = item.start().await;
        let end_ts = chrono::Utc::now().timestamp_millis();
        let secs_elapsed = (end_ts - self.start_ts) as f64 / 1000.0;

        match result {
            Ok(()) => {
                if item.is_cancelled() {
                    tracing::debug!("download cancelled");
                    self.end_file_transfer(&file_transfer_id, Some("cancelled")).await;
                    return (TransferItemState::Cancelled, None);
                }
                let stats = item.stats().snapshot();
                if close_dest {
                    if let Err(e) = dest.read().await.close().await {
                        tracing::error!("dest close failed: {}", e);
                        let emsg = e.to_string();
                        self.end_file_transfer(&file_transfer_id, Some(&emsg)).await;
                        return (TransferItemState::Error, Some(e));
                    }
                    tracing::debug!("dest closed");
                } else {
                    if let Err(e) = dest.read().await.flush().await {
                        tracing::error!("dest flush failed: {}", e);
                        let emsg = e.to_string();
                        self.end_file_transfer(&file_transfer_id, Some(&emsg)).await;
                        return (TransferItemState::Error, Some(e));
                    }
                    tracing::debug!("dest flushed");
                }
                self.finalized.store(true, Ordering::SeqCst);
                tracing::debug!(
                    "download complete, took {:.2} sec, avg {:.2} MB/s",
                    secs_elapsed,
                    stats.total_bytes as f64 / secs_elapsed / 1024.0 / 1024.0
                );
                self.end_file_transfer(&file_transfer_id, None).await;
                (TransferItemState::Finished, None)
            }
            Err(e) => {
                if item.is_cancelled() {
                    self.end_file_transfer(&file_transfer_id, Some("cancelled")).await;
                    return (TransferItemState::Cancelled, None);
                }
                let stats = item.stats().snapshot();
                tracing::debug!(
                    "transfer failed, took {:.2} sec, avg {:.2} MB/s: {}",
                    secs_elapsed,
                    stats.total_bytes as f64 / secs_elapsed / 1024.0 / 1024.0,
                    e
                );
                let emsg = e.to_string();
                if close_dest {
                    if let Err(ce) = dest.read().await.close().await {
                        tracing::error!("dest close failed during error handling: {}", ce);
                    }
                    tracing::debug!("dest closed");
                }
                self.end_file_transfer(&file_transfer_id, Some(&emsg)).await;
                (TransferItemState::Error, Some(e))
            }
        }
    }

    async fn end_file_transfer(&self, file_transfer_id: &str, error: Option<&str>) {
        let urltoken = self.urltoken.clone().unwrap_or_default();
        let res = {
            let back = self.back.read().await;
            back.file_end(true, &urltoken, file_transfer_id, error.is_none(), None, error)
                .await
        };
        if let Err(e) = res {
            tracing::error!("file_end failed: {}", e);
        }
    }

    /// Finish transfer and send end stats. Mirrors TS `finishTransfer`.
    pub async fn finish_transfer(&mut self, error: Option<&str>) {
        self.stop_update_timer();

        if let Err(e) = self.send_end_transfer(error).await {
            tracing::debug!("failed to send end transfer: {}", e);
        }
    }

    /// Tear a warm transfer down for good (the app-layer eviction/reopen path):
    /// stop the session loop, send the terminal `transfer_end` bookend —
    /// skipped when the session already `expired` server-side (told by the
    /// caller or heard by the loop), there is nothing left to end — and cascade
    /// `cleanup()` through the composed source (P2P closes its WebRTC
    /// connection). The transfer must not be used afterwards.
    pub async fn close(&self, expired: bool) {
        self.stop_update_timer();
        if !expired
            && !self.session_expired().await
            && let Err(e) = self.send_end_transfer(None).await
        {
            tracing::debug!("transfer_end on close failed: {}", e);
        }
        // `source` is the composed MultiSource; its cleanup cascades to the
        // members.
        if let Some(source) = &self.source {
            source.write().await.cleanup();
        }
    }

    pub fn handle(&self) -> TransferHandle {
        TransferHandle {
            items: self.items.clone(),
            is_cancelled: self.is_cancelled.clone(),
            finalized: self.finalized.clone(),
        }
    }

    pub async fn pause(&self) {
        self.handle().pause().await;
    }

    pub async fn cancel(&self) {
        self.handle().cancel().await;
    }

    pub fn is_cancelled(&self) -> bool {
        self.is_cancelled.load(Ordering::SeqCst)
    }

    /// Start the session loop once per session (idempotent).
    fn start_update_timer(&self) {
        if self.timer_handle.lock().unwrap().is_some() {
            return;
        }
        let (Some(urltoken), Some(multi)) = (self.urltoken.clone(), self.multi.clone()) else {
            return;
        };
        tracing::debug!("transfer timer start");

        let back = self.back.clone();
        let items = self.items.clone();
        let start_ts = self.start_ts;

        // The session loop: from connect to close, while the backend is
        // `Online`, one `transfer_update` every UPDATE_INTERVAL when an item is
        // running and every KEEPALIVE_INTERVAL when none is. While the backend
        // is not there it waits for it instead of ticking into `check_online`'s
        // fail-fast. Sleep-paced, so a suspended process wakes to one late
        // update, never a burst. The one thing it learns is a
        // `transfer_not_found`: recorded in `session_expired`, then it stops;
        // any other failure is logged (a lost update is no verdict).
        let handle = tokio::spawn(async move {
            let mut state = back.read().await.state_watch();
            let mut idle = Duration::ZERO;
            loop {
                let _ = state.wait_for(|s| s.is_online()).await;
                tokio::time::sleep(UPDATE_INTERVAL).await;
                if !state.borrow().is_online() {
                    continue;
                }
                let snapshot = items.lock().unwrap().clone();
                idle += UPDATE_INTERVAL;
                if snapshot.is_empty() && idle < KEEPALIVE_INTERVAL {
                    continue;
                }
                idle = Duration::ZERO;
                let back_stats = compute_transfer_stats(&snapshot, start_ts).await;
                // The current id — a reopen may have changed it — and the
                // members' byte counters.
                let (transfer_id, ds_stats) = {
                    let m = multi.read().await;
                    (m.transfer_id(), m.ds_stats().await)
                };
                let Some(transfer_id) = transfer_id else {
                    continue;
                };

                let res = {
                    let back = back.read().await;
                    back.transfer_update(true, &urltoken, &transfer_id, &back_stats, &ds_stats)
                        .await
                };
                match res {
                    Ok(()) => {}
                    Err(e) if e.is_session_expired() => {
                        tracing::info!(transfer_id = %transfer_id, "transfer session expired: {}", e);
                        // The next use of the source reopens; no more pings on this id.
                        multi.read().await.mark_expired();
                        break;
                    }
                    Err(e) => tracing::error!("transfer_update failed: {}", e),
                }
            }
        });

        *self.timer_handle.lock().unwrap() = Some(handle);
    }

    fn stop_update_timer(&self) {
        if let Some(handle) = self.timer_handle.lock().unwrap().take() {
            handle.abort();
        }
    }

    async fn send_end_transfer(&self, error: Option<&str>) -> Result<(), String> {
        let snapshot = self.items.lock().unwrap().clone();
        let back_stats = compute_transfer_stats(&snapshot, self.start_ts).await;
        let (transfer_id, ds_stats) = match &self.multi {
            Some(m) => {
                let m = m.read().await;
                (m.transfer_id(), m.ds_stats().await)
            }
            None => (None, Vec::new()),
        };

        if let (Some(urltoken), Some(transfer_id)) = (&self.urltoken, &transfer_id) {
            let back = self.back.read().await;
            back.transfer_end(true, urltoken, transfer_id, &back_stats, &ds_stats, error)
                .await
                .map_err(|e| e.to_string())?;
        }

        Ok(())
    }
}

impl Drop for TransferDownload {
    fn drop(&mut self) {
        self.stop_update_timer();
    }
}

// =====================================================================
// TransferUpload
// =====================================================================

pub struct TransferUpload {
    back: Arc<RwLock<Back>>,

    pub transfer_id: Option<String>,
    pub error: Option<StorageError>,
    pub start_ts: i64,
    pub end_ts: i64,

    items: Arc<Mutex<Vec<Arc<TransferItem>>>>,
    dest: Option<Arc<RwLock<dyn StorageDestMultipart>>>,
    dest_data_source_id: Option<String>,

    pub urltoken: Option<String>,
    is_cancelled: Arc<AtomicBool>,
    /// Set once `do_upload` has closed/flushed the dest — everything is really
    /// in storage. Until then `progress()` caps the ratio at 0.99 so the bar
    /// can't claim 100% while finalization (journal/meta writes) still runs.
    finalized: Arc<AtomicBool>,
    /// The backend answered `transfer_not_found` for this session: a fresh
    /// run is a fresh session (the app's `schedule_uploads`).
    session_expired: Arc<AtomicBool>,

    /// The session loop (`start_update_timer`), connect → finish.
    timer_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    ninja_params_override: Option<NinjaStorageV2Params>,
}

impl TransferUpload {
    pub fn new(back: Arc<RwLock<Back>>) -> Self {
        Self {
            back,
            transfer_id: None,
            error: None,
            start_ts: 0,
            end_ts: 0,
            items: Arc::new(Mutex::new(Vec::new())),
            dest: None,
            dest_data_source_id: None,
            urltoken: None,
            is_cancelled: Arc::new(AtomicBool::new(false)),
            finalized: Arc::new(AtomicBool::new(false)),
            session_expired: Arc::new(AtomicBool::new(false)),
            timer_handle: Arc::new(Mutex::new(None)),
            ninja_params_override: None,
        }
    }

    /// The backend has forgotten this session; a fresh `TransferUpload` is
    /// needed.
    pub fn is_session_expired(&self) -> bool {
        self.session_expired.load(Ordering::SeqCst)
    }

    pub fn with_ninja_params(mut self, params: NinjaStorageV2Params) -> Self {
        self.ninja_params_override = Some(params);
        self
    }

    /// Connect to upload destination. Mirrors TS `connectDest`.
    pub async fn connect_dest(&mut self, urltoken: &str, encryption_key: Option<[u8; 32]>) -> Result<(), StorageError> {
        tracing::debug!("upload item start");
        self.urltoken = Some(urltoken.to_string());
        self.start_ts = chrono::Utc::now().timestamp_millis();

        match self.connect_dest_inner(urltoken, encryption_key).await {
            Ok(_dest) => Ok(()),
            Err(e) => {
                tracing::debug!("uploadStart fail {}", e);
                let msg = e.to_string();
                self.error = Some(StorageError::Other(msg.clone()));
                if self.transfer_id.is_some() {
                    self.finish_transfer(Some(&msg)).await;
                }
                Err(e)
            }
        }
    }

    async fn connect_dest_inner(
        &mut self,
        urltoken: &str,
        encryption_key: Option<[u8; 32]>,
    ) -> Result<Arc<RwLock<dyn StorageDestMultipart>>, StorageError> {
        let back = self.back.read().await;
        let tsa = back.upload_start(urltoken).await.map_err(|e| {
            if e.is_quota_exceeded() {
                StorageError::CloudQuotaExceeded(e.to_string())
            } else {
                StorageError::Other(e.to_string())
            }
        })?;
        drop(back);

        tracing::debug!("uploadStart result, transfer_id: {}", tsa.transfer_id);
        self.transfer_id = Some(tsa.transfer_id.clone());

        let s3_ds = tsa
            .datasources
            .iter()
            .find_map(|ds| match ds {
                LinkDataSource::S3(s3) => Some(s3),
                _ => None,
            })
            .ok_or_else(|| StorageError::Other("No S3 data source".to_string()))?;

        tracing::debug!(
            "cloud upload item {} bucket {} prefix {}",
            urltoken,
            s3_ds.bucket,
            s3_ds.prefix
        );

        self.dest_data_source_id = Some(s3_ds.datasource_id.clone());

        let mut s3 = S3Storage::new(self.back.clone(), urltoken.to_string(), s3_ds.presigned_files.clone());
        s3.set_data_source_id(s3_ds.datasource_id.clone());
        let s3_wrapped: Arc<RwLock<dyn StorageSourceAndDest>> = Arc::new(RwLock::new(s3));
        let ninja_params = ninja_params_with_key(&self.ninja_params_override, encryption_key);
        let mut ninja = NinjaStorageV2::new(ninja_params, s3_wrapped);
        ninja.set_data_source_id(s3_ds.datasource_id.clone());

        let dest: Arc<RwLock<dyn StorageDestMultipart>> = Arc::new(RwLock::new(ninja));
        self.dest = Some(dest.clone());

        {
            let mut d = dest.write().await;
            d.connect().await?;
        }
        tracing::info!("dest connected");
        self.start_update_timer();
        Ok(dest)
    }

    pub async fn start_upload(
        &mut self,
        source: Arc<RwLock<dyn StorageSource>>,
        remote_path: String,
        n_streams: usize,
    ) -> Arc<TransferItem> {
        self.start_upload_with(source, remote_path, n_streams, TransferPolicies::default())
            .await
    }

    /// `start_upload` under explicit existing-entry / error policies.
    pub async fn start_upload_with(
        &mut self,
        source: Arc<RwLock<dyn StorageSource>>,
        remote_path: String,
        n_streams: usize,
        policies: TransferPolicies,
    ) -> Arc<TransferItem> {
        // A new item is starting on this (possibly reused) transfer: un-latch
        // the pause/cancel flag from the previous run. It never resets
        // otherwise, so a resumed upload's next pause() would hit the
        // already-cancelled early-return and leave the live item running
        // behind an idle row (same fix as `start_download`).
        // Uploads have no selection UI — a non-empty path maps to a one-root
        // selection, "" to the whole source (signature unchanged on purpose).
        let selection = if remote_path.is_empty() {
            None
        } else {
            Some(vec![remote_path])
        };
        self.start_upload_with_root(source, selection, n_streams, policies, "")
            .await
    }

    /// `start_upload_with` with an explicit selection and the item's
    /// `source_root` (see `TransferItem::with_source_root`): a file-manager
    /// copy into a share lands the selection at the share root by basename.
    pub async fn start_upload_with_root(
        &mut self,
        source: Arc<RwLock<dyn StorageSource>>,
        selection: Option<Vec<String>>,
        n_streams: usize,
        policies: TransferPolicies,
        source_root: &str,
    ) -> Arc<TransferItem> {
        self.is_cancelled.store(false, Ordering::SeqCst);
        let dest = self.dest.clone().expect("no dest!");
        let item = Arc::new(
            TransferItem::new(source, dest, selection, n_streams)
                .await
                .with_policies(policies)
                .with_source_root(source_root),
        );
        self.items.lock().unwrap().push(item.clone());
        item
    }

    pub async fn do_upload(&mut self, item: Arc<TransferItem>, close_dest: bool) -> TransferItemState {
        let res = self.run_upload(item.clone(), close_dest).await;
        // The run is over — retire the item from the shared list so a dead
        // item doesn't keep counting its bytes into progress() and the
        // session stats ticker on the next (resumed) run.
        self.items.lock().unwrap().retain(|i| !Arc::ptr_eq(i, &item));
        res
    }

    async fn run_upload(&mut self, item: Arc<TransferItem>, close_dest: bool) -> TransferItemState {
        let urltoken = self.urltoken.clone().unwrap_or_default();
        let transfer_id = self.transfer_id.clone().unwrap_or_default();
        // The item is starting — its finalization hasn't happened yet.
        self.finalized.store(false, Ordering::SeqCst);

        // A session the backend has already forgotten can't open a file
        // transfer — say so without the round trip; the app reopens.
        if self.is_session_expired() {
            self.error = Some(StorageError::SessionExpired(format!(
                "transfer session {transfer_id} expired"
            )));
            if close_dest
                && let Some(dest) = self.dest.clone()
                && let Err(ce) = dest.read().await.close().await
            {
                tracing::error!("dest close failed during error handling: {}", ce);
            }
            return TransferItemState::Error;
        }

        // Open the per-file transfer before running the item. `encrypted_metadata`
        // is a placeholder matching the current TS stub — to become the real
        // share-key-encrypted `{path, estimated_bytes}` blob.
        let start_res = {
            let back = self.back.read().await;
            back.file_start(false, &urltoken, &transfer_id, b"Ymxh".to_vec(), None, None)
                .await
        };
        let file_transfer_id = match start_res {
            Ok(id) => id,
            Err(e) => {
                tracing::error!("file_start failed: {}", e);
                // Classified like the download side: the app reopens on
                // `SessionExpired` and waits out `Transport`.
                self.error = Some(if e.is_session_expired() {
                    StorageError::SessionExpired(e.to_string())
                } else if e.is_transport() {
                    StorageError::Transport(e.to_string())
                } else {
                    StorageError::Other(e.to_string())
                });
                if close_dest
                    && let Some(dest) = self.dest.clone()
                    && let Err(ce) = dest.read().await.close().await
                {
                    tracing::error!("dest close failed during error handling: {}", ce);
                }
                return TransferItemState::Error;
            }
        };

        let result = item.start().await;
        self.end_ts = chrono::Utc::now().timestamp_millis();
        let secs_elapsed = (self.end_ts - self.start_ts) as f64 / 1000.0;

        match result {
            Ok(()) => {
                if item.is_cancelled() {
                    tracing::debug!("upload cancelled");
                    self.end_file_transfer(&file_transfer_id, Some("cancelled")).await;
                    return TransferItemState::Cancelled;
                }
                let stats = item.stats().snapshot();
                if let Some(dest) = self.dest.clone() {
                    if close_dest {
                        if let Err(e) = dest.read().await.close().await {
                            tracing::error!("dest close failed: {}", e);
                            let emsg = e.to_string();
                            self.error = Some(e);
                            self.end_file_transfer(&file_transfer_id, Some(&emsg)).await;
                            return TransferItemState::Error;
                        }
                        tracing::debug!("dest closed");
                    } else {
                        if let Err(e) = dest.read().await.flush().await {
                            tracing::error!("dest flush failed: {}", e);
                            let emsg = e.to_string();
                            self.error = Some(e);
                            self.end_file_transfer(&file_transfer_id, Some(&emsg)).await;
                            return TransferItemState::Error;
                        }
                        tracing::debug!("dest flushed");
                    }
                }
                self.finalized.store(true, Ordering::SeqCst);
                tracing::debug!(
                    "upload complete, took {:.2} sec, avg {:.2} MB/s",
                    secs_elapsed,
                    stats.total_bytes as f64 / secs_elapsed / 1024.0 / 1024.0
                );
                self.end_file_transfer(&file_transfer_id, None).await;
                TransferItemState::Finished
            }
            Err(e) => {
                if item.is_cancelled() {
                    self.end_file_transfer(&file_transfer_id, Some("cancelled")).await;
                    return TransferItemState::Cancelled;
                }
                let stats = item.stats().snapshot();
                tracing::debug!(
                    "upload failed, took {:.2} sec, avg {:.2} MB/s: {}",
                    secs_elapsed,
                    stats.total_bytes as f64 / secs_elapsed / 1024.0 / 1024.0,
                    e
                );
                let emsg = e.to_string();
                self.error = Some(e);
                if close_dest && let Some(dest) = self.dest.clone() {
                    if let Err(ce) = dest.read().await.close().await {
                        tracing::error!("dest close failed during error handling: {}", ce);
                    }
                    tracing::debug!("dest closed");
                }
                self.end_file_transfer(&file_transfer_id, Some(&emsg)).await;
                TransferItemState::Error
            }
        }
    }

    async fn end_file_transfer(&self, file_transfer_id: &str, error: Option<&str>) {
        let urltoken = self.urltoken.clone().unwrap_or_default();
        let res = {
            let back = self.back.read().await;
            back.file_end(false, &urltoken, file_transfer_id, error.is_none(), None, error)
                .await
        };
        if let Err(e) = res {
            tracing::error!("file_end failed: {}", e);
        }
    }

    /// Finish transfer and send end stats. Mirrors TS `finishTransfer`.
    pub async fn finish_transfer(&mut self, error: Option<&str>) {
        self.stop_update_timer();

        if let Err(e) = self.send_end_transfer(error).await {
            tracing::debug!("failed to send end transfer: {}", e);
        }
    }

    pub fn handle(&self) -> TransferHandle {
        TransferHandle {
            items: self.items.clone(),
            is_cancelled: self.is_cancelled.clone(),
            finalized: self.finalized.clone(),
        }
    }

    pub async fn pause(&self) {
        self.handle().pause().await;
    }

    pub async fn cancel(&self) {
        self.handle().cancel().await;
    }

    pub fn is_cancelled(&self) -> bool {
        self.is_cancelled.load(Ordering::SeqCst)
    }

    /// Start the session loop once per session (idempotent; started at dest
    /// creation).
    fn start_update_timer(&mut self) {
        if self.timer_handle.lock().unwrap().is_some() {
            return;
        }
        let (Some(urltoken), Some(transfer_id)) = (self.urltoken.clone(), self.transfer_id.clone()) else {
            return;
        };
        tracing::debug!("transfer timer start");

        let back = self.back.clone();
        let items = self.items.clone();
        let dest_data_source_id = self.dest_data_source_id.clone();
        let dest = self.dest.clone();
        let start_ts = self.start_ts;
        let session_expired = self.session_expired.clone();

        // Same loop as `TransferDownload::start_update_timer`.
        let handle = tokio::spawn(async move {
            let mut state = back.read().await.state_watch();
            let mut idle = Duration::ZERO;
            loop {
                let _ = state.wait_for(|s| s.is_online()).await;
                tokio::time::sleep(UPDATE_INTERVAL).await;
                if !state.borrow().is_online() {
                    continue;
                }
                let snapshot = items.lock().unwrap().clone();
                idle += UPDATE_INTERVAL;
                if snapshot.is_empty() && idle < KEEPALIVE_INTERVAL {
                    continue;
                }
                idle = Duration::ZERO;
                let back_stats = compute_transfer_stats(&snapshot, start_ts).await;
                let ds_stats = dest_ds_stats(&dest_data_source_id, &dest).await;

                let back = back.read().await;
                match back
                    .transfer_update(false, &urltoken, &transfer_id, &back_stats, &ds_stats)
                    .await
                {
                    Ok(()) => {}
                    Err(e) if e.is_session_expired() => {
                        tracing::info!(transfer_id = %transfer_id, "transfer session expired: {}", e);
                        session_expired.store(true, Ordering::SeqCst);
                        break;
                    }
                    Err(e) => tracing::error!("transfer_update failed: {}", e),
                }
            }
        });

        *self.timer_handle.lock().unwrap() = Some(handle);
    }

    fn stop_update_timer(&self) {
        if let Some(handle) = self.timer_handle.lock().unwrap().take() {
            handle.abort();
        }
    }

    async fn send_end_transfer(&self, error: Option<&str>) -> Result<(), String> {
        let snapshot = self.items.lock().unwrap().clone();
        let back_stats = compute_transfer_stats(&snapshot, self.start_ts).await;
        let ds_stats = dest_ds_stats(&self.dest_data_source_id, &self.dest).await;

        if let (Some(urltoken), Some(transfer_id)) = (&self.urltoken, &self.transfer_id) {
            let back = self.back.read().await;
            back.transfer_end(false, urltoken, transfer_id, &back_stats, &ds_stats, error)
                .await
                .map_err(|e| e.to_string())?;
        }

        Ok(())
    }
}

impl Drop for TransferUpload {
    fn drop(&mut self) {
        self.stop_update_timer();
    }
}

/// Sent-byte stats for the single upload destination.
async fn dest_ds_stats(
    dest_data_source_id: &Option<String>,
    dest: &Option<Arc<RwLock<dyn StorageDestMultipart>>>,
) -> Vec<TransferDSStat> {
    let mut ds_stats = Vec::new();
    if let (Some(ds_id), Some(dest)) = (dest_data_source_id, dest) {
        let d = dest.read().await;
        ds_stats.push(TransferDSStat {
            datasource_id: ds_id.clone(),
            bytes: d.total_bytes_sent(),
        });
    }
    ds_stats
}

// =====================================================================
// TransferHandle (shared by both directions)
// =====================================================================

#[derive(Clone)]
pub struct TransferHandle {
    items: Arc<Mutex<Vec<Arc<TransferItem>>>>,
    is_cancelled: Arc<AtomicBool>,
    /// Owned by the direction struct: false while a `do_upload`/`do_download`
    /// is in flight, true once its dest close/flush succeeded. Gates the
    /// 100%-display in `progress()`.
    finalized: Arc<AtomicBool>,
}

impl TransferHandle {
    fn items_snapshot(&self) -> Vec<Arc<TransferItem>> {
        self.items.lock().unwrap().clone()
    }

    /// Pause/cancel the items; the session loop is the session's, not the
    /// items' — it goes on (at keepalive cadence once the items retire) until
    /// the transfer closes.
    pub async fn pause(&self) {
        if self.is_cancelled.swap(true, Ordering::SeqCst) {
            return;
        }
        for item in self.items_snapshot() {
            item.cancel(true).await;
        }
    }

    pub async fn cancel(&self) {
        if self.is_cancelled.swap(true, Ordering::SeqCst) {
            return;
        }
        for item in self.items_snapshot() {
            item.cancel(false).await;
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.is_cancelled.load(Ordering::SeqCst)
    }

    /// Keys with in-flight work across the live items — the per-file status
    /// API's `Active` set (docs/core/PartialDownloads.md).
    pub fn active_keys(&self) -> std::collections::HashSet<String> {
        let mut keys = std::collections::HashSet::new();
        for item in self.items_snapshot() {
            keys.extend(item.active_keys());
        }
        keys
    }

    /// Per-selection-root sized byte totals across the live items — the
    /// status API's percentage source for selection-root dirs.
    pub fn root_totals(&self) -> std::collections::HashMap<String, u64> {
        let mut totals = std::collections::HashMap::new();
        for item in self.items_snapshot() {
            totals.extend(item.root_totals());
        }
        totals
    }

    /// Bytes the live items have landed under dir `key` — the status API's
    /// dir `bytes_done` while a run is live (no disk involved).
    pub fn dir_bytes_for(&self, key: &str) -> u64 {
        self.items_snapshot().iter().map(|item| item.dir_bytes_for(key)).sum()
    }

    pub async fn progress(&self) -> TransferProgress {
        let mut agg = TransferProgress::default();
        for item in self.items_snapshot() {
            let p = item.progress().await;
            agg.bytes += p.bytes;
            agg.total_bytes += p.total_bytes;
            agg.bytes_per_sec += p.bytes_per_sec;
            agg.files_written += p.files_written;
            agg.dirs_written += p.dirs_written;
            agg.preparing |= p.preparing;
            agg.total_files += p.total_files;
        }
        agg.ratio = if agg.total_bytes > 0 {
            agg.bytes as f64 / agg.total_bytes as f64
        } else {
            0.0
        };
        // Never show 100% before finalization (final zip/journal/meta writes,
        // dest close) is done — hold at 99% until `do_upload` flips the flag.
        if !self.finalized.load(Ordering::SeqCst) {
            agg.ratio = agg.ratio.min(0.99);
        }
        agg.eta_seconds = if agg.total_bytes > 0 && agg.bytes_per_sec > 0.0 {
            Some((agg.total_bytes.saturating_sub(agg.bytes) as f64 / agg.bytes_per_sec).ceil() as u64)
        } else {
            None
        };
        agg
    }
}

#[cfg(test)]
impl TransferUpload {
    /// Tests: an upload over an already-"opened" session (no backend
    /// connect), dest injected, with the session loop running — the
    /// keepalive tests.
    pub(crate) fn for_session_test(
        back: Arc<RwLock<Back>>,
        urltoken: &str,
        transfer_id: &str,
        dest: Option<Arc<RwLock<dyn StorageDestMultipart>>>,
    ) -> Self {
        let mut t = Self::new(back);
        t.urltoken = Some(urltoken.to_string());
        t.transfer_id = Some(transfer_id.to_string());
        t.start_ts = chrono::Utc::now().timestamp_millis();
        t.dest = dest;
        t.start_update_timer();
        t
    }
}

#[cfg(test)]
impl TransferDownload {
    /// Tests: a transfer over a ready `MultiSource` (no connect), with the
    /// session loop running — the keepalive tests.
    pub(crate) fn with_multi_for_test(
        back: Arc<RwLock<Back>>,
        urltoken: &str,
        multi: Arc<RwLock<MultiSource>>,
    ) -> Self {
        let mut t = Self::new(back);
        t.urltoken = Some(urltoken.to_string());
        t.start_ts = chrono::Utc::now().timestamp_millis();
        t.multi = Some(multi.clone());
        t.source = Some(multi);
        t.start_update_timer();
        t
    }
}

/// The session loop (docs/app/reconnect.md, change set 2), under paused
/// tokio time against `MockBackend`.
#[cfg(test)]
mod session_loop_tests {
    use super::*;
    use crate::back::BackState;
    use crate::back::test_support::{Answer, MockBackend, advance, settle};
    use crate::transfers::multi_source::SessionParams;

    async fn transfer(
        mock: &MockBackend,
        state: BackState,
    ) -> (Arc<RwLock<Back>>, Arc<RwLock<MultiSource>>, TransferDownload) {
        let back = mock.back_in(state);
        let ms = MultiSource::new(None, None, None);
        ms.bind_session_for_test(SessionParams {
            back: back.clone(),
            urltoken: "tok".into(),
            key: None,
            ninja_params: NinjaStorageV2Params::default(),
        });
        let multi = Arc::new(RwLock::new(ms));
        let t = TransferDownload::with_multi_for_test(back.clone(), "tok", multi.clone());
        (back, multi, t)
    }

    #[tokio::test(start_paused = true)]
    async fn idle_source_pings_once_a_minute() {
        let mock = MockBackend::start().await;
        let (_back, _multi, _t) = transfer(&mock, BackState::Online).await;
        advance(59).await;
        assert_eq!(mock.count("download/update"), 0);
        advance(1).await;
        mock.wait_count("download/update", 1).await;
        advance(120).await;
        mock.wait_count("download/update", 3).await;
        assert_eq!(mock.count("download/update"), 3);
        for c in mock.calls("download/update") {
            assert_eq!(c.body["transfer_id"], "tid-0");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn loop_waits_for_online() {
        let mock = MockBackend::start().await;
        let (back, _multi, _t) = transfer(&mock, BackState::NoConnection).await;
        advance(300).await;
        assert_eq!(
            mock.count("download/update"),
            0,
            "no pings into a backend that is not there"
        );
        back.read().await.set_state(BackState::Online);
        advance(60).await;
        mock.wait_count("download/update", 1).await;
    }

    #[tokio::test(start_paused = true)]
    async fn not_found_marks_the_source_and_stops_the_loop() {
        let mock = MockBackend::start().await;
        mock.script("download/update", &[Answer::NotFound]);
        let (_back, multi, t) = transfer(&mock, BackState::Online).await;
        advance(60).await;
        mock.wait_count("download/update", 1).await;
        settle().await;
        assert!(multi.read().await.is_expired(), "the 404 marks the session gone");
        advance(300).await;
        assert_eq!(mock.count("download/update"), 1, "no more pings on a dead session");
        // Closing a transfer whose session is gone sends no `transfer_end`.
        MockBackend::with_io(t.close(false)).await;
        settle().await;
        assert_eq!(mock.count("download/end"), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn other_errors_do_not_stop_the_loop() {
        let mock = MockBackend::start().await;
        mock.script("download/update", &[Answer::ServerError]);
        let (_back, multi, _t) = transfer(&mock, BackState::Online).await;
        advance(120).await;
        mock.wait_count("download/update", 2).await;
        assert!(!multi.read().await.is_expired());
    }

    #[tokio::test(start_paused = true)]
    async fn close_on_a_live_session_ends_it() {
        let mock = MockBackend::start().await;
        let (_back, _multi, t) = transfer(&mock, BackState::Online).await;
        // Awaited inline under paused time: without real IO the request
        // would be jumped past by its own timeout.
        MockBackend::with_io(t.close(false)).await;
        assert_eq!(mock.count("download/end"), 1);
        let end = &mock.calls("download/end")[0];
        assert_eq!(end.body["transfer_id"], "tid-0");
    }
}

/// The download/upload runs against the session machinery (docs/app/reconnect.md,
/// change set 2): a marked session is reopened — or refused — before
/// `file_start`, and a clock jump yields one late update, not a burst.
#[cfg(test)]
mod session_run_tests {
    use super::*;
    use crate::back::BackState;
    use crate::back::test_support::{Answer, MockBackend, advance, settle};
    use crate::transfers::TransferItemState;
    use crate::transfers::multi_source::SessionParams;
    use crate::transfers::test_support::{InMemDest, InMemDestProxy, InMemSource, files};

    async fn multi(mock: &MockBackend, back: &Arc<RwLock<Back>>) -> Arc<RwLock<MultiSource>> {
        let src: Arc<RwLock<dyn StorageSource>> = Arc::new(RwLock::new(InMemSource::new(files(&[("f", b"DATA")]))));
        let ms = MultiSource::new(Some(src), None, None);
        ms.bind_session_for_test(SessionParams {
            back: back.clone(),
            urltoken: "tok".into(),
            key: None,
            ninja_params: NinjaStorageV2Params::default(),
        });
        let multi = Arc::new(RwLock::new(ms));
        multi.write().await.connect().await.unwrap();
        let _ = mock; // the session is bound to the mock through `back`
        multi
    }

    fn dest() -> (Arc<InMemDest>, Arc<RwLock<dyn StorageDestMultipart>>) {
        let inner = Arc::new(InMemDest::new());
        let proxy: Arc<RwLock<dyn StorageDestMultipart>> = Arc::new(RwLock::new(InMemDestProxy(inner.clone())));
        (inner, proxy)
    }

    /// A session the loop knows is gone is reopened before the file transfer
    /// is opened on it: one `download/start`, `files/start` on the new id,
    /// never a call on the old one.
    #[tokio::test]
    async fn do_download_on_marked_session_reopens_before_file_start() {
        let mock = MockBackend::start().await;
        let back = mock.back();
        let m = multi(&mock, &back).await;
        let t = TransferDownload::with_multi_for_test(back, "tok", m.clone());
        m.read().await.mark_expired();

        let (inner, d) = dest();
        let item = t.start_download(None, d, 1).await;
        let (state, err) = t.do_download(item, false).await;
        assert_eq!(state, TransferItemState::Finished, "err: {err:?}");
        assert_eq!(
            inner.snapshot().await.get("f").map(|v| v.as_slice()),
            Some(b"DATA".as_slice())
        );

        assert_eq!(mock.count("download/start"), 1);
        let starts = mock.calls("download/files");
        assert_eq!(starts.len(), 1);
        assert_eq!(
            starts[0].body["transfer_id"], "tid-1",
            "the file transfer opens on the reopened session"
        );
        t.close(false).await;
    }

    /// No session bound (injected members only): the run is refused before
    /// `file_start` with the expired-session error.
    #[tokio::test]
    async fn do_download_without_a_session_is_refused_before_file_start() {
        let mock = MockBackend::start().await;
        let back = mock.back();
        let src: Arc<RwLock<dyn StorageSource>> = Arc::new(RwLock::new(InMemSource::new(files(&[("f", b"DATA")]))));
        let ms = MultiSource::new(Some(src), None, None);
        let m = Arc::new(RwLock::new(ms));
        m.write().await.connect().await.unwrap();
        let t = TransferDownload::with_multi_for_test(back, "tok", m.clone());
        m.read().await.mark_expired();

        let (_inner, d) = dest();
        let item = t.start_download(None, d, 1).await;
        let (state, err) = t.do_download(item, false).await;
        assert_eq!(state, TransferItemState::Error);
        assert!(matches!(err, Some(StorageError::SessionExpired(_))), "got {err:?}");
        assert_eq!(mock.count("download/files"), 0);
        assert_eq!(mock.count("download/start"), 0);
    }

    /// The 5 s cadence is sleep-paced: a 30-minute clock jump yields one late
    /// update, never a catch-up burst.
    #[tokio::test(start_paused = true)]
    async fn ticker_does_not_burst_after_a_clock_jump() {
        let mock = MockBackend::start().await;
        let back = mock.back();
        let m = multi(&mock, &back).await;
        let t = TransferDownload::with_multi_for_test(back, "tok", m);
        // An item on the list = progress cadence (5 s), no run needed.
        let (_inner, d) = dest();
        let _item = t.start_download(None, d, 1).await;

        advance(10).await;
        let before = mock.count("download/update");
        assert_eq!(before, 2);

        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(1800)).await;
        settle().await;
        assert_eq!(mock.count("download/update"), before + 1, "one late update, not 360");
        t.close(false).await;
    }

    // ── upload side ────────────────────────────────────────────────────────

    /// Same keepalive as the download side: one ping a minute while idle.
    #[tokio::test(start_paused = true)]
    async fn upload_idle_session_pings_once_a_minute() {
        let mock = MockBackend::start().await;
        let back = mock.back();
        let _t = TransferUpload::for_session_test(back, "tok", "tid-0", None);
        advance(59).await;
        assert_eq!(mock.count("upload/update"), 0);
        advance(1).await;
        mock.wait_count("upload/update", 1).await;
        advance(120).await;
        mock.wait_count("upload/update", 3).await;
        for c in mock.calls("upload/update") {
            assert_eq!(c.body["transfer_id"], "tid-0");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn upload_loop_waits_for_online() {
        let mock = MockBackend::start().await;
        let back = mock.back_in(BackState::NoConnection);
        let _t = TransferUpload::for_session_test(back.clone(), "tok", "tid-0", None);
        advance(300).await;
        assert_eq!(
            mock.count("upload/update"),
            0,
            "no pings into a backend that is not there"
        );
        back.read().await.set_state(BackState::Online);
        advance(60).await;
        mock.wait_count("upload/update", 1).await;
    }

    /// `transfer_not_found` flags the upload and stops the loop; `do_upload`
    /// then refuses before `files/start` — a fresh run is a fresh session.
    #[tokio::test(start_paused = true)]
    async fn upload_not_found_flags_stops_and_refuses_the_run() {
        let mock = MockBackend::start().await;
        mock.script("upload/update", &[Answer::NotFound]);
        let back = mock.back();
        let (_inner, d) = dest();
        let mut t = TransferUpload::for_session_test(back, "tok", "tid-0", Some(d));
        advance(60).await;
        mock.wait_count("upload/update", 1).await;
        settle().await;
        assert!(t.is_session_expired(), "the 404 flags the session");
        advance(300).await;
        assert_eq!(mock.count("upload/update"), 1, "no more pings on a dead session");

        let src: Arc<RwLock<dyn StorageSource>> = Arc::new(RwLock::new(InMemSource::new(files(&[("f", b"DATA")]))));
        let item = t.start_upload(src, String::new(), 1).await;
        let state = crate::back::test_support::MockBackend::with_io(t.do_upload(item, false)).await;
        assert_eq!(state, TransferItemState::Error);
        assert!(
            matches!(t.error, Some(StorageError::SessionExpired(_))),
            "got {:?}",
            t.error
        );
        assert_eq!(mock.count("upload/files"), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn upload_other_errors_do_not_stop_the_loop() {
        let mock = MockBackend::start().await;
        mock.script("upload/update", &[Answer::ServerError]);
        let back = mock.back();
        let t = TransferUpload::for_session_test(back, "tok", "tid-0", None);
        advance(120).await;
        mock.wait_count("upload/update", 2).await;
        assert!(!t.is_session_expired());
    }
}
