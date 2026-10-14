//! MultiSource — combines a Ninja/S3 source and a P2P source with stream-and-
//! failover get, a timer-gated coalesced meta refresh, and P2P reconnect/backoff.
//! Rust port of the Electron `MultiSource` (post-Megasync streaming design).
//!
//! Shared state lives in [`MultiInner`] behind an `Arc` so the streaming
//! `get_streamed_v2` producer task can drive the failover loop (`get_int`) with
//! `'static` access to the sources, flags and refresh gate.

use async_trait::async_trait;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{Notify, RwLock, mpsc};

/// Pause before retrying a leg whose wire died while the backend was Online.
const TRANSPORT_RETRY_PAUSE: Duration = Duration::from_secs(1);

use super::preview_state::PreviewState;
use super::{
    ListUpdatesOptions, ListUpdatesResult, P2PStorage, PreviewImage, S3Storage, SearchOptions, SearchResult, Storage,
    StorageChange, StorageDirStats, StorageError, StorageGetOptions, StorageMetaState, StorageObjectInfo,
    StorageSearchQuery, StorageSource, StorageSourceType, StorageStatInfo,
};
use crate::back::{Back, LinkDataSource, TransferDSStat, TransferStartAnswer};
use crate::ninja::{NinjaStorageV2, NinjaStorageV2Params, StorageSourceAndDest};

const META_REFRESH_MS: u64 = 5_000;
const P2P_BACKOFF_INITIAL_MS: u64 = 5_000;
const P2P_BACKOFF_MAX_MS: u64 = 30_000;
const P2P_MAX_FAILS: u32 = 5;
/// Longest a preview request waits for a P2P member that is still dialing.
const PREVIEW_P2P_WAIT: Duration = Duration::from_secs(10);

/// Error when no source can serve a request.
#[derive(Debug)]
#[allow(dead_code)]
pub struct NoSourceError(pub String);

impl std::fmt::Display for NoSourceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "No source for {}", self.0)
    }
}

impl std::error::Error for NoSourceError {}

/// Parameters for MultiSource
#[derive(Debug, Clone)]
pub struct MultiSourceParams {
    /// When both sources are up, serve `get` from P2P and skip Ninja/S3 (P2P is
    /// the live, always-complete source). Mirrors the TS `preferP2P` (default
    /// false: Ninja serves first, P2P covers what Ninja can't serve yet).
    pub prefer_p2p: bool,
    /// Compose the P2P member for a hybrid (cloud + P2P) session. Off drops
    /// the P2P datasource when the session also offers the cloud one; a
    /// P2P-only session keeps it. A debug switch (desktop Settings).
    pub hybrid_p2p: bool,
}

impl Default for MultiSourceParams {
    fn default() -> Self {
        Self { prefer_p2p: false, hybrid_p2p: true }
    }
}

impl MultiSourceParams {
    pub fn new() -> Self {
        Self::default()
    }
}

/// A member source plus its shared active flag (shared so background tasks and
/// `&self` methods can flip it — mirrors the TS `MultiStorageState.isActive`).
struct MemberSource {
    src: Arc<RwLock<dyn StorageSource>>,
    is_active: Arc<AtomicBool>,
    /// Last successful reads of the member's byte counters, keyed by getter —
    /// returned when the non-blocking `try_read` hits lock contention, so a
    /// stats sample repeats the previous value instead of contributing 0.
    last_total_recv: AtomicU64,
    last_total_sent: AtomicU64,
}

impl MemberSource {
    fn new(src: Arc<RwLock<dyn StorageSource>>, is_active: Arc<AtomicBool>) -> Self {
        Self {
            src,
            is_active,
            last_total_recv: AtomicU64::new(0),
            last_total_sent: AtomicU64::new(0),
        }
    }

    fn active(&self) -> bool {
        self.is_active.load(Ordering::Relaxed)
    }

    /// Non-blocking counter sample with a last-known-good fallback (see the
    /// `last_*` field docs).
    fn sample(&self, cache: &AtomicU64, get: impl FnOnce(&dyn StorageSource) -> u64) -> u64 {
        match self.src.try_read() {
            Ok(g) => {
                let v = get(&*g);
                cache.store(v, Ordering::Relaxed);
                v
            }
            Err(_) => cache.load(Ordering::Relaxed),
        }
    }
}

/// Shared P2P reconnect state — drives the fail-cap + exponential backoff timer.
/// Mirrors the TS `onP2PError`/`onP2PConnected`/`startP2PTimer` triad. Held in an
/// `Arc` so the disconnect callback and the delayed reconnect task can drive it.
struct P2PCtl {
    src: Arc<RwLock<dyn StorageSource>>,
    is_active: Arc<AtomicBool>,
    is_canceled: Arc<AtomicBool>,
    /// Shared with `MultiInner`: the backend forgot the transfer session
    /// (a reconnect answered `SessionExpired`). Terminal for this source —
    /// every call answers `SessionExpired` so the holder reopens.
    session_expired: Arc<AtomicBool>,
    fail_count: AtomicU32,
    backoff_ms: AtomicU64,
    timer: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Shared with MultiSource's refresh gate: a reconnect wakes any in-flight
    /// `refresh_delay` early so a waiting `get` retries against the freshly-online
    /// P2P source immediately (TS `refreshWake`).
    refresh_wake: Arc<Notify>,
}

impl P2PCtl {
    fn on_connected(&self) {
        if self.is_canceled.load(Ordering::Relaxed) {
            return;
        }
        tracing::debug!("p2p is now connected");
        self.backoff_ms.store(P2P_BACKOFF_INITIAL_MS, Ordering::Relaxed);
        self.is_active.store(true, Ordering::Relaxed);
        // a source just came online — cut any in-flight refresh wait short
        self.refresh_wake.notify_waiters();
    }

    /// The reconnect learned the session is gone: no more attempts (they
    /// would all answer the same), flag it, and wake any waiting `get` so it
    /// sees the flag instead of the refresh timeout.
    fn on_session_expired(&self, e: &str) {
        if self.is_canceled.load(Ordering::Relaxed) {
            return;
        }
        tracing::info!("p2p reconnect: transfer session expired: {}", e);
        self.is_active.store(false, Ordering::Relaxed);
        self.session_expired.store(true, Ordering::Relaxed);
        self.refresh_wake.notify_waiters();
    }

    fn on_error(self: &Arc<Self>) {
        if self.is_canceled.load(Ordering::Relaxed) {
            return;
        }
        self.is_active.store(false, Ordering::Relaxed);
        let fails = self.fail_count.fetch_add(1, Ordering::Relaxed) + 1;
        if fails < P2P_MAX_FAILS {
            self.start_timer();
        } else {
            tracing::debug!("p2p reached max fail count");
        }
    }

    /// Schedule a single reconnect attempt after the current backoff, doubling it
    /// (capped) for next time. No-op if a timer is already pending.
    fn start_timer(self: &Arc<Self>) {
        let mut guard = self.timer.lock().unwrap();
        if guard.is_some() {
            return;
        }
        let backoff = self.backoff_ms.load(Ordering::Relaxed);
        tracing::debug!("starting p2p timer, retrying in {}ms", backoff);
        let ctl = self.clone();
        let handle = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(backoff)).await;
            *ctl.timer.lock().unwrap() = None;
            if ctl.is_canceled.load(Ordering::Relaxed) {
                return;
            }
            let res = { ctl.src.write().await.connect().await };
            match res {
                Ok(_) => ctl.on_connected(),
                Err(StorageError::SessionExpired(e)) => ctl.on_session_expired(&e),
                Err(_) => ctl.on_error(),
            }
        });
        *guard = Some(handle);
        self.backoff_ms
            .store((backoff * 2).min(P2P_BACKOFF_MAX_MS), Ordering::Relaxed);
    }
}

/// What it takes to open the backend transfer session (`download_start`) and
/// build the members from its answer: `MultiSource::open`.
pub struct SessionParams {
    pub back: Arc<RwLock<Back>>,
    pub urltoken: String,
    /// The share key — the P2P member's session key and the ninja member's
    /// master key (already folded into `ninja_params`).
    pub key: Option<[u8; 32]>,
    pub ninja_params: NinjaStorageV2Params,
}

/// The session this source rides on, plus the two member objects that carry
/// session state: the S3 member's presigned manifest and the P2P member's
/// `transfer_id`. A reopen (`MultiInner::reopen`) does `download_start` again
/// and hands them the new values — nothing is rebuilt, whoever holds the
/// source keeps the same live objects. Absent for injected members (tests):
/// a lapse then surfaces as `SessionExpired`.
struct SessionInfo {
    params: SessionParams,
    s3: Option<Arc<RwLock<S3Storage>>>,
    p2p: Option<Arc<RwLock<P2PStorage>>>,
}

/// `download_start`, after the backend is there. The one place the session
/// is opened — first time (`MultiSource::open`) and again (`reopen`).
async fn start_session(
    params: &SessionParams,
    canceled: &AtomicBool,
    wake: &Notify,
) -> Result<TransferStartAnswer, StorageError> {
    let mut state = params.back.read().await.state_watch();
    loop {
        if canceled.load(Ordering::Relaxed) {
            return Err(StorageError::Cancelled);
        }
        tokio::select! {
            _ = state.wait_for(|st| st.is_online()) => break,
            _ = wake.notified() => {}
        }
    }
    params
        .back
        .read()
        .await
        .download_start(&params.urltoken)
        .await
        .map_err(|e| {
            if e.is_transport() {
                StorageError::Transport(e.to_string())
            } else {
                StorageError::Other(e.to_string())
            }
        })
}

/// Shared MultiSource state — held behind an `Arc` so the streaming producer task
/// can run the failover loop with `'static` access. All `&self` failover logic
/// lives here; [`MultiSource`] is a thin wrapper that adds the (mutable) data
/// source id and forwards trait calls.
struct MultiInner {
    src_ninja: Option<MemberSource>,
    src_p2p: Option<MemberSource>,
    p2p_ctl: Option<Arc<P2PCtl>>,
    params: MultiSourceParams,
    is_canceled: Arc<AtomicBool>,
    /// The session is gone (a member answered `SessionExpired`, the P2P
    /// reconnect learned it, or the keepalive heard the 404): the next entry
    /// point reopens before touching it. See `P2PCtl::session_expired`.
    session_expired: Arc<AtomicBool>,
    session: std::sync::OnceLock<SessionInfo>,
    transfer_id: std::sync::Mutex<Option<String>>,
    /// Single-flight reopen.
    reopen_lock: tokio::sync::Mutex<()>,
    /// Refresh gate (TS `refreshGate`): `refresh_lock` admits one refresh at a time;
    /// `last_refresh` is the completion time used to coalesce concurrent followers
    /// (a call skips iff a refresh finished *after* it started waiting).
    refresh_lock: tokio::sync::Mutex<()>,
    last_refresh: std::sync::Mutex<Option<Instant>>,
    refresh_wake: Arc<Notify>,
    /// The cache of the peer's thumbnails (`preview_state`).
    previews: PreviewState,
}

impl MultiInner {
    fn p2p_active(&self) -> bool {
        self.src_p2p.as_ref().map(|p| p.active()).unwrap_or(false)
    }

    fn ninja_active(&self) -> bool {
        self.src_ninja.as_ref().map(|n| n.active()).unwrap_or(false)
    }

    fn is_canceled(&self) -> bool {
        self.is_canceled.load(Ordering::Relaxed)
    }

    /// A lapsed session is reopened here, before the call that needs it.
    async fn ensure_session(&self) -> Result<(), StorageError> {
        if self.session_expired.load(Ordering::Relaxed) {
            self.reopen().await?;
        }
        Ok(())
    }

    /// Single-flight. `download_start` again; the S3 member takes the new
    /// manifest, the P2P member the new `transfer_id` and redials (an optional
    /// leg — a peer that is gone is not a failed reopen). Nothing is rebuilt.
    async fn reopen(&self) -> Result<(), StorageError> {
        let _guard = self.reopen_lock.lock().await;
        if !self.session_expired.load(Ordering::Relaxed) {
            return Ok(()); // the caller that won the race already did it
        }
        let Some(s) = self.session.get() else {
            return Err(StorageError::SessionExpired("transfer session expired".into()));
        };
        let tsa = start_session(&s.params, &self.is_canceled, &self.refresh_wake).await?;
        tracing::info!(urltoken = %s.params.urltoken, transfer_id = %tsa.transfer_id, "transfer session reopened");
        *self.transfer_id.lock().unwrap() = Some(tsa.transfer_id.clone());
        for ds in &tsa.datasources {
            match ds {
                LinkDataSource::S3(s3_ds) => {
                    if let Some(s3) = &s.s3 {
                        s3.read().await.set_presigned_files(s3_ds.presigned_files.clone());
                    }
                }
                LinkDataSource::P2P(_) => {
                    if let (Some(p2p), Some(ctl)) = (&s.p2p, &self.p2p_ctl) {
                        if let Some(h) = ctl.timer.lock().unwrap().take() {
                            h.abort();
                        }
                        ctl.fail_count.store(0, Ordering::Relaxed);
                        match p2p.write().await.reconnect(tsa.transfer_id.clone()).await {
                            Ok(()) => ctl.on_connected(),
                            Err(e) => {
                                tracing::debug!("p2p redial on reopened session failed: {}", e);
                                ctl.on_error();
                            }
                        }
                    }
                }
            }
        }
        self.session_expired.store(false, Ordering::Relaxed);
        self.refresh_wake.notify_waiters();
        Ok(())
    }

    /// Wait for the backend to be `Online`; `cancel()` / `cleanup()` end it
    /// (they notify `refresh_wake`).
    async fn wait_online(&self, s: &SessionInfo) -> Result<(), StorageError> {
        let mut state = s.params.back.read().await.state_watch();
        loop {
            if self.is_canceled() {
                return Err(StorageError::Cancelled);
            }
            tokio::select! {
                _ = state.wait_for(|st| st.is_online()) => return Ok(()),
                _ = self.refresh_wake.notified() => {}
            }
        }
    }

    /// Ninja source is active *and* its journal meta is complete — the gate the TS
    /// uses (`metaStats().metaState === Complete`) for getDir/search/getDirStats.
    async fn ninja_complete(&self) -> bool {
        if let Some(n) = &self.src_ninja {
            if !n.active() {
                return false;
            }
            return n
                .src
                .clone()
                .read()
                .await
                .meta_stats()
                .await
                .map(|s| s.meta_state == StorageMetaState::Complete)
                .unwrap_or(false);
        }
        false
    }

    /// Disable the P2P source and schedule a reconnect — mirrors TS `disableSource`
    /// for a P2P member (cancel + `startP2PTimer`).
    async fn disable_p2p(&self) {
        if let Some(ctl) = &self.p2p_ctl
            && ctl.is_active.swap(false, Ordering::Relaxed)
        {
            tracing::debug!("disabling P2P source");
            let _ = ctl.src.read().await.cancel().await;
            ctl.start_timer();
        }
    }

    /// Core streaming get: stream the requested range into `tx`, failing over
    /// between sources, and — when `options.wait_for_parts` — pacing a meta refresh
    /// between retries until the bytes appear. Chunks flow to `tx` as they arrive
    /// from whichever source serves them; a source error mid-stream is swallowed and
    /// the remainder resumes elsewhere (the bytes already delivered stay delivered).
    /// Terminal failures are sent as a final `Err` item. Mirrors the TS `getInt`
    /// generator.
    ///
    /// Errors map to the TS taxonomy: `NoSuchKey` (TS `NoSourceError`) for a file
    /// that exists nowhere / on a complete source that doesn't have it; `NoPartError`
    /// for a multipart byte range not yet available with `wait_for_parts=false`.
    async fn get_int(
        &self,
        object: &str,
        options: &StorageGetOptions,
        offset: Option<u64>,
        length: Option<u64>,
        tx: mpsc::Sender<Result<Vec<u8>, StorageError>>,
    ) {
        let start = offset.unwrap_or(0);
        let mut done: u64 = 0;

        loop {
            if self.is_canceled() {
                let _ = tx
                    .send(Err(StorageError::NoSuchKey(format!("canceled {}", object))))
                    .await;
                return;
            }
            // A lapsed session heals here; the pass repeats from `done`.
            if let Err(e) = self.ensure_session().await {
                let _ = tx.send(Err(e)).await;
                return;
            }
            if tx.is_closed() {
                return; // consumer dropped
            }
            let off = start + done;
            let len = length.map(|l| l - done);
            if let Some(0) = len {
                return; // fully served — closing tx signals clean EOF
            }

            let ninja_active = self.ninja_active();
            let p2p_active = self.p2p_active();
            let ninja_complete = if ninja_active {
                self.ninja_complete().await
            } else {
                false
            };
            let before = done;

            // Ninja/S3 serves the remainder (it sorts out its own parts
            // internally); skipped when P2P is preferred and up — then P2P serves
            // below. Presence is the stream's own first answer: `NoSuchKey`
            // before a byte just means "not on ninja" (fall through).
            let mut on_ninja = false;
            if ninja_active && (!self.params.prefer_p2p || !p2p_active) {
                let src = self.src_ninja.as_ref().unwrap().src.clone();
                let (n, eof, err) = self.stream_from(&src, object, off, len, &tx).await;
                done += n;
                if eof {
                    return;
                }
                on_ninja = !(n == 0 && matches!(err, Some(StorageError::NoSuchKey(_))));
                // A lapse or a dead wire under the leg is not "ran out of
                // sources": with a session bound the next pass reopens
                // (`ensure_session` at the loop top) or retries once the
                // backend is back — bytes already delivered stay delivered.
                if on_ninja && self.session.get().is_some() {
                    match err {
                        Some(StorageError::SessionExpired(_)) => continue,
                        Some(StorageError::Transport(_)) => {
                            if let Some(s) = self.session.get()
                                && self.wait_online(s).await.is_err()
                            {
                                let _ = tx.send(Err(StorageError::Cancelled)).await;
                                return;
                            }
                            // The wire died under the leg while the backend
                            // was fine (a dropped connection, a storage host
                            // refusing): one retry per second, not a loop at
                            // task speed hammering the host.
                            tokio::select! {
                                _ = tokio::time::sleep(TRANSPORT_RETRY_PAUSE) => {}
                                _ = self.refresh_wake.notified() => {}
                            }
                            if self.is_canceled() {
                                let _ = tx.send(Err(StorageError::Cancelled)).await;
                                return;
                            }
                            continue;
                        }
                        _ => {}
                    }
                }
            }

            // P2P: the preferred source, or the fallback when ninja couldn't serve.
            if done == before && self.p2p_active() {
                let src = self.src_p2p.as_ref().unwrap().src.clone();
                let (n, eof, err) = self.stream_from(&src, object, off, len, &tx).await;
                done += n;
                if eof {
                    return;
                }
                if self.session.get().is_some() && matches!(err, Some(StorageError::SessionExpired(_))) {
                    continue; // the loop top reopens; the peer is redialed there
                }
                if n == 0 {
                    // couldn't even start -> drop it (schedules reconnect) and
                    // retry at once: this pass skipped ninja while P2P was up,
                    // the next one probes it and serves the remainder.
                    self.disable_p2p().await;
                    continue;
                }
            }

            if done > before {
                continue;
            }

            // "Not found" is ninja's authoritative answer — valid only when
            // ninja was actually probed this pass, not skipped for a
            // then-active P2P (its absence proves nothing about ninja).
            let ninja_skipped = ninja_active && self.params.prefer_p2p && p2p_active;
            if ninja_active && ninja_complete && !on_ninja && !ninja_skipped {
                let _ = tx
                    .send(Err(StorageError::NoSuchKey(format!("not found {}", object))))
                    .await;
                return;
            }
            if options.wait_for_parts {
                tracing::debug!("wait {} @{}", object, off);
                // The wait is this request's alone: `wait_cancel` ends it
                // without touching the shared source (a pause must not wait
                // for the uploader to produce the range).
                let refreshed = tokio::select! {
                    _ = options.wait_cancel.cancelled() => {
                        let _ = tx
                            .send(Err(StorageError::NoPartError(format!("wait cancelled {} @{}", object, off))))
                            .await;
                        return;
                    }
                    r = self.gated_refresh() => r,
                };
                if let Err(e) = refreshed {
                    let _ = tx.send(Err(e)).await;
                    return;
                }
                continue;
            }
            // No source this pass but the P2P reconnect is still pending (it
            // waits for the backend inside `P2PStorage::connect`): not "ran
            // out of sources" yet — pace on the refresh wait, which the
            // reconnect wakes. Only a peer past the fail cap is gone.
            if let Some(ctl) = &self.p2p_ctl
                && !self.p2p_active()
                && ctl.fail_count.load(Ordering::Relaxed) < P2P_MAX_FAILS
            {
                self.refresh_delay().await;
                continue;
            }
            let _ = tx
                .send(Err(StorageError::NoPartError(format!("unavailable {}", object))))
                .await;
            return;
        }
    }

    /// Stream `[off, len)` from one source into `tx`. Returns the bytes delivered
    /// (`n`) and whether the source ran to its end without error (`eof` = the range
    /// is served). A per-source error is swallowed (logged) so the caller resumes
    /// the remainder elsewhere; a dropped consumer reports `eof=true` so `get_int`
    /// stops. Mirrors the TS `streamFrom`.
    async fn stream_from(
        &self,
        src: &Arc<RwLock<dyn StorageSource>>,
        object: &str,
        off: u64,
        len: Option<u64>,
        tx: &mpsc::Sender<Result<Vec<u8>, StorageError>>,
    ) -> (u64, bool, Option<StorageError>) {
        let mut rx = src.read().await.get_streamed(object, Some(off), len).await;
        let mut n: u64 = 0;
        while let Some(item) = rx.recv().await {
            match item {
                Ok(chunk) => {
                    n += chunk.len() as u64;
                    if tx.send(Ok(chunk)).await.is_err() {
                        return (n, true, None); // consumer gone — make get_int stop
                    }
                }
                Err(e) => {
                    tracing::debug!("source failed {} @{} +{}: {}", object, off, n, e);
                    // The backend transfer session is gone — for the other
                    // leg too. Mark it: the next `get_int` pass reopens and
                    // resumes from `done`.
                    if matches!(e, StorageError::SessionExpired(_)) {
                        self.session_expired.store(true, Ordering::Relaxed);
                    }
                    return (n, false, Some(e));
                }
            }
        }
        (n, true, None)
    }

    /// Coalesced, paced meta refresh (TS `updateMeta` → `refreshGate`). One refresh
    /// runs at a time (`refresh_lock`). A caller skips iff a refresh *completed after
    /// it started waiting* — i.e. it was a concurrent follower already covered by
    /// that refresh, so it coalesces out with no extra journal read. A caller
    /// arriving after the last refresh runs its own, waiting `refresh_delay`
    /// (≤ META_REFRESH_MS, woken early on reconnect/cancel) first — that pre-wait
    /// paces the retry loop and prevents busy-spin.
    async fn gated_refresh(&self) -> Result<Vec<StorageChange>, StorageError> {
        self.ensure_session().await?;
        let arrived = Instant::now();
        let _guard = self.refresh_lock.lock().await;

        if self.last_refresh.lock().unwrap().is_some_and(|t| t > arrived) {
            return Ok(Vec::new());
        }

        self.refresh_delay().await;
        if self.is_canceled() {
            return Ok(Vec::new());
        }

        let changes = if let Some(n) = &self.src_ninja {
            if n.active() {
                match n.src.clone().read().await.update_meta().await {
                    Ok(c) => c,
                    // The journal read hit a lapsed session: reopen, read once more.
                    Err(StorageError::SessionExpired(_)) => {
                        self.session_expired.store(true, Ordering::Relaxed);
                        self.reopen().await?;
                        n.src.clone().read().await.update_meta().await?
                    }
                    Err(e) => return Err(e),
                }
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };

        *self.last_refresh.lock().unwrap() = Some(Instant::now());
        Ok(changes)
    }

    /// A share with a complete cloud meta connects its peer in the background
    /// (`connect`), so the first screen of a browser asks for previews while
    /// P2P is still dialing — "not up yet" is not "no previews". Wait for the
    /// dial, bounded: `on_connected` (like a cancel) notifies `refresh_wake`.
    async fn wait_for_p2p(&self) {
        if self.src_p2p.is_none() {
            return;
        }
        let deadline = tokio::time::Instant::now() + PREVIEW_P2P_WAIT;
        while !self.p2p_active() && !self.is_canceled() {
            let woken = self.refresh_wake.notified();
            tokio::pin!(woken);
            // Registered before the re-check, so a wake in between is not lost.
            woken.as_mut().enable();
            if self.p2p_active() {
                return;
            }
            if tokio::time::timeout_at(deadline, woken).await.is_err() {
                return;
            }
        }
    }

    /// Wait up to META_REFRESH_MS, woken early when P2P reconnects or on cancel
    /// (`refresh_wake`). Mirrors TS `refreshDelay`.
    async fn refresh_delay(&self) {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(META_REFRESH_MS)) => {}
            _ = self.refresh_wake.notified() => {}
        }
    }
}

/// Combines a Ninja/S3 source and a P2P source with stream-and-failover get.
pub struct MultiSource {
    inner: Arc<MultiInner>,
    data_source_id: Option<String>,
}

impl MultiSource {
    pub fn new(
        s3: Option<Arc<RwLock<dyn StorageSource>>>,
        p2p: Option<Arc<RwLock<dyn StorageSource>>>,
        params: Option<MultiSourceParams>,
    ) -> Self {
        let is_canceled = Arc::new(AtomicBool::new(false));
        let session_expired = Arc::new(AtomicBool::new(false));
        let refresh_wake = Arc::new(Notify::new());

        let src_ninja = s3.map(|src| MemberSource::new(src, Arc::new(AtomicBool::new(false))));

        let (src_p2p, p2p_ctl) = match p2p {
            Some(src) => {
                let is_active = Arc::new(AtomicBool::new(false));
                let ctl = Arc::new(P2PCtl {
                    src: src.clone(),
                    is_active: is_active.clone(),
                    is_canceled: is_canceled.clone(),
                    session_expired: session_expired.clone(),
                    fail_count: AtomicU32::new(0),
                    backoff_ms: AtomicU64::new(P2P_BACKOFF_INITIAL_MS),
                    timer: std::sync::Mutex::new(None),
                    refresh_wake: refresh_wake.clone(),
                });
                (Some(MemberSource::new(src, is_active)), Some(ctl))
            }
            None => (None, None),
        };

        Self {
            inner: Arc::new(MultiInner {
                src_ninja,
                src_p2p,
                p2p_ctl,
                params: params.unwrap_or_default(),
                is_canceled,
                session_expired,
                session: std::sync::OnceLock::new(),
                transfer_id: std::sync::Mutex::new(None),
                reopen_lock: tokio::sync::Mutex::new(()),
                refresh_lock: tokio::sync::Mutex::new(()),
                last_refresh: std::sync::Mutex::new(None),
                refresh_wake,
                previews: PreviewState::default(),
            }),
            data_source_id: None,
        }
    }

    /// The real-transfer constructor: open the backend session and build the
    /// members from its datasources — `NinjaStorageV2` over
    /// `S3Storage` for the cloud one, `P2PStorage` for the peer.
    /// Not connected yet: `connect()` follows. A later lapse of the session
    /// reopens it in place (`MultiInner::reopen`).
    pub async fn open(params: SessionParams, multi: Option<MultiSourceParams>) -> Result<Self, StorageError> {
        let tsa = start_session(&params, &AtomicBool::new(false), &Notify::new()).await?;
        tracing::debug!("download item {} transfer {}", params.urltoken, tsa.transfer_id);

        let mut ninja_src: Option<Arc<RwLock<dyn StorageSource>>> = None;
        let mut p2p_src: Option<Arc<RwLock<dyn StorageSource>>> = None;
        let mut s3_typed = None;
        let mut p2p_typed = None;
        for ds in &tsa.datasources {
            match ds {
                LinkDataSource::S3(s3_ds) => {
                    tracing::debug!("has S3 storage, bucket {} prefix {}", s3_ds.bucket, s3_ds.prefix);
                    let mut s3 = S3Storage::new(
                        params.back.clone(),
                        params.urltoken.clone(),
                        s3_ds.presigned_files.clone(),
                    );
                    s3.set_data_source_id(s3_ds.datasource_id.clone());
                    let s3 = Arc::new(RwLock::new(s3));
                    s3_typed = Some(s3.clone());
                    let s3_wrapped: Arc<RwLock<dyn StorageSourceAndDest>> = s3;
                    let mut ninja = NinjaStorageV2::new(params.ninja_params.clone(), s3_wrapped);
                    ninja.set_data_source_id(s3_ds.datasource_id.clone());
                    ninja_src = Some(Arc::new(RwLock::new(ninja)));
                }
                LinkDataSource::P2P(p2p_ds) => {
                    tracing::debug!("have P2P agent, adding P2P source");
                    let mut p2p = P2PStorage::new(
                        params.back.clone(),
                        p2p_ds.agent_id.clone(),
                        tsa.transfer_id.clone(),
                        params.key,
                    );
                    p2p.set_data_source_id(p2p_ds.datasource_id.clone());
                    let p2p = Arc::new(RwLock::new(p2p));
                    p2p_typed = Some(p2p.clone());
                    p2p_src = Some(p2p);
                }
            }
        }
        if ninja_src.is_none() && p2p_src.is_none() {
            return Err(StorageError::Other("No sources".to_string()));
        }
        let hybrid_p2p = multi.as_ref().is_none_or(|m| m.hybrid_p2p);
        if !hybrid_p2p && ninja_src.is_some() && p2p_src.is_some() {
            tracing::info!(urltoken = %params.urltoken, "hybrid P2P off: dropping the P2P source");
            p2p_src = None;
            p2p_typed = None;
        }

        tracing::debug!("composing MultiSource");
        let this = Self::new(ninja_src, p2p_src, multi);
        let _ = this.inner.session.set(SessionInfo {
            params,
            s3: s3_typed,
            p2p: p2p_typed,
        });
        *this.inner.transfer_id.lock().unwrap() = Some(tsa.transfer_id);
        Ok(this)
    }

    /// Per-datasource received bytes off the members, for the session stats.
    pub async fn ds_stats(&self) -> Vec<TransferDSStat> {
        let mut out = Vec::new();
        for m in [&self.inner.src_ninja, &self.inner.src_p2p].into_iter().flatten() {
            let src = m.src.read().await;
            if let Some(id) = src.data_source_id() {
                out.push(TransferDSStat {
                    datasource_id: id.to_string(),
                    bytes: src.total_bytes_recv(),
                });
            }
        }
        out
    }

    /// Tests: bind a session to injected members — the reopen then does a
    /// real `download_start` (against `MockBackend`) with nothing to renew.
    #[cfg(test)]
    pub(crate) fn bind_session_for_test(&self, params: SessionParams) {
        let _ = self.inner.session.set(SessionInfo {
            params,
            s3: None,
            p2p: None,
        });
        *self.inner.transfer_id.lock().unwrap() = Some("tid-0".to_string());
    }

    /// The current backend session id — a new one after every reopen.
    pub fn transfer_id(&self) -> Option<String> {
        self.inner.transfer_id.lock().unwrap().clone()
    }

    /// The session loop heard `transfer_not_found`: the next entry point
    /// reopens before touching the dead session.
    pub fn mark_expired(&self) {
        self.inner.session_expired.store(true, Ordering::Relaxed);
    }

    pub fn is_expired(&self) -> bool {
        self.inner.session_expired.load(Ordering::Relaxed)
    }

    /// Reopen now if the session is marked gone (a run does this before
    /// `file_start`, so the id it opens the file transfer on is live).
    pub async fn ensure_session(&self) -> Result<(), StorageError> {
        self.inner.ensure_session().await
    }
}

#[async_trait]
impl Storage for MultiSource {
    fn cleanup(&mut self) {
        // Tear down for good: stop the reconnect/backoff machinery (a pending
        // timer would redial P2P after close), then cascade to the members —
        // P2P closes its WebRTC connection. The trait method is sync, so the
        // member write locks are taken in a spawned task, like
        // `P2PStorage::cleanup` does for its connector.
        self.inner.is_canceled.store(true, Ordering::Relaxed);
        self.inner.refresh_wake.notify_waiters();
        if let Some(ctl) = &self.inner.p2p_ctl
            && let Some(h) = ctl.timer.lock().unwrap().take()
        {
            h.abort();
        }
        let members = [&self.inner.src_ninja, &self.inner.src_p2p];
        for member in members.into_iter().flatten() {
            let src = member.src.clone();
            tokio::spawn(async move {
                src.write().await.cleanup();
            });
        }
    }

    async fn connect(&mut self) -> Result<StorageMetaState, StorageError> {
        let inner = &self.inner;
        let mut have_complete_meta = false;

        if let Some(n) = &inner.src_ninja {
            let src = n.src.clone();
            src.write().await.connect().await?;
            n.is_active.store(true, Ordering::Relaxed);
            // Completeness via `storage#info` stats (`metaStats`), as in TS.
            if src
                .read()
                .await
                .meta_stats()
                .await
                .map(|s| s.meta_state == StorageMetaState::Complete)
                .unwrap_or(false)
            {
                have_complete_meta = true;
            }
        }

        if let (Some(p), Some(ctl)) = (&inner.src_p2p, &inner.p2p_ctl) {
            // Wire the disconnect callback → reconnect/backoff machinery.
            {
                let ctl_cb = ctl.clone();
                let cb: Arc<dyn Fn() + Send + Sync> = Arc::new(move || ctl_cb.on_error());
                p.src.write().await.set_on_disconnect(cb);
            }
            if !have_complete_meta {
                tracing::debug!("don't have complete meta or missing S3, connecting to P2P");
                p.src.write().await.connect().await?;
                p.is_active.store(true, Ordering::Relaxed);
            } else {
                // Optional P2P: connect in the background, don't block or fail.
                let ctl_bg = ctl.clone();
                tokio::spawn(async move {
                    let res = { ctl_bg.src.write().await.connect().await };
                    match res {
                        Ok(_) => ctl_bg.on_connected(),
                        Err(StorageError::SessionExpired(e)) => ctl_bg.on_session_expired(&e),
                        Err(e) => {
                            tracing::debug!("background P2P connect failed: {}", e);
                            ctl_bg.on_error();
                        }
                    }
                });
            }
        }

        if have_complete_meta {
            Ok(StorageMetaState::Complete)
        } else if inner.ninja_active() || inner.p2p_active() {
            if inner.ninja_complete().await {
                Ok(StorageMetaState::Complete)
            } else {
                Ok(StorageMetaState::Incomplete)
            }
        } else {
            Err(StorageError::Other("No sources connected".into()))
        }
    }

    async fn cancel(&self) -> Result<(), StorageError> {
        let inner = &self.inner;
        if inner.is_canceled.swap(true, Ordering::Relaxed) {
            return Ok(());
        }
        // unblock any in-flight refresh wait so waiters exit promptly
        inner.refresh_wake.notify_waiters();
        if let Some(n) = &inner.src_ninja {
            let _ = n.src.read().await.cancel().await;
        }
        if let Some(p) = &inner.src_p2p {
            let _ = p.src.read().await.cancel().await;
        }
        if let Some(ctl) = &inner.p2p_ctl
            && let Some(h) = ctl.timer.lock().unwrap().take()
        {
            h.abort();
        }
        Ok(())
    }

    async fn update_meta(&self) -> Result<Vec<StorageChange>, StorageError> {
        // Timer-gated + coalesced (TS `updateMeta` → `refreshGate`): paces the
        // retry loop and shares one in-flight refresh across concurrent callers.
        self.inner.gated_refresh().await
    }

    /// Mirrors TS `metaStats`: with both members the combined source is Complete
    /// (P2P covers any gaps), total from ninja; otherwise the single member's stats.
    async fn meta_stats(&self) -> Result<StorageStatInfo, StorageError> {
        let inner = &self.inner;
        match (&inner.src_ninja, &inner.src_p2p) {
            (Some(n), Some(_)) => {
                let stats = n.src.clone().read().await.meta_stats().await?;
                Ok(StorageStatInfo {
                    meta_state: StorageMetaState::Complete,
                    total_size: stats.total_size,
                })
            }
            (Some(n), None) => n.src.clone().read().await.meta_stats().await,
            (None, Some(p)) => p.src.clone().write().await.meta_stats().await,
            (None, None) => Err(StorageError::Other("no source".into())),
        }
    }

    /// Mirrors TS `getDir`: P2P fast-path for single-dir (non-recursive) listing;
    /// otherwise the complete Ninja meta, falling back to P2P — and, with no
    /// peer, the *incomplete* Ninja meta: every entry it holds is real (the
    /// journal already carries in-progress multiparts), the ones still
    /// missing arrive with the transfer engine's later passes. This is what
    /// lets a cloud-only share be downloaded while it is still uploading.
    async fn get_dir(
        &self,
        path: &str,
        recursive: bool,
        start_after: Option<&str>,
        max_batch_size: Option<usize>,
    ) -> mpsc::Receiver<Result<Vec<StorageObjectInfo>, StorageError>> {
        let inner = &self.inner;
        if let Err(e) = inner.ensure_session().await {
            let (tx, rx) = mpsc::channel(1);
            let _ = tx.send(Err(e)).await;
            return rx;
        }
        if inner.p2p_active() && !recursive {
            tracing::debug!("p2p getdir fastpath for {}", path);
            let src = inner.src_p2p.as_ref().unwrap().src.clone();
            return src
                .read()
                .await
                .get_dir(path, recursive, start_after, max_batch_size)
                .await;
        }
        if inner.ninja_complete().await {
            tracing::debug!("ninja src complete, getdiring");
            let src = inner.src_ninja.as_ref().unwrap().src.clone();
            return src
                .read()
                .await
                .get_dir(path, recursive, start_after, max_batch_size)
                .await;
        }
        if inner.p2p_active() {
            let src = inner.src_p2p.as_ref().unwrap().src.clone();
            return src
                .read()
                .await
                .get_dir(path, recursive, start_after, max_batch_size)
                .await;
        }
        if inner.ninja_active() {
            tracing::debug!("ninja src incomplete and no peer, getdiring what it has");
            let src = inner.src_ninja.as_ref().unwrap().src.clone();
            return src
                .read()
                .await
                .get_dir(path, recursive, start_after, max_batch_size)
                .await;
        }
        let (tx, rx) = mpsc::channel(1);
        let _ = tx
            .send(Err(StorageError::Other(format!("no source for getDir {}", path))))
            .await;
        rx
    }

    /// Mirrors TS `getAttrs`: P2P (live) preferred, else Ninja.
    async fn get_attrs(&self, path: &str) -> Result<StorageObjectInfo, StorageError> {
        let inner = &self.inner;
        inner.ensure_session().await?;
        if inner.p2p_active() {
            let src = inner.src_p2p.as_ref().unwrap().src.clone();
            return src.read().await.get_attrs(path).await;
        }
        if inner.ninja_active() {
            let src = inner.src_ninja.as_ref().unwrap().src.clone();
            return src.read().await.get_attrs(path).await;
        }
        Err(StorageError::Other(format!("No source for attrs {}", path)))
    }

    /// P2P first, else a complete ninja meta — forwarding the chosen source's
    /// progress blocks straight through. An incomplete ninja meta with no peer
    /// answers `Incomplete`, never a number: a count over a partial meta is
    /// not a fact (unlike a listing, whose entries are), and the engine
    /// treats `Incomplete` as "total unknown yet — size again next pass".
    async fn get_dir_stats_streamed(&self, path: &str) -> mpsc::Receiver<Result<StorageDirStats, StorageError>> {
        let inner = &self.inner;
        if let Err(e) = inner.ensure_session().await {
            let (tx, rx) = mpsc::channel(1);
            let _ = tx.send(Err(e)).await;
            return rx;
        }
        let src = if inner.p2p_active() {
            Some(inner.src_p2p.as_ref().unwrap().src.clone())
        } else if inner.ninja_complete().await {
            Some(inner.src_ninja.as_ref().unwrap().src.clone())
        } else {
            None
        };
        match src {
            Some(src) => src.read().await.get_dir_stats_streamed(path).await,
            None => {
                let (tx, rx) = mpsc::channel(1);
                let err = if inner.ninja_active() {
                    StorageError::Incomplete(format!("dir stats of {} on a still-uploading source", path))
                } else {
                    StorageError::Other(format!("no source for getDirStats {}", path))
                };
                let _ = tx.send(Err(err)).await;
                rx
            }
        }
    }

    async fn search(&self, query: &StorageSearchQuery, options: &SearchOptions) -> Result<SearchResult, StorageError> {
        let inner = &self.inner;
        inner.ensure_session().await?;
        if inner.ninja_complete().await {
            let src = inner.src_ninja.as_ref().unwrap().src.clone();
            return src.read().await.search(query, options).await;
        }
        if inner.p2p_active() {
            let src = inner.src_p2p.as_ref().unwrap().src.clone();
            return src.read().await.search(query, options).await;
        }
        if inner.ninja_active() {
            // Same as `get_dir`: what the incomplete meta holds is real.
            let src = inner.src_ninja.as_ref().unwrap().src.clone();
            return src.read().await.search(query, options).await;
        }
        Err(StorageError::Other("no active sources for search".into()))
    }

    async fn list_updates_begin(&self, path: &str) -> Result<String, StorageError> {
        let inner = &self.inner;
        if let Some(n) = &inner.src_ninja {
            return n.src.clone().read().await.list_updates_begin(path).await;
        }
        if let Some(p) = &inner.src_p2p {
            return p.src.clone().read().await.list_updates_begin(path).await;
        }
        Err(StorageError::Other("listUpdatesBegin not implemented".into()))
    }

    async fn list_updates(
        &self,
        cursor: &str,
        options: &ListUpdatesOptions,
    ) -> Result<ListUpdatesResult, StorageError> {
        let inner = &self.inner;
        if let Some(n) = &inner.src_ninja {
            return n.src.clone().read().await.list_updates(cursor, options).await;
        }
        if let Some(p) = &inner.src_p2p {
            return p.src.clone().read().await.list_updates(cursor, options).await;
        }
        Err(StorageError::Other("no source for listUpdates".into()))
    }

    fn total_bytes_recv(&self) -> u64 {
        let inner = &self.inner;
        let mut t = 0;
        if let Some(n) = &inner.src_ninja {
            t += n.sample(&n.last_total_recv, |g| g.total_bytes_recv());
        }
        if let Some(p) = &inner.src_p2p {
            t += p.sample(&p.last_total_recv, |g| g.total_bytes_recv());
        }
        t
    }

    fn total_bytes_sent(&self) -> u64 {
        let inner = &self.inner;
        let mut t = 0;
        if let Some(n) = &inner.src_ninja {
            t += n.sample(&n.last_total_sent, |g| g.total_bytes_sent());
        }
        if let Some(p) = &inner.src_p2p {
            t += p.sample(&p.last_total_sent, |g| g.total_bytes_sent());
        }
        t
    }

    /// Sum in-flight reads across both member sources for `progress()` smoothing.
    /// Non-blocking `try_read` with a last-known-good fallback per member
    /// ([`MemberSource::sample`]) — a contended sample must not read as 0.
    fn data_source_id(&self) -> Option<&str> {
        self.data_source_id.as_deref()
    }

    fn set_data_source_id(&mut self, id: String) {
        self.data_source_id = Some(id);
    }
}

#[async_trait]
impl StorageSource for MultiSource {
    fn source_name(&self) -> &str {
        "Multi"
    }

    fn source_type(&self) -> StorageSourceType {
        // No dedicated `Multi` variant in the Rust enum; the actual type depends on
        // the underlying sources. S3 is the conventional default.
        StorageSourceType::S3
    }

    /// Options-aware streaming get (TS `getStreamedV2`) — THE primitive. Spawns the
    /// failover producer (`get_int`); chunks flow to the consumer as each source
    /// delivers them. `get`/`get_v2` are the base drains over this.
    async fn get_streamed_v2(
        &self,
        object: &str,
        options: &StorageGetOptions,
        offset: Option<u64>,
        length: Option<u64>,
    ) -> mpsc::Receiver<Result<Vec<u8>, StorageError>> {
        let (tx, rx) = mpsc::channel(4);
        let inner = self.inner.clone();
        let object = object.to_string();
        let options = options.clone();
        tokio::spawn(async move {
            inner.get_int(&object, &options, offset, length, tx).await;
        });
        rx
    }

    async fn get_streamed(
        &self,
        object: &str,
        offset: Option<u64>,
        length: Option<u64>,
    ) -> mpsc::Receiver<Result<Vec<u8>, StorageError>> {
        self.get_streamed_v2(object, &StorageGetOptions::default(), offset, length)
            .await
    }

    /// Previews of a screen of rows (docs/core/previews.md): what the cache
    /// holds is answered at once, the rest is one request to the peer, its
    /// answers cached (an image, or the peer's "no preview") as they pass.
    /// Previews come from P2P alone — no ninja fallback; a peer that is still
    /// dialing is waited for (`wait_for_p2p`). `Err` = something is missing
    /// and there is no peer to ask.
    async fn get_previews(
        &self,
        objects: Vec<String>,
        px: u32,
    ) -> Result<mpsc::Receiver<(usize, Result<PreviewImage, StorageError>)>, StorageError> {
        let inner = &self.inner;
        let (tx, rx) = mpsc::channel(objects.len().max(1));
        let (hits, missing) = inner.previews.cached(&objects, px);
        if !missing.is_empty() {
            inner.ensure_session().await?;
            inner.wait_for_p2p().await;
            if !inner.p2p_active() {
                return Err(StorageError::NotImplemented("previews need a P2P peer".into()));
            }
            let asked: Vec<String> = missing.iter().map(|&i| objects[i].clone()).collect();
            let src = inner.src_p2p.as_ref().unwrap().src.clone();
            let mut answers = src.read().await.get_previews(asked.clone(), px).await?;
            // Back under the caller's indices, caching on the way. A caller that
            // went away still gets its answers cached: they arrive regardless.
            let (inner, tx) = (inner.clone(), tx.clone());
            tokio::spawn(async move {
                while let Some((i, result)) = answers.recv().await {
                    let (Some(&index), Some(object)) = (missing.get(i), asked.get(i)) else {
                        continue;
                    };
                    match &result {
                        Ok(image) => inner.previews.store(object, px, Some(image.clone())),
                        Err(StorageError::NoPreview(_)) => inner.previews.store(object, px, None),
                        Err(_) => {}
                    }
                    let _ = tx.try_send((index, result));
                }
            });
        }
        for hit in hits {
            let _ = tx.try_send(hit);
        }
        Ok(rx)
    }

    async fn get_symlink(&self, object: &str) -> Result<String, StorageError> {
        let inner = &self.inner;
        if let Some(n) = &inner.src_ninja
            && n.active()
        {
            let src = n.src.clone();
            match src.read().await.get_symlink(object).await {
                Ok(target) => return Ok(target),
                Err(e) => tracing::debug!("can't get symlink on ninja {}: {}", object, e),
            }
        }
        if let Some(p) = &inner.src_p2p
            && p.active()
        {
            let src = p.src.clone();
            return src.read().await.get_symlink(object).await;
        }
        Err(StorageError::Other(format!("No source for symlink {}", object)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// Which error a `Mem` raises once its `serve_budget` runs dry.
    enum BudgetFailKind {
        P2P,
        SessionExpired,
        Transport,
    }

    /// Minimal in-memory source. `complete` drives `meta_stats`
    /// completeness; `fail_get` makes `get_streamed` raise `NoPartError` (a
    /// not-yet-available part) while `get_attrs` still reports the key present.
    /// The remaining fields (defaulted by `new`) script specific behaviours:
    /// a byte budget after which gets fail, connect failures, an update_meta
    /// that "delivers" the missing parts, and call counters.
    struct Mem {
        name: &'static str,
        files: HashMap<String, Vec<u8>>,
        complete: bool,
        fail_get: AtomicBool,
        /// `update_meta` clears `fail_get` — the parts became available.
        clear_fail_on_update: bool,
        update_meta_calls: AtomicU32,
        /// Total bytes this source may serve; once dry, gets fail with `budget_kind`.
        serve_budget: Option<AtomicU64>,
        budget_kind: BudgetFailKind,
        /// After the budget failure the budget is set to this — the source
        /// serves again on the next get (a lapse or a wire flap, not a dead source).
        refill_budget: Option<u64>,
        total_size: Option<u64>,
        symlink: Option<String>,
        /// This many `connect` calls fail before one succeeds.
        connect_fail_times: u32,
        /// What a scripted connect failure raises (`Other` by default).
        connect_fail_expired: bool,
        connect_calls: u32,
        cancel_calls: AtomicU32,
        get_calls: AtomicU32,
        on_disconnect: Option<Arc<dyn Fn() + Send + Sync>>,
        /// The objects of every `get_previews` request, in arrival order.
        preview_requests: std::sync::Mutex<Vec<Vec<String>>>,
        /// The version the previews report.
        etag: std::sync::Mutex<String>,
    }

    impl Mem {
        fn new(name: &'static str, files: HashMap<String, Vec<u8>>, complete: bool) -> Self {
            Self {
                name,
                files,
                complete,
                fail_get: AtomicBool::new(false),
                clear_fail_on_update: false,
                update_meta_calls: AtomicU32::new(0),
                serve_budget: None,
                budget_kind: BudgetFailKind::P2P,
                refill_budget: None,
                total_size: None,
                symlink: None,
                connect_fail_times: 0,
                connect_fail_expired: false,
                connect_calls: 0,
                cancel_calls: AtomicU32::new(0),
                get_calls: AtomicU32::new(0),
                on_disconnect: None,
                preview_requests: std::sync::Mutex::new(Vec::new()),
                etag: std::sync::Mutex::new("v1".into()),
            }
        }

        fn budget_error(&self) -> StorageError {
            match self.budget_kind {
                BudgetFailKind::P2P => StorageError::P2P(format!("{} budget dry", self.name)),
                BudgetFailKind::SessionExpired => StorageError::SessionExpired(format!("{} session gone", self.name)),
                BudgetFailKind::Transport => StorageError::Transport(format!("{} wire died", self.name)),
            }
        }
    }

    #[async_trait]
    impl Storage for Mem {
        async fn connect(&mut self) -> Result<StorageMetaState, StorageError> {
            self.connect_calls += 1;
            if self.connect_fail_times > 0 {
                self.connect_fail_times -= 1;
                if self.connect_fail_expired {
                    return Err(StorageError::SessionExpired(format!("{} session gone", self.name)));
                }
                return Err(StorageError::Other(format!("{} connect failed", self.name)));
            }
            Ok(StorageMetaState::Complete)
        }
        async fn cancel(&self) -> Result<(), StorageError> {
            self.cancel_calls.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        async fn update_meta(&self) -> Result<Vec<StorageChange>, StorageError> {
            self.update_meta_calls.fetch_add(1, Ordering::Relaxed);
            if self.clear_fail_on_update {
                self.fail_get.store(false, Ordering::Relaxed);
            }
            Ok(Vec::new())
        }
        async fn meta_stats(&self) -> Result<StorageStatInfo, StorageError> {
            Ok(StorageStatInfo {
                meta_state: if self.complete {
                    StorageMetaState::Complete
                } else {
                    StorageMetaState::Incomplete
                },
                total_size: self.total_size,
            })
        }
        async fn get_attrs(&self, path: &str) -> Result<StorageObjectInfo, StorageError> {
            self.files
                .get(path)
                .map(|v| {
                    let mut soi = StorageObjectInfo::new(path);
                    soi.size = v.len() as u64;
                    soi
                })
                .ok_or_else(|| StorageError::NoSuchKey(path.into()))
        }
        /// One entry named after the source — lets routing tests see who served.
        async fn get_dir(
            &self,
            _path: &str,
            _recursive: bool,
            _start_after: Option<&str>,
            _max_batch_size: Option<usize>,
        ) -> mpsc::Receiver<Result<Vec<StorageObjectInfo>, StorageError>> {
            let (tx, rx) = mpsc::channel(1);
            let _ = tx.send(Ok(vec![StorageObjectInfo::new(self.name)])).await;
            rx
        }
        fn set_on_disconnect(&mut self, callback: Arc<dyn Fn() + Send + Sync>) {
            self.on_disconnect = Some(callback);
        }
        fn data_source_id(&self) -> Option<&str> {
            None
        }
        fn set_data_source_id(&mut self, _id: String) {}
    }

    #[async_trait]
    impl StorageSource for Mem {
        fn source_name(&self) -> &str {
            self.name
        }
        async fn get_streamed(
            &self,
            object: &str,
            offset: Option<u64>,
            length: Option<u64>,
        ) -> mpsc::Receiver<Result<Vec<u8>, StorageError>> {
            self.get_calls.fetch_add(1, Ordering::Relaxed);
            let (tx, rx) = mpsc::channel(4);
            if self.fail_get.load(Ordering::Relaxed) {
                let _ = tx.send(Err(StorageError::NoPartError(object.to_string()))).await;
                return rx;
            }
            let Some(data) = self.files.get(object) else {
                let _ = tx.send(Err(StorageError::NoSuchKey(object.to_string()))).await;
                return rx;
            };
            let start = offset.unwrap_or(0) as usize;
            let end = length.map(|l| start + l as usize).unwrap_or(data.len()).min(data.len());
            if let Some(budget) = &self.serve_budget {
                let want = end.saturating_sub(start);
                let serve = want.min(budget.load(Ordering::Relaxed) as usize);
                if serve > 0 {
                    budget.fetch_sub(serve as u64, Ordering::Relaxed);
                    let _ = tx.send(Ok(data[start..start + serve].to_vec())).await;
                }
                if serve < want {
                    let _ = tx.send(Err(self.budget_error())).await;
                    if let Some(r) = self.refill_budget {
                        budget.store(r, Ordering::Relaxed);
                    }
                }
                return rx;
            }
            let _ = tx.send(Ok(data[start..end].to_vec())).await;
            rx
        }
        async fn get_previews(
            &self,
            objects: Vec<String>,
            _px: u32,
        ) -> Result<mpsc::Receiver<(usize, Result<PreviewImage, StorageError>)>, StorageError> {
            self.preview_requests.lock().unwrap().push(objects.clone());
            let (tx, rx) = mpsc::channel(objects.len().max(1));
            let (name, etag) = (self.name, self.etag.lock().unwrap().clone());
            tokio::spawn(async move {
                for (index, object) in objects.into_iter().enumerate() {
                    let result = if object.ends_with(".bin") {
                        Err(StorageError::NoPreview(object))
                    } else {
                        Ok(PreviewImage {
                            bytes: format!("{name}:{object}@{etag}").into_bytes(),
                            mime_type: "image/png".into(),
                            etag: Some(etag.clone()),
                        })
                    };
                    let _ = tx.send((index, result)).await;
                }
            });
            Ok(rx)
        }
        async fn get_symlink(&self, object: &str) -> Result<String, StorageError> {
            self.symlink
                .clone()
                .ok_or_else(|| StorageError::NotImplemented(format!("symlink {}", object)))
        }
    }

    fn arc(m: Mem) -> Arc<RwLock<dyn StorageSource>> {
        Arc::new(RwLock::new(m))
    }

    fn activate(ms: &MultiSource) {
        if let Some(n) = &ms.inner.src_ninja {
            n.is_active.store(true, Ordering::Relaxed);
        }
        if let Some(p) = &ms.inner.src_p2p {
            p.is_active.store(true, Ordering::Relaxed);
        }
    }

    fn one(key: &str, bytes: &[u8]) -> HashMap<String, Vec<u8>> {
        let mut h = HashMap::new();
        h.insert(key.into(), bytes.to_vec());
        h
    }

    /// preferP2P=true with both sources up serves the P2P copy, not Ninja.
    #[tokio::test]
    async fn prefer_p2p_serves_from_p2p() {
        let ninja = arc(Mem::new("ninja", one("f", b"NINJA"), true));
        let p2p = arc(Mem::new("p2p", one("f", b"P2P--"), true));
        let ms = MultiSource::new(
            Some(ninja),
            Some(p2p),
            Some(MultiSourceParams {
                prefer_p2p: true,
                ..Default::default()
            }),
        );
        activate(&ms);

        let got = ms.get("f", None, None).await.unwrap();
        assert_eq!(got, b"P2P--");
    }

    async fn one_preview(ms: &MultiSource, object: &str) -> Option<Result<PreviewImage, StorageError>> {
        let mut rx = ms.get_previews(vec![object.to_string()], 64).await.ok()?;
        rx.recv().await.map(|(_, result)| result)
    }

    /// Previews come from the peer alone — never ninja; with no peer and
    /// nothing cached the request is refused (once the wait for a dialing peer
    /// runs out), and what was cached still serves.
    #[tokio::test(start_paused = true)]
    async fn previews_are_served_by_p2p_only() {
        let ninja = arc(Mem::new("ninja", one("f", b"NINJA"), true));
        let p2p = arc(Mem::new("p2p", one("f", b"P2P--"), true));
        let ms = MultiSource::new(Some(ninja), Some(p2p), None);
        activate(&ms);

        let image = one_preview(&ms, "f").await.unwrap().unwrap();
        assert_eq!(image.bytes, b"p2p:f@v1");

        ms.inner
            .src_p2p
            .as_ref()
            .unwrap()
            .is_active
            .store(false, Ordering::Relaxed);
        assert!(matches!(
            ms.get_previews(vec!["other".into()], 64).await,
            Err(StorageError::NotImplemented(_))
        ));
        assert!(one_preview(&ms, "f").await.unwrap().is_ok(), "a cached preview needs no peer");
    }

    /// The first screen asks while the peer is still dialing in the background
    /// (a cloud + P2P share): the request waits for `on_connected` instead of
    /// being refused; with no peer coming up it is refused after the bound.
    #[tokio::test(start_paused = true)]
    async fn previews_wait_for_a_dialing_peer() {
        let p2p = arc(Mem::new("p2p", HashMap::new(), true));
        let ms = Arc::new(MultiSource::new(None, Some(p2p), None));
        // A background connect: the member exists, not active yet.
        let ctl = ms.inner.p2p_ctl.clone().expect("a P2P member has its control");

        let asking = tokio::spawn({
            let ms = ms.clone();
            async move { one_preview(&ms, "a.png").await }
        });
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert!(!asking.is_finished(), "the request waits for the dial");
        ctl.on_connected();
        assert_eq!(asking.await.unwrap().unwrap().unwrap().bytes, b"p2p:a.png@v1");

        let never = MultiSource::new(None, Some(arc(Mem::new("p2p", HashMap::new(), true))), None);
        assert!(matches!(
            never.get_previews(vec!["a.png".into()], 64).await,
            Err(StorageError::NotImplemented(_))
        ));
    }

    /// A screen of rows is one request to the peer; what it answered — images
    /// and "no preview" alike — never reaches the peer again, and every answer
    /// comes back under the caller's own index.
    #[tokio::test]
    async fn previews_batch_and_cache() {
        let p2p = Arc::new(RwLock::new(Mem::new("p2p", HashMap::new(), true)));
        let ms = MultiSource::new(None, Some(p2p.clone() as Arc<RwLock<dyn StorageSource>>), None);
        activate(&ms);

        async fn ask(ms: &MultiSource, objects: &[&str]) -> Vec<Option<Result<PreviewImage, StorageError>>> {
            let objects: Vec<String> = objects.iter().map(|o| o.to_string()).collect();
            let mut out: Vec<Option<_>> = objects.iter().map(|_| None).collect();
            let mut rx = ms.get_previews(objects, 64).await.unwrap();
            while let Some((index, result)) = rx.recv().await {
                assert!(out[index].is_none(), "index {index} answered twice");
                out[index] = Some(result);
            }
            out
        }

        let got = ask(&ms, &["a.png", "c.bin", "b.png"]).await;
        assert_eq!(got[0].as_ref().unwrap().as_ref().unwrap().bytes, b"p2p:a.png@v1");
        assert!(matches!(got[1], Some(Err(StorageError::NoPreview(_)))));
        assert_eq!(got[2].as_ref().unwrap().as_ref().unwrap().bytes, b"p2p:b.png@v1");

        let got = ask(&ms, &["d.png", "a.png", "c.bin"]).await;
        assert_eq!(got[0].as_ref().unwrap().as_ref().unwrap().bytes, b"p2p:d.png@v1");
        assert!(got[1].as_ref().unwrap().is_ok(), "a cached image");
        assert!(matches!(got[2], Some(Err(StorageError::NoPreview(_)))), "a cached no-preview");

        assert_eq!(
            *p2p.read().await.preview_requests.lock().unwrap(),
            [
                vec!["a.png".to_string(), "c.bin".to_string(), "b.png".to_string()],
                vec!["d.png".to_string()],
            ],
            "one request per screen, and only for what the cache lacks"
        );
    }

    /// With P2P down, Ninja serves (the fallback path).
    #[tokio::test]
    async fn falls_back_to_ninja_when_p2p_inactive() {
        let ninja = arc(Mem::new("ninja", one("f", b"NINJA"), true));
        let p2p = arc(Mem::new("p2p", one("f", b"P2P--"), true));
        let ms = MultiSource::new(Some(ninja), Some(p2p), None);
        // only ninja active
        ms.inner
            .src_ninja
            .as_ref()
            .unwrap()
            .is_active
            .store(true, Ordering::Relaxed);

        let got = ms.get("f", None, None).await.unwrap();
        assert_eq!(got, b"NINJA");
    }

    /// A part that isn't available yet, with wait_for_parts=false, fails fast with
    /// NoPartError (rather than hanging).
    #[tokio::test]
    async fn missing_part_no_wait_is_nopart() {
        let mem = Mem::new("ninja", one("f", b"data"), false);
        mem.fail_get.store(true, Ordering::Relaxed); // key present (get_attrs ok), bytes not yet (get -> NoPart)
        let ms = MultiSource::new(Some(arc(mem)), None, None);
        activate(&ms);

        let err = ms
            .get_v2(
                "f",
                &StorageGetOptions {
                    wait_for_parts: false,
                    ..Default::default()
                },
                Some(0),
                Some(4),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, StorageError::NoPartError(_)), "got {err:?}");
    }

    /// A file absent on a complete source is NoSuchKey (TS NoSourceError), not NoPart.
    #[tokio::test]
    async fn absent_on_complete_source_is_nosuchkey() {
        let ninja = arc(Mem::new("ninja", one("f", b"data"), true));
        let ms = MultiSource::new(Some(ninja), None, None);
        activate(&ms);

        let err = ms.get("missing", None, None).await.unwrap_err();
        assert!(matches!(err, StorageError::NoSuchKey(_)), "got {err:?}");
    }

    /// Typed handle + coerced trait object over the same Mem, so tests can
    /// inspect/mutate the fake after wiring it into a MultiSource.
    fn typed(m: Mem) -> (Arc<RwLock<Mem>>, Arc<RwLock<dyn StorageSource>>) {
        let t = Arc::new(RwLock::new(m));
        let d: Arc<RwLock<dyn StorageSource>> = t.clone();
        (t, d)
    }

    // ── failover ───────────────────────────────────────────────────────────

    /// P2P dies mid-stream: the bytes it delivered stay delivered, the dry
    /// retry drops it (cancel + reconnect timer), and the loop immediately
    /// re-enters to let Ninja serve exactly the remainder of the requested
    /// window — no NoSuchKey/NoPart exit, no refresh wait.
    #[tokio::test]
    async fn failover_resumes_mid_stream_from_other_source() {
        let data = b"0123456789AB";
        let (ninja_t, ninja) = typed(Mem::new("ninja", one("f", data), true));
        let mut p2p_mem = Mem::new("p2p", one("f", data), true);
        p2p_mem.serve_budget = Some(AtomicU64::new(4));
        let (p2p_t, p2p) = typed(p2p_mem);
        let ms = MultiSource::new(
            Some(ninja),
            Some(p2p),
            Some(MultiSourceParams {
                prefer_p2p: true,
                ..Default::default()
            }),
        );
        activate(&ms);

        let got = ms.get("f", Some(2), Some(8)).await.unwrap();
        assert_eq!(
            got, b"23456789",
            "window served across the failover, no gap, no overlap"
        );

        let ctl = ms.inner.p2p_ctl.as_ref().unwrap();
        assert!(!ctl.is_active.load(Ordering::Relaxed), "dry P2P source dropped");
        assert!(ctl.timer.lock().unwrap().is_some(), "reconnect scheduled");
        assert!(p2p_t.read().await.cancel_calls.load(Ordering::Relaxed) >= 1);
        assert_eq!(ninja_t.read().await.get_calls.load(Ordering::Relaxed), 1);
        assert_eq!(
            ninja_t.read().await.update_meta_calls.load(Ordering::Relaxed),
            0,
            "no refresh needed to fail over"
        );
    }

    /// Same outage on a live share (ninja meta still incomplete): the failover
    /// still lands in the same get — an incomplete ninja that has the bytes
    /// serves them, without a not-found exit or a refresh in between.
    #[tokio::test]
    async fn failover_works_while_ninja_incomplete() {
        let data = b"0123456789AB";
        let (ninja_t, ninja) = typed(Mem::new("ninja", one("f", data), false));
        let mut p2p_mem = Mem::new("p2p", one("f", data), true);
        p2p_mem.serve_budget = Some(AtomicU64::new(4));
        let (_pt, p2p) = typed(p2p_mem);
        let ms = MultiSource::new(
            Some(ninja),
            Some(p2p),
            Some(MultiSourceParams {
                prefer_p2p: true,
                ..Default::default()
            }),
        );
        activate(&ms);

        let got = ms
            .get_v2(
                "f",
                &StorageGetOptions {
                    wait_for_parts: true,
                    ..Default::default()
                },
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(got, b"0123456789AB");
        assert!(!ms.inner.p2p_active(), "dry P2P source dropped");
        assert_eq!(
            ninja_t.read().await.update_meta_calls.load(Ordering::Relaxed),
            0,
            "no refresh needed to fail over"
        );
    }

    /// SessionExpired is terminal: surfaced to the consumer, no failover spin
    /// (the ninja leg is never consulted — its refresh path is just as dead).
    #[tokio::test]
    async fn session_expired_surfaces_without_failover() {
        let data = b"0123456789AB";
        let (ninja_t, ninja) = typed(Mem::new("ninja", one("f", data), true));
        let mut p2p_mem = Mem::new("p2p", one("f", data), true);
        p2p_mem.serve_budget = Some(AtomicU64::new(4));
        p2p_mem.budget_kind = BudgetFailKind::SessionExpired;
        let (_p2p_t, p2p) = typed(p2p_mem);
        let ms = MultiSource::new(
            Some(ninja),
            Some(p2p),
            Some(MultiSourceParams {
                prefer_p2p: true,
                ..Default::default()
            }),
        );
        activate(&ms);

        let err = ms.get("f", None, None).await.unwrap_err();
        assert!(matches!(err, StorageError::SessionExpired(_)), "got {err:?}");
        assert_eq!(
            ninja_t.read().await.get_calls.load(Ordering::Relaxed),
            0,
            "no failover against a dead session"
        );
        assert!(ms.inner.p2p_active(), "session expiry is not a source-drop");
    }

    /// wait_for_parts=true: a not-yet-available range paces through the gated
    /// refresh (≈5 s, virtual here) until update_meta delivers the parts.
    #[tokio::test(start_paused = true)]
    async fn wait_for_parts_retries_after_refresh() {
        let mut mem = Mem::new("ninja", one("f", b"data"), false);
        mem.fail_get.store(true, Ordering::Relaxed);
        mem.clear_fail_on_update = true;
        let (ninja_t, ninja) = typed(mem);
        let ms = MultiSource::new(Some(ninja), None, None);
        activate(&ms);

        let got = ms
            .get_v2(
                "f",
                &StorageGetOptions {
                    wait_for_parts: true,
                    ..Default::default()
                },
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(got, b"data");
        assert_eq!(
            ninja_t.read().await.update_meta_calls.load(Ordering::Relaxed),
            1,
            "one refresh delivered the parts"
        );
    }

    /// `wait_cancel` ends a `wait_for_parts` wait at once with `NoPartError` —
    /// not after the refresh interval, and without touching the source.
    #[tokio::test]
    async fn wait_for_parts_ends_on_wait_cancel() {
        let mem = Mem::new("ninja", one("f", b"data"), false);
        mem.fail_get.store(true, Ordering::Relaxed); // never delivered
        let (_ninja_t, ninja) = typed(mem);
        let ms = Arc::new(MultiSource::new(Some(ninja), None, None));
        activate(&ms);

        let wait_cancel = tokio_util::sync::CancellationToken::new();
        let get = tokio::spawn({
            let ms = ms.clone();
            let options = StorageGetOptions {
                wait_for_parts: true,
                wait_cancel: wait_cancel.clone(),
            };
            async move { ms.get_v2("f", &options, None, None).await }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        wait_cancel.cancel();

        let err = tokio::time::timeout(Duration::from_secs(1), get)
            .await
            .expect("the wait must end on the token, not on the refresh interval")
            .unwrap()
            .unwrap_err();
        assert!(matches!(err, StorageError::NoPartError(_)), "got {err:?}");
        assert!(!ms.inner.is_canceled(), "the shared source is untouched");
    }

    // ── gated refresh ──────────────────────────────────────────────────────

    /// Concurrent refresh callers coalesce into one underlying update_meta;
    /// a later caller runs its own.
    #[tokio::test(start_paused = true)]
    async fn refresh_coalesces_concurrent_callers_and_paces_later_ones() {
        let (ninja_t, ninja) = typed(Mem::new("ninja", one("f", b"data"), true));
        let ms = MultiSource::new(Some(ninja), None, None);
        activate(&ms);

        let h1 = tokio::spawn({
            let inner = ms.inner.clone();
            async move { inner.gated_refresh().await }
        });
        let h2 = tokio::spawn({
            let inner = ms.inner.clone();
            async move { inner.gated_refresh().await }
        });
        h1.await.unwrap().unwrap();
        h2.await.unwrap().unwrap();
        assert_eq!(
            ninja_t.read().await.update_meta_calls.load(Ordering::Relaxed),
            1,
            "concurrent followers coalesce into the one in-flight refresh"
        );

        ms.inner.gated_refresh().await.unwrap();
        assert_eq!(
            ninja_t.read().await.update_meta_calls.load(Ordering::Relaxed),
            2,
            "a caller arriving after the last refresh runs its own"
        );
    }

    /// A refresh parked in its pre-wait is cut short when P2P reconnects, so a
    /// waiting get retries immediately instead of sitting out the full delay.
    #[tokio::test]
    async fn refresh_wakes_on_p2p_reconnect() {
        let (ninja_t, ninja) = typed(Mem::new("ninja", one("f", b"data"), true));
        let (_p2p_t, p2p) = typed(Mem::new("p2p", one("f", b"data"), true));
        let ms = MultiSource::new(Some(ninja), Some(p2p), None);
        ms.inner
            .src_ninja
            .as_ref()
            .unwrap()
            .is_active
            .store(true, Ordering::Relaxed);

        let refresh = tokio::spawn({
            let inner = ms.inner.clone();
            async move { inner.gated_refresh().await }
        });
        let ctl = ms.inner.p2p_ctl.as_ref().unwrap().clone();
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            while !refresh.is_finished() {
                ctl.on_connected(); // notifies the refresh_wake waiters
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("refresh should be woken well before the 5 s delay");
        assert_eq!(ninja_t.read().await.update_meta_calls.load(Ordering::Relaxed), 1);
        assert!(ms.inner.p2p_active());
    }

    // ── cancel ─────────────────────────────────────────────────────────────

    /// cancel(): members cancelled, a parked refresh exits without refreshing,
    /// and subsequent gets fail as canceled.
    #[tokio::test(start_paused = true)]
    async fn cancel_stops_gets_and_refreshes() {
        let (ninja_t, ninja) = typed(Mem::new("ninja", one("f", b"data"), true));
        let (p2p_t, p2p) = typed(Mem::new("p2p", one("f", b"data"), true));
        let ms = MultiSource::new(Some(ninja), Some(p2p), None);
        activate(&ms);

        let refresh = tokio::spawn({
            let inner = ms.inner.clone();
            async move { inner.gated_refresh().await }
        });
        tokio::time::sleep(Duration::from_millis(50)).await; // let it park in the pre-wait
        ms.cancel().await.unwrap();

        refresh.await.unwrap().unwrap();
        assert_eq!(
            ninja_t.read().await.update_meta_calls.load(Ordering::Relaxed),
            0,
            "cancel wakes the parked refresh without running it"
        );
        assert!(ninja_t.read().await.cancel_calls.load(Ordering::Relaxed) >= 1);
        assert!(p2p_t.read().await.cancel_calls.load(Ordering::Relaxed) >= 1);

        let err = ms.get("f", None, None).await.unwrap_err();
        assert!(
            matches!(&err, StorageError::NoSuchKey(m) if m.contains("canceled")),
            "got {err:?}"
        );
    }

    // ── meta_stats ─────────────────────────────────────────────────────────

    /// With both members the combined meta is Complete (P2P covers any ninja
    /// gaps) with ninja's byte total; single members pass through.
    #[tokio::test]
    async fn meta_stats_completeness_rules() {
        let mut ninja_mem = Mem::new("ninja", one("f", b"data"), false);
        ninja_mem.total_size = Some(42);
        let (_nt, ninja) = typed(ninja_mem);
        let (_pt, p2p) = typed(Mem::new("p2p", one("f", b"data"), true));
        let both = MultiSource::new(Some(ninja), Some(p2p), None);
        let stats = both.meta_stats().await.unwrap();
        assert_eq!(stats.meta_state, StorageMetaState::Complete, "P2P presence ⇒ Complete");
        assert_eq!(stats.total_size, Some(42), "byte total still from ninja");

        let ninja_only = MultiSource::new(Some(arc(Mem::new("ninja", one("f", b"d"), false))), None, None);
        assert_eq!(
            ninja_only.meta_stats().await.unwrap().meta_state,
            StorageMetaState::Incomplete
        );

        let p2p_only = MultiSource::new(None, Some(arc(Mem::new("p2p", one("f", b"d"), true))), None);
        assert_eq!(
            p2p_only.meta_stats().await.unwrap().meta_state,
            StorageMetaState::Complete
        );
    }

    // ── connect ────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn connect_no_sources_errors() {
        let mut ms = MultiSource::new(None, None, None);
        let err = ms.connect().await.unwrap_err();
        assert!(err.to_string().contains("No sources connected"), "got: {err}");
    }

    /// Complete ninja meta: connect returns Complete at once, P2P connects in
    /// the background, and the disconnect callback is wired to the reconnect
    /// machinery.
    #[tokio::test]
    async fn connect_complete_ninja_backgrounds_p2p_and_wires_disconnect() {
        let (_nt, ninja) = typed(Mem::new("ninja", one("f", b"data"), true));
        let (p2p_t, p2p) = typed(Mem::new("p2p", one("f", b"data"), true));
        let mut ms = MultiSource::new(Some(ninja), Some(p2p), None);

        let state = ms.connect().await.unwrap();
        assert_eq!(state, StorageMetaState::Complete);
        assert!(ms.inner.ninja_active());

        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            while !ms.inner.p2p_active() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("background P2P connect never landed");
        assert_eq!(p2p_t.read().await.connect_calls, 1);

        let cb = p2p_t
            .read()
            .await
            .on_disconnect
            .clone()
            .expect("disconnect callback wired");
        cb();
        assert!(!ms.inner.p2p_active(), "disconnect deactivates P2P");
        let ctl = ms.inner.p2p_ctl.as_ref().unwrap();
        assert_eq!(ctl.fail_count.load(Ordering::Relaxed), 1);
        assert!(ctl.timer.lock().unwrap().is_some(), "reconnect scheduled");
    }

    /// Incomplete ninja meta makes P2P mandatory: its connect failure fails
    /// the whole connect; success yields Incomplete.
    #[tokio::test]
    async fn connect_incomplete_ninja_requires_p2p() {
        let mut p2p_mem = Mem::new("p2p", one("f", b"data"), true);
        p2p_mem.connect_fail_times = 1;
        let (_pt, p2p) = typed(p2p_mem);
        let mut ms = MultiSource::new(Some(arc(Mem::new("ninja", one("f", b"data"), false))), Some(p2p), None);
        assert!(
            ms.connect().await.is_err(),
            "mandatory P2P connect failure must propagate"
        );

        let mut ms = MultiSource::new(
            Some(arc(Mem::new("ninja", one("f", b"data"), false))),
            Some(arc(Mem::new("p2p", one("f", b"data"), true))),
            None,
        );
        assert_eq!(ms.connect().await.unwrap(), StorageMetaState::Incomplete);
        assert!(ms.inner.p2p_active());
    }

    /// Optional P2P (ninja complete): a failing background connect is
    /// tolerated — connect still reports Complete, the failure only feeds the
    /// backoff machinery.
    #[tokio::test]
    async fn p2p_connect_failure_tolerated_when_ninja_complete() {
        let mut p2p_mem = Mem::new("p2p", one("f", b"data"), true);
        p2p_mem.connect_fail_times = u32::MAX;
        let (_pt, p2p) = typed(p2p_mem);
        let mut ms = MultiSource::new(Some(arc(Mem::new("ninja", one("f", b"data"), true))), Some(p2p), None);
        assert_eq!(ms.connect().await.unwrap(), StorageMetaState::Complete);

        let ctl = ms.inner.p2p_ctl.as_ref().unwrap().clone();
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            while ctl.fail_count.load(Ordering::Relaxed) == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("background connect failure never recorded");
        assert!(!ms.inner.p2p_active());
    }

    // ── P2P reconnect / backoff ────────────────────────────────────────────

    /// Failing reconnects retry on the backoff timer until the fail cap, then
    /// stop for good.
    #[tokio::test(start_paused = true)]
    async fn p2p_reconnect_gives_up_after_max_fails() {
        let mut p2p_mem = Mem::new("p2p", one("f", b"data"), true);
        p2p_mem.connect_fail_times = u32::MAX;
        let (p2p_t, p2p) = typed(p2p_mem);
        let ms = MultiSource::new(None, Some(p2p), None);
        let ctl = ms.inner.p2p_ctl.as_ref().unwrap().clone();

        ctl.on_error(); // the initial disconnect
        tokio::time::timeout(Duration::from_secs(300), async {
            while ctl.fail_count.load(Ordering::Relaxed) < P2P_MAX_FAILS {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("fail cap never reached");

        assert_eq!(
            p2p_t.read().await.connect_calls,
            P2P_MAX_FAILS - 1,
            "one reconnect attempt per pre-cap failure"
        );
        assert!(ctl.timer.lock().unwrap().is_none(), "no further retry scheduled");
        assert!(!ms.inner.p2p_active());
    }

    /// A reconnect that eventually succeeds reactivates the source and resets
    /// the backoff for the next outage.
    #[tokio::test(start_paused = true)]
    async fn p2p_reconnect_success_restores_active() {
        let mut p2p_mem = Mem::new("p2p", one("f", b"data"), true);
        p2p_mem.connect_fail_times = 2;
        let (p2p_t, p2p) = typed(p2p_mem);
        let ms = MultiSource::new(None, Some(p2p), None);
        let ctl = ms.inner.p2p_ctl.as_ref().unwrap().clone();

        ctl.on_error();
        tokio::time::timeout(Duration::from_secs(300), async {
            while !ms.inner.p2p_active() {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("reconnect never succeeded");

        assert_eq!(p2p_t.read().await.connect_calls, 3, "two failures, then the success");
        assert_eq!(
            ctl.backoff_ms.load(Ordering::Relaxed),
            P2P_BACKOFF_INITIAL_MS,
            "backoff reset on connect"
        );
    }

    // ── routing ────────────────────────────────────────────────────────────

    /// getDir source selection: P2P fast-path for non-recursive, complete
    /// ninja for recursive, P2P fallback while ninja is incomplete, error with
    /// no source.
    #[tokio::test]
    async fn get_dir_routes_by_state() {
        async fn first_key(
            rx: &mut mpsc::Receiver<Result<Vec<StorageObjectInfo>, StorageError>>,
        ) -> Result<String, StorageError> {
            match rx.recv().await.expect("one batch") {
                Ok(batch) => Ok(batch[0].key.clone()),
                Err(e) => Err(e),
            }
        }

        let (ninja_t, ninja) = typed(Mem::new("ninja", one("f", b"data"), true));
        let (_pt, p2p) = typed(Mem::new("p2p", one("f", b"data"), true));
        let ms = MultiSource::new(Some(ninja), Some(p2p), None);
        activate(&ms);

        let mut rx = ms.get_dir("", false, None, None).await;
        assert_eq!(first_key(&mut rx).await.unwrap(), "p2p", "non-recursive fast-path");

        let mut rx = ms.get_dir("", true, None, None).await;
        assert_eq!(
            first_key(&mut rx).await.unwrap(),
            "ninja",
            "recursive from complete ninja"
        );

        ninja_t.write().await.complete = false;
        let mut rx = ms.get_dir("", true, None, None).await;
        assert_eq!(
            first_key(&mut rx).await.unwrap(),
            "p2p",
            "incomplete ninja → P2P fallback"
        );

        // No peer: the incomplete ninja meta still lists what it has — the
        // still-uploading cloud share case.
        ms.inner
            .src_p2p
            .as_ref()
            .unwrap()
            .is_active
            .store(false, Ordering::Relaxed);
        let mut rx = ms.get_dir("", true, None, None).await;
        assert_eq!(
            first_key(&mut rx).await.unwrap(),
            "ninja",
            "incomplete ninja, no peer → ninja"
        );

        ms.inner
            .src_ninja
            .as_ref()
            .unwrap()
            .is_active
            .store(false, Ordering::Relaxed);
        let mut rx = ms.get_dir("", true, None, None).await;
        let err = first_key(&mut rx).await.unwrap_err();
        assert!(err.to_string().contains("no source"), "got: {err}");
    }

    /// Dir stats over an incomplete meta with no peer is not a number: the
    /// source answers `Incomplete`, so the engine defers sizing instead of
    /// reporting a partial total as if it were the whole.
    #[tokio::test]
    async fn dir_stats_on_incomplete_ninja_without_peer_is_incomplete() {
        let (_nt, ninja) = typed(Mem::new("ninja", one("f", b"data"), false));
        let ms = MultiSource::new(Some(ninja), None, None);
        activate(&ms);
        let mut rx = ms.get_dir_stats_streamed("").await;
        let err = rx.recv().await.expect("one item").unwrap_err();
        assert!(matches!(err, StorageError::Incomplete(_)), "got {err:?}");
        assert!(rx.recv().await.is_none(), "the stream ends with the error");
    }

    /// getSymlink prefers ninja and falls back to P2P when ninja can't serve.
    #[tokio::test]
    async fn get_symlink_falls_back_to_p2p() {
        let (ninja_t, ninja) = typed(Mem::new("ninja", one("f", b"data"), true));
        let mut p2p_mem = Mem::new("p2p", one("f", b"data"), true);
        p2p_mem.symlink = Some("p2p-target".into());
        let (_pt, p2p) = typed(p2p_mem);
        let ms = MultiSource::new(Some(ninja), Some(p2p), None);
        activate(&ms);

        assert_eq!(ms.get_symlink("l").await.unwrap(), "p2p-target", "ninja failed → P2P");

        ninja_t.write().await.symlink = Some("ninja-target".into());
        assert_eq!(ms.get_symlink("l").await.unwrap(), "ninja-target", "ninja preferred");
    }

    // ── session reopen (docs/app/reconnect.md, change set 3) ──────────────

    use crate::back::test_support::{Answer, MockBackend};
    use crate::back::{Back, BackState};

    fn session(back: Arc<RwLock<Back>>) -> SessionParams {
        SessionParams {
            back,
            urltoken: "tok".into(),
            key: None,
            ninja_params: crate::ninja::NinjaStorageV2Params::default(),
        }
    }

    /// A session marked gone (the keepalive's 404) is reopened before the
    /// next call touches it: one `download/start`, a new `transfer_id`, the
    /// call answered.
    #[tokio::test]
    async fn marked_session_reopens_before_use() {
        let mock = MockBackend::start().await;
        let ms = MultiSource::new(Some(arc(Mem::new("ninja", one("f", b"DATA"), true))), None, None);
        ms.bind_session_for_test(session(mock.back()));
        activate(&ms);

        ms.mark_expired();
        let attrs = ms.get_attrs("f").await.unwrap();
        assert_eq!(attrs.key, "f");
        assert_eq!(mock.count("download/start"), 1);
        assert_eq!(ms.transfer_id().as_deref(), Some("tid-1"));
        assert!(!ms.is_expired());
    }

    /// Concurrent callers on a lapsed session share one reopen.
    #[tokio::test]
    async fn reopen_is_single_flight() {
        let mock = MockBackend::start().await;
        let ms = MultiSource::new(Some(arc(Mem::new("ninja", one("f", b"DATA"), true))), None, None);
        ms.bind_session_for_test(session(mock.back()));
        activate(&ms);
        ms.mark_expired();

        let ms = Arc::new(ms);
        let tasks: Vec<_> = (0..8)
            .map(|_| {
                let ms = ms.clone();
                tokio::spawn(async move { ms.get_attrs("f").await.map(|a| a.key) })
            })
            .collect();
        for t in tasks {
            assert_eq!(t.await.unwrap().unwrap(), "f");
        }
        assert_eq!(mock.count("download/start"), 1);
    }

    /// A member that answers `SessionExpired` mid-stream: the read reopens
    /// and resumes from the bytes already delivered — every byte exactly
    /// once, one `download/start`.
    #[tokio::test]
    async fn read_resumes_after_reopen_mid_stream() {
        let mock = MockBackend::start().await;
        let mut mem = Mem::new("ninja", one("f", b"ABCDEF"), true);
        mem.serve_budget = Some(AtomicU64::new(3));
        mem.budget_kind = BudgetFailKind::SessionExpired;
        mem.refill_budget = Some(1000);
        let mem = arc(mem);
        let ms = MultiSource::new(Some(mem.clone()), None, None);
        ms.bind_session_for_test(session(mock.back()));
        activate(&ms);

        let got = ms.get("f", None, None).await.unwrap();
        assert_eq!(got, b"ABCDEF");
        assert_eq!(mock.count("download/start"), 1);
        assert_eq!(ms.transfer_id().as_deref(), Some("tid-1"));
    }

    /// A member that answers `Transport` while the backend is away: the read
    /// waits for `Online` and retries on the same session — no
    /// `download/start` spent on a network flap.
    #[tokio::test]
    async fn transport_waits_for_online_then_retries_same_session() {
        let mock = MockBackend::start().await;
        let back = mock.back_in(BackState::NoConnection);
        let mut mem = Mem::new("ninja", one("f", b"ABCDEF"), true);
        mem.serve_budget = Some(AtomicU64::new(3));
        mem.budget_kind = BudgetFailKind::Transport;
        mem.refill_budget = Some(1000);
        let ms = MultiSource::new(Some(arc(mem)), None, None);
        ms.bind_session_for_test(session(back.clone()));
        activate(&ms);

        let ms = Arc::new(ms);
        let reader = {
            let ms = ms.clone();
            tokio::spawn(async move { ms.get("f", None, None).await })
        };
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!reader.is_finished(), "the read must wait for the backend");

        back.read().await.set_state(BackState::Online);
        let got = tokio::time::timeout(Duration::from_secs(5), reader)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(got, b"ABCDEF");
        assert_eq!(mock.count("download/start"), 0);
        assert_eq!(ms.transfer_id().as_deref(), Some("tid-0"));
    }

    /// A dead wire while the WS still reads `Online` (the pong timeout has
    /// not fired yet) retries the same session instead of surfacing "ran out
    /// of sources" — the e2e freeze regression.
    #[tokio::test]
    async fn transport_while_ws_still_online_retries() {
        let mock = MockBackend::start().await;
        let mut mem = Mem::new("ninja", one("f", b"ABCDEF"), true);
        mem.serve_budget = Some(AtomicU64::new(0));
        mem.budget_kind = BudgetFailKind::Transport;
        mem.refill_budget = Some(1000);
        let ms = MultiSource::new(Some(arc(mem)), None, None);
        ms.bind_session_for_test(session(mock.back()));
        activate(&ms);

        let got = tokio::time::timeout(Duration::from_secs(5), ms.get("f", None, None))
            .await
            .expect("must retry, not park or surface")
            .unwrap();
        assert_eq!(got, b"ABCDEF");
        assert_eq!(mock.count("download/start"), 0, "a wire flap is not a lapsed session");
    }

    /// A lapse before the first byte (the keepalive has not marked it yet)
    /// reopens and serves the whole read — not "unavailable".
    #[tokio::test]
    async fn lapse_at_zero_bytes_reopens() {
        let mock = MockBackend::start().await;
        let mut mem = Mem::new("ninja", one("f", b"ABCDEF"), true);
        mem.serve_budget = Some(AtomicU64::new(0));
        mem.budget_kind = BudgetFailKind::SessionExpired;
        mem.refill_budget = Some(1000);
        let ms = MultiSource::new(Some(arc(mem)), None, None);
        ms.bind_session_for_test(session(mock.back()));
        activate(&ms);

        let got = ms.get("f", None, None).await.unwrap();
        assert_eq!(got, b"ABCDEF");
        assert_eq!(mock.count("download/start"), 1);
        assert_eq!(ms.transfer_id().as_deref(), Some("tid-1"));
    }

    /// `cancel()` ends the wait for the backend.
    #[tokio::test]
    async fn wait_for_online_ends_on_cancel() {
        let mock = MockBackend::start().await;
        let back = mock.back_in(BackState::NoConnection);
        let mut mem = Mem::new("ninja", one("f", b"ABCDEF"), true);
        mem.serve_budget = Some(AtomicU64::new(0));
        mem.budget_kind = BudgetFailKind::Transport;
        let ms = MultiSource::new(Some(arc(mem)), None, None);
        ms.bind_session_for_test(session(back));
        activate(&ms);

        let ms = Arc::new(ms);
        let reader = {
            let ms = ms.clone();
            tokio::spawn(async move { ms.get("f", None, None).await })
        };
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!reader.is_finished());
        ms.cancel().await.unwrap();
        let res = tokio::time::timeout(Duration::from_secs(5), reader)
            .await
            .unwrap()
            .unwrap();
        assert!(res.is_err(), "a cancelled read fails, it does not hang");
        assert_eq!(mock.count("download/start"), 0);
    }

    /// A P2P-only share whose peer refuses every reconnect: the fail cap
    /// runs out and the read surfaces as "ran out of sources"
    /// (`NoPartError` → sourceUnavailable) — never as a session problem,
    /// and no `download/start` is spent on it.
    #[tokio::test(start_paused = true)]
    async fn read_on_a_gone_peer_surfaces_without_a_reopen() {
        let mock = MockBackend::start().await;
        let mut p2p_mem = Mem::new("p2p", one("f", b"DATA"), true);
        p2p_mem.serve_budget = Some(AtomicU64::new(0));
        p2p_mem.connect_fail_times = u32::MAX;
        let ms = MultiSource::new(None, Some(arc(p2p_mem)), None);
        ms.bind_session_for_test(session(mock.back()));
        activate(&ms);

        let err = tokio::time::timeout(Duration::from_secs(600), ms.get("f", None, None))
            .await
            .expect("a gone peer must surface, not hang")
            .unwrap_err();
        assert!(matches!(err, StorageError::NoPartError(_)), "got {err:?}");
        assert_eq!(mock.count("download/start"), 0);
    }

    /// The P2P reconnect hears `SessionExpired`: the *session* is marked —
    /// no fail counted against the peer, no further dial — and the next
    /// entry point reopens and answers.
    #[tokio::test(start_paused = true)]
    async fn p2p_reconnect_session_expired_marks_the_session() {
        let mock = MockBackend::start().await;
        let (_nt, ninja) = typed(Mem::new("ninja", one("f", b"DATA"), true));
        let mut p2p_mem = Mem::new("p2p", one("f", b"DATA"), true);
        p2p_mem.connect_fail_times = 1;
        p2p_mem.connect_fail_expired = true;
        let (_p2p_t, p2p) = typed(p2p_mem);
        let ms = MultiSource::new(Some(ninja), Some(p2p), None);
        ms.bind_session_for_test(session(mock.back()));
        activate(&ms);
        let ctl = ms.inner.p2p_ctl.as_ref().unwrap().clone();

        ctl.on_error(); // the disconnect that schedules the redial
        tokio::time::timeout(Duration::from_secs(60), async {
            while !ms.is_expired() {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("the redial's SessionExpired never marked the session");
        assert_eq!(
            ctl.fail_count.load(Ordering::Relaxed),
            1,
            "the lapse is not a peer failure"
        );
        assert!(ctl.timer.lock().unwrap().is_none(), "no further dial on a dead session");

        // The next use reopens: one download/start, the peer redialed. The
        // reopen's HTTP runs under paused time — real IO needs the blocking
        // guard or the auto-advance jumps its timeouts.
        let attrs = MockBackend::with_io(ms.get_attrs("f")).await.unwrap();
        assert_eq!(attrs.key, "f");
        assert_eq!(mock.count("download/start"), 1);
        assert!(!ms.is_expired());
        // No redial to observe here: `bind_session_for_test` injects the
        // members without session refs — a reopen has nothing to renew (the
        // real member renewal exists only through `MultiSource::open`).
    }

    /// No session bound (injected members only): a lapse surfaces as
    /// `SessionExpired`, there is nothing to reopen with.
    #[tokio::test]
    async fn without_a_session_a_lapse_surfaces() {
        let ms = MultiSource::new(Some(arc(Mem::new("ninja", one("f", b"DATA"), true))), None, None);
        activate(&ms);
        ms.mark_expired();
        let err = ms.get_attrs("f").await.unwrap_err();
        assert!(matches!(err, StorageError::SessionExpired(_)), "got {err:?}");
    }

    // ── the real P2P leg against the mock backend ─────────────────────────

    use crate::transfers::p2p_storage::P2PStorage;

    /// `P2PStorage::connect` waits for the backend instead of spending a
    /// reconnect attempt on a fail-fast offline error; the first call goes
    /// out the moment `Online` arrives.
    #[tokio::test]
    async fn p2p_connect_waits_for_online() {
        let mock = MockBackend::start().await;
        mock.script("agents/call", &[Answer::NotFound]);
        let back = mock.back_in(crate::back::BackState::NoConnection);
        let storage = Arc::new(RwLock::new(P2PStorage::new(
            back.clone(),
            "agent-1".into(),
            "tid-0".into(),
            None,
        )));

        let task = {
            let storage = storage.clone();
            tokio::spawn(async move { storage.write().await.connect().await })
        };
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(mock.count("agents/call"), 0, "no call into a backend that is not there");
        assert!(!task.is_finished());

        back.read().await.set_state(crate::back::BackState::Online);
        let res = tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(res, Err(StorageError::SessionExpired(_))), "got {res:?}");
        let calls = mock.calls("agents/call");
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].body["transfer_id"], "tid-0");
    }

    /// The fail cap counts only refusals from a live backend: five minutes of
    /// `NoConnection` cost nothing, five real refusals run it out.
    #[tokio::test(start_paused = true)]
    async fn p2p_fail_cap_counts_only_live_refusals() {
        use crate::back::test_support::advance;
        let mock = MockBackend::start().await;
        let back = mock.back_in(crate::back::BackState::NoConnection);
        let storage: Arc<RwLock<dyn StorageSource>> = Arc::new(RwLock::new(P2PStorage::new(
            back.clone(),
            "agent-1".into(),
            "tid-0".into(),
            None,
        )));
        let ms = MultiSource::new(None, Some(storage), None);
        let ctl = ms.inner.p2p_ctl.as_ref().unwrap().clone();

        ctl.on_error(); // the initial disconnect schedules a redial
        advance(300).await;
        assert_eq!(mock.count("agents/call"), 0, "an offline wait is not an attempt");
        assert_eq!(ctl.fail_count.load(Ordering::Relaxed), 1, "only the disconnect counted");

        back.read().await.set_state(crate::back::BackState::Online);
        // Refusals now run the backoff ladder (5→10→20→30 s) to the cap.
        advance(120).await;
        assert_eq!(ctl.fail_count.load(Ordering::Relaxed), P2P_MAX_FAILS);
        assert_eq!(
            mock.count("agents/call") as u32,
            P2P_MAX_FAILS - 1,
            "one dial per pre-cap failure"
        );
        assert!(ctl.timer.lock().unwrap().is_none(), "no further retry scheduled");
        advance(300).await;
        assert_eq!(mock.count("agents/call") as u32, P2P_MAX_FAILS - 1);
    }
}
