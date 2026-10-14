//! Paged name search over a walked tree, shared by `FsSource` (the upload
//! roots) and `LocalFs` (a file-manager root): the producer-side filter and
//! batch sink the walk pushes into, and the pager that drains the walk's
//! batch receiver into pages of `page_limit` entries, parking the live walk
//! between page requests under a page token.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::mpsc;

use crate::transfers::{SearchOptions, SearchResult, StorageError, StorageObjectInfo, search_entry_wire_estimate};

/// Entries walked between receiver-liveness probes during a filtered (search)
/// walk. Matches can be arbitrarily sparse there, so the batch-send
/// backpressure that normally surfaces a dropped receiver may never fire —
/// the probe is what stops an abandoned search from walking the whole tree.
const SEARCH_CANCEL_CHECK_EVERY: u32 = 4096;
/// A parked search walk (see `SearchCursor`) is dropped if no follow-up page
/// request arrives within this window.
const SEARCH_CURSOR_TTL: std::time::Duration = std::time::Duration::from_secs(60);
/// Producer batch size for search walks — small enough that a page cut lands
/// near a batch boundary, and cheap because only matches cross the channel.
pub(super) const SEARCH_BATCH: usize = 256;
/// The page size when the caller names none.
const DEFAULT_PAGE_LIMIT: usize = 1000;

/// The walk's batch stream: what a search producer sends and the pager drains.
pub(super) type BatchReceiver = mpsc::Receiver<Result<Vec<StorageObjectInfo>, StorageError>>;

/// Batches walked entries and streams them over a channel via `blocking_send`,
/// preserving backpressure from the blocking producer thread. A dropped receiver
/// surfaces as `Err(Cancelled)` from `push`, so the walk unwinds quietly.
pub(super) struct BatchSink {
    pub(super) tx: mpsc::Sender<Result<Vec<StorageObjectInfo>, StorageError>>,
    pub(super) buf: Vec<StorageObjectInfo>,
    pub(super) limit: usize,
}

impl BatchSink {
    pub(super) fn push(&mut self, soi: StorageObjectInfo) -> Result<(), StorageError> {
        self.buf.push(soi);
        if self.buf.len() >= self.limit {
            self.tx
                .blocking_send(Ok(std::mem::take(&mut self.buf)))
                .map_err(|_| StorageError::Cancelled)?;
        }
        Ok(())
    }

    pub(super) fn flush(&mut self) {
        if !self.buf.is_empty() {
            let _ = self.tx.blocking_send(Ok(std::mem::take(&mut self.buf)));
        }
    }

    pub(super) fn send_err(&self, e: StorageError) {
        let _ = self.tx.blocking_send(Err(e));
    }
}

/// Case-insensitive `needle`-in-`basename` without allocating for an all-ASCII
/// basename (the overwhelmingly common case); a non-ASCII basename falls back
/// to a full Unicode lowercase, matching `StorageMeta::search`. `needle` must
/// be lowercased and non-empty (`windows(0)` would panic).
pub(super) fn basename_contains(basename: &str, needle: &str) -> bool {
    if basename.is_ascii() {
        basename
            .as_bytes()
            .windows(needle.len())
            .any(|w| w.eq_ignore_ascii_case(needle.as_bytes()))
    } else {
        basename.to_lowercase().contains(needle)
    }
}

/// Producer-side search filter: only entries whose basename contains `needle`
/// are materialized and sent — non-matches never become `StorageObjectInfo`s
/// or cross the channel. The walk runs to the end of the tree unless the
/// receiver goes away (a dropped or reaped cursor).
pub(super) struct SearchFilter {
    /// Lowercased, non-empty needle; `None` admits every entry.
    pub(super) needle: Option<String>,
    /// Entries seen since the last receiver-liveness probe.
    pub(super) visited: u32,
}

impl SearchFilter {
    /// The filter for a query: an empty `name_match` admits everything.
    pub(super) fn new(name_match: Option<&str>) -> Self {
        let needle = name_match.filter(|m| !m.is_empty()).map(|m| m.to_lowercase());
        Self { needle, visited: 0 }
    }
}

/// Push one walked entry, applying the search filter when present: the
/// basename is tested before `make` builds the `StorageObjectInfo`, and the
/// filtered walk is ended via `Cancelled` when the receiver is gone (the
/// producer flushes on that exit too).
pub(super) fn push_filtered(
    sink: &mut BatchSink,
    filter: &mut Option<SearchFilter>,
    basename: &str,
    make: impl FnOnce() -> StorageObjectInfo,
) -> Result<(), StorageError> {
    let Some(f) = filter else {
        return sink.push(make());
    };
    f.visited += 1;
    if f.visited >= SEARCH_CANCEL_CHECK_EVERY {
        f.visited = 0;
        if sink.tx.is_closed() {
            return Err(StorageError::Cancelled);
        }
    }
    if let Some(needle) = &f.needle
        && !basename_contains(basename, needle)
    {
        return Ok(());
    }
    sink.push(make())
}

/// A live search walk parked between page requests. The pager cuts a page
/// when it holds `page_limit` entries or the caller's deadline or byte budget
/// runs out, and parks the walk here under the returned page token; the next
/// request with that token resumes draining the same producer — nothing is
/// re-walked. The producer sits blocked on its bounded channel while parked,
/// which is why parked cursors are TTL-reaped.
struct SearchCursor {
    rx: BatchReceiver,
    /// Drained from `rx` but not yet returned (a page cut can split a batch).
    pending: VecDeque<StorageObjectInfo>,
    /// Bumped on every park; a TTL reaper only evicts the generation it was
    /// armed for, so it can't kill a cursor that was resumed and re-parked.
    generation: u64,
}

/// The page-token side of a source's search: its parked walks and the token
/// id source. Cross-requester token collisions are harmless: a token only
/// resumes a walk over data the requester already has access to.
pub(super) struct SearchPager {
    /// `Arc` so the TTL reaper tasks can reach the map after `page` returns.
    cursors: Arc<tokio::sync::Mutex<HashMap<String, SearchCursor>>>,
    seq: AtomicU64,
}

impl SearchPager {
    pub(super) fn new() -> Self {
        Self {
            cursors: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            seq: AtomicU64::new(0),
        }
    }

    /// One page of a search: a request without a page token opens the walk
    /// via `open` — the producer's batch receiver, plus any entries to emit
    /// ahead of it — a request with one resumes the parked walk. The page is
    /// cut at `page_limit` entries, the deadline, or the byte budget, and the
    /// walk parked under the returned token; a walk that ran out answers
    /// without a token.
    pub(super) async fn page(
        &self,
        options: &SearchOptions,
        open: impl FnOnce() -> (BatchReceiver, VecDeque<StorageObjectInfo>),
    ) -> Result<SearchResult, StorageError> {
        let page_limit = options
            .page_limit
            .filter(|l| *l > 0)
            .map(|l| l as usize)
            .unwrap_or(DEFAULT_PAGE_LIMIT);
        let deadline = options.deadline.map(|d| tokio::time::Instant::now() + d);

        let mut cursor = match options.page_token.as_deref().filter(|t| !t.is_empty()) {
            Some(token) => self
                .cursors
                .lock()
                .await
                .remove(token)
                .ok_or_else(|| StorageError::Other(format!("search cursor expired: {token}")))?,
            None => {
                let (rx, pending) = open();
                SearchCursor {
                    rx,
                    pending,
                    generation: 0,
                }
            }
        };

        let mut page: Vec<StorageObjectInfo> = Vec::new();
        let mut page_bytes = 0usize;
        let mut parked = false; // page cut with the walk still live

        'page: loop {
            while let Some(soi) = cursor.pending.pop_front() {
                if page.len() >= page_limit {
                    cursor.pending.push_front(soi);
                    parked = true;
                    break 'page;
                }
                let est = search_entry_wire_estimate(&soi.key);
                if let Some(max) = options.max_response_bytes
                    && !page.is_empty()
                    && page_bytes + est > max
                {
                    cursor.pending.push_front(soi);
                    parked = true;
                    break 'page;
                }
                page_bytes += est;
                page.push(soi);
            }
            let received = match deadline {
                Some(dl) => match tokio::time::timeout_at(dl, cursor.rx.recv()).await {
                    Ok(r) => r,
                    Err(_) => {
                        parked = true;
                        break 'page;
                    }
                },
                None => cursor.rx.recv().await,
            };
            match received {
                Some(batch) => cursor.pending.extend(batch?),
                None => break 'page, // producer finished: tree walked
            }
        }

        if parked {
            let token = self.park(cursor, options.page_token.clone()).await;
            return Ok(SearchResult {
                items: page,
                next_page_token: Some(token),
            });
        }
        // Terminal page: dropping the cursor drops `rx`, which stops a
        // still-running producer via its receiver-liveness probe.
        Ok(SearchResult {
            items: page,
            next_page_token: None,
        })
    }

    /// Park a cut page's walk under its (reused, or freshly minted) token and
    /// arm a TTL reaper — an abandoned cursor must not hold its producer
    /// thread forever. The generation bump keeps an older reaper from
    /// evicting a cursor that has been resumed and re-parked since.
    async fn park(&self, mut cursor: SearchCursor, token: Option<String>) -> String {
        let token = token
            .filter(|t| !t.is_empty())
            .unwrap_or_else(|| format!("s{}", self.seq.fetch_add(1, Ordering::Relaxed) + 1));
        cursor.generation += 1;
        let generation = cursor.generation;
        self.cursors.lock().await.insert(token.clone(), cursor);
        let cursors = Arc::clone(&self.cursors);
        let reap_token = token.clone();
        tokio::spawn(async move {
            tokio::time::sleep(SEARCH_CURSOR_TTL).await;
            let mut map = cursors.lock().await;
            if map.get(&reap_token).is_some_and(|c| c.generation == generation) {
                map.remove(&reap_token);
            }
        });
        token
    }
}
