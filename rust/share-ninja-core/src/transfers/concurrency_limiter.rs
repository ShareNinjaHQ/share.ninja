//! ConcurrencyLimiter — semaphore-style download gate.
//!
//! Replaces the work-phase of the old OrderedPromisePool. Pure backpressure,
//! no ordering. The reorder concern belongs to [`MultipartReorderDest`] now.
//!
//! Usage shape (mirrors the TS):
//! ```ignore
//! limiter.acquire().await?;
//! tokio::spawn({
//!     let limiter = limiter.clone();
//!     async move {
//!         match do_work().await {
//!             Ok(_) => {}
//!             Err(e) => { limiter.cancel(Some(format!("{e}"))).await; }
//!         }
//!         limiter.release().await;
//!     }
//! });
//! // ...
//! limiter.await_all().await?;
//! ```
//!
//! Error propagation: a failing task calls [`ConcurrencyLimiter::cancel`] with the
//! error message, after which subsequent `acquire` and `await_all` return
//! [`StorageError::Cancelled`] (with the stored message preserved in the log).
//! This mirrors the TS `OrderedPromisePool.onIdle()` behavior.

use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::{Mutex, oneshot};

use super::StorageError;

struct Inner {
    limit: usize,
    in_use: usize,
    waiters: VecDeque<oneshot::Sender<Result<(), StorageError>>>,
    idle_waiters: Vec<oneshot::Sender<Result<(), StorageError>>>,
    stopped: bool,
    stop_reason: Option<String>,
}

/// Semaphore-style download gate with dynamic limit and cancellable await-all.
#[derive(Clone)]
pub struct ConcurrencyLimiter {
    inner: Arc<Mutex<Inner>>,
}

impl ConcurrencyLimiter {
    /// `limit` must be >= 1.
    pub fn new(limit: usize) -> Self {
        assert!(limit >= 1, "limit must be >= 1");
        Self {
            inner: Arc::new(Mutex::new(Inner {
                limit,
                in_use: 0,
                waiters: VecDeque::new(),
                idle_waiters: Vec::new(),
                stopped: false,
                stop_reason: None,
            })),
        }
    }

    pub async fn set_limit(&self, n: usize) {
        assert!(n >= 1, "limit must be >= 1");
        let mut inner = self.inner.lock().await;
        inner.limit = n;
        Self::notify_acquire_waiters(&mut inner);
    }

    pub async fn get_limit(&self) -> usize {
        self.inner.lock().await.limit
    }

    pub async fn in_flight(&self) -> usize {
        self.inner.lock().await.in_use
    }

    /// Try to take a slot. Returns immediately if a slot is free, otherwise
    /// blocks (FIFO) until one becomes available. Returns
    /// [`StorageError::Cancelled`] if the limiter is stopped.
    pub async fn acquire(&self) -> Result<(), StorageError> {
        let rx = {
            let mut inner = self.inner.lock().await;
            if inner.stopped {
                return Err(Self::cancelled_err(&inner));
            }
            if inner.in_use < inner.limit {
                inner.in_use += 1;
                return Ok(());
            }
            let (tx, rx) = oneshot::channel();
            inner.waiters.push_back(tx);
            rx
        };
        match rx.await {
            Ok(result) => result,
            Err(_) => Err(StorageError::Cancelled), // sender dropped
        }
    }

    /// Release a slot acquired via [`Self::acquire`]. Pairs 1:1; calling
    /// without a held slot is a no-op (logged warning).
    pub async fn release(&self) {
        let mut inner = self.inner.lock().await;
        if inner.in_use == 0 {
            tracing::warn!("ConcurrencyLimiter::release() called with no slots in use");
            return;
        }
        inner.in_use -= 1;
        Self::notify_acquire_waiters(&mut inner);
        Self::notify_idle_waiters_if_idle(&mut inner);
    }

    /// Block until all slots are released. Returns
    /// [`StorageError::Cancelled`] if the limiter was cancelled at any point.
    pub async fn await_all(&self) -> Result<(), StorageError> {
        let rx = {
            let mut inner = self.inner.lock().await;
            if inner.in_use == 0 {
                if inner.stopped {
                    return Err(Self::cancelled_err(&inner));
                }
                return Ok(());
            }
            let (tx, rx) = oneshot::channel();
            inner.idle_waiters.push(tx);
            rx
        };
        match rx.await {
            Ok(result) => result,
            Err(_) => Err(StorageError::Cancelled),
        }
    }

    /// Stop the limiter. Pending acquirers reject immediately with
    /// `Cancelled`. Subsequent `acquire`/`await_all` calls also reject.
    /// `await_all` callers waiting on in-flight work will reject once the
    /// last `release()` lands. Idempotent.
    pub async fn cancel(&self, reason: Option<String>) {
        let mut inner = self.inner.lock().await;
        if inner.stopped {
            return;
        }
        inner.stopped = true;
        inner.stop_reason = reason;

        // Reject all parked acquirers.
        let parked: Vec<_> = inner.waiters.drain(..).collect();
        for tx in parked {
            let _ = tx.send(Err(Self::cancelled_err(&inner)));
        }

        if inner.in_use == 0 {
            let idle = std::mem::take(&mut inner.idle_waiters);
            for tx in idle {
                let _ = tx.send(Err(Self::cancelled_err(&inner)));
            }
        }
    }

    pub async fn is_stopped(&self) -> bool {
        self.inner.lock().await.stopped
    }

    fn notify_acquire_waiters(inner: &mut Inner) {
        while !inner.waiters.is_empty() && inner.in_use < inner.limit {
            if let Some(tx) = inner.waiters.pop_front() {
                inner.in_use += 1;
                // If the receiver was dropped, return the slot.
                if tx.send(Ok(())).is_err() {
                    inner.in_use -= 1;
                }
            }
        }
    }

    fn notify_idle_waiters_if_idle(inner: &mut Inner) {
        if inner.in_use > 0 {
            return;
        }
        let waiters = std::mem::take(&mut inner.idle_waiters);
        for tx in waiters {
            if inner.stopped {
                let _ = tx.send(Err(Self::cancelled_err(inner)));
            } else {
                let _ = tx.send(Ok(()));
            }
        }
    }

    fn cancelled_err(inner: &Inner) -> StorageError {
        if let Some(ref r) = inner.stop_reason {
            tracing::debug!("limiter cancelled: {}", r);
        }
        StorageError::Cancelled
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn basic_acquire_release() {
        let lim = ConcurrencyLimiter::new(2);
        lim.acquire().await.unwrap();
        lim.acquire().await.unwrap();
        assert_eq!(lim.in_flight().await, 2);
        lim.release().await;
        lim.release().await;
        assert_eq!(lim.in_flight().await, 0);
    }

    #[tokio::test]
    async fn acquire_blocks_at_limit() {
        let lim = ConcurrencyLimiter::new(1);
        lim.acquire().await.unwrap();

        let lim2 = lim.clone();
        let h = tokio::spawn(async move { lim2.acquire().await });

        // Give the spawned task a chance to park.
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert!(!h.is_finished());

        lim.release().await;
        let r = h.await.unwrap();
        assert!(r.is_ok());
        assert_eq!(lim.in_flight().await, 1);
    }

    #[tokio::test]
    async fn await_all_idles_when_empty() {
        let lim = ConcurrencyLimiter::new(2);
        // No slots held; await_all returns immediately.
        lim.await_all().await.unwrap();
    }

    #[tokio::test]
    async fn await_all_waits_for_release() {
        let lim = ConcurrencyLimiter::new(2);
        lim.acquire().await.unwrap();
        lim.acquire().await.unwrap();

        let lim2 = lim.clone();
        let h = tokio::spawn(async move { lim2.await_all().await });

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert!(!h.is_finished());

        lim.release().await;
        lim.release().await;
        let r = h.await.unwrap();
        assert!(r.is_ok());
    }

    #[tokio::test]
    async fn cancel_rejects_pending_acquirers() {
        let lim = ConcurrencyLimiter::new(1);
        lim.acquire().await.unwrap();

        let lim2 = lim.clone();
        let h = tokio::spawn(async move { lim2.acquire().await });

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        lim.cancel(Some("test cancel".into())).await;

        let r = h.await.unwrap();
        assert!(matches!(r, Err(StorageError::Cancelled)));
    }

    #[tokio::test]
    async fn cancel_after_idle_rejects_await_all() {
        let lim = ConcurrencyLimiter::new(1);
        lim.cancel(None).await;
        let r = lim.await_all().await;
        assert!(matches!(r, Err(StorageError::Cancelled)));
    }

    #[tokio::test]
    async fn dynamic_set_limit_wakes_parked() {
        let lim = ConcurrencyLimiter::new(1);
        lim.acquire().await.unwrap();

        let lim2 = lim.clone();
        let h = tokio::spawn(async move { lim2.acquire().await });

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert!(!h.is_finished());

        // Raising the limit should immediately wake the parked acquirer.
        lim.set_limit(2).await;
        let r = h.await.unwrap();
        assert!(r.is_ok());
        assert_eq!(lim.in_flight().await, 2);
    }
}
