//! Transfer policies — what the engine does when a destination entry already
//! exists, and when one unit of work (a file, dir, symlink, or multipart part)
//! fails. Both default to today's engine behaviour (`Skip` existing = resume
//! credit; `Fail` = the first error ends the transfer); a driver that wants
//! interactive resolution (a file manager) supplies `Ask` + a resolver and
//! reads the parked state off the item.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;

use super::{StorageError, StorageObjectInfo};

/// What to do with a source entry whose destination already holds a
/// complete entry of the same kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OnExisting {
    /// Leave the destination as is and count it as done (resume semantics).
    #[default]
    Skip,
    /// Delete the destination entry and transfer the source one.
    Overwrite,
    /// `Overwrite` when the source is newer than the destination, else `Skip`.
    /// Unknown mtimes compare as older.
    KeepNewer,
    /// `Overwrite` when the source is larger than the destination, else `Skip`.
    KeepLarger,
    /// `Overwrite` when the sizes differ, else `Skip`.
    IfSizeDiffers,
    /// Collect every conflict up front (during `Preparing`), park in
    /// `HasConflicts`, and ask the [`ConflictResolver`] once. Without a
    /// resolver this behaves as `Skip`.
    Ask,
}

/// One existing-destination conflict: the source entry and what the
/// destination holds under `key` — the *dest* key (the source key minus the
/// item's source root; identical without one).
#[derive(Debug, Clone)]
pub struct Conflict {
    pub key: String,
    pub source: StorageObjectInfo,
    pub dest: StorageObjectInfo,
}

/// The resolver's answer for a batch of conflicts.
#[derive(Debug, Clone)]
pub enum ConflictDecision {
    /// Per-key decisions; keys not listed fall back to `All`-less default (`Skip`).
    Each(HashMap<String, OnExisting>),
    /// One decision for every conflict in the batch (and for any conflict a
    /// live source surfaces later).
    All(OnExisting),
    /// Abort the transfer before any byte moves.
    Cancel,
}

#[async_trait]
pub trait ConflictResolver: Send + Sync {
    async fn resolve(&self, conflicts: Vec<Conflict>) -> ConflictDecision;
}

/// What to do when one unit of work fails with a per-file error.
/// `Cancelled`, `NoSpace` and `SessionExpired` are never per-file choices —
/// they end the transfer regardless.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OnError {
    /// The first error ends the transfer (today's behaviour).
    #[default]
    Fail,
    /// Skip the failing entry, keep going; skipped keys are listed on the item.
    Skip,
    /// Park in `HasError` and ask the [`ErrorResolver`]. Without a resolver
    /// this behaves as `Fail`.
    Ask,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorDecision {
    /// Run the failed unit again.
    Retry,
    /// Skip the entry (a failed part skips its whole file) and keep going.
    Skip,
    /// End the transfer with this error.
    Cancel,
}

/// `key` is the failing unit's dest key.
#[async_trait]
pub trait ErrorResolver: Send + Sync {
    async fn resolve(&self, key: &str, error: &StorageError) -> ErrorDecision;
}

/// The two policies a transfer item runs under.
#[derive(Clone, Default)]
pub struct TransferPolicies {
    pub on_existing: OnExisting,
    pub conflict_resolver: Option<Arc<dyn ConflictResolver>>,
    pub on_error: OnError,
    pub error_resolver: Option<Arc<dyn ErrorResolver>>,
}

impl std::fmt::Debug for TransferPolicies {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransferPolicies")
            .field("on_existing", &self.on_existing)
            .field("conflict_resolver", &self.conflict_resolver.is_some())
            .field("on_error", &self.on_error)
            .field("error_resolver", &self.error_resolver.is_some())
            .finish()
    }
}
