//! Transfer-related modules for storage sources and destinations

mod concurrency_limiter;
mod multi_source;
mod multipart_reorder_dest;
mod p2p_storage;
mod preview_state;
pub mod policy;
pub mod s3_storage;
mod selection;
mod storage;
mod stream_dest;
#[cfg(test)]
pub(crate) mod test_support;
mod transfer;
mod transfer_item;
mod zip_dest;

pub use concurrency_limiter::ConcurrencyLimiter;
pub use multi_source::{MultiSource, MultiSourceParams};
pub use multipart_reorder_dest::MultipartReorderDest;
pub use p2p_storage::P2PStorage;
pub use policy::{
    Conflict, ConflictDecision, ConflictResolver, ErrorDecision, ErrorResolver, OnError, OnExisting, TransferPolicies,
};
pub use s3_storage::S3Storage;
pub use selection::{
    ScopeCompletion, collapse_into_parent, complete_dirs_cover, covers, merge_complete_dirs, normalize_complete_dirs,
    normalize_selection, selection_covers,
};
pub use storage::*;
pub use stream_dest::StreamDest;
pub use transfer::{TransferDownload, TransferHandle, TransferUpload};
pub use transfer_item::{
    TransferItem, TransferItemState, TransferItemStats, TransferItemStatsSnapshot, TransferProgress,
};
pub use zip_dest::ZipDest;
