//! Share.Ninja Core Library
//!
//! Rust library for secure file transfers with S3 storage, P2P connections,
//! and encrypted storage. This is a Rust port of the TypeScript sharon-lib.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                      share-ninja-lib                        │
//! │  ┌──────┐  ┌──────────────┐                                 │
//! │  │ Back │  │ TransferItem │                                 │
//! │  └──────┘  └──────────────┘                                 │
//! │                                                             │
//! │  Sources:                    Destinations:                  │
//! │  ┌───────────┐ ┌───────────┐ ┌───────────┐ ┌───────────┐    │
//! │  │ S3Storage │ │ P2PStorage│ │ StreamDest│ │  ZipDest  │    │
//! │  └───────────┘ └───────────┘ └───────────┘ └───────────┘    │
//! │  ┌───────────────┐ ┌───────────┐                            │
//! │  │NinjaStorageV2 │ │MultiSource│                            │
//! │  └───────────────┘ └───────────┘                            │
//! │                                                             │
//! │  ┌────────────┐                                             │
//! │  │ CallServer │  P2P call handling                          │
//! │  └────────────┘                                             │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Modules
//!
//! - [`back`] - Backend API client, auth, transfer coordination
//! - [`fs`] - Local filesystem source/dest (streamed walk, multipart meta store)
//! - [`transfers`] - Storage interfaces, S3, P2P, and transfer orchestration
//! - [`ninja`] - Encrypted storage with journaling (NinjaStorageV2)
//! - [`p2p`] - WebRTC peer-to-peer connections and file serving
//! - [`secrets`] - Injected secret store + the device-secret blob over it
//! - [`vault`] - Device-side vault: join the trust circle, unlock the identity key
//! - [`utils`] - Base64, Result codes, speed calculation

pub mod back;
pub mod fs;
pub mod ninja;
pub mod p2p;
pub mod secrets;
pub mod transfers;
pub mod utils;
pub mod vault;

// Re-export main types
pub use back::{Back, BackError, BackParams, BackState, DebugLoggingMiddleware, RequestIdMiddleware};
// Re-export the generated API models that appear in the public API (e.g. the
// request types taken by `Back::vault_claim_*`) so downstream crates can
// construct them without depending on `share-ninja-api` directly.
pub use ninja::{NinjaStorageV2, NinjaStorageV2Params};
pub use p2p::{CallServer, CallServerShare};
pub use secrets::{MemorySecretStore, SecretStore, SecureStore};
pub use share_ninja_api::models;
pub use transfers::{
    MultiSource, P2PStorage, S3Storage, Storage, StorageChange, StorageDest, StorageDestMultipart, StorageError,
    StorageFileAttrs, StorageFileType, StorageMeta, StorageMetaState, StorageMultipartUpload, StorageObjectInfo,
    StoragePartInfo, StorageSource, StreamDest, ZipDest,
};
pub use utils::result::Result as NinjaResult;
