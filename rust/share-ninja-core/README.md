# share_ninja_core

Rust library for Share.Ninja - secure file transfers with S3 storage, P2P connections, and encrypted storage.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    share_ninja_core                         │
│  ┌──────┐  ┌──────────────┐                                 │
│  │ Back │  │ TransferItem │                                 │
│  └──────┘  └──────────────┘                                 │
│                                                             │
│  Sources:                    Destinations:                  │
│  ┌───────────┐ ┌───────────┐ ┌───────────┐ ┌───────────┐    │
│  │ S3Storage │ │ P2PStorage│ │ StreamDest│ │  ZipDest  │    │
│  └───────────┘ └───────────┘ └───────────┘ └───────────┘    │
│  ┌───────────────┐ ┌───────────┐                            │
│  │NinjaStorageV2 │ │MultiSource│                            │
│  └───────────────┘ └───────────┘                            │
│                                                             │
│  ┌────────────┐                                             │
│  │ CallServer │  P2P call handling                          │
│  └────────────┘                                             │
└─────────────────────────────────────────────────────────────┘
```

## Modules

- **`back`** - Backend API client, authentication, transfer coordination
- **`transfers`** - Storage interfaces, S3, P2P, and transfer orchestration
- **`ninja`** - Encrypted storage with journaling (NinjaStorageV2)
- **`p2p`** - WebRTC peer-to-peer connections and file serving
- **`utils`** - Base64 encoding, Result codes, speed calculation

## Key Types

### Storage Traits

- `Storage` - Base trait for all storage types
- `StorageSource` - Read operations (get, get_symlink)
- `StorageDest` - Write operations (put_file, put_dir, put_symlink)
- `StorageDestMultipart` - Multipart upload support

### Storage Implementations

- `S3Storage` - S3 over backend-presigned URLs (plain HTTP; no S3 SDK, no credentials on the client)
- `P2PStorage` - WebRTC peer-to-peer storage (source only)
- `NinjaStorageV2` - Encrypted storage with journaling and ZIP batching
- `MultiSource` - Combines multiple sources with failover

### Destinations

- `StreamDest` - Writes to a byte buffer (sequential)
- `ZipDest` - Writes to a ZIP archive

### Transfer

- `TransferItem` - Orchestrates file transfers between sources and destinations
- `Back` - Backend API client for authentication and transfer coordination

## Usage

```rust
use share_ninja_core::{Back, BackParams, TransferItem};
use std::sync::Arc;
use tokio::sync::RwLock;

#[tokio::main]
async fn main() {
    // Create backend client
    let params = BackParams {
        host: "share.ninja".to_string(),
        prefix: "/api".to_string(),
        product_id: "my-app".to_string(),
        product_version: "1.0.0".to_string(),
        // ... other params
    };

    let back = Back::new_shared(params).await;

    // Register with backend (lock-light; also wires auto-reconnect)
    Back::run_register(back.clone()).await.unwrap();

    // Create a link for sharing
    // ... see Back::link(), Back::upload_start(), Back::download_start()
}
```

## Dependencies

- `share-ninja-api` - Auto-generated backend API client
- `webrtc` - WebRTC support (webrtc-rs)
- `tokio` - Async runtime
- `aes-gcm` - AES-GCM encryption
- `zip` - ZIP archive support
