//! The engine's UI-facing facts that have no core type of their own: links
//! and offers as the panels show them, the connection snapshot, the vault
//! join requests, and what a panel connects to. Core and API model types
//! are used directly everywhere they fit.

use chrono::{DateTime, FixedOffset};

pub use share_ninja_core::models;
pub use share_ninja_core::utils::device::Product;
pub use share_ninja_core::{BackParams, BackState};

/// Free and total bytes of what a panel sits on: the local volume under
/// the listed directory, or the account's storage quota for a share.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Space {
    pub free: u64,
    pub total: u64,
}

/// A local volume the panel selector offers: a Windows drive, or `/` and
/// the mounts under `/Volumes` on macOS.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Drive {
    /// The root to open a local storage on (`C:\`, `/`, `/Volumes/Data`).
    pub root: String,
    /// `C: fixed`, `Local`, the volume name.
    pub label: String,
    pub space: Option<Space>,
}

/// One of this account's (anonymous: this device's) links, `GET /links`.
#[derive(Clone, Debug, PartialEq)]
pub struct LinkRow {
    /// The decrypted description, else the token.
    pub name: String,
    pub token: String,
    /// `actual_bytes` once the upload finished, else the estimate.
    pub size: u64,
    pub created_at: DateTime<FixedOffset>,
    /// When the backend stops serving the link; `None` never.
    pub invalidate_at: Option<DateTime<FixedOffset>>,
    /// Cloud storage is attached: downloadable with the owner offline.
    pub cloud: bool,
    /// The owning agent was online for a direct transfer as of the listing.
    pub p2p: bool,
}

impl LinkRow {
    /// Something serves the share — without it there is nothing to enter.
    pub fn has_source(&self) -> bool {
        self.cloud || self.p2p
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum StorageType {
    Ninja,
    Local,
    /// An archive file browsed in place on another storage (read-only).
    Archive,
}

/// Lifecycle of a nearby offer we sent (the desktop app's
/// `OutboundOfferStatus`): `Sending` (creating link/offer) → `Waiting`
/// (delivered) → `Transferring` (peer pulling) → `Sent`; or `Declined`,
/// `NoResponse` (30 s), `Failed` (the peer's session died).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OfferStatus {
    Sending,
    Waiting,
    Transferring,
    Sent,
    Declined,
    NoResponse,
    Failed,
}

/// One outbound nearby offer as the panel shows it.
#[derive(Clone, Debug)]
pub struct OfferView {
    pub agent_id: String,
    pub status: OfferStatus,
    pub bytes: Option<u64>,
    pub total_bytes: Option<u64>,
}

impl OfferView {
    /// The status suffix a device row / the Offer detail row shows.
    pub fn label(&self) -> String {
        match self.status {
            OfferStatus::Sending => "sending…".to_string(),
            OfferStatus::Waiting => "waiting…".to_string(),
            OfferStatus::Transferring => match (self.bytes, self.total_bytes) {
                (Some(b), Some(t)) if t > 0 => format!("{}%", (b.min(t) * 100 / t)),
                (Some(b), _) => format!("{} B", b),
                _ => "transferring…".to_string(),
            },
            OfferStatus::Sent => "sent".to_string(),
            OfferStatus::Declined => "declined".to_string(),
            OfferStatus::NoResponse => "no answer".to_string(),
            OfferStatus::Failed => "failed".to_string(),
        }
    }
}

/// An inbound nearby offer as the accept / decline dialog shows it. The
/// share key it came with stays in the engine.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InboundOfferView {
    pub offer_token: String,
    /// The sender's device name.
    pub sender_name: String,
    /// The sender's user display name, when signed in.
    pub sender_display_name: Option<String>,
    /// The share's decrypted description; empty when the link fetch failed
    /// or the key did not unwrap.
    pub description: String,
    /// The link's declared size, when it declared one.
    pub bytes: Option<u64>,
}

/// A storage the engine holds open — what the panel selector lists.
#[derive(Clone, Debug)]
pub struct StorageInfo {
    pub storage_id: String,
    pub name: String,
    pub r#type: StorageType,
    /// The share URL / token a `Ninja` storage was opened from.
    pub link_url: Option<String>,
}

/// What to open in a panel.
#[derive(Clone, Debug)]
pub enum StorageConnectRequest {
    /// A local directory as the panel's root.
    Local { root_path: String },
    /// A Share.Ninja share URL / token (resolved through the session's keys).
    Ninja { link_url: String },
    /// An archive file to browse in place: `path` is its key on the open
    /// storage `storage_id` (a local root or a share).
    Archive { storage_id: String, path: String },
}

impl StorageConnectRequest {
    pub fn storage_type(&self) -> StorageType {
        match self {
            StorageConnectRequest::Local { .. } => StorageType::Local,
            StorageConnectRequest::Ninja { .. } => StorageType::Ninja,
            StorageConnectRequest::Archive { .. } => StorageType::Archive,
        }
    }
}

/// Current connection state (changes at runtime).
#[derive(Clone, Debug)]
pub struct BackendConnection {
    pub state: BackState,
    pub agent_id: String,
    pub agent_name: String,
    pub access_token: String,
    pub refresh_token: String,
    /// `not_initialized` / `not_joined` / `pending_approval` / `active`;
    /// `None` when anonymous.
    pub vault_status: Option<String>,
    /// The device-wrapped identity key the register response carried.
    pub encrypted_identity_key: Option<Vec<u8>>,
    pub user: Option<models::User>,
}

impl BackendConnection {
    pub fn is_online(&self) -> bool {
        matches!(self.state, BackState::Online | BackState::OnlineAndLoggedIn)
    }

    pub fn state_label(&self) -> String {
        let label = match self.state {
            BackState::NoConnection => "No connection",
            BackState::Connecting => "Connecting…",
            BackState::Online => "Online",
            BackState::OnlineAndLoggedIn => "Online, logged in",
        };
        label.to_string()
    }
}

/// The connection pane's snapshot of `Back` (which sits behind an async
/// lock the draw cannot take).
#[derive(Clone, Debug)]
pub struct BackendStatus {
    pub params: BackParams,
    pub connection: BackendConnection,
}

/// A pending join request — one shape for the two WS frames that carry
/// them (`vault.join_requests_snapshot` rows have `expires_at`,
/// `vault.device_join_requested` does not).
#[derive(Clone, Debug)]
pub struct VaultJoinRequest {
    pub request_id: String,
    pub agent_id: String,
    pub device_name: String,
    pub device_fingerprint: String,
    /// What the approval wraps our identity to.
    pub device_public_key: Vec<u8>,
    pub ip: Option<String>,
    pub country: Option<String>,
    /// The app the requesting agent registered as; `None` when it sent no
    /// product id.
    pub product: Option<Product>,
    pub expires_at: Option<DateTime<FixedOffset>>,
}

/// The dialog's "App" line for a product (English, like the rest of the UI).
pub fn product_label(product: &Product) -> String {
    match product {
        Product::Web => "Share.Ninja web app".to_string(),
        Product::Cli => "Share.Ninja command line".to_string(),
        Product::Platform(name) => format!("Share.Ninja for {name}"),
        Product::Unknown(id) => id.clone(),
    }
}

/// A trust-circle device our pending join request went to for approval
/// (`VaultDeviceJoinResponse.approvers`) — where the user confirms. A
/// snapshot as of the (re)sent request.
#[derive(Clone, Debug)]
pub struct VaultJoinApprover {
    pub display_name: String,
    /// Device line derived from the announced profile ("Safari on macOS");
    /// `None` when the profile yields nothing.
    pub desc: Option<String>,
    pub online: bool,
    pub last_seen_at: DateTime<FixedOffset>,
}

/// Our standing join request: the fingerprint the approver verifies and the
/// devices that can approve it (empty when the circle has no device left).
#[derive(Clone, Debug)]
pub struct PendingJoin {
    pub fingerprint: String,
    pub approvers: Vec<VaultJoinApprover>,
}

/// This device's vault standing, as the UI renders it — taken as one
/// snapshot to avoid render-time tearing.
#[derive(Clone, Debug, Default)]
pub struct VaultStateSnapshot {
    pub status: Option<String>,
    pub identity_loaded: bool,
    pub pending_join_requests: Vec<VaultJoinRequest>,
    /// BIP39 mnemonic shown once after setup or regenerate.
    pub setup_recovery_words: Option<Vec<String>>,
    /// Set after `vault.device_revoked` named us; cleared when acked.
    pub revoked_pending: bool,
    /// Our join request while it is pending: fingerprint + approvers.
    pub pending_join: Option<PendingJoin>,
}

/// Outcome of a step-up-gated vault call (recover): done, or parked behind
/// the emailed code.
#[derive(Clone, Debug)]
pub enum StepUpOutcome {
    Completed,
    /// The backend emailed a code; collect it and verify. `expires_at` is
    /// when the backend stops accepting it (RFC 3339).
    ChallengeRequired {
        expires_at: String,
    },
}

pub type NearbyDevice = models::WsNearbyAllOfNearby;
