//! Engine → UI events. The sync TUI loop drains this channel once per tick;
//! the engine (and the tasks the loop spawns on it) push into it.

use tokio::sync::mpsc;

use share_ninja_core::StorageObjectInfo;
use share_ninja_core::transfers::{Conflict, SearchResult, TransferItemState, TransferProgress};

use crate::types::{
    BackendStatus, Drive, InboundOfferView, LinkRow, NearbyDevice, OfferView, Space, StepUpOutcome, StorageInfo,
    StorageType, VaultJoinRequest, VaultStateSnapshot, models,
};
use crate::viewer::SearchOutcome;

/// Panel identifier for routing responses
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PanelId {
    #[default]
    Left,
    Right,
}

/// A copy to run on the transfer engine.
#[derive(Debug, Clone)]
pub struct StartTransfer {
    pub source_storage_id: String,
    /// The source panel's directory — stripped off every dest key, so the
    /// selection lands under `dest_path` by basename (MC semantics).
    pub source_root: String,
    /// Source keys (full, root-relative) to copy.
    pub source_paths: Vec<String>,
    pub dest: TransferDest,
    /// `true`: existing entries and errors ask through the dialogs; `false`
    /// (the temp copy behind F3 view): overwrite, first error fails.
    pub interactive: bool,
}

/// Where a copy lands.
#[derive(Debug, Clone)]
pub enum TransferDest {
    /// A directory of a local storage: `path` relative to its root.
    Storage { storage_id: String, path: String },
    /// A new cloud share holding the selection (F5 onto the Links panel).
    NewShare,
}

pub type EventSender = mpsc::UnboundedSender<UiEvent>;
pub type EventReceiver = mpsc::UnboundedReceiver<UiEvent>;

pub fn channel() -> (EventSender, EventReceiver) {
    mpsc::unbounded_channel()
}

// Transient one-shot messages, few alive at once — not worth boxing the big
// variants over.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum UiEvent {
    ListStorages(Vec<StorageInfo>),
    /// The local volumes (`Engine::local_drives`), the selector's first block.
    Drives(Vec<Drive>),
    ConnectStorage {
        panel: PanelId,
        name: String,
        storage_type: StorageType,
        result: Result<String, String>,
    },
    /// One batch of the panel's directory listing `seq` (names in the
    /// level), as the storage streams it.
    ListDirBatch {
        panel: PanelId,
        seq: u64,
        items: Vec<StorageObjectInfo>,
    },
    /// The listing `seq` ended: the volume's space, or the error it hit.
    ListDirDone {
        panel: PanelId,
        seq: u64,
        result: Result<Option<Space>, String>,
    },
    /// The Links panel's listing (`Engine::list_links`).
    ListLinks {
        panel: PanelId,
        result: Result<Vec<LinkRow>, String>,
    },
    /// A link's P2P liveness flipped (`link.p2p_online` / `link.p2p_offline`).
    LinkP2p {
        token: String,
        online: bool,
    },
    Delete(Result<(), String>),
    Mkdir(Result<(), String>),
    /// F6: the outcome, with the new name to put the cursor on.
    Rename {
        result: Result<(), String>,
        to: String,
    },
    /// F3 on a folder: its total size, for `key` in directory `dir` of the
    /// panel's storage.
    DirSize {
        panel: PanelId,
        dir: String,
        key: String,
        result: Result<u64, String>,
    },
    /// Alt+F7: one page of the find run `seq` (`App::find`), or its error.
    FindPage {
        seq: u64,
        result: Result<SearchResult, String>,
    },
    StartTransfer(Result<(), String>),
    /// A progress tick of the running copy.
    TransferProgress(TransferProgress),
    /// The running copy ended: its final state, its last progress, and the
    /// error a failed run ended with.
    TransferFinished {
        state: TransferItemState,
        progress: TransferProgress,
        error: Option<String>,
    },
    /// The copy is parked on existing-entry conflicts — answer with
    /// `Engine::resolve_conflicts`.
    TransferConflicts(Vec<Conflict>),
    /// One unit of the copy failed — answer with `Engine::resolve_error`.
    TransferError {
        key: String,
        message: String,
    },
    CancelTransfer(Result<(), String>),
    /// The viewer's file size (`get_attrs`); `seq` ties it to the viewer
    /// instance that asked, so a closed viewer's answers are dropped.
    ViewerOpened {
        seq: u64,
        result: Result<u64, String>,
    },
    /// One aligned block (`viewer::BLOCK_SIZE`) the viewer asked for.
    ViewerBlock {
        seq: u64,
        idx: u64,
        result: Result<Vec<u8>, String>,
    },
    /// Progress and outcome of the viewer's forward search.
    ViewerSearch {
        seq: u64,
        outcome: Result<SearchOutcome, String>,
    },
    LoginComplete(Result<(), String>),
    LogoutResult(Result<(), String>),
    /// The trust circle: the devices overview's trusted cards.
    VaultDevices(Result<Vec<models::DeviceCard>, String>),
    VaultApproveResult(Result<(), String>),
    VaultRejectResult(Result<(), String>),
    VaultRevokeResult(Result<(), String>),
    VaultRecoveryRegenerateResult(Result<Vec<String>, String>),
    VaultJoinStartResult(Result<(), String>),
    VaultRecoverResult(Result<StepUpOutcome, String>),
    VaultStepUpVerifyResult(Result<StepUpOutcome, String>),
    VaultBindResult(Result<(), String>),
    /// Fresh vault snapshot — pushed after anything that changed it.
    VaultSnapshot(VaultStateSnapshot),
    BackendStatus(BackendStatus),
    /// The account's storage quota standing (`Engine::usage_space`), what a
    /// share panel shows as its space; `None` = no cap known.
    Usage(Option<Space>),
    NearbyDevices {
        devices: Vec<NearbyDevice>,
    },
    /// The outbound nearby offers changed (status / progress) — re-list
    /// Nearby panels.
    NearbyOffers(Vec<OfferView>),
    /// Outcome of sending a nearby offer (`Engine::send_nearby_offer`).
    NearbyOffer(Result<(), String>),
    /// Outcome of cancelling one.
    NearbyOfferCancel(Result<(), String>),
    /// `share.offered`: a nearby device offers us a share — for the accept /
    /// decline dialog. The engine holds it (and its key) as the one pending
    /// inbound offer until answered.
    ShareOffered(InboundOfferView),
    /// `share.cancelled`: the sender withdrew that pending offer.
    ShareOfferCancelled {
        offer_token: String,
    },
    /// Outcome of accepting (`Engine::accept_inbound_offer`): the storage id
    /// and name the share opened under.
    ShareOfferAccepted(Result<(String, String), String>),
    /// Outcome of declining (`Engine::reject_inbound_offer`).
    ShareOfferRejected(Result<(), String>),
    /// A `vault.device_join_requested` arrived (already in the snapshot's
    /// pending list) — for the notification dialog.
    VaultJoinRequestArrived(VaultJoinRequest),
    /// `vault.device_approved`: the identity unlock outcome.
    VaultDeviceApproved(Result<(), String>),
    /// `vault.device_rejected`: a trusted device declined our join request.
    VaultDeviceRejected {
        reason: Option<String>,
    },
    /// This device was revoked from the trust circle.
    VaultRevoked,
}
