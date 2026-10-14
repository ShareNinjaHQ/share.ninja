//! UI components and rendering

pub mod command_line;
pub mod connection_pane;
pub mod dialog;
pub mod info_panel;
pub mod log_pane;
pub mod menu;
pub mod panel;
pub mod player;
pub mod progress_dialog;
pub mod skin;
pub mod status_bar;
pub mod tetris;
pub mod user_pane;
pub mod viewer;

pub use command_line::CommandLine;
pub use connection_pane::ConnectionPaneWidget as ConnectionPane;
pub use dialog::{
    AddStorageDialog, CopyConfirmDialog, DeleteConfirmDialog, EditorCloseDialog, ErrorDialog, FileConflictDialog,
    FindDialog, FindResultsDialog, HelpDialog, InputDialog, ListingErrorDialog, MkdirDialog,
    RecoveryWordsDisplayDialog, SelectorItem, ShareOfferDialog, SkinSelectDialog, StepUpCodeDialog,
    StorageListDialog, TransferErrorDialog, VaultJoinNotifyDialog, VaultJoinPromptDialog, VaultJoinRequestsDialog,
    VaultRecoveryInputDialog, VaultRecoveryRegenerateConfirmDialog, VaultRevokeConfirmDialog, VaultRevokedNotifyDialog,
    VaultTrustCircleDialog, centered_rect, find_list_height, render_shadow,
};
pub use info_panel::InfoPanel;
pub use log_pane::LogPaneWidget as LogPane;
pub use menu::MenuBar;
pub use panel::{DetailsPanel, ListPanel};
pub use player::PlayerDialog;
pub use progress_dialog::ProgressDialog;
pub use status_bar::StatusBar;
pub use tetris::TetrisWidget;
pub use user_pane::UserPaneWidget as UserPane;
pub use viewer::ViewerWidget;
