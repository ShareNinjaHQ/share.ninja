//! UI state: the two panels, the dialog mode, and everything the widgets
//! render. Mutated only by the event loop (key handling + drained events).

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use share_ninja_core::transfers::{Conflict, OnExisting, TransferProgress};
use share_ninja_core::utils::base64::to_base64;
use share_ninja_core::utils::device::{nearby_device_label, os_name};
use share_ninja_core::{StorageFileType, StorageObjectInfo};

use crate::events::PanelId;
use crate::types::{
    BackendStatus, Drive, InboundOfferView, LinkRow, NearbyDevice, OfferView, Space, StorageConnectRequest,
    StorageInfo, StorageType, VaultJoinRequest, VaultStateSnapshot, models,
};

/// A listed object is a directory.
pub fn is_dir(e: &StorageObjectInfo) -> bool {
    e.file_type == StorageFileType::Dir
}

/// A listed object is a file (anything a listing shows that is neither a
/// directory nor a symlink).
pub fn is_file(e: &StorageObjectInfo) -> bool {
    !matches!(e.file_type, StorageFileType::Dir | StorageFileType::Symlink)
}
use crate::viewer::ViewerState;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AppMode {
    #[default]
    Normal,
    HelpDialog,
    AddStorageDialog,
    SelectStorageDialog,
    CopyConfirmDialog,
    Copying,
    ConflictResolution,
    TransferErrorDialog,
    DeleteConfirmDialog,
    Deleting,
    MkdirDialog,
    SkinSelectDialog,
    ListingErrorDialog,
    VaultJoinNotifyDialog,
    VaultTrustCircleDialog,
    VaultJoinRequestsDialog,
    VaultRevokeConfirmDialog,
    VaultErrorDialog,
    VaultRecoveryRegenerateConfirmDialog,
    RecoveryWordsDisplayDialog,
    VaultJoinPromptDialog,
    VaultRevokedNotifyDialog,
    VaultRecoveryInputDialog,
    /// An inbound nearby offer: Accept / Decline.
    ShareOfferDialog,
    /// F6: the new name for the cursor's entry.
    RenameDialog,
    /// Alt+F7: what to find and where.
    FindDialog,
    /// The find run's results (`App::find`), filling as pages land.
    FindResults,
    /// F9: the pull-down menu bar.
    Menu,
    /// The Dos Navigator tribute, a window over the panels.
    Tetris,
    /// The emailed step-up code for a parked vault recover.
    StepUpCodeDialog,
    /// The built-in file viewer (`App::viewer`) fills the screen.
    Viewer,
    /// The viewer's goto-offset prompt.
    ViewerGotoDialog,
    /// The viewer's search prompt.
    ViewerSearchDialog,
    /// The mini music player dialog (`App::player`).
    Player,
    /// The built-in editor (`App::editor`) fills the screen.
    Editor,
    /// The editor's search prompt.
    EditorSearchDialog,
    /// The editor's goto-line prompt.
    EditorGotoDialog,
    /// The editor's unsaved-changes guard (Save / Discard / Cancel).
    EditorCloseConfirmDialog,
    /// The editor's F9 menu bar over the editor.
    EditorMenu,
}

/// A one-line text field: the text and a char-index cursor. The command
/// line and every dialog input edit through it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LineEdit {
    pub text: String,
    /// Cursor position in chars.
    pub cursor: usize,
}

impl LineEdit {
    fn byte_index(&self) -> usize {
        self.text
            .char_indices()
            .nth(self.cursor)
            .map(|(i, _)| i)
            .unwrap_or(self.text.len())
    }

    pub fn len(&self) -> usize {
        self.text.chars().count()
    }

    pub fn is_empty(&self) -> bool {
        self.text.is_empty()
    }

    pub fn insert(&mut self, ch: char) {
        let at = self.byte_index();
        self.text.insert(at, ch);
        self.cursor += 1;
    }

    /// Delete the char before the cursor.
    pub fn backspace(&mut self) {
        if self.cursor == 0 {
            return;
        }
        self.cursor -= 1;
        let at = self.byte_index();
        self.text.remove(at);
    }

    /// Delete the char under the cursor.
    pub fn delete(&mut self) {
        let at = self.byte_index();
        if at < self.text.len() {
            self.text.remove(at);
        }
    }

    pub fn left(&mut self) {
        self.cursor = self.cursor.saturating_sub(1);
    }

    pub fn right(&mut self) {
        self.cursor = (self.cursor + 1).min(self.len());
    }

    pub fn home(&mut self) {
        self.cursor = 0;
    }

    pub fn end(&mut self) {
        self.cursor = self.len();
    }

    /// Replace the text, cursor at its end.
    pub fn set(&mut self, text: String) {
        self.cursor = text.chars().count();
        self.text = text;
    }

    pub fn clear(&mut self) {
        self.text.clear();
        self.cursor = 0;
    }

    /// The editing keys: ←/→/Home/End, Backspace/Delete, and printable
    /// chars (without Ctrl/Alt). `true` when the key was one of them.
    pub fn handle_key(&mut self, key: KeyEvent) -> bool {
        match key.code {
            KeyCode::Left => self.left(),
            KeyCode::Right => self.right(),
            KeyCode::Home => self.home(),
            KeyCode::End => self.end(),
            KeyCode::Backspace => self.backspace(),
            KeyCode::Delete => self.delete(),
            KeyCode::Char(c) if !key.modifiers.intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) => self.insert(c),
            _ => return false,
        }
        true
    }
}

/// Something to run with the TUI suspended (terminal handed over).
#[derive(Debug, Clone)]
pub enum ExternalCommand {
    /// `cmd path` — an editor that owns the screen (F4 mcedit).
    Program { cmd: String, path: String },
    /// One command line: the subshell runs it where it is (moved to `cwd`
    /// first when the panel has one), the console runs it in `cwd`.
    Shell {
        cwd: Option<std::path::PathBuf>,
        command: String,
    },
    /// Ctrl+O: the shell's screen — the subshell itself, or the console's
    /// buffer with the command line live at its bottom.
    Subshell,
}

#[derive(Debug, Default)]
pub struct DialogState {
    pub input_name: String,
    pub input_url: String,
    pub active_field: usize,
    pub selected_index: usize,
    /// The panel the storage selector / add-storage dialog is for (FAR's
    /// Alt+F1 = left, Alt+F2 = right — not necessarily the active one).
    pub storage_target: PanelId,
    pub copy_source_name: String,
    pub copy_dest_path: String,
    pub copy_ok_selected: bool,
    pub delete_name: String,
    pub delete_is_dir: bool,
    pub delete_yes_selected: bool,
    pub mkdir: LineEdit,
    pub mkdir_focus: u8,
    pub conflict_focus: usize,
    pub transfer_error_message: String,
    pub transfer_error_focus: u8,
    pub skin_list: Vec<(String, Option<std::path::PathBuf>)>,
    pub skin_selected_index: usize,
    pub skin_scroll_offset: usize,
    pub skin_focus: u8,
    pub listing_error_message: String,
    pub listing_error_panel: Option<PanelId>,
    /// F5 onto the Nearby panel: the device the confirm dialog sends to.
    pub offer_target: Option<(String, String)>,
    /// F8 on the Nearby panel: the device whose offer the confirm withdraws.
    pub cancel_offer_agent: Option<String>,
    /// F8 on the Links panel: the link the delete dialog is about.
    pub delete_link_token: Option<String>,
    pub vault_join_message: String,
    pub vault_join_control_selected: bool,
    /// Pending notification dialogs waiting to be shown after the current one
    /// is dismissed (one per join request that arrived while another dialog
    /// was already open).
    pub vault_join_queue: std::collections::VecDeque<String>,
    /// Vault trust-circle dialog state (the trusted device cards).
    pub vault_devices: Vec<models::DeviceCard>,
    pub vault_devices_selected: usize,
    pub vault_devices_scroll: usize,
    /// 0=list, 1=revoke, 2=close
    pub vault_devices_focus: u8,
    pub vault_devices_error: Option<String>,
    pub vault_devices_loading: bool,
    /// Revoke-confirmation modal state (shown on top of the trust-circle dialog).
    pub vault_revoke_agent_id: String,
    pub vault_revoke_device_name: String,
    pub vault_revoke_yes_selected: bool,
    /// Generic vault error dialog (rendered atop whatever was open).
    pub vault_error_title: String,
    pub vault_error_message: String,
    /// Mode to return to when the vault error dialog is dismissed.
    pub vault_error_return_mode: AppMode,
    /// Recovery-words regenerate confirmation modal.
    pub recovery_regen_yes_selected: bool,
    /// 0 = "I've written them down", 1 = "Close" (keep words available)
    pub recovery_display_focus: u8,
    /// "Send join request" / "Cancel" — true = Send focused.
    pub vault_join_prompt_send_selected: bool,
    /// The inbound nearby offer the dialog shows.
    pub share_offer: Option<InboundOfferView>,
    /// An offer that arrived while another dialog was up — shown on the
    /// next return to `Normal`.
    pub share_offer_pending: Option<InboundOfferView>,
    /// "Accept" / "Decline" — true = Accept focused.
    pub share_offer_accept_selected: bool,
    /// Vault recovery input dialog: text box + buttons.
    pub recovery_input_text: String,
    /// 0 = text input, 1 = Recover, 2 = Cancel
    pub recovery_input_focus: u8,
    /// Vault join-requests dialog state (the list mirrors the snapshot's
    /// pending requests).
    pub vault_join_requests: Vec<VaultJoinRequest>,
    pub vault_join_selected: usize,
    pub vault_join_scroll: usize,
    /// 0=list, 1=approve, 2=decline, 3=close
    pub vault_join_focus: u8,
    pub vault_join_error: Option<String>,
    /// Step-up code dialog: the typed code + buttons.
    pub step_up_code_text: String,
    /// 0 = input, 1 = Verify, 2 = Cancel
    pub step_up_focus: u8,
    /// When the backend stops accepting the code (RFC 3339), for the copy.
    pub step_up_expires_at: String,
    /// The viewer's / editor's goto and search prompt.
    pub viewer_input: LineEdit,
    /// 0 = input, 1 = OK, 2 = Cancel
    pub viewer_input_focus: u8,
    /// The editor's close guard: 0 = Save, 1 = Discard, 2 = Cancel.
    pub editor_close_focus: u8,
    /// F6: the entry being renamed, the new name being typed, and the
    /// focus (0 = input, 1 = OK, 2 = Cancel).
    pub rename_from: String,
    pub rename: LineEdit,
    pub rename_focus: u8,
    /// F9: whose bar, the open menu (an index into its titles) and its
    /// highlighted row.
    pub menu_kind: crate::menu::MenuBarKind,
    pub menu_open: usize,
    pub menu_item: usize,
    /// Alt+F7: the name to find (kept between runs, FAR-style), where
    /// (`false` = the current folder, `true` = the whole storage), whether
    /// subfolders are walked (on to begin with), and the focus (0 = name,
    /// 1 = where, 2 = subfolders, 3 = Search, 4 = Cancel).
    pub find_name: LineEdit,
    pub find_whole: bool,
    pub find_recursive: bool,
    pub find_focus: u8,
}

/// A find run and its results (Alt+F7): pages land in `items` as the
/// storage's search walks; the dialog lists them grouped by directory.
pub struct FindState {
    /// The panel searched, its storage, and the scope inside it.
    pub panel: PanelId,
    pub storage_id: String,
    pub local: bool,
    /// Root-relative directory the search runs under (`""` = the root).
    pub scope: String,
    pub needle: String,
    /// Distinguishes this run's pages from a superseded run's.
    pub seq: u64,
    pub running: bool,
    pub items: Vec<StorageObjectInfo>,
    pub files: usize,
    pub folders: usize,
    pub error: Option<String>,
    /// The cursor's item, and the list's scroll in display rows.
    pub selected: usize,
    pub scroll: usize,
    /// 0 = list, 1 = New search, 2 = Go to, 3 = View, 4 = Panel, 5 = Cancel.
    pub focus: u8,
    /// The list viewport's rows, set by the draw.
    pub visible: usize,
    abort: Option<tokio::task::AbortHandle>,
}

/// A display row of the find results: a directory header, or an item.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FindRow {
    Dir(usize),
    Item(usize),
}

impl FindState {
    pub fn new(panel: PanelId, storage_id: String, local: bool, scope: String, needle: String, seq: u64) -> Self {
        Self {
            panel,
            storage_id,
            local,
            scope,
            needle,
            seq,
            running: true,
            items: Vec::new(),
            files: 0,
            folders: 0,
            error: None,
            selected: 0,
            scroll: 0,
            focus: 0,
            visible: 1,
            abort: None,
        }
    }

    pub fn started(&mut self, abort: tokio::task::AbortHandle) {
        self.abort = Some(abort);
        self.running = true;
    }

    /// Stop the run; the task's receiver drop ends the storage's walk.
    pub fn abort(&mut self) {
        if let Some(h) = self.abort.take() {
            h.abort();
        }
        self.running = false;
    }

    /// The directory of item `i` (root-relative, `""` = the root) and its name.
    pub fn split(&self, i: usize) -> (&str, &str) {
        let key = self.items[i].key.as_str();
        key.rsplit_once('/').unwrap_or(("", key))
    }

    /// The rows as displayed: a header row wherever the directory changes
    /// from the item before (`Dir(i)` names item `i`'s directory), then the
    /// item rows.
    pub fn rows(&self) -> Vec<FindRow> {
        let mut rows = Vec::with_capacity(self.items.len() * 2);
        let mut last_dir: Option<&str> = None;
        for i in 0..self.items.len() {
            let (dir, _) = self.split(i);
            if last_dir != Some(dir) {
                rows.push(FindRow::Dir(i));
                last_dir = Some(dir);
            }
            rows.push(FindRow::Item(i));
        }
        rows
    }

    /// Keep the selected item's row (and the header right above it) in view.
    pub fn scroll_to_selected(&mut self) {
        let rows = self.rows();
        let Some(row) = rows.iter().position(|r| *r == FindRow::Item(self.selected)) else {
            return;
        };
        let top = if row > 0 && matches!(rows[row - 1], FindRow::Dir(_)) {
            row - 1
        } else {
            row
        };
        if top < self.scroll {
            self.scroll = top;
        } else if row >= self.scroll + self.visible {
            self.scroll = row + 1 - self.visible.max(1);
        }
    }

    pub fn select(&mut self, idx: usize) {
        self.selected = idx.min(self.items.len().saturating_sub(1));
        self.scroll_to_selected();
    }

    pub fn move_by(&mut self, delta: i32) {
        let idx = (self.selected as i64 + delta as i64).max(0) as usize;
        self.select(idx);
    }
}

/// A find's results panelized (FAR's temporary panel): the entries are the
/// matches, keyed by their full root-relative path, and `..` leads back.
#[derive(Clone, Debug)]
pub struct FoundList {
    /// The directory the search ran under: where `..` returns to.
    pub back_to: String,
    pub needle: String,
}

// ── panel contents ─────────────────────────────────────────────────────────

/// What a panel slot can show. The slot ([`Panel`]) is the same either way;
/// the content owns its own cursor and per-kind state, and the event loop
/// routes keys by content.
// Two instances live (one per slot): the file list's size is no concern.
#[allow(clippy::large_enum_variant)]
pub enum Content {
    /// A storage listing (local directory or share).
    Files(FileList),
    /// This account's links (`GET /links`); Enter opens one here, F5 onto it
    /// creates a new share.
    Links(LinkList),
    /// The nearby devices ([`App::nearby`]); Enter shows one, F5 onto it
    /// sends an offer, F8 withdraws it.
    Nearby(NearbyList),
    /// The backend connection: params and session state.
    Connection,
    /// The signed-in user (Enter: login / logout).
    User,
    /// The captured log tail.
    Log,
    /// Facts about the other panel's listing.
    Info,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContentKind {
    Files,
    Links,
    Nearby,
    Connection,
    User,
    Log,
    Info,
}

impl ContentKind {
    /// The non-storage contents the storage selector offers.
    pub const OPENABLE: [ContentKind; 6] = [
        ContentKind::Links,
        ContentKind::Nearby,
        ContentKind::Connection,
        ContentKind::User,
        ContentKind::Log,
        ContentKind::Info,
    ];

    pub fn label(self) -> &'static str {
        match self {
            ContentKind::Files => "Files",
            ContentKind::Links => "Links",
            ContentKind::Nearby => "Nearby",
            ContentKind::Connection => "Connection",
            ContentKind::User => "User",
            ContentKind::Log => "Log",
            ContentKind::Info => "Info",
        }
    }
}

impl Content {
    /// A fresh content of `kind` (`Files` starts with no storage).
    pub fn new(kind: ContentKind) -> Self {
        match kind {
            ContentKind::Files => Content::Files(FileList::new()),
            ContentKind::Links => Content::Links(LinkList::default()),
            ContentKind::Nearby => Content::Nearby(NearbyList::default()),
            ContentKind::Connection => Content::Connection,
            ContentKind::User => Content::User,
            ContentKind::Log => Content::Log,
            ContentKind::Info => Content::Info,
        }
    }

    pub fn kind(&self) -> ContentKind {
        match self {
            Content::Files(_) => ContentKind::Files,
            Content::Links(_) => ContentKind::Links,
            Content::Nearby(_) => ContentKind::Nearby,
            Content::Connection => ContentKind::Connection,
            Content::User => ContentKind::User,
            Content::Log => ContentKind::Log,
            Content::Info => ContentKind::Info,
        }
    }
}

/// One row of the panel selector (Alt+1 / Alt+2).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SelectorRow {
    /// A local volume, by index into [`App::drives`].
    Drive(usize),
    Kind(ContentKind),
    /// An open share or extra local root, by index into [`App::storages`].
    Storage(usize),
}

/// How a panel lays its file list out (FAR's panel modes).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ViewMode {
    /// Names only, three columns.
    Brief,
    /// Names only, two columns.
    Medium,
    /// Name, size, date.
    #[default]
    Full,
    /// Name and size.
    Wide,
}

impl ViewMode {
    pub const ALL: [ViewMode; 4] = [ViewMode::Brief, ViewMode::Medium, ViewMode::Full, ViewMode::Wide];

    pub fn label(self) -> &'static str {
        match self {
            ViewMode::Brief => "Brief",
            ViewMode::Medium => "Medium",
            ViewMode::Full => "Full",
            ViewMode::Wide => "Wide",
        }
    }

    /// Name columns side by side.
    pub fn columns(self) -> usize {
        match self {
            ViewMode::Brief => 3,
            ViewMode::Medium => 2,
            ViewMode::Full | ViewMode::Wide => 1,
        }
    }
}

/// How a file list orders its rows; directories always come first and `..`
/// stays on top.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SortMode {
    #[default]
    Name,
    Extension,
    /// Newest first.
    Modified,
    /// Largest first.
    Size,
    /// As the storage lists them.
    Unsorted,
}

impl SortMode {
    pub const ALL: [SortMode; 5] = [
        SortMode::Name,
        SortMode::Extension,
        SortMode::Modified,
        SortMode::Size,
        SortMode::Unsorted,
    ];

    pub fn label(self) -> &'static str {
        match self {
            SortMode::Name => "Name",
            SortMode::Extension => "Extension",
            SortMode::Modified => "Modification time",
            SortMode::Size => "Size",
            SortMode::Unsorted => "Unsorted",
        }
    }

    /// FAR's one-letter sort indicator in the panel header.
    pub fn letter(self) -> char {
        match self {
            SortMode::Name => 'n',
            SortMode::Extension => 'x',
            SortMode::Modified => 'w',
            SortMode::Size => 's',
            SortMode::Unsorted => 'u',
        }
    }
}

/// A list cursor: the selected row and the first visible one.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Cursor {
    pub index: usize,
    pub scroll: usize,
}

impl Cursor {
    /// Put the cursor on `idx` (clamped to the list), keeping it in view;
    /// `visible` rows fit, and the view scrolls in steps of `step` rows (a
    /// multi-column list scrolls by whole columns).
    pub fn set(&mut self, idx: usize, len: usize, visible: usize, step: usize) {
        if len == 0 {
            self.reset();
            return;
        }
        self.index = idx.min(len - 1);
        self.adjust(visible, step);
    }

    pub fn move_by(&mut self, delta: i32, len: usize, visible: usize, step: usize) {
        if len == 0 {
            self.reset();
            return;
        }
        let idx = if delta < 0 {
            self.index.saturating_sub((-delta) as usize)
        } else {
            self.index + delta as usize
        };
        self.set(idx, len, visible, step);
    }

    pub fn reset(&mut self) {
        self.index = 0;
        self.scroll = 0;
    }

    fn adjust(&mut self, visible: usize, step: usize) {
        let visible = visible.max(1);
        let step = step.max(1);
        if self.index < self.scroll {
            self.scroll = self.index / step * step;
        } else if self.index >= self.scroll + visible {
            self.scroll = (self.index + 1 - visible).div_ceil(step) * step;
        }
    }
}

/// The storage a file list is on (`Engine` storage id).
#[derive(Clone, Debug)]
pub struct FileStorage {
    pub id: String,
    pub name: String,
    pub kind: StorageType,
}

/// Where a file list was entered from — what `..` at its root leads back to.
#[derive(Clone, Debug)]
pub enum OpenedFrom {
    /// The Links panel: back to the list, cursor on the link.
    Links,
    /// Archive file `name` in directory `dir` of `storage`: back to that
    /// listing, cursor on the archive; `from` is what that listing itself
    /// was entered from (a nested archive, a share opened from Links).
    Archive {
        storage: FileStorage,
        dir: String,
        name: String,
        from: Option<Box<OpenedFrom>>,
    },
}

/// A storage listing: one directory level of a local root or a share.
pub struct FileList {
    /// `None` while the storage is still connecting.
    pub storage: Option<FileStorage>,
    /// Root-relative directory, `""` at the root.
    pub current_path: String,
    /// The level's objects; `key` is the name in this directory.
    pub entries: Vec<StorageObjectInfo>,
    /// The local volume's space under this directory (from the listing).
    pub space: Option<Space>,
    /// F3-computed folder sizes of this directory's entries (FAR: the total
    /// replaces `<DIR>` in the size column), by entry key; forgotten with
    /// the next listing.
    pub dir_sizes: HashMap<String, u64>,
    pub cursor: Cursor,
    /// Marked entry names (Ctrl+T) in the current directory.
    pub marked: HashSet<String>,
    /// The entry the cursor was on, per directory path.
    dir_history: HashMap<String, String>,
    /// Reset the cursor to 0 when the listing loads.
    pub reset_selection_on_load: bool,
    /// Previous path for back navigation on listing error.
    pub previous_path: Option<String>,
    /// Entered from the Links panel or an archive file: `..` at the root
    /// goes back there.
    pub opened_from: Option<OpenedFrom>,
    /// MC quick search (Ctrl+S): the name prefix typed so far, while active.
    pub quick_search: Option<String>,
    /// The list is a find's results, not a directory: `current_path` is
    /// `""` so every key is its own path, and a listing is never requested.
    pub temporary: Option<FoundList>,
    /// A listing was requested: its first batch replaces `entries`, the
    /// ones after append.
    pub replace_on_batch: bool,
}

impl FileList {
    pub fn new() -> Self {
        Self {
            storage: None,
            current_path: String::new(),
            entries: Vec::new(),
            space: None,
            dir_sizes: HashMap::new(),
            cursor: Cursor::default(),
            marked: HashSet::new(),
            dir_history: HashMap::new(),
            reset_selection_on_load: false,
            previous_path: None,
            opened_from: None,
            quick_search: None,
            temporary: None,
            replace_on_batch: false,
        }
    }

    /// The cursor on `idx`, scrolled into view: `visible` cells, `step`
    /// per scroll (the panel's, see [`Panel::step`]).
    pub fn cursor_set(&mut self, idx: usize, visible: usize, step: usize) {
        self.cursor.set(idx, self.entries.len(), visible, step);
    }

    pub fn cursor_move(&mut self, delta: i32, visible: usize, step: usize) {
        self.cursor.move_by(delta, self.entries.len(), visible, step);
    }

    /// Order the listing per `mode`: `..` first, then directories, then the
    /// rest, each group by the mode. `Unsorted` keeps the storage's order.
    pub fn sort_entries(&mut self, mode: SortMode) {
        if mode == SortMode::Unsorted {
            return;
        }
        let up = if self.entries.first().is_some_and(|e| e.key == "..") {
            1
        } else {
            0
        };
        self.entries[up..].sort_by(|a, b| {
            let (da, db) = (is_dir(a), is_dir(b));
            if da != db {
                return db.cmp(&da);
            }
            let by_name = || a.key.to_lowercase().cmp(&b.key.to_lowercase());
            match mode {
                SortMode::Name | SortMode::Unsorted => by_name(),
                SortMode::Extension => extension(&a.key).cmp(&extension(&b.key)).then_with(by_name),
                SortMode::Modified => b.attrs.mtime.cmp(&a.attrs.mtime).then_with(by_name),
                SortMode::Size => b.size.cmp(&a.size).then_with(by_name),
            }
        });
    }

    pub fn storage_id(&self) -> Option<&str> {
        self.storage.as_ref().map(|s| s.id.as_str())
    }

    pub fn storage_type(&self) -> Option<StorageType> {
        self.storage.as_ref().map(|s| s.kind)
    }

    pub fn is_local(&self) -> bool {
        self.storage_type() == Some(StorageType::Local)
    }

    pub fn selected_entry(&self) -> Option<&StorageObjectInfo> {
        self.entries.get(self.cursor.index)
    }

    /// The entry under the cursor as a root-relative key.
    pub fn key_of(&self, name: &str) -> String {
        if self.current_path.is_empty() {
            name.to_string()
        } else {
            format!("{}/{}", self.current_path, name)
        }
    }

    /// The first entry from `from` on (wrapping round the list; `skip_from`
    /// starts at the one after it) whose name starts with `prefix`,
    /// case-insensitively — the listing is sorted case-insensitively too.
    pub fn find_prefix(&self, prefix: &str, from: usize, skip_from: bool) -> Option<usize> {
        find_prefix(self.entries.iter().map(|e| e.key.as_str()), prefix, from, skip_from)
    }

    /// Toggle the mark on the current entry and advance the cursor.
    pub fn toggle_mark(&mut self, visible: usize, step: usize) {
        let Some(entry) = self.entries.get(self.cursor.index) else {
            return;
        };
        let name = entry.key.clone();
        if name == ".." {
            return;
        }
        if !self.marked.remove(&name) {
            self.marked.insert(name);
        }
        self.cursor_move(1, visible, step);
    }

    /// Remember the cursor for the current path (before leaving it).
    pub fn save_selection_to_history(&mut self) {
        if let Some(entry) = self.selected_entry() {
            self.dir_history.insert(self.current_path.clone(), entry.key.clone());
        }
    }

    /// Put the cursor back where it was in this directory, else on
    /// `fallback_name` (the directory we came up from).
    pub fn restore_selection_or_find(&mut self, fallback_name: &str, visible: usize, step: usize) {
        let remembered = self.dir_history.get(&self.current_path).cloned();
        let idx = remembered
            .and_then(|name| self.entries.iter().position(|e| e.key == name))
            .or_else(|| self.entries.iter().position(|e| e.key == fallback_name));
        if let Some(idx) = idx {
            self.cursor_set(idx, visible, step);
        }
    }

    /// The last component of the current path.
    pub fn current_dir_name(&self) -> String {
        self.current_path.rsplit('/').next().unwrap_or("").to_string()
    }

    /// The size an entry shows: a file's own, a folder's computed total once
    /// F3 sized it.
    pub fn shown_size(&self, e: &StorageObjectInfo) -> Option<u64> {
        if is_dir(e) {
            self.dir_sizes.get(&e.key).copied()
        } else {
            Some(e.size)
        }
    }

    /// Switch storages; `current_path` is the caller's to set.
    pub fn set_storage(&mut self, storage: FileStorage) {
        self.storage = Some(storage);
        self.entries.clear();
        self.dir_sizes.clear();
        self.cursor.reset();
        // Per-directory cursor memory is keyed by path, so a storage change
        // must not let another storage's paths shadow this one's.
        self.dir_history.clear();
    }

    /// The path for the title: on a local storage the absolute directory
    /// as the platform writes it (`~`-shortened on unix); on a share `/dir`
    /// inside it.
    pub fn display_path(&self) -> String {
        match &self.storage {
            Some(s) if s.kind == StorageType::Local => {
                crate::local_path::display(&crate::local_path::join(&s.id, &self.current_path))
            }
            _ => format!("/{}", self.current_path),
        }
    }
}

impl Default for FileList {
    fn default() -> Self {
        Self::new()
    }
}

/// The links list.
#[derive(Default)]
pub struct LinkList {
    pub rows: Vec<LinkRow>,
    pub cursor: Cursor,
}

impl LinkList {
    pub fn selected(&self) -> Option<&LinkRow> {
        self.rows.get(self.cursor.index)
    }
}

/// The nearby devices; the rows are [`App::nearby`].
#[derive(Default)]
pub struct NearbyList {
    pub cursor: Cursor,
    /// The device whose details are shown (its agent id), else the list.
    pub open: Option<String>,
}

/// The lower-cased extension of a name (`""` when none).
fn extension(name: &str) -> String {
    match name.rsplit_once('.') {
        Some((stem, ext)) if !stem.is_empty() => ext.to_lowercase(),
        _ => String::new(),
    }
}

fn find_prefix<'a>(
    names: impl ExactSizeIterator<Item = &'a str>,
    prefix: &str,
    from: usize,
    skip_from: bool,
) -> Option<usize> {
    let n = names.len();
    if n == 0 {
        return None;
    }
    let names: Vec<&str> = names.collect();
    let prefix = prefix.to_lowercase();
    let start = if skip_from { from + 1 } else { from };
    (0..n)
        .map(|k| (start + k) % n)
        .find(|&i| names[i].to_lowercase().starts_with(&prefix))
}

// ── panel slot ─────────────────────────────────────────────────────────────

/// One of the two panel slots: its content plus the slot's transient state.
pub struct Panel {
    pub content: Content,
    /// A listing (files / links) is in flight.
    pub loading: bool,
    /// When loading started (for the elapsed time display).
    pub loading_started: Option<Instant>,
    pub error: Option<String>,
    pub status_message: Option<String>,
    /// Inner rows, set by the draw.
    pub visible_height: usize,
    /// The file list an info content replaced (Alt+C on a listing …),
    /// restored when that content is toggled off again.
    pub stash: Option<FileList>,
    /// The listing in flight: its number (a superseded listing's batches
    /// are dropped) and the task streaming it (aborted by the next one —
    /// the dropped receiver ends the storage's walk).
    pub listing_seq: u64,
    pub listing: Option<tokio::task::AbortHandle>,
    /// The panel's layout and sort — its own, whatever it shows: they
    /// outlive every listing (a storage change, a link, an archive, the
    /// Links / Nearby views) and the session (`state.rs`).
    pub view: ViewMode,
    pub sort: SortMode,
    /// The last local directory listed here, absolute — where the panel
    /// starts next time.
    pub last_local_dir: Option<std::path::PathBuf>,
}

impl Panel {
    pub fn new() -> Self {
        Self {
            content: Content::Files(FileList::new()),
            loading: false,
            loading_started: None,
            error: None,
            status_message: None,
            visible_height: 20,
            stash: None,
            listing_seq: 0,
            listing: None,
            view: ViewMode::default(),
            sort: SortMode::default(),
            last_local_dir: None,
        }
    }

    /// Name columns side by side: the view's for a file list, 1 for any
    /// other content.
    pub fn columns(&self) -> usize {
        match self.content {
            Content::Files(_) => self.view.columns(),
            _ => 1,
        }
    }

    /// The scroll step: a multi-column list (Brief / Medium) scrolls by
    /// whole columns, `visible / columns` rows; a single-column one (Full
    /// / Wide) by one row, the cursor pushing the view along.
    pub fn step(&self) -> usize {
        match self.columns() {
            0 | 1 => 1,
            columns => (self.visible_height / columns).max(1),
        }
    }

    pub fn files(&self) -> Option<&FileList> {
        match &self.content {
            Content::Files(f) => Some(f),
            _ => None,
        }
    }

    pub fn files_mut(&mut self) -> Option<&mut FileList> {
        match &mut self.content {
            Content::Files(f) => Some(f),
            _ => None,
        }
    }

    /// The file list, making the content a fresh one (no storage) if it was
    /// something else — for a storage connect targeting this panel; the
    /// stash is dropped, a storage choice supersedes it.
    pub fn ensure_files(&mut self) -> &mut FileList {
        if !matches!(self.content, Content::Files(_)) {
            self.content = Content::Files(FileList::new());
            self.stash = None;
        }
        match &mut self.content {
            Content::Files(f) => f,
            _ => unreachable!("made Files above"),
        }
    }

    /// Show `kind` here. A file list gives way to the stash (restored by
    /// [`Panel::toggle_content`]); a listing state is cleared.
    /// End the listing in flight: its task stops streaming, and a batch of
    /// it already on the channel is dropped by the number.
    pub fn stop_listing(&mut self) {
        if let Some(h) = self.listing.take() {
            h.abort();
        }
        self.listing_seq += 1;
    }

    pub fn show(&mut self, kind: ContentKind) {
        self.stop_listing();
        let previous = std::mem::replace(&mut self.content, Content::new(kind));
        if let Content::Files(files) = previous {
            self.stash = Some(files);
        }
        self.loading = false;
        self.loading_started = None;
        self.error = None;
        self.status_message = None;
    }

    /// Alt+C-style toggle: show `kind`, or, when it is already shown, go
    /// back to the stashed file list. `true` when the content changed.
    pub fn toggle_content(&mut self, kind: ContentKind) -> bool {
        if self.content.kind() != kind {
            self.show(kind);
            return true;
        }
        let Some(files) = self.stash.take() else { return false };
        self.content = Content::Files(files);
        self.loading = false;
        self.loading_started = None;
        self.error = None;
        self.status_message = None;
        true
    }

    /// The title the panel frame shows before the path.
    pub fn title(&self, side: &str) -> String {
        match &self.content {
            Content::Files(f) => f
                .storage
                .as_ref()
                .map(|s| s.name.clone())
                .unwrap_or_else(|| side.to_string()),
            other => other.kind().label().to_string(),
        }
    }
}

impl Default for Panel {
    fn default() -> Self {
        Self::new()
    }
}

// ── app ────────────────────────────────────────────────────────────────────

pub struct App {
    pub left_panel: Panel,
    pub right_panel: Panel,
    pub active_panel: PanelId,
    pub mode: AppMode,
    pub dialog_state: DialogState,
    /// The engine's open storages — the shares in the panel selector.
    pub storages: Vec<StorageInfo>,
    /// The local volumes — the panel selector's first block.
    pub drives: Vec<Drive>,
    pub should_quit: bool,
    /// A copy is running on the engine (progress dialog / conflict / error
    /// dialogs are its projection).
    pub transfer_active: bool,
    /// The running copy's last progress tick.
    pub transfer_progress: TransferProgress,
    pub transfer_start_time: Option<Instant>,
    /// The parked conflict batch, answered one by one or in bulk.
    pub transfer_conflicts: Vec<Conflict>,
    pub transfer_conflict_total: usize,
    /// Per-key answers given so far in the current batch.
    pub pending_conflict_resolutions: Vec<(String, OnExisting)>,
    pub log_messages: Vec<String>,
    pub backend_status: Option<BackendStatus>,
    /// The last `nearby.updated`: what a Nearby panel lists, and the count
    /// a share panel shows.
    pub nearby: Vec<NearbyDevice>,
    /// Outbound nearby offers by recipient agent id.
    pub offers: HashMap<String, OfferView>,
    /// The account's storage quota standing — a share panel's space label.
    pub usage: Option<Space>,
    /// Storage connects queued during startup that need the backend to be
    /// online before they can succeed (ninja shares — resolving the link
    /// needs the session). Drained on the first online transition.
    pub pending_storage_connects: Vec<(PanelId, String, StorageConnectRequest)>,
    /// The engine's last vault snapshot — what the vault dialogs render.
    pub vault_snapshot: VaultStateSnapshot,
    pub pending_command: Option<ExternalCommand>,
    /// The open built-in viewer (F3 / Shift+L), while `mode` is one of the
    /// viewer modes.
    pub viewer: Option<ViewerState>,
    /// Counts viewers opened, so a closed viewer's late events are dropped.
    pub viewer_seq: u64,
    /// The open music player (Enter on an audio file), while `mode` is
    /// [`AppMode::Player`].
    pub player: Option<crate::player::PlayerState>,
    /// The open built-in editor (F4), while `mode` is one of the editor
    /// modes.
    pub editor: Option<crate::editor::EditorState>,
    /// The find run (Alt+F7) whose results are open, or are behind the
    /// viewer they opened.
    pub find: Option<FindState>,
    /// Counts find runs, so a superseded run's late pages are dropped.
    pub find_seq: u64,
    /// The Tetris game, while `mode` is [`AppMode::Tetris`].
    pub tetris: Option<crate::tetris::Tetris>,
    /// Path to log file (for Shift+L viewer)
    pub log_path: Option<String>,
    /// The state file between runs (`state.rs`), when the binary keeps one.
    pub state_path: Option<std::path::PathBuf>,
    /// Whether --debug-api is enabled (API logging)
    pub debug_api: bool,
    /// The MC-style command line under the panels: printable keys type into
    /// it, Enter runs it in the active panel's directory.
    pub command: LineEdit,
    /// Executed command lines, oldest first (Alt+P / Alt+N walk them).
    pub command_history: Vec<String>,
    /// Position while walking the history; `None` = editing a fresh line.
    pub command_history_index: Option<usize>,
    /// The unfinished line stashed when history walking starts.
    pub command_draft: String,
}

impl App {
    pub fn new() -> Self {
        Self {
            left_panel: Panel::new(),
            right_panel: Panel::new(),
            active_panel: PanelId::Left,
            mode: AppMode::Normal,
            dialog_state: DialogState {
                find_recursive: true,
                ..DialogState::default()
            },
            storages: Vec::new(),
            drives: Vec::new(),
            should_quit: false,
            transfer_active: false,
            transfer_progress: TransferProgress::default(),
            transfer_start_time: None,
            transfer_conflicts: Vec::new(),
            transfer_conflict_total: 0,
            pending_conflict_resolutions: Vec::new(),
            log_messages: Vec::new(),
            backend_status: None,
            nearby: Vec::new(),
            offers: HashMap::new(),
            usage: None,
            pending_storage_connects: Vec::new(),
            vault_snapshot: VaultStateSnapshot::default(),
            pending_command: None,
            viewer: None,
            viewer_seq: 0,
            player: None,
            editor: None,
            find: None,
            find_seq: 0,
            tetris: None,
            log_path: None,
            state_path: None,
            debug_api: false,
            command: LineEdit::default(),
            command_history: Vec::new(),
            command_history_index: None,
            command_draft: String::new(),
        }
    }

    // ── command line ───────────────────────────────────────────────────────

    /// Clear the line and end any history walk.
    pub fn command_clear(&mut self) {
        self.command.clear();
        self.command_history_index = None;
        self.command_draft.clear();
    }

    // ── panels ─────────────────────────────────────────────────────────────

    pub fn active_panel_mut(&mut self) -> &mut Panel {
        let id = self.active_panel;
        self.panel_mut(id)
    }

    pub fn active_panel(&self) -> &Panel {
        self.panel(self.active_panel)
    }

    pub fn active_panel_id(&self) -> PanelId {
        self.active_panel
    }

    pub fn inactive_panel(&self) -> &Panel {
        self.panel(self.inactive_panel_id())
    }

    pub fn inactive_panel_id(&self) -> PanelId {
        match self.active_panel {
            PanelId::Left => PanelId::Right,
            PanelId::Right => PanelId::Left,
        }
    }

    pub fn panel(&self, id: PanelId) -> &Panel {
        match id {
            PanelId::Left => &self.left_panel,
            PanelId::Right => &self.right_panel,
        }
    }

    pub fn panel_mut(&mut self, id: PanelId) -> &mut Panel {
        match id {
            PanelId::Left => &mut self.left_panel,
            PanelId::Right => &mut self.right_panel,
        }
    }

    pub fn toggle_panel(&mut self) {
        self.active_panel = self.inactive_panel_id();
    }

    /// The active panel's file list (the actions that need one).
    pub fn active_files(&self) -> Option<&FileList> {
        self.active_panel().files()
    }

    pub fn active_files_mut(&mut self) -> Option<&mut FileList> {
        self.active_panel_mut().files_mut()
    }

    // ── list navigation over whichever content is a list ───────────────────

    /// Rows of `panel`'s list content: entries, links, or devices. `None`
    /// for contents without a cursor (and a device's details view).
    pub fn rows(&self, id: PanelId) -> Option<usize> {
        match &self.panel(id).content {
            Content::Files(f) => Some(f.entries.len()),
            Content::Links(l) => Some(l.rows.len()),
            Content::Nearby(n) if n.open.is_none() => Some(self.nearby.len()),
            _ => None,
        }
    }

    pub fn cursor(&self, id: PanelId) -> Option<Cursor> {
        match &self.panel(id).content {
            Content::Files(f) => Some(f.cursor),
            Content::Links(l) => Some(l.cursor),
            Content::Nearby(n) if n.open.is_none() => Some(n.cursor),
            _ => None,
        }
    }

    fn cursor_mut(&mut self, id: PanelId) -> Option<&mut Cursor> {
        match &mut self.panel_mut(id).content {
            Content::Files(f) => Some(&mut f.cursor),
            Content::Links(l) => Some(&mut l.cursor),
            Content::Nearby(n) if n.open.is_none() => Some(&mut n.cursor),
            _ => None,
        }
    }

    /// The scroll step of `id`'s list: whole columns for a multi-column
    /// file list, one row otherwise.
    fn step(&self, id: PanelId) -> usize {
        self.panel(id).step()
    }

    pub fn move_cursor(&mut self, id: PanelId, delta: i32) {
        let Some(len) = self.rows(id) else { return };
        let visible = self.panel(id).visible_height;
        let step = self.step(id);
        if let Some(c) = self.cursor_mut(id) {
            c.move_by(delta, len, visible, step);
        }
    }

    pub fn set_cursor(&mut self, id: PanelId, idx: usize) {
        let Some(len) = self.rows(id) else { return };
        let visible = self.panel(id).visible_height;
        let step = self.step(id);
        if let Some(c) = self.cursor_mut(id) {
            c.set(idx, len, visible, step);
        }
    }

    pub fn move_active_cursor(&mut self, delta: i32) {
        let id = self.active_panel_id();
        self.move_cursor(id, delta);
    }

    pub fn set_active_cursor(&mut self, idx: usize) {
        let id = self.active_panel_id();
        self.set_cursor(id, idx);
    }

    /// Keep every list cursor inside its (possibly shrunk) list.
    pub fn clamp_cursors(&mut self) {
        for id in [PanelId::Left, PanelId::Right] {
            let Some(len) = self.rows(id) else { continue };
            let visible = self.panel(id).visible_height;
            let step = self.step(id);
            if let Some(c) = self.cursor_mut(id) {
                let idx = c.index;
                c.set(idx, len, visible, step);
            }
        }
    }

    /// The Alt+1 / Alt+2 selector's rows: the local volumes, then the other
    /// contents, then the open shares and extra local roots (a local storage
    /// that IS a listed volume is its drive row).
    pub fn selector_rows(&self) -> Vec<SelectorRow> {
        let mut rows: Vec<SelectorRow> = (0..self.drives.len()).map(SelectorRow::Drive).collect();
        rows.extend(ContentKind::OPENABLE.iter().copied().map(SelectorRow::Kind));
        rows.extend(
            self.storages
                .iter()
                .enumerate()
                .filter(|(_, s)| self.drive_of_storage(s).is_none())
                .map(|(i, _)| SelectorRow::Storage(i)),
        );
        rows
    }

    /// The drive row a local storage stands for (its id is its root).
    fn drive_of_storage(&self, s: &StorageInfo) -> Option<usize> {
        if s.r#type != StorageType::Local {
            return None;
        }
        self.drives.iter().position(|d| d.root == s.storage_id)
    }

    /// The selector row that is `id`'s current content; 0 when unlisted.
    pub fn selector_index(&self, id: PanelId) -> usize {
        let rows = self.selector_rows();
        let wanted = match &self.panel(id).content {
            Content::Files(f) => {
                let Some(sid) = f.storage_id() else { return 0 };
                let Some(i) = self.storages.iter().position(|s| s.storage_id == sid) else {
                    return 0;
                };
                match self.drive_of_storage(&self.storages[i]) {
                    Some(d) => SelectorRow::Drive(d),
                    None => SelectorRow::Storage(i),
                }
            }
            other => SelectorRow::Kind(other.kind()),
        };
        rows.iter().position(|r| *r == wanted).unwrap_or(0)
    }

    // ── nearby ─────────────────────────────────────────────────────────────

    pub fn nearby_device(&self, agent_id: &str) -> Option<&NearbyDevice> {
        self.nearby.iter().find(|d| d.agent_id == agent_id)
    }

    /// A device row: `name [offer status]`.
    pub fn nearby_row_label(&self, d: &NearbyDevice) -> String {
        let mut name = d.name.clone();
        if let Some(o) = self.offers.get(&d.agent_id) {
            name = format!("{} [{}]", name, o.label());
        }
        name
    }

    /// The device F5/F8 on `panel` act on: the opened device, else the one
    /// under the cursor. `(agent id, name)`.
    pub fn nearby_target(&self, id: PanelId) -> Option<(String, String)> {
        let Content::Nearby(list) = &self.panel(id).content else {
            return None;
        };
        let dev = match &list.open {
            Some(agent_id) => self.nearby_device(agent_id)?,
            None => self.nearby.get(list.cursor.index)?,
        };
        Some((dev.agent_id.clone(), dev.name.clone()))
    }

    /// A device's details view: (label, value) rows.
    pub fn nearby_details(&self, agent_id: &str) -> Option<Vec<(&'static str, String)>> {
        let dev = self.nearby_device(agent_id)?;
        let dash = || "—".to_string();
        let offer = self
            .offers
            .get(agent_id)
            .map(|o| o.label())
            .unwrap_or_else(|| "none".to_string());
        Some(vec![
            ("Name", dev.name.clone()),
            (
                "User",
                dev.display_name.clone().filter(|u| !u.is_empty()).unwrap_or_else(dash),
            ),
            ("Device", nearby_device_label(&dev.device)),
            (
                "OS",
                Some(os_name(dev.device.os).to_string())
                    .filter(|s| !s.is_empty())
                    .unwrap_or_else(dash),
            ),
            ("Location", dev.device.location.clone().unwrap_or_else(dash)),
            ("Agent ID", dev.agent_id.clone()),
            ("Fingerprint", dev.fingerprint.clone().unwrap_or_default()),
            (
                "Public key",
                dev.device_public_key.as_deref().map(to_base64).unwrap_or_default(),
            ),
            ("Offer", offer),
        ])
    }

    /// Reset the copy bookkeeping for a fresh transfer.
    pub fn begin_transfer(&mut self) {
        self.transfer_active = true;
        self.transfer_progress = TransferProgress::default();
        self.transfer_conflicts.clear();
        self.transfer_conflict_total = 0;
        self.pending_conflict_resolutions.clear();
        self.transfer_start_time = Some(Instant::now());
    }
}

impl Default for App {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn files(names: &[&str]) -> FileList {
        let mut f = FileList::new();
        f.entries = names.iter().map(|n| StorageObjectInfo::new(*n)).collect();
        f
    }

    #[test]
    fn quick_search_prefix_wraps_and_ignores_case() {
        let f = files(&["..", "Cargo.toml", "docs", "README.md", "rust", "src"]);
        assert_eq!(f.find_prefix("r", 0, false), Some(3));
        assert_eq!(f.find_prefix("r", 3, true), Some(4));
        assert_eq!(f.find_prefix("r", 4, true), Some(3)); // wrapped
        assert_eq!(f.find_prefix("RE", 5, false), Some(3));
        assert_eq!(f.find_prefix("d", 2, false), Some(2));
        assert_eq!(f.find_prefix("zzz", 0, false), None);
        assert_eq!(files(&[]).find_prefix("a", 0, false), None);
    }

    #[test]
    fn cursor_keeps_the_row_in_view() {
        let mut c = Cursor::default();
        c.move_by(7, 10, 5, 1);
        assert_eq!((c.index, c.scroll), (7, 3));
        c.move_by(-7, 10, 5, 1);
        assert_eq!((c.index, c.scroll), (0, 0));
        c.set(99, 10, 5, 1);
        assert_eq!((c.index, c.scroll), (9, 5));
        c.set(0, 0, 5, 1);
        assert_eq!((c.index, c.scroll), (0, 0));
    }

    #[test]
    fn single_column_views_scroll_by_one_row() {
        let mut panel = Panel::new();
        panel.visible_height = 20;
        for view in [ViewMode::Full, ViewMode::Wide] {
            panel.view = view;
            assert_eq!(panel.step(), 1);
        }
        panel.view = ViewMode::Medium;
        assert_eq!(panel.step(), 10);
        panel.view = ViewMode::Brief;
        assert_eq!(panel.step(), 6);
        // Any other content is one column of rows.
        panel.show(ContentKind::Links);
        assert_eq!(panel.step(), 1);
    }

    #[test]
    fn multi_column_cursor_scrolls_by_whole_columns() {
        // 3 columns × 4 rows = 12 cells; the step is a column of 4.
        let mut c = Cursor::default();
        c.move_by(12, 30, 12, 4);
        assert_eq!((c.index, c.scroll), (12, 4));
        c.move_by(1, 30, 12, 4);
        assert_eq!((c.index, c.scroll), (13, 4));
        c.set(3, 30, 12, 4);
        assert_eq!((c.index, c.scroll), (3, 0));
    }

    #[test]
    fn sort_modes_keep_dotdot_and_dirs_first() {
        let mut f = FileList::new();
        let mut d = StorageObjectInfo::new("zdir");
        d.file_type = StorageFileType::Dir;
        let mut a = StorageObjectInfo::new("a.txt");
        a.size = 5;
        let mut b = StorageObjectInfo::new("b.rs");
        b.size = 9;
        f.entries = vec![StorageObjectInfo::new(".."), a, b, d];
        f.sort_entries(SortMode::Size);
        let keys: Vec<&str> = f.entries.iter().map(|e| e.key.as_str()).collect();
        assert_eq!(keys, ["..", "zdir", "b.rs", "a.txt"]);
        f.sort_entries(SortMode::Extension);
        let keys: Vec<&str> = f.entries.iter().map(|e| e.key.as_str()).collect();
        assert_eq!(keys, ["..", "zdir", "b.rs", "a.txt"]);
        f.sort_entries(SortMode::Name);
        let keys: Vec<&str> = f.entries.iter().map(|e| e.key.as_str()).collect();
        assert_eq!(keys, ["..", "zdir", "a.txt", "b.rs"]);
    }

    #[test]
    fn toggling_a_content_stashes_and_restores_the_listing() {
        let mut p = Panel::new();
        p.files_mut().unwrap().current_path = "a/b".to_string();
        assert!(p.toggle_content(ContentKind::Connection));
        assert_eq!(p.content.kind(), ContentKind::Connection);
        assert!(p.files().is_none());
        // Another info content replaces it; the listing stays stashed.
        p.show(ContentKind::Log);
        assert!(p.toggle_content(ContentKind::Log));
        assert_eq!(p.files().unwrap().current_path, "a/b");
        // Toggling with nothing stashed changes nothing.
        let mut q = Panel::new();
        q.show(ContentKind::Nearby);
        q.stash = None;
        assert!(!q.toggle_content(ContentKind::Nearby));
    }

    #[test]
    fn line_edit_edits_at_char_cursor() {
        let mut e = LineEdit::default();
        for ch in "écho".chars() {
            e.insert(ch);
        }
        assert_eq!(e.text, "écho");
        assert_eq!(e.cursor, 4);
        e.cursor = 1;
        e.insert('x');
        assert_eq!(e.text, "éxcho");
        e.backspace();
        assert_eq!((e.text.as_str(), e.cursor), ("écho", 1));
        e.delete();
        assert_eq!(e.text, "ého");
        e.backspace();
        assert_eq!((e.text.as_str(), e.cursor), ("ho", 0));
        e.backspace(); // at 0 — no-op
        assert_eq!(e.text, "ho");
        e.end();
        e.left();
        e.insert('!');
        assert_eq!(e.text, "h!o");
        e.home();
        e.right();
        e.right();
        e.delete();
        assert_eq!(e.text, "h!");
    }
}
