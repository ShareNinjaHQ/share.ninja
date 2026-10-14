//! TUI event loop handling

use std::io;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossterm::{
    event::{
        self, DisableBracketedPaste, EnableBracketedPaste, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers,
        KeyboardEnhancementFlags, PopKeyboardEnhancementFlags, PushKeyboardEnhancementFlags,
    },
    execute,
    terminal::{
        EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode, supports_keyboard_enhancement,
    },
};
use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
};

use share_ninja_core::transfers::{
    ConflictDecision, ErrorDecision, OnExisting, SearchOptions, StorageSearchQuery, TransferItemState,
};

/// Alt+F7: entries per search page — a page lands in the results as one.
const FIND_PAGE_LIMIT: i32 = 200;
use share_ninja_core::{StorageFileType, StorageObjectInfo};

use crate::app::{
    App, AppMode, Content, ContentKind, ExternalCommand, FileList, FileStorage, FindState, FoundList, LinkList,
    OpenedFrom, SelectorRow, is_dir, is_file,
};
use crate::engine::{Engine, basename, storage_error_text};
#[cfg(unix)]
use crate::shell::{Left, Until};
use crate::shell::{Shell, console_run};
use crate::events::{EventReceiver, EventSender, PanelId, StartTransfer, TransferDest, UiEvent};
use crate::local_path;
use crate::log_capture::LogBuffer;
use crate::menu::{self, MenuAction, MenuBarKind};
use crate::types::{InboundOfferView, StepUpOutcome, StorageConnectRequest, StorageType};
use crate::ui::panel::format_compact;
use crate::ui::skin;
use crate::ui::{
    AddStorageDialog, CommandLine, ConnectionPane, CopyConfirmDialog, DeleteConfirmDialog, DetailsPanel, ErrorDialog,
    FileConflictDialog, FindDialog, FindResultsDialog, HelpDialog, InfoPanel, InputDialog, ListPanel,
    ListingErrorDialog, LogPane, MenuBar, MkdirDialog, PlayerDialog, ProgressDialog, RecoveryWordsDisplayDialog,
    SelectorItem, ShareOfferDialog, SkinSelectDialog, StatusBar, StepUpCodeDialog, StorageListDialog, TetrisWidget,
    TransferErrorDialog, UserPane, VaultJoinNotifyDialog, VaultJoinPromptDialog, VaultJoinRequestsDialog,
    VaultRecoveryInputDialog,
    VaultRecoveryRegenerateConfirmDialog, VaultRevokeConfirmDialog, VaultRevokedNotifyDialog, VaultTrustCircleDialog,
    ViewerWidget, find_list_height,
};
use crate::viewer::{self, Move, SearchOutcome, ViewerState};

pub struct EventLoop {
    pub app: App,
    /// The terminal takes the kitty keyboard protocol: modifiers arrive on
    /// every key (Shift+arrows, Ctrl+Shift+Z), and a held key repeats as
    /// `Repeat` events.
    kitty_keys: bool,
    engine: Arc<Engine>,
    tx: EventSender,
    rx: EventReceiver,
    rt: tokio::runtime::Handle,
    terminal: Terminal<CrosstermBackend<io::Stdout>>,
    log_buffer: LogBuffer,
    /// Name to focus after directory listing completes (for go_back)
    pending_focus_name: Option<String>,
    /// The command line's shell, on the normal screen behind the panels.
    shell: Shell,
}

impl EventLoop {
    pub fn new(
        app: App,
        engine: Arc<Engine>,
        tx: EventSender,
        rx: EventReceiver,
        rt: tokio::runtime::Handle,
        log_buffer: LogBuffer,
    ) -> io::Result<Self> {
        enable_raw_mode()?;
        let kitty_keys = supports_keyboard_enhancement().unwrap_or(false);
        let (cols, rows) = crossterm::terminal::size()?;
        let shell = Shell::open(cols, rows);
        let mut stdout = io::stdout();
        execute!(stdout, EnterAlternateScreen)?;
        enter_input_modes(kitty_keys)?;
        let backend = CrosstermBackend::new(stdout);
        let terminal = Terminal::new(backend)?;

        Ok(Self {
            app,
            kitty_keys,
            engine,
            tx,
            rx,
            rt,
            terminal,
            log_buffer,
            pending_focus_name: None,
            shell,
        })
    }

    // ── engine calls ───────────────────────────────────────────────────────
    //
    // Every engine call runs on the runtime; its outcome comes back through
    // the event channel and is applied in `process_events`.

    fn spawn<F>(&self, fut: F)
    where
        F: std::future::Future<Output = UiEvent> + Send + 'static,
    {
        let tx = self.tx.clone();
        self.rt.spawn(async move {
            let _ = tx.send(fut.await);
        });
    }

    fn spawn_silent<F>(&self, fut: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        self.rt.spawn(fut);
    }

    fn req_list_storages(&self) {
        let engine = self.engine.clone();
        self.spawn(async move { UiEvent::ListStorages(engine.list_storages().await) });
    }

    /// Stream the level into the panel: a `ListDirBatch` per batch with the
    /// keys cut to names, then `ListDirDone`. The task's handle lets the
    /// next listing abort this one.
    fn req_list_dir(&self, panel: PanelId, seq: u64, storage_id: String, path: String) -> tokio::task::AbortHandle {
        let engine = self.engine.clone();
        let tx = self.tx.clone();
        let handle = self.rt.spawn(async move {
            let (mut rx, space) = match engine.list_dir(&storage_id, &path).await {
                Ok(v) => v,
                Err(e) => {
                    let _ = tx.send(UiEvent::ListDirDone {
                        panel,
                        seq,
                        result: Err(e),
                    });
                    return;
                }
            };
            while let Some(batch) = rx.recv().await {
                let mut items = match batch {
                    Ok(items) => items,
                    Err(e) => {
                        let _ = tx.send(UiEvent::ListDirDone {
                            panel,
                            seq,
                            result: Err(storage_error_text(&e)),
                        });
                        return;
                    }
                };
                for soi in &mut items {
                    soi.key = basename(&soi.key);
                }
                if tx.send(UiEvent::ListDirBatch { panel, seq, items }).is_err() {
                    return;
                }
            }
            let _ = tx.send(UiEvent::ListDirDone {
                panel,
                seq,
                result: Ok(space),
            });
        });
        handle.abort_handle()
    }

    fn req_drives(&self) {
        let engine = self.engine.clone();
        self.spawn(async move { UiEvent::Drives(engine.local_drives().await) });
    }

    fn req_usage(&self) {
        let engine = self.engine.clone();
        self.spawn(async move { UiEvent::Usage(engine.usage_space().await) });
    }

    fn req_list_links(&self, panel: PanelId) {
        let engine = self.engine.clone();
        self.spawn(async move {
            UiEvent::ListLinks {
                panel,
                result: engine.list_links().await,
            }
        });
    }

    fn req_connect_storage(&self, panel: PanelId, name: String, request: StorageConnectRequest) {
        let engine = self.engine.clone();
        let storage_type = request.storage_type();
        self.spawn(async move {
            let result = engine.connect_storage(&name, request).await;
            UiEvent::ConnectStorage {
                panel,
                name,
                storage_type,
                result,
            }
        });
    }

    fn req_delete(&self, storage_id: String, path: String) {
        let engine = self.engine.clone();
        self.spawn(async move { UiEvent::Delete(engine.delete(&storage_id, &path).await) });
    }

    fn req_delete_link(&self, token: String) {
        let engine = self.engine.clone();
        self.spawn(async move { UiEvent::Delete(engine.delete_link(&token).await) });
    }

    fn req_mkdir(&self, storage_id: String, path: String) {
        let engine = self.engine.clone();
        self.spawn(async move { UiEvent::Mkdir(engine.mkdir(&storage_id, &path).await) });
    }

    /// Every running total the walk publishes lands in the size column as
    /// it comes; the last one is the answer.
    fn req_dir_size(&self, panel: PanelId, storage_id: String, dir: String, key: String) {
        let engine = self.engine.clone();
        let tx = self.tx.clone();
        self.spawn_silent(async move {
            let path = if dir.is_empty() {
                key.clone()
            } else {
                format!("{}/{}", dir, key)
            };
            let mut rx = match engine.dir_size(&storage_id, &path).await {
                Ok(rx) => rx,
                Err(e) => {
                    let _ = tx.send(UiEvent::DirSize {
                        panel,
                        dir,
                        key,
                        result: Err(e),
                    });
                    return;
                }
            };
            while let Some(block) = rx.recv().await {
                let result = block.map(|s| s.total_size).map_err(|e| e.to_string());
                let failed = result.is_err();
                let _ = tx.send(UiEvent::DirSize {
                    panel,
                    dir: dir.clone(),
                    key: key.clone(),
                    result,
                });
                if failed {
                    return;
                }
            }
        });
    }

    fn req_rename(&self, storage_id: String, from: String, to: String) {
        let engine = self.engine.clone();
        self.spawn(async move {
            let result = engine.rename(&storage_id, &from, &to).await;
            UiEvent::Rename {
                result,
                to: to.rsplit('/').next().unwrap_or(&to).to_string(),
            }
        });
    }

    fn req_start_transfer(&self, req: StartTransfer) {
        let engine = self.engine.clone();
        self.spawn(async move { UiEvent::StartTransfer(engine.start_transfer(req).await) });
    }

    fn req_cancel_transfer(&self) {
        let engine = self.engine.clone();
        self.spawn(async move { UiEvent::CancelTransfer(engine.cancel_transfer().await) });
    }

    fn req_resolve_conflicts(&self, decision: ConflictDecision) {
        let engine = self.engine.clone();
        self.spawn_silent(async move { engine.resolve_conflicts(decision).await });
    }

    fn req_resolve_error(&self, decision: ErrorDecision) {
        let engine = self.engine.clone();
        self.spawn_silent(async move { engine.resolve_error(decision).await });
    }

    fn req_login(&self) {
        let engine = self.engine.clone();
        self.spawn(async move { UiEvent::LoginComplete(engine.login().await) });
    }

    fn req_logout(&self) {
        let engine = self.engine.clone();
        self.spawn(async move { UiEvent::LogoutResult(engine.logout().await) });
    }

    fn req_vault_approve(&self, agent_id: String) {
        let engine = self.engine.clone();
        self.spawn(async move { UiEvent::VaultApproveResult(engine.vault_approve_join(&agent_id).await) });
    }

    fn req_vault_reject(&self, agent_id: String) {
        let engine = self.engine.clone();
        self.spawn(async move { UiEvent::VaultRejectResult(engine.vault_reject_join(&agent_id).await) });
    }

    fn req_vault_revoke(&self, agent_id: String) {
        let engine = self.engine.clone();
        self.spawn(async move { UiEvent::VaultRevokeResult(engine.vault_revoke_device(&agent_id).await) });
    }

    fn req_vault_regenerate(&self) {
        let engine = self.engine.clone();
        self.spawn(async move { UiEvent::VaultRecoveryRegenerateResult(engine.vault_regenerate_recovery().await) });
    }

    fn req_vault_join_start(&self) {
        let engine = self.engine.clone();
        self.spawn(async move { UiEvent::VaultJoinStartResult(engine.vault_join_start().await) });
    }

    fn req_vault_recover(&self, mnemonic: String) {
        let engine = self.engine.clone();
        self.spawn(async move { UiEvent::VaultRecoverResult(engine.vault_recover(&mnemonic).await) });
    }

    fn req_vault_step_up_verify(&self, code: String) {
        let engine = self.engine.clone();
        self.spawn(async move { UiEvent::VaultStepUpVerifyResult(engine.vault_step_up_verify(&code).await) });
    }

    pub fn run(&mut self) -> io::Result<()> {
        // Request initial directory listing for panels that have storage connected
        self.request_initial_listings();

        let mut vault_devices_poll_counter = 0u32;
        // A frame is drawn only when it can differ from the last one: after
        // input, an engine event, new log lines, or while something animates
        // (`animating`). An idle loop just polls — the panels are rebuilt
        // from scratch per draw, which at 20 Hz is a few percent of a core.
        let mut dirty = true;
        loop {
            // Handle pending external command BEFORE drawing
            if let Some(cmd) = self.app.pending_command.take() {
                self.run_external_command(&cmd)?;
                // A shell may have touched the panel's directory — or, the
                // subshell, moved out of it.
                if matches!(cmd, ExternalCommand::Shell { .. } | ExternalCommand::Subshell) && !self.follow_subshell_cwd() {
                    self.refresh_active_panel();
                }
                dirty = true;
            }

            if self.process_events() {
                dirty = true;
            }

            // An offer that arrived behind another dialog: shown once the
            // screen is free again.
            if self.app.mode == AppMode::Normal
                && let Some(offer) = self.app.dialog_state.share_offer_pending.take()
            {
                self.open_share_offer_dialog(offer);
                dirty = true;
            }

            // Sync logs from capture buffer to app's log pane
            // (after process_events so logs emitted there are visible immediately)
            if self.sync_logs() {
                dirty = true;
            }

            if dirty || self.animating() {
                self.draw()?;
                dirty = false;
            }

            // Auto-refresh the trust-circle list every ~3s while its dialog
            // is open (join requests are WS-fed — no polling).
            match self.app.mode {
                AppMode::VaultTrustCircleDialog => {
                    vault_devices_poll_counter += 1;
                    if vault_devices_poll_counter >= 60 {
                        vault_devices_poll_counter = 0;
                        self.fetch_vault_devices();
                        dirty = true;
                    }
                }
                _ => {
                    vault_devices_poll_counter = 0;
                }
            }

            // Use shorter poll when loading so fast responses are processed at once
            let poll_duration = if self.app.left_panel.loading || self.app.right_panel.loading {
                Duration::from_millis(5)
            } else {
                Duration::from_millis(50)
            };

            // Windows reports key releases (and repeats) as key events too;
            // only a press is an action, or every key fires twice. Under the
            // kitty protocol a held key comes as repeats and nothing else.
            if event::poll(poll_duration)? {
                match event::read()? {
                    Event::Key(key)
                        if key.kind == KeyEventKind::Press || (self.kitty_keys && key.kind == KeyEventKind::Repeat) =>
                    {
                        self.handle_key(key);
                    }
                    Event::Paste(text) => self.handle_paste(&text),
                    _ => {}
                }
                dirty = true;
            }

            if self.app.should_quit {
                break;
            }
        }
        self.save_state();
        Ok(())
    }

    /// What changes on screen with time alone, nobody typing and no event
    /// landing: the panels' loading spinners, the copy dialog's clock, the
    /// player's spectrum, tetris gravity, and a local viewer, whose
    /// `viewer_tick` fetches the blocks the previous frame's layout asked for.
    fn animating(&self) -> bool {
        self.app.left_panel.loading
            || self.app.right_panel.loading
            || matches!(
                self.app.mode,
                AppMode::Copying
                    | AppMode::ConflictResolution
                    | AppMode::TransferErrorDialog
                    | AppMode::Player
                    | AppMode::Tetris
            )
            || self.app.viewer.as_ref().is_some_and(|v| v.local)
    }

    /// `true` when lines were moved into the log pane.
    fn sync_logs(&mut self) -> bool {
        let mut buffer = self.log_buffer.lock();
        if buffer.is_empty() {
            return false;
        }
        for msg in buffer.drain(..) {
            self.app.log_messages.push(msg);
        }
        // Keep only the last N messages
        const MAX_LOG_MESSAGES: usize = 1000;
        if self.app.log_messages.len() > MAX_LOG_MESSAGES {
            let excess = self.app.log_messages.len() - MAX_LOG_MESSAGES;
            self.app.log_messages.drain(..excess);
        }
        true
    }

    fn run_external_command(&mut self, cmd: &ExternalCommand) -> io::Result<()> {
        self.release_terminal()?;
        match cmd {
            ExternalCommand::Program { cmd, path } => {
                // The editor takes the terminal cooked and owns the screen.
                disable_raw_mode()?;
                let _ = std::process::Command::new(cmd).arg(path).status();
                enable_raw_mode()?;
            }
            ExternalCommand::Shell { cwd, command } => self.shell_run(cwd.as_deref(), command)?,
            ExternalCommand::Subshell => self.shell_view()?,
        }
        self.reclaim_terminal()
    }

    /// Hand the terminal over: the input modes off, the normal screen with
    /// its cursor. Raw mode stays — the shell views relay keys themselves.
    /// Synchronized output (DEC 2026) makes the switch one frame; the normal
    /// buffer is kept as it is — it is the shell's screen.
    fn release_terminal(&mut self) -> io::Result<()> {
        use std::io::Write;
        leave_input_modes(self.kitty_keys);
        let mut stdout = io::stdout().lock();
        write!(stdout, "\x1b[?2026h\x1b[?1049l\x1b[?25h\x1b[?2026l")?;
        stdout.flush()
    }

    /// The terminal back: the alternate screen for the panels, the input
    /// modes, and a full redraw.
    fn reclaim_terminal(&mut self) -> io::Result<()> {
        execute!(io::stdout(), EnterAlternateScreen, crossterm::cursor::Hide)?;
        enter_input_modes(self.kitty_keys)?;
        self.terminal.clear()
    }

    /// One command line: into the subshell, moved to `cwd` first when the
    /// panel has one; or on the console in `cwd`.
    fn shell_run(&mut self, cwd: Option<&std::path::Path>, command: &str) -> io::Result<()> {
        match &mut self.shell {
            #[cfg(unix)]
            Shell::Pty(pty) => {
                let result = match cwd.map(|dir| pty.sync_cwd(dir)).unwrap_or(Ok(())) {
                    Ok(()) => pty.run(command),
                    Err(e) => Err(e),
                };
                self.settle_subshell(result);
                Ok(())
            }
            Shell::Console => match cwd {
                Some(cwd) => console_run(cwd, command),
                None => Ok(()),
            },
        }
    }

    /// Ctrl+O: the subshell until Ctrl+O at its prompt (moved to the
    /// panel's directory first), or the console view.
    fn shell_view(&mut self) -> io::Result<()> {
        let cwd = self.command_cwd();
        match &mut self.shell {
            #[cfg(unix)]
            Shell::Pty(pty) => {
                let result = match cwd.map(|dir| pty.sync_cwd(&dir)).unwrap_or(Ok(())) {
                    Ok(()) => pty.view(Until::CtrlO),
                    Err(e) => Err(e),
                };
                self.settle_subshell(result);
                Ok(())
            }
            Shell::Console => self.console_view(),
        }
    }

    /// How a subshell turn ended: its exit ends the session (MC), a
    /// failure hands the command line to the console for the rest of it.
    #[cfg(unix)]
    fn settle_subshell(&mut self, result: io::Result<Left>) {
        match result {
            Ok(Left::Prompt) => {}
            Ok(Left::Died) => {
                tracing::info!("subshell exited: quitting");
                self.app.should_quit = true;
            }
            Err(e) => {
                tracing::warn!("subshell: {e}; commands run on the console now");
                self.app.active_panel_mut().error = Some(format!("Subshell: {e}. Commands run on the console now."));
                self.shell = Shell::Console;
            }
        }
    }

    /// Back from the subshell: a `cd` typed there moves the local panel
    /// along, when the shell stayed under the panel's root. `true` when it
    /// did (the listing is on its way).
    fn follow_subshell_cwd(&mut self) -> bool {
        #[cfg(unix)]
        if let Shell::Pty(pty) = &self.shell {
            let shell_cwd = pty.cwd().to_path_buf();
            if self.command_cwd().is_some_and(|panel_cwd| panel_cwd != shell_cwd) {
                return self.navigate_local(&shell_cwd);
            }
        }
        false
    }

    /// FAR's Ctrl+O: the console buffer with the command line live at its
    /// bottom row — Enter runs the line right there (`cd` moves the
    /// panel), Alt+P/N walk the history, Ctrl+O brings the panels back.
    fn console_view(&mut self) -> io::Result<()> {
        use crossterm::cursor::MoveToColumn;
        use crossterm::queue;
        use crossterm::style::Print;
        use crossterm::terminal::{Clear, ClearType};
        use std::io::Write;
        if crossterm::cursor::position()?.0 > 0 {
            execute!(io::stdout(), Print("\r\n"))?;
        }
        loop {
            {
                let prompt = self.command_prompt();
                let (cols, _) = crossterm::terminal::size()?;
                let cl = CommandLine::new(&prompt, &self.app.command.text, self.app.command.cursor);
                let (line, col) = cl.inline(cols as usize);
                let mut out = io::stdout().lock();
                queue!(out, MoveToColumn(0), Clear(ClearType::CurrentLine), Print(line), MoveToColumn(col))?;
                out.flush()?;
            }
            let Event::Key(key) = event::read()? else {
                continue;
            };
            if key.kind != KeyEventKind::Press {
                continue;
            }
            match key.code {
                KeyCode::Char('o') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    execute!(io::stdout(), MoveToColumn(0), Clear(ClearType::CurrentLine))?;
                    return Ok(());
                }
                KeyCode::Enter => {
                    let line = self.app.command.text.trim().to_string();
                    self.app.command_clear();
                    if line.is_empty() {
                        continue;
                    }
                    if self.app.command_history.last() != Some(&line) {
                        self.app.command_history.push(line.clone());
                    }
                    execute!(io::stdout(), MoveToColumn(0), Clear(ClearType::CurrentLine))?;
                    if line == "cd" || line.starts_with("cd ") {
                        let arg = line.strip_prefix("cd").expect("checked prefix").trim().to_string();
                        self.change_directory(&arg);
                        continue;
                    }
                    let Some(cwd) = self.command_cwd() else {
                        execute!(io::stdout(), Print("Commands run on local panels only\r\n"))?;
                        continue;
                    };
                    tracing::info!("Action: Run command [{}]", line);
                    console_run(&cwd, &line)?;
                }
                KeyCode::Char('p') if key.modifiers.contains(KeyModifiers::ALT) => self.command_history_prev(),
                KeyCode::Char('n') if key.modifiers.contains(KeyModifiers::ALT) => self.command_history_next(),
                _ => {
                    if self.app.command.handle_key(key) {
                        self.app.command_history_index = None;
                    }
                }
            }
        }
    }

    // ── command line ───────────────────────────────────────────────────────

    /// The absolute root of the active panel's storage, when it is local.
    /// A local storage's id is its root path — see `Engine::connect_storage`.
    fn local_panel_root(&self) -> Option<std::path::PathBuf> {
        let files = self.app.active_files()?;
        if !files.is_local() {
            return None;
        }
        Some(std::path::PathBuf::from(files.storage_id()?))
    }

    /// The directory shell commands run in: the active panel's directory,
    /// when it is a local storage.
    fn command_cwd(&self) -> Option<std::path::PathBuf> {
        let root = self.local_panel_root()?;
        let files = self.app.active_files()?;
        Some(local_path::join(&root.to_string_lossy(), &files.current_path))
    }

    /// The command line prompt: the directory commands would run in
    /// (home-shortened on unix), or a bare `$` on a non-local panel.
    fn command_prompt(&self) -> String {
        match self.command_cwd() {
            Some(cwd) => local_path::prompt(&cwd),
            None => "$ ".to_string(),
        }
    }

    /// Enter on a non-empty command line: `cd` navigates the panel, anything
    /// else goes to the shell — the subshell where it is (a local panel's
    /// directory first), the console in the panel's directory only.
    fn run_command_line(&mut self) {
        let line = self.app.command.text.trim().to_string();
        self.app.command_clear();
        if line.is_empty() {
            return;
        }
        if self.app.command_history.last() != Some(&line) {
            self.app.command_history.push(line.clone());
        }
        if line == "cd" || line.starts_with("cd ") {
            let arg = line.strip_prefix("cd").expect("checked prefix").trim().to_string();
            self.change_directory(&arg);
            return;
        }
        let cwd = self.command_cwd();
        if cwd.is_none() && matches!(self.shell, Shell::Console) {
            self.app.active_panel_mut().status_message = Some("Commands run on local panels only".to_string());
            return;
        }
        tracing::info!("Action: Run command [{}]", line);
        self.app.pending_command = Some(ExternalCommand::Shell { cwd, command: line });
    }

    /// `cd [dir]` on the command line: navigate the active panel (local
    /// only). No shell involved: `~` expands, relative paths resolve
    /// against the panel directory, `.`/`..` normalize lexically.
    fn change_directory(&mut self, arg: &str) {
        let Some(cwd) = self.command_cwd() else {
            self.app.active_panel_mut().status_message = Some("Commands run on local panels only".to_string());
            return;
        };
        // Strip simple surrounding quotes.
        let arg = arg
            .strip_prefix('"')
            .and_then(|s| s.strip_suffix('"'))
            .or_else(|| arg.strip_prefix('\'').and_then(|s| s.strip_suffix('\'')))
            .unwrap_or(arg);
        let target = if arg.is_empty() || arg == "~" {
            let Some(home) = dirs::home_dir() else { return };
            home
        } else if let Some(rest) = arg.strip_prefix("~/") {
            let Some(home) = dirs::home_dir() else { return };
            home.join(rest)
        } else if std::path::Path::new(arg).is_absolute() {
            std::path::PathBuf::from(arg)
        } else {
            cwd.join(arg)
        };
        let target = normalize_lexically(&target);
        if !self.navigate_local(&target) {
            self.app.active_panel_mut().status_message = Some("cd: outside this panel's root".to_string());
        }
    }

    /// The active local panel to the absolute `target`, listing it; `false`
    /// when `target` is outside the panel's root (nothing changes).
    fn navigate_local(&mut self, target: &std::path::Path) -> bool {
        let Some(root) = self.local_panel_root() else {
            return false;
        };
        let Ok(rel) = target.strip_prefix(&root) else {
            return false;
        };
        let new_path = local_path::key(rel);
        tracing::info!("Action: cd [{}]", target.display());
        let Some(files) = self.app.active_files_mut() else {
            return false;
        };
        files.marked.clear();
        files.previous_path = Some(files.current_path.clone());
        files.save_selection_to_history();
        files.current_path = new_path;
        files.reset_selection_on_load = true;
        self.refresh_active_panel();
        true
    }

    /// Ctrl+O: the shell's screen.
    fn open_subshell(&mut self) {
        tracing::info!("Action: Subshell");
        self.app.pending_command = Some(ExternalCommand::Subshell);
    }

    /// Alt+P: the previous history line (the unfinished line is stashed).
    fn command_history_prev(&mut self) {
        if self.app.command_history.is_empty() {
            return;
        }
        let idx = match self.app.command_history_index {
            None => {
                self.app.command_draft = self.app.command.text.clone();
                self.app.command_history.len() - 1
            }
            Some(0) => 0,
            Some(i) => i - 1,
        };
        self.app.command_history_index = Some(idx);
        self.app.command.set(self.app.command_history[idx].clone());
    }

    /// Alt+N: the next history line, back to the stashed draft at the end.
    fn command_history_next(&mut self) {
        let Some(i) = self.app.command_history_index else {
            return;
        };
        if i + 1 < self.app.command_history.len() {
            self.app.command_history_index = Some(i + 1);
            self.app.command.set(self.app.command_history[i + 1].clone());
        } else {
            self.app.command_history_index = None;
            let draft = std::mem::take(&mut self.app.command_draft);
            self.app.command.set(draft);
        }
    }

    /// (Re)list `panel_id`'s content: a file list on a storage, or the
    /// links. Other contents render from state the events keep fresh.
    fn request_listing(&mut self, panel_id: PanelId) {
        enum Listing {
            Dir(String, String),
            Links,
        }
        let panel = self.app.panel_mut(panel_id);
        let listing = match &panel.content {
            // A find's temporary list is not a directory: nothing to list.
            Content::Files(f) if f.temporary.is_some() => return,
            Content::Files(f) => match f.storage_id() {
                Some(id) => Listing::Dir(id.to_string(), f.current_path.clone()),
                None => return,
            },
            Content::Links(_) => Listing::Links,
            _ => return,
        };
        panel.loading = true;
        panel.loading_started = Some(Instant::now());
        match listing {
            Listing::Dir(storage_id, path) => {
                // A superseded listing stops streaming; the new one's first
                // batch replaces what is shown.
                panel.stop_listing();
                let seq = panel.listing_seq;
                if let Some(files) = panel.files_mut() {
                    files.replace_on_batch = true;
                }
                let handle = self.req_list_dir(panel_id, seq, storage_id, path);
                self.app.panel_mut(panel_id).listing = Some(handle);
            }
            Listing::Links => self.req_list_links(panel_id),
        }
    }

    fn request_initial_listings(&mut self) {
        // The selector's storage rows are kept current from here on (and
        // after every connect), so it is complete the frame it opens.
        self.req_list_storages();
        self.req_drives();
        self.request_listing(PanelId::Left);
        self.request_listing(PanelId::Right);
    }

    pub fn fetch_vault_devices(&mut self) {
        self.app.dialog_state.vault_devices_loading = true;
        let engine = self.engine.clone();
        self.spawn(async move { UiEvent::VaultDevices(engine.vault_devices().await) });
    }

    pub fn request_connect_storage(&mut self, panel: PanelId, name: String, request: StorageConnectRequest) {
        let storage_type = request.storage_type();
        let target_panel = self.app.panel_mut(panel);
        target_panel.loading = true;
        target_panel.loading_started = Some(Instant::now());

        // A share needs the backend online (resolving the link and opening
        // its sources go through the session). Before the register handshake
        // has finished (typical at startup), queue it and let the
        // `BackendStatus` arm drain when the state transitions.
        if storage_type == StorageType::Ninja && !self.is_backend_online() {
            tracing::info!(panel = ?panel, "Backend not online yet — deferring ninja connect");
            self.app.pending_storage_connects.push((panel, name, request));
            return;
        }

        self.req_connect_storage(panel, name, request);
    }

    /// Whether the backend has reached at least `Online` per the most recent
    /// `BackendStatus` push.
    fn is_backend_online(&self) -> bool {
        self.app
            .backend_status
            .as_ref()
            .map(|s| s.connection.is_online())
            .unwrap_or(false)
    }

    /// Fire any storage connects that were queued while the backend was
    /// offline. Called on every `BackendStatus` transition into Online.
    fn drain_pending_storage_connects(&mut self) {
        let pending = std::mem::take(&mut self.app.pending_storage_connects);
        for (panel, name, request) in pending {
            tracing::info!(panel = ?panel, name = %name, "Backend online — firing deferred storage connect");
            self.req_connect_storage(panel, name, request);
        }
    }

    fn draw(&mut self) -> io::Result<()> {
        // If a panel produced an error and we're idle, surface it as an
        // alert instead of leaving it on the (now-removed) status bar.
        // Clear the panel error so the alert fires once.
        if self.app.mode == AppMode::Normal {
            let pending_error = self
                .app
                .left_panel
                .error
                .clone()
                .or_else(|| self.app.right_panel.error.clone());
            if let Some(err) = pending_error {
                self.app.left_panel.error = None;
                self.app.right_panel.error = None;
                self.show_vault_error("Error", err);
            }
        }

        // Calculate layout to get panel heights
        let term_size = self.terminal.size()?;
        let size = ratatui::layout::Rect::new(0, 0, term_size.width, term_size.height);

        self.viewer_tick(size);
        self.player_tick();
        if let Some(game) = self.app.tetris.as_mut() {
            game.tick();
        }

        // The viewer takes the whole screen (the help dialog sits on it);
        // the command line row exists only on the panel screen.
        let viewer_mode = self.app.viewer.is_some()
            && matches!(
                self.app.mode,
                AppMode::Viewer | AppMode::ViewerGotoDialog | AppMode::ViewerSearchDialog | AppMode::HelpDialog
            );
        // The editor fills the screen the same way (its dialogs sit on it).
        let editor_mode = self.app.editor.is_some()
            && matches!(
                self.app.mode,
                AppMode::Editor
                    | AppMode::EditorSearchDialog
                    | AppMode::EditorGotoDialog
                    | AppMode::EditorCloseConfirmDialog
                    | AppMode::EditorMenu
            );
        let fullscreen_mode = viewer_mode || editor_mode;

        // Main layout: content area + command line (panel screen only) + key
        // bar. The F9 menu bar overlays the top row; the panels stay put.
        let main_chunks = if fullscreen_mode {
            Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Min(3), Constraint::Length(1)])
                .split(size)
        } else {
            Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Min(3), Constraint::Length(1), Constraint::Length(1)])
                .split(size)
        };

        let content_area = main_chunks[0];
        let command_area = (!fullscreen_mode).then(|| main_chunks[1]);
        let command_prompt = self.command_prompt();

        let panel_halves = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
            .split(content_area);

        // Update visible capacity: rows (panel height minus borders, the
        // column header, the mini-status separator and row) times a
        // multi-column view's columns.
        let panel_rows = panel_halves[0].height.saturating_sub(5) as usize;
        for panel in [&mut self.app.left_panel, &mut self.app.right_panel] {
            panel.visible_height = panel_rows * panel.columns();
        }
        if let Some(find) = self.app.find.as_mut() {
            find.visible = find_list_height(size).max(1);
        }
        let key_labels = key_labels(&self.app.active_panel().content);

        let app = &self.app;
        let status_area = *main_chunks.last().expect("layout has rows");

        self.terminal.draw(|frame| {
            let size = frame.area();

            if viewer_mode {
                if let Some(v) = app.viewer.as_ref() {
                    frame.render_widget(ViewerWidget::new(v), content_area);
                    let wrap = v.wrap_label();
                    let hex = if v.hex { "Ascii" } else { "Hex" };
                    let labels = [
                        (" 1", "Help"),
                        (" 2", wrap),
                        (" 3", "Quit"),
                        (" 4", hex),
                        (" 5", "Goto"),
                        (" 6", ""),
                        (" 7", "Search"),
                        (" 8", ""),
                        (" 9", ""),
                        ("10", "Quit"),
                    ];
                    frame.render_widget(StatusBar::with_labels(&labels), status_area);
                }
                match app.mode {
                    AppMode::ViewerGotoDialog => {
                        let dialog = InputDialog::new(
                            "Goto",
                            "Offset (bytes, 0x hex, or NN%):",
                            &app.dialog_state.viewer_input.text,
                            app.dialog_state.viewer_input.cursor,
                            app.dialog_state.viewer_input_focus,
                        );
                        frame.render_widget(dialog, size);
                    }
                    AppMode::ViewerSearchDialog => {
                        let dialog = InputDialog::new(
                            "Search",
                            "Search for:",
                            &app.dialog_state.viewer_input.text,
                            app.dialog_state.viewer_input.cursor,
                            app.dialog_state.viewer_input_focus,
                        );
                        frame.render_widget(dialog, size);
                    }
                    AppMode::HelpDialog => {
                        frame.render_widget(HelpDialog, size);
                    }
                    _ => {}
                }
                return;
            }

            if editor_mode {
                if let Some(ed) = app.editor.as_ref() {
                    // Header line (name, [*], line:col, save error), then the
                    // buffer, then the editor's own key bar.
                    let header_area = ratatui::layout::Rect::new(content_area.x, content_area.y, content_area.width, 1);
                    let edit_area = ratatui::layout::Rect::new(
                        content_area.x,
                        content_area.y + 1,
                        content_area.width,
                        content_area.height.saturating_sub(1),
                    );
                    let header =
                        ratatui::widgets::Paragraph::new(ed.header()).style(crate::ui::skin::statusbar_default());
                    frame.render_widget(header, header_area);
                    frame.render_widget(&ed.textarea, edit_area);
                    let labels: [(&str, &str); 10] = [
                        (" 1", ""),
                        (" 2", "Save"),
                        (" 3", ""),
                        (" 4", ""),
                        (" 5", ""),
                        (" 6", ""),
                        (" 7", "Search"),
                        (" 8", ""),
                        (" 9", "Menu"),
                        ("10", "Quit"),
                    ];
                    frame.render_widget(StatusBar::with_labels(&labels), status_area);
                }
                match app.mode {
                    AppMode::EditorMenu => {
                        let d = &app.dialog_state;
                        let items = menu::items(app, d.menu_kind, d.menu_open);
                        let bar = MenuBar {
                            titles: menu::titles(d.menu_kind),
                            open: d.menu_open,
                            items: &items,
                            selected: d.menu_item,
                        };
                        frame.render_widget(bar, content_area);
                    }
                    AppMode::EditorSearchDialog => {
                        let dialog = InputDialog::new(
                            "Search",
                            "Search for (regex):",
                            &app.dialog_state.viewer_input.text,
                            app.dialog_state.viewer_input.cursor,
                            app.dialog_state.viewer_input_focus,
                        );
                        frame.render_widget(dialog, size);
                    }
                    AppMode::EditorGotoDialog => {
                        let dialog = InputDialog::new(
                            "Goto",
                            "Line number:",
                            &app.dialog_state.viewer_input.text,
                            app.dialog_state.viewer_input.cursor,
                            app.dialog_state.viewer_input_focus,
                        );
                        frame.render_widget(dialog, size);
                    }
                    AppMode::EditorCloseConfirmDialog => {
                        if let Some(ed) = app.editor.as_ref() {
                            let dialog = crate::ui::EditorCloseDialog {
                                name: &ed.title,
                                focus: app.dialog_state.editor_close_focus,
                            };
                            frame.render_widget(dialog, size);
                        }
                    }
                    _ => {}
                }
                return;
            }

            render_panel(frame, app, PanelId::Left, panel_halves[0]);
            render_panel(frame, app, PanelId::Right, panel_halves[1]);

            // The key bar follows the active panel's content; errors pop as
            // alerts (handled below).
            frame.render_widget(StatusBar::with_labels(&key_labels), status_area);

            // MC-style command line between the panels and the key bar; the
            // terminal cursor sits on it only while no dialog is open.
            if let Some(command_area) = command_area {
                let cl = CommandLine::new(&command_prompt, &app.command.text, app.command.cursor);
                if app.mode == AppMode::Normal {
                    frame.set_cursor_position((cl.cursor_x(command_area), command_area.y));
                }
                frame.render_widget(cl, command_area);
            }

            // Dialogs
            match app.mode {
                AppMode::HelpDialog => {
                    frame.render_widget(HelpDialog, size);
                }
                AppMode::Player => {
                    if let Some(player) = app.player.as_ref() {
                        frame.render_widget(PlayerDialog::new(player), size);
                    }
                }
                AppMode::Tetris => {
                    if let Some(game) = app.tetris.as_ref() {
                        frame.render_widget(TetrisWidget { game }, size);
                    }
                }
                AppMode::AddStorageDialog => {
                    let dialog = AddStorageDialog::new(
                        &app.dialog_state.input_name,
                        &app.dialog_state.input_url,
                        app.dialog_state.active_field,
                        0,
                    );
                    frame.render_widget(dialog, size);
                }
                AppMode::CopyConfirmDialog => {
                    let dialog = CopyConfirmDialog::new(
                        &app.dialog_state.copy_source_name,
                        &app.dialog_state.copy_dest_path,
                        app.dialog_state.copy_ok_selected,
                    );
                    frame.render_widget(dialog, size);
                }
                AppMode::DeleteConfirmDialog => {
                    let dialog = DeleteConfirmDialog::new(
                        &app.dialog_state.delete_name,
                        app.dialog_state.delete_is_dir,
                        app.dialog_state.delete_yes_selected,
                    );
                    frame.render_widget(dialog, size);
                }
                AppMode::RenameDialog => {
                    let label = format!("Rename {} to:", app.dialog_state.rename_from);
                    let dialog = InputDialog::new(
                        "Rename",
                        &label,
                        &app.dialog_state.rename.text,
                        app.dialog_state.rename.cursor,
                        app.dialog_state.rename_focus,
                    );
                    frame.render_widget(dialog, size);
                }
                AppMode::Menu => {
                    let d = &app.dialog_state;
                    let items = menu::items(app, d.menu_kind, d.menu_open);
                    let bar = MenuBar {
                        titles: menu::titles(d.menu_kind),
                        open: d.menu_open,
                        items: &items,
                        selected: d.menu_item,
                    };
                    frame.render_widget(bar, size);
                }
                AppMode::FindDialog => {
                    let d = &app.dialog_state;
                    let dialog = FindDialog {
                        name: &d.find_name.text,
                        cursor: d.find_name.cursor,
                        whole: d.find_whole,
                        recursive: d.find_recursive,
                        focus: d.find_focus,
                    };
                    frame.render_widget(dialog, size);
                }
                AppMode::FindResults => {
                    if let Some(find) = app.find.as_ref() {
                        let dialog = FindResultsDialog {
                            find,
                            root: &find.storage_id,
                        };
                        frame.render_widget(dialog, size);
                    }
                }
                AppMode::MkdirDialog => {
                    let dialog = MkdirDialog::new(
                        &app.dialog_state.mkdir.text,
                        app.dialog_state.mkdir.cursor,
                        app.dialog_state.mkdir_focus,
                    );
                    frame.render_widget(dialog, size);
                }
                AppMode::Copying | AppMode::ConflictResolution | AppMode::TransferErrorDialog => {
                    let elapsed_secs = app.transfer_start_time.map(|t| t.elapsed().as_secs()).unwrap_or(0);
                    let p = &app.transfer_progress;
                    let progress_dialog = ProgressDialog::new(
                        "Copying",
                        &app.dialog_state.copy_source_name,
                        &app.dialog_state.copy_dest_path,
                        p.current_file.as_deref(),
                        p.bytes,
                        p.total_bytes,
                        p.files_written + p.files_skipped,
                        p.total_files,
                        elapsed_secs,
                    );
                    frame.render_widget(progress_dialog, size);

                    if app.mode == AppMode::ConflictResolution {
                        // Conflict dialog on top of the progress dialog
                        if let Some(conflict) = app.transfer_conflicts.first() {
                            let conflict_index = app.transfer_conflict_total - app.transfer_conflicts.len() + 1;
                            let dialog = FileConflictDialog::new(
                                &conflict.source,
                                &conflict.dest,
                                app.dialog_state.conflict_focus,
                                conflict_index,
                                app.transfer_conflict_total,
                            );
                            frame.render_widget(dialog, size);
                        }
                    } else if app.mode == AppMode::TransferErrorDialog {
                        let dialog = TransferErrorDialog::new(
                            &app.dialog_state.transfer_error_message,
                            app.dialog_state.transfer_error_focus,
                        );
                        frame.render_widget(dialog, size);
                    }
                }
                AppMode::ListingErrorDialog => {
                    let dialog = ListingErrorDialog::new(&app.dialog_state.listing_error_message);
                    frame.render_widget(dialog, size);
                }
                AppMode::VaultJoinNotifyDialog => {
                    let dialog = VaultJoinNotifyDialog::new(
                        &app.dialog_state.vault_join_message,
                        app.dialog_state.vault_join_control_selected,
                    );
                    frame.render_widget(dialog, size);
                }
                AppMode::VaultTrustCircleDialog => {
                    let dialog = VaultTrustCircleDialog::new(
                        &app.dialog_state.vault_devices,
                        app.dialog_state.vault_devices_selected,
                        app.dialog_state.vault_devices_scroll,
                        app.dialog_state.vault_devices_focus,
                        app.dialog_state.vault_devices_error.as_deref(),
                        app.dialog_state.vault_devices_loading,
                    );
                    frame.render_widget(dialog, size);
                }
                AppMode::VaultJoinRequestsDialog => {
                    let dialog = VaultJoinRequestsDialog::new(
                        &app.dialog_state.vault_join_requests,
                        app.dialog_state.vault_join_selected,
                        app.dialog_state.vault_join_scroll,
                        app.dialog_state.vault_join_focus,
                        app.dialog_state.vault_join_error.as_deref(),
                        false,
                    );
                    frame.render_widget(dialog, size);
                }
                AppMode::VaultRevokeConfirmDialog => {
                    // Render the trust-circle dialog underneath so the user
                    // can still see what they're confirming against.
                    let bg_dialog = VaultTrustCircleDialog::new(
                        &app.dialog_state.vault_devices,
                        app.dialog_state.vault_devices_selected,
                        app.dialog_state.vault_devices_scroll,
                        app.dialog_state.vault_devices_focus,
                        app.dialog_state.vault_devices_error.as_deref(),
                        app.dialog_state.vault_devices_loading,
                    );
                    frame.render_widget(bg_dialog, size);
                    let confirm = VaultRevokeConfirmDialog::new(
                        &app.dialog_state.vault_revoke_device_name,
                        &app.dialog_state.vault_revoke_agent_id,
                        app.dialog_state.vault_revoke_yes_selected,
                    );
                    frame.render_widget(confirm, size);
                }
                AppMode::VaultErrorDialog => {
                    match app.dialog_state.vault_error_return_mode {
                        AppMode::VaultTrustCircleDialog => {
                            let bg_dialog = VaultTrustCircleDialog::new(
                                &app.dialog_state.vault_devices,
                                app.dialog_state.vault_devices_selected,
                                app.dialog_state.vault_devices_scroll,
                                app.dialog_state.vault_devices_focus,
                                app.dialog_state.vault_devices_error.as_deref(),
                                app.dialog_state.vault_devices_loading,
                            );
                            frame.render_widget(bg_dialog, size);
                        }
                        AppMode::StepUpCodeDialog => {
                            let bg_dialog = StepUpCodeDialog::new(
                                &app.dialog_state.step_up_code_text,
                                &app.dialog_state.step_up_expires_at,
                                app.dialog_state.step_up_focus,
                            );
                            frame.render_widget(bg_dialog, size);
                        }
                        _ => {}
                    }
                    let err = ErrorDialog::new(
                        &app.dialog_state.vault_error_title,
                        &app.dialog_state.vault_error_message,
                    );
                    frame.render_widget(err, size);
                }
                AppMode::VaultRecoveryRegenerateConfirmDialog => {
                    let confirm =
                        VaultRecoveryRegenerateConfirmDialog::new(app.dialog_state.recovery_regen_yes_selected);
                    frame.render_widget(confirm, size);
                }
                AppMode::VaultJoinPromptDialog => {
                    let dialog = VaultJoinPromptDialog::new(app.dialog_state.vault_join_prompt_send_selected);
                    frame.render_widget(dialog, size);
                }
                AppMode::ShareOfferDialog => {
                    if let Some(offer) = &app.dialog_state.share_offer {
                        let dialog = ShareOfferDialog::new(offer, app.dialog_state.share_offer_accept_selected);
                        frame.render_widget(dialog, size);
                    }
                }
                AppMode::VaultRevokedNotifyDialog => {
                    frame.render_widget(VaultRevokedNotifyDialog, size);
                }
                AppMode::VaultRecoveryInputDialog => {
                    let dialog = VaultRecoveryInputDialog::new(
                        &app.dialog_state.recovery_input_text,
                        app.dialog_state.recovery_input_focus,
                    );
                    frame.render_widget(dialog, size);
                }
                AppMode::StepUpCodeDialog => {
                    let dialog = StepUpCodeDialog::new(
                        &app.dialog_state.step_up_code_text,
                        &app.dialog_state.step_up_expires_at,
                        app.dialog_state.step_up_focus,
                    );
                    frame.render_widget(dialog, size);
                }
                AppMode::RecoveryWordsDisplayDialog => {
                    let words: &[String] = app.vault_snapshot.setup_recovery_words.as_deref().unwrap_or(&[]);
                    let dialog = RecoveryWordsDisplayDialog::new(words, app.dialog_state.recovery_display_focus);
                    frame.render_widget(dialog, size);
                }
                AppMode::SelectStorageDialog => {
                    let items: Vec<SelectorItem> = app
                        .selector_rows()
                        .iter()
                        .map(|row| match *row {
                            SelectorRow::Drive(i) => {
                                let d = &app.drives[i];
                                SelectorItem {
                                    hotkey: drive_hotkey(d),
                                    name: d.label.clone(),
                                    space: Some(match d.space {
                                        Some(sp) => (format_compact(sp.total), format_compact(sp.free)),
                                        None => (String::new(), String::new()),
                                    }),
                                    detail: None,
                                }
                            }
                            SelectorRow::Kind(kind) => SelectorItem {
                                hotkey: kind_hotkey(kind),
                                name: kind.label().to_string(),
                                space: None,
                                detail: None,
                            },
                            SelectorRow::Storage(i) => {
                                let s = &app.storages[i];
                                SelectorItem {
                                    hotkey: None,
                                    name: s.name.clone(),
                                    space: None,
                                    detail: s.link_url.clone(),
                                }
                            }
                        })
                        .collect();
                    let side = app.dialog_state.storage_target;
                    let title = match side {
                        PanelId::Left => "Left panel",
                        PanelId::Right => "Right panel",
                    };
                    let dialog = StorageListDialog {
                        title,
                        items: &items,
                        drives: app.drives.len(),
                        selected_index: app.dialog_state.selected_index,
                        side,
                    };
                    frame.render_widget(dialog, size);
                }
                AppMode::SkinSelectDialog => {
                    let dialog = SkinSelectDialog::new(
                        &app.dialog_state.skin_list,
                        app.dialog_state.skin_selected_index,
                        app.dialog_state.skin_scroll_offset,
                        app.dialog_state.skin_focus,
                    );
                    frame.render_widget(dialog, size);
                }
                _ => {}
            }
        })?;
        Ok(())
    }

    /// `true` when an event was handled.
    fn process_events(&mut self) -> bool {
        // Process only one event per call to allow UI updates between events
        let Some(event) = self.rx.try_recv().ok() else {
            return false;
        };
        self.on_event(event);
        true
    }

    fn on_event(&mut self, event: UiEvent) {
        match event {
            UiEvent::ListStorages(storages) => {
                self.app.storages = storages;
            }
            UiEvent::Drives(drives) => {
                self.app.drives = drives;
            }
            UiEvent::ConnectStorage {
                panel,
                name,
                storage_type,
                result,
            } => {
                let target_panel = self.app.panel_mut(panel);
                match result {
                    Ok(storage_id) => {
                        // A storage chosen: whatever listing was stashed is moot.
                        target_panel.stash = None;
                        target_panel.ensure_files().set_storage(FileStorage {
                            id: storage_id,
                            name,
                            kind: storage_type,
                        });
                        target_panel.loading = true;
                        target_panel.loading_started = Some(Instant::now());
                        self.app.mode = AppMode::Normal;
                        self.request_listing(panel);
                        // A new storage: the selector's rows.
                        self.req_list_storages();
                    }
                    Err(e) => {
                        target_panel.loading = false;
                        target_panel.error = Some(e);
                        // An archive that would not open: back to the
                        // listing it was entered from, cursor on it.
                        let back = target_panel.files_mut().and_then(|f| f.opened_from.take());
                        if let Some(OpenedFrom::Archive {
                            storage,
                            dir,
                            name,
                            from,
                        }) = back
                        {
                            self.show_parent_listing(panel, storage, dir, name, from);
                        }
                        self.app.mode = AppMode::Normal;
                    }
                }
            }
            UiEvent::ListDirBatch {
                panel: panel_id,
                seq,
                items,
            } => {
                let panel = self.app.panel_mut(panel_id);
                if panel.listing_seq != seq {
                    return; // a superseded listing's batch
                }
                let (visible, step, sort) = (panel.visible_height, panel.step(), panel.sort);
                // The content changed while the listing was in flight.
                let Some(files) = panel.files_mut() else { return };
                if files.replace_on_batch {
                    files.replace_on_batch = false;
                    files.entries = level_head(files);
                    // A new listing ends a quick search (MC stops it on
                    // chdir) and is a fresh view: the computed folder sizes go.
                    files.quick_search = None;
                    files.dir_sizes.clear();
                    if files.reset_selection_on_load {
                        files.cursor.reset();
                        files.reset_selection_on_load = false;
                    }
                }
                files.entries.extend(items);
                files.sort_entries(sort);
                let idx = files.cursor.index;
                files.cursor_set(idx, visible, step);
            }
            UiEvent::ListDirDone {
                panel: panel_id,
                seq,
                result,
            } => {
                let is_active_panel = self.app.active_panel_id() == panel_id;
                let panel = self.app.panel_mut(panel_id);
                if panel.listing_seq != seq {
                    return;
                }
                panel.listing = None;
                panel.loading = false;
                panel.loading_started = None;
                let (visible, step) = (panel.visible_height, panel.step());
                match result {
                    Ok(space) => {
                        panel.error = None;
                        let Some(files) = panel.files_mut() else { return };
                        files.space = space;
                        // A local directory listed: where the panel starts
                        // next time.
                        let local_dir = match (files.is_local(), files.storage_id()) {
                            (true, Some(root)) => Some(local_path::join(root, &files.current_path)),
                            _ => None,
                        };
                        if files.replace_on_batch {
                            // An empty level: no batch came to replace the list.
                            files.replace_on_batch = false;
                            files.entries = level_head(files);
                            files.quick_search = None;
                            files.dir_sizes.clear();
                            if files.reset_selection_on_load {
                                files.cursor.reset();
                                files.reset_selection_on_load = false;
                            }
                        }
                        files.previous_path = None;
                        // Restore focus if we have a pending focus name (from go_back)
                        if is_active_panel && let Some(focus_name) = self.pending_focus_name.take() {
                            files.restore_selection_or_find(&focus_name, visible, step);
                        }
                        let idx = files.cursor.index;
                        files.cursor_set(idx, visible, step);
                        if let Some(dir) = local_dir {
                            panel.last_local_dir = Some(dir);
                        }
                    }
                    Err(e) => {
                        let mut relist = false;
                        if let Some(files) = panel.files_mut() {
                            tracing::warn!(error = %e, path = %files.current_path, "Failed to list directory");
                            files.quick_search = None;
                            // Restore previous path so panel stays where it was;
                            // batches that already replaced its list mean it
                            // must be listed again.
                            relist = !files.replace_on_batch;
                            files.replace_on_batch = false;
                            if let Some(prev) = files.previous_path.take() {
                                files.current_path = prev;
                            }
                            files.reset_selection_on_load = false;
                        }
                        panel.error = None;
                        // Clear pending focus on error
                        if is_active_panel {
                            self.pending_focus_name = None;
                        }
                        // Show error dialog with Back button
                        self.app.dialog_state.listing_error_message = e;
                        self.app.dialog_state.listing_error_panel = Some(panel_id);
                        self.app.mode = AppMode::ListingErrorDialog;
                        if relist {
                            self.request_listing(panel_id);
                        }
                    }
                }
            }
            UiEvent::ListLinks {
                panel: panel_id,
                result,
            } => {
                let is_active_panel = self.app.active_panel_id() == panel_id;
                let panel = self.app.panel_mut(panel_id);
                panel.loading = false;
                panel.loading_started = None;
                let visible = panel.visible_height;
                match result {
                    Ok(rows) => {
                        panel.error = None;
                        let Content::Links(list) = &mut panel.content else {
                            return;
                        };
                        // Back from a share: the cursor on the link we came from.
                        let focus = if is_active_panel {
                            self.pending_focus_name.take()
                        } else {
                            None
                        };
                        let idx = focus
                            .and_then(|name| rows.iter().position(|r| r.name == name))
                            .unwrap_or(list.cursor.index);
                        list.rows = rows;
                        list.cursor.set(idx, list.rows.len(), visible, 1);
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "Failed to list links");
                        if is_active_panel {
                            self.pending_focus_name = None;
                        }
                        self.app.dialog_state.listing_error_message = e;
                        self.app.dialog_state.listing_error_panel = Some(panel_id);
                        self.app.mode = AppMode::ListingErrorDialog;
                    }
                }
            }
            UiEvent::Delete(result) => {
                self.app.mode = AppMode::Normal;
                match result {
                    Ok(()) => {
                        self.refresh_active_panel();
                    }
                    Err(e) => {
                        self.app.active_panel_mut().error = Some(e);
                    }
                }
            }
            UiEvent::DirSize {
                panel: panel_id,
                dir,
                key,
                result,
            } => {
                match result {
                    Ok(size) => {
                        // Still the same directory: the figure belongs to it.
                        if let Some(files) = self.app.panel_mut(panel_id).files_mut()
                            && files.current_path == dir
                        {
                            files.dir_sizes.insert(key, size);
                        }
                    }
                    Err(e) => {
                        self.app.panel_mut(panel_id).error = Some(e);
                    }
                }
            }
            UiEvent::FindPage { seq, result } => {
                let Some(find) = self.app.find.as_mut() else { return };
                if find.seq != seq {
                    return; // a superseded run's page
                }
                match result {
                    Ok(page) => {
                        for item in &page.items {
                            if is_dir(item) {
                                find.folders += 1;
                            } else {
                                find.files += 1;
                            }
                        }
                        find.items.extend(page.items);
                        if page.next_page_token.is_none() {
                            find.running = false;
                        }
                    }
                    Err(e) => {
                        find.error = Some(e);
                        find.running = false;
                    }
                }
            }
            UiEvent::Rename { result, to } => {
                self.app.mode = AppMode::Normal;
                match result {
                    Ok(()) => {
                        self.pending_focus_name = Some(to);
                        self.refresh_active_panel();
                    }
                    Err(e) => {
                        self.app.active_panel_mut().error = Some(e);
                    }
                }
            }
            UiEvent::Mkdir(result) => {
                self.app.mode = AppMode::Normal;
                match result {
                    Ok(()) => {
                        // Store the created directory name to focus it after refresh
                        let created_name = self.app.dialog_state.mkdir.text.clone();
                        if !created_name.is_empty() {
                            self.pending_focus_name = Some(created_name);
                        }
                        self.refresh_active_panel();
                    }
                    Err(e) => {
                        self.app.active_panel_mut().error = Some(e);
                    }
                }
            }
            UiEvent::StartTransfer(result) => match result {
                Ok(()) => {
                    self.app.begin_transfer();
                    self.app.mode = AppMode::Copying;
                }
                Err(e) => {
                    self.app.active_panel_mut().error = Some(e);
                    self.app.mode = AppMode::Normal;
                }
            },
            UiEvent::TransferProgress(progress) => {
                if self.app.transfer_active {
                    self.app.transfer_progress = progress;
                }
            }
            UiEvent::TransferFinished { state, progress, error } => {
                if !self.app.transfer_active {
                    return;
                }
                self.app.transfer_progress = progress;
                self.app.transfer_active = false;
                self.app.mode = AppMode::Normal;
                // A finished copy moved bytes: the quota standing changed.
                self.req_usage();
                match state {
                    TransferItemState::Finished => {
                        self.app.active_panel_mut().status_message = Some("Transfer complete".to_string());
                        // Refresh destination (inactive) panel
                        let dest = self.app.inactive_panel_id();
                        self.request_listing(dest);
                    }
                    TransferItemState::Cancelled => {
                        self.app.active_panel_mut().status_message = Some("Transfer cancelled".to_string());
                    }
                    _ => {
                        self.app.active_panel_mut().error = error.or(Some("Transfer failed".to_string()));
                    }
                }
            }
            UiEvent::TransferConflicts(conflicts) => {
                self.app.transfer_conflict_total = conflicts.len();
                self.app.transfer_conflicts = conflicts;
                self.app.pending_conflict_resolutions.clear();
                self.app.dialog_state.conflict_focus = 0;
                self.app.mode = AppMode::ConflictResolution;
            }
            UiEvent::TransferError { key, message } => {
                let msg = format!("{}: {}", key, message);
                tracing::info!("Transfer has error: {}", msg);
                self.app.dialog_state.transfer_error_message = msg;
                self.app.dialog_state.transfer_error_focus = 1; // default to Skip
                self.app.mode = AppMode::TransferErrorDialog;
            }
            UiEvent::CancelTransfer(result) => {
                if let Err(e) = result {
                    self.app.active_panel_mut().error = Some(e);
                }
                // The terminal `Cancelled` status follows as a progress event.
            }
            UiEvent::ViewerOpened { seq, result } => {
                if let Some(v) = self.app.viewer.as_mut()
                    && v.seq == seq
                {
                    match result {
                        Ok(size) => v.set_size(size),
                        Err(e) => {
                            tracing::error!(error = %e, path = %v.path, "viewer: open failed");
                            v.error = Some(e);
                        }
                    }
                }
            }
            UiEvent::ViewerBlock { seq, idx, result } => {
                if let Some(v) = self.app.viewer.as_mut()
                    && v.seq == seq
                {
                    if let Err(e) = &result {
                        tracing::error!(error = %e, path = %v.path, idx, "viewer: block read failed");
                    }
                    v.on_block(idx, result);
                }
            }
            UiEvent::ViewerSearch { seq, outcome } => {
                if let Some(v) = self.app.viewer.as_mut()
                    && v.seq == seq
                {
                    match outcome {
                        Ok(SearchOutcome::Progress(off)) => {
                            let pct = match v.size {
                                Some(s) if s > 0 => off.min(s) * 100 / s,
                                _ => 100,
                            };
                            v.status = Some(format!("Searching… {}%", pct));
                        }
                        Ok(SearchOutcome::Found(off)) => {
                            v.search_finished();
                            let len = v.needle.as_ref().map(|n| n.len()).unwrap_or(0);
                            v.on_found(off, len);
                            v.status = Some(format!("Found at {}", off));
                        }
                        Ok(SearchOutcome::NotFound) => {
                            v.search_finished();
                            v.status = Some("Not found".to_string());
                        }
                        Err(e) => {
                            tracing::error!(error = %e, path = %v.path, "viewer: search failed");
                            v.search_finished();
                            v.status = Some(format!("Search failed: {e}"));
                        }
                    }
                }
            }
            UiEvent::Usage(space) => {
                self.app.usage = space;
            }
            UiEvent::NearbyDevices { devices } => {
                self.app.nearby = devices;
                self.app.clamp_cursors();
            }
            UiEvent::NearbyOffers(views) => {
                self.app.offers = views.into_iter().map(|v| (v.agent_id.clone(), v)).collect();
            }
            UiEvent::LinkP2p { token, online } => {
                for panel in [&mut self.app.left_panel, &mut self.app.right_panel] {
                    if let Content::Links(list) = &mut panel.content {
                        for row in list.rows.iter_mut().filter(|r| r.token == token) {
                            row.p2p = online;
                        }
                    }
                }
            }
            UiEvent::NearbyOffer(result) => match result {
                Ok(()) => {
                    self.app.active_panel_mut().status_message =
                        Some("Offer sent — waiting for the device to accept".to_string());
                }
                Err(e) => {
                    tracing::error!(error = %e, "nearby offer failed");
                    self.show_vault_error("Nearby offer", e);
                }
            },
            UiEvent::NearbyOfferCancel(result) => {
                if let Err(e) = result {
                    self.show_vault_error("Nearby offer", e);
                }
            }
            UiEvent::ShareOffered(offer) => {
                tracing::info!(sender = %offer.sender_name, "nearby offer received");
                // A newer offer replaces the one shown (the engine keeps one);
                // behind any other dialog it waits for the screen.
                if matches!(self.app.mode, AppMode::Normal | AppMode::ShareOfferDialog) {
                    self.open_share_offer_dialog(offer);
                } else {
                    self.app.dialog_state.share_offer_pending = Some(offer);
                }
            }
            UiEvent::ShareOfferCancelled { offer_token } => {
                let ds = &mut self.app.dialog_state;
                if ds.share_offer_pending.as_ref().is_some_and(|o| o.offer_token == offer_token) {
                    ds.share_offer_pending = None;
                }
                if self.app.mode == AppMode::ShareOfferDialog
                    && ds.share_offer.as_ref().is_some_and(|o| o.offer_token == offer_token)
                {
                    ds.share_offer = None;
                    self.app.mode = AppMode::Normal;
                    self.app.active_panel_mut().status_message = Some("The nearby offer was withdrawn".to_string());
                }
            }
            UiEvent::ShareOfferAccepted(result) => match result {
                Ok((token, name)) => {
                    // The share is registered; the bare token puts it in the
                    // other panel through the ordinary connect path. The
                    // panel's path is its own (`set_storage` keeps it), so it
                    // starts at the share's root like the selector does.
                    let panel_id = self.app.inactive_panel_id();
                    let panel = self.app.panel_mut(panel_id);
                    panel.stash = None;
                    panel.error = None;
                    panel.status_message = None;
                    let files = panel.ensure_files();
                    files.current_path = String::new();
                    files.opened_from = None;
                    files.marked.clear();
                    self.request_connect_storage(panel_id, name.clone(), StorageConnectRequest::Ninja { link_url: token });
                    self.app.active_panel_mut().status_message =
                        Some(format!("{name} opened in the other panel — F5 copies it"));
                }
                Err(e) => {
                    tracing::error!(error = %e, "nearby offer accept failed");
                    self.show_vault_error("Nearby offer", e);
                }
            },
            UiEvent::ShareOfferRejected(result) => {
                if let Err(e) = result {
                    self.show_vault_error("Nearby offer", e);
                }
            }
            UiEvent::VaultDevices(result) => {
                self.app.dialog_state.vault_devices_loading = false;
                match result {
                    Ok(devices) => {
                        tracing::info!(count = devices.len(), "VaultDevices: OK");
                        self.app.dialog_state.vault_devices = devices;
                        self.app.dialog_state.vault_devices_error = None;
                        let max = self.app.dialog_state.vault_devices.len().saturating_sub(1);
                        if self.app.dialog_state.vault_devices_selected > max {
                            self.app.dialog_state.vault_devices_selected = max;
                        }
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "VaultDevices: failed");
                        self.app.dialog_state.vault_devices_error = Some(e);
                    }
                }
            }
            UiEvent::VaultApproveResult(result) => match result {
                Ok(()) => {
                    tracing::info!("Vault join request approved");
                    self.app.dialog_state.vault_join_selected = 0;
                    self.app.dialog_state.vault_join_error = None;
                    if self.app.mode == AppMode::VaultTrustCircleDialog {
                        self.fetch_vault_devices();
                    }
                }
                Err(e) => {
                    tracing::error!(error = %e, "Vault approve failed");
                    self.show_vault_error("Approve failed", e);
                }
            },
            UiEvent::VaultRejectResult(result) => match result {
                Ok(()) => {
                    tracing::info!("Vault join request rejected");
                    self.app.dialog_state.vault_join_selected = 0;
                    self.app.dialog_state.vault_join_error = None;
                }
                Err(e) => {
                    tracing::error!(error = %e, "Vault reject failed");
                    self.show_vault_error("Decline failed", e);
                }
            },
            UiEvent::VaultRecoveryRegenerateResult(result) => match result {
                Ok(words) => {
                    tracing::info!(count = words.len(), "Vault recovery words regenerated");
                    if self.app.mode == AppMode::Normal || self.app.mode == AppMode::VaultTrustCircleDialog {
                        self.app.dialog_state.recovery_display_focus = 0;
                        self.app.mode = AppMode::RecoveryWordsDisplayDialog;
                    }
                }
                Err(e) => {
                    tracing::error!(error = %e, "Vault recovery regenerate failed");
                    self.show_vault_error("Regenerate failed", e);
                }
            },
            UiEvent::VaultRecoverResult(result) => match result {
                Ok(StepUpOutcome::Completed) => {
                    tracing::info!("Vault recovered");
                    self.app.dialog_state.recovery_input_text.clear();
                    self.app.active_panel_mut().status_message =
                        Some("Vault recovered — identity key unlocked.".to_string());
                }
                Ok(StepUpOutcome::ChallengeRequired { expires_at }) => {
                    tracing::info!("Vault recover: step-up code required");
                    self.open_step_up_dialog(expires_at);
                }
                Err(e) => {
                    tracing::error!(error = %e, "Vault recover failed");
                    self.show_vault_error("Recovery failed", e);
                }
            },
            UiEvent::VaultStepUpVerifyResult(result) => {
                match result {
                    Ok(StepUpOutcome::Completed) => {
                        tracing::info!("Vault recovered (step-up verified)");
                        self.app.dialog_state.step_up_code_text.clear();
                        self.app.dialog_state.recovery_input_text.clear();
                        self.app.active_panel_mut().status_message =
                            Some("Vault recovered — identity key unlocked.".to_string());
                        self.app.mode = AppMode::Normal;
                    }
                    // The old code expired; the backend sent a fresh one.
                    Ok(StepUpOutcome::ChallengeRequired { expires_at }) => {
                        tracing::info!("Vault recover: new step-up code sent");
                        self.open_step_up_dialog(expires_at);
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "Step-up verify failed");
                        // The recover stays parked — back to the code dialog.
                        self.app.mode = AppMode::StepUpCodeDialog;
                        self.show_vault_error("Verification failed", e);
                    }
                }
            }
            UiEvent::VaultJoinStartResult(result) => match result {
                Ok(()) => tracing::info!("Vault join request sent"),
                Err(e) => {
                    tracing::error!(error = %e, "Vault join start failed");
                    self.show_vault_error("Send join request failed", e);
                }
            },
            UiEvent::VaultRevokeResult(result) => match result {
                Ok(()) => {
                    tracing::info!("Vault device revoked");
                    self.app.dialog_state.vault_devices_error = None;
                    self.fetch_vault_devices();
                }
                Err(e) => {
                    tracing::error!(error = %e, "Vault revoke failed");
                    self.show_vault_error("Revoke failed", e);
                }
            },
            UiEvent::LoginComplete(result) => match result {
                Ok(()) => {
                    tracing::info!("Login: completed successfully");
                    self.app.active_panel_mut().status_message = Some("Login successful".to_string());
                }
                Err(e) => {
                    tracing::error!(error = %e, "Login: failed");
                    self.app.active_panel_mut().error = Some(format!("Login failed: {}", e));
                }
            },
            UiEvent::LogoutResult(result) => match result {
                Ok(()) => {
                    tracing::info!("Logout: completed successfully");
                    self.app.active_panel_mut().status_message = Some("Signed out".to_string());
                }
                Err(e) => {
                    tracing::error!(error = %e, "Logout: failed");
                    self.app.active_panel_mut().error = Some(format!("Sign-out failed: {}", e));
                }
            },
            UiEvent::BackendStatus(status) => {
                tracing::info!(
                    state = ?status.connection.state,
                    agent_id = %status.connection.agent_id,
                    "backend status"
                );
                let prev_online = self.is_backend_online();
                let is_online = status.connection.is_online();
                self.app.backend_status = Some(status);
                // Backend just transitioned offline → online: drain any
                // storage connects we deferred at startup (e.g. ninja
                // shares passed on the command line).
                if !prev_online && is_online {
                    self.drain_pending_storage_connects();
                }
            }
            UiEvent::VaultSnapshot(snapshot) => {
                let prev_status = self.app.vault_snapshot.status.clone();
                let not_joined_arrived =
                    snapshot.status.as_deref() == Some("not_joined") && prev_status.as_deref() != Some("not_joined");
                self.app.dialog_state.vault_join_requests = snapshot.pending_join_requests.clone();
                let max = self.app.dialog_state.vault_join_requests.len().saturating_sub(1);
                if self.app.dialog_state.vault_join_selected > max {
                    self.app.dialog_state.vault_join_selected = max;
                }
                self.app.vault_snapshot = snapshot;
                if not_joined_arrived && self.app.mode == AppMode::Normal {
                    tracing::info!("Vault not_joined arrived; prompting for join request");
                    self.app.dialog_state.vault_join_prompt_send_selected = false; // default focus on Cancel
                    self.app.mode = AppMode::VaultJoinPromptDialog;
                }
            }
            UiEvent::VaultJoinRequestArrived(req) => {
                tracing::info!(agent_id = %req.agent_id, device_name = %req.device_name, "vault join request");
                let device = if req.device_name.is_empty() {
                    "Unknown device".to_string()
                } else {
                    req.device_name.clone()
                };
                let location = match (req.country.as_deref(), req.ip.as_deref()) {
                    (Some(c), Some(p)) if !c.is_empty() && !p.is_empty() => format!(" from {} ({})", c, p),
                    (Some(c), _) if !c.is_empty() => format!(" from {}", c),
                    (_, Some(p)) if !p.is_empty() => format!(" from {}", p),
                    _ => String::new(),
                };
                let msg = format!(
                    "Device '{}'{} requests to join a trusted circle of your vault.",
                    device, location
                );
                // Surface a notification dialog if the user is idle; queue it
                // behind an open notification; the join-requests dialog
                // shows the list itself.
                match self.app.mode {
                    AppMode::Normal => {
                        self.app.dialog_state.vault_join_message = msg;
                        self.app.dialog_state.vault_join_control_selected = true;
                        self.app.mode = AppMode::VaultJoinNotifyDialog;
                    }
                    AppMode::VaultJoinNotifyDialog => {
                        self.app.dialog_state.vault_join_queue.push_back(msg);
                    }
                    _ => {}
                }
            }
            UiEvent::VaultDeviceApproved(result) => match result {
                Ok(()) => {
                    tracing::info!("vault: this device was approved");
                    self.app.active_panel_mut().status_message =
                        Some("Approved — vault unlocked on this device.".to_string());
                }
                Err(e) => {
                    tracing::error!(error = %e, "vault: handle_device_approved failed");
                    self.show_vault_error("Approval failed", e);
                }
            },
            UiEvent::VaultDeviceRejected { reason } => {
                tracing::info!(?reason, "vault: join request declined");
                let msg = match reason {
                    Some(r) if !r.is_empty() => format!("A trusted device declined this device's join request: {}", r),
                    _ => "A trusted device declined this device's join request.".to_string(),
                };
                self.show_vault_error("Join request declined", msg);
            }
            UiEvent::VaultRevoked => {
                tracing::info!("vault: this device was revoked");
                if self.app.mode == AppMode::Normal {
                    self.app.mode = AppMode::VaultRevokedNotifyDialog;
                }
            }
            UiEvent::VaultBindResult(result) => {
                if let Err(e) = result {
                    tracing::warn!(error = %e, "vault bind failed");
                    self.show_vault_error("Vault", e);
                    return;
                }
                // Surface freshly-set recovery words from a successful setup.
                let words_appeared = self
                    .app
                    .vault_snapshot
                    .setup_recovery_words
                    .as_ref()
                    .map(|w| !w.is_empty())
                    .unwrap_or(false);
                if words_appeared && self.app.mode == AppMode::Normal {
                    self.app.dialog_state.recovery_display_focus = 0;
                    self.app.mode = AppMode::RecoveryWordsDisplayDialog;
                }
            }
        }
    }

    fn open_step_up_dialog(&mut self, expires_at: String) {
        self.app.dialog_state.step_up_code_text.clear();
        self.app.dialog_state.step_up_focus = 0;
        self.app.dialog_state.step_up_expires_at = expires_at;
        self.app.mode = AppMode::StepUpCodeDialog;
    }

    fn handle_key(&mut self, key: KeyEvent) {
        // Global quit — except in the editor, where Ctrl+C is copy.
        let in_editor = matches!(
            self.app.mode,
            AppMode::Editor
                | AppMode::EditorSearchDialog
                | AppMode::EditorGotoDialog
                | AppMode::EditorCloseConfirmDialog
                | AppMode::EditorMenu
        );
        if !in_editor && key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL) {
            self.app.should_quit = true;
            return;
        }

        match self.app.mode {
            AppMode::Normal => self.handle_normal_key(key),
            AppMode::HelpDialog => self.handle_help_key(key),
            AppMode::AddStorageDialog => self.handle_add_storage_key(key),
            AppMode::SelectStorageDialog => self.handle_select_storage_key(key),
            AppMode::CopyConfirmDialog => self.handle_copy_confirm_key(key),
            AppMode::DeleteConfirmDialog => self.handle_delete_confirm_key(key),
            AppMode::MkdirDialog => self.handle_mkdir_key(key),
            AppMode::Copying => self.handle_copying_key(key),
            AppMode::ConflictResolution => self.handle_conflict_key(key),
            AppMode::TransferErrorDialog => self.handle_transfer_error_key(key),
            AppMode::ListingErrorDialog => self.handle_listing_error_key(key),
            AppMode::SkinSelectDialog => self.handle_skin_select_key(key),
            AppMode::VaultJoinNotifyDialog => self.handle_vault_join_notify_key(key),
            AppMode::VaultTrustCircleDialog => self.handle_vault_trust_circle_key(key),
            AppMode::VaultJoinRequestsDialog => self.handle_vault_join_requests_key(key),
            AppMode::VaultRevokeConfirmDialog => self.handle_vault_revoke_confirm_key(key),
            AppMode::VaultErrorDialog => self.handle_vault_error_dialog_key(key),
            AppMode::VaultRecoveryRegenerateConfirmDialog => self.handle_vault_recovery_regenerate_key(key),
            AppMode::RecoveryWordsDisplayDialog => self.handle_recovery_words_display_key(key),
            AppMode::VaultJoinPromptDialog => self.handle_vault_join_prompt_key(key),
            AppMode::ShareOfferDialog => self.handle_share_offer_key(key),
            AppMode::VaultRevokedNotifyDialog => self.handle_vault_revoked_notify_key(key),
            AppMode::VaultRecoveryInputDialog => self.handle_vault_recovery_input_key(key),
            AppMode::StepUpCodeDialog => self.handle_step_up_code_key(key),
            AppMode::RenameDialog => self.handle_rename_key(key),
            AppMode::FindDialog => self.handle_find_dialog_key(key),
            AppMode::FindResults => self.handle_find_results_key(key),
            AppMode::Menu | AppMode::EditorMenu => self.handle_menu_key(key),
            AppMode::Viewer => self.handle_viewer_key(key),
            AppMode::ViewerGotoDialog | AppMode::ViewerSearchDialog => self.handle_viewer_input_key(key),
            AppMode::Player => self.handle_player_key(key),
            AppMode::Tetris => self.handle_tetris_key(key),
            AppMode::Editor => self.handle_editor_key(key),
            AppMode::EditorSearchDialog | AppMode::EditorGotoDialog => self.handle_editor_input_key(key),
            AppMode::EditorCloseConfirmDialog => self.handle_editor_close_confirm_key(key),
            AppMode::Deleting => {}
        }
    }

    fn handle_normal_key(&mut self, key: KeyEvent) {
        if self.app.active_files().is_some_and(|f| f.quick_search.is_some()) && self.handle_quick_search_key(key) {
            return;
        }
        match key.code {
            KeyCode::Esc => {
                if !self.app.command.text.is_empty() {
                    self.app.command_clear();
                    return;
                }
                let panel = self.app.active_panel_mut();
                panel.status_message = None;
                panel.error = None;
            }
            KeyCode::Char('q') if key.modifiers.contains(KeyModifiers::ALT) => {
                tracing::info!("Action: Quit");
                self.app.should_quit = true;
            }
            KeyCode::F(10) => {
                tracing::info!("Action: Quit");
                self.app.should_quit = true;
            }
            KeyCode::Tab => {
                self.app.toggle_panel();
            }
            KeyCode::Char('k') if key.modifiers.contains(KeyModifiers::ALT) => {
                self.app.move_active_cursor(-1);
            }
            KeyCode::Char('j') if key.modifiers.contains(KeyModifiers::ALT) => {
                self.app.move_active_cursor(1);
            }
            KeyCode::Up => {
                self.app.move_active_cursor(-1);
            }
            KeyCode::Down => {
                self.app.move_active_cursor(1);
            }
            KeyCode::PageUp => {
                let page_size = self.app.active_panel().visible_height.max(1) as i32;
                self.app.move_active_cursor(-page_size);
            }
            KeyCode::PageDown => {
                let page_size = self.app.active_panel().visible_height.max(1) as i32;
                self.app.move_active_cursor(page_size);
            }
            KeyCode::Home => {
                if !self.app.command.text.is_empty() {
                    self.app.command.cursor = 0;
                    return;
                }
                self.app.set_active_cursor(0);
            }
            KeyCode::End => {
                if !self.app.command.text.is_empty() {
                    self.app.command.cursor = self.app.command.text.chars().count();
                    return;
                }
                self.app.set_active_cursor(usize::MAX);
            }
            // In a multi-column view ←/→ step a column (Backspace / Enter keep
            // their directory meaning); ← on the first entry (`..`) goes up
            // like anywhere else.
            KeyCode::Left | KeyCode::Right
                if self.app.command.text.is_empty()
                    && self.active_columns() > 1
                    && !(key.code == KeyCode::Left && self.app.active_files().is_some_and(|f| f.cursor.index == 0)) =>
            {
                let id = self.app.active_panel_id();
                let step = self.app.active_panel().visible_height / self.active_columns();
                let delta = step.max(1) as i32;
                self.app
                    .move_cursor(id, if key.code == KeyCode::Right { delta } else { -delta });
            }
            KeyCode::Enter | KeyCode::Right => {
                if !self.app.command.text.is_empty() {
                    if key.code == KeyCode::Enter {
                        self.run_command_line();
                    } else if self.app.command.cursor < self.app.command.text.chars().count() {
                        self.app.command.cursor += 1;
                    }
                    return;
                }
                if let Some(entry) = self.app.active_files().and_then(|f| f.selected_entry()) {
                    tracing::info!("Action: Enter [{}]", entry.key);
                }
                self.enter_selected();
            }
            KeyCode::Backspace | KeyCode::Left => {
                if !self.app.command.text.is_empty() {
                    if key.code == KeyCode::Backspace {
                        self.app.command_history_index = None;
                        self.app.command.backspace();
                    } else if self.app.command.cursor > 0 {
                        self.app.command.cursor -= 1;
                    }
                    return;
                }
                tracing::info!("Action: Back");
                self.go_back();
            }
            // FAR's drive menu: Alt+F1 for the left panel, Alt+F2 for the
            // right, whichever is active. Alt+1 / Alt+2 do the same: a
            // terminal with Option-as-Meta (Terminal.app) sends ESC+digit,
            // which decodes reliably, while Alt+F-keys often never arrive.
            KeyCode::F(1) | KeyCode::Char('1') if key.modifiers.contains(KeyModifiers::ALT) => {
                tracing::info!("Action: Open storage selector (left)");
                self.open_storage_selector(PanelId::Left);
            }
            KeyCode::F(2) | KeyCode::Char('2') if key.modifiers.contains(KeyModifiers::ALT) => {
                tracing::info!("Action: Open storage selector (right)");
                self.open_storage_selector(PanelId::Right);
            }
            KeyCode::F(1) => {
                tracing::info!("Action: Show Help dialog");
                self.app.mode = AppMode::HelpDialog;
            }
            KeyCode::F(3) => {
                self.open_viewer_selected();
            }
            KeyCode::F(4) => {
                self.open_editor_selected();
            }
            KeyCode::F(5) => {
                self.prepare_copy();
            }
            KeyCode::F(6) => {
                self.prepare_rename();
            }
            KeyCode::F(9) => {
                tracing::info!("Action: Menu");
                self.app.dialog_state.menu_kind = MenuBarKind::Panels;
                self.app.dialog_state.menu_open = match self.app.active_panel_id() {
                    PanelId::Left => 0,
                    PanelId::Right => 2,
                };
                self.app.dialog_state.menu_item = 0;
                self.app.mode = AppMode::Menu;
            }
            KeyCode::F(7) | KeyCode::Char('7') if key.modifiers.contains(KeyModifiers::ALT) => {
                self.open_find_dialog();
            }
            KeyCode::F(7) => {
                tracing::info!("Action: Mkdir");
                self.app.dialog_state.mkdir.clear();
                self.app.dialog_state.mkdir_focus = 0;
                self.app.mode = AppMode::MkdirDialog;
            }
            KeyCode::Char('m') if key.modifiers.contains(KeyModifiers::ALT) => {
                tracing::info!("Action: Mkdir");
                self.app.dialog_state.mkdir.clear();
                self.app.dialog_state.mkdir_focus = 0;
                self.app.mode = AppMode::MkdirDialog;
            }
            KeyCode::F(8) => {
                self.prepare_delete();
            }
            KeyCode::Delete => {
                if !self.app.command.text.is_empty() {
                    self.app.command_history_index = None;
                    self.app.command.delete();
                    return;
                }
                self.prepare_delete();
            }
            KeyCode::Char('a') if key.modifiers.contains(KeyModifiers::ALT) => {
                tracing::info!("Action: Open add storage dialog");
                self.app.dialog_state.storage_target = self.app.active_panel_id();
                self.app.dialog_state.input_name.clear();
                self.app.dialog_state.input_url.clear();
                self.app.dialog_state.active_field = 0;
                self.app.mode = AppMode::AddStorageDialog;
            }
            KeyCode::Char('r') if key.modifiers.contains(KeyModifiers::ALT) => {
                tracing::info!("Action: Refresh");
                self.refresh_active_panel();
            }
            KeyCode::Char('i') if key.modifiers.contains(KeyModifiers::ALT) => {
                self.toggle_content(ContentKind::Info);
            }
            KeyCode::Char('l') if key.modifiers.contains(KeyModifiers::ALT) => {
                self.toggle_content(ContentKind::Log);
            }
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::ALT) => {
                self.toggle_content(ContentKind::Connection);
            }
            KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::ALT) => {
                self.toggle_content(ContentKind::User);
            }
            KeyCode::Char('V') if key.modifiers.contains(KeyModifiers::ALT) => {
                tracing::info!("Action: Open Vault Trust Circle dialog");
                self.open_vault_trust_circle_dialog();
            }
            KeyCode::Char('J') if key.modifiers.contains(KeyModifiers::ALT) => {
                tracing::info!("Action: Open Vault join requests dialog");
                self.open_vault_join_requests_dialog();
            }
            KeyCode::Char('R') if key.modifiers.contains(KeyModifiers::ALT) => {
                // Recovery is for users who don't have an unlocked, trusted
                // device — the canonical case is "lost all devices, restoring
                // from BIP39 words on a fresh machine" (vault_status =
                // not_joined / pending_approval). It also covers the rare
                // "locked" case (active member but identity key not in
                // memory). Otherwise (active + unlocked), Shift+R rotates the
                // recovery key via the regenerate dialog.
                let needs_recovery = {
                    let status = self.app.vault_snapshot.status.as_deref();
                    matches!(status, Some("not_joined") | Some("pending_approval"))
                        || (status == Some("active") && !self.app.vault_snapshot.identity_loaded)
                };
                if needs_recovery {
                    tracing::info!("Action: Open Vault recovery input dialog");
                    self.app.dialog_state.recovery_input_text.clear();
                    self.app.dialog_state.recovery_input_focus = 0;
                    self.app.mode = AppMode::VaultRecoveryInputDialog;
                } else {
                    tracing::info!("Action: Open Vault recovery regenerate dialog");
                    self.app.dialog_state.recovery_regen_yes_selected = false;
                    self.app.mode = AppMode::VaultRecoveryRegenerateConfirmDialog;
                }
            }
            KeyCode::Char('W') if key.modifiers.contains(KeyModifiers::ALT) => {
                let has_words = self
                    .app
                    .vault_snapshot
                    .setup_recovery_words
                    .as_ref()
                    .map(|w| !w.is_empty())
                    .unwrap_or(false);
                if has_words {
                    tracing::info!("Action: Show Vault recovery words");
                    self.app.dialog_state.recovery_display_focus = 0;
                    self.app.mode = AppMode::RecoveryWordsDisplayDialog;
                } else {
                    tracing::info!("Action: Show Vault recovery words: none pending");
                }
            }
            KeyCode::Char('L') if key.modifiers.contains(KeyModifiers::ALT) => {
                if let Some(path) = self.app.log_path.clone() {
                    tracing::info!("Action: View log file [{}]", path);
                    // On the local storage of its volume (the home volume's
                    // is open from the start).
                    let (root, key) = local_path::split(std::path::Path::new(&path));
                    self.open_viewer(root, key, true, path);
                } else {
                    tracing::warn!("Action: View log file - no log path configured");
                    self.app.active_panel_mut().status_message = Some("No log file (use --log-path)".to_string());
                }
            }
            KeyCode::Char('t') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                tracing::info!("Action: Toggle selection");
                let (visible, step) = (self.app.active_panel().visible_height, self.app.active_panel().step());
                if let Some(files) = self.app.active_files_mut() {
                    files.toggle_mark(visible, step);
                }
            }
            KeyCode::Char('s') if key.modifiers.intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) => {
                tracing::info!("Action: Quick search");
                if let Some(files) = self.app.active_files_mut() {
                    files.quick_search = Some(String::new());
                }
            }
            KeyCode::Char('S') if key.modifiers.contains(KeyModifiers::ALT) => {
                tracing::info!("Action: Open skin selector");
                self.open_skin_selector();
            }
            KeyCode::Char('N') if key.modifiers.contains(KeyModifiers::ALT) => {
                self.toggle_content(ContentKind::Nearby);
            }
            KeyCode::Char('T') if key.modifiers.contains(KeyModifiers::ALT) => {
                self.open_tetris();
            }
            KeyCode::Char('o') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.open_subshell();
            }
            KeyCode::Char('p') if key.modifiers.contains(KeyModifiers::ALT) => {
                self.command_history_prev();
            }
            KeyCode::Char('n') if key.modifiers.contains(KeyModifiers::ALT) => {
                self.command_history_next();
            }
            // Everything printable types into the command line.
            KeyCode::Char(c) if !key.modifiers.intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) => {
                self.app.command_history_index = None;
                self.app.command.insert(c);
            }
            _ => {}
        }
    }

    /// Name columns of the active panel's file list (1 for anything else).
    fn active_columns(&self) -> usize {
        self.app.active_panel().columns()
    }

    /// MC quick search on the active panel: printable keys extend the prefix
    /// (one nothing matches is dropped), Backspace shortens it, Ctrl+S /
    /// Alt+S jumps to the next match, Esc ends it. Anything else ends it and
    /// is handled as usual — `false` hands the key back. Matching starts at
    /// the cursor and wraps round the list.
    fn handle_quick_search_key(&mut self, key: KeyEvent) -> bool {
        let ctrl_alt = key.modifiers.intersects(KeyModifiers::CONTROL | KeyModifiers::ALT);
        let (visible, step) = (self.app.active_panel().visible_height, self.app.active_panel().step());
        let Some(files) = self.app.active_files_mut() else {
            return false;
        };
        match key.code {
            KeyCode::Esc => {
                files.quick_search = None;
                true
            }
            KeyCode::Char('s') if ctrl_alt => {
                let text = files.quick_search.clone().unwrap_or_default();
                if !text.is_empty()
                    && let Some(i) = files.find_prefix(&text, files.cursor.index, true)
                {
                    files.cursor_set(i, visible, step);
                }
                true
            }
            KeyCode::Backspace => {
                if let Some(q) = files.quick_search.as_mut() {
                    q.pop();
                }
                true
            }
            KeyCode::Char(c) if !ctrl_alt => {
                let Some(q) = files.quick_search.as_mut() else {
                    return true;
                };
                q.push(c);
                let text = q.clone();
                match files.find_prefix(&text, files.cursor.index, false) {
                    Some(i) => files.cursor_set(i, visible, step),
                    None => {
                        if let Some(q) = files.quick_search.as_mut() {
                            q.pop();
                        }
                    }
                }
                true
            }
            _ => {
                files.quick_search = None;
                false
            }
        }
    }

    fn handle_help_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Esc | KeyCode::Enter | KeyCode::F(1) => {
                tracing::info!("Action: Help dialog: Close");
                // Back to the viewer when help was opened from it.
                self.app.mode = if self.app.viewer.is_some() {
                    AppMode::Viewer
                } else {
                    AppMode::Normal
                };
            }
            _ => {}
        }
    }

    fn open_skin_selector(&mut self) {
        self.app.dialog_state.skin_list = skin::available_skins();
        // Pre-select the currently active skin
        let current = skin::current_skin_name();
        self.app.dialog_state.skin_selected_index = self
            .app
            .dialog_state
            .skin_list
            .iter()
            .position(|(name, _)| *name == current)
            .unwrap_or(0);
        self.app.dialog_state.skin_scroll_offset = 0;
        self.app.dialog_state.skin_focus = 0;
        self.app.mode = AppMode::SkinSelectDialog;
    }

    fn handle_skin_select_key(&mut self, key: KeyEvent) {
        let list_len = self.app.dialog_state.skin_list.len();
        let visible_rows: usize = 12; // must match max_list_height in dialog

        match key.code {
            KeyCode::Esc => {
                tracing::info!("Action: Skin select dialog: Cancel");
                self.app.mode = AppMode::Normal;
            }
            KeyCode::Up => {
                if self.app.dialog_state.skin_focus > 0 {
                    // From buttons back to list
                    self.app.dialog_state.skin_focus = 0;
                } else if self.app.dialog_state.skin_selected_index > 0 {
                    self.app.dialog_state.skin_selected_index -= 1;
                    if self.app.dialog_state.skin_selected_index < self.app.dialog_state.skin_scroll_offset {
                        self.app.dialog_state.skin_scroll_offset = self.app.dialog_state.skin_selected_index;
                    }
                }
            }
            KeyCode::Down => {
                if self.app.dialog_state.skin_focus > 0 {
                    // Already at buttons, do nothing
                } else if self.app.dialog_state.skin_selected_index + 1 < list_len {
                    self.app.dialog_state.skin_selected_index += 1;
                    if self.app.dialog_state.skin_selected_index
                        >= self.app.dialog_state.skin_scroll_offset + visible_rows
                    {
                        self.app.dialog_state.skin_scroll_offset =
                            self.app.dialog_state.skin_selected_index + 1 - visible_rows;
                    }
                }
            }
            KeyCode::Tab => {
                // Cycle: list(0) -> OK(1) -> Cancel(2) -> list(0)
                self.app.dialog_state.skin_focus = (self.app.dialog_state.skin_focus + 1) % 3;
            }
            KeyCode::Left | KeyCode::Right => {
                // Toggle between OK(1) and Cancel(2) when on buttons
                if self.app.dialog_state.skin_focus == 1 {
                    self.app.dialog_state.skin_focus = 2;
                } else if self.app.dialog_state.skin_focus == 2 {
                    self.app.dialog_state.skin_focus = 1;
                }
            }
            KeyCode::Enter => {
                match self.app.dialog_state.skin_focus {
                    2 => {
                        tracing::info!("Skin Select Dialog action: Cancel (button)");
                        self.app.mode = AppMode::Normal;
                    }
                    _ => {
                        // List (0) or OK (1): apply selected skin
                        self.apply_selected_skin();
                    }
                }
            }
            _ => {}
        }
    }

    fn apply_selected_skin(&mut self) {
        let idx = self.app.dialog_state.skin_selected_index;
        if let Some((name, path)) = self.app.dialog_state.skin_list.get(idx) {
            match path {
                None => {
                    tracing::info!("Applying default skin");
                    skin::load_default_skin();
                }
                Some(p) => {
                    tracing::info!("Applying skin: {} from {:?}", name, p);
                    if let Err(e) = skin::load_skin_from_file(p) {
                        tracing::error!("Failed to load skin: {}", e);
                        self.app.active_panel_mut().error = Some(e);
                    }
                }
            }
        }
        self.save_state();
        self.app.mode = AppMode::Normal;
    }

    /// The state between runs, written now (a view / sort / skin change,
    /// the quit); a failure is logged, the session goes on.
    fn save_state(&self) {
        let Some(path) = &self.app.state_path else {
            return;
        };
        if let Err(e) = crate::state::State::of(&self.app).save(path) {
            tracing::warn!(path = %path.display(), "state: {e}");
        }
    }

    fn open_storage_selector(&mut self, target: PanelId) {
        // Volumes come and go: refresh their list and space on every open.
        self.req_drives();
        self.app.dialog_state.storage_target = target;
        // The cursor starts on what the panel shows now.
        self.app.dialog_state.selected_index = self.app.selector_index(target);
        self.app.mode = AppMode::SelectStorageDialog;
    }

    fn handle_select_storage_key(&mut self, key: KeyEvent) {
        let rows = self.app.selector_rows();
        match key.code {
            KeyCode::Esc => {
                tracing::info!("Action: Select storage dialog: Cancel");
                self.app.mode = AppMode::Normal;
            }
            KeyCode::Up => {
                if self.app.dialog_state.selected_index > 0 {
                    self.app.dialog_state.selected_index -= 1;
                }
            }
            KeyCode::Down => {
                if self.app.dialog_state.selected_index + 1 < rows.len() {
                    self.app.dialog_state.selected_index += 1;
                }
            }
            KeyCode::Enter => {
                tracing::info!("Action: Select storage dialog: Select");
                self.select_storage();
            }
            KeyCode::Char('a') | KeyCode::Char('+') => {
                tracing::info!("Action: Select storage dialog: Add Storage");
                // Switch to add storage dialog
                self.app.dialog_state.input_name.clear();
                self.app.dialog_state.input_url.clear();
                self.app.dialog_state.active_field = 0;
                self.app.mode = AppMode::AddStorageDialog;
            }
            // Hotkeys: a content's digit, a drive's letter.
            KeyCode::Char(c) => {
                let hit = rows.iter().position(|row| match *row {
                    SelectorRow::Kind(kind) => kind_hotkey(kind) == Some(c),
                    SelectorRow::Drive(i) => {
                        drive_hotkey(&self.app.drives[i]).is_some_and(|h| h.eq_ignore_ascii_case(&c))
                    }
                    SelectorRow::Storage(_) => false,
                });
                if let Some(idx) = hit {
                    self.app.dialog_state.selected_index = idx;
                    tracing::info!("Action: Select storage dialog: hotkey {}", c);
                    self.select_storage();
                }
            }
            _ => {}
        }
    }

    /// Open the selector's chosen row in its panel: a volume connects a
    /// local storage rooted there (the root one opens at home), a share is
    /// switched to, a content kind is shown.
    fn select_storage(&mut self) {
        let idx = self.app.dialog_state.selected_index;
        let panel_id = self.app.dialog_state.storage_target;
        self.app.mode = AppMode::Normal;
        let Some(row) = self.app.selector_rows().get(idx).copied() else {
            return;
        };
        match row {
            SelectorRow::Drive(i) => {
                let drive = self.app.drives[i].clone();
                // The home volume opens at home, any other at its root.
                let (home_root, home_key) = local_path::home_volume();
                let start = if drive.root == home_root {
                    home_key
                } else {
                    String::new()
                };
                let panel = self.app.panel_mut(panel_id);
                panel.stash = None;
                panel.error = None;
                panel.status_message = None;
                let files = panel.ensure_files();
                files.entries.clear();
                files.cursor.reset();
                files.current_path = start;
                files.opened_from = None;
                files.marked.clear();
                self.request_connect_storage(
                    panel_id,
                    drive.label,
                    StorageConnectRequest::Local { root_path: drive.root },
                );
            }
            SelectorRow::Storage(i) => {
                let storage = &self.app.storages[i];
                let storage = FileStorage {
                    id: storage.storage_id.clone(),
                    name: storage.name.clone(),
                    kind: storage.r#type,
                };
                let panel = self.app.panel_mut(panel_id);
                panel.stash = None;
                panel.error = None;
                panel.status_message = None;
                let files = panel.ensure_files();
                files.set_storage(storage);
                files.current_path = String::new();
                files.opened_from = None;
                files.marked.clear();
                self.request_listing(panel_id);
            }
            SelectorRow::Kind(kind) => self.open_content(panel_id, kind),
        }
    }

    fn handle_add_storage_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Esc => {
                tracing::info!("Action: Add storage dialog: Cancel");
                self.app.mode = AppMode::Normal;
            }
            KeyCode::Tab => {
                self.app.dialog_state.active_field = (self.app.dialog_state.active_field + 1) % 2;
            }
            KeyCode::Enter => {
                tracing::info!("Action: Add storage dialog: Submit");
                self.submit_add_storage();
            }
            KeyCode::Backspace => {
                if self.app.dialog_state.active_field == 0 {
                    self.app.dialog_state.input_name.pop();
                } else {
                    self.app.dialog_state.input_url.pop();
                }
            }
            KeyCode::Char(c) => {
                if self.app.dialog_state.active_field == 0 {
                    self.app.dialog_state.input_name.push(c);
                } else {
                    self.app.dialog_state.input_url.push(c);
                }
            }
            _ => {}
        }
    }

    fn handle_vault_join_notify_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Esc => {
                tracing::info!("Action: Vault join notify: Ignore (Esc)");
                self.advance_vault_join_queue();
            }
            KeyCode::Left | KeyCode::Right | KeyCode::Tab => {
                self.app.dialog_state.vault_join_control_selected = !self.app.dialog_state.vault_join_control_selected;
            }
            KeyCode::Enter => {
                if self.app.dialog_state.vault_join_control_selected {
                    tracing::info!("Action: Vault join notify: Decide");
                    self.open_vault_join_requests_dialog();
                } else {
                    tracing::info!("Action: Vault join notify: Ignore");
                    self.advance_vault_join_queue();
                }
            }
            _ => {}
        }
    }

    /// Show the next queued vault-join notification, or return to Normal mode
    /// if the queue is empty.
    fn advance_vault_join_queue(&mut self) {
        if let Some(msg) = self.app.dialog_state.vault_join_queue.pop_front() {
            self.app.dialog_state.vault_join_message = msg;
            self.app.dialog_state.vault_join_control_selected = true;
            // mode stays VaultJoinNotifyDialog
        } else {
            self.app.mode = AppMode::Normal;
        }
    }

    fn open_vault_trust_circle_dialog(&mut self) {
        self.app.dialog_state.vault_devices_focus = 0;
        self.app.dialog_state.vault_devices_selected = 0;
        self.app.dialog_state.vault_devices_scroll = 0;
        self.app.dialog_state.vault_devices_error = None;
        self.app.mode = AppMode::VaultTrustCircleDialog;
        self.fetch_vault_devices();
    }

    fn open_vault_join_requests_dialog(&mut self) {
        // If this device isn't in the trust circle, viewing other devices'
        // join requests is meaningless (we can't approve them anyway). Route
        // to the join-prompt dialog instead so the user can act on their own
        // status first.
        let not_joined = self.app.vault_snapshot.status.as_deref() == Some("not_joined");
        if not_joined {
            tracing::info!("Vault join requests requested while not_joined; showing join prompt instead");
            self.app.dialog_state.vault_join_queue.clear();
            self.app.dialog_state.vault_join_prompt_send_selected = false;
            self.app.mode = AppMode::VaultJoinPromptDialog;
            return;
        }
        self.app.dialog_state.vault_join_focus = 0;
        self.app.dialog_state.vault_join_selected = 0;
        self.app.dialog_state.vault_join_scroll = 0;
        self.app.dialog_state.vault_join_error = None;
        self.app.dialog_state.vault_join_queue.clear();
        self.app.dialog_state.vault_join_requests = self.app.vault_snapshot.pending_join_requests.clone();
        self.app.mode = AppMode::VaultJoinRequestsDialog;
    }

    fn handle_vault_trust_circle_key(&mut self, key: KeyEvent) {
        let list_len = self.app.dialog_state.vault_devices.len();
        let visible_rows: usize = 8;
        match key.code {
            KeyCode::Esc => {
                tracing::info!("Action: Vault Trust Circle dialog: Close");
                self.app.mode = AppMode::Normal;
            }
            KeyCode::Up => {
                if self.app.dialog_state.vault_devices_focus > 0 {
                    self.app.dialog_state.vault_devices_focus = 0;
                } else if self.app.dialog_state.vault_devices_selected > 0 {
                    self.app.dialog_state.vault_devices_selected -= 1;
                    if self.app.dialog_state.vault_devices_selected < self.app.dialog_state.vault_devices_scroll {
                        self.app.dialog_state.vault_devices_scroll = self.app.dialog_state.vault_devices_selected;
                    }
                }
            }
            KeyCode::Down => {
                if self.app.dialog_state.vault_devices_focus == 0
                    && self.app.dialog_state.vault_devices_selected + 1 < list_len
                {
                    self.app.dialog_state.vault_devices_selected += 1;
                    if self.app.dialog_state.vault_devices_selected
                        >= self.app.dialog_state.vault_devices_scroll + visible_rows
                    {
                        self.app.dialog_state.vault_devices_scroll =
                            self.app.dialog_state.vault_devices_selected + 1 - visible_rows;
                    }
                }
            }
            KeyCode::Tab => {
                // Cycle: list(0) → Revoke(1) → Close(2) → list(0)
                self.app.dialog_state.vault_devices_focus = (self.app.dialog_state.vault_devices_focus + 1) % 3;
            }
            KeyCode::Left => {
                if self.app.dialog_state.vault_devices_focus > 1 {
                    self.app.dialog_state.vault_devices_focus -= 1;
                }
            }
            KeyCode::Right => {
                if self.app.dialog_state.vault_devices_focus >= 1 && self.app.dialog_state.vault_devices_focus < 2 {
                    self.app.dialog_state.vault_devices_focus += 1;
                }
            }
            KeyCode::Enter => match self.app.dialog_state.vault_devices_focus {
                1 => self.open_vault_revoke_confirm_dialog(),
                2 => {
                    tracing::info!("Action: Vault Trust Circle dialog: Close");
                    self.app.mode = AppMode::Normal;
                }
                _ => {}
            },
            _ => {}
        }
    }

    fn open_vault_revoke_confirm_dialog(&mut self) {
        let dev = self
            .app
            .dialog_state
            .vault_devices
            .get(self.app.dialog_state.vault_devices_selected)
            .cloned();
        let Some(dev) = dev else {
            tracing::info!("Vault Trust Circle: Revoke pressed with no selection; ignoring");
            return;
        };
        tracing::info!(
            "Action: Vault Trust Circle dialog: Revoke {} ({})",
            dev.display_name,
            dev.agent_id
        );
        self.app.dialog_state.vault_revoke_agent_id = dev.agent_id;
        self.app.dialog_state.vault_revoke_device_name = dev.display_name;
        self.app.dialog_state.vault_revoke_yes_selected = false; // default focus on No (safer)
        self.app.mode = AppMode::VaultRevokeConfirmDialog;
    }

    fn handle_vault_error_dialog_key(&mut self, _key: KeyEvent) {
        // Any key dismisses; return to whatever was open before.
        tracing::info!("Action: Vault error dialog: dismissed");
        self.app.mode = self.app.dialog_state.vault_error_return_mode;
    }

    fn show_vault_error(&mut self, title: impl Into<String>, message: impl Into<String>) {
        // Remember whatever's open so we go back to it after dismissal.
        self.app.dialog_state.vault_error_return_mode = match self.app.mode {
            AppMode::VaultErrorDialog => AppMode::Normal, // already on the error mode
            other => other,
        };
        self.app.dialog_state.vault_error_title = title.into();
        self.app.dialog_state.vault_error_message = message.into();
        self.app.mode = AppMode::VaultErrorDialog;
    }

    fn handle_recovery_words_display_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Esc => {
                tracing::info!("Action: Recovery words display: Close (Esc)");
                self.app.mode = AppMode::Normal;
            }
            KeyCode::Left | KeyCode::Right | KeyCode::Tab => {
                self.app.dialog_state.recovery_display_focus = (self.app.dialog_state.recovery_display_focus + 1) % 2;
            }
            KeyCode::Enter => match self.app.dialog_state.recovery_display_focus {
                0 => {
                    tracing::info!("Action: Recovery words display: Acknowledge");
                    let engine = self.engine.clone();
                    self.spawn_silent(async move { engine.vault_ack_recovery_words().await });
                    self.app.mode = AppMode::Normal;
                }
                _ => {
                    tracing::info!("Action: Recovery words display: Close (keep words)");
                    self.app.mode = AppMode::Normal;
                }
            },
            _ => {}
        }
    }

    fn handle_vault_revoked_notify_key(&mut self, _key: KeyEvent) {
        // Any key dismisses; ack the flag so it doesn't re-fire, and if the
        // vault still reads as `not_joined`, chain into the join prompt.
        tracing::info!("Action: Vault revoked notify: dismissed");
        let engine = self.engine.clone();
        self.spawn_silent(async move { engine.vault_ack_revoked().await });
        // Optimistic local clear; the engine pushes a fresh snapshot.
        self.app.vault_snapshot.revoked_pending = false;
        let still_not_joined = self.app.vault_snapshot.status.as_deref() == Some("not_joined");
        if still_not_joined {
            self.app.dialog_state.vault_join_prompt_send_selected = false;
            self.app.mode = AppMode::VaultJoinPromptDialog;
        } else {
            self.app.mode = AppMode::Normal;
        }
    }

    fn handle_vault_join_prompt_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Esc => {
                tracing::info!("Action: Vault join prompt: Cancel (Esc)");
                self.app.mode = AppMode::Normal;
            }
            KeyCode::Left | KeyCode::Right | KeyCode::Tab => {
                self.app.dialog_state.vault_join_prompt_send_selected =
                    !self.app.dialog_state.vault_join_prompt_send_selected;
            }
            KeyCode::Enter => {
                if self.app.dialog_state.vault_join_prompt_send_selected {
                    tracing::info!("Action: Vault join prompt: Send join request");
                    self.req_vault_join_start();
                } else {
                    tracing::info!("Action: Vault join prompt: Cancel");
                }
                self.app.mode = AppMode::Normal;
            }
            _ => {}
        }
    }

    fn handle_vault_recovery_input_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Esc => {
                tracing::info!("Action: Vault recovery input: Cancel (Esc)");
                self.app.mode = AppMode::Normal;
            }
            KeyCode::Tab => {
                // Cycle: input(0) → Recover(1) → Cancel(2) → input(0)
                self.app.dialog_state.recovery_input_focus = (self.app.dialog_state.recovery_input_focus + 1) % 3;
            }
            KeyCode::Left => {
                if self.app.dialog_state.recovery_input_focus > 1 {
                    self.app.dialog_state.recovery_input_focus -= 1;
                }
            }
            KeyCode::Right => {
                if self.app.dialog_state.recovery_input_focus >= 1 && self.app.dialog_state.recovery_input_focus < 2 {
                    self.app.dialog_state.recovery_input_focus += 1;
                }
            }
            KeyCode::Backspace if self.app.dialog_state.recovery_input_focus == 0 => {
                self.app.dialog_state.recovery_input_text.pop();
            }
            KeyCode::Char(c) if self.app.dialog_state.recovery_input_focus == 0 => {
                self.app.dialog_state.recovery_input_text.push(c);
            }
            KeyCode::Enter => {
                match self.app.dialog_state.recovery_input_focus {
                    2 => {
                        tracing::info!("Action: Vault recovery input: Cancel");
                        self.app.mode = AppMode::Normal;
                    }
                    _ => {
                        // Input or Recover: submit
                        let mnemonic = self
                            .app
                            .dialog_state
                            .recovery_input_text
                            .split_whitespace()
                            .collect::<Vec<_>>()
                            .join(" ");
                        if mnemonic.is_empty() {
                            tracing::info!("Action: Vault recovery input: Recover (empty, ignored)");
                        } else {
                            tracing::info!("Action: Vault recovery input: Recover");
                            self.req_vault_recover(mnemonic);
                            self.app.mode = AppMode::Normal;
                        }
                    }
                }
            }
            _ => {}
        }
    }

    /// The emailed step-up code for the parked recover.
    fn handle_step_up_code_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Esc => {
                tracing::info!("Action: Step-up code: Cancel (Esc)");
                let engine = self.engine.clone();
                self.spawn_silent(async move { engine.vault_step_up_cancel().await });
                self.app.mode = AppMode::Normal;
            }
            KeyCode::Tab => {
                // Cycle: input(0) → Verify(1) → Cancel(2) → input(0)
                self.app.dialog_state.step_up_focus = (self.app.dialog_state.step_up_focus + 1) % 3;
            }
            KeyCode::Left => {
                if self.app.dialog_state.step_up_focus > 1 {
                    self.app.dialog_state.step_up_focus -= 1;
                }
            }
            KeyCode::Right => {
                if self.app.dialog_state.step_up_focus >= 1 && self.app.dialog_state.step_up_focus < 2 {
                    self.app.dialog_state.step_up_focus += 1;
                }
            }
            KeyCode::Backspace if self.app.dialog_state.step_up_focus == 0 => {
                self.app.dialog_state.step_up_code_text.pop();
            }
            KeyCode::Char(c) if self.app.dialog_state.step_up_focus == 0 => {
                self.app.dialog_state.step_up_code_text.push(c);
            }
            KeyCode::Enter => match self.app.dialog_state.step_up_focus {
                2 => {
                    tracing::info!("Action: Step-up code: Cancel");
                    let engine = self.engine.clone();
                    self.spawn_silent(async move { engine.vault_step_up_cancel().await });
                    self.app.mode = AppMode::Normal;
                }
                _ => {
                    let code = self.app.dialog_state.step_up_code_text.trim().to_string();
                    if code.is_empty() {
                        tracing::info!("Action: Step-up code: Verify (empty, ignored)");
                    } else {
                        tracing::info!("Action: Step-up code: Verify");
                        self.req_vault_step_up_verify(code);
                    }
                }
            },
            _ => {}
        }
    }

    fn handle_vault_recovery_regenerate_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Esc => {
                tracing::info!("Action: Vault recovery regenerate confirm: Cancel (Esc)");
                self.app.mode = AppMode::Normal;
            }
            KeyCode::Left | KeyCode::Right | KeyCode::Tab => {
                self.app.dialog_state.recovery_regen_yes_selected = !self.app.dialog_state.recovery_regen_yes_selected;
            }
            KeyCode::Enter => {
                if self.app.dialog_state.recovery_regen_yes_selected {
                    tracing::info!("Action: Vault recovery regenerate confirm: Regenerate");
                    self.req_vault_regenerate();
                } else {
                    tracing::info!("Action: Vault recovery regenerate confirm: Cancel");
                }
                self.app.mode = AppMode::Normal;
            }
            _ => {}
        }
    }

    fn handle_vault_revoke_confirm_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Esc => {
                tracing::info!("Action: Vault Revoke confirm: Cancel (Esc)");
                self.app.mode = AppMode::VaultTrustCircleDialog;
            }
            KeyCode::Left | KeyCode::Right | KeyCode::Tab => {
                self.app.dialog_state.vault_revoke_yes_selected = !self.app.dialog_state.vault_revoke_yes_selected;
            }
            KeyCode::Enter => {
                if self.app.dialog_state.vault_revoke_yes_selected {
                    let agent_id = self.app.dialog_state.vault_revoke_agent_id.clone();
                    tracing::info!("Action: Vault Revoke confirm: Yes ({})", agent_id);
                    self.req_vault_revoke(agent_id);
                } else {
                    tracing::info!("Action: Vault Revoke confirm: No");
                }
                self.app.mode = AppMode::VaultTrustCircleDialog;
            }
            _ => {}
        }
    }

    fn handle_vault_join_requests_key(&mut self, key: KeyEvent) {
        let list_len = self.app.dialog_state.vault_join_requests.len();
        let visible_rows: usize = 8;
        match key.code {
            KeyCode::Esc => {
                tracing::info!("Action: Vault join requests dialog: Close");
                self.app.mode = AppMode::Normal;
            }
            KeyCode::Up => {
                if self.app.dialog_state.vault_join_focus > 0 {
                    self.app.dialog_state.vault_join_focus = 0;
                } else if self.app.dialog_state.vault_join_selected > 0 {
                    self.app.dialog_state.vault_join_selected -= 1;
                    if self.app.dialog_state.vault_join_selected < self.app.dialog_state.vault_join_scroll {
                        self.app.dialog_state.vault_join_scroll = self.app.dialog_state.vault_join_selected;
                    }
                }
            }
            KeyCode::Down => {
                if self.app.dialog_state.vault_join_focus == 0
                    && self.app.dialog_state.vault_join_selected + 1 < list_len
                {
                    self.app.dialog_state.vault_join_selected += 1;
                    if self.app.dialog_state.vault_join_selected
                        >= self.app.dialog_state.vault_join_scroll + visible_rows
                    {
                        self.app.dialog_state.vault_join_scroll =
                            self.app.dialog_state.vault_join_selected + 1 - visible_rows;
                    }
                }
            }
            KeyCode::Tab => {
                // Cycle: list(0) → Approve(1) → Decline(2) → Close(3) → list(0)
                self.app.dialog_state.vault_join_focus = (self.app.dialog_state.vault_join_focus + 1) % 4;
            }
            KeyCode::Left => {
                if self.app.dialog_state.vault_join_focus > 1 {
                    self.app.dialog_state.vault_join_focus -= 1;
                }
            }
            KeyCode::Right => {
                if self.app.dialog_state.vault_join_focus >= 1 && self.app.dialog_state.vault_join_focus < 3 {
                    self.app.dialog_state.vault_join_focus += 1;
                }
            }
            KeyCode::Enter => {
                let agent_id = self
                    .app
                    .dialog_state
                    .vault_join_requests
                    .get(self.app.dialog_state.vault_join_selected)
                    .map(|r| r.agent_id.clone());
                match self.app.dialog_state.vault_join_focus {
                    1 => {
                        if let Some(id) = agent_id {
                            tracing::info!("Action: vault join Requests: Approve {}", id);
                            self.req_vault_approve(id);
                        }
                    }
                    2 => {
                        if let Some(id) = agent_id {
                            tracing::info!("Action: Vault join requests: Decline {}", id);
                            self.req_vault_reject(id);
                        }
                    }
                    3 => {
                        tracing::info!("Action: Vault join requests: Close");
                        self.app.mode = AppMode::Normal;
                    }
                    _ => {} // focus on list, no default action
                }
            }
            _ => {}
        }
    }

    fn handle_copy_confirm_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Esc => {
                tracing::info!("Action: Copy confirm dialog: Cancel");
                self.app.mode = AppMode::Normal;
            }
            KeyCode::Left | KeyCode::Right | KeyCode::Tab => {
                self.app.dialog_state.copy_ok_selected = !self.app.dialog_state.copy_ok_selected;
            }
            KeyCode::Enter => {
                if self.app.dialog_state.copy_ok_selected {
                    tracing::info!("Action: Copy confirm dialog: OK");
                    self.start_copy();
                } else {
                    tracing::info!("Action: Copy confirm dialog: Cancel");
                    self.app.mode = AppMode::Normal;
                }
            }
            _ => {}
        }
    }

    fn handle_delete_confirm_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Esc => {
                tracing::info!("Action: Delete confirm dialog: Cancel");
                self.app.mode = AppMode::Normal;
            }
            KeyCode::Left | KeyCode::Right | KeyCode::Tab => {
                self.app.dialog_state.delete_yes_selected = !self.app.dialog_state.delete_yes_selected;
            }
            KeyCode::Enter => {
                if self.app.dialog_state.delete_yes_selected {
                    tracing::info!("Action: Delete confirm dialog: Yes");
                    self.do_delete();
                } else {
                    tracing::info!("Action: Delete confirm dialog: No");
                    self.app.mode = AppMode::Normal;
                }
            }
            KeyCode::Char('y') | KeyCode::Char('Y') => {
                tracing::info!("Action: Delete confirm dialog: Yes");
                self.do_delete();
            }
            KeyCode::Char('n') | KeyCode::Char('N') => {
                tracing::info!("Action: Delete confirm dialog: No");
                self.app.mode = AppMode::Normal;
            }
            _ => {}
        }
    }

    // ── find (Alt+F7) ──────────────────────────────────────────────────────

    /// Alt+F7 on a file list: what to find and where. The name is kept from
    /// the last run (FAR).
    fn open_find_dialog(&mut self) {
        let Some(files) = self.app.active_files() else { return };
        if files.storage.is_none() {
            return;
        }
        tracing::info!("Action: Find file dialog");
        let d = &mut self.app.dialog_state;
        d.find_focus = 0;
        d.find_name.end();
        self.app.mode = AppMode::FindDialog;
    }

    fn handle_find_dialog_key(&mut self, key: KeyEvent) {
        let d = &mut self.app.dialog_state;
        match key.code {
            KeyCode::Esc => {
                tracing::info!("Action: Find dialog: Cancel");
                self.close_find();
            }
            KeyCode::Tab | KeyCode::Down => {
                d.find_focus = (d.find_focus + 1) % 5;
            }
            KeyCode::BackTab | KeyCode::Up => {
                d.find_focus = (d.find_focus + 4) % 5;
            }
            KeyCode::Char(' ') | KeyCode::Left | KeyCode::Right if d.find_focus == 1 => {
                d.find_whole = !d.find_whole;
            }
            KeyCode::Char(' ') if d.find_focus == 2 => {
                d.find_recursive = !d.find_recursive;
            }
            KeyCode::Left | KeyCode::Right if d.find_focus >= 3 => {
                d.find_focus = if d.find_focus == 3 { 4 } else { 3 };
            }
            KeyCode::Enter => {
                if d.find_focus == 4 {
                    tracing::info!("Action: Find dialog: Cancel");
                    self.close_find();
                } else {
                    self.start_find();
                }
            }
            _ if d.find_focus == 0 && d.find_name.handle_key(key) => {}
            _ => {}
        }
    }

    /// Run the search on the active panel's storage and open the results.
    fn start_find(&mut self) {
        let panel_id = self.app.active_panel_id();
        let Some(files) = self.app.active_files() else { return };
        let Some(storage_id) = files.storage_id().map(str::to_string) else {
            return;
        };
        let d = &self.app.dialog_state;
        let needle = d.find_name.text.trim().to_string();
        let scope = if d.find_whole {
            String::new()
        } else {
            files.current_path.clone()
        };
        let recursive = d.find_recursive;
        let local = files.is_local();
        tracing::info!("Action: Find [{}] in [{}] recursive={}", needle, scope, recursive);

        if let Some(mut old) = self.app.find.take() {
            old.abort();
        }
        self.app.find_seq += 1;
        let seq = self.app.find_seq;
        let mut find = FindState::new(panel_id, storage_id.clone(), local, scope.clone(), needle.clone(), seq);

        let engine = self.engine.clone();
        let tx = self.tx.clone();
        let handle = self.rt.spawn(async move {
            let query = StorageSearchQuery {
                name_match: (!needle.is_empty()).then_some(needle),
            };
            let mut options = SearchOptions {
                path_scope: (!scope.is_empty()).then_some(scope),
                recursive: Some(recursive),
                page_limit: Some(FIND_PAGE_LIMIT),
                ..Default::default()
            };
            loop {
                let result = engine.search(&storage_id, &query, &options).await;
                let next = result.as_ref().ok().and_then(|r| r.next_page_token.clone());
                if tx.send(UiEvent::FindPage { seq, result }).is_err() {
                    return;
                }
                match next {
                    Some(token) => options.page_token = Some(token),
                    None => return,
                }
            }
        });
        find.started(handle.abort_handle());
        self.app.find = Some(find);
        self.app.mode = AppMode::FindResults;
    }

    /// Drop the run and its results.
    fn close_find(&mut self) {
        if let Some(mut find) = self.app.find.take() {
            find.abort();
        }
        self.app.mode = AppMode::Normal;
    }

    fn handle_find_results_key(&mut self, key: KeyEvent) {
        let Some(find) = self.app.find.as_mut() else {
            self.app.mode = AppMode::Normal;
            return;
        };
        let page = find.visible.max(1) as i32;
        match key.code {
            KeyCode::Esc => {
                tracing::info!("Action: Find results: Cancel");
                self.close_find();
            }
            KeyCode::Tab => {
                find.focus = (find.focus + 1) % 6;
            }
            KeyCode::BackTab => {
                find.focus = (find.focus + 5) % 6;
            }
            KeyCode::Up if find.focus == 0 => find.move_by(-1),
            KeyCode::Down if find.focus == 0 => find.move_by(1),
            KeyCode::PageUp if find.focus == 0 => find.move_by(-page),
            KeyCode::PageDown if find.focus == 0 => find.move_by(page),
            KeyCode::Home if find.focus == 0 => find.select(0),
            KeyCode::End if find.focus == 0 => find.select(usize::MAX),
            KeyCode::Left if find.focus > 1 => find.focus -= 1,
            KeyCode::Right if find.focus >= 1 && find.focus < 5 => find.focus += 1,
            KeyCode::F(3) => self.find_view(),
            KeyCode::Enter => match find.focus {
                0 | 2 => self.find_go_to(),
                1 => {
                    tracing::info!("Action: Find results: New search");
                    if let Some(mut find) = self.app.find.take() {
                        find.abort();
                    }
                    self.app.dialog_state.find_focus = 0;
                    self.app.mode = AppMode::FindDialog;
                }
                3 => self.find_view(),
                4 => self.find_panelize(),
                _ => {
                    tracing::info!("Action: Find results: Cancel");
                    self.close_find();
                }
            },
            _ => {}
        }
    }

    /// The results' panel, if it still shows the storage that was searched.
    fn find_panel(&self) -> Option<PanelId> {
        let find = self.app.find.as_ref()?;
        let files = self.app.panel(find.panel).files()?;
        (files.storage_id() == Some(find.storage_id.as_str())).then_some(find.panel)
    }

    /// Go to: the panel on the item's directory, the cursor on it.
    fn find_go_to(&mut self) {
        let Some(find) = self.app.find.as_ref() else { return };
        if find.items.is_empty() {
            return;
        }
        let (dir, name) = find.split(find.selected);
        let (dir, name) = (dir.to_string(), name.to_string());
        tracing::info!("Action: Find results: Go to [{}/{}]", dir, name);
        let Some(panel_id) = self.find_panel() else {
            self.close_find();
            return;
        };
        self.close_find();
        self.app.active_panel = panel_id;
        if let Some(files) = self.app.active_files_mut() {
            files.temporary = None;
            files.marked.clear();
            files.current_path = dir;
            files.reset_selection_on_load = true;
        }
        self.pending_focus_name = Some(name);
        self.refresh_active_panel();
    }

    /// View: the built-in viewer on the item; closing it returns here.
    fn find_view(&mut self) {
        let Some(find) = self.app.find.as_ref() else { return };
        let Some(item) = find.items.get(find.selected) else {
            return;
        };
        if !is_file(item) {
            return;
        }
        let (_, name) = find.split(find.selected);
        let (storage_id, path, local, title) =
            (find.storage_id.clone(), item.key.clone(), find.local, name.to_string());
        tracing::info!("Action: Find results: View [{}]", path);
        self.open_viewer(storage_id, path, local, title);
    }

    /// Panel: the results as the panel's list (FAR's temporary panel).
    fn find_panelize(&mut self) {
        let Some(panel_id) = self.find_panel() else {
            self.close_find();
            return;
        };
        let Some(mut find) = self.app.find.take() else { return };
        find.abort();
        tracing::info!("Action: Find results: Panel ({} items)", find.items.len());
        let panel = self.app.panel_mut(panel_id);
        panel.stop_listing();
        let sort = panel.sort;
        let Some(files) = panel.files_mut() else { return };
        let mut up = StorageObjectInfo::new("..");
        up.file_type = StorageFileType::Dir;
        let mut entries = Vec::with_capacity(find.items.len() + 1);
        entries.push(up);
        entries.extend(find.items);
        files.temporary = Some(FoundList {
            back_to: find.scope,
            needle: find.needle,
        });
        files.current_path = String::new();
        files.entries = entries;
        files.sort_entries(sort);
        files.marked.clear();
        files.dir_sizes.clear();
        files.quick_search = None;
        files.cursor.reset();
        panel.error = None;
        self.app.active_panel = panel_id;
        self.app.mode = AppMode::Normal;
    }

    fn handle_mkdir_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Esc => {
                tracing::info!("Action: Mkdir dialog: Cancel");
                self.app.mode = AppMode::Normal;
            }
            KeyCode::Tab => {
                self.app.dialog_state.mkdir_focus = (self.app.dialog_state.mkdir_focus + 1) % 3;
            }
            KeyCode::Enter => {
                if self.app.dialog_state.mkdir_focus == 2 {
                    tracing::info!("Action: Mkdir dialog: Cancel");
                    self.app.mode = AppMode::Normal;
                } else {
                    tracing::info!("Action: Mkdir dialog: OK");
                    self.do_mkdir();
                }
            }
            _ if self.app.dialog_state.mkdir_focus == 0 && self.app.dialog_state.mkdir.handle_key(key) => {}
            _ => {}
        }
    }

    /// The progress dialog has one button: Cancel.
    fn handle_copying_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Esc | KeyCode::Enter => {
                tracing::info!("Action: Copy progress dialog: Cancel");
                self.req_cancel_transfer();
            }
            _ => {}
        }
    }

    fn handle_conflict_key(&mut self, key: KeyEvent) {
        // Button indices: 0=Yes, 1=No, 2=All, 3=Older, 4=None, 5=Smaller, 6=Size differs, 7=Abort
        match key.code {
            KeyCode::Esc | KeyCode::Char('b') | KeyCode::Char('B') => {
                tracing::info!("Action: Conflict dialog: Abort");
                self.resolve_current_conflict(7);
            }
            KeyCode::Left | KeyCode::Up => {
                if self.app.dialog_state.conflict_focus > 0 {
                    self.app.dialog_state.conflict_focus -= 1;
                }
            }
            KeyCode::Right | KeyCode::Down => {
                if self.app.dialog_state.conflict_focus < 7 {
                    self.app.dialog_state.conflict_focus += 1;
                }
            }
            KeyCode::Tab => {
                self.app.dialog_state.conflict_focus = (self.app.dialog_state.conflict_focus + 1) % 8;
            }
            KeyCode::Enter => {
                let labels = ["Yes", "No", "All", "Older", "None", "Smaller", "Size differs", "Abort"];
                let focus = self.app.dialog_state.conflict_focus;
                let label = labels.get(focus).unwrap_or(&"Unknown");
                tracing::info!("Action: Conflict dialog: {}", label);
                self.resolve_current_conflict(focus);
            }
            // Hotkeys
            KeyCode::Char('y') | KeyCode::Char('Y') => {
                tracing::info!("Action: Conflict dialog: Yes");
                self.resolve_current_conflict(0); // Yes - overwrite this file
            }
            KeyCode::Char('n') | KeyCode::Char('N') => {
                tracing::info!("Action: Conflict dialog: No");
                self.resolve_current_conflict(1); // No - skip this file
            }
            KeyCode::Char('a') | KeyCode::Char('A') => {
                tracing::info!("Action: Conflict dialog: All");
                self.resolve_current_conflict(2); // All - overwrite all
            }
            KeyCode::Char('o') | KeyCode::Char('O') => {
                tracing::info!("Action: Conflict dialog: Older");
                self.resolve_current_conflict(3); // Older - overwrite when the source is newer
            }
            KeyCode::Char('s') | KeyCode::Char('S') => {
                tracing::info!("Action: Conflict dialog: Smaller");
                self.resolve_current_conflict(5); // Smaller - overwrite when the source is larger
            }
            KeyCode::Char('z') | KeyCode::Char('Z') => {
                tracing::info!("Action: Conflict dialog: Size differs");
                self.resolve_current_conflict(6); // Size differs - overwrite when sizes differ
            }
            _ => {}
        }
    }

    /// Answer the parked conflict batch. Per-file Yes/No accumulate until
    /// the batch is exhausted; the bulk buttons decide the rest at once.
    fn resolve_current_conflict(&mut self, choice: usize) {
        let each = |items: Vec<(String, OnExisting)>| ConflictDecision::Each(items.into_iter().collect());

        match choice {
            0 | 1 => {
                // Yes/No - accumulate resolution for this file, show next
                let resolution = if choice == 0 {
                    OnExisting::Overwrite
                } else {
                    OnExisting::Skip
                };
                if let Some(conflict) = self.app.transfer_conflicts.first() {
                    self.app
                        .pending_conflict_resolutions
                        .push((conflict.key.clone(), resolution));
                }
                if !self.app.transfer_conflicts.is_empty() {
                    self.app.transfer_conflicts.remove(0);
                }
                if self.app.transfer_conflicts.is_empty() {
                    // All conflicts resolved individually — send the full batch
                    let resolutions = std::mem::take(&mut self.app.pending_conflict_resolutions);
                    self.req_resolve_conflicts(each(resolutions));
                    self.app.mode = AppMode::Copying;
                } else {
                    self.app.dialog_state.conflict_focus = 0;
                }
            }
            2..=6 => {
                // All/Older/None/Smaller/Size differs - bulk resolve all remaining
                let resolution = match choice {
                    3 => OnExisting::KeepNewer,
                    4 => OnExisting::Skip,
                    5 => OnExisting::KeepLarger,
                    6 => OnExisting::IfSizeDiffers,
                    _ => OnExisting::Overwrite, // 2 = All
                };
                let mut resolutions = std::mem::take(&mut self.app.pending_conflict_resolutions);
                // No per-file answers yet: one rule for the whole batch (and
                // for any conflict a live source surfaces later). Otherwise
                // the rule fills in the files not answered individually.
                let decision = if resolutions.is_empty() {
                    ConflictDecision::All(resolution)
                } else {
                    resolutions.extend(self.app.transfer_conflicts.iter().map(|c| (c.key.clone(), resolution)));
                    each(resolutions)
                };
                self.req_resolve_conflicts(decision);
                self.app.transfer_conflicts.clear();
                self.app.mode = AppMode::Copying;
            }
            7 => {
                // Abort - the engine cancels the transfer before any write
                self.req_resolve_conflicts(ConflictDecision::Cancel);
                self.app.pending_conflict_resolutions.clear();
                self.app.transfer_conflicts.clear();
                self.app.mode = AppMode::Copying;
            }
            _ => {}
        }
    }

    fn handle_transfer_error_key(&mut self, key: KeyEvent) {
        // Button indices: 0=Retry, 1=Skip, 2=Cancel
        match key.code {
            KeyCode::Esc | KeyCode::Char('c') | KeyCode::Char('C') => {
                self.send_transfer_error_action(ErrorDecision::Cancel);
            }
            KeyCode::Left => {
                if self.app.dialog_state.transfer_error_focus > 0 {
                    self.app.dialog_state.transfer_error_focus -= 1;
                }
            }
            KeyCode::Right => {
                if self.app.dialog_state.transfer_error_focus < 2 {
                    self.app.dialog_state.transfer_error_focus += 1;
                }
            }
            KeyCode::Tab => {
                self.app.dialog_state.transfer_error_focus = (self.app.dialog_state.transfer_error_focus + 1) % 3;
            }
            KeyCode::Enter => {
                let action = match self.app.dialog_state.transfer_error_focus {
                    0 => ErrorDecision::Retry,
                    1 => ErrorDecision::Skip,
                    _ => ErrorDecision::Cancel,
                };
                self.send_transfer_error_action(action);
            }
            KeyCode::Char('r') | KeyCode::Char('R') => {
                self.send_transfer_error_action(ErrorDecision::Retry);
            }
            KeyCode::Char('s') | KeyCode::Char('S') => {
                self.send_transfer_error_action(ErrorDecision::Skip);
            }
            _ => {}
        }
    }

    fn send_transfer_error_action(&mut self, action: ErrorDecision) {
        tracing::info!("Action: Transfer error dialog: {:?}", action);
        self.req_resolve_error(action);
        // Back to the progress dialog; the terminal status follows as an event.
        self.app.mode = AppMode::Copying;
    }

    fn handle_listing_error_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Esc | KeyCode::Enter | KeyCode::Char('b') | KeyCode::Char('B') => {
                tracing::info!("Action: Listing error dialog: Back");
                self.app.dialog_state.listing_error_panel = None;
                self.app.mode = AppMode::Normal;
            }
            _ => {}
        }
    }

    fn refresh_active_panel(&mut self) {
        let panel_id = self.app.active_panel_id();
        self.request_listing(panel_id);
    }

    /// Show `kind` in `panel_id` (the storage selector); a Links panel lists.
    fn open_content(&mut self, panel_id: PanelId, kind: ContentKind) {
        tracing::info!("Action: {} panel", kind.label());
        self.app.panel_mut(panel_id).show(kind);
        self.request_listing(panel_id);
    }

    /// Alt+C-style hotkey: `kind` in the active panel, or — when it already
    /// shows it — back to its file list.
    fn toggle_content(&mut self, kind: ContentKind) {
        let panel_id = self.app.active_panel_id();
        tracing::info!("Action: {} panel (toggle)", kind.label());
        if self.app.panel_mut(panel_id).toggle_content(kind) {
            self.request_listing(panel_id);
        }
    }

    /// Enter on the User panel: log in or out, per the session state.
    fn user_enter(&mut self) {
        let is_logged_in = self
            .app
            .backend_status
            .as_ref()
            .and_then(|s| s.connection.user.as_ref())
            .is_some();
        if is_logged_in {
            tracing::info!("Action: Logout");
            self.app.active_panel_mut().status_message = Some("Signing out...".to_string());
            self.req_logout();
        } else if self.is_backend_online() {
            tracing::info!("Action: Start OAuth login");
            self.app.active_panel_mut().status_message = Some("Starting login...".to_string());
            self.req_login();
        }
    }

    fn req_send_nearby_offer(&self, storage_id: String, keys: Vec<String>, agent_id: String, name: String) {
        let engine = self.engine.clone();
        self.spawn(
            async move { UiEvent::NearbyOffer(engine.send_nearby_offer(&agent_id, &storage_id, keys, name).await) },
        );
    }

    fn req_cancel_nearby_offer(&self, agent_id: String) {
        let engine = self.engine.clone();
        self.spawn(async move { UiEvent::NearbyOfferCancel(engine.cancel_nearby_offer(&agent_id).await) });
    }

    fn req_accept_inbound_offer(&self) {
        let engine = self.engine.clone();
        self.spawn(async move { UiEvent::ShareOfferAccepted(engine.accept_inbound_offer().await) });
    }

    fn req_reject_inbound_offer(&self) {
        let engine = self.engine.clone();
        self.spawn(async move { UiEvent::ShareOfferRejected(engine.reject_inbound_offer().await) });
    }

    fn open_share_offer_dialog(&mut self, offer: InboundOfferView) {
        self.app.dialog_state.share_offer = Some(offer);
        self.app.dialog_state.share_offer_accept_selected = true;
        self.app.mode = AppMode::ShareOfferDialog;
    }

    /// Accept / Decline; Esc closes without answering — the offer stays
    /// pending on the sender's side (its card times out to "no answer").
    fn handle_share_offer_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Esc => {
                tracing::info!("Action: Nearby offer: Cancel (Esc)");
                self.app.dialog_state.share_offer = None;
                self.app.mode = AppMode::Normal;
            }
            KeyCode::Left | KeyCode::Right | KeyCode::Tab => {
                self.app.dialog_state.share_offer_accept_selected = !self.app.dialog_state.share_offer_accept_selected;
            }
            KeyCode::Enter => {
                if self.app.dialog_state.share_offer_accept_selected {
                    tracing::info!("Action: Nearby offer: Accept");
                    self.req_accept_inbound_offer();
                } else {
                    tracing::info!("Action: Nearby offer: Decline");
                    self.req_reject_inbound_offer();
                }
                self.app.dialog_state.share_offer = None;
                self.app.mode = AppMode::Normal;
            }
            _ => {}
        }
    }

    fn enter_selected(&mut self) {
        let panel_id = self.app.active_panel_id();
        match self.app.active_panel().content.kind() {
            ContentKind::Files => self.enter_files(),
            // A link: open the share in this panel.
            ContentKind::Links => {
                let Content::Links(list) = &self.app.active_panel().content else {
                    return;
                };
                let Some(link) = list.selected().cloned() else { return };
                if !link.has_source() {
                    tracing::info!(token = %link.token, "link {} has no source", link.name);
                    self.app.active_panel_mut().error = Some(format!("{}: no sources available", link.name));
                    return;
                }
                tracing::info!(token = %link.token, "opening link {}", link.name);
                let mut files = FileList::new();
                files.opened_from = Some(OpenedFrom::Links);
                let panel = self.app.active_panel_mut();
                panel.content = Content::Files(files);
                panel.error = None;
                self.request_connect_storage(
                    panel_id,
                    link.name,
                    StorageConnectRequest::Ninja { link_url: link.token },
                );
            }
            // A device: its details.
            ContentKind::Nearby => {
                let Content::Nearby(list) = &self.app.active_panel().content else {
                    return;
                };
                if list.open.is_some() {
                    return;
                }
                let Some(agent_id) = self.app.nearby.get(list.cursor.index).map(|d| d.agent_id.clone()) else {
                    return;
                };
                if let Content::Nearby(list) = &mut self.app.active_panel_mut().content {
                    list.open = Some(agent_id);
                }
            }
            ContentKind::User => self.user_enter(),
            ContentKind::Connection | ContentKind::Log | ContentKind::Info => {}
        }
    }

    /// Enter on a file list: into a directory (`..` up, or back to where the
    /// list was entered from at its root); an archive opens in the panel; an
    /// audio file opens the mini player (F3 still views the bytes); other
    /// files are a no-op.
    fn enter_files(&mut self) {
        let panel_id = self.app.active_panel_id();
        let Some(files) = self.app.active_files_mut() else {
            return;
        };
        let Some(entry) = files.selected_entry().cloned() else {
            return;
        };
        if !is_file(&entry) {
            files.marked.clear();
            // Out of a find's temporary list: `..` back to the searched
            // directory, a directory into itself (its key is its path).
            if let Some(found) = files.temporary.take() {
                files.current_path = if entry.key == ".." {
                    found.back_to
                } else {
                    entry.key.clone()
                };
                files.reset_selection_on_load = true;
                self.refresh_active_panel();
                return;
            }
            if entry.key == ".." && files.current_path.is_empty() {
                self.leave_root(panel_id);
                return;
            }
            if entry.key == ".." {
                // Going up - save current dir name for focus restoration
                let current_dir_name = files.current_dir_name();
                files.current_path = match files.current_path.rfind('/') {
                    Some(pos) => files.current_path[..pos].to_string(),
                    None => String::new(),
                };
                self.pending_focus_name = Some(current_dir_name);
            } else {
                // Going down - save previous path for back navigation on error
                files.previous_path = Some(files.current_path.clone());
                files.save_selection_to_history();
                files.current_path = files.key_of(&entry.key);
            }
            files.reset_selection_on_load = true;
            self.refresh_active_panel();
            return;
        }
        if crate::archive::is_archive(&entry.key) {
            self.open_archive_selected();
            return;
        }
        if let Some(ext) = crate::player::playable_extension(&entry.key) {
            self.open_player_selected(ext);
        }
    }

    /// Enter on an archive file: browse it in this panel as a storage of
    /// its own, opened on the panel's storage (`..` at its root comes back
    /// here).
    fn open_archive_selected(&mut self) {
        let panel_id = self.app.active_panel_id();
        let Some(files) = self.app.active_files_mut() else {
            return;
        };
        let Some(storage) = files.storage.clone() else { return };
        let Some(entry) = files.selected_entry().cloned() else {
            return;
        };
        let path = files.key_of(&entry.key);
        tracing::info!("Action: Open archive [{}]", path);
        let mut inside = FileList::new();
        inside.opened_from = Some(OpenedFrom::Archive {
            storage: storage.clone(),
            dir: files.current_path.clone(),
            name: entry.key.clone(),
            from: files.opened_from.take().map(Box::new),
        });
        let panel = self.app.panel_mut(panel_id);
        panel.content = Content::Files(inside);
        panel.error = None;
        self.request_connect_storage(
            panel_id,
            entry.key,
            StorageConnectRequest::Archive {
                storage_id: storage.id,
                path,
            },
        );
    }

    /// `..` at the root of a list entered from somewhere: back there.
    fn leave_root(&mut self, panel_id: PanelId) {
        let Some(files) = self.app.panel_mut(panel_id).files_mut() else {
            return;
        };
        match files.opened_from.take() {
            Some(OpenedFrom::Links) => self.return_to_links(panel_id),
            Some(OpenedFrom::Archive {
                storage,
                dir,
                name,
                from,
            }) => {
                self.show_parent_listing(panel_id, storage, dir, name, from);
            }
            None => {}
        }
    }

    /// Back from a share entered from the Links panel to the list, with the
    /// cursor on the link we came from.
    fn return_to_links(&mut self, panel_id: PanelId) {
        let is_active = self.app.active_panel_id() == panel_id;
        let panel = self.app.panel_mut(panel_id);
        let Some(files) = panel.files() else { return };
        let came_from = files.storage.as_ref().map(|s| s.name.clone()).unwrap_or_default();
        tracing::info!("Action: back to Links from {}", came_from);
        panel.content = Content::Links(LinkList::default());
        panel.error = None;
        panel.status_message = None;
        if is_active {
            self.pending_focus_name = Some(came_from);
        }
        self.request_listing(panel_id);
    }

    /// Back from an archive to the listing it was entered from: directory
    /// `dir` of `storage`, cursor on the archive `name`; that listing's own
    /// origin `from` is restored with it.
    fn show_parent_listing(
        &mut self,
        panel_id: PanelId,
        storage: FileStorage,
        dir: String,
        name: String,
        from: Option<Box<OpenedFrom>>,
    ) {
        let is_active = self.app.active_panel_id() == panel_id;
        tracing::info!("Action: back to [{}] from archive {}", dir, name);
        let panel = self.app.panel_mut(panel_id);
        let files = panel.ensure_files();
        files.set_storage(storage);
        files.current_path = dir;
        files.opened_from = from.map(|f| *f);
        files.marked.clear();
        files.reset_selection_on_load = true;
        panel.status_message = None;
        if is_active {
            self.pending_focus_name = Some(name);
        }
        self.request_listing(panel_id);
    }

    /// Backspace / ←: up one directory (or back to the Links panel), or
    /// from a device's details back to the Nearby list.
    fn go_back(&mut self) {
        let panel_id = self.app.active_panel_id();
        match &mut self.app.active_panel_mut().content {
            Content::Nearby(list) => {
                list.open = None;
            }
            Content::Files(files) => {
                if let Some(found) = files.temporary.take() {
                    files.marked.clear();
                    files.current_path = found.back_to;
                    files.reset_selection_on_load = true;
                    self.refresh_active_panel();
                    return;
                }
                if files.current_path.is_empty() {
                    self.leave_root(panel_id);
                    return;
                }
                files.marked.clear();
                // Save current dir name for focus restoration
                let current_dir_name = files.current_dir_name();
                files.current_path = match files.current_path.rfind('/') {
                    Some(pos) => files.current_path[..pos].to_string(),
                    None => String::new(),
                };
                self.pending_focus_name = Some(current_dir_name);
                files.reset_selection_on_load = true;
                self.refresh_active_panel();
            }
            _ => {}
        }
    }

    // ── player ─────────────────────────────────────────────────────────────

    /// Enter on an mp3/aac/m4a: the mini music player, playing straight off
    /// the panel's storage — a local file and a streamed share file go
    /// through the same `Engine::read_range` path (nothing is copied).
    fn open_player_selected(&mut self, extension: String) {
        let Some(files) = self.app.active_files() else { return };
        let Some(storage_id) = files.storage_id().map(str::to_string) else {
            return;
        };
        let Some(entry) = files.selected_entry() else { return };
        let path = files.key_of(&entry.key);
        let title = entry.key.clone();
        tracing::info!("Action: Play [{}]", path);
        if let Some(old) = self.app.player.take() {
            old.stop();
        }
        self.app.player = Some(crate::player::open(
            self.engine.clone(),
            self.rt.clone(),
            storage_id,
            path,
            title,
            extension,
        ));
        self.app.mode = AppMode::Player;
    }

    fn close_player(&mut self) {
        if let Some(player) = self.app.player.take() {
            player.stop();
        }
        self.app.mode = AppMode::Normal;
    }

    fn handle_player_key(&mut self, key: KeyEvent) {
        let Some(player) = self.app.player.as_ref() else {
            self.app.mode = AppMode::Normal;
            return;
        };
        match key.code {
            KeyCode::Esc | KeyCode::F(10) | KeyCode::Char('q') => {
                self.close_player();
            }
            KeyCode::Char(' ') => player.toggle_pause(),
            KeyCode::Left => player.seek_by(-10),
            KeyCode::Right => player.seek_by(10),
            KeyCode::Up => player.volume_step(true),
            KeyCode::Down => player.volume_step(false),
            _ => {}
        }
    }

    // ── tetris ─────────────────────────────────────────────────────────────

    /// The Dos Navigator tribute: a fresh game, full screen.
    fn open_tetris(&mut self) {
        tracing::info!("Action: Tetris");
        self.app.tetris = Some(crate::tetris::Tetris::new());
        self.app.mode = AppMode::Tetris;
    }

    fn handle_tetris_key(&mut self, key: KeyEvent) {
        let Some(game) = self.app.tetris.as_mut() else {
            self.app.mode = AppMode::Normal;
            return;
        };
        match key.code {
            KeyCode::Esc | KeyCode::F(10) | KeyCode::Char('q') => {
                tracing::info!("Action: Tetris: quit (score {})", game.score);
                self.app.tetris = None;
                self.app.mode = AppMode::Normal;
            }
            KeyCode::Left => game.move_left(),
            KeyCode::Right => game.move_right(),
            KeyCode::Up => game.rotate(),
            KeyCode::Down => game.soft_drop(),
            KeyCode::Char(' ') => game.hard_drop(),
            KeyCode::Char('p') | KeyCode::Char('P') => game.toggle_pause(),
            KeyCode::Enter if game.phase == crate::tetris::Phase::GameOver => {
                *game = crate::tetris::Tetris::new();
            }
            _ => {}
        }
    }

    /// Per-draw spectrum update while the player dialog is up, and the
    /// auto-advance: a track that ended cleanly hands over to the next audio
    /// file in the same directory (panel order, no wrap-around).
    fn player_tick(&mut self) {
        if self.app.mode != AppMode::Player {
            return;
        }
        let ended = self
            .app
            .player
            .as_ref()
            .is_some_and(|p| p.shared.is_done() && p.shared.error().is_none());
        if ended {
            self.play_next_in_dir();
        }
        if let Some(player) = self.app.player.as_mut() {
            player.tick();
        }
    }

    /// The next playable entry after the one that just finished, found by
    /// name in the panel the player was opened from (the listing can't
    /// navigate while the modal is up). The cursor follows the track, so a
    /// closed player leaves the selection on what played last. At the end of
    /// the directory the dialog just stays on "Finished".
    fn play_next_in_dir(&mut self) {
        let Some(current) = self.app.player.as_ref().map(|p| p.title.clone()) else {
            return;
        };
        let Some(files) = self.app.active_files() else { return };
        let Some(pos) = files.entries.iter().position(|e| e.key == current) else {
            return;
        };
        let next = files.entries[pos + 1..].iter().enumerate().find_map(|(i, e)| {
            if !is_file(e) {
                return None;
            }
            crate::player::playable_extension(&e.key).map(|ext| (pos + 1 + i, ext))
        });
        let Some((idx, ext)) = next else {
            return;
        };
        tracing::info!("Action: Play next [{}]", files.entries[idx].key);
        self.app.set_active_cursor(idx);
        self.open_player_selected(ext);
    }

    // ── viewer ─────────────────────────────────────────────────────────────

    /// F3: the selected file in the built-in viewer; on a folder, its total
    /// size is computed into the size column (FAR). Storage keys are
    /// root-relative (no leading `/`) for local and share panels alike.
    fn open_viewer_selected(&mut self) {
        let panel_id = self.app.active_panel_id();
        let Some(files) = self.app.active_files() else { return };
        let Some(entry) = files.selected_entry() else { return };
        if is_dir(entry) {
            if entry.key == ".." {
                return;
            }
            let Some(storage_id) = files.storage_id().map(str::to_string) else {
                return;
            };
            tracing::info!("Action: Size [{}]", files.key_of(&entry.key));
            self.req_dir_size(panel_id, storage_id, files.current_path.clone(), entry.key.clone());
            return;
        }
        if !is_file(entry) {
            return;
        }
        let Some(storage_id) = files.storage_id().map(str::to_string) else {
            return;
        };
        let path = files.key_of(&entry.key);
        let title = entry.key.clone();
        let local = files.is_local();
        tracing::info!("Action: View [{}]", path);
        self.open_viewer(storage_id, path, local, title);
    }

    /// Open the viewer on a storage key. On a local storage the size is
    /// fetched synchronously (the UI thread is off the runtime, so blocking
    /// on the engine is fine and the disk answers at once); a share file's
    /// size comes back as `ViewerOpened`. Then the draw pulls the blocks the
    /// viewport needs.
    fn open_viewer(&mut self, storage_id: String, path: String, local: bool, title: String) {
        if let Some(mut old) = self.app.viewer.take() {
            old.abort_search();
        }
        self.app.viewer_seq += 1;
        let seq = self.app.viewer_seq;
        let mut viewer = ViewerState::new(seq, storage_id.clone(), path.clone(), local, title);
        self.app.mode = AppMode::Viewer;
        if local {
            match self.rt.block_on(self.engine.file_size(&storage_id, &path)) {
                Ok(size) => viewer.set_size(size),
                Err(e) => {
                    tracing::error!(error = %e, path = %path, "viewer: open failed");
                    viewer.error = Some(e);
                }
            }
            self.app.viewer = Some(viewer);
            return;
        }
        self.app.viewer = Some(viewer);
        let engine = self.engine.clone();
        self.spawn(async move {
            let result = engine.file_size(&storage_id, &path).await;
            UiEvent::ViewerOpened { seq, result }
        });
    }

    fn close_viewer(&mut self) {
        if let Some(mut v) = self.app.viewer.take() {
            v.abort_search();
        }
        // Back to the find results the viewer was opened from, if any.
        self.app.mode = if self.app.find.is_some() {
            AppMode::FindResults
        } else {
            AppMode::Normal
        };
    }

    /// Before every draw while a viewer is open: size the viewport, lay the
    /// rows out, and fetch the block the layout is missing (or one around
    /// the viewport). Answers come back as `ViewerBlock`.
    fn viewer_tick(&mut self, size: ratatui::layout::Rect) {
        let Some(v) = self.app.viewer.as_mut() else { return };
        // The header line and the key bar bracket the rows.
        v.set_viewport(size.height.saturating_sub(2) as usize, size.width as usize);
        let wanted = v.plan_requests();
        if wanted.is_empty() {
            return;
        }
        let seq = v.seq;
        let storage_id = v.storage_id.clone();
        let path = v.path.clone();
        // A local storage: fetch the blocks through the engine now, so this
        // very frame has them.
        if v.local {
            for idx in wanted {
                let result = self.rt.block_on(self.engine.read_range(
                    &storage_id,
                    &path,
                    idx * viewer::BLOCK_SIZE,
                    viewer::BLOCK_SIZE,
                ));
                if let Some(v) = self.app.viewer.as_mut() {
                    v.on_block(idx, result);
                }
            }
            return;
        }
        for idx in wanted {
            let engine = self.engine.clone();
            let storage_id = storage_id.clone();
            let path = path.clone();
            self.spawn(async move {
                let result = engine
                    .read_range(&storage_id, &path, idx * viewer::BLOCK_SIZE, viewer::BLOCK_SIZE)
                    .await;
                UiEvent::ViewerBlock { seq, idx, result }
            });
        }
    }

    fn handle_viewer_key(&mut self, key: KeyEvent) {
        let Some(v) = self.app.viewer.as_mut() else {
            self.app.mode = AppMode::Normal;
            return;
        };
        let page = v.rows().max(1);
        match key.code {
            // Esc first stops a running search; the next one closes.
            KeyCode::Esc if v.search_running => {
                tracing::info!("Action: Viewer: Cancel search");
                v.abort_search();
                v.status = Some("Search cancelled".to_string());
            }
            KeyCode::Esc | KeyCode::F(3) | KeyCode::F(10) | KeyCode::Char('q') => {
                tracing::info!("Action: Viewer: Close");
                self.close_viewer();
            }
            KeyCode::F(1) => {
                self.app.mode = AppMode::HelpDialog;
            }
            KeyCode::F(2) => {
                tracing::info!("Action: Viewer: {}", v.wrap_label());
                v.cycle_wrap();
            }
            KeyCode::F(4) => {
                tracing::info!("Action: Viewer: Toggle hex");
                v.toggle_hex();
            }
            KeyCode::F(5) => {
                self.app.dialog_state.viewer_input.clear();
                self.app.dialog_state.viewer_input_focus = 0;
                self.app.mode = AppMode::ViewerGotoDialog;
            }
            KeyCode::F(7) if key.modifiers.contains(KeyModifiers::SHIFT) => {
                self.viewer_search_next();
            }
            KeyCode::F(7) => {
                let last = v
                    .needle
                    .as_ref()
                    .map(|n| String::from_utf8_lossy(n).into_owned())
                    .unwrap_or_default();
                self.app.dialog_state.viewer_input.set(last);
                self.app.dialog_state.viewer_input_focus = 0;
                self.app.mode = AppMode::ViewerSearchDialog;
            }
            KeyCode::Char('n') => {
                self.viewer_search_next();
            }
            KeyCode::Up | KeyCode::Char('k') => v.apply(Move::Up(1)),
            KeyCode::Down | KeyCode::Char('j') => v.apply(Move::Down(1)),
            KeyCode::PageUp => v.apply(Move::Up(page)),
            KeyCode::PageDown | KeyCode::Char(' ') => v.apply(Move::Down(page)),
            KeyCode::Home => v.apply(Move::Home),
            KeyCode::End => v.apply(Move::End),
            KeyCode::Left => v.scroll_left(),
            KeyCode::Right => v.scroll_right(),
            _ => {}
        }
    }

    /// The goto / search prompt: text field, OK, Cancel.
    fn handle_viewer_input_key(&mut self, key: KeyEvent) {
        let focus = self.app.dialog_state.viewer_input_focus;
        match key.code {
            KeyCode::Esc => {
                self.app.mode = AppMode::Viewer;
            }
            KeyCode::Tab => {
                self.app.dialog_state.viewer_input_focus = (focus + 1) % 3;
            }
            KeyCode::Enter => {
                if focus == 2 {
                    self.app.mode = AppMode::Viewer;
                } else {
                    self.submit_viewer_input();
                }
            }
            _ if focus == 0 && self.app.dialog_state.viewer_input.handle_key(key) => {}
            _ => {}
        }
    }

    fn submit_viewer_input(&mut self) {
        let input = self.app.dialog_state.viewer_input.text.clone();
        let prompt = self.app.mode;
        self.app.mode = AppMode::Viewer;
        let Some(v) = self.app.viewer.as_mut() else { return };
        match prompt {
            AppMode::ViewerGotoDialog => {
                let Some(size) = v.size else {
                    v.status = Some("Still opening".to_string());
                    return;
                };
                match viewer::parse_goto(&input, size) {
                    Ok(off) => {
                        tracing::info!("Action: Viewer: Goto {}", off);
                        v.apply(Move::Goto(off));
                    }
                    Err(e) => v.status = Some(e),
                }
            }
            AppMode::ViewerSearchDialog => {
                if input.is_empty() {
                    v.status = Some("Empty search string".to_string());
                    return;
                }
                tracing::info!("Action: Viewer: Search");
                v.needle = Some(input.into_bytes());
                // A fresh search starts at the top of the view.
                v.match_range = None;
                self.viewer_search_next();
            }
            _ => {}
        }
    }

    /// Scan forward from past the current match (or the top of the view)
    /// for the last search string, on the runtime; progress and the outcome
    /// arrive as `ViewerSearch`.
    fn viewer_search_next(&mut self) {
        let Some(v) = self.app.viewer.as_mut() else { return };
        let Some(needle) = v.needle.clone() else {
            v.status = Some("No search string (F7)".to_string());
            return;
        };
        if v.search_running {
            return;
        }
        let Some(size) = v.size else {
            v.status = Some("Still opening".to_string());
            return;
        };
        let from = v.search_from();
        // The formatted text is all in memory: searched right here.
        if v.is_formatted() {
            match v.find_forward(from, &needle) {
                Some(off) => {
                    v.status = None;
                    v.on_found(off, needle.len());
                }
                None => v.status = Some("Not found".to_string()),
            }
            return;
        }
        let seq = v.seq;
        let storage_id = v.storage_id.clone();
        let path = v.path.clone();
        let engine = self.engine.clone();
        let tx = self.tx.clone();
        let handle = self.rt.spawn(async move {
            let progress_tx = tx.clone();
            let res = engine
                .search_forward(&storage_id, &path, from, size, &needle, move |off| {
                    let _ = progress_tx.send(UiEvent::ViewerSearch {
                        seq,
                        outcome: Ok(SearchOutcome::Progress(off)),
                    });
                })
                .await;
            let outcome = res.map(|hit| match hit {
                Some(off) => SearchOutcome::Found(off),
                None => SearchOutcome::NotFound,
            });
            let _ = tx.send(UiEvent::ViewerSearch { seq, outcome });
        });
        if let Some(v) = self.app.viewer.as_mut() {
            v.search_started(handle.abort_handle());
            v.status = Some("Searching…".to_string());
        }
    }

    // ── editor ─────────────────────────────────────────────────────────────

    /// F4: the selected local file in the built-in editor (replaces the old
    /// mcedit shell-out). Local storages only — a share file is immutable
    /// content with no write-back, F3 views it.
    fn open_editor_selected(&mut self) {
        let Some(files) = self.app.active_files() else { return };
        if !files.is_local() {
            self.app.active_panel_mut().status_message = Some("Edit is only available for local files".to_string());
            return;
        }
        let Some(root) = self.local_panel_root() else { return };
        let Some(files) = self.app.active_files() else { return };
        let Some(entry) = files.selected_entry() else { return };
        if !is_file(entry) {
            return;
        }
        let mut abs = root;
        if !files.current_path.is_empty() {
            abs = abs.join(&files.current_path);
        }
        abs = abs.join(&entry.key);
        let title = entry.key.clone();
        tracing::info!("Action: Edit [{}]", abs.display());
        match crate::editor::load(abs, title) {
            Ok(state) => {
                self.app.editor = Some(state);
                self.app.mode = AppMode::Editor;
            }
            Err(e) => {
                self.app.active_panel_mut().status_message = Some(e);
            }
        }
    }

    /// The edited file's size/mtime changed on disk — refresh the panel on
    /// the way out, like the external-editor path did.
    fn close_editor(&mut self) {
        self.app.editor = None;
        self.app.mode = AppMode::Normal;
        self.refresh_active_panel();
    }

    /// F2. On failure the editor stays up with the error on its header line.
    fn save_editor(&mut self) -> bool {
        let Some(editor) = self.app.editor.as_mut() else {
            return false;
        };
        match editor.save() {
            Ok(()) => true,
            Err(e) => {
                tracing::error!(error = %e, "editor: save failed");
                editor.error = Some(e);
                false
            }
        }
    }

    /// Run `op` on the open editor's buffer; its `true` marks the buffer
    /// modified.
    fn edit_buffer(&mut self, op: impl FnOnce(&mut crate::editor::EditorState) -> bool) {
        if let Some(ed) = self.app.editor.as_mut()
            && op(ed)
        {
            ed.modified = true;
        }
    }

    /// The editor's F7 / Alt+L prompt.
    fn open_editor_prompt(&mut self, mode: AppMode) {
        self.app.dialog_state.viewer_input.clear();
        self.app.dialog_state.viewer_input_focus = 0;
        self.app.mode = mode;
    }

    /// Esc / F10 / menu Close: straight out, or the unsaved-changes guard.
    fn request_close_editor(&mut self) {
        let Some(editor) = self.app.editor.as_ref() else { return };
        if editor.modified {
            self.app.dialog_state.editor_close_focus = 0;
            self.app.mode = AppMode::EditorCloseConfirmDialog;
        } else {
            self.close_editor();
        }
    }

    /// A terminal paste: into the editor whole.
    fn handle_paste(&mut self, text: &str) {
        if self.app.mode == AppMode::Editor {
            self.edit_buffer(|ed| ed.paste_text(text));
        }
    }

    fn handle_editor_key(&mut self, key: KeyEvent) {
        if self.app.editor.is_none() {
            self.app.mode = AppMode::Normal;
            return;
        }
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        let shift = key.modifiers.contains(KeyModifiers::SHIFT);
        let alt = key.modifiers.contains(KeyModifiers::ALT);
        match key.code {
            KeyCode::Esc | KeyCode::F(10) => self.request_close_editor(),
            KeyCode::F(2) => {
                self.save_editor();
            }
            KeyCode::F(7) => self.open_editor_prompt(AppMode::EditorSearchDialog),
            KeyCode::F(9) => {
                tracing::info!("Action: Editor menu");
                self.app.dialog_state.menu_kind = MenuBarKind::Editor;
                self.app.dialog_state.menu_open = 0;
                self.app.dialog_state.menu_item = 0;
                self.app.mode = AppMode::EditorMenu;
            }
            // Next/previous match for the last F7 pattern.
            KeyCode::Char('n') if alt => self.edit_buffer(|ed| {
                ed.textarea.search_forward(false);
                false
            }),
            KeyCode::Char('p') if alt => self.edit_buffer(|ed| {
                ed.textarea.search_back(false);
                false
            }),
            KeyCode::Char('l') if alt => self.open_editor_prompt(AppMode::EditorGotoDialog),
            // FAR's selection and clipboard keys, ahead of the widget's own
            // (emacs-flavoured) bindings for the same keys.
            KeyCode::Char('a') if ctrl => self.edit_buffer(|ed| {
                ed.textarea.select_all();
                false
            }),
            KeyCode::Char('u') if ctrl => self.edit_buffer(|ed| {
                ed.textarea.cancel_selection();
                false
            }),
            KeyCode::Insert if ctrl => self.edit_buffer(|ed| {
                ed.copy();
                false
            }),
            KeyCode::Char('c') if ctrl => self.edit_buffer(|ed| {
                ed.copy();
                false
            }),
            KeyCode::Insert if shift => self.edit_buffer(|ed| ed.paste()),
            KeyCode::Char('v') if ctrl => self.edit_buffer(|ed| ed.paste()),
            KeyCode::Delete if shift => self.edit_buffer(|ed| ed.cut()),
            KeyCode::Char('x') if ctrl => self.edit_buffer(|ed| ed.cut()),
            KeyCode::Char('z' | 'Z') if ctrl && shift => self.edit_buffer(|ed| ed.textarea.redo()),
            KeyCode::Char('z') if ctrl => self.edit_buffer(|ed| ed.textarea.undo()),
            KeyCode::Char('y') if ctrl => self.edit_buffer(|ed| ed.delete_line()),
            // Everything else is the buffer's: typing, movement, Shift+movement
            // selection, undo/redo (Ctrl+U/R), copy/cut/paste (Ctrl+C/X/Y).
            _ => self.edit_buffer(|ed| ed.textarea.input(key)),
        }
    }

    fn handle_editor_input_key(&mut self, key: KeyEvent) {
        let focus = self.app.dialog_state.viewer_input_focus;
        match key.code {
            KeyCode::Esc => {
                self.app.mode = AppMode::Editor;
            }
            KeyCode::Tab => {
                self.app.dialog_state.viewer_input_focus = (focus + 1) % 3;
            }
            KeyCode::Enter => {
                if focus == 2 {
                    self.app.mode = AppMode::Editor;
                } else {
                    self.submit_editor_input();
                }
            }
            _ if focus == 0 && self.app.dialog_state.viewer_input.handle_key(key) => {}
            _ => {}
        }
    }

    fn submit_editor_input(&mut self) {
        let input = self.app.dialog_state.viewer_input.text.clone();
        let prompt = self.app.mode;
        self.app.mode = AppMode::Editor;
        let Some(editor) = self.app.editor.as_mut() else { return };
        match prompt {
            AppMode::EditorSearchDialog => match editor.textarea.set_search_pattern(&input) {
                Ok(()) => {
                    editor.textarea.search_forward(true);
                }
                Err(e) => {
                    editor.error = Some(format!("bad pattern: {e}"));
                }
            },
            AppMode::EditorGotoDialog => {
                if let Ok(line) = input.trim().parse::<u16>() {
                    editor
                        .textarea
                        .move_cursor(tui_textarea::CursorMove::Jump(line.saturating_sub(1), 0));
                }
            }
            _ => {}
        }
    }

    fn handle_editor_close_confirm_key(&mut self, key: KeyEvent) {
        let focus = self.app.dialog_state.editor_close_focus;
        match key.code {
            KeyCode::Esc => {
                self.app.mode = AppMode::Editor;
            }
            KeyCode::Left | KeyCode::BackTab => {
                self.app.dialog_state.editor_close_focus = (focus + 2) % 3;
            }
            KeyCode::Right | KeyCode::Tab => {
                self.app.dialog_state.editor_close_focus = (focus + 1) % 3;
            }
            KeyCode::Enter => match focus {
                // Save — close only when the write actually succeeded.
                0 => {
                    if self.save_editor() {
                        self.close_editor();
                    } else {
                        self.app.mode = AppMode::Editor;
                    }
                }
                // Discard.
                1 => {
                    self.close_editor();
                }
                // Cancel.
                _ => {
                    self.app.mode = AppMode::Editor;
                }
            },
            _ => {}
        }
    }

    /// F6: the new name for the cursor's entry, local storages only (a
    /// share is read-only).
    fn prepare_rename(&mut self) {
        let Some(files) = self.app.active_files() else { return };
        if !files.is_local() {
            self.app.active_panel_mut().status_message = Some("Rename is only available on local storages".to_string());
            return;
        }
        let Some(entry) = files.selected_entry() else { return };
        if entry.key == ".." {
            return;
        }
        let name = entry.key.clone();
        tracing::info!("Action: Rename [{}]", name);
        self.app.dialog_state.rename.set(name.clone());
        self.app.dialog_state.rename_from = name;
        self.app.dialog_state.rename_focus = 0;
        self.app.mode = AppMode::RenameDialog;
    }

    fn handle_rename_key(&mut self, key: KeyEvent) {
        let focus = self.app.dialog_state.rename_focus;
        match key.code {
            KeyCode::Esc => {
                self.app.mode = AppMode::Normal;
            }
            KeyCode::Tab => {
                self.app.dialog_state.rename_focus = (focus + 1) % 3;
            }
            KeyCode::Enter => {
                if focus == 2 {
                    self.app.mode = AppMode::Normal;
                } else {
                    self.submit_rename();
                }
            }
            _ if focus == 0 && self.app.dialog_state.rename.handle_key(key) => {}
            _ => {}
        }
    }

    fn submit_rename(&mut self) {
        let from = self.app.dialog_state.rename_from.clone();
        let to = self.app.dialog_state.rename.text.trim().to_string();
        if to.is_empty() || to == from {
            self.app.mode = AppMode::Normal;
            return;
        }
        let Some(files) = self.app.active_files() else { return };
        let Some(storage_id) = files.storage_id().map(str::to_string) else {
            return;
        };
        let (from_key, to_key) = (files.key_of(&from), files.key_of(&to));
        tracing::info!("Action: Rename [{}] -> [{}]", from_key, to_key);
        self.req_rename(storage_id, from_key, to_key);
    }

    /// F9 menu: ←/→ switch menus, ↑/↓ walk the rows (separators skipped),
    /// Enter picks, Esc / F9 closes.
    fn handle_menu_key(&mut self, key: KeyEvent) {
        let kind = self.app.dialog_state.menu_kind;
        let open = self.app.dialog_state.menu_open;
        let items = menu::items(&self.app, kind, open);
        let item = self.app.dialog_state.menu_item;
        let count = menu::titles(kind).len();
        // Where closing the bar lands: the screen it was opened over.
        let back = match kind {
            MenuBarKind::Panels => AppMode::Normal,
            MenuBarKind::Editor => AppMode::Editor,
        };
        match key.code {
            KeyCode::Esc | KeyCode::F(9) => {
                self.app.mode = back;
            }
            KeyCode::Left => {
                self.app.dialog_state.menu_open = (open + count - 1) % count;
                self.app.dialog_state.menu_item = 0;
            }
            KeyCode::Right => {
                self.app.dialog_state.menu_open = (open + 1) % count;
                self.app.dialog_state.menu_item = 0;
            }
            KeyCode::Up => {
                let mut i = item;
                while i > 0 {
                    i -= 1;
                    if items[i].action.is_some() {
                        self.app.dialog_state.menu_item = i;
                        break;
                    }
                }
            }
            KeyCode::Down => {
                let mut i = item;
                while i + 1 < items.len() {
                    i += 1;
                    if items[i].action.is_some() {
                        self.app.dialog_state.menu_item = i;
                        break;
                    }
                }
            }
            KeyCode::Enter => {
                if let Some(action) = items.get(item).and_then(|i| i.action) {
                    self.app.mode = back;
                    self.apply_menu_action(action);
                }
            }
            _ => {}
        }
    }

    fn apply_menu_action(&mut self, action: MenuAction) {
        tracing::info!(?action, "Action: Menu");
        match action {
            MenuAction::View(id, view) => {
                self.app.panel_mut(id).view = view;
                // The capacity changed: the cursor may need a new scroll.
                self.app.clamp_cursors();
                self.save_state();
            }
            MenuAction::Sort(id, sort) => {
                let panel = self.app.panel_mut(id);
                panel.sort = sort;
                let (visible, step) = (panel.visible_height, panel.step());
                self.save_state();
                let Some(files) = self.app.panel_mut(id).files_mut() else {
                    return;
                };
                let current = files.selected_entry().map(|e| e.key.clone());
                files.sort_entries(sort);
                if let Some(idx) = current.and_then(|k| files.entries.iter().position(|e| e.key == k)) {
                    files.cursor_set(idx, visible, step);
                }
                // The storage's own order is only had by listing again.
                if sort == crate::app::SortMode::Unsorted {
                    self.request_listing(id);
                }
            }
            MenuAction::Tetris => self.open_tetris(),
            MenuAction::FileView => self.open_viewer_selected(),
            MenuAction::FileEdit => self.open_editor_selected(),
            MenuAction::FileCopy => self.prepare_copy(),
            MenuAction::FileRename => self.prepare_rename(),
            MenuAction::FileFind => self.open_find_dialog(),
            MenuAction::EditorSave => {
                self.save_editor();
            }
            MenuAction::EditorClose => self.request_close_editor(),
            MenuAction::EditorUndo => self.edit_buffer(|ed| ed.textarea.undo()),
            MenuAction::EditorRedo => self.edit_buffer(|ed| ed.textarea.redo()),
            MenuAction::EditorCut => self.edit_buffer(|ed| ed.cut()),
            MenuAction::EditorCopy => self.edit_buffer(|ed| {
                ed.copy();
                false
            }),
            MenuAction::EditorPaste => self.edit_buffer(|ed| ed.paste()),
            MenuAction::EditorDeleteLine => self.edit_buffer(|ed| ed.delete_line()),
            MenuAction::EditorSelectAll => self.edit_buffer(|ed| {
                ed.textarea.select_all();
                false
            }),
            MenuAction::EditorUnselect => self.edit_buffer(|ed| {
                ed.textarea.cancel_selection();
                false
            }),
            MenuAction::EditorFind => self.open_editor_prompt(AppMode::EditorSearchDialog),
            MenuAction::EditorFindNext => self.edit_buffer(|ed| {
                ed.textarea.search_forward(false);
                false
            }),
            MenuAction::EditorFindPrev => self.edit_buffer(|ed| {
                ed.textarea.search_back(false);
                false
            }),
            MenuAction::EditorGoto => self.open_editor_prompt(AppMode::EditorGotoDialog),
        }
    }

    /// F5: the confirm dialog for what the panel pair means — files onto a
    /// file list copy; onto the Nearby panel they are offered to a device;
    /// onto the Links panel they become a new share.
    fn prepare_copy(&mut self) {
        let Some(src) = self.app.active_files() else { return };
        if src.storage.is_none() {
            return;
        }
        // Use selection if non-empty, else current item
        let source_name = if !src.marked.is_empty() {
            let count = src.marked.len();
            if count == 1 {
                src.marked.iter().next().expect("one marked").clone()
            } else {
                format!("{} items", count)
            }
        } else if let Some(entry) = src.selected_entry() {
            if entry.key == ".." {
                return;
            }
            entry.key.clone()
        } else {
            return;
        };
        let src_is_local = src.is_local();
        let dest_id = self.app.inactive_panel_id();

        self.app.dialog_state.offer_target = None;
        let dest_dir = match self.app.inactive_panel().content.kind() {
            ContentKind::Files => {
                let Some(dest) = self.app.inactive_panel().files() else {
                    return;
                };
                if dest.storage.is_none() {
                    return;
                }
                dest.display_path()
            }
            ContentKind::Nearby => {
                let Some((agent_id, device)) = self.app.nearby_target(dest_id) else {
                    self.app.active_panel_mut().status_message = Some("Select a device to send to".to_string());
                    return;
                };
                if !src_is_local {
                    self.app.active_panel_mut().status_message =
                        Some("Only local files can be sent to a device".to_string());
                    return;
                }
                self.app.dialog_state.offer_target = Some((agent_id, device.clone()));
                format!("Nearby: {}", device)
            }
            ContentKind::Links => "Links (new share)".to_string(),
            ContentKind::Connection | ContentKind::User | ContentKind::Log | ContentKind::Info => {
                self.app.active_panel_mut().status_message = Some("The other panel is not a copy target".to_string());
                return;
            }
        };

        tracing::info!("Action: Copy [{}] -> [{}]", source_name, dest_dir);
        self.app.dialog_state.copy_source_name = source_name;
        self.app.dialog_state.copy_dest_path = dest_dir;
        self.app.dialog_state.copy_ok_selected = true;
        self.app.mode = AppMode::CopyConfirmDialog;
    }

    fn start_copy(&mut self) {
        let Some(src) = self.app.active_files() else { return };
        let Some(src_sid) = src.storage_id().map(str::to_string) else {
            return;
        };
        let current_path = src.current_path.clone();
        // Build list of source paths: selection if non-empty, else current entry
        let source_paths: Vec<String> = if !src.marked.is_empty() {
            src.entries
                .iter()
                .filter(|e| src.marked.contains(&e.key))
                .map(|e| src.key_of(&e.key))
                .collect()
        } else if let Some(entry) = src.selected_entry() {
            if entry.key == ".." {
                return;
            }
            vec![src.key_of(&entry.key)]
        } else {
            return;
        };
        if source_paths.is_empty() {
            return;
        }
        // `None`: the Nearby panel — an offer, not a copy.
        let dest = match &self.app.inactive_panel().content {
            Content::Files(dest) => match dest.storage_id() {
                Some(id) => Some(TransferDest::Storage {
                    storage_id: id.to_string(),
                    path: dest.current_path.clone(),
                }),
                None => return,
            },
            Content::Links(_) => Some(TransferDest::NewShare),
            Content::Nearby(_) => None,
            _ => return,
        };

        // Clear selection after starting copy
        if let Some(files) = self.app.active_files_mut() {
            files.marked.clear();
        }

        // A nearby offer instead of a copy: the selection, by basename, to
        // the device the confirm dialog named.
        if let Some((agent_id, device)) = self.app.dialog_state.offer_target.take() {
            let name = source_paths
                .iter()
                .map(|p| p.rsplit('/').next().unwrap_or(p).to_string())
                .collect::<Vec<_>>()
                .join(", ");
            tracing::info!(agent_id = %agent_id, device = %device, "Action: Nearby offer [{}]", name);
            self.app.mode = AppMode::Normal;
            self.app.active_panel_mut().status_message = Some(format!("Sending offer to {}…", device));
            self.req_send_nearby_offer(src_sid, source_paths, agent_id, name);
            return;
        }
        let Some(dest) = dest else { return };

        self.req_start_transfer(StartTransfer {
            source_storage_id: src_sid,
            source_root: current_path,
            source_paths,
            dest,
            interactive: true,
        });
    }

    fn prepare_delete(&mut self) {
        let panel_id = self.app.active_panel_id();
        // F8 on the Nearby panel withdraws the offer to the device.
        if matches!(self.app.active_panel().content, Content::Nearby(_)) {
            let Some((agent_id, device)) = self.app.nearby_target(panel_id) else {
                return;
            };
            tracing::info!(agent_id = %agent_id, "Action: Cancel nearby offer");
            self.app.dialog_state.cancel_offer_agent = Some(agent_id);
            self.app.dialog_state.delete_name = format!("the offer to {}", device);
            self.app.dialog_state.delete_is_dir = false;
            self.app.dialog_state.delete_yes_selected = true;
            self.app.mode = AppMode::DeleteConfirmDialog;
            return;
        }
        self.app.dialog_state.cancel_offer_agent = None;
        // F8 on the Links panel deletes the link.
        if let Content::Links(list) = &self.app.active_panel().content {
            let Some(link) = list.selected().cloned() else { return };
            tracing::info!(token = %link.token, "Action: Delete link [{}]", link.name);
            self.app.dialog_state.delete_link_token = Some(link.token);
            self.app.dialog_state.delete_name = link.name;
            self.app.dialog_state.delete_is_dir = false;
            self.app.dialog_state.delete_yes_selected = true;
            self.app.mode = AppMode::DeleteConfirmDialog;
            return;
        }
        self.app.dialog_state.delete_link_token = None;
        let Some(files) = self.app.active_files() else { return };
        let Some(entry) = files.selected_entry() else { return };
        if entry.key == ".." {
            return;
        }
        let name = entry.key.clone();
        let is_dir = is_dir(entry);
        tracing::info!("Action: Delete [{}]", files.key_of(&name));
        self.app.dialog_state.delete_name = name;
        self.app.dialog_state.delete_is_dir = is_dir;
        self.app.dialog_state.delete_yes_selected = true;
        self.app.mode = AppMode::DeleteConfirmDialog;
    }

    fn do_delete(&mut self) {
        if let Some(agent_id) = self.app.dialog_state.cancel_offer_agent.take() {
            self.app.mode = AppMode::Normal;
            self.req_cancel_nearby_offer(agent_id);
            return;
        }
        if let Some(token) = self.app.dialog_state.delete_link_token.take() {
            self.app.mode = AppMode::Deleting;
            self.req_delete_link(token);
            return;
        }
        let Some(files) = self.app.active_files() else { return };
        let Some(storage_id) = files.storage_id().map(str::to_string) else {
            return;
        };
        let path = files.key_of(&self.app.dialog_state.delete_name);
        self.app.mode = AppMode::Deleting;
        self.req_delete(storage_id, path);
    }

    fn do_mkdir(&mut self) {
        let Some(files) = self.app.active_files() else { return };
        let Some(storage_id) = files.storage_id().map(str::to_string) else {
            return;
        };
        let name = self.app.dialog_state.mkdir.text.clone();
        if name.is_empty() {
            return;
        }
        let path = files.key_of(&name);
        self.req_mkdir(storage_id, path);
    }

    fn submit_add_storage(&mut self) {
        let name = self.app.dialog_state.input_name.clone();
        let url = self.app.dialog_state.input_url.clone();

        if url.is_empty() {
            return;
        }

        // A URL opens a share; anything else is a local directory.
        let (display_name, request) = if url.starts_with("http") || url.starts_with("shareninja://") {
            let display = if name.is_empty() {
                let token = url
                    .trim_end_matches('/')
                    .rsplit('/')
                    .next()
                    .unwrap_or("")
                    .split('#')
                    .next()
                    .unwrap_or("");
                if token.is_empty() {
                    "Share.Ninja".to_string()
                } else {
                    format!("Share.Ninja ({})", token)
                }
            } else {
                name
            };
            (display, StorageConnectRequest::Ninja { link_url: url })
        } else {
            let display = if name.is_empty() { url.clone() } else { name };
            (display, StorageConnectRequest::Local { root_path: url })
        };

        // The panel the dialog was opened for (`a` = the active one, the
        // selector's `a` = the selector's panel)
        let panel = self.app.dialog_state.storage_target;
        {
            let p = self.app.panel_mut(panel);
            p.stash = None;
            let files = p.ensure_files();
            files.current_path = String::new();
            files.opened_from = None;
            files.marked.clear();
        }
        self.request_connect_storage(panel, display_name, request);
    }
}

/// The selector hotkey of a content kind: its digit, in `OPENABLE` order.
fn kind_hotkey(kind: ContentKind) -> Option<char> {
    ContentKind::OPENABLE
        .iter()
        .position(|k| *k == kind)
        .and_then(|i| char::from_digit(i as u32 + 1, 10))
}

/// The selector hotkey of a volume: the Windows drive letter; none for a
/// path-rooted volume.
fn drive_hotkey(drive: &crate::types::Drive) -> Option<char> {
    let mut chars = drive.root.chars();
    match (chars.next(), chars.next()) {
        (Some(letter), Some(':')) if letter.is_ascii_alphabetic() => Some(letter.to_ascii_uppercase()),
        _ => None,
    }
}

/// One panel slot, by content.
fn render_panel(frame: &mut ratatui::Frame, app: &App, id: PanelId, area: Rect) {
    let panel = app.panel(id);
    let is_active = app.active_panel_id() == id;
    let side = match id {
        PanelId::Left => "Left",
        PanelId::Right => "Right",
    };
    match &panel.content {
        Content::Files(_) | Content::Links(_) => frame.render_widget(ListPanel::for_panel(app, id, side), area),
        Content::Nearby(list) => match &list.open {
            None => frame.render_widget(ListPanel::for_panel(app, id, side), area),
            Some(agent_id) => {
                let (title, rows) = match app.nearby_details(agent_id) {
                    Some(rows) => {
                        let name = app.nearby_device(agent_id).map(|d| d.name.as_str()).unwrap_or(agent_id);
                        (format!("Nearby - /{}", name), rows)
                    }
                    None => ("Nearby".to_string(), vec![("Device", "no longer nearby".to_string())]),
                };
                frame.render_widget(
                    DetailsPanel {
                        panel,
                        is_active,
                        title,
                        rows,
                    },
                    area,
                );
            }
        },
        Content::Connection => frame.render_widget(
            ConnectionPane::new(app.backend_status.as_ref(), &app.vault_snapshot, is_active),
            area,
        ),
        Content::User => frame.render_widget(UserPane::new(app.backend_status.as_ref(), is_active), area),
        Content::Log => {
            let mut log = LogPane::new(&app.log_messages, "Log", is_active);
            log.log_path = app.log_path.as_deref();
            log.debug_api = app.debug_api;
            frame.render_widget(log, area);
        }
        Content::Info => {
            let (other_id, other_side) = match id {
                PanelId::Left => (PanelId::Right, "Right"),
                PanelId::Right => (PanelId::Left, "Left"),
            };
            frame.render_widget(
                InfoPanel {
                    other: app.panel(other_id),
                    other_side,
                    is_active,
                },
                area,
            );
        }
    }
}

/// The key bar for the active panel's content.
/// What a fresh level starts with: `..` above the root — or at the root of a
/// list entered from somewhere (the Links panel, an archive's directory),
/// where it leads back there.
fn level_head(files: &FileList) -> Vec<StorageObjectInfo> {
    if files.current_path.is_empty() && files.opened_from.is_none() {
        return Vec::new();
    }
    let mut up = StorageObjectInfo::new("..");
    up.file_type = StorageFileType::Dir;
    vec![up]
}

fn key_labels(content: &Content) -> [(&'static str, &'static str); 10] {
    let (f3, f4, f5, f6, f7, f8) = match content {
        Content::Files(_) => ("View", "Edit", "Copy", "Rename", "Mkdir", "Delete"),
        Content::Nearby(_) => ("", "", "", "", "", "Withdraw"),
        Content::Links(_) => ("", "", "", "", "", "Delete"),
        Content::Connection | Content::User | Content::Log | Content::Info => ("", "", "", "", "", ""),
    };
    [
        (" 1", "Help"),
        (" 2", ""),
        (" 3", f3),
        (" 4", f4),
        (" 5", f5),
        (" 6", f6),
        (" 7", f7),
        (" 8", f8),
        (" 9", "Menu"),
        ("10", "Quit"),
    ]
}

/// Bracketed paste (a terminal paste arrives as one `Event::Paste`) and,
/// where the terminal has it, the kitty keyboard protocol. Raw mode first.
fn enter_input_modes(kitty_keys: bool) -> io::Result<()> {
    let mut stdout = io::stdout();
    execute!(stdout, EnableBracketedPaste)?;
    if kitty_keys {
        execute!(
            stdout,
            PushKeyboardEnhancementFlags(KeyboardEnhancementFlags::DISAMBIGUATE_ESCAPE_CODES)
        )?;
    }
    Ok(())
}

fn leave_input_modes(kitty_keys: bool) {
    let mut stdout = io::stdout();
    if kitty_keys {
        let _ = execute!(stdout, PopKeyboardEnhancementFlags);
    }
    let _ = execute!(stdout, DisableBracketedPaste);
}

impl Drop for EventLoop {
    fn drop(&mut self) {
        leave_input_modes(self.kitty_keys);
        let _ = disable_raw_mode();
        let _ = execute!(self.terminal.backend_mut(), LeaveAlternateScreen);
    }
}

/// Lexical `.`/`..` normalization for absolute paths — no filesystem access,
/// symlinks are NOT resolved (`..` pops the literal parent component).
fn normalize_lexically(path: &std::path::Path) -> std::path::PathBuf {
    use std::path::Component;
    let mut out = std::path::PathBuf::new();
    for c in path.components() {
        match c {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            other => out.push(other),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::normalize_lexically;
    use std::path::{Path, PathBuf};

    #[test]
    fn cd_normalization_is_lexical() {
        assert_eq!(normalize_lexically(Path::new("/a/b/../c/./d")), PathBuf::from("/a/c/d"));
        assert_eq!(normalize_lexically(Path::new("/a/../../..")), PathBuf::from("/"));
        assert_eq!(normalize_lexically(Path::new("/")), PathBuf::from("/"));
        assert_eq!(normalize_lexically(Path::new("/a/./b/")), PathBuf::from("/a/b"));
    }
}
