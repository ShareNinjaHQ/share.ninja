//! The commander (`sninja cmd`) — an MC-like dual-pane file manager over
//! `share-ninja-core`, self-contained: each of the left/right panel slots
//! shows one [`app::Content`] — a file list on a local filesystem
//! (`LocalFs`) or a Share.Ninja share (a warm `TransferDownload` source),
//! the account's links, the nearby devices, or the connection / user / log /
//! info views — copies run on the core transfer engine with interactive
//! existing-entry / error policies, F3 views any file in the built-in pager
//! ([`viewer`]) reading ranges straight off the source — a share file
//! streams, nothing is copied — and the backend session (login, vault
//! trust circle, nearby) is the `sninja` binary's own, through
//! `share-ninja-client`. The binary's `commands/cmd.rs` wires it up.
//!
//! Threading: the ratatui event loop is synchronous and owns an
//! `Arc<Engine>`; it spawns engine calls on the tokio runtime and drains
//! their outcomes — and the engine's own backend-driven events (connection
//! state, WS vault/nearby frames, transfer progress) — off one
//! [`events::UiEvent`] channel each tick.

pub mod app;
pub mod archive;
pub mod editor;
pub mod engine;
pub mod event_loop;
pub mod events;
pub mod format;
pub mod local_path;
pub mod log_capture;
pub mod menu;
pub mod player;
pub mod shell;
pub mod state;
pub mod storage_reader;
pub mod syntax;
pub mod tetris;
pub mod types;
pub mod ui;
pub mod viewer;

pub use app::{App, AppMode, Panel};
pub use engine::Engine;
pub use event_loop::EventLoop;
pub use events::{PanelId, StartTransfer, TransferDest, UiEvent};
pub use log_capture::{LogBuffer, LogCaptureLayer, new_log_buffer};
