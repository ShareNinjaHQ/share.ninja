//! `sninja cmd` — the commander TUI (`share-ninja-commander`) on this
//! binary's session: the left pane opens on the local filesystem, the right
//! one on the share URL given (else local too) — each local pane in the
//! directory it was last in (`state.rs`), else home; the backend registers
//! in the background once the TUI is up.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use tracing::info;

use share_ninja_commander::state::{PanelState, State};
use share_ninja_commander::types::StorageConnectRequest;
use share_ninja_commander::{App, Engine, EventLoop, LogBuffer, PanelId, events};
use share_ninja_core::Back;

use super::common::open_backend;
use crate::config::Config;

pub async fn execute(
    cfg: &Config,
    link_url: Option<String>,
    log_path: Option<String>,
    log_buffer: LogBuffer,
) -> Result<()> {
    // The backend session: wired now (keychain secrets, token persistence),
    // registered in the background once the TUI is up.
    let session = open_backend(cfg).await;

    let (tx, rx) = events::channel();
    let engine = Arc::new(Engine::new(session, tx.clone()));
    engine.install_hooks().await;

    // Register with the backend in the background; the connection state
    // reaches the UI through the engine's state callback.
    {
        let back = engine.back.clone();
        tokio::spawn(async move {
            if let Err(e) = Back::run_register(back).await {
                tracing::warn!(error = %e, "backend registration failed");
            }
        });
    }

    // The state between runs: the skin, each panel's layout, sort and
    // directory — next to config.toml.
    let state_path = Config::app_dir()?.join("commander.toml");
    let state = State::load(&state_path);
    state.apply_skin();

    // Create app - TUI starts immediately
    let mut app = App::new();
    app.log_path = log_path;
    app.state_path = Some(state_path);
    app.debug_api = cfg.log_api;
    for (panel, saved) in [(&mut app.left_panel, &state.left), (&mut app.right_panel, &state.right)] {
        panel.view = saved.view;
        panel.sort = saved.sort;
    }

    // Local panels start where they were last, else at home; each on its
    // directory's volume (`/`, or the drive).
    let left = LocalStart::of(&state.left);
    let right = LocalStart::of(&state.right);

    // Set initial paths (storages open asynchronously)
    let now = Instant::now();
    app.left_panel.ensure_files().current_path = left.key.clone();
    app.left_panel.last_local_dir = Some(left.dir.clone());
    app.left_panel.loading = true;
    app.left_panel.loading_started = Some(now);

    // Right panel: ninja storage starts at root, local starts where it was
    if link_url.is_none() {
        app.right_panel.ensure_files().current_path = right.key.clone();
        app.right_panel.last_local_dir = Some(right.dir.clone());
    }
    app.right_panel.loading = true;
    app.right_panel.loading_started = Some(now);

    let handle = tokio::runtime::Handle::current();

    // The TUI loop blocks on terminal input — run it off the async workers.
    let ui = tokio::task::spawn_blocking(move || {
        let mut event_loop = EventLoop::new(app, engine, tx, rx, handle, log_buffer)?;

        info!(dir = %left.dir.display(), "Connecting left panel to its volume");
        event_loop.request_connect_storage(
            PanelId::Left,
            left.name,
            StorageConnectRequest::Local { root_path: left.root },
        );

        if let Some(link_url) = &link_url {
            info!(link_url = %link_url, "Connecting right panel to Share.ninja");
            let token = link_url
                .trim_end_matches('/')
                .rsplit('/')
                .next()
                .unwrap_or("")
                .split('#')
                .next()
                .unwrap_or("");
            let ninja_name = if token.is_empty() {
                "Share.Ninja".to_string()
            } else {
                format!("Share.Ninja ({})", token)
            };
            event_loop.request_connect_storage(
                PanelId::Right,
                ninja_name,
                StorageConnectRequest::Ninja {
                    link_url: link_url.clone(),
                },
            );
        } else {
            info!(dir = %right.dir.display(), "Connecting right panel to its volume");
            event_loop.request_connect_storage(
                PanelId::Right,
                right.name,
                StorageConnectRequest::Local { root_path: right.root },
            );
        }

        event_loop.run()
    });

    ui.await.context("TUI thread failed")?.context("Error running TUI")?;
    Ok(())
}

/// Where a local panel opens: its saved directory when it still exists,
/// else home — as the volume root the storage is opened on, the key
/// inside it, and the storage's name (`~` for the home volume on unix).
struct LocalStart {
    dir: PathBuf,
    root: String,
    key: String,
    name: String,
}

impl LocalStart {
    fn of(saved: &PanelState) -> Self {
        let home = dirs::home_dir().unwrap_or_else(|| PathBuf::from(std::path::MAIN_SEPARATOR_STR));
        let dir = saved.existing_dir().unwrap_or_else(|| home.clone());
        let (root, key) = share_ninja_commander::local_path::split(&dir);
        let (home_root, _) = share_ninja_commander::local_path::split(&home);
        let name = if cfg!(windows) || root != home_root {
            root.clone()
        } else {
            "~".to_string()
        };
        Self { dir, root, key, name }
    }
}
