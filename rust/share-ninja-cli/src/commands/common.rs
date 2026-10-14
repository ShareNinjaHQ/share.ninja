use anyhow::Result;
use std::sync::{Arc, LazyLock};

use indicatif::{ProgressBar, ProgressStyle};
use share_ninja_client::{ClientParams, create_session, open_session};
use share_ninja_core::secrets::SecureStore;
use share_ninja_core::transfers::TransferItem;

use crate::config::Config;

// The shared client plumbing (`share-ninja-client`), under the names the
// commands use.
pub use share_ninja_client::session::{Session, machine_name};
pub use share_ninja_client::share::{format_size, list_share, resolve_share_access};
pub use share_ninja_core::back::claims::create_share_claim;

/// The binary's product id (CLI and commander alike) — one id on every
/// platform (the OS rides in `os`/`os_version`). Core owns the constant: its
/// device label matches on it to render a CLI agent as "CLI on <OS>".
const PRODUCT_ID: &str = share_ninja_core::utils::device::PRODUCT_ID_CLI;
/// Keychain item (service attribute) holding the binary's secrets blob — its
/// own, distinct from the desktop app's.
const KEYCHAIN_SERVICE: &str = "net.shareninja.cli";
/// This process's runtime instance id (`POST /auth/agents` `instance_id`).
/// Every P2P link `put` or the commander creates is bound to it, so the link's
/// P2P availability dies with the process: both serve from memory and nothing
/// re-serves after exit. One per process, like the web tab's — it survives
/// this process's reconnects and re-registers.
static INSTANCE_ID: LazyLock<String> = LazyLock::new(|| format!("cli-{}", uuid::Uuid::new_v4()));

fn client_params(cfg: &Config) -> ClientParams {
    ClientParams {
        base_url: cfg.base_url().to_string(),
        product_id: PRODUCT_ID,
        product_version: env!("CARGO_PKG_VERSION"),
        user_agent: format!("Share.Ninja CLI/{}", env!("CARGO_PKG_VERSION")),
        device_name: machine_name(),
        keychain_service: KEYCHAIN_SERVICE,
        secrets_file: cfg.secrets_file.clone(),
        log_api: cfg.log_api,
        instance_id: Some(INSTANCE_ID.clone()),
    }
}

/// The CLI's device secrets (keypair + refresh token): the secrets file when
/// one is configured, else the CLI's keychain item.
pub fn secure_store(cfg: &Config) -> Arc<SecureStore> {
    share_ninja_client::keychain::secure_store(KEYCHAIN_SERVICE, cfg.secrets_file.as_deref())
}

/// Where the secrets live, for messages: the file, or the system keychain.
pub fn secrets_location(cfg: &Config) -> String {
    match &cfg.secrets_file {
        Some(path) => path.display().to_string(),
        None => "the system keychain".to_string(),
    }
}

/// Register with the backend as the CLI (the refresh token comes from the
/// keychain item) and unlock the vault identity when this device is trusted.
pub async fn create_backend(cfg: &Config) -> Result<Session> {
    create_session(&client_params(cfg)).await
}

/// The same session wired but not registered — for the commander, which
/// registers in the background once the TUI is up.
pub async fn open_backend(cfg: &Config) -> Session {
    open_session(&client_params(cfg)).await
}

/// A byte-progress bar for one transfer item.
pub fn progress_bar(total: u64) -> Result<ProgressBar> {
    let pb = ProgressBar::new(total);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta})")?
            .progress_chars("#>-"),
    );
    Ok(pb)
}

/// Drive `pb` from the item's own progress snapshot (bytes moved so far, total
/// once the source-side sizing is done) until aborted. Abort the returned task
/// when the transfer's `do_*` returns, then `finish_and_clear` the bar.
pub fn track_progress(item: Arc<TransferItem>, pb: ProgressBar) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_millis(250));
        loop {
            tick.tick().await;
            let p = item.progress().await;
            if p.total_bytes > 0 && pb.length() != Some(p.total_bytes) {
                pb.set_length(p.total_bytes);
            }
            pb.set_position(p.bytes);
        }
    })
}

/// Print a tree view of storage items
pub fn print_tree(items: &[share_ninja_core::StorageObjectInfo]) {
    use colored::Colorize;
    use std::collections::BTreeMap;

    let mut dir_count = 0usize;
    let mut file_count = 0usize;
    let mut total_size = 0u64;

    for item in items {
        match item.file_type {
            share_ninja_core::StorageFileType::Regular => {
                file_count += 1;
                total_size += item.size;
            }
            share_ninja_core::StorageFileType::Dir => {
                dir_count += 1;
            }
            _ => {}
        }
    }

    #[derive(Default)]
    struct TreeNode {
        is_dir: bool,
        size: u64,
        last_modified: Option<chrono::DateTime<chrono::Utc>>,
        children: BTreeMap<String, TreeNode>,
    }

    // Build tree structure
    let mut root = TreeNode::default();
    for item in items {
        let parts: Vec<_> = item.key.split('/').collect();
        let mut current = &mut root;

        for (i, part) in parts.iter().enumerate() {
            if part.is_empty() {
                continue;
            }
            let is_last = i == parts.len() - 1;
            let node = current.children.entry(part.to_string()).or_default();
            if is_last {
                node.is_dir = item.file_type == share_ninja_core::StorageFileType::Dir;
                node.size = item.size;
                node.last_modified = item.attrs.mtime;
            }
            current = node;
        }
    }

    // Print tree recursively
    fn print_node(name: &str, node: &TreeNode, prefix: &str, is_last: bool, format_size: fn(u64) -> String) {
        let connector = if is_last { "└── " } else { "├── " };
        let icon = if node.is_dir { "📁" } else { "📄" };

        if node.is_dir {
            println!("  {}{}{} {}", prefix, connector, icon, name);
        } else {
            let modified_str = node
                .last_modified
                .map(|dt: chrono::DateTime<chrono::Utc>| dt.format("%Y-%m-%d %H:%M").to_string())
                .unwrap_or_default();
            if modified_str.is_empty() {
                println!(
                    "  {}{}{} {} ({})",
                    prefix,
                    connector,
                    icon,
                    name,
                    format_size(node.size)
                );
            } else {
                println!(
                    "  {}{}{} {} ({}, {})",
                    prefix,
                    connector,
                    icon,
                    name,
                    format_size(node.size),
                    modified_str
                );
            }
        }

        let child_prefix = if is_last {
            format!("{}    ", prefix)
        } else {
            format!("{}│   ", prefix)
        };

        let children: Vec<_> = node.children.iter().collect();
        for (i, (child_name, child_node)) in children.iter().enumerate() {
            let child_is_last = i == children.len() - 1;
            print_node(child_name, child_node, &child_prefix, child_is_last, format_size);
        }
    }

    // Print root children
    let children: Vec<_> = root.children.iter().collect();
    for (i, (name, node)) in children.iter().enumerate() {
        let is_last = i == children.len() - 1;
        print_node(name, node, "", is_last, format_size);
    }

    // Print summary
    println!(
        "\n  {} dirs, {} files, {} total size",
        dir_count.to_string().blue(),
        file_count.to_string().green(),
        format_size(total_size).yellow()
    );
}
