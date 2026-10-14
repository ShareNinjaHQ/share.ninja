//! What `put` and `direct` share: the link, its claim URL and the connected
//! source — everything up to the point where bytes start moving.

use anyhow::{Context, Result};
use colored::Colorize;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;

use share_ninja_core::{
    Back, StorageSource,
    back::LinkInfo,
    fs::FsSource,
    models,
    utils::crypting::{LinkMetadata, encrypt_metadata},
};

use super::common::{create_backend, create_share_claim, format_size, list_share};
use crate::config::Config;

/// A created link with its connected source, ready to move bytes.
pub struct PreparedShare {
    pub back: Arc<RwLock<Back>>,
    pub url_token: String,
    /// Host + claim fragment — what the user hands out.
    pub share_url: String,
    pub source: Arc<RwLock<dyn StorageSource + Send + Sync>>,
    pub encryption_key: [u8; 32],
    pub total_size: u64,
}

/// Create the link — a cloud datasource when `cloud`, P2P otherwise — mint
/// its claim, connect and list the source, and patch the declared size onto
/// the link.
pub async fn prepare_share(
    cfg: &Config,
    paths: Vec<PathBuf>,
    description: Option<String>,
    validity: i64,
    cloud: bool,
) -> Result<PreparedShare> {
    // Validate paths exist
    for path in &paths {
        if !path.exists() {
            anyhow::bail!("Path does not exist: {}", path.display());
        }
    }
    let roots = upload_roots(&paths)?;

    println!("{} Connecting to Share.Ninja...", "→".blue());

    let session = create_backend(cfg).await?;
    let back = session.back.clone();

    // Share master key (encrypts content for both cloud and P2P, and the
    // link metadata).
    let encryption_key = generate_encryption_key();

    // The description rides encrypted under the share key, never in the
    // plaintext `description` field. Default = basename of the single path,
    // else comma-joined basenames (same rule as the desktop app's quick share).
    let name = description.unwrap_or_else(|| default_share_name(&roots));
    let encrypted_metadata = encrypt_metadata(
        &LinkMetadata {
            description: Some(name),
        },
        &encryption_key,
    )
    .context("Failed to encrypt link metadata")?;

    // Create a new link
    println!("{} Creating share link...", "→".blue());

    let link_info = LinkInfo {
        description: None,
        validity: Some(validity),
        workspace_id: None,
        cloud_storage: Some(cloud),
        p2p_storage: Some(!cloud),
        estimated_bytes: None,
        encrypted_metadata: Some(encrypted_metadata),
        // Owner's own key-store row, written in the create tx — the key is
        // never persisted locally.
        encrypted_share_key: session.wrap_own_share_key(&encryption_key).await,
        notify_on_open: None,
        notify_on_download: None,
    };

    let link_answer = {
        let back_guard = back.read().await;
        back_guard.link(&link_info).await.context("Failed to create link")?
    };

    tracing::debug!("Link URL: {}", link_answer.url);
    tracing::debug!("URL token: {}", link_answer.url_token);

    // The server clamps a cloud link's lifetime to the account tier's
    // retention and says so in the answer.
    if let (Some(true), Some(max)) = (link_answer.retention_clamped, link_answer.max_allowed_retention_sec) {
        println!(
            "{} Validity cut to this account's maximum: {} (requested {})",
            "!".yellow(),
            format_validity(i64::from(max)),
            format_validity(validity)
        );
    }

    // Wrap the master key in a vault claim; the URL fragment carries the claim
    // token, not the raw key (receivers unwrap it via /vault/claims). A minted
    // claim resolves to its own link server-side, so the share URL is
    // token-less: host + claim fragment (mirrors the frontend's mintLink).
    // Claims are the only key channel receivers accept — a claim failure
    // fails the share.
    let claim = create_share_claim(&back, &link_answer.url_token, &encryption_key)
        .await
        .context("failed to create vault claim")?;
    let share_url = format!("{}/#{}", cfg.base_url(), claim);

    // Create filesystem source
    let source: Arc<RwLock<dyn StorageSource + Send + Sync>> = Arc::new(RwLock::new(FsSource::new(roots)));

    // Connect source and scan files
    println!("{} Scanning files...", "→".blue());
    {
        let mut source_guard = source.write().await;
        source_guard.connect().await.context("Failed to connect to source")?;
    }

    // Source listing for display
    let items = list_share(&*source, "", true).await?;
    let items = items.as_slice();
    let mut total_size = 0u64;
    let mut file_count = 0usize;
    let mut dir_count = 0usize;

    // Count files, directories and total size
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

    println!("\nRoot:");
    print_file_tree(items);
    println!(
        "\n  {} dirs, {} files, {} total size\n",
        dir_count.to_string().blue(),
        file_count.to_string().green(),
        format_size(total_size).yellow()
    );

    // The link was created before the scan — patch the declared size onto it
    // (the desktop does the same after its sizing walk) so receiver rows can
    // show a size.
    let update = models::LinkUpdateRequest {
        estimated_bytes: Some(total_size as i64),
        ..Default::default()
    };
    if let Err(e) = back.read().await.link_update(&link_answer.url_token, update).await {
        // The tier-quota refusal is authoritative — the declared size does not
        // fit the owner's cap, so the upload would only die mid-flight. Any
        // other PATCH failure stays advisory: the estimate is cosmetic.
        if e.is_quota_exceeded() {
            anyhow::bail!("cloud storage limit reached — the share does not fit the storage quota ({e})");
        }
        tracing::warn!("link_update estimated_bytes failed: {e}");
    }

    Ok(PreparedShare {
        back,
        url_token: link_answer.url_token,
        share_url,
        source,
        encryption_key,
        total_size,
    })
}

/// The share link and its URL token, as handed to the user.
pub fn print_share_link(share: &PreparedShare) {
    println!("\n{} Share link:", "🔗".blue());
    println!("  {}", share.share_url.green());

    println!("\n{} URL token:", "🔑".yellow());
    println!("  {}", share.url_token);
}

/// A lifetime in seconds as the largest whole unit it divides into: `2w`,
/// `3d`, `12h`, `45m`, `90s`; 0 is "no expiry".
fn format_validity(secs: i64) -> String {
    if secs == 0 {
        return "no expiry".to_string();
    }
    for (unit, size) in [("w", 60 * 60 * 24 * 7), ("d", 60 * 60 * 24), ("h", 60 * 60), ("m", 60)] {
        if secs % size == 0 {
            return format!("{}{unit}", secs / size);
        }
    }
    format!("{secs}s")
}

/// Resolve the upload roots handed to `FsSource`: every path made absolute
/// and `.`/`..`-free (so `.` names itself, not "."), and the `dir/` / `dir/*`
/// "contents only" convention expanded to the directory's children as roots.
fn upload_roots(paths: &[PathBuf]) -> Result<Vec<PathBuf>> {
    let mut roots = Vec::new();
    for p in paths {
        let s = p.to_string_lossy();
        let contents_only = s.ends_with('/') || s.ends_with("/*");
        let trimmed = s.trim_end_matches("/*").trim_end_matches('/');
        let clean: &Path = if trimmed.is_empty() {
            p.as_path()
        } else {
            Path::new(trimmed)
        };
        let abs =
            normalize(std::path::absolute(clean).with_context(|| format!("Failed to resolve path: {}", p.display()))?);
        if contents_only {
            let entries =
                std::fs::read_dir(&abs).with_context(|| format!("Failed to read directory: {}", abs.display()))?;
            for e in entries {
                roots.push(e?.path());
            }
        } else {
            roots.push(abs);
        }
    }
    Ok(roots)
}

/// Drop `.` components and fold `..` lexically — no symlink resolution.
fn normalize(p: PathBuf) -> PathBuf {
    let mut out = PathBuf::new();
    for c in p.components() {
        match c {
            Component::CurDir => {}
            Component::ParentDir => {
                if !matches!(out.components().next_back(), Some(Component::RootDir) | None) {
                    out.pop();
                }
            }
            other => out.push(other.as_os_str()),
        }
    }
    out
}

/// Basename of the single path, else comma-joined basenames — mirrors the
/// desktop app's quick-share default name.
fn default_share_name(paths: &[PathBuf]) -> String {
    let base = |p: &PathBuf| -> String {
        p.file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| p.to_string_lossy().to_string())
    };
    match paths {
        [one] => base(one),
        many => many.iter().map(base).collect::<Vec<_>>().join(", "),
    }
}

fn generate_encryption_key() -> [u8; 32] {
    use rand::Rng;
    let mut key = [0u8; 32];
    rand::rng().fill_bytes(&mut key);
    key
}

fn print_file_tree(items: &[share_ninja_core::StorageObjectInfo]) {
    use std::collections::BTreeMap;

    #[derive(Default)]
    struct TreeNode {
        is_dir: bool,
        size: u64,
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
            }
            current = node;
        }
    }

    // Print tree recursively
    fn print_node(name: &str, node: &TreeNode, prefix: &str, is_last: bool) {
        let connector = if is_last { "└── " } else { "├── " };
        let icon = if node.is_dir { "📁" } else { "📄" };

        if node.is_dir {
            println!("  {}{}{} {}", prefix, connector, icon, name);
        } else {
            println!(
                "  {}{}{} {} ({})",
                prefix,
                connector,
                icon,
                name,
                format_size(node.size)
            );
        }

        let child_prefix = if is_last {
            format!("{}    ", prefix)
        } else {
            format!("{}│   ", prefix)
        };

        let children: Vec<_> = node.children.iter().collect();
        for (i, (child_name, child_node)) in children.iter().enumerate() {
            let child_is_last = i == children.len() - 1;
            print_node(child_name, child_node, &child_prefix, child_is_last);
        }
    }

    // Print root children
    let children: Vec<_> = root.children.iter().collect();
    for (i, (name, node)) in children.iter().enumerate() {
        let is_last = i == children.len() - 1;
        print_node(name, node, "", is_last);
    }
}
