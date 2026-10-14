//! Get command - download from a share URL

use anyhow::{Context, Result};
use colored::Colorize;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

use share_ninja_core::{
    StorageDestMultipart, StorageFileType,
    fs::{CollisionPolicy, FsDest, NameRule},
    transfers::{TransferDownload, TransferItemState},
};

use super::common::{create_backend, list_share, print_tree, progress_bar, resolve_share_access, track_progress};
use crate::config::{APP_DIR, Config};

#[allow(clippy::too_many_arguments)] // one parameter per CLI flag
pub async fn execute(
    cfg: &Config,
    url: &str,
    filter: Option<String>,
    output: Option<PathBuf>,
    key: Option<String>,
    force: bool,
    streams: usize,
) -> Result<()> {
    let streams = streams.max(1);
    let pattern = filter
        .as_deref()
        .map(glob::Pattern::new)
        .transpose()
        .context("Invalid --filter pattern")?;

    println!("{} Connecting to Share.Ninja...", "→".blue());

    let session = create_backend(cfg).await?;
    let back = session.back.clone();
    let (token, share_key) = resolve_share_access(&session, url, key).await?;
    let enc_key_bytes = Some(share_key);

    println!("{} Starting download for token: {}", "→".blue(), token.yellow());

    let mut transfer = TransferDownload::new(back.clone());

    println!("{} Fetching file list...", "→".blue());
    transfer
        .connect_source(&token, enc_key_bytes)
        .await
        .context("Failed to connect to source")?;
    let source = transfer.source().expect("source set after connect_source");

    let items = list_share(&*source, "", true).await?;
    let items = items.as_slice();
    if items.is_empty() {
        println!("{} No files found in share", "!".yellow());
        return Ok(());
    }

    println!("\n{} Files in share:", "📁".green());
    print_tree(items);

    // `--filter` → an explicit selection of file keys (the engine downloads
    // exactly those, and only those, of the share tree).
    let selection: Option<Vec<String>> = pattern.map(|pat| {
        items
            .iter()
            .filter(|i| i.file_type == StorageFileType::Regular && pat.matches(&i.key))
            .map(|i| i.key.clone())
            .collect()
    });
    if let Some(sel) = &selection {
        if sel.is_empty() {
            anyhow::bail!("no files in the share match --filter");
        }
        let total_files = items.iter().filter(|i| i.file_type == StorageFileType::Regular).count();
        println!(
            "\n{} {} of {} files selected by --filter",
            "→".blue(),
            sel.len(),
            total_files
        );
    }

    let total_size: u64 = items
        .iter()
        .filter(|i| i.file_type == StorageFileType::Regular)
        .filter(|i| selection.as_ref().is_none_or(|s| s.contains(&i.key)))
        .map(|i| i.size)
        .sum();

    // Destination: the share tree lands under `--output` (default: cwd).
    // Existing files: fail (default) or land next to them under a new name
    // (`--force`). Multipart bookkeeping (resume, part verification) lives in
    // a per-share meta dir outside the destination.
    let dest_path = std::path::absolute(output.unwrap_or_else(|| PathBuf::from(".")))
        .context("Failed to resolve output directory")?;
    std::fs::create_dir_all(&dest_path).context("Failed to create output directory")?;
    let policy = if force {
        CollisionPolicy::Avoid
    } else {
        CollisionPolicy::Strict
    };
    let dest = FsDest::new(
        dest_path.clone(),
        download_meta_dir(&token)?,
        policy,
        NameRule::platform(),
        true,
    )
        .await
        .context("Failed to open destination")?;
    let dest: Arc<RwLock<dyn StorageDestMultipart>> = Arc::new(RwLock::new(dest));

    println!("{} Downloading to: {}", "→".blue(), dest_path.display());

    let item = transfer.start_download(selection, dest, streams).await;
    let pb = progress_bar(total_size)?;
    let ticker = track_progress(item.clone(), pb.clone());

    let (state, err) = transfer.do_download(item, true).await;

    ticker.abort();
    pb.finish_and_clear();

    let error_msg = err.as_ref().map(|e| e.to_string());
    transfer.finish_transfer(error_msg.as_deref()).await;

    match state {
        TransferItemState::Finished => {
            println!("\n{} Download complete!", "✓".green());
        }
        TransferItemState::Cancelled => {
            println!("\n{} Download cancelled!", "!".yellow());
        }
        TransferItemState::Error => {
            println!("\n{} Download failed!", "✗".red());
            if let Some(ref e) = err {
                println!("  Error: {}", e);
            }
        }
        _ => {}
    }

    Ok(())
}

/// Per-share multipart meta dir: `<data dir>/Share.Ninja/downloads/<token>`.
/// Kept across runs so a re-run of the same `get` resumes mid-file.
fn download_meta_dir(token: &str) -> Result<PathBuf> {
    let base = dirs::data_local_dir().context("Could not determine data directory")?;
    Ok(base.join(APP_DIR).join("downloads").join(token))
}
