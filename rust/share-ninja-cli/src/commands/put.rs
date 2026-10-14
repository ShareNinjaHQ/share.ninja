//! Put command - upload files to cloud storage and create a share link

use anyhow::{Context, Result};
use colored::Colorize;
use std::path::PathBuf;

use share_ninja_core::transfers::{TransferItemState, TransferUpload};

use super::common::{progress_bar, track_progress};
use super::prepare::{prepare_share, print_share_link};
use crate::config::Config;

pub async fn execute(
    cfg: &Config,
    paths: Vec<PathBuf>,
    description: Option<String>,
    validity: i64,
    streams: usize,
) -> Result<()> {
    let streams = streams.max(1);

    let share = prepare_share(cfg, paths, description, validity, true).await?;

    println!("{} Uploading to cloud...", "☁️".blue());

    // `connect_dest` owns the entire upload setup — upload_start, the S3
    // client + refresher, NinjaStorageV2 wrapping (encryption + zip batches),
    // and connect.
    let mut transfer = TransferUpload::new(share.back.clone());
    transfer
        .connect_dest(&share.url_token, Some(share.encryption_key))
        .await
        .context("Failed to connect to destination")?;

    let transfer_item = transfer
        .start_upload(share.source.clone(), String::new(), streams)
        .await;
    let pb = progress_bar(share.total_size)?;
    let ticker = track_progress(transfer_item.clone(), pb.clone());

    let state = transfer.do_upload(transfer_item, true).await;

    ticker.abort();
    pb.finish_and_clear();

    // Finish transfer
    let error_msg = if state == TransferItemState::Error {
        transfer.error.as_ref().map(|e| e.to_string())
    } else {
        None
    };
    transfer.finish_transfer(error_msg.as_deref()).await;

    match state {
        TransferItemState::Finished => {
            println!("\n{} Upload complete!", "✓".green());
        }
        TransferItemState::Cancelled => {
            println!("\n{} Upload cancelled!", "!".yellow());
            return Ok(());
        }
        TransferItemState::Error => {
            println!("\n{} Upload failed!", "✗".red());
            if let Some(ref e) = transfer.error {
                println!("  Error: {}", e);
            }
            return Ok(());
        }
        _ => {}
    }

    print_share_link(&share);

    Ok(())
}
