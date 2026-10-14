//! List command - list files in a share without downloading

use anyhow::{Context, Result};
use colored::Colorize;

use share_ninja_core::transfers::TransferDownload;

use super::common::{create_backend, format_size, list_share, print_tree, resolve_share_access};
use crate::config::Config;

pub async fn execute(
    cfg: &Config,
    url: &str,
    key: Option<String>,
    output_format: &str,
    non_recursive: bool,
    path_filter: Option<String>,
    json_file: Option<String>,
) -> Result<()> {
    println!("{} Connecting to Share.Ninja...", "→".blue());

    let session = create_backend(cfg).await?;
    let back = session.back.clone();
    let (token, share_key) = resolve_share_access(&session, url, key).await?;
    let enc_key_bytes = Some(share_key);

    // Use TransferDownload to connect source
    let mut transfer = TransferDownload::new(back.clone());

    tracing::debug!("resolving link: {}", token);
    transfer
        .connect_source(&token, enc_key_bytes)
        .await
        .context("Failed to connect to source")?;
    let source = transfer.source().expect("source set after connect_source");

    // `--path` / `--non-recursive` go straight to the source's `get_dir`.
    let prefix = path_filter.as_deref().map(|p| p.trim_matches('/')).unwrap_or("");
    tracing::debug!(prefix = %prefix, recursive = !non_recursive, "listing");
    let items = list_share(&*source, prefix, !non_recursive).await?;

    // Write JSON to file if requested (independent of output_format)
    if let Some(ref json_path) = json_file {
        let json_items: Vec<_> = items
            .iter()
            .map(|item| {
                serde_json::json!({
                    "key": item.key,
                    "size": item.size,
                    "type": format!("{:?}", item.file_type),
                    "is_complete": item.is_complete,
                    "last_modified": item.attrs.mtime.map(|dt| dt.to_rfc3339()),
                })
            })
            .collect();
        let json_str = serde_json::to_string_pretty(&json_items)?;
        std::fs::write(json_path, &json_str).with_context(|| format!("Failed to write JSON to {}", json_path))?;
        println!("{} Wrote {} items to {}", "✓".green(), items.len(), json_path);
    }

    if output_format == "table" {
        println!("\n{} Files in share:", "📁".green());
        print_table(&items);
    } else {
        // Default to tree
        println!("\n{} Files in share:", "📁".green());
        print_tree(&items);
    }

    Ok(())
}

fn truncate_path(path: &str, max_len: usize) -> String {
    if path.len() <= max_len {
        path.to_string()
    } else {
        format!("...{}", &path[path.len() - max_len + 3..])
    }
}

fn print_table(items: &[share_ninja_core::StorageObjectInfo]) {
    println!(
        "\n{:<50} {:>12} {:<10} {:<20}",
        "PATH".bold(),
        "SIZE".bold(),
        "TYPE".bold(),
        "MODIFIED".bold()
    );
    println!("{}", "-".repeat(95));

    let mut total_size = 0u64;
    let mut file_count = 0usize;
    let mut dir_count = 0usize;

    for item in items {
        let type_str = match item.file_type {
            share_ninja_core::StorageFileType::Dir => {
                dir_count += 1;
                "dir"
            }
            share_ninja_core::StorageFileType::Symlink => "symlink",
            share_ninja_core::StorageFileType::Regular => {
                file_count += 1;
                total_size += item.size;
                "file"
            }
            _ => "other",
        };

        let size_str = if item.file_type == share_ninja_core::StorageFileType::Dir {
            String::new()
        } else {
            format_size(item.size)
        };
        let modified_str = item
            .attrs
            .mtime
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
            .unwrap_or_default();
        println!(
            "{:<50} {:>12} {:<10} {:<20}",
            truncate_path(&item.key, 50),
            size_str,
            type_str,
            modified_str
        );
    }

    println!("{}", "-".repeat(95));
    println!(
        "{} dirs, {} files, {} total size",
        dir_count.to_string().blue(),
        file_count.to_string().green(),
        format_size(total_size).yellow()
    );
}
