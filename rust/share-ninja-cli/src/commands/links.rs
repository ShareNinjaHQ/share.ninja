//! Links command - list this account's (or, anonymous, this device's) links
//! from the backend (`GET /links`, all pages).

use anyhow::{Context, Result};
use colored::Colorize;

use share_ninja_core::models;
use share_ninja_core::utils::crypting::decrypt_link_description;

use super::common::{create_backend, format_size};
use crate::config::Config;

pub async fn execute(cfg: &Config, json_file: Option<String>) -> Result<()> {
    println!("{} Connecting to Share.Ninja...", "→".blue());
    let session = create_backend(cfg).await?;
    let links = session
        .back
        .read()
        .await
        .links()
        .await
        .context("Failed to list links")?;

    // The description rides E2E-encrypted under the share key; the owner's
    // key-store row (`encrypted_share_key`) is sealed to the identity key when
    // the share was created logged-in, else to the device key. Unwrap with the
    // unlocked identity (this device trusted in the vault), else the device
    // key; rows we can't open show the server's plaintext field when present.
    let root: &[u8; 32] = match &session.identity {
        Some(id) => &id.private,
        None => &session.device.1,
    };
    let rows: Vec<Row> = links.iter().map(|l| Row::from_link(l, root)).collect();
    if session.identity.is_none() && rows.iter().any(|r| r.description.is_none()) {
        match session.vault_status.as_deref() {
            Some("active") | Some("not_joined") | Some("pending_approval") => println!(
                "{} some descriptions are sealed to your account identity — run 'sninja vault join' to enrol this device",
                "ℹ️".cyan()
            ),
            _ => {}
        }
    }

    if let Some(ref json_path) = json_file {
        let json_rows: Vec<_> = rows.iter().map(Row::to_json).collect();
        let json_str = serde_json::to_string_pretty(&json_rows)?;
        std::fs::write(json_path, &json_str).with_context(|| format!("Failed to write JSON to {}", json_path))?;
        println!("{} Wrote {} links to {}", "✓".green(), rows.len(), json_path);
    }

    if rows.is_empty() {
        println!("{} No links", "!".yellow());
        return Ok(());
    }
    print_table(&rows);
    Ok(())
}

struct Row {
    token: String,
    description: Option<String>,
    created: String,
    expires: Option<String>,
    size: Option<u64>,
    cloud: bool,
    p2p: bool,
    downloads: Option<i64>,
    url: Option<String>,
}

impl Row {
    fn from_link(l: &models::Link, root_priv: &[u8; 32]) -> Self {
        let description = match (l.encrypted_share_key.as_ref(), l.encrypted_metadata.as_ref()) {
            (Some(esk), Some(em)) => decrypt_link_description(esk, em, root_priv),
            _ => None,
        }
        .or_else(|| l.description.clone());
        // `actual_bytes` is 0 until an upload completes; fall back to the estimate.
        let size = l.size.as_ref().and_then(|s| {
            if s.actual_bytes > 0 {
                Some(s.actual_bytes as u64)
            } else {
                s.estimated_bytes.filter(|b| *b > 0).map(|b| b as u64)
            }
        });
        Self {
            token: l.url_token.clone(),
            description,
            created: l.created_at.format("%Y-%m-%d %H:%M").to_string(),
            expires: l.invalidate_at.map(|t| t.format("%Y-%m-%d %H:%M").to_string()),
            size,
            cloud: l.cloud,
            p2p: l.p2p,
            downloads: l.download_count,
            url: l.url.clone(),
        }
    }

    fn mode(&self) -> &'static str {
        match (self.cloud, self.p2p) {
            (true, true) => "cloud+p2p",
            (true, false) => "cloud",
            (false, true) => "p2p",
            (false, false) => "-",
        }
    }

    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "url_token": self.token,
            "description": self.description,
            "created_at": self.created,
            "invalidate_at": self.expires,
            "size": self.size,
            "cloud": self.cloud,
            "p2p": self.p2p,
            "download_count": self.downloads,
            "url": self.url,
        })
    }
}

fn print_table(rows: &[Row]) {
    println!(
        "\n{:<10} {:<32} {:<17} {:<17} {:>10} {:<10} {:>5}",
        "TOKEN".bold(),
        "DESCRIPTION".bold(),
        "CREATED".bold(),
        "EXPIRES".bold(),
        "SIZE".bold(),
        "MODE".bold(),
        "DLS".bold()
    );
    println!("{}", "-".repeat(108));
    for r in rows {
        let desc = r.description.as_deref().unwrap_or("(encrypted)");
        let desc: String = if desc.chars().count() > 32 {
            format!("{}…", desc.chars().take(31).collect::<String>())
        } else {
            desc.to_string()
        };
        println!(
            "{:<10} {:<32} {:<17} {:<17} {:>10} {:<10} {:>5}",
            r.token,
            desc,
            r.created,
            r.expires.as_deref().unwrap_or("-"),
            r.size.map(format_size).unwrap_or_else(|| "-".to_string()),
            r.mode(),
            r.downloads.map(|d| d.to_string()).unwrap_or_else(|| "-".to_string()),
        );
    }
    println!("{}", "-".repeat(108));
    println!("{} links", rows.len().to_string().green());
}
