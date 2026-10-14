//! Link command - detailed info on one link (`GET /links/{url_token}`), and
//! optionally a fresh copy-ready `#claim` URL for it.

use anyhow::{Context, Result};
use colored::Colorize;

use share_ninja_core::models;
use share_ninja_core::utils::crypting::decrypt_metadata_description;

use super::common::{Session, create_backend, create_share_claim, format_size};
use crate::config::Config;

pub async fn execute(cfg: &Config, token: &str, mint: bool, json_file: Option<String>) -> Result<()> {
    let session = create_backend(cfg).await?;
    let link = session
        .back
        .read()
        .await
        .link_get(token, None)
        .await
        .with_context(|| format!("Failed to fetch link {}", token))?;

    // Our own key-store row, when we hold its root: the unlocked identity, or
    // the device key for shares this CLI created anonymously.
    let share_key = match link.encrypted_share_key.as_deref() {
        Some(esk) => session.unwrap_own_share_key(esk),
        None => None,
    };
    let description = share_key
        .as_ref()
        .and_then(|k| {
            link.encrypted_metadata
                .as_deref()
                .and_then(|em| decrypt_metadata_description(em, k))
        })
        .or_else(|| link.description.clone());

    // A claim URL is only ever known at creation (the server keeps the hash),
    // so a copy-ready link means minting a fresh claim under our share key.
    let claim_url = if mint {
        let key = share_key.as_ref().ok_or_else(|| {
            anyhow::anyhow!(
                "cannot mint a link: this device holds no key for it{}",
                mint_hint(&session)
            )
        })?;
        let claim = create_share_claim(&session.back, &link.url_token, key)
            .await
            .context("Failed to create claim")?;
        Some(format!("{}/#{}", cfg.base_url(), claim))
    } else {
        None
    };

    let token_url = link
        .url
        .as_deref()
        .map(|u| format!("{}{}", cfg.base_url(), u))
        .unwrap_or_else(|| format!("{}/{}/", cfg.base_url(), link.url_token));

    if let Some(ref json_path) = json_file {
        let v = to_json(&link, description.as_deref(), &token_url, claim_url.as_deref());
        std::fs::write(json_path, serde_json::to_string_pretty(&v)?)
            .with_context(|| format!("Failed to write JSON to {}", json_path))?;
        println!("{} Wrote link details to {}", "✓".green(), json_path);
    }

    print_details(&link, description.as_deref(), share_key.is_some(), &token_url);
    if let Some(url) = claim_url {
        println!("\n{} Share link (new claim):", "🔗".blue());
        println!("  {}", url.green().bold());
    }
    Ok(())
}

fn mint_hint(session: &Session) -> &'static str {
    match session.vault_status.as_deref() {
        Some("active") | Some("not_joined") | Some("pending_approval") if session.identity.is_none() => {
            " — it is sealed to your account identity; run 'sninja vault join' to enrol this device"
        }
        _ => "",
    }
}

fn fmt_time(t: &chrono::DateTime<chrono::FixedOffset>) -> String {
    t.format("%Y-%m-%d %H:%M").to_string()
}

fn size_of(s: Option<&models::DatasourceSize>) -> String {
    match s {
        Some(s) if s.actual_bytes > 0 => format_size(s.actual_bytes as u64),
        Some(s) => match s.estimated_bytes.filter(|b| *b > 0) {
            Some(b) => format!("~{}", format_size(b as u64)),
            None => "-".to_string(),
        },
        None => "-".to_string(),
    }
}

fn mode(l: &models::Link) -> &'static str {
    match (l.cloud, l.p2p) {
        (true, true) => "cloud+p2p",
        (true, false) => "cloud",
        (false, true) => "p2p",
        (false, false) => "-",
    }
}

fn permission_name(p: &models::LinkPermission) -> String {
    // Serde names are the wire strings (`link:read`, …).
    serde_json::to_value(p)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_else(|| format!("{:?}", p))
}

fn print_details(l: &models::Link, description: Option<&str>, key_held: bool, token_url: &str) {
    let row = |k: &str, v: String| println!("{:<14} {}", format!("{k}:").dimmed(), v);
    println!("\n{} {}", "Link".bold(), l.url_token.yellow());
    row(
        "Description",
        description
            .map(str::to_string)
            .unwrap_or_else(|| "(encrypted)".to_string()),
    );
    if let Some(u) = l.user.as_deref() {
        let who = match u.email.as_deref() {
            Some(e) if !e.is_empty() => format!("{} <{}>", u.display_name, e),
            _ => u.display_name.clone(),
        };
        row("Owner", who);
    }
    if let Some(a) = l.agent.as_deref() {
        row("Device", format!("{} ({})", a.name, a.agent_id));
    }
    if let Some(w) = l.workspace_id.as_deref() {
        row("Workspace", w.to_string());
    }
    row("Mode", mode(l).to_string());
    row("Size", size_of(l.size.as_deref()));
    row("Created", fmt_time(&l.created_at));
    row(
        "Expires",
        l.invalidate_at
            .as_ref()
            .map(fmt_time)
            .unwrap_or_else(|| "-".to_string()),
    );
    row(
        "Downloads",
        l.download_count
            .map(|d| d.to_string())
            .unwrap_or_else(|| "-".to_string()),
    );
    row(
        "Permissions",
        l.effective_permissions
            .iter()
            .map(permission_name)
            .collect::<Vec<_>>()
            .join(", "),
    );
    row(
        "Share key",
        if key_held {
            "held on this device".to_string()
        } else {
            "not held".to_string()
        },
    );
    row("URL", token_url.to_string());
}

fn to_json(l: &models::Link, description: Option<&str>, token_url: &str, claim_url: Option<&str>) -> serde_json::Value {
    serde_json::json!({
        "url_token": l.url_token,
        "description": description,
        "owner": l.user.as_deref().map(|u| serde_json::json!({
            "user_id": u.user_id, "email": u.email, "display_name": u.display_name,
        })),
        "device": l.agent.as_deref().map(|a| serde_json::json!({ "agent_id": a.agent_id, "name": a.name })),
        "workspace_id": l.workspace_id,
        "cloud": l.cloud,
        "p2p": l.p2p,
        "size": l.size.as_deref().map(|s| serde_json::json!({
            "estimated_bytes": s.estimated_bytes, "actual_bytes": s.actual_bytes,
        })),
        "created_at": l.created_at.to_rfc3339(),
        "invalidate_at": l.invalidate_at.map(|t| t.to_rfc3339()),
        "download_count": l.download_count,
        "effective_permissions": l.effective_permissions.iter().map(permission_name).collect::<Vec<_>>(),
        "url": token_url,
        "claim_url": claim_url,
    })
}
