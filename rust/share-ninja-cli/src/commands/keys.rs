//! API keys — long-lived bearer credentials for programmatic access
//! (`/users/api-keys`): create (the token is shown once), list, revoke. A
//! signed-in user session is required; the backend refuses key management
//! from a key itself.

use anyhow::{Context, Result, bail};
use colored::Colorize;

use share_ninja_core::models;

use super::common::create_backend;
use crate::config::Config;

/// Mint a key and print its token — the only time the backend shows it.
/// `--json` prints the created key as one JSON object instead.
pub async fn create(
    cfg: &Config,
    name: &str,
    scopes: Vec<String>,
    expires_at: Option<chrono::DateTime<chrono::FixedOffset>>,
    json: bool,
) -> Result<()> {
    let session = create_backend(cfg).await?;
    require_login(&session).await?;
    let key = session
        .back
        .read()
        .await
        .api_key_create(name, scopes, expires_at)
        .await
        .context("Failed to create API key")?;

    if json {
        println!("{}", serde_json::to_string_pretty(&key)?);
        return Ok(());
    }
    println!("{} API key created", "✓".green());
    println!("{:<10} {}", "Key ID:".dimmed(), key.key_id);
    println!("{:<10} {}", "Name:".dimmed(), key.name);
    println!("{:<10} {}", "Prefix:".dimmed(), key.prefix);
    println!("{:<10} {}", "Scopes:".dimmed(), scopes_text(&key.scopes));
    println!("{:<10} {}", "Expires:".dimmed(), expiry_text(key.expires_at.flatten()));
    println!();
    println!("{}", "The token is shown only once — store it now (e.g. SNINJA_API_KEY):".yellow());
    println!("{}", key.token);
    Ok(())
}

/// List the live keys, oldest first.
pub async fn list(cfg: &Config, json: bool) -> Result<()> {
    let session = create_backend(cfg).await?;
    require_login(&session).await?;
    let keys = session
        .back
        .read()
        .await
        .api_keys()
        .await
        .context("Failed to list API keys")?;

    if json {
        println!("{}", serde_json::to_string_pretty(&keys)?);
        return Ok(());
    }
    if keys.is_empty() {
        println!("{} No API keys", "!".yellow());
        return Ok(());
    }
    print_table(&keys);
    Ok(())
}

/// Revoke a key by its id (`key_…`) or by its visible prefix; a prefix must
/// match exactly one live key.
pub async fn revoke(cfg: &Config, key: &str) -> Result<()> {
    let session = create_backend(cfg).await?;
    require_login(&session).await?;
    let key_id = if key.starts_with("key_") {
        key.to_string()
    } else {
        let keys = session
            .back
            .read()
            .await
            .api_keys()
            .await
            .context("Failed to list API keys")?;
        let matches: Vec<&models::UserApiKey> = keys.iter().filter(|k| k.prefix == key).collect();
        match matches.as_slice() {
            [one] => one.key_id.clone(),
            [] => bail!("no API key with prefix '{key}' — see 'sninja keys list'"),
            _ => bail!("prefix '{key}' matches several keys — revoke by key id"),
        }
    };
    session
        .back
        .read()
        .await
        .api_key_revoke(&key_id)
        .await
        .context("Failed to revoke API key")?;
    println!("{} Revoked {}", "✓".green(), key_id);
    Ok(())
}

async fn require_login(session: &super::common::Session) -> Result<()> {
    if session.back.read().await.user.is_none() {
        bail!("not logged in — run 'sninja auth login' first (API keys belong to a user account)");
    }
    Ok(())
}

fn scopes_text(scopes: &[String]) -> String {
    if scopes.is_empty() {
        "(full rights)".to_string()
    } else {
        scopes.join(", ")
    }
}

fn expiry_text(at: Option<chrono::DateTime<chrono::FixedOffset>>) -> String {
    match at {
        Some(t) => t.format("%Y-%m-%d %H:%M").to_string(),
        None => "never".to_string(),
    }
}

fn print_table(keys: &[models::UserApiKey]) {
    println!(
        "\n{:<10} {:<24} {:<28} {:<17} {:<17} {:<17} {}",
        "PREFIX".bold(),
        "NAME".bold(),
        "SCOPES".bold(),
        "CREATED".bold(),
        "LAST USED".bold(),
        "EXPIRES".bold(),
        "KEY ID".bold()
    );
    println!("{}", "-".repeat(160));
    for k in keys {
        let name: String = if k.name.chars().count() > 24 {
            format!("{}…", k.name.chars().take(23).collect::<String>())
        } else {
            k.name.clone()
        };
        let scopes = scopes_text(&k.scopes);
        let scopes: String = if scopes.chars().count() > 28 {
            format!("{}…", scopes.chars().take(27).collect::<String>())
        } else {
            scopes
        };
        let last_used = match k.last_used_at.flatten() {
            Some(t) => t.format("%Y-%m-%d %H:%M").to_string(),
            None => "-".to_string(),
        };
        println!(
            "{:<10} {:<24} {:<28} {:<17} {:<17} {:<17} {}",
            k.prefix,
            name,
            scopes,
            k.created_at.format("%Y-%m-%d %H:%M"),
            last_used,
            expiry_text(k.expires_at.flatten()),
            k.key_id.dimmed()
        );
    }
}
