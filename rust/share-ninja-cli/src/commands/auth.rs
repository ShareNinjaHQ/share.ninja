//! OAuth2 authorization_code (PKCE) login flow.
//!
//! Opens the browser-based consent page, listens on a local loopback callback
//! for the authorization code, exchanges it for access + refresh tokens, and
//! persists the refresh token in the system keychain (or `--secrets-file`).

use anyhow::{Context, Result};
use colored::Colorize;

use share_ninja_client::oauth::{LoginFlow, open_in_browser};

use crate::commands::common::{create_backend, secrets_location, secure_store};
use crate::config::Config;

/// Run the interactive browser-based OAuth login flow.
pub async fn login(cfg: &Config, callback_host: &str, callback_port: u16, callback_path: &str) -> Result<()> {
    let back = create_backend(cfg).await?.back;
    let flow = LoginFlow::begin(&back, callback_host, callback_port, callback_path).await?;

    println!("{} {}", "Opening consent page:".dimmed(), flow.consent_url);
    open_in_browser(&flow.consent_url).with_context(|| format!("Failed to open browser for {}", flow.consent_url))?;

    println!("{}", "Waiting for callback...".dimmed());
    // Exchange runs the full register handshake (WS + `GET /me`) lock-light.
    flow.finish(back.clone()).await?;

    println!("\n{}", "Authentication successful!".green().bold());
    println!(
        "{}",
        format!("Credentials saved to {}.", secrets_location(cfg)).dimmed()
    );
    let guard = back.read().await;
    if let Some(ref user) = guard.user {
        println!("{:<14} {}", "User ID:".dimmed(), user.user_id);
        println!("{:<14} {}", "Email:".dimmed(), user.email);
        println!("{:<14} {}", "Display name:".dimmed(), user.display_name);
    }

    if let Some(access_token) = guard.access_token() {
        share_ninja_core::back::log_jwt_info(&access_token);
    }

    Ok(())
}

/// Sign out: revoke the session server-side and remove stored credentials.
pub async fn logout(cfg: &Config) -> Result<()> {
    let back = create_backend(cfg).await?.back;
    {
        let mut guard = back.write().await;
        if let Err(e) = guard.logout().await {
            tracing::debug!("Server-side logout failed: {}", e);
        }
    }
    // Ensure the stored token is gone even if the server call failed.
    secure_store(cfg).set_refresh_token(None);
    println!(
        "{}",
        format!("Signed out. Credentials removed from {}.", secrets_location(cfg)).green()
    );
    Ok(())
}

/// Show the current authentication status. A stored refresh token alone
/// says nothing about login — an anonymous agent session has one too (it is
/// what keeps this device's agent identity across runs). The register
/// handshake in `create_backend` exchanges the token and fetches `/me`,
/// so the answer is simply whether a user came back.
pub async fn status(cfg: &Config) -> Result<()> {
    let back = create_backend(cfg).await?.back;
    let guard = back.read().await;

    match guard.user {
        Some(ref user) => {
            println!("{}", "Logged in".green().bold());
            println!("{:<14} {}", "User ID:".dimmed(), user.user_id);
            println!("{:<14} {}", "Email:".dimmed(), user.email);
            println!("{:<14} {}", "Display name:".dimmed(), user.display_name);
        }
        None => {
            println!("{}", "Not logged in (anonymous device session).".yellow());
            println!("{}", "Run 'sninja auth login' to authenticate.".dimmed());
        }
    }

    if let Some(access_token) = guard.access_token() {
        share_ninja_core::back::log_jwt_info(&access_token);
    }
    Ok(())
}
