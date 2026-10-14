//! Vault commands — this device's standing in the account's trust circle.
//!
//! Setup (creating the identity + recovery phrase) is deliberately not here:
//! do that in the Share.Ninja app. The CLI joins an existing vault and
//! unlocks the identity a trusted device wraps for it — and, once trusted,
//! can stay up approving every other device that asks (`auto-approve`).

use std::collections::HashSet;

use anyhow::{Context, Result};
use colored::Colorize;
use tokio::sync::{mpsc, oneshot};

use share_ninja_client::ws::BackWsExt;
use share_ninja_commander::types::product_label;
use share_ninja_core::utils::device::product;
use share_ninja_core::{Back, models, vault};

use super::common::{Session, create_backend, open_backend};
use crate::config::Config;

/// Print this device's vault standing.
pub async fn status(cfg: &Config) -> Result<()> {
    let session = create_backend(cfg).await?;
    print_status(&session);
    Ok(())
}

fn print_status(session: &Session) {
    match (session.vault_status.as_deref(), &session.identity) {
        (None, _) => {
            println!("{}", "Not logged in.".yellow());
            println!("{}", "Run 'sninja auth login' first.".dimmed());
        }
        (Some("not_initialized"), _) => {
            println!("{}", "Vault not set up for this account.".yellow());
            println!(
                "{}",
                "Set it up in the Share.Ninja app first, then run 'sninja vault join'.".dimmed()
            );
        }
        (Some("not_joined"), _) => {
            println!("{}", "This device is not in the vault.".yellow());
            println!(
                "{}",
                "Run 'sninja vault join' and approve it on one of your trusted devices.".dimmed()
            );
        }
        (Some("pending_approval"), _) => {
            println!("{}", "Join request pending approval.".yellow());
            println!(
                "{}",
                "Approve this device on one of your trusted devices, or run 'sninja vault join --wait'.".dimmed()
            );
        }
        (Some("active"), Some(_)) => {
            println!("{}", "Enrolled — vault unlocked on this device.".green().bold());
        }
        (Some("active"), None) => {
            println!("{}", "Enrolled, but the identity key could not be unlocked.".red());
            println!("{}", "Run with --verbose for the reason.".dimmed());
        }
        (Some(other), _) => println!("{} {}", "Vault status:".dimmed(), other),
    }
}

/// Request to join the trust circle. With `wait`, keep the connection open
/// until a trusted device approves (or rejects) and unlock on the spot.
pub async fn join(cfg: &Config, wait: bool) -> Result<()> {
    let session = create_backend(cfg).await?;
    match session.vault_status.as_deref() {
        None => anyhow::bail!("not logged in — run 'sninja auth login' first"),
        Some("not_initialized") => {
            anyhow::bail!("vault not set up for this account — set it up in the Share.Ninja app first")
        }
        Some("active") if session.identity.is_some() => {
            println!("{}", "Already enrolled — vault unlocked on this device.".green());
            return Ok(());
        }
        _ => {}
    }

    // Subscribe before asking, so the answer can't slip past us.
    let (tx, rx) = oneshot::channel::<models::WsMessage>();
    if wait {
        let tx = std::sync::Mutex::new(Some(tx));
        session
            .back
            .read()
            .await
            .set_ws_message_handler(move |msg: models::WsMessage| {
                if matches!(
                    msg,
                    models::WsMessage::VaultDeviceApproved(_) | models::WsMessage::VaultDeviceRejected(_)
                ) && let Some(tx) = tx.lock().unwrap().take()
                {
                    let _ = tx.send(msg);
                }
            })
            .await;
    }

    let join = vault::join_start(&session.back).await.context("Join request failed")?;
    println!("{} Join request sent.", "→".blue());
    println!("{:<14} {}", "Fingerprint:".dimmed(), join.device_fingerprint.bold());
    println!(
        "{}",
        "Approve this device on one of your trusted devices and check the fingerprint matches.".dimmed()
    );
    for a in &join.approvers {
        let presence = if a.online {
            "online".green().to_string()
        } else {
            format!("last seen {}", a.last_seen_at.format("%Y-%m-%d %H:%M")).dimmed().to_string()
        };
        println!("  {} {}  {}", "•".dimmed(), a.display_name.bold(), presence);
    }

    if !wait {
        println!("{}", "Run 'sninja vault status' after approving.".dimmed());
        return Ok(());
    }

    println!(
        "{}",
        "Waiting for approval (Ctrl+C to stop waiting; the request stays pending)...".dimmed()
    );
    let msg = rx.await.context("connection closed while waiting for approval")?;
    match msg {
        models::WsMessage::VaultDeviceApproved(data) => {
            let identity_pub = session.identity_public_key_b64().await;
            vault::handle_device_approved(&data.encrypted_identity_key, session.device, identity_pub)
                .context("Approved, but the identity key could not be unlocked")?;
            println!(
                "{}",
                "Approved — this device is enrolled and the vault is unlocked."
                    .green()
                    .bold()
            );
        }
        models::WsMessage::VaultDeviceRejected(data) => {
            println!(
                "{} {}",
                "Rejected.".red().bold(),
                data.reason.as_deref().unwrap_or("").dimmed()
            );
        }
        _ => unreachable!("filtered above"),
    }
    Ok(())
}

/// One join request, from either frame that carries them.
struct JoinRequest {
    request_id: String,
    agent_id: String,
    device_name: String,
    device_fingerprint: String,
    device_public_key: Vec<u8>,
    ip: Option<String>,
    country: Option<String>,
    product_id: Option<String>,
}

/// Stay connected and approve every vault join request of this account as it
/// arrives, the ones already pending included — for a trusted device running
/// unattended (a container). Anyone who can sign in to the account gets the
/// vault through it. Runs until Ctrl+C / SIGTERM.
pub async fn auto_approve(cfg: &Config) -> Result<()> {
    // Subscribe before registering: the server sends its pending set right on
    // connect (not replayed), and on every reconnect after.
    let mut session = open_backend(cfg).await;
    let (tx, mut rx) = mpsc::unbounded_channel::<JoinRequest>();
    session
        .back
        .read()
        .await
        .set_ws_message_handler(move |msg| match msg {
            models::WsMessage::VaultDeviceJoinRequested(d) => {
                let _ = tx.send(JoinRequest {
                    request_id: d.request_id,
                    agent_id: d.agent_id,
                    device_name: d.device_name,
                    device_fingerprint: d.device_fingerprint,
                    device_public_key: d.device_public_key,
                    ip: d.ip,
                    country: d.country,
                    product_id: d.product_id,
                });
            }
            models::WsMessage::VaultJoinRequestsSnapshot(d) => {
                for r in d.requests {
                    let _ = tx.send(JoinRequest {
                        request_id: r.request_id,
                        agent_id: r.agent_id,
                        device_name: r.device_name,
                        device_fingerprint: r.device_fingerprint,
                        device_public_key: r.device_public_key,
                        ip: r.ip,
                        country: r.country,
                        product_id: r.product_id,
                    });
                }
            }
            _ => {}
        })
        .await;

    Back::run_register(session.back.clone())
        .await
        .context("Failed to register with backend")?;
    session.refresh_vault().await;
    let Some(identity) = session.identity.clone() else {
        print_status(&session);
        anyhow::bail!("only an enrolled device with the vault unlocked can approve others");
    };

    println!(
        "{}",
        "Approving every vault join request of this account (Ctrl+C to stop)...".dimmed()
    );
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
    // Request ids decided in this run: a snapshot after a reconnect re-lists
    // what is still pending, and a request must not be approved twice.
    let mut decided: HashSet<String> = HashSet::new();
    loop {
        let req = tokio::select! {
            req = rx.recv() => match req {
                Some(req) => req,
                None => break,
            },
            _ = &mut shutdown => break,
        };
        if decided.contains(&req.request_id) {
            continue;
        }
        let who = format!(
            "{} ({}) {} {}{}",
            req.device_name.bold(),
            req.product_id
                .as_deref()
                .map(|id| product_label(&product(id)))
                .unwrap_or_else(|| "unknown app".to_string()),
            req.device_fingerprint.dimmed(),
            req.ip.as_deref().unwrap_or("-"),
            req.country.as_deref().map(|c| format!(" {c}")).unwrap_or_default(),
        );
        match vault::approve_join(&session.back, &req.agent_id, &req.device_public_key, &identity).await {
            Ok(()) => {
                decided.insert(req.request_id);
                println!("{} Approved {who}", "✓".green());
            }
            // Decided on another device, or expired: nothing left to do.
            Err(vault::VaultError::Back { source, .. }) if vault::join_already_decided(&source) => {
                decided.insert(req.request_id);
                println!("{} Already decided: {who}", "•".dimmed());
            }
            // Left undecided: the next snapshot (reconnect) retries it.
            Err(e) => eprintln!("{} Approving {who} failed: {e}", "✗".red()),
        }
    }
    println!("{}", "Stopped.".dimmed());
    Ok(())
}

/// Ctrl+C, or SIGTERM on Unix — `docker stop` sends that, and as PID 1 in a
/// container the process has no default handler for it.
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = term.recv() => {}
                }
            }
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
