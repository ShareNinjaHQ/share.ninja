//! Direct command - share files straight from this machine over P2P, serving
//! them for as long as the process runs

use anyhow::Result;
use colored::Colorize;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

use share_ninja_client::ws::BackWsExt;
use share_ninja_core::{Back, StorageSource, back::CallInInfo, models, p2p::CallServer};

use super::prepare::{prepare_share, print_share_link};
use crate::config::Config;

pub async fn execute(
    cfg: &Config,
    paths: Vec<PathBuf>,
    description: Option<String>,
    validity: i64,
    ttl: Option<u64>,
) -> Result<()> {
    let share = prepare_share(cfg, paths, description, validity, false).await?;

    println!("{} Waiting for connections...", "📡".blue());
    println!("  Files are transferred directly when someone downloads.\n");

    // Set up call handler for P2P connections
    let _call_server = setup_call_handler(share.back.clone(), share.source.clone(), Some(share.encryption_key)).await?;

    // Print the share link immediately so the user can share it.
    print_share_link(&share);

    println!("\n{} Keep this terminal open to serve files.", "ℹ️".cyan());
    if let Some(ttl_secs) = ttl {
        println!("  Will exit after {} seconds (--ttl).", ttl_secs);
    } else {
        println!("  Press Ctrl+C to stop sharing.");
    }
    println!();

    // Wait for P2P connections (with optional TTL timeout)
    if let Some(ttl_secs) = ttl {
        tokio::time::sleep(tokio::time::Duration::from_secs(ttl_secs)).await;
        println!("{} TTL expired, exiting.", "ℹ️".cyan());
    } else {
        // Wait indefinitely
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
        }
    }

    Ok(())
}

/// Serve this share's files to incoming P2P calls. Mirrors the desktop App's
/// `serve_call`: the raw WS frames are filtered for `call.incoming`, duplicate
/// deliveries are dropped via `CallServer::is_call_active` (the server tracks
/// active serves itself, keyed by url_token / call id), relay-less calls are
/// refused, and the call is answered over the relay path.
async fn setup_call_handler(
    back: Arc<RwLock<Back>>,
    source: Arc<RwLock<dyn StorageSource + Send + Sync>>,
    encryption_key: Option<[u8; 32]>,
) -> Result<Arc<Mutex<CallServer>>> {
    let call_server = Arc::new(Mutex::new(CallServer::new()));
    {
        let mut cs = call_server.lock().await;
        cs.set_back(back.clone());
    }

    let call_server_clone = call_server.clone();
    let source_clone = source.clone();
    {
        let back_guard = back.read().await;
        back_guard
            .set_ws_message_handler(move |msg: models::WsMessage| {
                // We only care about call.incoming.
                let models::WsMessage::CallIncoming(data) = msg else {
                    return;
                };
                let ci = CallInInfo::from_ws_call(*data);
                tracing::debug!("Received incoming call: {}", ci.call_id);

                let call_server = call_server_clone.clone();
                let source = source_clone.clone();
                tokio::spawn(async move {
                    let cs = call_server.lock().await;
                    if cs.is_call_active(&ci.call_id).await {
                        tracing::debug!("Call {} already being handled, ignoring duplicate", ci.call_id);
                        return;
                    }
                    if ci.relay.is_none() {
                        tracing::warn!("Call {} without relay — not supported", ci.call_id);
                        return;
                    }
                    let peer_info = ci.peer_name.as_deref().unwrap_or("unknown");
                    println!(
                        "  {} Incoming P2P connection from {} (call: {})",
                        "📞".green(),
                        peer_info,
                        ci.call_id
                    );
                    if let Err(e) = cs.on_call_with_relay(source, ci, encryption_key).await {
                        tracing::error!("Failed to setup relay call: {}", e);
                    }
                });
            })
            .await;
    }

    Ok(call_server)
}
