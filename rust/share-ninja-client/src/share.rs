//! Share URLs, keys, and listings.

use anyhow::{Context, Result};
use tokio::sync::RwLock;

use share_ninja_core::back::claims::resolve_share_key;
use share_ninja_core::{StorageObjectInfo, StorageSource};

use crate::session::Session;

/// Parse a share URL and extract the token and optional claim fragment.
/// Supports: `https://share.ninja/TOKEN#CLAIM` / `https://share.ninja/#CLAIM`,
/// `shareninja://download/?token=TOKEN&key=CLAIM`, or just `TOKEN`.
pub fn parse_share_url(url: &str) -> Result<(String, Option<String>)> {
    if url.starts_with("http://") || url.starts_with("https://") {
        let parsed = url::Url::parse(url).context("Invalid URL")?;
        let token = parsed.path().trim_start_matches('/').trim_end_matches('/').to_string();
        let key = parsed.fragment().map(|f| f.to_string());
        Ok((token, key))
    } else if url.starts_with("shareninja://") {
        let parsed = url::Url::parse(url).context("Invalid URL")?;
        let token = parsed
            .query_pairs()
            .find(|(k, _)| k == "token")
            .map(|(_, v)| v.to_string())
            .context("No token in URL")?;
        let key = parsed
            .query_pairs()
            .find(|(k, _)| k == "key")
            .map(|(_, v)| v.to_string());
        Ok((token, key))
    } else {
        Ok((url.to_string(), None))
    }
}

/// Resolve a share URL / token to `(url_token, share_key)` — what
/// `connect_source` needs. Two key channels, picked deterministically:
/// - a claim fragment (`https://…/#<claim>` or an explicit `key`) →
///   `/vault/claims`, whose `url_token` is authoritative;
/// - a bare token → our own key-store row on the link (`GET /links/{token}` →
///   `encrypted_share_key`), unwrapped with the identity or device key — the
///   owner's / an ACL grantee's path (mirrors the App's `resolve_link_share_key`).
pub async fn resolve_share_access(session: &Session, url: &str, key: Option<String>) -> Result<(String, [u8; 32])> {
    let (token, url_key) = parse_share_url(url)?;
    if let Some(key_str) = key.or(url_key) {
        let (key, url_token) = resolve_share_key(&session.back, &key_str).await?;
        return Ok((url_token, key));
    }
    if token.is_empty() {
        anyhow::bail!("share URL carries neither a token nor a claim fragment");
    }
    let link = session
        .back
        .read()
        .await
        .link_get(&token, None)
        .await
        .with_context(|| format!("Failed to fetch link {}", token))?;
    let esk = link.encrypted_share_key.as_deref().ok_or_else(|| {
        anyhow::anyhow!(
            "no key for {}: you have no access row on this link — pass its share URL (#claim)",
            token
        )
    })?;
    let key = session.unwrap_own_share_key(esk).ok_or_else(|| {
        anyhow::anyhow!(
            "cannot unwrap the key for {}: it is sealed to your account identity — enrol this device in the vault",
            token
        )
    })?;
    Ok((token, key))
}

/// List a connected share source under `path` via the streamed `get_dir` —
/// the listing primitive every source implements (P2P lists live, ninja from
/// its journal). Batches are drained into one vector.
pub async fn list_share<S: StorageSource + ?Sized>(
    source: &RwLock<S>,
    path: &str,
    recursive: bool,
) -> Result<Vec<StorageObjectInfo>> {
    let mut rx = source.write().await.get_dir(path, recursive, None, Some(500)).await;
    let mut items = Vec::new();
    while let Some(batch) = rx.recv().await {
        items.extend(batch.context("Failed to list share")?);
    }
    Ok(items)
}

/// Format a byte size as a human-readable string (1024-based).
pub fn format_size(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}
