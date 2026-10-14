//! Vault claims — the URL-fragment key channel (Vault.md §4.2).
//!
//! A share URL carries a random 12-byte *claim token* in its fragment, never
//! the raw share key. The share key is wrapped under
//! HKDF(claim_token, "vault:claim-v1") and stored server-side against
//! SHA256(claim_token); receivers fetch the row via `/vault/claims/{hash}` and
//! unwrap it. The server never sees the claim token itself. Mirrors the
//! frontend's `createLink()` / `resolveShareKey()`; shared by the App
//! orchestrator and the `ninja` CLI.

use std::sync::Arc;

use rand::Rng;
use sha2::{Digest, Sha256};
use tokio::sync::RwLock;

use crate::back::{Back, BackError};
use crate::models;
use crate::utils::base58::{from_base58, to_base58};
use crate::utils::base64::to_base64_special;
use crate::utils::crypting::{aes_gcm_decrypt, aes_gcm_encrypt, derive_key};

/// HKDF info for the claim key.
pub const CLAIM_KEY_INFO: &str = "vault:claim-v1";

/// Claim token length: 96 bits — the floor for resisting an offline brute
/// force by the server itself (it stores only the hash).
pub const CLAIM_TOKEN_LEN: usize = 12;

#[derive(Debug, thiserror::Error)]
pub enum ClaimError {
    /// The URL fragment isn't base58.
    #[error("invalid base58 claim token: {0}")]
    BadToken(String),

    /// The fragment decoded to something other than a 12-byte claim token.
    #[error("claim token must be 12 bytes (got {0})")]
    BadLength(usize),

    /// `/vault/claims` has no row for this token (revoked or expired).
    #[error("share link claim not found (revoked or expired?)")]
    NotFound,

    /// Key derivation / AES failure, or a malformed unwrapped key.
    #[error("{0}")]
    Crypto(String),

    /// The backend call itself failed.
    #[error(transparent)]
    Back(BackError),
}

pub type Result<T> = std::result::Result<T, ClaimError>;

/// Create a vault claim for a freshly-created share and return the *claim
/// token* to place in the URL fragment (base58, up to 17 chars — base58 so a
/// human can read the link aloud without confusable glyphs).
pub async fn create_share_claim(back: &Arc<RwLock<Back>>, url_token: &str, master_key: &[u8; 32]) -> Result<String> {
    tracing::debug!(url_token = %url_token, "create_share_claim");

    let mut claim_token = [0u8; CLAIM_TOKEN_LEN];
    rand::rng().fill_bytes(&mut claim_token);

    let claim_key =
        derive_key(&claim_token, CLAIM_KEY_INFO).map_err(|e| ClaimError::Crypto(format!("derive claim key: {e}")))?;

    // Wrap the master key: IV(12) || ciphertext || tag(16).
    let encrypted_share_key =
        aes_gcm_encrypt(master_key, &claim_key).map_err(|e| ClaimError::Crypto(format!("wrap share key: {e}")))?;

    // Server stores only SHA256(claim_token).
    let claim_token_hash = Sha256::digest(claim_token).to_vec();

    let request = models::VaultClaimCreateRequest::new(url_token.to_string(), claim_token_hash, encrypted_share_key);
    back.read()
        .await
        .vault_claim_create(request)
        .await
        .map_err(ClaimError::Back)?;

    Ok(to_base58(&claim_token))
}

/// Unwrap the real 32-byte share key for a raw claim token via `/vault/claims`.
/// Returns the key plus the claim's `url_token` — the authoritative token for
/// the download (a token-less share URL has no token in its path). `Ok(None)`
/// when the backend has no claim for this token.
pub async fn resolve_claim(back: &Arc<RwLock<Back>>, claim_token: &[u8]) -> Result<Option<([u8; 32], String)>> {
    tracing::debug!("resolve_claim");

    let hash_b64 = to_base64_special(&Sha256::digest(claim_token));
    let claim = match back.read().await.vault_claim_get(hash_b64).await {
        Ok(c) => c,
        Err(e) => {
            tracing::debug!("no vault claim for token: {e}");
            return Ok(None);
        }
    };

    let claim_key =
        derive_key(claim_token, CLAIM_KEY_INFO).map_err(|e| ClaimError::Crypto(format!("derive claim key: {e}")))?;
    let plaintext = aes_gcm_decrypt(&claim.encrypted_share_key, &claim_key)
        .map_err(|e| ClaimError::Crypto(format!("claim decrypt: {e}")))?;
    let key: [u8; 32] = plaintext
        .as_slice()
        .try_into()
        .map_err(|_| ClaimError::Crypto("decrypted share key is not 32 bytes".into()))?;
    Ok(Some((key, claim.url_token)))
}

/// Resolve the 32-byte share key from a URL fragment — a base58 12-byte claim
/// token, the only key form that rides URLs. Returns the key plus the claim's
/// `url_token`.
pub async fn resolve_share_key(back: &Arc<RwLock<Back>>, key_str: &str) -> Result<([u8; 32], String)> {
    tracing::debug!("resolve_share_key");
    let fragment = from_base58(key_str).map_err(|e| ClaimError::BadToken(e.to_string()))?;
    if fragment.len() != CLAIM_TOKEN_LEN {
        return Err(ClaimError::BadLength(fragment.len()));
    }
    match resolve_claim(back, &fragment).await? {
        Some((key, url_token)) => Ok((key, url_token)),
        None => Err(ClaimError::NotFound),
    }
}
