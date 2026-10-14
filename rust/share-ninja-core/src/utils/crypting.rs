//! Cryptographic utilities for metadata encryption, X25519 key exchange, and hybrid encryption.
//!
//! Ported from the TypeScript reference library (`utils/crypting.ts`).

use aes_gcm::aead::Aead;
use aes_gcm::{Aes256Gcm, KeyInit, Nonce};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use hkdf::Hkdf;
use rand::Rng;
use sha2::Sha256;
use x25519_dalek::{PublicKey, StaticSecret};

/// Cipher suite version bytes (§3.3 Crypto Versioning in Vault.md)
pub const CRYPTO_SUITE_V1: u8 = 0x01;
pub const CRYPTO_SUITE_LATEST: u8 = CRYPTO_SUITE_V1;

/// Derive a 256-bit AES key from a master key using HKDF-SHA256.
///
/// Uses an empty salt and the provided `info` string for domain separation.
pub fn derive_key(master_key: &[u8], info: &str) -> Result<[u8; 32], CryptingError> {
    let hk = Hkdf::<Sha256>::new(None, master_key);
    let mut okm = [0u8; 32];
    hk.expand(info.as_bytes(), &mut okm)
        .map_err(|e| CryptingError::Hkdf(e.to_string()))?;
    Ok(okm)
}

/// Encrypt data with AES-256-GCM.
///
/// Returns `IV (12 bytes) || ciphertext || auth_tag (16 bytes)`.
pub fn aes_gcm_encrypt(plaintext: &[u8], key: &[u8; 32]) -> Result<Vec<u8>, CryptingError> {
    let cipher = Aes256Gcm::new(key.into());
    let mut iv = [0u8; 12];
    rand::rng().fill_bytes(&mut iv);
    let nonce = Nonce::try_from(&iv[..]).map_err(|_| CryptingError::Aes("invalid nonce length".to_string()))?;
    let ciphertext = cipher
        .encrypt(&nonce, plaintext)
        .map_err(|e| CryptingError::Aes(e.to_string()))?;

    let mut result = Vec::with_capacity(12 + ciphertext.len());
    result.extend_from_slice(&iv);
    result.extend_from_slice(&ciphertext);
    Ok(result)
}

/// Decrypt data produced by [`aes_gcm_encrypt`].
///
/// Expects `IV (12 bytes) || ciphertext || auth_tag (16 bytes)`.
pub fn aes_gcm_decrypt(data: &[u8], key: &[u8; 32]) -> Result<Vec<u8>, CryptingError> {
    if data.len() < 12 + 16 {
        return Err(CryptingError::DataTooShort("AES-GCM data too short".into()));
    }
    let (iv, ciphertext) = data.split_at(12);
    let cipher = Aes256Gcm::new(key.into());
    let nonce = Nonce::try_from(iv).map_err(|_| CryptingError::Aes("invalid nonce length".to_string()))?;
    cipher
        .decrypt(&nonce, ciphertext)
        .map_err(|e| CryptingError::Aes(e.to_string()))
}

/// The plaintext inside a link's `encrypted_metadata` — what
/// [`encrypt_metadata`] seals with the share key at link creation and
/// [`decrypt_metadata`] opens on the receiving side.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LinkMetadata {
    pub description: Option<String>,
}

/// Decrypt a link's description: unwrap its `encrypted_share_key` with the
/// private key it was sealed to (vault identity or device key,
/// `vault:hybrid-v1`), then `decrypt_metadata`. Mirrors the TS
/// `decryptShareKey` → `decryptMetadata`. `None` if we can't.
pub fn decrypt_link_description(
    encrypted_share_key: &[u8],
    encrypted_metadata: &[u8],
    private: &[u8; 32],
) -> Option<String> {
    let share_key = hybrid_decrypt(encrypted_share_key, private, None).ok()?;
    let share_key: [u8; 32] = share_key.as_slice().try_into().ok()?;
    decrypt_metadata_description(encrypted_metadata, &share_key)
}

/// Decrypt a link's `encrypted_metadata` description with an already-unwrapped
/// share key (e.g. one that arrived device-sealed on a share offer). `None` if
/// we can't.
pub fn decrypt_metadata_description(encrypted_metadata: &[u8], share_key: &[u8; 32]) -> Option<String> {
    // The openapi `Vec<u8>` may arrive as the base64 text or the raw blob; try both.
    if let Ok(s) = std::str::from_utf8(encrypted_metadata)
        && let Ok(m) = decrypt_metadata::<LinkMetadata>(s, share_key)
    {
        return m.description;
    }
    decrypt_metadata::<LinkMetadata>(&STANDARD.encode(encrypted_metadata), share_key)
        .ok()
        .and_then(|m| m.description)
}

/// Encrypt a JSON-serializable value as metadata.
///
/// Uses HKDF to derive a metadata-specific key from `master_key`, then
/// AES-256-GCM encrypts the JSON representation.
/// Returns a base64-encoded string of `IV || ciphertext || tag`.
pub fn encrypt_metadata<T: serde::Serialize>(data: &T, master_key: &[u8; 32]) -> Result<String, CryptingError> {
    let json = serde_json::to_string(data).map_err(|e| CryptingError::Serialization(e.to_string()))?;
    let metadata_key = derive_key(master_key, "sharon-metadata-v1")?;
    let encrypted = aes_gcm_encrypt(json.as_bytes(), &metadata_key)?;
    Ok(STANDARD.encode(&encrypted))
}

/// Decrypt base64-encoded metadata produced by [`encrypt_metadata`].
pub fn decrypt_metadata<T: serde::de::DeserializeOwned>(
    encrypted_base64: &str,
    master_key: &[u8; 32],
) -> Result<T, CryptingError> {
    let encrypted = STANDARD
        .decode(encrypted_base64)
        .map_err(|e| CryptingError::Base64(e.to_string()))?;
    let metadata_key = derive_key(master_key, "sharon-metadata-v1")?;
    let decrypted = aes_gcm_decrypt(&encrypted, &metadata_key)?;
    serde_json::from_slice(&decrypted).map_err(|e| CryptingError::Serialization(e.to_string()))
}

/// X25519 keypair (raw bytes).
pub struct X25519Keypair {
    pub public_key: [u8; 32],
    pub private_key: [u8; 32],
}

/// Generate an X25519 keypair.
pub fn generate_x25519_keypair() -> X25519Keypair {
    let mut private_bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut private_bytes);
    let secret = StaticSecret::from(private_bytes);
    let public = PublicKey::from(&secret);
    X25519Keypair {
        public_key: *public.as_bytes(),
        private_key: secret.to_bytes(),
    }
}

/// Perform X25519 Diffie-Hellman to derive a 32-byte shared secret.
pub fn x25519_derive_shared_secret(our_private_key: &[u8; 32], their_public_key: &[u8; 32]) -> [u8; 32] {
    let secret = StaticSecret::from(*our_private_key);
    let public = PublicKey::from(*their_public_key);
    *secret.diffie_hellman(&public).as_bytes()
}

/// HKDF info string of the hybrid (X25519 + AES-GCM) wrap: the default for
/// `hybrid_encrypt` / `hybrid_decrypt`, and what every device-sealed share
/// key (nearby offers, ACL grants) is wrapped under.
pub const P2P_HYBRID: &str = "vault:hybrid-v1";

/// Hybrid encryption: encrypt `plaintext` to a recipient's X25519 public key.
///
/// Wire format: `version (1) || ephemeral_pubkey (32) || IV (12) || ciphertext || auth_tag (16)`.
///
/// Uses HKDF with the given `info` string (default `"vault:hybrid-v1"`) to
/// derive the AES-256-GCM key from the ECDH shared secret.
pub fn hybrid_encrypt(
    plaintext: &[u8],
    recipient_public_key: &[u8; 32],
    info: Option<&str>,
) -> Result<Vec<u8>, CryptingError> {
    let info = info.unwrap_or(P2P_HYBRID);

    let mut eph_bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut eph_bytes);
    let ephemeral_secret = StaticSecret::from(eph_bytes);
    let ephemeral_public = PublicKey::from(&ephemeral_secret);
    let recipient_pk = PublicKey::from(*recipient_public_key);
    let shared_secret = ephemeral_secret.diffie_hellman(&recipient_pk);

    let aes_key = derive_key(shared_secret.as_bytes(), info)?;
    let encrypted = aes_gcm_encrypt(plaintext, &aes_key)?;

    // Wire format: version (1) || ephemeral_pubkey (32) || IV (12) || ciphertext || auth_tag (16)
    let mut result = Vec::with_capacity(1 + 32 + encrypted.len());
    result.push(CRYPTO_SUITE_LATEST);
    result.extend_from_slice(ephemeral_public.as_bytes());
    result.extend_from_slice(&encrypted);
    Ok(result)
}

/// Hybrid decryption: decrypt data encrypted with [`hybrid_encrypt`] using our
/// X25519 private key.
///
/// Expects wire format: `version (1) || ephemeral_pubkey (32) || IV (12) || ciphertext || auth_tag (16)`.
pub fn hybrid_decrypt(data: &[u8], private_key: &[u8; 32], info: Option<&str>) -> Result<Vec<u8>, CryptingError> {
    let info = info.unwrap_or(P2P_HYBRID);

    if data.len() < 1 + 32 + 12 + 16 {
        return Err(CryptingError::DataTooShort(
            "Data too short for hybrid decryption".into(),
        ));
    }

    let version = data[0];
    if version != CRYPTO_SUITE_V1 {
        return Err(CryptingError::UnsupportedVersion(format!(
            "Unknown crypto suite version: 0x{:02x}",
            version
        )));
    }

    let (ephemeral_pubkey_bytes, encrypted) = data[1..].split_at(32);
    let mut epk = [0u8; 32];
    epk.copy_from_slice(ephemeral_pubkey_bytes);

    let ephemeral_pk = PublicKey::from(epk);
    let our_secret = StaticSecret::from(*private_key);
    let shared_secret = our_secret.diffie_hellman(&ephemeral_pk);

    let aes_key = derive_key(shared_secret.as_bytes(), info)?;
    aes_gcm_decrypt(encrypted, &aes_key)
}

/// Errors from cryptographic operations.
#[derive(Debug, thiserror::Error)]
pub enum CryptingError {
    #[error("HKDF error: {0}")]
    Hkdf(String),

    #[error("AES-GCM error: {0}")]
    Aes(String),

    #[error("Base64 error: {0}")]
    Base64(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Data too short: {0}")]
    DataTooShort(String),

    #[error("Unsupported version: {0}")]
    UnsupportedVersion(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aes_gcm_roundtrip() {
        let key = [0x42u8; 32];
        let plaintext = b"Hello, world!";
        let encrypted = aes_gcm_encrypt(plaintext, &key).unwrap();
        let decrypted = aes_gcm_decrypt(&encrypted, &key).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_derive_key_deterministic() {
        let master = [0xAAu8; 32];
        let k1 = derive_key(&master, "test-info").unwrap();
        let k2 = derive_key(&master, "test-info").unwrap();
        assert_eq!(k1, k2);

        let k3 = derive_key(&master, "other-info").unwrap();
        assert_ne!(k1, k3);
    }

    #[test]
    fn test_encrypt_decrypt_metadata() {
        let master_key = [0xBBu8; 32];
        let data = serde_json::json!({"name": "test", "value": 42});
        let encrypted = encrypt_metadata(&data, &master_key).unwrap();
        let decrypted: serde_json::Value = decrypt_metadata(&encrypted, &master_key).unwrap();
        assert_eq!(data, decrypted);
    }

    #[test]
    fn test_x25519_shared_secret() {
        let alice = generate_x25519_keypair();
        let bob = generate_x25519_keypair();
        let secret_ab = x25519_derive_shared_secret(&alice.private_key, &bob.public_key);
        let secret_ba = x25519_derive_shared_secret(&bob.private_key, &alice.public_key);
        assert_eq!(secret_ab, secret_ba);
    }

    #[test]
    fn test_hybrid_roundtrip() {
        let recipient = generate_x25519_keypair();
        let plaintext = b"Secret message for hybrid encryption";
        let encrypted = hybrid_encrypt(plaintext, &recipient.public_key, None).unwrap();
        let decrypted = hybrid_decrypt(&encrypted, &recipient.private_key, None).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_hybrid_decrypt_too_short() {
        let key = [0u8; 32];
        let result = hybrid_decrypt(&[0u8; 10], &key, None);
        assert!(result.is_err());
    }
}
