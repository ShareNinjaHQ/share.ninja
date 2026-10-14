//! Vault — the account's E2E identity key (the device-trust root) as every
//! client drives it: joining the trust circle and unlocking the identity a
//! trusted device wrapped for us (device side); approving/rejecting joins,
//! revoking devices, recovery-phrase material, recover and reset (trusted
//! side). Pure crypto + backend calls, no client state: the App orchestrator,
//! the `ninja` CLI and the commander keep their own state/events over this.
//! Mirrors the TS `vaultSetup` / `vaultUnlock` / `vaultJoinStart` /
//! `approveVaultJoinRequest` / `vaultRecover`.

use std::sync::Arc;

use tokio::sync::RwLock;

use rand::Rng;

use crate::back::{Back, BackError};
use crate::models;
use crate::utils::base64::from_base64;
use crate::utils::crypting::{
    aes_gcm_decrypt, aes_gcm_encrypt, derive_key, generate_x25519_keypair, hybrid_decrypt, hybrid_encrypt,
};

/// HKDF info under which a trusted device wraps the identity private key for
/// another device (`vault.device_approved` / register `encrypted_identity_key`).
pub const IDENTITY_WRAP: &str = "vault:identity-wrap-v1";
/// HKDF info of the recovery key derived from the 24-word phrase's entropy.
pub const RECOVERY: &str = "vault:recovery-v1";

/// The unlocked vault identity keypair (the account E2E root).
#[derive(Clone)]
pub struct VaultIdentity {
    pub private: [u8; 32],
    pub public: [u8; 32],
}

#[derive(Debug, thiserror::Error)]
pub enum VaultError {
    #[error("vault key is not 32 bytes")]
    BadKey,

    #[error("vault crypto: {0}")]
    Crypto(String),

    #[error("no identity_public_key on user")]
    NoIdentityPublicKey,

    #[error("vault active but no encrypted_identity_key")]
    NoEncryptedIdentityKey,

    #[error("bad recovery phrase: {0}")]
    BadRecoveryPhrase(String),

    /// A backend call failed; `op` names it (`vault_device_register`, …).
    #[error("{op}: {source}")]
    Back { op: &'static str, source: BackError },
}

pub type Result<T> = std::result::Result<T, VaultError>;

pub fn key32(v: &[u8]) -> Result<[u8; 32]> {
    v.try_into().map_err(|_| VaultError::BadKey)
}

fn crypt_err(e: impl std::fmt::Display) -> VaultError {
    VaultError::Crypto(e.to_string())
}

fn back_err(op: &'static str) -> impl FnOnce(BackError) -> VaultError {
    move |source| VaultError::Back { op, source }
}

/// Decrypt a device-wrapped identity private key (`vault:identity-wrap-v1`);
/// the public key comes from the backend user record. Shared by unlock +
/// approval.
pub fn decrypt_identity(
    encrypted: &[u8],
    device_priv: &[u8; 32],
    identity_pub_b64: Option<String>,
) -> Result<VaultIdentity> {
    let priv_bytes =
        hybrid_decrypt(encrypted, device_priv, Some(IDENTITY_WRAP)).map_err(|e| VaultError::Crypto(e.to_string()))?;
    let private = key32(&priv_bytes)?;
    let public = identity_pub_b64
        .and_then(|s| from_base64(&s).ok())
        .map(|v| key32(&v))
        .transpose()?
        .ok_or(VaultError::NoIdentityPublicKey)?;
    Ok(VaultIdentity { private, public })
}

/// Active vault, this device already trusted: decrypt the device-wrapped
/// identity the register response carried.
pub fn unlock(
    enc_identity: Option<Vec<u8>>,
    device: ([u8; 32], [u8; 32]),
    identity_pub_b64: Option<String>,
) -> Result<VaultIdentity> {
    let enc = enc_identity.ok_or(VaultError::NoEncryptedIdentityKey)?;
    decrypt_identity(&enc, &device.1, identity_pub_b64)
}

/// Handle an inbound `vault.device_approved` — decrypt the identity key a
/// trusted device wrapped for us. Mirrors the TS `handleVaultDeviceApproved`.
pub fn handle_device_approved(
    encrypted_identity_key: &[u8],
    device: ([u8; 32], [u8; 32]),
    identity_pub_b64: Option<String>,
) -> Result<VaultIdentity> {
    decrypt_identity(encrypted_identity_key, &device.1, identity_pub_b64)
}

/// Ask to join the trust circle. No body — the backend already knows the
/// calling agent's device key and name from its registration. Returns the
/// standing request: the device fingerprint the user verifies on the
/// approving device, and the trust-circle devices the request went to.
pub async fn join_start(back: &Arc<RwLock<Back>>) -> Result<models::VaultDeviceJoinResponse> {
    back.read()
        .await
        .vault_device_join()
        .await
        .map_err(back_err("vault_device_join"))
}

// ── trusted side ────────────────────────────────────────────────────────────

/// First device: generate the identity keypair, register it (wrapped under
/// the device key) and return it. No recovery blob is created here — the
/// recovery key is a deliberate later step (`regenerate_recovery` from
/// Settings), so a fresh account has none until the user sets one up.
pub async fn setup(back: &Arc<RwLock<Back>>, device: ([u8; 32], [u8; 32])) -> Result<VaultIdentity> {
    let (device_pub, _device_priv) = device;
    let kp = generate_x25519_keypair();

    let enc_identity = hybrid_encrypt(&kp.private_key, &device_pub, Some(IDENTITY_WRAP)).map_err(crypt_err)?;
    back.read()
        .await
        .vault_device_register(models::VaultDeviceRegisterRequest {
            device_public_key: device_pub.to_vec(),
            encrypted_identity_key: enc_identity,
            identity_public_key: kp.public_key.to_vec(),
            encrypted_identity_key_for_recovery: None,
        })
        .await
        .map_err(back_err("vault_device_register"))?;

    Ok(VaultIdentity {
        private: kp.private_key,
        public: kp.public_key,
    })
}

/// Fresh recovery material for an identity: 32 bytes of entropy become the
/// 24 BIP-39 words, and the derived recovery key (`vault:recovery-v1`) wraps
/// the identity private key. Returns (wrapped identity, words). Used by the
/// Settings set-up / regenerate — the only place a recovery key is minted.
pub fn make_recovery_blob(identity_priv: &[u8; 32]) -> Result<(Vec<u8>, Vec<String>)> {
    let mut entropy = [0u8; 32];
    rand::rng().fill_bytes(&mut entropy);
    let recovery_key = derive_key(&entropy, RECOVERY).map_err(crypt_err)?;
    let enc_recovery = aes_gcm_encrypt(identity_priv, &recovery_key).map_err(crypt_err)?;
    let mnemonic = bip39::Mnemonic::from_entropy(&entropy).map_err(|e| crypt_err(format!("bip39: {e}")))?;
    let words = mnemonic.words().map(|w| w.to_string()).collect();
    Ok((enc_recovery, words))
}

/// Store a recovery phrase for the unlocked identity — the account's first
/// one, or a replacement (the old phrase stops working). Returns the 24
/// words — show them once.
pub async fn regenerate_recovery(back: &Arc<RwLock<Back>>, identity: &VaultIdentity) -> Result<Vec<String>> {
    let (enc_recovery, words) = make_recovery_blob(&identity.private)?;
    back.read()
        .await
        .vault_recovery_upsert(models::VaultRecoveryRequest {
            encrypted_identity_key_for_recovery: enc_recovery,
        })
        .await
        .map_err(back_err("vault_recovery_upsert"))?;
    Ok(words)
}

/// Full reset material (Vault.md Recovery Options, Variant D): a brand-new
/// identity keypair wrapped under this device. No recovery blob, as at
/// `setup` — the backend deletes the old recovery key with the old identity,
/// and the user sets a new one up later from Settings (`regenerate_recovery`).
/// Nothing is sent — the caller submits the request (step-up gated) and only
/// adopts the identity once the backend accepted it.
pub fn reset_prepare(device: ([u8; 32], [u8; 32])) -> Result<(models::VaultResetRequest, VaultIdentity)> {
    let (device_pub, _) = device;
    let kp = generate_x25519_keypair();
    let enc_identity = hybrid_encrypt(&kp.private_key, &device_pub, Some(IDENTITY_WRAP)).map_err(crypt_err)?;
    let req = models::VaultResetRequest {
        device_public_key: device_pub.to_vec(),
        identity_public_key: kp.public_key.to_vec(),
        encrypted_identity_key: enc_identity,
        encrypted_identity_key_for_recovery: None,
    };
    Ok((
        req,
        VaultIdentity {
            private: kp.private_key,
            public: kp.public_key,
        },
    ))
}

/// Wrap our identity private key for a joining device (the approve payload).
pub fn wrap_identity_for(device_public_key: &[u8], identity: &VaultIdentity) -> Result<Vec<u8>> {
    let new_pub = key32(device_public_key)?;
    hybrid_encrypt(&identity.private, &new_pub, Some(IDENTITY_WRAP)).map_err(crypt_err)
}

/// A trusted device approves a join request: wrap our identity for the
/// requesting device's public key and submit. Mirrors the TS
/// `approveVaultJoinRequest`.
pub async fn approve_join(
    back: &Arc<RwLock<Back>>,
    agent_id: &str,
    device_public_key: &[u8],
    identity: &VaultIdentity,
) -> Result<()> {
    let enc = wrap_identity_for(device_public_key, identity)?;
    back.read()
        .await
        .vault_device_approve(
            agent_id,
            models::VaultDeviceApproveRequest {
                encrypted_identity_key: enc,
            },
        )
        .await
        .map(|_| ())
        .map_err(back_err("vault_device_approve"))
}

pub async fn reject_join(back: &Arc<RwLock<Back>>, agent_id: &str) -> Result<()> {
    back.read()
        .await
        .vault_device_reject(agent_id)
        .await
        .map_err(back_err("vault_device_reject"))
}

/// The backend says the join request already reached a terminal outcome
/// (decided on another device, expired): nothing left to decide, so a
/// decision landing on it is settled, not failed.
pub fn join_already_decided(e: &BackError) -> bool {
    matches!(e, BackError::Api { code, .. } if code.starts_with("join_request_"))
}

pub async fn revoke_device(back: &Arc<RwLock<Back>>, agent_id: &str) -> Result<()> {
    back.read()
        .await
        .vault_device_revoke(agent_id)
        .await
        .map_err(back_err("vault_device_revoke"))
}

/// Recover the vault from the BIP-39 mnemonic: decrypt the recovery blob and
/// re-wrap it under this device. Returns the `recovery/complete` request and
/// the identity it carries — the caller submits the request (step-up gated,
/// so it may have to be retried byte-identically with a confirmed token) and
/// adopts the identity only once the backend accepted it. Mirrors the TS
/// `vaultRecover` split around `withStepUp`.
pub async fn recover_prepare(
    back: &Arc<RwLock<Back>>,
    mnemonic: &str,
    device: ([u8; 32], [u8; 32]),
    identity_pub_b64: Option<String>,
) -> Result<(models::VaultRecoveryCompleteRequest, VaultIdentity)> {
    let m =
        bip39::Mnemonic::parse_normalized(mnemonic.trim()).map_err(|e| VaultError::BadRecoveryPhrase(e.to_string()))?;
    let entropy = m.to_entropy();
    let recovery_key = derive_key(&entropy, RECOVERY).map_err(crypt_err)?;
    let (device_pub, _device_priv) = device;

    let result = back
        .read()
        .await
        .vault_recovery_recover(models::VaultRecoveryRecoverRequest {
            device_public_key: device_pub.to_vec(),
        })
        .await
        .map_err(back_err("vault_recovery_recover"))?;
    let identity_priv =
        aes_gcm_decrypt(&result.encrypted_identity_key_for_recovery, &recovery_key).map_err(crypt_err)?;
    let private = key32(&identity_priv)?;

    // identity_public_key (proof of decryption) comes from the server user record.
    let identity_pub_bytes = identity_pub_b64
        .and_then(|s| from_base64(&s).ok())
        .ok_or(VaultError::NoIdentityPublicKey)?;
    let public = key32(&identity_pub_bytes)?;

    let enc_identity = hybrid_encrypt(&identity_priv, &device_pub, Some(IDENTITY_WRAP)).map_err(crypt_err)?;
    let req = models::VaultRecoveryCompleteRequest {
        device_public_key: device_pub.to_vec(),
        encrypted_identity_key: enc_identity,
        identity_public_key: identity_pub_bytes,
    };
    Ok((req, VaultIdentity { private, public }))
}

/// The BIP-39 English wordlist the recovery phrase is drawn from, for
/// per-word validation of a typed phrase (the one source, so the entry UI
/// can't drift from what `recover_prepare` decodes against).
pub fn bip39_wordlist() -> Vec<String> {
    bip39::Language::English
        .word_list()
        .iter()
        .map(|w| w.to_string())
        .collect()
}
