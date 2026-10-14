//! Device secrets — the one place private material lives.
//!
//! Core persists no secrets itself: the platform shell injects a
//! [`SecretStore`] and [`SecureStore`] keeps the consolidated JSON blob (device
//! X25519 keypair + refresh token) behind it.
//!
//! [`SecretStore`] is only the raw "persist these bytes where the OS keeps
//! secrets" primitive, so each shell plugs in the backend that fits its
//! platform:
//!
//! - **Desktop** (`apps/desktop`): the OS keychain via the `keyring` crate
//!   (macOS Keychain / Windows Credential Manager / Linux Secret Service).
//! - **iOS**: Keychain Services · **Android**: Keystore / EncryptedSharedPreferences,
//!   supplied across the FFI as a UniFFI callback interface.
//! - **`ninja` CLI**: its own keyring item, distinct from the desktop app's.
//! - **Headless servers / tests**: a plain file, or [`MemorySecretStore`].
//!
//! Inverting this dependency is what lets Android stop using the `keyring` crate
//! (which has no Android backend and silently falls back to a non-persistent
//! store, regenerating the device key every launch). The single-blob shape is
//! deliberate: all secrets live in *one* opaque entry.
//!
//! Nothing is cached: every read goes to the backend and every write is a
//! read-modify-write of the stored blob. Several processes may share one item
//! (the `sninja` CLI and the commander do), and the refresh token rotates on
//! the server, so a process must always see what the store holds *now* —
//! never its own snapshot from launch time.

use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use crate::utils::base64::{from_base64_special, to_base64_special};

/// The raw secure-storage backend secret persistence is delegated to. One
/// opaque blob in, one out; the structure of that blob is [`SecureStore`]'s
/// concern.
pub trait SecretStore: Send + Sync {
    /// Load the stored secret blob, or `None` if nothing has been stored yet.
    fn load(&self) -> Option<String>;
    /// Persist the secret blob. Best-effort: implementations log failures rather
    /// than propagating them (a missing keychain must not take the app down).
    fn store(&self, blob: String);
}

/// In-memory [`SecretStore`] — for tests and short-lived tools that shouldn't
/// touch the real keychain. Secrets don't survive the process.
#[derive(Default)]
pub struct MemorySecretStore {
    blob: Mutex<Option<String>>,
}

impl SecretStore for MemorySecretStore {
    fn load(&self) -> Option<String> {
        self.blob.lock().unwrap().clone()
    }

    fn store(&self, blob: String) {
        *self.blob.lock().unwrap() = Some(blob);
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct Secrets {
    /// X25519 device public key, base64url.
    #[serde(skip_serializing_if = "Option::is_none")]
    device_public_key: Option<String>,
    /// X25519 device private key, base64url.
    #[serde(skip_serializing_if = "Option::is_none")]
    device_private_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    refresh_token: Option<String>,
}

/// Device-secret manager over a platform [`SecretStore`] backend: owns the
/// blob shape and device-key generation.
pub struct SecureStore {
    backend: Arc<dyn SecretStore>,
}

impl SecureStore {
    pub fn new(backend: Arc<dyn SecretStore>) -> Self {
        Self { backend }
    }

    fn load(&self) -> Secrets {
        self.backend
            .load()
            .and_then(|json| serde_json::from_str::<Secrets>(&json).ok())
            .unwrap_or_default()
    }

    fn store(&self, secrets: Secrets) {
        if let Ok(json) = serde_json::to_string(&secrets) {
            self.backend.store(json);
        }
    }

    /// Return the device keypair `(public, private)` (raw 32-byte each),
    /// generating and persisting one on first use.
    pub fn ensure_device_key(&self) -> (Vec<u8>, Vec<u8>) {
        let secrets = self.load();
        if let (Some(pubk), Some(privk)) = (&secrets.device_public_key, &secrets.device_private_key)
            && let (Ok(pb), Ok(sb)) = (from_base64_special(pubk), from_base64_special(privk))
        {
            return (pb, sb);
        }
        let kp = crate::utils::crypting::generate_x25519_keypair();
        let mut s = secrets;
        s.device_public_key = Some(to_base64_special(&kp.public_key));
        s.device_private_key = Some(to_base64_special(&kp.private_key));
        self.store(s);
        tracing::info!("generated new device key");
        (kp.public_key.to_vec(), kp.private_key.to_vec())
    }

    /// The refresh token the store holds right now — re-read on every call,
    /// so a sibling process's rotation is visible.
    pub fn refresh_token(&self) -> Option<String> {
        self.load().refresh_token
    }

    pub fn set_refresh_token(&self, token: Option<String>) {
        let mut s = self.load();
        s.refresh_token = token;
        self.store(s);
    }
}
