//! A registered backend connection plus what this device holds for E2E.

use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::sync::RwLock;

use share_ninja_core::utils::base64::{from_base64, from_base64_special, to_base64};
use share_ninja_core::utils::crypting::{hybrid_decrypt, hybrid_encrypt};
use share_ninja_core::vault::{self, VaultIdentity};
use share_ninja_core::{Back, BackParams};

use crate::keychain::secure_store;

/// What a binary contributes to its backend registration.
pub struct ClientParams {
    /// Server origin, e.g. `https://share.ninja` (no trailing slash, no `/api`).
    pub base_url: String,
    /// Product id — one per binary, the same on every platform.
    pub product_id: &'static str,
    pub product_version: &'static str,
    pub user_agent: String,
    pub device_name: String,
    /// Keychain item (service attribute) holding this binary's secrets blob.
    pub keychain_service: &'static str,
    /// A file holding the secrets blob instead of the keychain — for hosts
    /// without one (containers). Must stay writable: the refresh token rotates.
    pub secrets_file: Option<std::path::PathBuf>,
    pub log_api: bool,
    /// Runtime instance id for `POST /auth/agents` and every P2P link created
    /// (see `BackParams::instance_id`). `None` leaves links agent-scoped.
    pub instance_id: Option<String>,
}

impl ClientParams {
    /// REST base, e.g. `https://share.ninja/api`.
    pub fn api_url(&self) -> String {
        format!("{}/api", self.base_url)
    }

    /// WebSocket base, e.g. `https://share.ninja/ws` (core swaps the scheme).
    pub fn ws_url(&self) -> String {
        format!("{}/ws", self.base_url)
    }
}

/// One registered backend connection plus what this device holds for E2E:
/// its keypair and — when the vault is active and this device is trusted —
/// the unlocked identity, decrypted from the device-wrapped key the register
/// response carried (in memory only, like the desktop app).
pub struct Session {
    pub back: Arc<RwLock<Back>>,
    /// `(public, private)` device keypair from the keychain blob.
    pub device: ([u8; 32], [u8; 32]),
    /// Raw `vault_status` from register: `not_initialized` / `not_joined` /
    /// `pending_approval` / `active`; `None` when anonymous.
    pub vault_status: Option<String>,
    pub identity: Option<VaultIdentity>,
}

impl Session {
    /// Unwrap one of our own key-store rows (`encrypted_share_key`) with the
    /// root it was sealed to: the unlocked identity when we have it, else this
    /// device's key (anonymous shares made by this binary). Same rule as the
    /// desktop app's `decrypt_share_key` — deterministic, no trial decrypts.
    pub fn unwrap_own_share_key(&self, encrypted_share_key: &[u8]) -> Option<[u8; 32]> {
        let root = match &self.identity {
            Some(id) => &id.private,
            None => &self.device.1,
        };
        let key = hybrid_decrypt(encrypted_share_key, root, None).ok()?;
        key.as_slice().try_into().ok()
    }

    /// The identity public key from the user record (needed to unlock).
    pub async fn identity_public_key_b64(&self) -> Option<String> {
        self.back
            .read()
            .await
            .user
            .as_ref()
            .and_then(|u| u.identity_public_key.clone().flatten())
    }

    /// Re-read the vault standing after the backend re-registered (login,
    /// logout, approval): status + a fresh unlock attempt.
    pub async fn refresh_vault(&mut self) {
        let (vault_status, enc_identity, identity_pub) = {
            let b = self.back.read().await;
            (
                b.vault_status.clone(),
                b.encrypted_identity_key.clone(),
                b.user.as_ref().and_then(|u| u.identity_public_key.clone().flatten()),
            )
        };
        self.identity = match vault_status.as_deref() {
            Some("active") => match vault::unlock(enc_identity, self.device, identity_pub) {
                Ok(id) => Some(id),
                Err(e) => {
                    tracing::warn!("vault unlock failed: {e}");
                    None
                }
            },
            _ => None,
        };
        self.vault_status = vault_status;
    }

    /// The owner's own wrapped share key for `LinkInfo::encrypted_share_key` —
    /// the key-store row the create transaction writes, so the owner's other
    /// devices (and the desktop app's restart path) recover the key from the
    /// server instead of a local file. The unlocked identity first, else the
    /// user's identity public key (logged in), else the device key (anonymous).
    /// A logged-in user with no identity key gets no row (same rule as the
    /// desktop app's `wrap_own_share_key`).
    pub async fn wrap_own_share_key(&self, key: &[u8; 32]) -> Option<String> {
        let (logged_in, identity_pub) = {
            let back = self.back.read().await;
            (
                back.user.is_some(),
                back.user.as_ref().and_then(|u| u.identity_public_key.clone().flatten()),
            )
        };
        let target: [u8; 32] = if let Some(id) = &self.identity {
            id.public
        } else if let Some(pub_b64) = identity_pub {
            let decoded = from_base64_special(&pub_b64).or_else(|_| from_base64(&pub_b64)).ok();
            match decoded.and_then(|v| <[u8; 32]>::try_from(v.as_slice()).ok()) {
                Some(t) => t,
                None => {
                    tracing::error!("wrap_own_share_key: invalid identity public key");
                    return None;
                }
            }
        } else if !logged_in {
            self.device.0
        } else {
            return None;
        };
        match hybrid_encrypt(key, &target, None) {
            Ok(v) => Some(to_base64(&v)),
            Err(e) => {
                tracing::error!("wrap_own_share_key failed: {e}");
                None
            }
        }
    }
}

/// Register with the backend (the refresh token comes from the keychain) and
/// unlock the vault identity when this device is trusted.
pub async fn create_session(params: &ClientParams) -> Result<Session> {
    let mut session = open_session(params).await;
    // run_register's request runs through the auth middleware, which exchanges
    // the keychain's refresh token for a fresh access token, so subsequent
    // calls act as the authenticated user without an explicit refresh here.
    Back::run_register(session.back.clone())
        .await
        .context("Failed to register with backend")?;
    // Trusted device + active vault: the register response carries our
    // device-wrapped identity key — unlock it now.
    session.refresh_vault().await;
    Ok(session)
}

/// The session's backend client, wired but not registered: the device key is
/// loaded and the keychain handed to `Back` as the refresh token's home, no
/// request is made. For a client that must come up before (or without) the
/// backend — it drives `Back::run_register` itself and calls
/// `refresh_vault` once online.
pub async fn open_session(params: &ClientParams) -> Session {
    // The keychain item is the refresh token's only home: `Back` reads it
    // there before every grant and writes only what the server issued, so
    // the CLI and the commander — which share the item — always send the
    // token whichever of them rotated last.
    let secure_store = secure_store(params.keychain_service, params.secrets_file.as_deref());

    // The device keypair is this binary's agent identity. Only the public half
    // is sent in `agent_register`; the private half unwraps identity-sealed keys.
    let (device_public_key, device_private_key) = secure_store.ensure_device_key();
    let to32 = |v: &[u8]| -> [u8; 32] { v.try_into().unwrap_or([0u8; 32]) };
    let device = (to32(&device_public_key), to32(&device_private_key));

    let back_params = BackParams {
        base_url: params.api_url(),
        ws_url: params.ws_url(),
        oauth_url: None,
        user_agent: params.user_agent.clone(),
        device_name: Some(params.device_name.clone()),
        product_id: params.product_id.to_string(),
        product_version: params.product_version.to_string(),
        os: std::env::consts::OS.to_string(),
        // Same source as the desktop shell's `os_info` command.
        os_version: os_version(),
        lang: "en".to_string(),
        response_mode: "token".to_string(),
        device_public_key,
        device_serial: None,
        device_model: device_model(),
        device_form_factor: None,
        log_api: params.log_api,
        // No discovery settings here — leave the server's stored visibility alone.
        nearby_visibility: None,
        instance_id: params.instance_id.clone(),
        connect_override: None,
    };

    let back = Back::new_shared(back_params, secure_store).await;

    Session {
        back,
        device,
        vault_status: None,
        identity: None,
    }
}

/// The user-facing machine name, for the device name — same logic as the
/// desktop shell's os_info: on macOS the Computer Name (e.g. "Ubercore") is
/// preferred over the mDNS hostname, which is suffixed with ".local".
pub fn machine_name() -> String {
    #[cfg(target_os = "macos")]
    {
        get_computer_name().unwrap_or_else(|| {
            hostname::get()
                .map(|h| h.to_string_lossy().to_string())
                .unwrap_or_default()
        })
    }
    #[cfg(target_os = "linux")]
    {
        share_ninja_core::utils::host_linux::machine_name().unwrap_or_default()
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        hostname::get()
            .map(|h| h.to_string_lossy().to_string())
            .unwrap_or_default()
    }
}

/// OS version for agent registration: on Linux the distribution and release
/// ("Debian GNU/Linux 13"), elsewhere the OS version number.
fn os_version() -> String {
    #[cfg(target_os = "linux")]
    {
        share_ninja_core::utils::host_linux::os_version().unwrap_or_else(|| os_info::get().version().to_string())
    }
    #[cfg(not(target_os = "linux"))]
    {
        os_info::get().version().to_string()
    }
}

/// Hardware model for agent registration; `None` (omitted) where unknown.
fn device_model() -> Option<String> {
    #[cfg(target_os = "linux")]
    {
        share_ninja_core::utils::host_linux::device_model()
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

#[cfg(target_os = "macos")]
fn get_computer_name() -> Option<String> {
    use core_foundation::base::TCFType;
    use core_foundation::string::{CFString, CFStringRef};

    #[link(name = "SystemConfiguration", kind = "framework")]
    unsafe extern "C" {
        fn SCDynamicStoreCopyComputerName(store: *const std::ffi::c_void, name_encoding: *mut u32) -> CFStringRef;
    }

    unsafe {
        // Passing a null store is permitted; it uses the default session.
        let name = SCDynamicStoreCopyComputerName(std::ptr::null(), std::ptr::null_mut());
        if name.is_null() {
            return None;
        }
        let cfstr: CFString = TCFType::wrap_under_create_rule(name);
        let s = cfstr.to_string();
        if s.is_empty() { None } else { Some(s) }
    }
}
