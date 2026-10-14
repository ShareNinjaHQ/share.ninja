//! A [`SecretStore`] over one generic-password item in the operating-system
//! keychain (macOS Keychain, Windows Credential Manager, Linux Secret Service)
//! holding the core `SecureStore` blob — device keypair + refresh token.
//!
//! Each binary owns its item (`service`), distinct from the desktop app's
//! (`net.shareninja` / `desktop_secrets`); they never share a session or a
//! device identity.
//!
//! Where there is no keychain (a container, a headless server) a secrets file
//! takes its place — [`FileSecretStore`].

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use share_ninja_core::secrets::{SecretStore, SecureStore};

/// Account attribute for the secrets item.
const ACCOUNT: &str = "secrets";

pub struct KeyringSecretStore {
    service: &'static str,
}

impl KeyringSecretStore {
    pub fn new(service: &'static str) -> Self {
        Self { service }
    }

    fn entry(&self) -> Option<keyring::Entry> {
        match keyring::Entry::new(self.service, ACCOUNT) {
            Ok(e) => Some(e),
            Err(e) => {
                tracing::warn!("failed to open system keychain entry: {e}");
                None
            }
        }
    }
}

impl SecretStore for KeyringSecretStore {
    fn load(&self) -> Option<String> {
        let entry = self.entry()?;
        match entry.get_password() {
            Ok(raw) => Some(raw),
            Err(keyring::Error::NoEntry) => None,
            Err(e) => {
                tracing::debug!("failed to read secrets from keychain: {e}");
                None
            }
        }
    }

    fn store(&self, blob: String) {
        if let Some(entry) = self.entry()
            && let Err(e) = entry.set_password(&blob)
        {
            tracing::warn!("failed to persist secrets to keychain: {e}");
        }
    }
}

/// [`SecretStore`] over a plain file — for hosts without an OS keychain
/// (containers, headless servers). The blob is the file's content, readable
/// by the owner only (0600 on Unix), so the file must be kept like an SSH key.
///
/// Nothing is cached: every `load` reads the file, so a sibling process's
/// rotation is seen. The file must stay writable — the refresh token rotates
/// on every refresh, and a rotation that cannot be written back leaves the
/// next run holding a dead token. A write goes to a temporary file renamed
/// over the target, so a crash never leaves half a blob; where the target is
/// itself a mount point (a single-file bind mount, `docker run -v file:file`)
/// the rename cannot replace it and the write goes in place instead.
pub struct FileSecretStore {
    path: PathBuf,
}

impl FileSecretStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn replace(&self, blob: &str) -> std::io::Result<()> {
        let dir = match self.path.parent() {
            Some(d) if !d.as_os_str().is_empty() => d.to_path_buf(),
            _ => PathBuf::from("."),
        };
        std::fs::create_dir_all(&dir)?;
        let name = self.path.file_name().map(|n| n.to_string_lossy()).unwrap_or_default();
        let tmp = dir.join(format!(".{name}.{}.tmp", std::process::id()));
        write_private(&tmp, blob)?;
        if let Err(e) = std::fs::rename(&tmp, &self.path) {
            let _ = std::fs::remove_file(&tmp);
            tracing::debug!(path = %self.path.display(), error = %e, "secrets file: rename failed, writing in place");
            return write_private(&self.path, blob);
        }
        // Best-effort: make the rename itself durable.
        if let Ok(d) = std::fs::File::open(&dir) {
            let _ = d.sync_all();
        }
        Ok(())
    }
}

/// Write `data` to `path` (created or truncated) owner-only, and sync it.
fn write_private(path: &Path, data: &str) -> std::io::Result<()> {
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
        opts.mode(0o600);
        let mut f = opts.open(path)?;
        // `mode` only applies on create; tighten a file that already existed.
        let _ = f.set_permissions(std::fs::Permissions::from_mode(0o600));
        f.write_all(data.as_bytes())?;
        f.sync_all()
    }
    #[cfg(not(unix))]
    {
        let mut f = opts.open(path)?;
        f.write_all(data.as_bytes())?;
        f.sync_all()
    }
}

impl SecretStore for FileSecretStore {
    fn load(&self) -> Option<String> {
        match std::fs::read_to_string(&self.path) {
            // Trimmed: a file written by hand (or `echo`) ends in a newline.
            Ok(s) => Some(s.trim().to_string()).filter(|s| !s.is_empty()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => {
                tracing::warn!(path = %self.path.display(), error = %e, "failed to read secrets file");
                None
            }
        }
    }

    fn store(&self, blob: String) {
        if let Err(e) = self.replace(&blob) {
            tracing::warn!(
                path = %self.path.display(),
                error = %e,
                "failed to write secrets file — a rotated refresh token is lost and the next run starts signed out; the file must be writable"
            );
        }
    }
}

/// The device secrets (keypair + refresh token): in `secrets_file` when given,
/// else in the keychain item `service`.
pub fn secure_store(service: &'static str, secrets_file: Option<&Path>) -> Arc<SecureStore> {
    let backend: Arc<dyn SecretStore> = match secrets_file {
        Some(path) => Arc::new(FileSecretStore::new(path)),
        None => Arc::new(KeyringSecretStore::new(service)),
    };
    Arc::new(SecureStore::new(backend))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TMP_SEQ: AtomicU64 = AtomicU64::new(0);

    /// RAII temp dir under the system temp, removed on drop.
    struct TmpDir(PathBuf);
    impl TmpDir {
        fn new() -> Self {
            let n = TMP_SEQ.fetch_add(1, Ordering::Relaxed);
            let p = std::env::temp_dir().join(format!("secrets_file_test_{}_{n}", std::process::id()));
            std::fs::create_dir_all(&p).unwrap();
            TmpDir(p)
        }
    }
    impl Drop for TmpDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn file_store_round_trips_and_creates_parents() {
        let tmp = TmpDir::new();
        let store = FileSecretStore::new(tmp.0.join("nested/dir/sninja.json"));
        assert_eq!(store.load(), None, "a missing file is no secrets yet");
        store.store("one".into());
        assert_eq!(store.load().as_deref(), Some("one"));
        store.store("two".into());
        assert_eq!(store.load().as_deref(), Some("two"));
        let entries = std::fs::read_dir(tmp.0.join("nested/dir")).unwrap().count();
        assert_eq!(entries, 1, "no temp files left behind");
    }

    #[test]
    fn file_store_sees_other_writers_and_trims() {
        let tmp = TmpDir::new();
        let path = tmp.0.join("sninja.json");
        let store = FileSecretStore::new(&path);
        store.store("mine".into());
        // A sibling process (or a hand edit) replaces the file: no snapshot.
        std::fs::write(&path, "theirs\n").unwrap();
        assert_eq!(store.load().as_deref(), Some("theirs"));
        std::fs::write(&path, "  \n").unwrap();
        assert_eq!(store.load(), None, "blank is no secrets");
    }

    #[cfg(unix)]
    #[test]
    fn file_store_is_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = TmpDir::new();
        let path = tmp.0.join("sninja.json");
        std::fs::write(&path, "x").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        FileSecretStore::new(&path).store("secret".into());
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
    }

    #[test]
    fn secure_store_over_a_file_keeps_the_device_key() {
        let tmp = TmpDir::new();
        let path = tmp.0.join("sninja.json");
        let first = secure_store("unused", Some(&path)).ensure_device_key();
        let again = secure_store("unused", Some(&path)).ensure_device_key();
        assert_eq!(first, again, "a second process reads the same device identity");
    }
}
