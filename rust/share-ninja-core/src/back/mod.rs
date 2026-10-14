//! Backend API client module

/// rustls config for the backend reqwest clients: verifies servers against the
/// bundled webpki (Mozilla) root set, using the ring provider. **Android only** —
/// it replaces reqwest's default verifier there because Android's verifier does
/// online OCSP/CRL revocation and rejects modern Let's Encrypt certs that omit an
/// OCSP responder ("Certificate does not specify OCSP responder" → Revoked).
/// Other platforms keep their native/platform verifier (respects enterprise CAs).
/// The WS path verifies against webpki roots on all platforms.
#[cfg(target_os = "android")]
pub(crate) fn webpki_client_config() -> rustls::ClientConfig {
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(rustls::crypto::ring::default_provider()))
        .with_safe_default_protocol_versions()
        .expect("ring provider supports the default TLS protocol versions")
        .with_root_certificates(roots)
        .with_no_client_auth()
}

mod auth;
#[allow(clippy::module_inception)] // `back::back` is flattened away by the pub use below
mod back;
pub mod claims;
mod debug_middleware;
mod logging_client;
#[cfg(test)]
pub(crate) mod test_support;
pub mod ws;

pub use auth::AuthManager;
pub use back::*;
pub use debug_middleware::{AuthMiddleware, DebugLoggingMiddleware, RequestIdMiddleware};
pub use logging_client::ApiLogger;
pub use ws::*;
