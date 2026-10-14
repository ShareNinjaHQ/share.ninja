//! Shared plumbing for the headless Share.Ninja binary — `sninja`, the CLI and
//! the commander TUI — on top of `share-ninja-core`: the keychain-backed
//! [`SecretStore`](share_ninja_core::secrets::SecretStore), a registered
//! backend [`session::Session`] (device key, refresh token, vault unlock), the
//! OAuth loopback login, share URL / key resolution, and backend WS frames
//! parsed into the generated envelope.

pub mod keychain;
pub mod oauth;
pub mod session;
pub mod share;
pub mod ws;

pub use session::{ClientParams, Session, create_session, open_session};
