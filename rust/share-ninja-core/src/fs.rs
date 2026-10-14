//! Local filesystem storage: the upload source (`FsSource`), the download
//! destination (`FsDest`) + its per-download multipart metadata store
//! (`meta_store`), the plain file-manager storage (`LocalFs`: one root,
//! source + dest, no meta store), and the shared fast directory walk (`walk`,
//! using `getattrlistbulk` on macOS/iOS) reused by all of them.

#[cfg(target_os = "android")]
pub(crate) mod android_content;
mod dest;
mod local;
mod meta_store;
mod object_store;
mod preview;
mod search;
mod source;
mod stream;
mod walk;

pub use dest::{FsDest, volume_free_capacity};
pub use local::LocalFs;
pub use meta_store::{CollisionPolicy, FsMetaStore, NameRule, split_legacy_path_map};
#[allow(unused_imports)] // wired in by the P2P file server (V3 root), next step
pub use object_store::FsObjectStore;
pub use source::FsSource;
