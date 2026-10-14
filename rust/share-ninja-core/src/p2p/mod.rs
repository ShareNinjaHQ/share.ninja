//! P2P (WebRTC) modules for peer-to-peer file transfers

mod bulk_gate;
mod call_server;
mod data_channel;
mod file_client;
mod file_server;
mod relay_transport;
#[cfg(test)]
mod test_support;
mod webrtc_connector;

pub use call_server::{CallServer, CallServerShare};
pub use data_channel::{DataChannelWrapper, EncryptionConfig, derive_session_key};
pub use file_client::{
    CapsResponse, DirEntry as P2PDirEntry, DirStatsResponse as P2PDirStatsResponse, FileAttrs as P2PFileAttrs,
    FileClient, GetAttrsResponse, GetDirResponse, GetResponse, ListUpdatesBeginResponse, ListUpdatesResponse,
    OpenResponse as P2POpenResponse, PreviewBlock, PutDirResponse as P2PPutDirResponse, PutFileResponse,
    SearchResponse as P2PSearchResponse, StorageChange, preview_request_groups,
};
pub use file_server::FileServer;
pub use relay_transport::{RelayConfig, WebSocketTransport, connect_relay};
pub use webrtc_connector::{IceCandidate, SessionDescription, WebRTCConnector, WebRTCState};
