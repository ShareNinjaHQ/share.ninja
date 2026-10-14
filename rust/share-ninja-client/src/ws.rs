//! Backend WebSocket frames as the generated `WsMessage` envelope. Core
//! forwards every frame raw (`Back::set_raw_ws_message_handler`); this parses
//! it once for the CLI and the commander.

use std::future::Future;

use share_ninja_core::{Back, models};

pub trait BackWsExt {
    /// `set_raw_ws_message_handler` with the frame parsed into
    /// [`models::WsMessage`]. A frame that doesn't parse (an event type newer
    /// than this build's contract) is skipped. Same single handler slot:
    /// setting one replaces the other.
    fn set_ws_message_handler<F>(&self, handler: F) -> impl Future<Output = ()> + Send
    where
        F: Fn(models::WsMessage) + Send + Sync + 'static;
}

impl BackWsExt for Back {
    async fn set_ws_message_handler<F>(&self, handler: F)
    where
        F: Fn(models::WsMessage) + Send + Sync + 'static,
    {
        self.set_raw_ws_message_handler(
            move |frame: &str| match serde_json::from_str::<models::WsMessage>(frame) {
                Ok(msg) => handler(msg),
                Err(e) => tracing::debug!("skipping unparseable WS frame ({e})"),
            },
        )
        .await;
    }
}
