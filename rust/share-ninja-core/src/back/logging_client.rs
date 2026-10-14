//! API logging utilities for request/response tracing

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// Controls whether API logging is enabled
#[derive(Clone)]
pub struct ApiLogger {
    enabled: Arc<AtomicBool>,
}

impl ApiLogger {
    /// Create a new API logger
    pub fn new(enabled: bool) -> Self {
        Self {
            enabled: Arc::new(AtomicBool::new(enabled)),
        }
    }

    /// Enable or disable logging
    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::SeqCst);
    }

    /// Check if logging is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::SeqCst)
    }

    /// Log an API request — the call only. Bodies are logged once, by
    /// `DebugLoggingMiddleware`, which redacts the credentials in them.
    pub fn log_request(&self, method: &str, url: &str) {
        if !self.is_enabled() {
            return;
        }

        tracing::debug!(">>> API {} {}", method, url);
    }

    /// Log an API response — the outcome only (see [`Self::log_request`]).
    pub fn log_response<T>(&self, method: &str, url: &str, result: &Result<T, impl std::fmt::Debug>) {
        if !self.is_enabled() {
            return;
        }

        match result {
            Ok(_) => {
                tracing::debug!("<<< API {} {} - OK", method, url);
            }
            Err(e) => {
                tracing::debug!("<<< API {} {} - ERROR: {:?}", method, url, e);
            }
        }
    }
}

impl Default for ApiLogger {
    fn default() -> Self {
        Self::new(false)
    }
}
