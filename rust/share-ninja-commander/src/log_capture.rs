//! Log capture layer for tracing that stores messages in a shared buffer

use parking_lot::Mutex;
use std::sync::Arc;
use tracing::{Event, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;

/// Shared log buffer that can be read by the UI
pub type LogBuffer = Arc<Mutex<Vec<String>>>;

/// Create a new log buffer
pub fn new_log_buffer() -> LogBuffer {
    Arc::new(Mutex::new(Vec::new()))
}

/// A tracing layer that captures log messages into a shared buffer
pub struct LogCaptureLayer {
    buffer: LogBuffer,
    max_messages: usize,
}

impl LogCaptureLayer {
    pub fn new(buffer: LogBuffer) -> Self {
        Self {
            buffer,
            max_messages: 1000,
        }
    }
}

impl<S: Subscriber> Layer<S> for LogCaptureLayer {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let metadata = event.metadata();
        let level = metadata.level();
        let target = metadata.target();

        // Only capture DEBUG/TRACE from our own crates; filter out noisy dependency logs
        let is_ours = target.starts_with("share_ninja")
            || target.starts_with("client")
            || target.starts_with("commander")
            || target.starts_with("sninja");
        if !is_ours && *level > tracing::Level::INFO {
            return;
        }

        // Format the message
        let mut visitor = MessageVisitor::default();
        event.record(&mut visitor);

        // Apply the same prefix rewrites the file formatter uses
        // (`share_ninja_commander` → `commander`) so the TUI log pane shows
        // the same names as the rest of the logs.
        let rewritten = share_ninja_core::utils::log_format::rewrite_target(target);
        // Show last two segments for better identification (e.g. "back::ws" not just "ws")
        let short_target = {
            let parts: Vec<&str> = rewritten.split("::").collect();
            if parts.len() >= 2 {
                format!("{}::{}", parts[parts.len() - 2], parts[parts.len() - 1])
            } else {
                rewritten.into_owned()
            }
        };

        // Strip control characters and ANSI escape sequences so each log entry
        // is a single clean line in the TUI (no rendering glitches).
        let raw_message = visitor.message.unwrap_or_default();
        let flat_message = strip_control_chars(&raw_message);
        let msg = format!(
            "{} [{}] {}: {}",
            chrono::Local::now().format("%H:%M:%S"),
            level,
            short_target,
            flat_message
        );

        let mut buffer = self.buffer.lock();
        buffer.push(msg);
        // Keep only the last N messages (safety cap; normally drained by sync_logs)
        if buffer.len() > self.max_messages {
            let excess = buffer.len() - self.max_messages;
            buffer.drain(..excess);
        }
    }
}

#[derive(Default)]
struct MessageVisitor {
    message: Option<String>,
}

impl tracing::field::Visit for MessageVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.message = Some(format!("{:?}", value));
        } else if self.message.is_none() {
            self.message = Some(format!("{}={:?}", field.name(), value));
        } else if let Some(existing) = &mut self.message {
            existing.push_str(&format!(", {}={:?}", field.name(), value));
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            self.message = Some(value.to_string());
        } else if self.message.is_none() {
            self.message = Some(format!("{}={}", field.name(), value));
        } else if let Some(existing) = &mut self.message {
            existing.push_str(&format!(", {}={}", field.name(), value));
        }
    }
}

/// Strip ANSI escape sequences and replace control characters with spaces.
fn strip_control_chars(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '\x1b' {
            // Skip ANSI escape sequence: ESC [ ... final_byte
            if let Some(next) = chars.next()
                && next == '['
            {
                // CSI sequence — consume until a letter (0x40..=0x7E)
                for c2 in chars.by_ref() {
                    if c2.is_ascii_alphabetic() || c2 == '~' {
                        break;
                    }
                }
            }
            // else: other ESC sequence, just skip the two chars
        } else if c.is_control() {
            result.push(' ');
        } else {
            result.push(c);
        }
    }
    result
}
