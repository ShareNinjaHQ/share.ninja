//! Tracing event formatter that rewrites the displayed `target` prefix.
//!
//! The default `tracing` target is the module path, so events from
//! `share_ninja_commander::*` show up with that long crate name in the log
//! line. Users want the displayed target shortened (`commander`) without
//! renaming the crates.
//!
//! Wire it like the default fmt layer:
//! ```ignore
//! tracing_subscriber::fmt::layer()
//!     .event_format(share_ninja_core::utils::log_format::PrefixRewriteFormat)
//! ```
//!
//! ANSI colors are not applied — file logs (where this is most needed) are
//! written without ANSI anyway.

use std::borrow::Cow;
use std::fmt;

use tracing::{Event, Subscriber};
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields, FormattedFields};
use tracing_subscriber::registry::LookupSpan;

/// Mappings applied to every event's `target` before it's written.
const PREFIX_REWRITES: &[(&str, &str)] = &[("share_ninja_commander", "commander")];

/// Apply the configured rewrites to a target string. Returns a borrow when
/// no rule matches so common cases avoid an allocation.
pub fn rewrite_target(target: &str) -> Cow<'_, str> {
    for (from, to) in PREFIX_REWRITES {
        if let Some(rest) = target.strip_prefix(from) {
            return Cow::Owned(format!("{to}{rest}"));
        }
    }
    Cow::Borrowed(target)
}

/// Custom `FormatEvent` that mirrors the default `tracing-subscriber` layout
/// but rewrites the target prefix.
///
/// Output: `<rfc3339-utc>  <LEVEL> [span: ...]<target>: <fields>`
pub struct PrefixRewriteFormat;

impl<S, N> FormatEvent<S, N> for PrefixRewriteFormat
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(&self, ctx: &FmtContext<'_, S, N>, mut writer: Writer<'_>, event: &Event<'_>) -> fmt::Result {
        let metadata = event.metadata();

        let now = chrono::Utc::now();
        write!(
            writer,
            "{}  {:>5}",
            now.format("%Y-%m-%dT%H:%M:%S%.6fZ"),
            metadata.level(),
        )?;

        if let Some(scope) = ctx.event_scope() {
            for span in scope.from_root() {
                write!(writer, " {}", span.name())?;
                let ext = span.extensions();
                if let Some(fields) = ext.get::<FormattedFields<N>>()
                    && !fields.is_empty()
                {
                    write!(writer, "{{{}}}", fields)?;
                }
                write!(writer, ":")?;
            }
        }

        write!(writer, " {}: ", rewrite_target(metadata.target()))?;

        ctx.field_format().format_fields(writer.by_ref(), event)?;
        writeln!(writer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewrites_known_prefixes() {
        assert_eq!(rewrite_target("share_ninja_commander::engine"), "commander::engine",);
    }

    #[test]
    fn passes_through_unknown_prefixes() {
        let unchanged = "share_ninja_core::back::ws";
        assert_eq!(rewrite_target(unchanged).as_ref(), unchanged);
        assert!(matches!(rewrite_target(unchanged), Cow::Borrowed(_)));
    }
}
