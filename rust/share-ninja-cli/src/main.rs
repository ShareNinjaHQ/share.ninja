mod commands;
mod config;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

use share_ninja_commander::{LogBuffer, LogCaptureLayer, new_log_buffer};

#[derive(Parser)]
#[command(name = "sninja")]
#[command(display_name = "Share.Ninja CLI")]
#[command(about = "Share.Ninja command line: secure file transfers, and the commander TUI (sninja cmd)", long_about = None)]
// Packaging builds (brew/21_build.sh) inject the release version via
// SNINJA_VERSION at compile time; dev builds fall back to the crate version.
#[command(version = option_env!("SNINJA_VERSION").unwrap_or(env!("CARGO_PKG_VERSION")))]
#[command(disable_help_subcommand = true)]
struct Cli {
    // An override, not a clap default: a `default_value` here would clobber
    // the configured URL (`sninja configure set`) on every run.
    #[arg(
        long,
        env = "NINJA_BASE_URL",
        help = format!(
            "Server origin, default {}; /api and /ws are appended. Overrides the configured one for this run",
            config::DEFAULT_BASE_URL
        )
    )]
    base_url: Option<String>,

    /// Keep the device secrets (key + refresh token) in this file instead of the
    /// system keychain — for containers and hosts without one. Must be writable
    /// (the refresh token rotates); keep it like an SSH key (written 0600)
    #[arg(long, env = "SNINJA_SECRETS_FILE", value_name = "PATH")]
    secrets_file: Option<std::path::PathBuf>,

    /// Enable verbose logging
    #[arg(long, short)]
    verbose: bool,

    /// Enable debug logging (more detailed than verbose)
    #[arg(long)]
    debug: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Upload files to cloud storage and create a share link
    #[command(arg_required_else_help = true)]
    Put {
        /// Files or directories to upload. Use trailing slash (dir/) or glob (dir/*) to include only contents.
        /// Paths like '.', '..', or relative paths (e.g. ../foo) are resolved to absolute paths before upload.
        #[arg(required = true)]
        paths: Vec<std::path::PathBuf>,

        /// Description for the share
        #[arg(short, long)]
        description: Option<String>,

        /// Link validity: seconds, or 90s / 15m / 12h / 7d / 2w; 0 = no expiry. Longer than the account's maximum is cut to it
        #[arg(long, default_value = "7d", value_parser = parse_validity)]
        validity: i64,

        /// Number of concurrent upload streams (1 = fully serial). Default 5.
        #[arg(long, default_value_t = 5)]
        streams: usize,
    },
    /// Share files directly from this machine (P2P); keeps running to serve them
    #[command(arg_required_else_help = true)]
    Direct {
        /// Files or directories to share. Use trailing slash (dir/) or glob (dir/*) to include only contents.
        /// Paths like '.', '..', or relative paths (e.g. ../foo) are resolved to absolute paths before sharing.
        #[arg(required = true)]
        paths: Vec<std::path::PathBuf>,

        /// Description for the share
        #[arg(short, long)]
        description: Option<String>,

        /// Link validity: seconds, or 90s / 15m / 12h / 7d / 2w; 0 = no expiry. Longer than the account's maximum is cut to it
        #[arg(long, default_value = "7d", value_parser = parse_validity)]
        validity: i64,

        /// Stop serving and exit after this many seconds (default: until Ctrl+C)
        #[arg(long)]
        ttl: Option<u64>,
    },
    /// Download from a share URL or token
    Get {
        /// Share URL (https://share.ninja/#CLAIM) or just the URL token
        link_url: String,

        /// Only download files whose path matches this glob (e.g. 'docs/**/*.pdf')
        #[arg(long)]
        filter: Option<String>,

        /// Destination directory
        #[arg(short, long)]
        output: Option<std::path::PathBuf>,

        /// Claim token (base58, the URL fragment) when passing a bare URL token
        #[arg(short, long)]
        key: Option<String>,

        /// If a file already exists, land the download next to it under a new
        /// name ("Foo 1") instead of failing
        #[arg(short, long)]
        force: bool,

        /// Number of concurrent download streams (1 = fully serial). Default 5.
        #[arg(long, default_value_t = 5)]
        streams: usize,
    },
    /// List files in a share without downloading
    List {
        /// Share URL (https://share.ninja/#CLAIM) or just the URL token
        link_url: String,

        /// Claim token (base58, the URL fragment) when passing a bare URL token
        #[arg(short, long)]
        key: Option<String>,

        /// Output as table
        #[arg(long, conflicts_with = "tree")]
        table: bool,

        /// Output as tree
        #[arg(long, conflicts_with = "table")]
        tree: bool,

        /// Write JSON output to file path
        #[arg(long)]
        json: Option<String>,

        /// Non-recursive listing
        #[arg(long)]
        non_recursive: bool,

        /// Starting path
        #[arg(long)]
        path: Option<String>,
    },
    /// Show one link in detail; --mint prints a fresh copy-ready share URL
    Link {
        /// URL token of the link
        token: String,

        /// Mint a new claim under the link's share key and print the share URL
        #[arg(long)]
        mint: bool,

        /// Write JSON output to file path
        #[arg(long)]
        json: Option<String>,
    },
    /// List your links (this account's, or this device's when anonymous)
    Links {
        /// Write JSON output to file path
        #[arg(long)]
        json: Option<String>,
    },
    /// Open the commander: MC-like dual-pane file manager, local filesystem ⇄ Share.Ninja shares
    Cmd {
        /// Share URL (https://share.ninja/#CLAIM) to open in the right pane
        link_url: Option<String>,

        /// Append the log to this file (the terminal is the TUI; nothing goes to stderr)
        #[arg(long)]
        log_path: Option<String>,

        /// Log Share.Ninja API requests/responses (same as the global --verbose)
        #[arg(long)]
        debug_api: bool,
    },
    /// Authenticate with Share.Ninja (OAuth2 + PKCE)
    Auth {
        #[command(subcommand)]
        command: AuthCommands,
    },
    /// Configure CLI settings
    Configure {
        #[command(subcommand)]
        command: ConfigureCommands,
    },
    /// This device's standing in your account's vault (device trust circle)
    Vault {
        #[command(subcommand)]
        command: VaultCommands,
    },
    /// API keys for programmatic access (create, list, revoke) — needs a logged-in account
    Keys {
        #[command(subcommand)]
        command: KeysCommands,
    },
}

#[derive(Subcommand)]
enum KeysCommands {
    /// Create an API key; the token is printed once and never shown again
    Create {
        /// Label shown in the key listing
        name: String,
        /// Limit the key to these scopes (repeatable, e.g. link:read); none = the account's full rights
        #[arg(long = "scope", value_name = "SCOPE")]
        scopes: Vec<String>,
        /// When the key stops working: a duration (30d, 12h, 2w) or a date (YYYY-MM-DD); default never
        #[arg(long, value_name = "WHEN", value_parser = parse_expires)]
        expires: Option<chrono::DateTime<chrono::FixedOffset>>,
        /// Print the created key as JSON (token included)
        #[arg(long)]
        json: bool,
    },
    /// List the account's live API keys
    List {
        /// Print the keys as JSON
        #[arg(long)]
        json: bool,
    },
    /// Revoke an API key by key id (key_…) or by its prefix
    Revoke {
        /// Key id or prefix, as `sninja keys list` shows them
        key: String,
    },
}

#[derive(Subcommand)]
enum VaultCommands {
    /// Show whether this device is enrolled and the vault unlocked
    Status,
    /// Ask to join the vault; approve the request on one of your trusted devices
    Join {
        /// Keep the connection open and unlock as soon as the request is approved
        #[arg(long)]
        wait: bool,
    },
    /// Stay connected and approve every join request of this account as it
    /// arrives (for an unattended trusted device, e.g. a container)
    AutoApprove,
}

#[derive(Subcommand)]
enum AuthCommands {
    /// Log in via browser-based OAuth2 (credentials stored in the system keychain, or --secrets-file)
    Login {
        /// Callback host to listen on (usually 127.0.0.1)
        #[arg(long, default_value = "127.0.0.1")]
        callback_host: String,
        /// Callback port to listen on (0 = let the OS pick a free port)
        #[arg(long, default_value_t = 0)]
        callback_port: u16,
        /// Callback path to listen on
        #[arg(long, default_value = "/oauth/callback")]
        callback_path: String,
    },
    /// Sign out and remove stored credentials (system keychain, or --secrets-file)
    Logout,
    /// Show current authentication status
    Status,
}

#[derive(Subcommand)]
enum ConfigureCommands {
    /// Set configuration values
    Set {
        /// API base URL
        #[arg(long)]
        base_url: Option<String>,
    },
    /// Show current configuration
    Show,
}

/// `--validity`: a bare number is seconds; a unit suffix `s`, `m`, `h`, `d`
/// or `w` scales it. `0` = the link never expires.
fn parse_validity(s: &str) -> Result<i64, String> {
    let s = s.trim();
    let (digits, unit) = match s.find(|c: char| !c.is_ascii_digit()) {
        Some(i) => s.split_at(i),
        None => (s, "s"),
    };
    let n: i64 = digits
        .parse()
        .map_err(|_| format!("'{s}': expected a number with an optional unit s, m, h, d or w"))?;
    let mult = match unit.trim() {
        "s" => 1,
        "m" => 60,
        "h" => 60 * 60,
        "d" => 60 * 60 * 24,
        "w" => 60 * 60 * 24 * 7,
        other => return Err(format!("'{s}': unknown unit '{other}', expected s, m, h, d or w")),
    };
    n.checked_mul(mult).ok_or_else(|| format!("'{s}': too long"))
}

/// `keys create --expires`: a `--validity`-style duration from now (`30d`,
/// `12h`), or a calendar day `YYYY-MM-DD` (its midnight, UTC).
fn parse_expires(s: &str) -> Result<chrono::DateTime<chrono::FixedOffset>, String> {
    let s = s.trim();
    if let Ok(day) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        let at = day
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| format!("'{s}': not a valid date"))?
            .and_utc();
        return Ok(at.fixed_offset());
    }
    let secs = parse_validity(s)?;
    if secs <= 0 {
        return Err(format!("'{s}': expiry must be in the future (omit --expires for a key that never expires)"));
    }
    Ok((chrono::Utc::now() + chrono::Duration::seconds(secs)).fixed_offset())
}

/// Third-party crates that flood trace/debug with useless spans (the HTTP
/// stack). Pinned to warn even when the app itself runs at trace/debug.
/// mDNS is best-effort: on networks that block multicast its sender ERRORs
/// on every packet ("No route to host") — fully off, in every mode.
const NOISE: &str = "hyper=warn,hyper_util=warn,h2=warn,tower=warn,rustls=warn,reqwest=warn,\
webrtc_mdns=off";

/// The webrtc 0.20 crates' chatter during a normal connect: `webrtc`
/// (STUN/IPv6 gather errors), `rtc`, `rtc_ice`, `rtc_dtls`. Harmless — the
/// connection falls back to IPv4 and works fine.
const WEBRTC_QUIET: &str = "webrtc=off,rtc=error,rtc_ice=error,rtc_dtls=error";

/// CLI commands: logs go to stderr — stdout is the command's output only
/// (share URLs, tables, JSON), so it can be piped and parsed. Default is
/// warnings and errors with the WebRTC chatter off; --verbose adds debug,
/// --debug trace (both let the WebRTC crates through).
fn init_cli_tracing(verbose: bool, debug: bool) {
    let filter = if debug {
        EnvFilter::new(format!("trace,{NOISE}"))
    } else if verbose {
        EnvFilter::new(format!("debug,{NOISE}"))
    } else {
        EnvFilter::new(format!("warn,{WEBRTC_QUIET},{NOISE}"))
    };

    tracing_subscriber::registry()
        .with(filter)
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(std::io::stderr)
                .with_ansi(false)
                .with_target(true)
                .with_span_events(tracing_subscriber::fmt::format::FmtSpan::ENTER),
        )
        .init();
}

/// The commander: the terminal is the TUI, so nothing goes to stdout/stderr —
/// events land in the log pane's buffer and, with `--log-path`, a file.
/// Default level info; --verbose/--debug-api debug, --debug trace; `RUST_LOG`
/// overrides the whole filter.
fn init_tui_tracing(log_api: bool, debug: bool, log_path: Option<&str>) -> Result<LogBuffer> {
    let log_buffer = new_log_buffer();
    let default_level = if debug {
        "trace"
    } else if log_api {
        "debug"
    } else {
        "info"
    };
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(format!("{default_level},{WEBRTC_QUIET},{NOISE}")));

    let file_layer = log_path
        .map(|path| {
            let file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .with_context(|| format!("Failed to open log file {path}"))?;
            Ok::<_, anyhow::Error>(
                tracing_subscriber::fmt::layer()
                    .with_writer(file)
                    .with_ansi(false)
                    .event_format(share_ninja_core::utils::log_format::PrefixRewriteFormat),
            )
        })
        .transpose()?;

    tracing_subscriber::registry()
        .with(filter)
        .with(LogCaptureLayer::new(log_buffer.clone()))
        .with(file_layer)
        .init();
    Ok(log_buffer)
}

#[tokio::main]
async fn main() -> Result<()> {
    // Install rustls crypto provider for WebRTC DTLS
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    let cli = Cli::parse();

    let mut cfg = config::Config::load()?;

    // An explicit --base-url / NINJA_BASE_URL always wins over the config file.
    if let Some(url) = cli.base_url {
        cfg.base_url = Some(url);
    }

    cfg.log_api = cli.verbose || cli.debug;
    cfg.secrets_file = cli.secrets_file;

    match cli.command {
        Commands::Cmd {
            link_url,
            log_path,
            debug_api,
        } => {
            cfg.log_api |= debug_api;
            let log_buffer = init_tui_tracing(cfg.log_api, cli.debug, log_path.as_deref())?;
            commands::cmd::execute(&cfg, link_url, log_path, log_buffer).await?;
        }
        command => {
            init_cli_tracing(cli.verbose, cli.debug);
            run(cfg, command).await?;
        }
    }

    Ok(())
}

async fn run(mut cfg: config::Config, command: Commands) -> Result<()> {
    match command {
        Commands::Cmd { .. } => unreachable!("dispatched in main"),
        Commands::Put {
            paths,
            description,
            validity,
            streams,
        } => {
            commands::put::execute(&cfg, paths, description, validity, streams).await?;
        }
        Commands::Direct {
            paths,
            description,
            validity,
            ttl,
        } => {
            commands::direct::execute(&cfg, paths, description, validity, ttl).await?;
        }
        Commands::Get {
            link_url,
            filter,
            output,
            key,
            force,
            streams,
        } => {
            commands::get::execute(&cfg, &link_url, filter, output, key, force, streams).await?;
        }
        Commands::List {
            link_url,
            key,
            table,
            tree,
            json,
            non_recursive,
            path,
        } => {
            let format = if tree {
                "tree"
            } else if table {
                "table"
            } else {
                "tree"
            };
            commands::list::execute(&cfg, &link_url, key, format, non_recursive, path, json).await?;
        }
        Commands::Link { token, mint, json } => {
            commands::link::execute(&cfg, &token, mint, json).await?;
        }
        Commands::Links { json } => {
            commands::links::execute(&cfg, json).await?;
        }
        Commands::Auth { command } => match command {
            AuthCommands::Login {
                callback_host,
                callback_port,
                callback_path,
            } => {
                commands::auth::login(&cfg, &callback_host, callback_port, &callback_path).await?;
            }
            AuthCommands::Logout => {
                commands::auth::logout(&cfg).await?;
            }
            AuthCommands::Status => {
                commands::auth::status(&cfg).await?;
            }
        },
        Commands::Vault { command } => match command {
            VaultCommands::Status => {
                commands::vault::status(&cfg).await?;
            }
            VaultCommands::Join { wait } => {
                commands::vault::join(&cfg, wait).await?;
            }
            VaultCommands::AutoApprove => {
                commands::vault::auto_approve(&cfg).await?;
            }
        },
        Commands::Keys { command } => match command {
            KeysCommands::Create {
                name,
                scopes,
                expires,
                json,
            } => {
                commands::keys::create(&cfg, &name, scopes, expires, json).await?;
            }
            KeysCommands::List { json } => {
                commands::keys::list(&cfg, json).await?;
            }
            KeysCommands::Revoke { key } => {
                commands::keys::revoke(&cfg, &key).await?;
            }
        },
        Commands::Configure { command } => match command {
            ConfigureCommands::Set { base_url } => {
                if let Some(url) = base_url {
                    cfg.base_url = Some(url);
                }
                cfg.save()?;
                println!("Configuration saved.");
            }
            ConfigureCommands::Show => {
                println!("Base URL: {}", cfg.base_url());
            }
        },
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::parse_validity;

    #[test]
    fn validity_units() {
        assert_eq!(parse_validity("604800"), Ok(604800));
        assert_eq!(parse_validity("90s"), Ok(90));
        assert_eq!(parse_validity("15m"), Ok(900));
        assert_eq!(parse_validity("12h"), Ok(43200));
        assert_eq!(parse_validity("7d"), Ok(604800));
        assert_eq!(parse_validity("2w"), Ok(1209600));
        assert_eq!(parse_validity("0"), Ok(0));
        assert!(parse_validity("d").is_err());
        assert!(parse_validity("3x").is_err());
        assert!(parse_validity("-1").is_err());
        assert!(parse_validity("1.5h").is_err());
    }
}
