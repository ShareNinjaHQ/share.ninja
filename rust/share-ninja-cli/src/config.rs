use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;

/// Production server origin. Override per run with `--base-url` /
/// `NINJA_BASE_URL`, or persistently with `sninja configure set --base-url`.
/// The `/api` and `/ws` paths are derived — see `api_url` / `ws_url`.
pub const DEFAULT_BASE_URL: &str = "https://share.ninja";

/// This tool's directory under the platform's config / data dirs
/// (`~/Library/Application Support/Share.Ninja` on macOS): config.toml,
/// the commander's state, the `get` resume meta.
pub const APP_DIR: &str = "Share.Ninja";

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Config {
    pub base_url: Option<String>,
    pub downloads_path: Option<String>,
    #[serde(skip)]
    pub log_api: bool,
    /// `--secrets-file` / `SNINJA_SECRETS_FILE`: the secrets live in this file
    /// instead of the system keychain. Per run, never saved.
    #[serde(skip)]
    pub secrets_file: Option<PathBuf>,
}

impl Config {
    /// The config directory, created.
    pub fn app_dir() -> Result<PathBuf> {
        let dir = dirs::config_dir()
            .context("Could not determine config directory")?
            .join(APP_DIR);
        fs::create_dir_all(&dir)?;
        Ok(dir)
    }

    fn config_path() -> Result<PathBuf> {
        Ok(Self::app_dir()?.join("config.toml"))
    }

    pub fn load() -> Result<Self> {
        let path = Self::config_path()?;
        if path.exists() {
            let content = fs::read_to_string(&path)?;
            let config: Config = toml::from_str(&content)?;
            Ok(config)
        } else {
            Ok(Config::default())
        }
    }

    pub fn save(&self) -> Result<()> {
        let path = Self::config_path()?;
        let content = toml::to_string_pretty(self)?;
        fs::write(path, content)?;
        Ok(())
    }

    /// Server origin without a trailing slash or `/api` — both are accepted on
    /// input (older configs stored `.../api`) and stripped.
    pub fn base_url(&self) -> &str {
        let url = self.base_url.as_deref().unwrap_or(DEFAULT_BASE_URL);
        let url = url.trim_end_matches('/');
        url.strip_suffix("/api").unwrap_or(url)
    }
}
