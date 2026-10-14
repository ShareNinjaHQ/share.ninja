//! The commander's state between runs — each panel's layout, sort and last
//! local directory, and the skin — in a TOML file the binary places
//! (`sninja`: `commander.toml` next to its config.toml, in the platform's
//! `Share.Ninja` config directory). Loaded once at startup, written
//! whenever one of those changes and at quit.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::app::{App, Panel, SortMode, ViewMode};
use crate::ui::skin;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct State {
    pub left: PanelState,
    pub right: PanelState,
    /// The skin's name as the selector lists it; `None` is the default.
    pub skin: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct PanelState {
    pub view: ViewMode,
    pub sort: SortMode,
    /// The last local directory listed, absolute.
    pub dir: Option<String>,
}

impl PanelState {
    fn of(panel: &Panel) -> Self {
        Self {
            view: panel.view,
            sort: panel.sort,
            dir: panel.last_local_dir.as_ref().map(|d| d.to_string_lossy().into_owned()),
        }
    }

    /// The saved directory when it still exists.
    pub fn existing_dir(&self) -> Option<PathBuf> {
        let dir = PathBuf::from(self.dir.as_deref()?);
        dir.is_dir().then_some(dir)
    }
}

impl State {
    /// The state saved at `path`; the defaults when there is none, or it
    /// does not parse.
    pub fn load(path: &Path) -> State {
        let text = match std::fs::read_to_string(path) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return State::default(),
            Err(e) => {
                tracing::warn!(path = %path.display(), "state: {e}");
                return State::default();
            }
        };
        match toml::from_str(&text) {
            Ok(state) => state,
            Err(e) => {
                tracing::warn!(path = %path.display(), "state: {e}; starting from the defaults");
                State::default()
            }
        }
    }

    /// The app's current state: the panels, and the skin in use.
    pub fn of(app: &App) -> State {
        State {
            left: PanelState::of(&app.left_panel),
            right: PanelState::of(&app.right_panel),
            skin: Some(skin::current_skin_name()),
        }
    }

    pub fn save(&self, path: &Path) -> std::io::Result<()> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let text = toml::to_string_pretty(self).map_err(std::io::Error::other)?;
        std::fs::write(path, text)
    }

    /// The saved skin, by the name the selector lists it under; an unknown
    /// one is logged and the default stays.
    pub fn apply_skin(&self) {
        let Some(name) = self.skin.as_deref() else {
            return;
        };
        match skin::available_skins().into_iter().find(|(n, _)| n == name) {
            Some((_, None)) => skin::load_default_skin(),
            Some((_, Some(path))) => {
                if let Err(e) = skin::load_skin_from_file(Path::new(&path)) {
                    tracing::warn!(skin = name, "state: {e}");
                }
            }
            None => tracing::warn!(skin = name, "state: no such skin; the default stays"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_through_toml() {
        let state = State {
            left: PanelState {
                view: ViewMode::Brief,
                sort: SortMode::Modified,
                dir: Some("/tmp".to_string()),
            },
            right: PanelState::default(),
            skin: Some("< default >".to_string()),
        };
        let text = toml::to_string_pretty(&state).unwrap();
        assert!(text.contains("view = \"brief\""), "{text}");
        assert!(text.contains("sort = \"modified\""), "{text}");
        assert_eq!(toml::from_str::<State>(&text).unwrap(), state);
    }

    #[test]
    fn missing_fields_are_the_defaults() {
        let state: State = toml::from_str("[left]\nview = \"wide\"\n").unwrap();
        assert_eq!(state.left.view, ViewMode::Wide);
        assert_eq!(state.left.sort, SortMode::Name);
        assert_eq!(state.right, PanelState::default());
        assert_eq!(state.skin, None);
    }
}
