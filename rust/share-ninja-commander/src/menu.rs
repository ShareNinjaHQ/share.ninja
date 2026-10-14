//! The F9 pull-down menus (FAR's): what each menu lists for the current
//! state, and what choosing a row does. The widget is `ui::menu`. The
//! panels and the editor each have their own bar.

use crate::app::{App, SortMode, ViewMode};
use crate::events::PanelId;

/// Whose bar is open: the panel screen's or the editor's.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum MenuBarKind {
    #[default]
    Panels,
    Editor,
}

pub const PANEL_TITLES: [&str; 4] = ["Left", "Files", "Commands", "Right"];
pub const EDITOR_TITLES: [&str; 3] = ["File", "Edit", "Search"];

pub fn titles(kind: MenuBarKind) -> &'static [&'static str] {
    match kind {
        MenuBarKind::Panels => &PANEL_TITLES,
        MenuBarKind::Editor => &EDITOR_TITLES,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MenuAction {
    View(PanelId, ViewMode),
    Sort(PanelId, SortMode),
    FileView,
    FileEdit,
    FileCopy,
    FileRename,
    /// Alt+F7.
    FileFind,
    /// The Dos Navigator tribute.
    Tetris,
    EditorSave,
    EditorClose,
    EditorUndo,
    EditorRedo,
    EditorCut,
    EditorCopy,
    EditorPaste,
    EditorDeleteLine,
    EditorSelectAll,
    EditorUnselect,
    EditorFind,
    EditorFindNext,
    EditorFindPrev,
    EditorGoto,
}

pub struct MenuItem {
    pub label: &'static str,
    /// The key that does the same outside the menu, shown right-aligned.
    pub hotkey: &'static str,
    pub checked: bool,
    /// `None` = a separator row.
    pub action: Option<MenuAction>,
}

impl MenuItem {
    fn separator() -> Self {
        Self {
            label: "",
            hotkey: "",
            checked: false,
            action: None,
        }
    }
}

fn action(label: &'static str, hotkey: &'static str, action: MenuAction) -> MenuItem {
    MenuItem {
        label,
        hotkey,
        checked: false,
        action: Some(action),
    }
}

/// The rows of menu `open` (an index into the bar's titles).
pub fn items(app: &App, kind: MenuBarKind, open: usize) -> Vec<MenuItem> {
    if kind == MenuBarKind::Editor {
        return match open {
            0 => vec![
                action("Save", "F2", MenuAction::EditorSave),
                MenuItem::separator(),
                action("Close", "F10", MenuAction::EditorClose),
            ],
            1 => vec![
                action("Undo", "Ctrl+Z", MenuAction::EditorUndo),
                action("Redo", "Ctrl+Shift+Z", MenuAction::EditorRedo),
                MenuItem::separator(),
                action("Cut", "Shift+Del", MenuAction::EditorCut),
                action("Copy", "Ctrl+Ins", MenuAction::EditorCopy),
                action("Paste", "Shift+Ins", MenuAction::EditorPaste),
                action("Delete line", "Ctrl+Y", MenuAction::EditorDeleteLine),
                MenuItem::separator(),
                action("Select all", "Ctrl+A", MenuAction::EditorSelectAll),
                action("Unselect", "Ctrl+U", MenuAction::EditorUnselect),
            ],
            _ => vec![
                action("Find", "F7", MenuAction::EditorFind),
                action("Next match", "Alt+N", MenuAction::EditorFindNext),
                action("Previous match", "Alt+P", MenuAction::EditorFindPrev),
                MenuItem::separator(),
                action("Go to line", "Alt+L", MenuAction::EditorGoto),
            ],
        };
    }
    match open {
        1 => vec![
            MenuItem {
                label: "View",
                hotkey: "F3",
                checked: false,
                action: Some(MenuAction::FileView),
            },
            MenuItem {
                label: "Edit",
                hotkey: "F4",
                checked: false,
                action: Some(MenuAction::FileEdit),
            },
            MenuItem {
                label: "Copy",
                hotkey: "F5",
                checked: false,
                action: Some(MenuAction::FileCopy),
            },
            MenuItem {
                label: "Rename",
                hotkey: "F6",
                checked: false,
                action: Some(MenuAction::FileRename),
            },
            MenuItem::separator(),
            MenuItem {
                label: "Find file",
                hotkey: "Alt+7",
                checked: false,
                action: Some(MenuAction::FileFind),
            },
        ],
        2 => vec![MenuItem {
            label: "Tetris",
            hotkey: "Alt+Shift+T",
            checked: false,
            action: Some(MenuAction::Tetris),
        }],
        _ => {
            let id = if open == 0 { PanelId::Left } else { PanelId::Right };
            let panel = app.panel(id);
            let mut rows: Vec<MenuItem> = ViewMode::ALL
                .iter()
                .map(|v| MenuItem {
                    label: v.label(),
                    hotkey: "",
                    checked: panel.view == *v,
                    action: Some(MenuAction::View(id, *v)),
                })
                .collect();
            rows.push(MenuItem::separator());
            rows.extend(SortMode::ALL.iter().map(|m| MenuItem {
                label: m.label(),
                hotkey: "",
                checked: panel.sort == *m,
                action: Some(MenuAction::Sort(id, *m)),
            }));
            rows
        }
    }
}
