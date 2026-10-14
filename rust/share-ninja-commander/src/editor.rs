//! The built-in mcedit-style editor (F4): whole-buffer editing of local
//! files over `tui-textarea` (vendored to match the workspace ratatui), with
//! save (F2), search (F7), goto line, FAR's selection and clipboard keys
//! (the system clipboard, the widget's own yank buffer when there is none),
//! an F9 menu, and a modified-changes guard on close.
//!
//! Local storages only — a share file is an immutable snapshot with no
//! write-back API, so there is nothing an editor could save to; F3 views it.
//! Unlike the streaming viewer the editor materializes the whole file
//! (mcedit does the same), so oversized and non-UTF-8 files are refused with
//! a pointer to F3.

use std::path::PathBuf;

use tui_textarea::TextArea;

use crate::ui::skin;

/// Refuse to load anything bigger — the buffer, undo history and per-key
/// redraw all scale with file size. F3 streams any size.
pub const MAX_EDIT_BYTES: u64 = 10 * 1024 * 1024;

pub struct EditorState {
    pub textarea: TextArea<'static>,
    /// Absolute local path the buffer saves to.
    pub abs_path: PathBuf,
    /// File name, for the header line.
    pub title: String,
    /// The file used CRLF line endings when loaded — preserved on save.
    crlf: bool,
    /// The file ended with a newline when loaded — preserved on save.
    trailing_newline: bool,
    pub modified: bool,
    /// Last save error, shown on the header line until the next save.
    pub error: Option<String>,
}

/// Load `abs_path` into a fresh editor. Errors are user-facing one-liners
/// (panel status message material).
pub fn load(abs_path: PathBuf, title: String) -> Result<EditorState, String> {
    let meta = std::fs::metadata(&abs_path).map_err(|e| format!("{title}: {e}"))?;
    if meta.len() > MAX_EDIT_BYTES {
        return Err(format!(
            "{title}: too large to edit ({} MB max) — F3 views any size",
            MAX_EDIT_BYTES / (1024 * 1024),
        ));
    }
    let bytes = std::fs::read(&abs_path).map_err(|e| format!("{title}: {e}"))?;
    let text = String::from_utf8(bytes).map_err(|_| format!("{title}: not UTF-8 text — F3 views the bytes"))?;

    let crlf = text.contains("\r\n");
    let normalized = if crlf { text.replace("\r\n", "\n") } else { text };
    let trailing_newline = normalized.ends_with('\n');
    let mut lines: Vec<String> = normalized.split('\n').map(str::to_string).collect();
    if trailing_newline {
        // `split` leaves one empty element after the final newline.
        lines.pop();
    }

    let mut textarea = TextArea::new(lines);
    textarea.set_style(skin::core_default());
    // No cursor-LINE underline (the default), mc-style: only the block cursor.
    textarea.set_cursor_line_style(ratatui::style::Style::default());
    textarea.set_selection_style(skin::core_selected());
    textarea.set_search_style(skin::core_markselect());

    Ok(EditorState {
        textarea,
        abs_path,
        title,
        crlf,
        trailing_newline,
        modified: false,
        error: None,
    })
}

impl EditorState {
    /// F2: write the buffer back with the loaded line endings and
    /// trailing-newline shape.
    pub fn save(&mut self) -> Result<(), String> {
        let sep = if self.crlf { "\r\n" } else { "\n" };
        let mut out = self.textarea.lines().join(sep);
        if self.trailing_newline {
            out.push_str(sep);
        }
        std::fs::write(&self.abs_path, out).map_err(|e| e.to_string())?;
        self.modified = false;
        self.error = None;
        Ok(())
    }

    /// Header line: name, modified marker, cursor position, the selection's
    /// line count while one is active, last save error.
    pub fn header(&self) -> String {
        let (row, col) = self.textarea.cursor();
        let marker = if self.modified { " [*]" } else { "" };
        let sel = self
            .textarea
            .selection_range()
            .map(|((r1, _), (r2, _))| format!("   Sel: {} line{}", r2 - r1 + 1, if r2 == r1 { "" } else { "s" }))
            .unwrap_or_default();
        let error = self
            .error
            .as_deref()
            .map(|e| format!("   SAVE FAILED: {e}"))
            .unwrap_or_default();
        format!("{}{marker}   {}:{}{sel}{error}", self.title, row + 1, col + 1)
    }

    /// Copy the selection: into the widget's yank buffer and the system
    /// clipboard.
    pub fn copy(&mut self) {
        self.textarea.copy();
        clipboard_set(&self.textarea.yank_text());
    }

    /// Cut the selection; `true` when the buffer changed.
    pub fn cut(&mut self) -> bool {
        let changed = self.textarea.cut();
        if changed {
            clipboard_set(&self.textarea.yank_text());
        }
        changed
    }

    /// Paste the system clipboard (the yank buffer when there is none);
    /// `true` when the buffer changed.
    pub fn paste(&mut self) -> bool {
        if let Some(text) = clipboard_get() {
            self.textarea.set_yank_text(text);
        }
        self.textarea.paste()
    }

    /// A terminal paste (bracketed): the text goes in whole.
    pub fn paste_text(&mut self, text: &str) -> bool {
        self.textarea.insert_str(text.replace("\r\n", "\n"))
    }

    /// FAR's Ctrl+Y: the cursor's line goes, the next one takes its place.
    pub fn delete_line(&mut self) -> bool {
        use tui_textarea::CursorMove;
        let (row, _) = self.textarea.cursor();
        let last = self.textarea.lines().len().saturating_sub(1);
        self.textarea.move_cursor(CursorMove::Head);
        let mut changed = self.textarea.delete_line_by_end();
        if row < last {
            changed |= self.textarea.delete_next_char(); // the line break after it
        } else if row > 0 {
            changed |= self.textarea.delete_char(); // the last line: the break before it
        }
        changed
    }
}

/// The system clipboard's text, `None` where there is no clipboard (a
/// headless session) or it holds no text.
fn clipboard_get() -> Option<String> {
    arboard::Clipboard::new()
        .ok()?
        .get_text()
        .ok()
        .filter(|t| !t.is_empty())
}

fn clipboard_set(text: &str) {
    if let Ok(mut cb) = arboard::Clipboard::new() {
        let _ = cb.set_text(text);
    }
}
