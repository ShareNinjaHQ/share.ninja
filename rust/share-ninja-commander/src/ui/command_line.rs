//! MC-style command line: `path$ text` on terminal-default colors, one row
//! between the panels and the key bar.

use ratatui::{buffer::Buffer, layout::Rect, style::Style, widgets::Widget};
use unicode_width::UnicodeWidthChar;

pub struct CommandLine<'a> {
    prompt: &'a str,
    text: &'a str,
    /// Cursor position in `text`, in chars.
    cursor: usize,
}

impl<'a> CommandLine<'a> {
    pub fn new(prompt: &'a str, text: &'a str, cursor: usize) -> Self {
        Self { prompt, text, cursor }
    }

    /// The row as one string for a `width`-wide line, and the cursor's
    /// column in it — the console view draws it straight on the terminal.
    pub fn inline(&self, width: usize) -> (String, u16) {
        let (skip, cursor_col) = self.window(width);
        let visible: String = self.text.chars().skip(skip).collect();
        let line: String = format!("{}{}", self.prompt, visible).chars().take(width).collect();
        (line, cursor_col.min(width.saturating_sub(1)) as u16)
    }

    /// Column (absolute, from `area.x`) the terminal cursor belongs at —
    /// the same windowing as `render`, so they agree.
    pub fn cursor_x(&self, area: Rect) -> u16 {
        let (_, cursor_col) = self.window(area.width as usize);
        area.x + cursor_col.min(area.width.saturating_sub(1) as usize) as u16
    }

    /// The visible text window: chars to skip from the left, and the
    /// on-screen column of the cursor (prompt included). The prompt always
    /// stays; the text scrolls left to keep the cursor in view.
    fn window(&self, width: usize) -> (usize, usize) {
        let prompt_w: usize = self.prompt.chars().map(|c| c.width().unwrap_or(0)).sum();
        let text_w = width.saturating_sub(prompt_w).max(1);
        let widths: Vec<usize> = self.text.chars().map(|c| c.width().unwrap_or(0)).collect();
        let cursor_w: usize = widths[..self.cursor.min(widths.len())].iter().sum();
        // Skip whole chars from the left until the cursor fits (one column is
        // reserved for the cursor itself sitting past the last char).
        let mut skip = 0;
        let mut skipped_w = 0;
        while cursor_w - skipped_w >= text_w {
            skipped_w += widths[skip];
            skip += 1;
        }
        (skip, prompt_w + (cursor_w - skipped_w))
    }
}

impl<'a> Widget for CommandLine<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        if area.height == 0 {
            return;
        }
        // Terminal-default colors, like MC's command row.
        let style = Style::reset();
        for x in area.x..area.x + area.width {
            buf[(x, area.y)].set_style(style);
        }
        let (skip, _) = self.window(area.width as usize);
        let visible: String = self.text.chars().skip(skip).collect();
        let line = format!("{}{}", self.prompt, visible);
        buf.set_stringn(area.x, area.y, line, area.width as usize, style);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn window_keeps_cursor_in_view() {
        // width 10, prompt "$ " (2) → 8 columns of text
        let text = "0123456789abcdef";
        let at_start = CommandLine::new("$ ", text, 0);
        assert_eq!(at_start.window(10), (0, 2));
        let inside = CommandLine::new("$ ", text, 7);
        assert_eq!(inside.window(10), (0, 9));
        let at_edge = CommandLine::new("$ ", text, 8);
        assert_eq!(at_edge.window(10), (1, 9));
        let at_end = CommandLine::new("$ ", text, 16);
        assert_eq!(at_end.window(10), (9, 9));
    }
}
