//! The Info content: facts about the other panel's listing.

use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::{Color, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Widget},
};

use super::panel::format_size;
use super::skin;
use crate::app::{Content, Panel, is_dir, is_file};

pub struct InfoPanel<'a> {
    /// The panel described: the other one.
    pub other: &'a Panel,
    pub other_side: &'a str,
    pub is_active: bool,
}

impl<'a> InfoPanel<'a> {
    fn lines(&self) -> Vec<Line<'static>> {
        let label = Style::default().fg(Color::Cyan);
        let value = Style::default().fg(Color::White);
        let mut lines = vec![Line::from(Span::styled(format!("{} panel:", self.other_side), label))];
        let Content::Files(files) = &self.other.content else {
            lines.push(Line::from(vec![
                Span::raw("  Content: "),
                Span::styled(
                    self.other.content.kind().label().to_string(),
                    Style::default().fg(Color::Yellow),
                ),
            ]));
            return lines;
        };
        // A connected local root has nothing to say about its status.
        let status = if self.other.loading {
            Some("Loading...")
        } else if self.other.error.is_some() {
            Some("Error")
        } else if files.storage.is_none() {
            Some("Not connected")
        } else if files.is_local() {
            None
        } else {
            Some("Connected")
        };
        let dir_count = files.entries.iter().filter(|e| is_dir(e)).count();
        let file_count = files.entries.len() - dir_count;
        let total_size: u64 = files.entries.iter().filter(|e| is_file(e)).map(|e| e.size).sum();
        lines.push(Line::from(vec![
            Span::raw("  Storage: "),
            Span::styled(
                files.storage_id().unwrap_or("None").to_string(),
                Style::default().fg(Color::Yellow),
            ),
        ]));
        if let Some(status) = status {
            lines.push(Line::from(vec![
                Span::raw("  Status: "),
                Span::styled(
                    status.to_string(),
                    Style::default().fg(if self.other.error.is_some() {
                        Color::Red
                    } else {
                        Color::Green
                    }),
                ),
            ]));
        }
        lines.push(Line::from(vec![
            Span::raw("  Path: "),
            Span::styled(files.display_path(), value),
        ]));
        lines.push(Line::from(vec![
            Span::raw("  Items: "),
            Span::styled(format!("{} dirs, {} files", dir_count, file_count), value),
        ]));
        lines.push(Line::from(vec![
            Span::raw("  Size: "),
            Span::styled(format_size(total_size), value),
        ]));
        if let Some(msg) = self.other.status_message.as_deref() {
            lines.push(Line::from(vec![
                Span::raw("  Message: "),
                Span::styled(msg.to_string(), Style::default().fg(Color::Cyan)),
            ]));
        }
        lines
    }
}

impl<'a> Widget for InfoPanel<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let title_style = if self.is_active {
            skin::panel_title_active()
        } else {
            skin::panel_title_inactive()
        };
        let block = Block::default()
            .title(Span::styled(" Info ", title_style))
            .borders(Borders::ALL)
            .border_style(skin::panel_border())
            .style(skin::core_default());
        let inner = block.inner(area);
        block.render(area, buf);
        Paragraph::new(self.lines()).render(inner, buf);
    }
}
