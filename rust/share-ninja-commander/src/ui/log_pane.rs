//! Log pane widget for displaying connection logs

use super::skin;
use ratatui::{
    buffer::Buffer,
    layout::{Alignment, Rect},
    style::{Color, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Widget},
};

pub struct LogPaneWidget<'a> {
    pub messages: &'a [String],
    pub title: &'a str,
    pub log_path: Option<&'a str>,
    pub debug_api: bool,
    pub is_active: bool,
}

impl<'a> LogPaneWidget<'a> {
    pub fn new(messages: &'a [String], title: &'a str, is_active: bool) -> Self {
        Self {
            messages,
            title,
            log_path: None,
            debug_api: false,
            is_active,
        }
    }
}

impl<'a> Widget for LogPaneWidget<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let title = format!(" {} ", self.title);

        let mut block = Block::default()
            .borders(Borders::ALL)
            .border_style(skin::panel_border())
            .style(skin::core_default())
            .title(Span::styled(
                title,
                if self.is_active {
                    skin::panel_title_active()
                } else {
                    skin::panel_title_inactive()
                },
            ));

        {
            let mut parts: Vec<Span> = Vec::new();
            if self.debug_api {
                parts.push(Span::styled(" [debug-api] ", skin::panel_title_inactive()));
            }
            if let Some(path) = self.log_path {
                parts.push(Span::styled(format!(" {} ", path), skin::panel_title_inactive()));
            }
            if !parts.is_empty() {
                block = block.title_bottom(Line::from(parts).alignment(Alignment::Right));
            }
        }

        let inner_area = block.inner(area);
        block.render(area, buf);

        if self.messages.is_empty() {
            let line = Line::from(Span::styled(
                "No logs yet...",
                Style::default().fg(Color::Gray).bg(Color::Blue),
            ));
            buf.set_line(inner_area.x, inner_area.y, &line, inner_area.width);
            return;
        }

        let visible_height = inner_area.height as usize;
        let start_idx = self.messages.len().saturating_sub(visible_height);

        let lines: Vec<Line> = self.messages[start_idx..]
            .iter()
            .map(|msg| {
                let style = if msg.contains("[ERROR]") {
                    skin::get("editor", "bookmark")
                } else if msg.contains("[WARN]") {
                    skin::get("viewer", "viewbold")
                } else if msg.contains("[INFO]") {
                    skin::get("editor", "editframeactive")
                } else {
                    // [DEBUG] and anything unmarked
                    skin::core_default()
                };
                let max_width = inner_area.width as usize;
                let display_msg: std::borrow::Cow<str> = if msg.len() > max_width && max_width > 3 {
                    std::borrow::Cow::Owned(format!("{}...", &msg[..max_width - 3]))
                } else {
                    std::borrow::Cow::Borrowed(msg.as_str())
                };
                Line::from(Span::styled(display_msg.into_owned(), style))
            })
            .collect();

        let paragraph = Paragraph::new(lines);

        paragraph.render(inner_area, buf);
    }
}
