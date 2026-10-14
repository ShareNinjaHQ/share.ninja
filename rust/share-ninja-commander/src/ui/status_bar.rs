//! MC-style function key bar widget

use super::skin;
use ratatui::{buffer::Buffer, layout::Rect, text::Span, widgets::Widget};

const KEY_LABELS: &[(&str, &str)] = &[
    (" 1", "Help"),
    (" 2", ""),
    (" 3", "View"),
    (" 4", "Edit"),
    (" 5", "Copy"),
    (" 6", ""),
    (" 7", "Mkdir"),
    (" 8", "Delete"),
    (" 9", ""),
    ("10", "Quit"),
];

pub struct StatusBar<'a> {
    labels: &'a [(&'a str, &'a str)],
}

impl StatusBar<'static> {
    /// The file-manager labels.
    pub fn new() -> Self {
        Self { labels: KEY_LABELS }
    }
}

impl<'a> StatusBar<'a> {
    /// Another screen's labels (the viewer's).
    pub fn with_labels(labels: &'a [(&'a str, &'a str)]) -> Self {
        Self { labels }
    }
}

impl Default for StatusBar<'static> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> Widget for StatusBar<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let hotkey_style = skin::buttonbar_hotkey();
        let button_style = skin::buttonbar_button();

        for x in area.x..area.x + area.width {
            buf[(x, area.y)].set_style(hotkey_style);
        }

        let total_width = area.width as usize;
        let num_buttons = self.labels.len();
        let button_width = total_width / num_buttons;

        let mut x = area.x;
        for (i, (key, label)) in self.labels.iter().enumerate() {
            let this_width = if i == num_buttons - 1 {
                (area.x + area.width).saturating_sub(x) as usize
            } else {
                button_width
            };

            if x >= area.x + area.width {
                break;
            }

            let key_span = Span::styled(*key, hotkey_style);
            let key_len = key.len() as u16;
            buf.set_span(x, area.y, &key_span, key_len);
            x += key_len;

            let label_space = this_width.saturating_sub(key.len());
            let padded_label = format!("{:<width$}", label, width = label_space);
            let label_span = Span::styled(padded_label, button_style);
            let label_len = label_space as u16;
            buf.set_span(x, area.y, &label_span, label_len);
            x += label_len;
        }
    }
}
