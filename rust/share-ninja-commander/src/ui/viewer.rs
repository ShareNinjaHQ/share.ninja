//! The built-in viewer's screen: a header line (file name, transient status,
//! position) over the rows `viewer::ViewerState` laid out. The key bar below
//! it is the shared `StatusBar` with the viewer's labels.

use super::skin;
use crate::syntax::Class;
use crate::viewer::ViewerState;
use ratatui::{
    buffer::Buffer,
    layout::Rect,
    text::{Line, Span},
    widgets::Widget,
};

pub struct ViewerWidget<'a> {
    pub viewer: &'a ViewerState,
}

impl<'a> ViewerWidget<'a> {
    pub fn new(viewer: &'a ViewerState) -> Self {
        Self { viewer }
    }
}

impl<'a> Widget for ViewerWidget<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        if area.height == 0 || area.width == 0 {
            return;
        }
        let v = self.viewer;
        let default = skin::viewer_default();
        for y in area.y..area.y + area.height {
            for x in area.x..area.x + area.width {
                buf[(x, y)].set_style(default);
                buf[(x, y)].set_symbol(" ");
            }
        }

        // ── header ─────────────────────────────────────────────────────────
        // MC's status bar colours across the whole top row.
        let header_y = area.y;
        let width = area.width as usize;
        let bar = skin::statusbar_default();
        for x in area.x..area.x + area.width {
            buf[(x, header_y)].set_style(bar);
        }
        let layout = v.current_layout();
        let title = format!(" {}", v.title);
        let position = match v.size {
            Some(size) => {
                let shown_to = layout.and_then(|l| l.rows.last().map(|r| r.end)).unwrap_or(v.top);
                let pct = (shown_to.min(size) * 100).checked_div(size).unwrap_or(100);
                let mut flags = String::new();
                if v.hex {
                    flags.push_str(" Hex");
                }
                if v.wrap {
                    flags.push_str(" Wrap");
                }
                if v.is_formatted() {
                    flags.push_str(" Fmt");
                }
                format!("{}/{} {:>3}%{} ", v.top, size, pct, flags)
            }
            None => "opening… ".to_string(),
        };
        let title_len = title.chars().count();
        let pos_len = position.chars().count();
        let title_shown: String = title.chars().take(width).collect();
        buf.set_string(area.x, header_y, &title_shown, bar);
        if title_len + pos_len < width {
            let x = area.x + (width - pos_len) as u16;
            buf.set_string(x, header_y, &position, bar);
            // Status (or the error) sits between the name and the position.
            let (msg, style) = match (&v.error, &v.status) {
                (Some(e), _) => (Some(e.as_str()), bar.add_modifier(ratatui::style::Modifier::UNDERLINED)),
                (None, Some(s)) => (Some(s.as_str()), bar),
                (None, None) => (None, bar),
            };
            if let Some(msg) = msg {
                let room = width - title_len - pos_len;
                if room > 3 {
                    let text: String = msg.chars().take(room - 2).collect();
                    let x = area.x + (title_len + (room - text.chars().count()) / 2) as u16;
                    buf.set_string(x, header_y, &text, style);
                }
            }
        }

        // ── rows ───────────────────────────────────────────────────────────
        let body = Rect::new(area.x, area.y + 1, area.width, area.height - 1);
        if body.height == 0 {
            return;
        }
        let mut y = body.y;
        let bottom = body.y + body.height;
        if let Some(layout) = layout {
            for row in &layout.rows {
                if y >= bottom {
                    break;
                }
                let spans: Vec<Span> = row
                    .spans
                    .iter()
                    .map(|(text, class)| {
                        let style = match class {
                            Class::Plain => default,
                            Class::Match => skin::viewer_selected(),
                            // A syntax colour on the viewer's ground.
                            class => default.patch(skin::syntax(*class)),
                        };
                        Span::styled(text.as_str(), style)
                    })
                    .collect();
                buf.set_line(body.x, y, &Line::from(spans), body.width);
                y += 1;
            }
            if layout.missing.is_some() && y < bottom && v.error.is_none() {
                buf.set_string(body.x, y, "Loading…", skin::viewer_bold());
            }
        }
        if v.size == Some(0) {
            buf.set_string(body.x, body.y, "(empty file)", skin::viewer_bold());
        }
    }
}
