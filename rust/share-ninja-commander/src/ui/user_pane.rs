//! The User content: the signed-in user (Enter: sign out), or the login
//! prompt, one column.

use super::skin;
use crate::types::BackendStatus;
use ratatui::{
    buffer::Buffer,
    layout::Rect,
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Widget},
};

pub struct UserPaneWidget<'a> {
    pub status: Option<&'a BackendStatus>,
    /// The panel is active: its Login / Sign out button takes Enter.
    pub is_active: bool,
}

impl<'a> UserPaneWidget<'a> {
    pub fn new(status: Option<&'a BackendStatus>, is_active: bool) -> Self {
        Self { status, is_active }
    }
}

const LABEL_W: usize = 16;

impl<'a> Widget for UserPaneWidget<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let outer_block = Block::default()
            .borders(Borders::ALL)
            .border_style(skin::panel_border())
            .style(skin::core_default())
            .title(Span::styled(
                " User ",
                if self.is_active {
                    skin::panel_title_active()
                } else {
                    skin::panel_title_inactive()
                },
            ));

        let inner = outer_block.inner(area);
        outer_block.render(area, buf);
        if inner.height == 0 || inner.width == 0 {
            return;
        }

        let label_style = skin::get("editor", "editframeactive");
        let value_style = skin::core_default();
        let heading_style = skin::core_header();
        let value_w = (inner.width as usize).saturating_sub(LABEL_W);
        let heading = |text: &str| -> Line<'static> {
            Line::from(Span::styled(
                format!("{:<width$}", text, width = inner.width as usize),
                heading_style,
            ))
        };
        let row = |label: &str, val: &str| -> Line<'static> {
            Line::from(vec![
                Span::styled(format!("{:<LABEL_W$}", label), label_style),
                Span::styled(format!("{:<width$}", val, width = value_w), value_style),
            ])
        };
        // The one button on screen, focused while the panel is active.
        let button = |text: &str| -> Line<'static> {
            let style = if self.is_active {
                skin::get("dialog", "dfocus")
            } else {
                skin::get("dialog", "dhotnormal")
            };
            Line::from(Span::styled(format!(" [ {} ] ", text), style))
        };

        let Some(status) = self.status else {
            let line = Line::from(Span::styled("No backend status available...", skin::core_input()));
            buf.set_line(inner.x, inner.y, &line, inner.width);
            return;
        };

        let state_label = status.connection.state_label();
        let lines: Vec<Line> = if let Some(user) = &status.connection.user {
            let mut lines = vec![
                heading(" Logged in"),
                row(" Display name", &user.display_name),
                row(" Email", &user.email),
                row(" User ID", &user.user_id),
                row(" State", &state_label),
                Line::from(""),
                heading(" Workspaces"),
            ];
            if user.workspaces.is_empty() {
                lines.push(row(" ", "(none)"));
            } else {
                for ws in &user.workspaces {
                    lines.push(row(" ", &format!("{} ({:?})", ws.name, ws.role)));
                }
            }
            lines.push(Line::from(""));
            lines.push(button("Sign out"));
            lines
        } else if status.connection.is_online() {
            vec![
                heading(" Not logged in"),
                row(" State", &state_label),
                Line::from(""),
                button("Login"),
                Line::from(""),
                Line::from(Span::styled(" Press Enter to open browser for login", value_style)),
            ]
        } else {
            vec![
                heading(" Offline"),
                row(" State", &state_label),
                Line::from(""),
                Line::from(Span::styled(
                    " Backend is not connected. Login is not available.",
                    value_style,
                )),
            ]
        };
        Paragraph::new(lines).render(inner, buf);
    }
}
