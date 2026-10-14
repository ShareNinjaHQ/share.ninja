//! The Connection content: the backend's parameters and session state, one
//! column (a panel is half the screen).

use super::skin;
use crate::types::{BackendStatus, VaultJoinApprover, VaultStateSnapshot};
use ratatui::{
    buffer::Buffer,
    layout::Rect,
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Widget},
};

pub struct ConnectionPaneWidget<'a> {
    pub status: Option<&'a BackendStatus>,
    pub vault: &'a VaultStateSnapshot,
    pub is_active: bool,
}

impl<'a> ConnectionPaneWidget<'a> {
    pub fn new(status: Option<&'a BackendStatus>, vault: &'a VaultStateSnapshot, is_active: bool) -> Self {
        Self {
            status,
            vault,
            is_active,
        }
    }
}

const LABEL_W: usize = 16;

/// `name · Safari on macOS · online|6h ago` — the settings device cards' line.
fn approver_line(a: &VaultJoinApprover) -> String {
    let mut parts = vec![a.display_name.clone()];
    if let Some(desc) = &a.desc {
        parts.push(desc.clone());
    }
    parts.push(if a.online {
        "online".to_string()
    } else {
        time_ago(a.last_seen_at)
    });
    parts.join(" · ")
}

fn time_ago(at: chrono::DateTime<chrono::FixedOffset>) -> String {
    let mins = (chrono::Utc::now() - at.with_timezone(&chrono::Utc)).num_minutes().max(0);
    if mins < 1 {
        "now".to_string()
    } else if mins < 60 {
        format!("{mins}m ago")
    } else if mins < 60 * 24 {
        format!("{}h ago", mins / 60)
    } else {
        format!("{}d ago", mins / (60 * 24))
    }
}

impl<'a> Widget for ConnectionPaneWidget<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let outer_block = Block::default()
            .borders(Borders::ALL)
            .border_style(skin::panel_border())
            .style(skin::core_default())
            .title(Span::styled(
                " Connection ",
                if self.is_active {
                    skin::panel_title_active()
                } else {
                    skin::panel_title_inactive()
                },
            ));

        let inner = outer_block.inner(area);
        outer_block.render(area, buf);

        let Some(status) = self.status else {
            let line = Line::from(Span::styled("No backend status available...", skin::core_input()));
            buf.set_line(inner.x, inner.y, &line, inner.width);
            return;
        };

        let label_style = skin::get("editor", "editframeactive");
        let value_style = skin::core_default();
        let heading_style = skin::core_header();
        let value_w = (inner.width as usize).saturating_sub(LABEL_W);

        let pad = |val: &str| -> String { format!("{:<width$}", val, width = value_w) };
        // A token, ellipsized in the middle to the value width.
        let fit_token = |token: &str| -> String {
            if token.is_empty() || token.len() <= value_w {
                return pad(token);
            }
            if value_w > 5 {
                let side = (value_w - 1) / 2;
                let right_len = value_w - 1 - side;
                format!("{}…{}", &token[..side], &token[token.len() - right_len..])
            } else {
                pad(&token[..value_w.min(token.len())])
            }
        };
        let heading = |text: &str| -> Line<'static> {
            Line::from(Span::styled(
                format!("{:<width$}", text, width = inner.width as usize),
                heading_style,
            ))
        };
        let row = |label: &str, val: String| -> Line<'static> {
            Line::from(vec![
                Span::styled(format!("{:<LABEL_W$}", label), label_style),
                Span::styled(val, value_style),
            ])
        };

        let p = &status.params;
        let c = &status.connection;
        let is_online = c.is_online();
        let val_or_blank = |v: &str| -> String { if is_online { pad(v) } else { pad("") } };
        let user_str = if !is_online {
            String::new()
        } else if let Some(user) = &c.user {
            format!("{} <{}>", user.display_name, user.email)
        } else {
            "not logged in".to_string()
        };
        let identity_state = if self.vault.identity_loaded {
            "unlocked"
        } else if c.encrypted_identity_key.is_some() {
            "locked"
        } else {
            "-"
        };

        let mut lines: Vec<Line> = vec![
            heading(" Connection params"),
            row(" Base URL", pad(&p.base_url)),
            row(" User agent", pad(&p.user_agent)),
            row(" Device name", pad(p.device_name.as_deref().unwrap_or("-"))),
            row(" Product", pad(&format!("{} v{}", p.product_id, p.product_version))),
            row(" OS", pad(&format!("{} {}", p.os, p.os_version))),
            Line::from(""),
            heading(" Connection state"),
            row(" State", pad(&c.state_label())),
            row(" Agent ID", val_or_blank(&c.agent_id)),
            row(" Agent name", val_or_blank(&c.agent_name)),
            row(" Access token", fit_token(if is_online { &c.access_token } else { "" })),
            row(
                " Refresh token",
                fit_token(if is_online { &c.refresh_token } else { "" }),
            ),
            row(" User", pad(&user_str)),
            row(" Vault", pad(self.vault.status.as_deref().unwrap_or("-"))),
            row(" Identity key", pad(identity_state)),
        ];
        // While our join request is pending: the fingerprint the approver
        // verifies, then where to confirm — one device per line, a snapshot
        // as of the (re)sent request.
        if let Some(join) = &self.vault.pending_join {
            lines.push(row(" Fingerprint", pad(&join.fingerprint)));
            if join.approvers.is_empty() {
                lines.push(row(" Approve on", pad("(no trusted device left)")));
            }
            for (i, a) in join.approvers.iter().enumerate() {
                let label = if i == 0 { " Approve on" } else { "" };
                lines.push(row(label, pad(&approver_line(a))));
            }
        }
        Paragraph::new(lines).render(inner, buf);
    }
}
