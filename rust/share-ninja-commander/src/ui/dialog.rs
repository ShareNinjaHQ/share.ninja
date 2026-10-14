//! Dialog widgets for user interaction

use super::skin;
use share_ninja_core::utils::base64::to_base64;
use share_ninja_core::{StorageFileType, StorageObjectInfo};

use crate::app::{FindRow, FindState};
use crate::events::PanelId;
use crate::types::{InboundOfferView, VaultJoinRequest, models, product_label};
use ratatui::{
    buffer::Buffer,
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::Modifier,
    text::{Line, Span},
    widgets::{Block, Borders, Clear, Paragraph, Widget, Wrap},
};

/// Error message dialog
pub struct ErrorDialog<'a> {
    pub title: &'a str,
    pub message: &'a str,
}

impl<'a> ErrorDialog<'a> {
    pub fn new(title: &'a str, message: &'a str) -> Self {
        Self { title, message }
    }
}

impl<'a> Widget for ErrorDialog<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let outer_width = 72.min(area.width.saturating_sub(6));
        let outer_height = 10.min(area.height.saturating_sub(3));

        let outer_area = centered_rect(outer_width, outer_height, area);

        render_shadow(outer_area, area, buf);

        Clear.render(outer_area, buf);
        for y in outer_area.y..outer_area.y + outer_area.height {
            for x in outer_area.x..outer_area.x + outer_area.width {
                buf[(x, y)].set_style(skin::error_default());
            }
        }

        let border_area = Rect::new(
            outer_area.x + 1,
            outer_area.y + 1,
            outer_area.width.saturating_sub(2),
            outer_area.height.saturating_sub(2),
        );

        let block = Block::default()
            .title(Span::styled(format!(" {} ", self.title), skin::error_title()))
            .title_alignment(Alignment::Center)
            .borders(Borders::ALL)
            .border_style(skin::error_default())
            .style(skin::error_default());

        let inner = block.inner(border_area);
        block.render(border_area, buf);

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Min(1), Constraint::Length(1)])
            .split(inner);

        let message = Paragraph::new(self.message)
            .style(skin::error_default())
            .alignment(Alignment::Center)
            .wrap(Wrap { trim: true });
        message.render(chunks[0], buf);

        let hint = Paragraph::new("Press any key to close")
            .style(skin::error_hot())
            .alignment(Alignment::Center);
        hint.render(chunks[1], buf);
    }
}

/// One row of the panel selector, formatted.
pub struct SelectorItem {
    /// The hotkey shown in front (`C`, `1`), blank when none.
    pub hotkey: Option<char>,
    pub name: String,
    /// A drive row's total and free space columns.
    pub space: Option<(String, String)>,
    /// A share's link, dimmed after the name.
    pub detail: Option<String>,
}

/// The panel selector (Alt+1 / Alt+2), FAR's "Change drive": the local
/// volumes with their space, a separator, then the other contents and the
/// open shares; drawn over the panel it is for.
pub struct StorageListDialog<'a> {
    pub title: &'a str,
    pub items: &'a [SelectorItem],
    /// Rows of the first block (the volumes).
    pub drives: usize,
    pub selected_index: usize,
    pub side: PanelId,
}

const SELECTOR_SPACE_W: usize = 8;

impl<'a> Widget for StorageListDialog<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        // Over the panel the selector is for: the left or right half.
        let half_w = area.width / 2;
        let half = match self.side {
            PanelId::Left => Rect::new(area.x, area.y, half_w, area.height),
            PanelId::Right => Rect::new(area.x + half_w, area.y, area.width - half_w, area.height),
        };
        let has_second = self.items.len() > self.drives;
        let list_rows = self.items.len() as u16 + u16::from(has_second && self.drives > 0);
        // +4 for outer margin (2) + border (2)
        let outer_height = (list_rows.max(1) + 4).min(area.height.saturating_sub(2));
        let longest = self
            .items
            .iter()
            .map(|i| 4 + i.name.chars().count())
            .max()
            .unwrap_or(20);
        // Not `clamp`: it panics when the half panel is narrower than the
        // 40-column minimum (a terminal under 84 columns) — the panel wins.
        let outer_width = ((longest + 2 * (SELECTOR_SPACE_W + 1) + 6) as u16)
            .max(40)
            .min(half.width.saturating_sub(2));
        let outer_area = centered_rect(outer_width, outer_height, half);

        render_shadow(outer_area, area, buf);

        Clear.render(outer_area, buf);
        for y in outer_area.y..outer_area.y + outer_area.height {
            for x in outer_area.x..outer_area.x + outer_area.width {
                buf[(x, y)].set_style(skin::dialog_default());
            }
        }

        let border_area = Rect::new(
            outer_area.x + 1,
            outer_area.y + 1,
            outer_area.width.saturating_sub(2),
            outer_area.height.saturating_sub(2),
        );
        let border_style = skin::dialog_default();
        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(border_style)
            .style(border_style);
        let inner = block.inner(border_area);
        block.render(border_area, buf);

        // Centered title
        let title_str = format!(" {} ", self.title);
        let title_len = title_str.len() as u16;
        let title_x = border_area.x + (border_area.width.saturating_sub(title_len)) / 2;
        buf.set_span(
            title_x,
            border_area.y,
            &Span::styled(&title_str, skin::dialog_title()),
            title_len,
        );

        // Drive rows: `hot name │ total │ free`.
        let space_cols = 2 * (SELECTOR_SPACE_W + 1);
        let name_w = (inner.width as usize).saturating_sub(4 + space_cols);
        let mut y = inner.y;
        for (i, item) in self.items.iter().enumerate() {
            if i == self.drives && self.drives > 0 && has_second {
                // Separator between the blocks: ├───┴───┴───┤.
                if y < border_area.y + border_area.height - 1 {
                    buf.set_string(border_area.x, y, skin::line_leftmiddle(), border_style);
                    for x in inner.x..inner.x + inner.width {
                        buf.set_string(x, y, skin::line_horiz(), border_style);
                    }
                    buf.set_string(
                        border_area.x + border_area.width - 1,
                        y,
                        skin::line_rightmiddle(),
                        border_style,
                    );
                    let c1 = inner.x + 4 + name_w as u16;
                    let c2 = c1 + 1 + SELECTOR_SPACE_W as u16;
                    buf.set_string(c1, y, "┴", border_style);
                    buf.set_string(c2, y, "┴", border_style);
                }
                y += 1;
            }
            if y >= inner.y + inner.height {
                break;
            }
            let selected = i == self.selected_index;
            let style = if selected {
                skin::dialog_focus().add_modifier(Modifier::BOLD)
            } else {
                skin::dialog_default()
            };
            let hot_style = if selected {
                skin::dialog_focus()
            } else {
                skin::get("dialog", "dhotnormal")
            };
            let hot = item.hotkey.map(|c| c.to_string()).unwrap_or_else(|| " ".to_string());
            let mut spans = vec![
                Span::styled(" ".to_string(), style),
                Span::styled(hot, hot_style),
                Span::styled("  ".to_string(), style),
            ];
            match &item.space {
                Some((total, free)) => {
                    spans.push(Span::styled(
                        format!("{:<width$}", truncate_ellipsis(&item.name, name_w), width = name_w),
                        style,
                    ));
                    spans.push(Span::styled("│", border_style));
                    spans.push(Span::styled(format!("{:>w$}", total, w = SELECTOR_SPACE_W), style));
                    spans.push(Span::styled("│", border_style));
                    spans.push(Span::styled(format!("{:>w$}", free, w = SELECTOR_SPACE_W), style));
                }
                None => {
                    let room = (inner.width as usize).saturating_sub(4);
                    let name = truncate_ellipsis(&item.name, room);
                    let used = name.chars().count();
                    spans.push(Span::styled(name, style));
                    if let Some(detail) = &item.detail {
                        let left = room.saturating_sub(used + 2);
                        if left > 3 {
                            spans.push(Span::styled(format!("  {}", truncate_ellipsis(detail, left)), style));
                        }
                    }
                }
            }
            buf.set_line(inner.x, y, &Line::from(spans), inner.width);
            y += 1;
        }
    }
}

/// Skin selection dialog with scrollable list and OK/Cancel buttons
pub struct SkinSelectDialog<'a> {
    pub skins: &'a [(String, Option<std::path::PathBuf>)],
    pub selected_index: usize,
    pub scroll_offset: usize,
    pub focus: u8,
}

impl<'a> SkinSelectDialog<'a> {
    pub fn new(
        skins: &'a [(String, Option<std::path::PathBuf>)],
        selected_index: usize,
        scroll_offset: usize,
        focus: u8,
    ) -> Self {
        Self {
            skins,
            selected_index,
            scroll_offset,
            focus,
        }
    }
}

impl<'a> Widget for SkinSelectDialog<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let max_list_height: u16 = 12;
        // +6 = outer padding (2) + border (2) + separator + button row
        let dialog_height = (self.skins.len() as u16 + 6)
            .min(max_list_height + 6)
            .min(area.height.saturating_sub(2));
        let dialog_width = 52.min(area.width.saturating_sub(4));

        let outer_area = centered_rect(dialog_width, dialog_height, area);

        render_shadow(outer_area, area, buf);

        Clear.render(outer_area, buf);
        for y in outer_area.y..outer_area.y + outer_area.height {
            for x in outer_area.x..outer_area.x + outer_area.width {
                buf[(x, y)].set_style(skin::dialog_default());
            }
        }

        let border_area = Rect::new(
            outer_area.x + 1,
            outer_area.y + 1,
            outer_area.width.saturating_sub(2),
            outer_area.height.saturating_sub(2),
        );

        let border_style = skin::dialog_default();
        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(border_style)
            .style(skin::dialog_default());

        let inner = block.inner(border_area);
        block.render(border_area, buf);

        let title_str = " Skin ";
        let title_len = title_str.len() as u16;
        let title_x = border_area.x + (border_area.width.saturating_sub(title_len)) / 2;
        buf.set_string(title_x, border_area.y, title_str, skin::dialog_title());

        // Layout: list area, separator, buttons
        let list_height = inner.height.saturating_sub(2); // -1 separator -1 buttons

        // Render skin list
        for i in 0..list_height as usize {
            let item_idx = self.scroll_offset + i;
            if item_idx >= self.skins.len() {
                break;
            }
            let y = inner.y + i as u16;
            let (name, _) = &self.skins[item_idx];

            let style = if item_idx == self.selected_index {
                skin::dialog_focus().add_modifier(Modifier::BOLD)
            } else {
                skin::dialog_default()
            };

            // Fill full width with style, then write name
            for x in inner.x..inner.x + inner.width {
                buf[(x, y)].set_style(style);
                buf[(x, y)].set_symbol(" ");
            }
            let display = format!(" {} ", name);
            let truncated: String = display.chars().take(inner.width as usize).collect();
            buf.set_string(inner.x, y, &truncated, style);
        }

        // Separator line
        let sep_y = inner.y + list_height;
        if sep_y < border_area.y + border_area.height - 1 {
            buf.set_string(border_area.x, sep_y, skin::line_leftmiddle(), border_style);
            for x in (border_area.x + 1)..(border_area.x + border_area.width - 1) {
                buf.set_string(x, sep_y, skin::line_horiz(), border_style);
            }
            buf.set_string(
                border_area.x + border_area.width - 1,
                sep_y,
                skin::line_rightmiddle(),
                border_style,
            );
        }

        // Buttons
        let btn_y = sep_y + 1;
        if btn_y < border_area.y + border_area.height - 1 {
            let ok_style = if self.focus == 1 {
                skin::dialog_focus()
            } else {
                skin::dialog_default()
            };
            let cancel_style = if self.focus == 2 {
                skin::dialog_focus()
            } else {
                skin::dialog_default()
            };

            let buttons = Line::from(vec![
                Span::raw("  "),
                Span::styled("[ OK ]", ok_style),
                Span::raw("   "),
                Span::styled("[ Cancel ]", cancel_style),
                Span::raw("  "),
            ]);
            let btn_para = Paragraph::new(buttons)
                .style(skin::dialog_default())
                .alignment(Alignment::Center);
            let btn_area = Rect::new(inner.x, btn_y, inner.width, 1);
            btn_para.render(btn_area, buf);
        }
    }
}

/// Add storage dialog with two input fields
pub struct AddStorageDialog<'a> {
    pub name: &'a str,
    pub link_url: &'a str,
    pub active_field: usize,
    pub cursor_pos: usize,
}

impl<'a> AddStorageDialog<'a> {
    pub fn new(name: &'a str, link_url: &'a str, active_field: usize, cursor_pos: usize) -> Self {
        Self {
            name,
            link_url,
            active_field,
            cursor_pos,
        }
    }
}

impl<'a> Widget for AddStorageDialog<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let dialog_width = 60.min(area.width.saturating_sub(4));
        let dialog_height = 10.min(area.height.saturating_sub(2));

        let dialog_area = centered_rect(dialog_width, dialog_height, area);

        Clear.render(dialog_area, buf);

        let block = Block::default()
            .title(Span::styled(" Add Storage ", skin::dialog_title()))
            .borders(Borders::ALL)
            .border_style(skin::dialog_default())
            .style(skin::dialog_default());

        let inner = block.inner(dialog_area);
        block.render(dialog_area, buf);

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(1),
                Constraint::Length(1),
                Constraint::Length(1),
                Constraint::Length(1),
                Constraint::Length(1),
                Constraint::Length(1),
                Constraint::Length(1),
            ])
            .split(inner);

        let name_label = Paragraph::new("Name:").style(skin::dialog_default());
        name_label.render(chunks[0], buf);

        let name_style = if self.active_field == 0 {
            skin::core_input()
        } else {
            skin::dialog_default()
        };
        let name_display = if self.active_field == 0 {
            format!("{}│", self.name)
        } else {
            self.name.to_string()
        };
        let name_input = Paragraph::new(name_display).style(name_style);
        name_input.render(chunks[1], buf);

        let url_label = Paragraph::new("Link URL:").style(skin::dialog_default());
        url_label.render(chunks[3], buf);

        let url_style = if self.active_field == 1 {
            skin::core_input()
        } else {
            skin::dialog_default()
        };
        let url_display = if self.active_field == 1 {
            format!("{}│", self.link_url)
        } else {
            self.link_url.to_string()
        };
        let url_input = Paragraph::new(url_display).style(url_style);
        url_input.render(chunks[4], buf);

        let hints = Paragraph::new("Tab: switch field | Enter: save | Esc: cancel")
            .style(skin::dialog_hot())
            .alignment(Alignment::Center);
        hints.render(chunks[6], buf);
    }
}

/// MC-style copy confirmation dialog with source name and destination
pub struct CopyConfirmDialog<'a> {
    pub source_name: &'a str,
    pub dest_path: &'a str,
    pub ok_selected: bool,
}

impl<'a> CopyConfirmDialog<'a> {
    pub fn new(source_name: &'a str, dest_path: &'a str, ok_selected: bool) -> Self {
        Self {
            source_name,
            dest_path,
            ok_selected,
        }
    }
}

impl<'a> Widget for CopyConfirmDialog<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let outer_width = 66.min(area.width.saturating_sub(6));
        let outer_height = 10.min(area.height.saturating_sub(3));

        let outer_area = centered_rect(outer_width, outer_height, area);

        render_shadow(outer_area, area, buf);

        Clear.render(outer_area, buf);
        for y in outer_area.y..outer_area.y + outer_area.height {
            for x in outer_area.x..outer_area.x + outer_area.width {
                buf[(x, y)].set_style(skin::dialog_default());
            }
        }

        let border_area = Rect::new(
            outer_area.x + 1,
            outer_area.y + 1,
            outer_area.width.saturating_sub(2),
            outer_area.height.saturating_sub(2),
        );

        let border_style = skin::dialog_default();
        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(border_style)
            .style(border_style);

        let inner = block.inner(border_area);
        block.render(border_area, buf);

        let title_str = " Copy ";
        let title_len = title_str.len() as u16;
        let title_x = border_area.x + (border_area.width.saturating_sub(title_len)) / 2;
        buf.set_span(
            title_x,
            border_area.y,
            &Span::styled(title_str, skin::dialog_title()),
            title_len,
        );

        let content = Rect::new(inner.x + 1, inner.y, inner.width.saturating_sub(2), inner.height);

        let content_chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(1),
                Constraint::Length(1),
                Constraint::Length(1),
                Constraint::Min(0),
            ])
            .split(content);

        let label = if self.source_name.ends_with("items") {
            format!("Copy {} to:", self.source_name)
        } else {
            format!("Copy '{}' to:", self.source_name)
        };
        let label_widget = Paragraph::new(label).style(skin::dialog_default());
        label_widget.render(content_chunks[0], buf);

        let path_widget = Paragraph::new(self.dest_path).style(skin::dialog_default());
        path_widget.render(content_chunks[2], buf);

        let sep_y = border_area.y + border_area.height - 3;
        if sep_y > border_area.y && sep_y < border_area.y + border_area.height - 1 {
            buf.set_string(border_area.x, sep_y, skin::line_leftmiddle(), border_style);
            for x in (border_area.x + 1)..(border_area.x + border_area.width - 1) {
                buf.set_string(x, sep_y, skin::line_horiz(), border_style);
            }
            buf.set_string(
                border_area.x + border_area.width - 1,
                sep_y,
                skin::line_rightmiddle(),
                border_style,
            );
        }

        let btn_y = sep_y + 1;
        let btn_area = Rect::new(inner.x, btn_y, inner.width, 1);

        let ok_style = if self.ok_selected {
            skin::dialog_focus()
        } else {
            skin::dialog_default()
        };
        let cancel_style = if !self.ok_selected {
            skin::dialog_focus()
        } else {
            skin::dialog_default()
        };

        let buttons = Line::from(vec![
            Span::styled("[ OK ]", ok_style),
            Span::raw("   "),
            Span::styled("[ Cancel ]", cancel_style),
        ]);
        let buttons_widget = Paragraph::new(buttons)
            .style(skin::dialog_default())
            .alignment(Alignment::Center);
        buttons_widget.render(btn_area, buf);
    }
}

/// Notification dialog shown when a vault device-join request arrives while
/// the vault pane is not active. Two buttons: "Vault control" / "Close".
pub struct VaultJoinNotifyDialog<'a> {
    pub message: &'a str,
    pub control_selected: bool,
}

impl<'a> VaultJoinNotifyDialog<'a> {
    pub fn new(message: &'a str, control_selected: bool) -> Self {
        Self {
            message,
            control_selected,
        }
    }
}

impl<'a> Widget for VaultJoinNotifyDialog<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let outer_width = 76.min(area.width.saturating_sub(6));
        let outer_height = 10.min(area.height.saturating_sub(3));

        let outer_area = centered_rect(outer_width, outer_height, area);

        render_shadow(outer_area, area, buf);

        Clear.render(outer_area, buf);
        for y in outer_area.y..outer_area.y + outer_area.height {
            for x in outer_area.x..outer_area.x + outer_area.width {
                buf[(x, y)].set_style(skin::dialog_default());
            }
        }

        let border_area = Rect::new(
            outer_area.x + 1,
            outer_area.y + 1,
            outer_area.width.saturating_sub(2),
            outer_area.height.saturating_sub(2),
        );

        let border_style = skin::dialog_default();
        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(border_style)
            .style(border_style);

        let inner = block.inner(border_area);
        block.render(border_area, buf);

        let title_str = " Vault join request ";
        let title_len = title_str.len() as u16;
        let title_x = border_area.x + (border_area.width.saturating_sub(title_len)) / 2;
        buf.set_span(
            title_x,
            border_area.y,
            &Span::styled(title_str, skin::dialog_title()),
            title_len,
        );

        let content = Rect::new(inner.x + 1, inner.y, inner.width.saturating_sub(2), inner.height);

        let message_widget = Paragraph::new(self.message)
            .style(skin::dialog_default())
            .alignment(Alignment::Center)
            .wrap(Wrap { trim: true });
        let message_area = Rect::new(content.x, content.y, content.width, content.height.saturating_sub(2));
        message_widget.render(message_area, buf);

        let sep_y = border_area.y + border_area.height - 3;
        if sep_y > border_area.y && sep_y < border_area.y + border_area.height - 1 {
            buf.set_string(border_area.x, sep_y, skin::line_leftmiddle(), border_style);
            for x in (border_area.x + 1)..(border_area.x + border_area.width - 1) {
                buf.set_string(x, sep_y, skin::line_horiz(), border_style);
            }
            buf.set_string(
                border_area.x + border_area.width - 1,
                sep_y,
                skin::line_rightmiddle(),
                border_style,
            );
        }

        let btn_y = sep_y + 1;
        let btn_area = Rect::new(inner.x, btn_y, inner.width, 1);

        let control_style = if self.control_selected {
            skin::dialog_focus()
        } else {
            skin::dialog_default()
        };
        let close_style = if !self.control_selected {
            skin::dialog_focus()
        } else {
            skin::dialog_default()
        };

        let buttons = Line::from(vec![
            Span::styled("[ Decide ]", control_style),
            Span::raw("   "),
            Span::styled("[ Ignore ]", close_style),
        ]);
        let buttons_widget = Paragraph::new(buttons)
            .style(skin::dialog_default())
            .alignment(Alignment::Center);
        buttons_widget.render(btn_area, buf);
    }
}

/// Vault trust-circle dialog: list of devices currently in the trust circle.
pub struct VaultTrustCircleDialog<'a> {
    pub devices: &'a [models::DeviceCard],
    pub selected_index: usize,
    pub scroll_offset: usize,
    pub focus: u8,
    pub error: Option<&'a str>,
    pub loading: bool,
}

impl<'a> VaultTrustCircleDialog<'a> {
    pub fn new(
        devices: &'a [models::DeviceCard],
        selected_index: usize,
        scroll_offset: usize,
        focus: u8,
        error: Option<&'a str>,
        loading: bool,
    ) -> Self {
        Self {
            devices,
            selected_index,
            scroll_offset,
            focus,
            error,
            loading,
        }
    }
}

impl<'a> Widget for VaultTrustCircleDialog<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        // Layout (top → bottom inside the bordered block):
        //   header (1) + list (N) + separator (1) + details (6) + separator (1) + buttons (1)
        const DETAILS_LINES: u16 = 6;
        const CHROME: u16 = 1 /*header*/ + 1 /*sep*/ + DETAILS_LINES + 1 /*sep*/ + 1 /*buttons*/;
        let max_list_height: u16 = 8;
        let row_count = (self.devices.len().max(1) as u16).min(max_list_height);
        let inner_height = row_count + CHROME;
        // outer = inner + border(2) + outer-shadow padding(2)
        let dialog_height = (inner_height + 4).min(area.height.saturating_sub(2));
        let dialog_width = 80.min(area.width.saturating_sub(4));

        let outer_area = centered_rect(dialog_width, dialog_height, area);
        render_shadow(outer_area, area, buf);
        Clear.render(outer_area, buf);
        for y in outer_area.y..outer_area.y + outer_area.height {
            for x in outer_area.x..outer_area.x + outer_area.width {
                buf[(x, y)].set_style(skin::dialog_default());
            }
        }

        let border_area = Rect::new(
            outer_area.x + 1,
            outer_area.y + 1,
            outer_area.width.saturating_sub(2),
            outer_area.height.saturating_sub(2),
        );
        let border_style = skin::dialog_default();
        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(border_style)
            .style(skin::dialog_default());
        let inner = block.inner(border_area);
        block.render(border_area, buf);

        let title_str = " Vault trust circle ";
        let title_len = title_str.len() as u16;
        let title_x = border_area.x + (border_area.width.saturating_sub(title_len)) / 2;
        buf.set_string(title_x, border_area.y, title_str, skin::dialog_title());

        // Compute row Y-coords from the bottom up so they're stable regardless
        // of any height clamping above.
        let btn_y = inner.y + inner.height.saturating_sub(1);
        let sep2_y = btn_y.saturating_sub(1);
        let details_y = sep2_y.saturating_sub(DETAILS_LINES);
        let sep1_y = details_y.saturating_sub(1);
        let header_y = inner.y;
        let list_y = header_y + 1;
        let list_height = sep1_y.saturating_sub(list_y);

        // Header
        let header_style = skin::dialog_title();
        let header = format!(" {:<28} {:<14}", "Device", "Fingerprint");
        for x in inner.x..inner.x + inner.width {
            buf[(x, header_y)].set_style(header_style);
            buf[(x, header_y)].set_symbol(" ");
        }
        let header_truncated: String = header.chars().take(inner.width as usize).collect();
        buf.set_string(inner.x, header_y, &header_truncated, header_style);

        // List
        if self.devices.is_empty() {
            let style = skin::dialog_default();
            let empty = if let Some(e) = self.error {
                format!(" Error: {}", e)
            } else if self.loading {
                "  (loading…)".to_string()
            } else {
                "  (no devices)".to_string()
            };
            buf.set_string(inner.x, list_y, &empty, style);
        } else {
            for i in 0..list_height as usize {
                let item_idx = self.scroll_offset + i;
                if item_idx >= self.devices.len() {
                    break;
                }
                let y = list_y + i as u16;
                let dev = &self.devices[item_idx];
                let style = if item_idx == self.selected_index && self.focus == 0 {
                    skin::dialog_focus().add_modifier(Modifier::BOLD)
                } else {
                    skin::dialog_default()
                };
                for x in inner.x..inner.x + inner.width {
                    buf[(x, y)].set_style(style);
                    buf[(x, y)].set_symbol(" ");
                }
                let current_marker = if dev.is_current { "*" } else { " " };
                let row = format!(
                    "{}{:<28} {:<14}",
                    current_marker,
                    truncate(&dev.display_name, 28),
                    truncate(dev.device_fingerprint.as_deref().unwrap_or(""), 14),
                );
                let row_truncated: String = row.chars().take(inner.width as usize).collect();
                buf.set_string(inner.x, y, &row_truncated, style);
            }
        }

        // Separator before details
        if sep1_y > inner.y {
            buf.set_string(border_area.x, sep1_y, skin::line_leftmiddle(), border_style);
            for x in (border_area.x + 1)..(border_area.x + border_area.width - 1) {
                buf.set_string(x, sep1_y, skin::line_horiz(), border_style);
            }
            buf.set_string(
                border_area.x + border_area.width - 1,
                sep1_y,
                skin::line_rightmiddle(),
                border_style,
            );
        }

        // Details for the currently selected device
        let label_style = skin::dialog_default();
        let value_style = skin::dialog_default();
        let value_w = (inner.width as usize).saturating_sub(14);
        let render_detail = |buf: &mut Buffer, y: u16, label: &str, value: &str| {
            for x in inner.x..inner.x + inner.width {
                buf[(x, y)].set_style(skin::dialog_default());
                buf[(x, y)].set_symbol(" ");
            }
            buf.set_string(inner.x + 1, y, format!("{:<13}", label), label_style);
            let v: String = value.chars().take(value_w).collect();
            buf.set_string(inner.x + 14, y, &v, value_style);
        };
        if let Some(dev) = self.devices.get(self.selected_index) {
            render_detail(buf, details_y, "Agent ID:", &dev.agent_id);
            render_detail(buf, details_y + 1, "Device:", &dev.display_name);
            render_detail(
                buf,
                details_y + 2,
                "Fingerprint:",
                dev.device_fingerprint.as_deref().unwrap_or(""),
            );
            render_detail(
                buf,
                details_y + 3,
                "Public key:",
                &dev.device_public_key.as_deref().map(to_base64).unwrap_or_default(),
            );
            render_detail(buf, details_y + 4, "Created:", &format_time(&dev.created_at));
            render_detail(buf, details_y + 5, "Last seen:", &format_time(&dev.last_seen_at));
        } else {
            for i in 0..DETAILS_LINES {
                for x in inner.x..inner.x + inner.width {
                    buf[(x, details_y + i)].set_style(skin::dialog_default());
                    buf[(x, details_y + i)].set_symbol(" ");
                }
            }
        }

        // Separator before buttons
        if sep2_y > details_y {
            buf.set_string(border_area.x, sep2_y, skin::line_leftmiddle(), border_style);
            for x in (border_area.x + 1)..(border_area.x + border_area.width - 1) {
                buf.set_string(x, sep2_y, skin::line_horiz(), border_style);
            }
            buf.set_string(
                border_area.x + border_area.width - 1,
                sep2_y,
                skin::line_rightmiddle(),
                border_style,
            );
        }

        // Buttons. The `Revoke` label uses the dialog "hot" style so it stands
        // out from `Close` even before it's focused.
        let revoke_style = if self.focus == 1 {
            skin::dialog_focus()
        } else {
            skin::dialog_hot()
        };
        let close_style = if self.focus == 2 {
            skin::dialog_focus()
        } else {
            skin::dialog_default()
        };
        let buttons = Line::from(vec![
            Span::styled("[ Revoke ]", revoke_style),
            Span::raw("   "),
            Span::styled("[ Close ]", close_style),
        ]);
        let btn_para = Paragraph::new(buttons)
            .style(skin::dialog_default())
            .alignment(Alignment::Center);
        let btn_area = Rect::new(inner.x, btn_y, inner.width, 1);
        btn_para.render(btn_area, buf);
    }
}

/// Vault join-requests dialog: list of pending join requests with a details
/// section and approve/decline/close buttons. Same layout as the trust-circle
/// dialog (header → list → separator → details → separator → buttons).
pub struct VaultJoinRequestsDialog<'a> {
    pub requests: &'a [VaultJoinRequest],
    pub selected_index: usize,
    pub scroll_offset: usize,
    pub focus: u8,
    pub error: Option<&'a str>,
    pub loading: bool,
}

impl<'a> VaultJoinRequestsDialog<'a> {
    pub fn new(
        requests: &'a [VaultJoinRequest],
        selected_index: usize,
        scroll_offset: usize,
        focus: u8,
        error: Option<&'a str>,
        loading: bool,
    ) -> Self {
        Self {
            requests,
            selected_index,
            scroll_offset,
            focus,
            error,
            loading,
        }
    }
}

impl<'a> Widget for VaultJoinRequestsDialog<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        // Layout (top → bottom inside the bordered block):
        //   header (1) + list (N) + separator (1) + details (8) + separator (1) + buttons (1)
        const DETAILS_LINES: u16 = 8;
        const CHROME: u16 = 1 /*header*/ + 1 /*sep*/ + DETAILS_LINES + 1 /*sep*/ + 1 /*buttons*/;
        let max_list_height: u16 = 8;
        let row_count = (self.requests.len().max(1) as u16).min(max_list_height);
        let inner_height = row_count + CHROME;
        let dialog_height = (inner_height + 4).min(area.height.saturating_sub(2));
        let dialog_width = 80.min(area.width.saturating_sub(4));

        let outer_area = centered_rect(dialog_width, dialog_height, area);
        render_shadow(outer_area, area, buf);
        Clear.render(outer_area, buf);
        for y in outer_area.y..outer_area.y + outer_area.height {
            for x in outer_area.x..outer_area.x + outer_area.width {
                buf[(x, y)].set_style(skin::dialog_default());
            }
        }

        let border_area = Rect::new(
            outer_area.x + 1,
            outer_area.y + 1,
            outer_area.width.saturating_sub(2),
            outer_area.height.saturating_sub(2),
        );
        let border_style = skin::dialog_default();
        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(border_style)
            .style(skin::dialog_default());
        let inner = block.inner(border_area);
        block.render(border_area, buf);

        let title_str = " Vault join requests ";
        let title_len = title_str.len() as u16;
        let title_x = border_area.x + (border_area.width.saturating_sub(title_len)) / 2;
        buf.set_string(title_x, border_area.y, title_str, skin::dialog_title());

        // Anchor sections from the bottom up (so heights stay stable).
        let btn_y = inner.y + inner.height.saturating_sub(1);
        let sep2_y = btn_y.saturating_sub(1);
        let details_y = sep2_y.saturating_sub(DETAILS_LINES);
        let sep1_y = details_y.saturating_sub(1);
        let header_y = inner.y;
        let list_y = header_y + 1;
        let list_height = sep1_y.saturating_sub(list_y);

        // Header
        let header_style = skin::dialog_title();
        let header = format!(" {:<28} {:<14}", "Device", "Fingerprint");
        for x in inner.x..inner.x + inner.width {
            buf[(x, header_y)].set_style(header_style);
            buf[(x, header_y)].set_symbol(" ");
        }
        let header_truncated: String = header.chars().take(inner.width as usize).collect();
        buf.set_string(inner.x, header_y, &header_truncated, header_style);

        // List
        if self.requests.is_empty() {
            let style = skin::dialog_default();
            let empty = if let Some(e) = self.error {
                format!(" Error: {}", e)
            } else if self.loading {
                "  (loading…)".to_string()
            } else {
                "  (no pending join requests)".to_string()
            };
            buf.set_string(inner.x, list_y, &empty, style);
        } else {
            for i in 0..list_height as usize {
                let item_idx = self.scroll_offset + i;
                if item_idx >= self.requests.len() {
                    break;
                }
                let y = list_y + i as u16;
                let req = &self.requests[item_idx];
                let style = if item_idx == self.selected_index && self.focus == 0 {
                    skin::dialog_focus().add_modifier(Modifier::BOLD)
                } else {
                    skin::dialog_default()
                };
                for x in inner.x..inner.x + inner.width {
                    buf[(x, y)].set_style(style);
                    buf[(x, y)].set_symbol(" ");
                }
                let row = format!(
                    " {:<28} {:<14}",
                    truncate(&req.device_name, 28),
                    truncate(&req.device_fingerprint, 14),
                );
                let row_truncated: String = row.chars().take(inner.width as usize).collect();
                buf.set_string(inner.x, y, &row_truncated, style);
            }
        }

        // Separator before details
        if sep1_y > inner.y {
            buf.set_string(border_area.x, sep1_y, skin::line_leftmiddle(), border_style);
            for x in (border_area.x + 1)..(border_area.x + border_area.width - 1) {
                buf.set_string(x, sep1_y, skin::line_horiz(), border_style);
            }
            buf.set_string(
                border_area.x + border_area.width - 1,
                sep1_y,
                skin::line_rightmiddle(),
                border_style,
            );
        }

        // Details for the currently selected request
        let label_style = skin::dialog_default();
        let value_style = skin::dialog_default();
        let value_w = (inner.width as usize).saturating_sub(14);
        let render_detail = |buf: &mut Buffer, y: u16, label: &str, value: &str| {
            for x in inner.x..inner.x + inner.width {
                buf[(x, y)].set_style(skin::dialog_default());
                buf[(x, y)].set_symbol(" ");
            }
            buf.set_string(inner.x + 1, y, format!("{:<13}", label), label_style);
            let v: String = value.chars().take(value_w).collect();
            buf.set_string(inner.x + 14, y, &v, value_style);
        };
        if let Some(req) = self.requests.get(self.selected_index) {
            render_detail(buf, details_y, "Request ID:", &req.request_id);
            render_detail(buf, details_y + 1, "Agent ID:", &req.agent_id);
            render_detail(buf, details_y + 2, "Public key:", &to_base64(&req.device_public_key));
            render_detail(buf, details_y + 3, "Fingerprint:", &req.device_fingerprint);
            render_detail(buf, details_y + 4, "Device name:", &req.device_name);
            let location = match (req.country.as_deref(), req.ip.as_deref()) {
                (Some(c), Some(p)) if !c.is_empty() && !p.is_empty() => format!("{} ({})", c, p),
                (Some(c), _) if !c.is_empty() => c.to_string(),
                (_, Some(p)) => p.to_string(),
                _ => String::new(),
            };
            render_detail(
                buf,
                details_y + 5,
                "App:",
                &req.product.as_ref().map(product_label).unwrap_or_default(),
            );
            render_detail(buf, details_y + 6, "Location:", &location);
            render_detail(
                buf,
                details_y + 7,
                "Expires:",
                &req.expires_at.as_ref().map(format_time).unwrap_or_default(),
            );
        } else {
            for i in 0..DETAILS_LINES {
                for x in inner.x..inner.x + inner.width {
                    buf[(x, details_y + i)].set_style(skin::dialog_default());
                    buf[(x, details_y + i)].set_symbol(" ");
                }
            }
        }

        // Separator before buttons
        if sep2_y > details_y {
            buf.set_string(border_area.x, sep2_y, skin::line_leftmiddle(), border_style);
            for x in (border_area.x + 1)..(border_area.x + border_area.width - 1) {
                buf.set_string(x, sep2_y, skin::line_horiz(), border_style);
            }
            buf.set_string(
                border_area.x + border_area.width - 1,
                sep2_y,
                skin::line_rightmiddle(),
                border_style,
            );
        }

        // Buttons
        let approve_style = if self.focus == 1 {
            skin::dialog_focus()
        } else {
            skin::dialog_default()
        };
        let decline_style = if self.focus == 2 {
            skin::dialog_focus()
        } else {
            skin::dialog_default()
        };
        let close_style = if self.focus == 3 {
            skin::dialog_focus()
        } else {
            skin::dialog_default()
        };
        let buttons = Line::from(vec![
            Span::styled("[ Approve ]", approve_style),
            Span::raw("   "),
            Span::styled("[ Decline ]", decline_style),
            Span::raw("   "),
            Span::styled("[ Close ]", close_style),
        ]);
        let btn_para = Paragraph::new(buttons)
            .style(skin::dialog_default())
            .alignment(Alignment::Center);
        let btn_area = Rect::new(inner.x, btn_y, inner.width, 1);
        btn_para.render(btn_area, buf);
    }
}

/// Red confirmation modal shown on top of the trust-circle dialog when the
/// user picks Revoke. Asks "Yes / No" before sending the irreversible action.
pub struct VaultRevokeConfirmDialog<'a> {
    pub device_name: &'a str,
    pub agent_id: &'a str,
    pub yes_selected: bool,
}

impl<'a> VaultRevokeConfirmDialog<'a> {
    pub fn new(device_name: &'a str, agent_id: &'a str, yes_selected: bool) -> Self {
        Self {
            device_name,
            agent_id,
            yes_selected,
        }
    }
}

impl<'a> Widget for VaultRevokeConfirmDialog<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let outer_width = 60.min(area.width.saturating_sub(6));
        let outer_height = 10.min(area.height.saturating_sub(3));

        let outer_area = centered_rect(outer_width, outer_height, area);
        render_shadow(outer_area, area, buf);
        Clear.render(outer_area, buf);

        let danger_bg = skin::error_default();
        for y in outer_area.y..outer_area.y + outer_area.height {
            for x in outer_area.x..outer_area.x + outer_area.width {
                buf[(x, y)].set_style(danger_bg);
            }
        }

        let border_area = Rect::new(
            outer_area.x + 1,
            outer_area.y + 1,
            outer_area.width.saturating_sub(2),
            outer_area.height.saturating_sub(2),
        );
        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(danger_bg.add_modifier(Modifier::BOLD))
            .style(danger_bg);
        let inner = block.inner(border_area);
        block.render(border_area, buf);

        let title_str = " Revoke device ";
        let title_len = title_str.len() as u16;
        let title_x = border_area.x + (border_area.width.saturating_sub(title_len)) / 2;
        buf.set_string(
            title_x,
            border_area.y,
            title_str,
            danger_bg.add_modifier(Modifier::BOLD),
        );

        // Bottom-up anchoring: button row last, separator above it, message
        // body fills what's left.
        let btn_y = inner.y + inner.height.saturating_sub(1);
        let sep_y = btn_y.saturating_sub(1);

        let body_height = sep_y.saturating_sub(inner.y);
        let body_area = Rect::new(inner.x + 1, inner.y, inner.width.saturating_sub(2), body_height);
        let line1 = Paragraph::new(format!("Revoke '{}' from the trust circle?", self.device_name))
            .style(danger_bg)
            .alignment(Alignment::Center);
        let line2 = Paragraph::new(format!("({})", self.agent_id))
            .style(danger_bg)
            .alignment(Alignment::Center);
        let line3 = Paragraph::new("This cannot be undone.")
            .style(danger_bg.add_modifier(Modifier::BOLD))
            .alignment(Alignment::Center);
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(1),
                Constraint::Length(1),
                Constraint::Length(1),
                Constraint::Min(0),
            ])
            .split(body_area);
        line1.render(chunks[0], buf);
        line2.render(chunks[1], buf);
        line3.render(chunks[2], buf);

        // Separator before buttons (spans the borders).
        if sep_y > inner.y {
            let sep_style = danger_bg.add_modifier(Modifier::BOLD);
            buf.set_string(border_area.x, sep_y, skin::line_leftmiddle(), sep_style);
            for x in (border_area.x + 1)..(border_area.x + border_area.width - 1) {
                buf.set_string(x, sep_y, skin::line_horiz(), sep_style);
            }
            buf.set_string(
                border_area.x + border_area.width - 1,
                sep_y,
                skin::line_rightmiddle(),
                sep_style,
            );
        }

        let yes_style = if self.yes_selected {
            skin::error_focus()
        } else {
            danger_bg
        };
        let no_style = if !self.yes_selected {
            skin::error_focus()
        } else {
            danger_bg
        };
        let buttons = Line::from(vec![
            Span::raw("  "),
            Span::styled("[ Revoke ]", yes_style),
            Span::raw("    "),
            Span::styled("[ Cancel ]", no_style),
            Span::raw("  "),
        ]);
        let btn_para = Paragraph::new(buttons).style(danger_bg).alignment(Alignment::Center);
        let btn_area = Rect::new(inner.x, btn_y, inner.width, 1);
        btn_para.render(btn_area, buf);
    }
}

/// Display the BIP39 recovery words once after vault setup or regenerate.
/// Buttons: "I've written them down" (acks + clears) and "Close" (keep them).
pub struct RecoveryWordsDisplayDialog<'a> {
    pub words: &'a [String],
    /// 0 = ack button, 1 = close button
    pub focus: u8,
}

impl<'a> RecoveryWordsDisplayDialog<'a> {
    pub fn new(words: &'a [String], focus: u8) -> Self {
        Self { words, focus }
    }
}

impl<'a> Widget for RecoveryWordsDisplayDialog<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        const COLS: usize = 4;
        let rows = self.words.len().div_ceil(COLS); // ceil
        // chrome: borders(2) + outer-shadow padding(2) + warning(1) + spacer(1) + sep(1) + buttons(1)
        let inner_height = (rows as u16) + 4;
        let outer_height = (inner_height + 4).min(area.height.saturating_sub(2));
        let outer_width = 76.min(area.width.saturating_sub(6));

        let outer_area = centered_rect(outer_width, outer_height, area);
        render_shadow(outer_area, area, buf);
        Clear.render(outer_area, buf);
        for y in outer_area.y..outer_area.y + outer_area.height {
            for x in outer_area.x..outer_area.x + outer_area.width {
                buf[(x, y)].set_style(skin::dialog_default());
            }
        }

        let border_area = Rect::new(
            outer_area.x + 1,
            outer_area.y + 1,
            outer_area.width.saturating_sub(2),
            outer_area.height.saturating_sub(2),
        );
        let border_style = skin::dialog_default();
        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(border_style)
            .style(skin::dialog_default());
        let inner = block.inner(border_area);
        block.render(border_area, buf);

        let title_str = " Vault recovery words ";
        let title_len = title_str.len() as u16;
        let title_x = border_area.x + (border_area.width.saturating_sub(title_len)) / 2;
        buf.set_string(title_x, border_area.y, title_str, skin::dialog_title());

        // Anchor button row at bottom; warning at top.
        let btn_y = inner.y + inner.height.saturating_sub(1);
        let sep_y = btn_y.saturating_sub(1);
        let warn_y = inner.y;
        let grid_y = inner.y + 1;
        let grid_height = sep_y.saturating_sub(grid_y);

        // Warning line — use the error skin's default (red bg + light fg)
        // for a strongly-contrasting in-skin highlight.
        let warn_style = skin::error_default().add_modifier(Modifier::BOLD);
        let warning = Paragraph::new("Write these down. They will not be shown again.")
            .style(warn_style)
            .alignment(Alignment::Center);
        warning.render(Rect::new(inner.x, warn_y, inner.width, 1), buf);

        // Word grid: 4 columns × 6 rows (for 24 words)
        let col_w = (inner.width as usize) / COLS;
        let value_style = skin::dialog_default().add_modifier(Modifier::BOLD);
        for (i, w) in self.words.iter().enumerate() {
            let row = i / COLS;
            let col = i % COLS;
            if (row as u16) >= grid_height {
                break;
            }
            let y = grid_y + row as u16;
            let x = inner.x + (col as u16 * col_w as u16);
            let label = format!(" {:>2}. {}", i + 1, w);
            let truncated: String = label.chars().take(col_w).collect();
            buf.set_string(x, y, &truncated, value_style);
        }

        // Separator before buttons
        if sep_y > grid_y {
            buf.set_string(border_area.x, sep_y, skin::line_leftmiddle(), border_style);
            for x in (border_area.x + 1)..(border_area.x + border_area.width - 1) {
                buf.set_string(x, sep_y, skin::line_horiz(), border_style);
            }
            buf.set_string(
                border_area.x + border_area.width - 1,
                sep_y,
                skin::line_rightmiddle(),
                border_style,
            );
        }

        // Buttons
        let ack_style = if self.focus == 0 {
            skin::dialog_focus()
        } else {
            skin::dialog_default()
        };
        let close_style = if self.focus == 1 {
            skin::dialog_focus()
        } else {
            skin::dialog_default()
        };
        let buttons = Line::from(vec![
            Span::styled("[ I've written them down ]", ack_style),
            Span::raw("    "),
            Span::styled("[ Close ]", close_style),
        ]);
        let btn_para = Paragraph::new(buttons)
            .style(skin::dialog_default())
            .alignment(Alignment::Center);
        btn_para.render(Rect::new(inner.x, btn_y, inner.width, 1), buf);
    }
}

/// Red confirmation modal asking the user to confirm regenerating the vault
/// recovery words (irreversible — invalidates the existing recovery key).
pub struct VaultRecoveryRegenerateConfirmDialog {
    pub yes_selected: bool,
}

impl VaultRecoveryRegenerateConfirmDialog {
    pub fn new(yes_selected: bool) -> Self {
        Self { yes_selected }
    }
}

impl Widget for VaultRecoveryRegenerateConfirmDialog {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let outer_width = 64.min(area.width.saturating_sub(6));
        let outer_height = 10.min(area.height.saturating_sub(3));

        let outer_area = centered_rect(outer_width, outer_height, area);
        render_shadow(outer_area, area, buf);
        Clear.render(outer_area, buf);

        let danger_bg = skin::error_default();
        for y in outer_area.y..outer_area.y + outer_area.height {
            for x in outer_area.x..outer_area.x + outer_area.width {
                buf[(x, y)].set_style(danger_bg);
            }
        }

        let border_area = Rect::new(
            outer_area.x + 1,
            outer_area.y + 1,
            outer_area.width.saturating_sub(2),
            outer_area.height.saturating_sub(2),
        );
        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(danger_bg.add_modifier(Modifier::BOLD))
            .style(danger_bg);
        let inner = block.inner(border_area);
        block.render(border_area, buf);

        let title_str = " Regenerate recovery words ";
        let title_len = title_str.len() as u16;
        let title_x = border_area.x + (border_area.width.saturating_sub(title_len)) / 2;
        buf.set_string(
            title_x,
            border_area.y,
            title_str,
            danger_bg.add_modifier(Modifier::BOLD),
        );

        // Bottom-up: button row last, separator above it, body fills the rest.
        let btn_y = inner.y + inner.height.saturating_sub(1);
        let sep_y = btn_y.saturating_sub(1);

        let body_height = sep_y.saturating_sub(inner.y);
        let body_area = Rect::new(inner.x + 1, inner.y, inner.width.saturating_sub(2), body_height);
        let line1 = Paragraph::new("Generate a new set of vault recovery words?")
            .style(danger_bg)
            .alignment(Alignment::Center);
        let line2 = Paragraph::new("Your current recovery words will be invalidated.")
            .style(danger_bg)
            .alignment(Alignment::Center);
        let line3 = Paragraph::new("This cannot be undone.")
            .style(danger_bg.add_modifier(Modifier::BOLD))
            .alignment(Alignment::Center);
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(1),
                Constraint::Length(1),
                Constraint::Length(1),
                Constraint::Min(0),
            ])
            .split(body_area);
        line1.render(chunks[0], buf);
        line2.render(chunks[1], buf);
        line3.render(chunks[2], buf);

        // Separator before buttons (spans the borders).
        if sep_y > inner.y {
            let sep_style = danger_bg.add_modifier(Modifier::BOLD);
            buf.set_string(border_area.x, sep_y, skin::line_leftmiddle(), sep_style);
            for x in (border_area.x + 1)..(border_area.x + border_area.width - 1) {
                buf.set_string(x, sep_y, skin::line_horiz(), sep_style);
            }
            buf.set_string(
                border_area.x + border_area.width - 1,
                sep_y,
                skin::line_rightmiddle(),
                sep_style,
            );
        }

        let yes_style = if self.yes_selected {
            skin::error_focus()
        } else {
            danger_bg
        };
        let no_style = if !self.yes_selected {
            skin::error_focus()
        } else {
            danger_bg
        };
        let buttons = Line::from(vec![
            Span::raw("  "),
            Span::styled("[ Regenerate ]", yes_style),
            Span::raw("    "),
            Span::styled("[ Cancel ]", no_style),
            Span::raw("  "),
        ]);
        let btn_para = Paragraph::new(buttons).style(danger_bg).alignment(Alignment::Center);
        let btn_area = Rect::new(inner.x, btn_y, inner.width, 1);
        btn_para.render(btn_area, buf);
    }
}

/// Vault recovery dialog: lets the user paste their BIP39 mnemonic to unlock
/// a vault whose identity key isn't loaded on this device. Wrapped text input
/// box with [ Recover ] / [ Cancel ] buttons.
pub struct VaultRecoveryInputDialog<'a> {
    pub text: &'a str,
    /// 0 = input, 1 = Recover, 2 = Cancel
    pub focus: u8,
}

impl<'a> VaultRecoveryInputDialog<'a> {
    pub fn new(text: &'a str, focus: u8) -> Self {
        Self { text, focus }
    }
}

impl<'a> Widget for VaultRecoveryInputDialog<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let outer_width = 76.min(area.width.saturating_sub(6));
        let outer_height = 12.min(area.height.saturating_sub(3));
        let outer_area = centered_rect(outer_width, outer_height, area);
        render_shadow(outer_area, area, buf);
        Clear.render(outer_area, buf);
        for y in outer_area.y..outer_area.y + outer_area.height {
            for x in outer_area.x..outer_area.x + outer_area.width {
                buf[(x, y)].set_style(skin::dialog_default());
            }
        }

        let border_area = Rect::new(
            outer_area.x + 1,
            outer_area.y + 1,
            outer_area.width.saturating_sub(2),
            outer_area.height.saturating_sub(2),
        );
        let border_style = skin::dialog_default();
        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(border_style)
            .style(skin::dialog_default());
        let inner = block.inner(border_area);
        block.render(border_area, buf);

        let title_str = " Vault Recovery ";
        let title_len = title_str.len() as u16;
        let title_x = border_area.x + (border_area.width.saturating_sub(title_len)) / 2;
        buf.set_string(title_x, border_area.y, title_str, skin::dialog_title());

        let btn_y = inner.y + inner.height.saturating_sub(1);
        let sep_y = btn_y.saturating_sub(1);
        let body_height = sep_y.saturating_sub(inner.y);
        let body_area = Rect::new(inner.x + 1, inner.y, inner.width.saturating_sub(2), body_height);

        // Body: hint + multi-line input box.
        let hint =
            Paragraph::new("Enter your 24 BIP39 recovery words, separated by spaces:").style(skin::dialog_default());
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(1), Constraint::Length(1), Constraint::Min(0)])
            .split(body_area);
        hint.render(chunks[0], buf);

        let input_style = if self.focus == 0 {
            skin::core_input()
        } else {
            skin::dialog_default()
        };
        let display = if self.focus == 0 {
            format!("{}│", self.text)
        } else {
            self.text.to_string()
        };
        let input = Paragraph::new(display).style(input_style).wrap(Wrap { trim: false });
        input.render(chunks[2], buf);

        if sep_y > inner.y {
            buf.set_string(border_area.x, sep_y, skin::line_leftmiddle(), border_style);
            for x in (border_area.x + 1)..(border_area.x + border_area.width - 1) {
                buf.set_string(x, sep_y, skin::line_horiz(), border_style);
            }
            buf.set_string(
                border_area.x + border_area.width - 1,
                sep_y,
                skin::line_rightmiddle(),
                border_style,
            );
        }

        let recover_style = if self.focus == 1 {
            skin::dialog_focus()
        } else {
            skin::dialog_default()
        };
        let cancel_style = if self.focus == 2 {
            skin::dialog_focus()
        } else {
            skin::dialog_default()
        };
        let buttons = Line::from(vec![
            Span::raw("  "),
            Span::styled("[ Recover ]", recover_style),
            Span::raw("    "),
            Span::styled("[ Cancel ]", cancel_style),
            Span::raw("  "),
        ]);
        let btn_para = Paragraph::new(buttons)
            .style(skin::dialog_default())
            .alignment(Alignment::Center);
        let btn_area = Rect::new(inner.x, btn_y, inner.width, 1);
        btn_para.render(btn_area, buf);
    }
}

/// Step-up code dialog: the backend emailed a code before it accepts the
/// vault recovery — one text box with [ Verify ] / [ Cancel ] buttons.
pub struct StepUpCodeDialog<'a> {
    pub text: &'a str,
    /// When the backend stops accepting the code (RFC 3339).
    pub expires_at: &'a str,
    /// 0 = input, 1 = Verify, 2 = Cancel
    pub focus: u8,
}

impl<'a> StepUpCodeDialog<'a> {
    pub fn new(text: &'a str, expires_at: &'a str, focus: u8) -> Self {
        Self {
            text,
            expires_at,
            focus,
        }
    }
}

impl<'a> Widget for StepUpCodeDialog<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let outer_width = 76.min(area.width.saturating_sub(6));
        let outer_height = 12.min(area.height.saturating_sub(3));
        let outer_area = centered_rect(outer_width, outer_height, area);
        render_shadow(outer_area, area, buf);
        Clear.render(outer_area, buf);
        for y in outer_area.y..outer_area.y + outer_area.height {
            for x in outer_area.x..outer_area.x + outer_area.width {
                buf[(x, y)].set_style(skin::dialog_default());
            }
        }

        let border_area = Rect::new(
            outer_area.x + 1,
            outer_area.y + 1,
            outer_area.width.saturating_sub(2),
            outer_area.height.saturating_sub(2),
        );
        let border_style = skin::dialog_default();
        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(border_style)
            .style(skin::dialog_default());
        let inner = block.inner(border_area);
        block.render(border_area, buf);

        let title_str = " Confirmation code ";
        let title_len = title_str.len() as u16;
        let title_x = border_area.x + (border_area.width.saturating_sub(title_len)) / 2;
        buf.set_string(title_x, border_area.y, title_str, skin::dialog_title());

        let btn_y = inner.y + inner.height.saturating_sub(1);
        let sep_y = btn_y.saturating_sub(1);
        let body_height = sep_y.saturating_sub(inner.y);
        let body_area = Rect::new(inner.x + 1, inner.y, inner.width.saturating_sub(2), body_height);

        // Body: hint + multi-line input box.
        let hint = Paragraph::new(format!(
            "Enter the code we emailed you (valid until {}):",
            self.expires_at
        ))
        .style(skin::dialog_default());
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(1), Constraint::Length(1), Constraint::Min(0)])
            .split(body_area);
        hint.render(chunks[0], buf);

        let input_style = if self.focus == 0 {
            skin::core_input()
        } else {
            skin::dialog_default()
        };
        let display = if self.focus == 0 {
            format!("{}│", self.text)
        } else {
            self.text.to_string()
        };
        let input = Paragraph::new(display).style(input_style).wrap(Wrap { trim: false });
        input.render(chunks[2], buf);

        if sep_y > inner.y {
            buf.set_string(border_area.x, sep_y, skin::line_leftmiddle(), border_style);
            for x in (border_area.x + 1)..(border_area.x + border_area.width - 1) {
                buf.set_string(x, sep_y, skin::line_horiz(), border_style);
            }
            buf.set_string(
                border_area.x + border_area.width - 1,
                sep_y,
                skin::line_rightmiddle(),
                border_style,
            );
        }

        let recover_style = if self.focus == 1 {
            skin::dialog_focus()
        } else {
            skin::dialog_default()
        };
        let cancel_style = if self.focus == 2 {
            skin::dialog_focus()
        } else {
            skin::dialog_default()
        };
        let buttons = Line::from(vec![
            Span::raw("  "),
            Span::styled("[ Verify ]", recover_style),
            Span::raw("    "),
            Span::styled("[ Cancel ]", cancel_style),
            Span::raw("  "),
        ]);
        let btn_para = Paragraph::new(buttons)
            .style(skin::dialog_default())
            .alignment(Alignment::Center);
        let btn_area = Rect::new(inner.x, btn_y, inner.width, 1);
        btn_para.render(btn_area, buf);
    }
}

/// Red notification shown when this device received a `vault_device_revoked`
/// push: another trusted device removed it from the trust circle. Single OK
/// button that acks the flag and dismisses.
pub struct VaultRevokedNotifyDialog;

impl Widget for VaultRevokedNotifyDialog {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let outer_width = 64.min(area.width.saturating_sub(6));
        let outer_height = 9.min(area.height.saturating_sub(3));

        let outer_area = centered_rect(outer_width, outer_height, area);
        render_shadow(outer_area, area, buf);
        Clear.render(outer_area, buf);

        let danger_bg = skin::error_default();
        for y in outer_area.y..outer_area.y + outer_area.height {
            for x in outer_area.x..outer_area.x + outer_area.width {
                buf[(x, y)].set_style(danger_bg);
            }
        }

        let border_area = Rect::new(
            outer_area.x + 1,
            outer_area.y + 1,
            outer_area.width.saturating_sub(2),
            outer_area.height.saturating_sub(2),
        );
        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(danger_bg.add_modifier(Modifier::BOLD))
            .style(danger_bg);
        let inner = block.inner(border_area);
        block.render(border_area, buf);

        let title_str = " Device Revoked ";
        let title_len = title_str.len() as u16;
        let title_x = border_area.x + (border_area.width.saturating_sub(title_len)) / 2;
        buf.set_string(
            title_x,
            border_area.y,
            title_str,
            danger_bg.add_modifier(Modifier::BOLD),
        );

        let btn_y = inner.y + inner.height.saturating_sub(1);
        let sep_y = btn_y.saturating_sub(1);
        let body_height = sep_y.saturating_sub(inner.y);
        let body_area = Rect::new(inner.x + 1, inner.y, inner.width.saturating_sub(2), body_height);
        let line1 = Paragraph::new("Your device was revoked from the vault trust circle.")
            .style(danger_bg)
            .alignment(Alignment::Center);
        let line2 = Paragraph::new("To rejoin, send a new join request.")
            .style(danger_bg)
            .alignment(Alignment::Center);
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(1), Constraint::Length(1), Constraint::Min(0)])
            .split(body_area);
        line1.render(chunks[0], buf);
        line2.render(chunks[1], buf);

        if sep_y > inner.y {
            let sep_style = danger_bg.add_modifier(Modifier::BOLD);
            buf.set_string(border_area.x, sep_y, skin::line_leftmiddle(), sep_style);
            for x in (border_area.x + 1)..(border_area.x + border_area.width - 1) {
                buf.set_string(x, sep_y, skin::line_horiz(), sep_style);
            }
            buf.set_string(
                border_area.x + border_area.width - 1,
                sep_y,
                skin::line_rightmiddle(),
                sep_style,
            );
        }

        let ok_style = skin::error_focus();
        let buttons = Line::from(vec![Span::raw("  "), Span::styled("[ OK ]", ok_style), Span::raw("  ")]);
        let btn_para = Paragraph::new(buttons).style(danger_bg).alignment(Alignment::Center);
        let btn_area = Rect::new(inner.x, btn_y, inner.width, 1);
        btn_para.render(btn_area, buf);
    }
}

/// Informational dialog shown when this device is online but not yet a member
/// of the vault trust circle. Two buttons: "Send join request" / "Cancel".
pub struct VaultJoinPromptDialog {
    pub send_selected: bool,
}

impl VaultJoinPromptDialog {
    pub fn new(send_selected: bool) -> Self {
        Self { send_selected }
    }
}

impl Widget for VaultJoinPromptDialog {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let outer_width = 64.min(area.width.saturating_sub(6));
        let outer_height = 9.min(area.height.saturating_sub(3));

        let outer_area = centered_rect(outer_width, outer_height, area);
        render_shadow(outer_area, area, buf);
        Clear.render(outer_area, buf);
        for y in outer_area.y..outer_area.y + outer_area.height {
            for x in outer_area.x..outer_area.x + outer_area.width {
                buf[(x, y)].set_style(skin::dialog_default());
            }
        }

        let border_area = Rect::new(
            outer_area.x + 1,
            outer_area.y + 1,
            outer_area.width.saturating_sub(2),
            outer_area.height.saturating_sub(2),
        );
        let border_style = skin::dialog_default();
        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(border_style)
            .style(skin::dialog_default());
        let inner = block.inner(border_area);
        block.render(border_area, buf);

        let title_str = " Vault trust circle ";
        let title_len = title_str.len() as u16;
        let title_x = border_area.x + (border_area.width.saturating_sub(title_len)) / 2;
        buf.set_string(title_x, border_area.y, title_str, skin::dialog_title());

        // Bottom-up: button row last, separator above it, body fills the rest.
        let btn_y = inner.y + inner.height.saturating_sub(1);
        let sep_y = btn_y.saturating_sub(1);

        let body_height = sep_y.saturating_sub(inner.y);
        let body_area = Rect::new(inner.x + 1, inner.y, inner.width.saturating_sub(2), body_height);
        let line1 = Paragraph::new("This device is not in the vault trust circle.")
            .style(skin::dialog_default())
            .alignment(Alignment::Center);
        let line2 = Paragraph::new("Send a join request to your other devices?")
            .style(skin::dialog_default())
            .alignment(Alignment::Center);
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(1), Constraint::Length(1), Constraint::Min(0)])
            .split(body_area);
        line1.render(chunks[0], buf);
        line2.render(chunks[1], buf);

        // Separator before buttons.
        if sep_y > inner.y {
            buf.set_string(border_area.x, sep_y, skin::line_leftmiddle(), border_style);
            for x in (border_area.x + 1)..(border_area.x + border_area.width - 1) {
                buf.set_string(x, sep_y, skin::line_horiz(), border_style);
            }
            buf.set_string(
                border_area.x + border_area.width - 1,
                sep_y,
                skin::line_rightmiddle(),
                border_style,
            );
        }

        let send_style = if self.send_selected {
            skin::dialog_focus()
        } else {
            skin::dialog_default()
        };
        let cancel_style = if !self.send_selected {
            skin::dialog_focus()
        } else {
            skin::dialog_default()
        };
        let buttons = Line::from(vec![
            Span::raw("  "),
            Span::styled("[ Send join request ]", send_style),
            Span::raw("    "),
            Span::styled("[ Cancel ]", cancel_style),
            Span::raw("  "),
        ]);
        let btn_para = Paragraph::new(buttons)
            .style(skin::dialog_default())
            .alignment(Alignment::Center);
        let btn_area = Rect::new(inner.x, btn_y, inner.width, 1);
        btn_para.render(btn_area, buf);
    }
}

/// An inbound nearby offer: who offers what, and Accept / Decline. Accept
/// opens the share in the other panel.
pub struct ShareOfferDialog<'a> {
    pub offer: &'a InboundOfferView,
    pub accept_selected: bool,
}

impl<'a> ShareOfferDialog<'a> {
    pub fn new(offer: &'a InboundOfferView, accept_selected: bool) -> Self {
        Self { offer, accept_selected }
    }
}

impl Widget for ShareOfferDialog<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let outer_width = 64.min(area.width.saturating_sub(6));
        let outer_height = 10.min(area.height.saturating_sub(3));

        let outer_area = centered_rect(outer_width, outer_height, area);
        render_shadow(outer_area, area, buf);
        Clear.render(outer_area, buf);
        for y in outer_area.y..outer_area.y + outer_area.height {
            for x in outer_area.x..outer_area.x + outer_area.width {
                buf[(x, y)].set_style(skin::dialog_default());
            }
        }

        let border_area = Rect::new(
            outer_area.x + 1,
            outer_area.y + 1,
            outer_area.width.saturating_sub(2),
            outer_area.height.saturating_sub(2),
        );
        let border_style = skin::dialog_default();
        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(border_style)
            .style(skin::dialog_default());
        let inner = block.inner(border_area);
        block.render(border_area, buf);

        let title_str = " Nearby offer ";
        let title_len = title_str.len() as u16;
        let title_x = border_area.x + (border_area.width.saturating_sub(title_len)) / 2;
        buf.set_string(title_x, border_area.y, title_str, skin::dialog_title());

        // Bottom-up: button row last, separator above it, body fills the rest.
        let btn_y = inner.y + inner.height.saturating_sub(1);
        let sep_y = btn_y.saturating_sub(1);

        let body_height = sep_y.saturating_sub(inner.y);
        let body_area = Rect::new(inner.x + 1, inner.y, inner.width.saturating_sub(2), body_height);
        let width = body_area.width as usize;
        // "John Doe (MacBook Pro)" for a signed-in sender, else the device.
        let sender = match self
            .offer
            .sender_display_name
            .as_deref()
            .filter(|d| !d.trim().is_empty())
        {
            Some(display) => format!("{display} ({})", self.offer.sender_name),
            None => self.offer.sender_name.clone(),
        };
        let what = match (self.offer.description.trim(), self.offer.bytes) {
            ("", None) => "a share".to_string(),
            ("", Some(bytes)) => format!("a share of {}", super::panel::format_size(bytes)),
            (desc, None) => desc.to_string(),
            (desc, Some(bytes)) => format!("{desc} ({})", super::panel::format_size(bytes)),
        };
        let line1 = Paragraph::new(truncate(&format!("{sender} offers you:"), width))
            .style(skin::dialog_default())
            .alignment(Alignment::Center);
        let line2 = Paragraph::new(truncate(&what, width))
            .style(skin::dialog_default().add_modifier(Modifier::BOLD))
            .alignment(Alignment::Center);
        let line3 = Paragraph::new("Accept opens it in the other panel; F5 copies it.")
            .style(skin::dialog_default())
            .alignment(Alignment::Center);
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(1),
                Constraint::Length(1),
                Constraint::Length(1),
                Constraint::Min(0),
            ])
            .split(body_area);
        line1.render(chunks[0], buf);
        line2.render(chunks[1], buf);
        line3.render(chunks[2], buf);

        // Separator before buttons.
        if sep_y > inner.y {
            buf.set_string(border_area.x, sep_y, skin::line_leftmiddle(), border_style);
            for x in (border_area.x + 1)..(border_area.x + border_area.width - 1) {
                buf.set_string(x, sep_y, skin::line_horiz(), border_style);
            }
            buf.set_string(
                border_area.x + border_area.width - 1,
                sep_y,
                skin::line_rightmiddle(),
                border_style,
            );
        }

        let accept_style = if self.accept_selected {
            skin::dialog_focus()
        } else {
            skin::dialog_default()
        };
        let decline_style = if !self.accept_selected {
            skin::dialog_focus()
        } else {
            skin::dialog_default()
        };
        let buttons = Line::from(vec![
            Span::raw("  "),
            Span::styled("[ Accept ]", accept_style),
            Span::raw("    "),
            Span::styled("[ Decline ]", decline_style),
            Span::raw("  "),
        ]);
        let btn_para = Paragraph::new(buttons)
            .style(skin::dialog_default())
            .alignment(Alignment::Center);
        let btn_area = Rect::new(inner.x, btn_y, inner.width, 1);
        btn_para.render(btn_area, buf);
    }
}

fn truncate(s: &str, max: usize) -> String {
    let chars: Vec<char> = s.chars().collect();
    if chars.len() <= max {
        return s.to_string();
    }
    if max == 0 {
        return String::new();
    }
    let cut = max.saturating_sub(1);
    let mut out: String = chars.iter().take(cut).collect();
    out.push('…');
    out
}

/// MC-style mkdir dialog with editable directory name
pub struct MkdirDialog<'a> {
    pub dir_name: &'a str,
    pub cursor_pos: usize,
    pub focus: u8,
}

impl<'a> MkdirDialog<'a> {
    pub fn new(dir_name: &'a str, cursor_pos: usize, focus: u8) -> Self {
        Self {
            dir_name,
            cursor_pos,
            focus,
        }
    }
}

impl<'a> Widget for MkdirDialog<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let outer_width = 66.min(area.width.saturating_sub(6));
        let outer_height = 8.min(area.height.saturating_sub(3));

        let outer_area = centered_rect(outer_width, outer_height, area);

        render_shadow(outer_area, area, buf);

        Clear.render(outer_area, buf);
        for y in outer_area.y..outer_area.y + outer_area.height {
            for x in outer_area.x..outer_area.x + outer_area.width {
                buf[(x, y)].set_style(skin::dialog_default());
            }
        }

        let border_area = Rect::new(
            outer_area.x + 1,
            outer_area.y + 1,
            outer_area.width.saturating_sub(2),
            outer_area.height.saturating_sub(2),
        );

        let border_style = skin::dialog_default();
        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(border_style)
            .style(border_style);

        let inner = block.inner(border_area);
        block.render(border_area, buf);

        let title_str = " Create a new directory ";
        let title_len = title_str.len() as u16;
        let title_x = border_area.x + (border_area.width.saturating_sub(title_len)) / 2;
        buf.set_span(
            title_x,
            border_area.y,
            &Span::styled(title_str, skin::dialog_title()),
            title_len,
        );

        let content = Rect::new(inner.x + 1, inner.y, inner.width.saturating_sub(2), inner.height);

        let content_chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(1), Constraint::Length(1), Constraint::Min(0)])
            .split(content);

        let label_widget = Paragraph::new("Enter directory name:").style(skin::dialog_default());
        label_widget.render(content_chunks[0], buf);

        let input_style = skin::dialog_focus();
        let input_area = content_chunks[1];

        for x in input_area.x..input_area.x + input_area.width {
            buf[(x, input_area.y)].set_style(input_style);
        }

        let field = input_area.width.saturating_sub(1) as usize;
        let skip = self.cursor_pos.saturating_sub(field);
        for (i, ch) in self.dir_name.chars().skip(skip).enumerate() {
            if (i as u16) < input_area.width {
                buf[(input_area.x + i as u16, input_area.y)].set_char(ch);
            }
        }

        if self.focus == 0 {
            let cursor_x = input_area.x + ((self.cursor_pos - skip) as u16).min(input_area.width.saturating_sub(1));
            buf[(cursor_x, input_area.y)].set_style(skin::dialog_default().add_modifier(Modifier::REVERSED));
        }

        let sep_y = border_area.y + border_area.height - 3;
        if sep_y > border_area.y && sep_y < border_area.y + border_area.height - 1 {
            buf.set_string(border_area.x, sep_y, skin::line_leftmiddle(), border_style);
            for x in (border_area.x + 1)..(border_area.x + border_area.width - 1) {
                buf.set_string(x, sep_y, skin::line_horiz(), border_style);
            }
            buf.set_string(
                border_area.x + border_area.width - 1,
                sep_y,
                skin::line_rightmiddle(),
                border_style,
            );
        }

        let btn_y = sep_y + 1;
        let btn_area = Rect::new(inner.x, btn_y, inner.width, 1);

        let ok_style = if self.focus == 1 {
            skin::dialog_focus()
        } else {
            skin::dialog_default()
        };
        let cancel_style = if self.focus == 2 {
            skin::dialog_focus()
        } else {
            skin::dialog_default()
        };

        let buttons = Line::from(vec![
            Span::styled("[ OK ]", ok_style),
            Span::raw("   "),
            Span::styled("[ Cancel ]", cancel_style),
        ]);
        let buttons_widget = Paragraph::new(buttons)
            .style(skin::dialog_default())
            .alignment(Alignment::Center);
        buttons_widget.render(btn_area, buf);
    }
}

/// MC-style delete confirmation dialog
pub struct DeleteConfirmDialog<'a> {
    pub name: &'a str,
    pub is_dir: bool,
    pub yes_selected: bool,
}

impl<'a> DeleteConfirmDialog<'a> {
    pub fn new(name: &'a str, is_dir: bool, yes_selected: bool) -> Self {
        Self {
            name,
            is_dir,
            yes_selected,
        }
    }
}

impl<'a> Widget for DeleteConfirmDialog<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let outer_width = 42.min(area.width.saturating_sub(6));
        let outer_height = 8.min(area.height.saturating_sub(3));

        let outer_area = centered_rect(outer_width, outer_height, area);

        render_shadow(outer_area, area, buf);

        Clear.render(outer_area, buf);
        for y in outer_area.y..outer_area.y + outer_area.height {
            for x in outer_area.x..outer_area.x + outer_area.width {
                buf[(x, y)].set_style(skin::error_default());
            }
        }

        let border_area = Rect::new(
            outer_area.x + 1,
            outer_area.y + 1,
            outer_area.width.saturating_sub(2),
            outer_area.height.saturating_sub(2),
        );

        let title_str = " Delete ";
        let title_len = title_str.len() as u16;
        let border_style = skin::error_default();

        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(border_style)
            .style(border_style);

        let inner = block.inner(border_area);
        block.render(border_area, buf);

        let title_x = border_area.x + (border_area.width.saturating_sub(title_len)) / 2;
        buf.set_span(
            title_x,
            border_area.y,
            &Span::styled(title_str, skin::error_title()),
            title_len,
        );

        let content = Rect::new(inner.x + 1, inner.y, inner.width.saturating_sub(2), inner.height);

        let content_chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(1), Constraint::Length(1), Constraint::Min(0)])
            .split(content);

        let kind = if self.is_dir { "directory" } else { "file" };
        let msg = format!("Delete {}", kind);
        let msg_widget = Paragraph::new(msg)
            .style(skin::error_default())
            .alignment(Alignment::Center);
        msg_widget.render(content_chunks[0], buf);

        let name_widget = Paragraph::new(format!("'{}'?", self.name))
            .style(skin::error_default())
            .alignment(Alignment::Center);
        name_widget.render(content_chunks[1], buf);

        let sep_y = border_area.y + border_area.height - 3;
        if sep_y > border_area.y && sep_y < border_area.y + border_area.height - 1 {
            buf.set_string(border_area.x, sep_y, skin::line_leftmiddle(), border_style);
            for x in (border_area.x + 1)..(border_area.x + border_area.width - 1) {
                buf.set_string(x, sep_y, skin::line_horiz(), border_style);
            }
            buf.set_string(
                border_area.x + border_area.width - 1,
                sep_y,
                skin::line_rightmiddle(),
                border_style,
            );
        }

        let btn_y = sep_y + 1;
        let btn_area = Rect::new(inner.x, btn_y, inner.width, 1);

        let yes_style = if self.yes_selected {
            skin::error_focus()
        } else {
            skin::error_default()
        };
        let no_style = if !self.yes_selected {
            skin::error_focus()
        } else {
            skin::error_default()
        };

        let buttons = Line::from(vec![
            Span::styled("[ Yes ]", yes_style),
            Span::raw("   "),
            Span::styled("[ No ]", no_style),
        ]);
        let buttons_widget = Paragraph::new(buttons)
            .style(skin::error_default())
            .alignment(Alignment::Center);
        buttons_widget.render(btn_area, buf);
    }
}

/// Render MC-style shadow (2 right, 1 down)
pub fn render_shadow(dialog_area: Rect, area: Rect, buf: &mut Buffer) {
    let shadow_style = skin::shadow();
    for y in (dialog_area.y + 1)..=(dialog_area.y + dialog_area.height) {
        for x in dialog_area.x + dialog_area.width..(dialog_area.x + dialog_area.width + 2).min(area.x + area.width) {
            if y < area.y + area.height {
                buf[(x, y)].set_style(shadow_style);
            }
        }
    }
    for x in (dialog_area.x + 2)..(dialog_area.x + dialog_area.width + 2).min(area.x + area.width) {
        let y = dialog_area.y + dialog_area.height;
        if y < area.y + area.height {
            buf[(x, y)].set_style(shadow_style);
        }
    }
}

/// One-line text prompt with OK / Cancel (the viewer's goto and search).
pub struct InputDialog<'a> {
    pub title: &'a str,
    pub label: &'a str,
    pub text: &'a str,
    /// Cursor position in chars.
    pub cursor: usize,
    /// 0 = input, 1 = OK, 2 = Cancel
    pub focus: u8,
}

impl<'a> InputDialog<'a> {
    pub fn new(title: &'a str, label: &'a str, text: &'a str, cursor: usize, focus: u8) -> Self {
        Self {
            title,
            label,
            text,
            cursor,
            focus,
        }
    }
}

impl<'a> Widget for InputDialog<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let outer_width = 66.min(area.width.saturating_sub(6));
        let outer_height = 8.min(area.height.saturating_sub(3));

        let outer_area = centered_rect(outer_width, outer_height, area);

        render_shadow(outer_area, area, buf);

        Clear.render(outer_area, buf);
        for y in outer_area.y..outer_area.y + outer_area.height {
            for x in outer_area.x..outer_area.x + outer_area.width {
                buf[(x, y)].set_style(skin::dialog_default());
            }
        }

        let border_area = Rect::new(
            outer_area.x + 1,
            outer_area.y + 1,
            outer_area.width.saturating_sub(2),
            outer_area.height.saturating_sub(2),
        );

        let border_style = skin::dialog_default();
        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(border_style)
            .style(border_style);

        let inner = block.inner(border_area);
        block.render(border_area, buf);

        let title_str = format!(" {} ", self.title);
        let title_len = title_str.chars().count() as u16;
        let title_x = border_area.x + (border_area.width.saturating_sub(title_len)) / 2;
        buf.set_span(
            title_x,
            border_area.y,
            &Span::styled(title_str, skin::dialog_title()),
            title_len,
        );

        let content = Rect::new(inner.x + 1, inner.y, inner.width.saturating_sub(2), inner.height);

        let content_chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(1), Constraint::Length(1), Constraint::Min(0)])
            .split(content);

        let label_widget = Paragraph::new(self.label).style(skin::dialog_default());
        label_widget.render(content_chunks[0], buf);

        let input_style = skin::dialog_focus();
        let input_area = content_chunks[1];

        for x in input_area.x..input_area.x + input_area.width {
            buf[(x, input_area.y)].set_style(input_style);
        }

        // The field scrolls to keep the cursor visible.
        let field = input_area.width.saturating_sub(1) as usize;
        let skip = self.cursor.saturating_sub(field);
        for (i, ch) in self.text.chars().skip(skip).enumerate() {
            if (i as u16) < input_area.width {
                buf[(input_area.x + i as u16, input_area.y)].set_char(ch);
            }
        }

        if self.focus == 0 {
            let cursor_x = input_area.x + ((self.cursor - skip) as u16).min(input_area.width.saturating_sub(1));
            buf[(cursor_x, input_area.y)].set_style(skin::dialog_default().add_modifier(Modifier::REVERSED));
        }

        let sep_y = border_area.y + border_area.height - 3;
        if sep_y > border_area.y && sep_y < border_area.y + border_area.height - 1 {
            buf.set_string(border_area.x, sep_y, skin::line_leftmiddle(), border_style);
            for x in (border_area.x + 1)..(border_area.x + border_area.width - 1) {
                buf.set_string(x, sep_y, skin::line_horiz(), border_style);
            }
            buf.set_string(
                border_area.x + border_area.width - 1,
                sep_y,
                skin::line_rightmiddle(),
                border_style,
            );
        }

        let btn_y = sep_y + 1;
        let btn_area = Rect::new(inner.x, btn_y, inner.width, 1);

        let ok_style = if self.focus == 1 {
            skin::dialog_focus()
        } else {
            skin::dialog_default()
        };
        let cancel_style = if self.focus == 2 {
            skin::dialog_focus()
        } else {
            skin::dialog_default()
        };

        let buttons = Line::from(vec![
            Span::styled("[ OK ]", ok_style),
            Span::raw("   "),
            Span::styled("[ Cancel ]", cancel_style),
        ]);
        let buttons_widget = Paragraph::new(buttons)
            .style(skin::dialog_default())
            .alignment(Alignment::Center);
        buttons_widget.render(btn_area, buf);
    }
}

/// Help dialog showing keyboard shortcuts
pub struct HelpDialog;

impl Widget for HelpDialog {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let help_lines = [
            "Keyboard shortcuts",
            "",
            "Type to put a shell command on the command line; Enter runs it",
            "in the active panel's directory (local panels; `cd` navigates).",
            "Esc clears the line, Alt+P / Alt+N walk the history.",
            "Ctrl+O       The shell's screen (Ctrl+O at its prompt returns)",
            "",
            "F1           Help (this dialog)",
            "F3           View file (built-in; shares stream, nothing is copied);",
            "             on a folder: compute its size into the Size column",
            "F4           Edit file (local only, built-in: F2 save, F7 search,",
            "             Alt+N/P next/prev match, Alt+L goto line, F9 menu;",
            "             Shift+arrows select, Ctrl+A all, Ctrl+U unselect,",
            "             Ctrl+Ins/Shift+Ins/Shift+Del copy/paste/cut (system",
            "             clipboard), Ctrl+Z/Ctrl+Shift+Z undo/redo, Ctrl+Y del line)",
            "F5           Copy",
            "F6           Rename (local storages)",
            "F7 / Alt+M   Make directory",
            "Alt+7        Find file by name (also Alt+F7; results list:",
            "             Enter = go to, F3 = view, Panel = list them here)",
            "F8           Delete",
            "F9           Menu: Left / Right panel view (Brief, Medium, Full,",
            "             Wide) and sort mode; Files: View, Edit, Copy, Rename",
            "F10 / Alt+Q  Quit",
            "",
            "Tab          Switch panel",
            "Enter / →    Open directory / file",
            "Bksp  / ←    Go to parent directory",
            "Alt+R        Refresh",
            "Alt+1 / 2    What the left / right panel shows (also Alt+F1 / F2):",
            "             a storage, Links, Nearby, Connection, User, Log, Info",
            "             Links: Enter opens a link, F5 onto Links = new share",
            "             Nearby: Enter = details, F5 onto a device = offer,",
            "             F8 = withdraw it, Bksp / ← back to the list",
            "             An offer from a device pops a dialog: Accept opens",
            "             the share in the other panel, F5 copies it",
            "Alt+A        Open a share URL / local path in this panel",
            "Ctrl+T       Mark / unmark entry",
            "Ctrl+S       Quick search: type a name prefix; Ctrl+S = next, Esc ends",
            "Alt+Shift+S  Skin selector",
            "Alt+Shift+L  View log file",
            "",
            "Viewer:      F2 wrap  F4 hex  F5 goto (bytes, 0x hex, %)  F7 search",
            "             n / Shift+F7 next match  ←/→ scroll  Esc/F3/F10/q close",
            "",
            "Alt+L / C    Log / Connection in this panel (again: back to the files)",
            "Alt+U / I    User (Enter: login / sign out) / Info in this panel",
            "Alt+Shift+V  Vault trust circle",
            "Alt+Shift+J  Vault join requests",
            "Alt+Shift+R  Vault recovery (recover or regenerate words)",
            "Alt+Shift+W  Show pending vault recovery words",
            "Alt+Shift+N  Nearby devices in this panel",
            "Alt+Shift+T  Tetris (also F9 → Commands) — the Dos Navigator tribute",
            "",
            "Press Esc or Enter to close",
        ];

        let max_line_len = help_lines.iter().map(|l| l.len()).max().unwrap_or(40);
        let outer_width = (max_line_len as u16 + 6).min(area.width.saturating_sub(6));
        let outer_height = (help_lines.len() as u16 + 4).min(area.height.saturating_sub(3));

        let outer_area = centered_rect(outer_width, outer_height, area);

        render_shadow(outer_area, area, buf);

        Clear.render(outer_area, buf);
        for y in outer_area.y..outer_area.y + outer_area.height {
            for x in outer_area.x..outer_area.x + outer_area.width {
                buf[(x, y)].set_style(skin::dialog_default());
            }
        }

        let border_area = Rect::new(
            outer_area.x + 1,
            outer_area.y + 1,
            outer_area.width.saturating_sub(2),
            outer_area.height.saturating_sub(2),
        );

        let border_style = skin::dialog_default();
        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(border_style)
            .style(border_style);

        let inner = block.inner(border_area);
        block.render(border_area, buf);

        let title_str = " Help ";
        let title_len = title_str.len() as u16;
        let title_x = border_area.x + (border_area.width.saturating_sub(title_len)) / 2;
        buf.set_span(
            title_x,
            border_area.y,
            &Span::styled(title_str, skin::dialog_title()),
            title_len,
        );

        let content = Rect::new(inner.x + 1, inner.y, inner.width.saturating_sub(2), inner.height);

        for (i, line) in help_lines.iter().enumerate() {
            if i as u16 >= content.height {
                break;
            }
            let y = content.y + i as u16;
            let style = if i == 0 {
                skin::dialog_title()
            } else {
                skin::dialog_default()
            };
            buf.set_string(content.x, y, *line, style);
        }
    }
}

/// File conflict resolution dialog (MC-style)
pub struct FileConflictDialog<'a> {
    /// Source object
    pub source: &'a StorageObjectInfo,
    /// Destination object
    pub dest: &'a StorageObjectInfo,
    /// Currently focused button (0=Yes, 1=No, 2=All, 3=Older, 4=None, 5=Smaller, 6=Size differs, 7=Abort)
    pub focus: usize,
    /// 1-based index of the current conflict
    pub conflict_index: usize,
    /// Total number of conflicts in this batch
    pub conflict_total: usize,
}

impl<'a> FileConflictDialog<'a> {
    pub fn new(
        source: &'a StorageObjectInfo,
        dest: &'a StorageObjectInfo,
        focus: usize,
        conflict_index: usize,
        conflict_total: usize,
    ) -> Self {
        Self {
            source,
            dest,
            focus,
            conflict_index,
            conflict_total,
        }
    }
}

impl<'a> Widget for FileConflictDialog<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let dialog_width = 76.min(area.width.saturating_sub(4));
        let dialog_height = 14.min(area.height.saturating_sub(2));

        let dialog_area = centered_rect(dialog_width, dialog_height, area);

        // Outer area: 1-char red background outside the bordered box
        let outer_area = Rect {
            x: dialog_area.x.saturating_sub(1),
            y: dialog_area.y.saturating_sub(1),
            width: (dialog_area.width + 2).min(area.x + area.width - dialog_area.x.saturating_sub(1)),
            height: (dialog_area.height + 2).min(area.y + area.height - dialog_area.y.saturating_sub(1)),
        };

        render_shadow(outer_area, area, buf);
        Clear.render(outer_area, buf);

        // Fill outer area with error (red) style
        for y in outer_area.y..outer_area.y + outer_area.height {
            for x in outer_area.x..outer_area.x + outer_area.width {
                buf[(x, y)].set_style(skin::error_default());
            }
        }

        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(skin::error_default())
            .style(skin::error_default());

        let inner = block.inner(dialog_area);
        block.render(dialog_area, buf);

        let title_str = if self.conflict_total > 1 {
            format!(" File exists ({}/{}) ", self.conflict_index, self.conflict_total)
        } else {
            " File exists ".to_string()
        };
        let title_len = title_str.len() as u16;
        let title_x = dialog_area.x + (dialog_area.width.saturating_sub(title_len)) / 2;
        buf.set_span(
            title_x,
            dialog_area.y,
            &Span::styled(&title_str, skin::error_title()),
            title_len,
        );

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(2), // File info section
                Constraint::Length(2), // Existing file info
                Constraint::Length(1), // Separator line
                Constraint::Length(1), // "Overwrite this file?" label
                Constraint::Length(1), // Yes/No buttons
                Constraint::Length(1), // Separator line
                Constraint::Length(1), // "Overwrite all files?" label
                Constraint::Length(1), // All/Older/None/Smaller/Size differs buttons
                Constraint::Length(1), // Separator line
                Constraint::Length(1), // Abort button
            ])
            .split(inner);

        // Draw horizontal separator lines
        let separator_style = skin::error_default();
        let draw_separator = |buf: &mut Buffer, y: u16| {
            // Left connector
            buf[(dialog_area.x, y)].set_symbol(&skin::line_leftmiddle());
            buf[(dialog_area.x, y)].set_style(separator_style);
            // Right connector
            buf[(dialog_area.x + dialog_width - 1, y)].set_symbol(&skin::line_rightmiddle());
            buf[(dialog_area.x + dialog_width - 1, y)].set_style(separator_style);
            // Horizontal line
            for x in (dialog_area.x + 1)..(dialog_area.x + dialog_width - 1) {
                buf[(x, y)].set_symbol(&skin::line_horiz());
                buf[(x, y)].set_style(separator_style);
            }
        };
        draw_separator(buf, chunks[2].y);
        draw_separator(buf, chunks[5].y);
        draw_separator(buf, chunks[8].y);

        let inner_width = inner.width as usize;

        // Source file info
        let source_label = "New        : ";
        let source_path_max = inner_width.saturating_sub(source_label.len() + 2);
        let source_info = format!("{}{}", source_label, truncate_path(&self.source.key, source_path_max));
        buf.set_string(inner.x + 1, chunks[0].y, &source_info, skin::error_default());
        // Second line of each file: the size in the left half, the modified
        // time in the right — the panels' formats, each clipped to its half
        // so neither runs into the other.
        let half_width = (inner_width.saturating_sub(2)) / 2;
        let size_date = |info: &StorageObjectInfo| -> (String, String) {
            let size = truncate(&super::panel::format_size(info.size), half_width);
            let date = info
                .attrs
                .mtime
                .as_ref()
                .map(|dt| truncate(&super::panel::format_modified(dt), half_width))
                .unwrap_or_default();
            (format!("{size:>half_width$}"), format!("{date:>half_width$}"))
        };
        let (source_size_str, source_date_str) = size_date(self.source);
        buf.set_string(inner.x + 1, chunks[0].y + 1, &source_size_str, skin::error_default());
        buf.set_string(
            inner.x + 1 + half_width as u16,
            chunks[0].y + 1,
            &source_date_str,
            skin::error_default(),
        );

        // Destination file info
        let dest_label = "Existing   : ";
        let dest_path_max = inner_width.saturating_sub(dest_label.len() + 2);
        let dest_info = format!("{}{}", dest_label, truncate_path(&self.dest.key, dest_path_max));
        buf.set_string(inner.x + 1, chunks[1].y, &dest_info, skin::error_default());
        let (dest_size_str, dest_date_str) = size_date(self.dest);
        buf.set_string(inner.x + 1, chunks[1].y + 1, &dest_size_str, skin::error_default());
        buf.set_string(
            inner.x + 1 + half_width as u16,
            chunks[1].y + 1,
            &dest_date_str,
            skin::error_default(),
        );

        // "Overwrite this file?" label
        let label1 = Paragraph::new("Overwrite this file?")
            .style(skin::error_default())
            .alignment(Alignment::Center);
        label1.render(chunks[3], buf);

        // Yes/No buttons
        let btn_style = |idx: usize| {
            if self.focus == idx {
                skin::error_focus().add_modifier(Modifier::BOLD)
            } else {
                skin::error_default()
            }
        };
        let hotkey_style = |idx: usize| {
            if self.focus == idx {
                skin::error_hot_focus().add_modifier(Modifier::BOLD)
            } else {
                skin::error_hot()
            }
        };

        let row1_buttons = Line::from(vec![
            Span::styled("[ ", btn_style(0)),
            Span::styled("Y", hotkey_style(0)),
            Span::styled("es ]", btn_style(0)),
            Span::raw(" "),
            Span::styled("[ ", btn_style(1)),
            Span::styled("N", hotkey_style(1)),
            Span::styled("o ]", btn_style(1)),
        ]);
        let row1_para = Paragraph::new(row1_buttons).alignment(Alignment::Center);
        row1_para.render(chunks[4], buf);

        // "Overwrite all files?" label
        let label2 = Paragraph::new("Overwrite all files?")
            .style(skin::error_default())
            .alignment(Alignment::Center);
        label2.render(chunks[6], buf);

        // All/Older/None/Smaller/Size differs buttons
        let row2_buttons = Line::from(vec![
            Span::styled("[ ", btn_style(2)),
            Span::styled("A", hotkey_style(2)),
            Span::styled("ll ]", btn_style(2)),
            Span::raw(" "),
            Span::styled("[ ", btn_style(3)),
            Span::styled("O", hotkey_style(3)),
            Span::styled("lder ]", btn_style(3)),
            Span::raw(" "),
            Span::styled("[ ", btn_style(4)),
            Span::styled("N", hotkey_style(4)),
            Span::styled("one ]", btn_style(4)),
            Span::raw(" "),
            Span::styled("[ ", btn_style(5)),
            Span::styled("S", hotkey_style(5)),
            Span::styled("maller ]", btn_style(5)),
            Span::raw(" "),
            Span::styled("[ Si", btn_style(6)),
            Span::styled("z", hotkey_style(6)),
            Span::styled("e differs ]", btn_style(6)),
        ]);
        let row2_para = Paragraph::new(row2_buttons).alignment(Alignment::Center);
        row2_para.render(chunks[7], buf);

        // Abort button
        let abort_button = Line::from(vec![
            Span::styled("[ A", btn_style(7)),
            Span::styled("b", hotkey_style(7)),
            Span::styled("ort ]", btn_style(7)),
        ]);
        let abort_para = Paragraph::new(abort_button).alignment(Alignment::Center);
        abort_para.render(chunks[9], buf);
    }
}

/// Transfer error dialog with Retry/Skip/Cancel buttons
pub struct TransferErrorDialog<'a> {
    pub message: &'a str,
    pub focus: u8,
}

impl<'a> TransferErrorDialog<'a> {
    pub fn new(message: &'a str, focus: u8) -> Self {
        Self { message, focus }
    }
}

impl<'a> Widget for TransferErrorDialog<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let dialog_width = 76.min(area.width.saturating_sub(4));
        let dialog_height = 6.min(area.height.saturating_sub(2));

        let dialog_area = centered_rect(dialog_width, dialog_height, area);

        // Outer area: 1-char red background outside the bordered box
        let outer_area = Rect {
            x: dialog_area.x.saturating_sub(1),
            y: dialog_area.y.saturating_sub(1),
            width: (dialog_area.width + 2).min(area.x + area.width - dialog_area.x.saturating_sub(1)),
            height: (dialog_area.height + 2).min(area.y + area.height - dialog_area.y.saturating_sub(1)),
        };

        render_shadow(outer_area, area, buf);
        Clear.render(outer_area, buf);

        // Fill outer area with error (red) style
        for y in outer_area.y..outer_area.y + outer_area.height {
            for x in outer_area.x..outer_area.x + outer_area.width {
                buf[(x, y)].set_style(skin::error_default());
            }
        }

        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(skin::error_default())
            .style(skin::error_default());

        let inner = block.inner(dialog_area);
        block.render(dialog_area, buf);

        // Centered title on the border
        let title_str = " Transfer error ";
        let title_len = title_str.len() as u16;
        let title_x = dialog_area.x + (dialog_area.width.saturating_sub(title_len)) / 2;
        buf.set_span(
            title_x,
            dialog_area.y,
            &Span::styled(title_str, skin::error_title()),
            title_len,
        );

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Min(1),    // Message
                Constraint::Length(1), // Separator line
                Constraint::Length(1), // Buttons
            ])
            .split(inner);

        // Separator line above buttons
        let separator_style = skin::error_default();
        buf[(dialog_area.x, chunks[1].y)].set_symbol(&skin::line_leftmiddle());
        buf[(dialog_area.x, chunks[1].y)].set_style(separator_style);
        buf[(dialog_area.x + dialog_width - 1, chunks[1].y)].set_symbol(&skin::line_rightmiddle());
        buf[(dialog_area.x + dialog_width - 1, chunks[1].y)].set_style(separator_style);
        for x in (dialog_area.x + 1)..(dialog_area.x + dialog_width - 1) {
            buf[(x, chunks[1].y)].set_symbol(&skin::line_horiz());
            buf[(x, chunks[1].y)].set_style(separator_style);
        }

        // Word-wrap the message to fit the inner width
        let max_width = inner.width.saturating_sub(2) as usize;
        let wrapped: Vec<Line> = self
            .message
            .chars()
            .collect::<Vec<_>>()
            .chunks(max_width.max(1))
            .map(|chunk| Line::from(chunk.iter().collect::<String>()))
            .collect();
        let message = Paragraph::new(wrapped)
            .style(skin::error_default())
            .alignment(Alignment::Center);
        message.render(chunks[0], buf);

        // Buttons: Retry / Skip / Cancel
        let btn_style = |idx: u8| {
            if self.focus == idx {
                skin::error_focus().add_modifier(Modifier::BOLD)
            } else {
                skin::error_default()
            }
        };
        let hotkey_style = |idx: u8| {
            if self.focus == idx {
                skin::error_hot_focus().add_modifier(Modifier::BOLD)
            } else {
                skin::error_hot()
            }
        };

        let buttons = Line::from(vec![
            Span::styled("[ ", btn_style(0)),
            Span::styled("R", hotkey_style(0)),
            Span::styled("etry ]", btn_style(0)),
            Span::raw("  "),
            Span::styled("[ ", btn_style(1)),
            Span::styled("S", hotkey_style(1)),
            Span::styled("kip ]", btn_style(1)),
            Span::raw("  "),
            Span::styled("[ ", btn_style(2)),
            Span::styled("C", hotkey_style(2)),
            Span::styled("ancel ]", btn_style(2)),
        ]);
        let buttons_para = Paragraph::new(buttons).alignment(Alignment::Center);
        buttons_para.render(chunks[2], buf);
    }
}

/// Listing error dialog with a single Back button
/// The editor's unsaved-changes guard: Save / Discard / Cancel.
pub struct EditorCloseDialog<'a> {
    pub name: &'a str,
    /// 0 = Save, 1 = Discard, 2 = Cancel.
    pub focus: u8,
}

impl<'a> Widget for EditorCloseDialog<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let dialog_width = 52.min(area.width.saturating_sub(4));
        let dialog_height = 7.min(area.height.saturating_sub(2));
        let dialog_area = centered_rect(dialog_width, dialog_height, area);

        let outer_area = Rect {
            x: dialog_area.x.saturating_sub(1),
            y: dialog_area.y.saturating_sub(1),
            width: (dialog_area.width + 2).min(area.x + area.width - dialog_area.x.saturating_sub(1)),
            height: (dialog_area.height + 2).min(area.y + area.height - dialog_area.y.saturating_sub(1)),
        };
        render_shadow(outer_area, area, buf);
        Clear.render(outer_area, buf);
        for y in outer_area.y..outer_area.y + outer_area.height {
            for x in outer_area.x..outer_area.x + outer_area.width {
                buf[(x, y)].set_style(skin::dialog_default());
            }
        }

        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(skin::dialog_default())
            .style(skin::dialog_default());
        let inner = block.inner(dialog_area);
        block.render(dialog_area, buf);

        let title_str = " Close ";
        let title_x = dialog_area.x + (dialog_area.width.saturating_sub(title_str.len() as u16)) / 2;
        buf.set_span(
            title_x,
            dialog_area.y,
            &Span::styled(title_str, skin::dialog_title()),
            title_str.len() as u16,
        );

        let msg = format!("'{}' has unsaved changes", self.name);
        let msg_widget = Paragraph::new(msg)
            .style(skin::dialog_default())
            .alignment(Alignment::Center);
        msg_widget.render(Rect::new(inner.x, inner.y + 1, inner.width, 1), buf);

        let style_for = |idx: u8| {
            if self.focus == idx {
                skin::dialog_focus()
            } else {
                skin::dialog_default()
            }
        };
        let buttons = Line::from(vec![
            Span::styled("[ Save ]", style_for(0)),
            Span::raw("  "),
            Span::styled("[ Discard ]", style_for(1)),
            Span::raw("  "),
            Span::styled("[ Cancel ]", style_for(2)),
        ]);
        let buttons_widget = Paragraph::new(buttons)
            .style(skin::dialog_default())
            .alignment(Alignment::Center);
        buttons_widget.render(Rect::new(inner.x, inner.y + 3, inner.width, 1), buf);
    }
}

pub struct ListingErrorDialog<'a> {
    pub message: &'a str,
}

impl<'a> ListingErrorDialog<'a> {
    pub fn new(message: &'a str) -> Self {
        Self { message }
    }
}

impl<'a> Widget for ListingErrorDialog<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let dialog_width = 76.min(area.width.saturating_sub(4));
        let dialog_height = 6.min(area.height.saturating_sub(2));

        let dialog_area = centered_rect(dialog_width, dialog_height, area);

        let outer_area = Rect {
            x: dialog_area.x.saturating_sub(1),
            y: dialog_area.y.saturating_sub(1),
            width: (dialog_area.width + 2).min(area.x + area.width - dialog_area.x.saturating_sub(1)),
            height: (dialog_area.height + 2).min(area.y + area.height - dialog_area.y.saturating_sub(1)),
        };

        render_shadow(outer_area, area, buf);
        Clear.render(outer_area, buf);

        for y in outer_area.y..outer_area.y + outer_area.height {
            for x in outer_area.x..outer_area.x + outer_area.width {
                buf[(x, y)].set_style(skin::error_default());
            }
        }

        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(skin::error_default())
            .style(skin::error_default());

        let inner = block.inner(dialog_area);
        block.render(dialog_area, buf);

        let title_str = " Error ";
        let title_len = title_str.len() as u16;
        let title_x = dialog_area.x + (dialog_area.width.saturating_sub(title_len)) / 2;
        buf.set_span(
            title_x,
            dialog_area.y,
            &Span::styled(title_str, skin::error_title()),
            title_len,
        );

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Min(1),    // Message
                Constraint::Length(1), // Separator line
                Constraint::Length(1), // Button
            ])
            .split(inner);

        // Separator line above button
        let separator_style = skin::error_default();
        buf[(dialog_area.x, chunks[1].y)].set_symbol(&skin::line_leftmiddle());
        buf[(dialog_area.x, chunks[1].y)].set_style(separator_style);
        buf[(dialog_area.x + dialog_width - 1, chunks[1].y)].set_symbol(&skin::line_rightmiddle());
        buf[(dialog_area.x + dialog_width - 1, chunks[1].y)].set_style(separator_style);
        for x in (dialog_area.x + 1)..(dialog_area.x + dialog_width - 1) {
            buf[(x, chunks[1].y)].set_symbol(&skin::line_horiz());
            buf[(x, chunks[1].y)].set_style(separator_style);
        }

        // Word-wrap the message
        let max_width = inner.width.saturating_sub(2) as usize;
        let wrapped: Vec<Line> = self
            .message
            .chars()
            .collect::<Vec<_>>()
            .chunks(max_width.max(1))
            .map(|chunk| Line::from(chunk.iter().collect::<String>()))
            .collect();
        let message = Paragraph::new(wrapped)
            .style(skin::error_default())
            .alignment(Alignment::Center);
        message.render(chunks[0], buf);

        // Single "Back" button, always focused
        let buttons = Line::from(vec![
            Span::styled("[ ", skin::error_focus().add_modifier(Modifier::BOLD)),
            Span::styled("B", skin::error_hot_focus().add_modifier(Modifier::BOLD)),
            Span::styled("ack ]", skin::error_focus().add_modifier(Modifier::BOLD)),
        ]);
        let buttons_para = Paragraph::new(buttons).alignment(Alignment::Center);
        buttons_para.render(chunks[2], buf);
    }
}

fn truncate_path(path: &str, max_len: usize) -> String {
    if path.len() <= max_len {
        path.to_string()
    } else if max_len > 5 {
        let half = (max_len - 3) / 2;
        let tail = max_len - 3 - half;
        format!("{}...{}", &path[..half], &path[path.len() - tail..])
    } else if max_len > 3 {
        format!("...{}", &path[path.len() - (max_len - 3)..])
    } else {
        "...".to_string()
    }
}

/// Truncate a string to max_width chars, adding "..." if it doesn't fit.
fn truncate_ellipsis(s: &str, max_width: usize) -> String {
    if s.chars().count() <= max_width {
        s.to_string()
    } else {
        let truncated: String = s.chars().take(max_width.saturating_sub(3)).collect();
        format!("{}...", truncated)
    }
}

/// Helper function to create a centered rect
/// A timestamp as "YYYY-MM-DD HH:MM:SS" (the dialogs' date format).
fn format_time(dt: &chrono::DateTime<chrono::FixedOffset>) -> String {
    dt.format("%Y-%m-%d %H:%M:%S").to_string()
}

pub fn centered_rect(width: u16, height: u16, area: Rect) -> Rect {
    let x = area.x + (area.width.saturating_sub(width)) / 2;
    let y = area.y + (area.height.saturating_sub(height)) / 2;
    Rect::new(x, y, width, height)
}

/// Alt+F7: what to find and where.
pub struct FindDialog<'a> {
    pub name: &'a str,
    pub cursor: usize,
    pub whole: bool,
    pub recursive: bool,
    /// 0 = name, 1 = where, 2 = subfolders, 3 = Search, 4 = Cancel.
    pub focus: u8,
}

impl<'a> Widget for FindDialog<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let outer_width = 66.min(area.width.saturating_sub(6));
        let outer_height = 12.min(area.height.saturating_sub(3));
        let outer_area = centered_rect(outer_width, outer_height, area);

        render_shadow(outer_area, area, buf);
        Clear.render(outer_area, buf);
        for y in outer_area.y..outer_area.y + outer_area.height {
            for x in outer_area.x..outer_area.x + outer_area.width {
                buf[(x, y)].set_style(skin::dialog_default());
            }
        }
        let border_area = Rect::new(
            outer_area.x + 1,
            outer_area.y + 1,
            outer_area.width.saturating_sub(2),
            outer_area.height.saturating_sub(2),
        );
        let border_style = skin::dialog_default();
        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(border_style)
            .style(border_style);
        let inner = block.inner(border_area);
        block.render(border_area, buf);

        let title_str = " Find file ";
        let title_len = title_str.len() as u16;
        let title_x = border_area.x + (border_area.width.saturating_sub(title_len)) / 2;
        buf.set_span(
            title_x,
            border_area.y,
            &Span::styled(title_str, skin::dialog_title()),
            title_len,
        );

        let content = Rect::new(inner.x + 1, inner.y, inner.width.saturating_sub(2), inner.height);
        let focused = |f: u8| {
            if self.focus == f {
                skin::dialog_focus()
            } else {
                skin::dialog_default()
            }
        };

        // Name contains: an input line.
        buf.set_string(content.x, content.y, "Name contains:", skin::dialog_default());
        let input_area = Rect::new(content.x, content.y + 1, content.width, 1);
        for x in input_area.x..input_area.x + input_area.width {
            buf[(x, input_area.y)].set_style(skin::dialog_focus());
        }
        let field = input_area.width.saturating_sub(1) as usize;
        let skip = self.cursor.saturating_sub(field);
        for (i, ch) in self.name.chars().skip(skip).enumerate() {
            if (i as u16) < input_area.width {
                buf[(input_area.x + i as u16, input_area.y)].set_char(ch);
            }
        }
        if self.focus == 0 {
            let cursor_x = input_area.x + ((self.cursor - skip) as u16).min(input_area.width.saturating_sub(1));
            buf[(cursor_x, input_area.y)].set_style(skin::dialog_default().add_modifier(Modifier::REVERSED));
        }

        // Where: a radio pair.
        let (cur, whole) = if self.whole { ("( )", "(•)") } else { ("(•)", "( )") };
        let where_line = Line::from(vec![
            Span::styled("Search in: ", skin::dialog_default()),
            Span::styled(format!("{cur} Current folder   {whole} Whole storage"), focused(1)),
        ]);
        buf.set_line(content.x, content.y + 3, &where_line, content.width);

        // Subfolders: a checkbox.
        let tick = if self.recursive { "[x]" } else { "[ ]" };
        buf.set_string(
            content.x,
            content.y + 5,
            format!("{tick} Search subfolders"),
            focused(2),
        );

        // Separator and buttons.
        let sep_y = border_area.y + border_area.height - 3;
        if sep_y > border_area.y && sep_y < border_area.y + border_area.height - 1 {
            buf.set_string(border_area.x, sep_y, skin::line_leftmiddle(), border_style);
            for x in (border_area.x + 1)..(border_area.x + border_area.width - 1) {
                buf.set_string(x, sep_y, skin::line_horiz(), border_style);
            }
            buf.set_string(
                border_area.x + border_area.width - 1,
                sep_y,
                skin::line_rightmiddle(),
                border_style,
            );
        }
        let buttons = Line::from(vec![
            Span::styled("[ Search ]", focused(3)),
            Span::raw("   "),
            Span::styled("[ Cancel ]", focused(4)),
        ]);
        Paragraph::new(buttons)
            .style(skin::dialog_default())
            .alignment(Alignment::Center)
            .render(Rect::new(inner.x, sep_y + 1, inner.width, 1), buf);
    }
}

/// The find results list: where it sits on the screen, shared by the draw
/// and the key handling (the viewport height paces the cursor).
pub fn find_results_area(area: Rect) -> Rect {
    Rect::new(
        area.x + 2,
        area.y + 1,
        area.width.saturating_sub(4),
        area.height.saturating_sub(2),
    )
}

/// The rows the results list shows inside `find_results_area`: the dialog's
/// border and the status + button block below the list.
pub fn find_list_height(area: Rect) -> usize {
    let outer = find_results_area(area);
    // outer margin + border (2 each side) + separator, status, separator, buttons
    (outer.height as usize).saturating_sub(4 + 4)
}

/// The find results, FAR style: items grouped under their directory, the
/// status line, and the buttons.
pub struct FindResultsDialog<'a> {
    pub find: &'a FindState,
    /// The panel storage's display root, for the directory headers.
    pub root: &'a str,
}

impl<'a> Widget for FindResultsDialog<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let outer_area = find_results_area(area);
        render_shadow(outer_area, area, buf);
        Clear.render(outer_area, buf);
        for y in outer_area.y..outer_area.y + outer_area.height {
            for x in outer_area.x..outer_area.x + outer_area.width {
                buf[(x, y)].set_style(skin::dialog_default());
            }
        }
        let border_area = Rect::new(
            outer_area.x + 1,
            outer_area.y + 1,
            outer_area.width.saturating_sub(2),
            outer_area.height.saturating_sub(2),
        );
        let border_style = skin::dialog_default();
        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(border_style)
            .style(border_style);
        let inner = block.inner(border_area);
        block.render(border_area, buf);

        let find = self.find;
        let title_str = format!(
            " Find file: {} ",
            if find.needle.is_empty() { "*" } else { &find.needle }
        );
        let title_len = title_str.chars().count() as u16;
        let title_x = border_area.x + (border_area.width.saturating_sub(title_len)) / 2;
        buf.set_span(
            title_x,
            border_area.y,
            &Span::styled(title_str, skin::dialog_title()),
            title_len,
        );

        // The list: `visible` rows from `scroll`.
        let list_h = find_list_height(area);
        let rows = find.rows();
        let header_style = skin::dialog_default().add_modifier(Modifier::BOLD);
        for (n, row) in rows.iter().skip(find.scroll).take(list_h).enumerate() {
            let y = inner.y + n as u16;
            let line = match *row {
                FindRow::Dir(i) => {
                    let (dir, _) = find.split(i);
                    let shown = if find.local {
                        crate::local_path::display(&crate::local_path::join(self.root, dir))
                    } else {
                        format!("/{dir}")
                    };
                    Line::from(Span::styled(shown, header_style))
                }
                FindRow::Item(i) => {
                    let item = &find.items[i];
                    let (_, name) = find.split(i);
                    let selected = i == find.selected && find.focus == 0;
                    let style = if selected {
                        skin::dialog_focus()
                    } else {
                        skin::dialog_default()
                    };
                    let date = item
                        .attrs
                        .mtime
                        .as_ref()
                        .map(super::panel::format_modified)
                        .unwrap_or_default();
                    let size = if item.file_type == StorageFileType::Dir {
                        " Folder".to_string()
                    } else {
                        super::panel::format_size(item.size)
                    };
                    let name = if item.file_type == StorageFileType::Dir {
                        format!("/{name}")
                    } else {
                        name.to_string()
                    };
                    Line::from(vec![
                        Span::styled(format!("  {:<19}", date), style),
                        Span::styled("│", border_style),
                        Span::styled(format!("{:>7}", size), style),
                        Span::styled("│", border_style),
                        Span::styled(name, style),
                    ])
                }
            };
            buf.set_line(inner.x, y, &line, inner.width);
        }

        // Separator, status, separator, buttons.
        let sep1 = inner.y + list_h as u16;
        let sep2 = sep1 + 2;
        for y in [sep1, sep2] {
            if y > border_area.y && y < border_area.y + border_area.height - 1 {
                buf.set_string(border_area.x, y, skin::line_leftmiddle(), border_style);
                for x in (border_area.x + 1)..(border_area.x + border_area.width - 1) {
                    buf.set_string(x, y, skin::line_horiz(), border_style);
                }
                buf.set_string(
                    border_area.x + border_area.width - 1,
                    y,
                    skin::line_rightmiddle(),
                    border_style,
                );
            }
        }
        let status = if let Some(e) = &find.error {
            format!("Search failed: {e}")
        } else if find.running {
            format!("Searching… found files: {}, folders: {}", find.files, find.folders)
        } else {
            format!("Search done. Found files: {}, folders: {}", find.files, find.folders)
        };
        let status_style = if find.error.is_some() {
            skin::error_default()
        } else {
            skin::dialog_default()
        };
        buf.set_line(
            inner.x + 1,
            sep1 + 1,
            &Line::from(Span::styled(status, status_style)),
            inner.width.saturating_sub(2),
        );

        let focused = |f: u8| {
            if find.focus == f {
                skin::dialog_focus()
            } else {
                skin::dialog_default()
            }
        };
        let buttons = Line::from(vec![
            Span::styled("[ New search ]", focused(1)),
            Span::raw(" "),
            Span::styled("[ Go to ]", focused(2)),
            Span::raw(" "),
            Span::styled("[ View ]", focused(3)),
            Span::raw(" "),
            Span::styled("[ Panel ]", focused(4)),
            Span::raw(" "),
            Span::styled("[ Cancel ]", focused(5)),
        ]);
        Paragraph::new(buttons)
            .style(skin::dialog_default())
            .alignment(Alignment::Center)
            .render(Rect::new(inner.x, sep2 + 1, inner.width, 1), buf);
    }
}
