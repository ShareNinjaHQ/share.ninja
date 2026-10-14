//! The F9 menu bar and its open pull-down.

use ratatui::{
    buffer::Buffer,
    layout::Rect,
    text::{Line, Span},
    widgets::{Clear, Widget},
};

use super::skin;
use crate::menu::MenuItem;

pub struct MenuBar<'a> {
    pub titles: &'a [&'static str],
    pub open: usize,
    pub items: &'a [MenuItem],
    pub selected: usize,
}

const TITLE_GAP: u16 = 3;

impl<'a> Widget for MenuBar<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        if area.height == 0 {
            return;
        }
        // The bar: the titles across the top row, the open one highlighted.
        // Cells are reset first: `set_style` patches, so a bold name under
        // the menu would otherwise keep its bold.
        let bar = Rect::new(area.x, area.y, area.width, 1);
        Clear.render(bar, buf);
        for x in bar.x..bar.x + bar.width {
            buf[(x, bar.y)].set_style(skin::menu_default());
        }
        let mut x = bar.x + 1;
        let mut open_x = x;
        for (i, title) in self.titles.iter().enumerate() {
            let label = format!(" {} ", title);
            let style = if i == self.open {
                skin::menu_selected()
            } else {
                skin::menu_default()
            };
            if i == self.open {
                open_x = x;
            }
            buf.set_string(x, bar.y, &label, style);
            x += label.chars().count() as u16 + TITLE_GAP;
        }

        // The pull-down under the open title.
        let label_w = self.items.iter().map(|i| i.label.chars().count()).max().unwrap_or(0);
        let hot_w = self.items.iter().map(|i| i.hotkey.chars().count()).max().unwrap_or(0);
        let inner_w = (2 + label_w + 2 + hot_w + 1) as u16;
        let width = (inner_w + 2).min(area.width.saturating_sub(open_x - area.x));
        let height = (self.items.len() as u16 + 2).min(area.height.saturating_sub(1));
        let x0 = open_x.min(area.x + area.width - width);
        let y0 = bar.y + 1;
        let style = skin::menu_default();
        let box_area = Rect::new(x0, y0, width, height);
        Clear.render(box_area, buf);
        for y in box_area.y..box_area.y + box_area.height {
            for x in box_area.x..box_area.x + box_area.width {
                buf[(x, y)].set_style(style);
            }
        }
        // Frame.
        buf.set_string(x0, y0, "┌", style);
        buf.set_string(x0 + width - 1, y0, "┐", style);
        buf.set_string(x0, y0 + height - 1, "└", style);
        buf.set_string(x0 + width - 1, y0 + height - 1, "┘", style);
        for x in x0 + 1..x0 + width - 1 {
            buf.set_string(x, y0, "─", style);
            buf.set_string(x, y0 + height - 1, "─", style);
        }
        for (i, item) in self.items.iter().enumerate() {
            let y = y0 + 1 + i as u16;
            if y >= y0 + height - 1 {
                break;
            }
            if item.action.is_none() {
                buf.set_string(x0, y, "├", style);
                for x in x0 + 1..x0 + width - 1 {
                    buf.set_string(x, y, "─", style);
                }
                buf.set_string(x0 + width - 1, y, "┤", style);
                continue;
            }
            let row_style = if i == self.selected {
                skin::menu_selected()
            } else {
                style
            };
            let check = if item.checked { "✓" } else { " " };
            let text = format!(
                " {} {:<lw$}  {:>hw$} ",
                check,
                item.label,
                item.hotkey,
                lw = label_w,
                hw = hot_w
            );
            buf.set_string(x0, y, "│", style);
            buf.set_line(x0 + 1, y, &Line::from(Span::styled(text, row_style)), width - 2);
            buf.set_string(x0 + width - 1, y, "│", style);
        }
    }
}
