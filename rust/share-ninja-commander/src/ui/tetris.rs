//! The Tetris window over the panels: the board, the next piece, the
//! numbers, the keys.

use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Clear, Paragraph, Widget},
};

use super::dialog::centered_rect;
use crate::tetris::{HEIGHT, Kind, Phase, Tetris, WIDTH};

pub struct TetrisWidget<'a> {
    pub game: &'a Tetris,
}

fn colour(kind: Kind) -> Color {
    match kind {
        Kind::I => Color::Cyan,
        Kind::O => Color::Yellow,
        Kind::T => Color::Magenta,
        Kind::S => Color::Green,
        Kind::Z => Color::Red,
        Kind::J => Color::Blue,
        Kind::L => Color::LightRed,
    }
}

impl<'a> Widget for TetrisWidget<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        // The window: the board (two columns a cell, framed) and the info
        // column to its right, inside a margin.
        let board_w = (WIDTH * 2) as u16 + 2;
        let board_h = HEIGHT as u16 + 2;
        let info_w: u16 = 22;
        let total_w = board_w + 2 + info_w;
        // One more row at the bottom for the dedication.
        let win = centered_rect(total_w + 4, board_h + 3, area);
        if win.width < total_w + 4 || win.height < board_h + 3 {
            let msg = Paragraph::new("Terminal too small for Tetris")
                .style(Style::default().bg(Color::Black).fg(Color::Gray));
            msg.render(area, buf);
            return;
        }
        Clear.render(win, buf);
        let bg = Style::default().bg(Color::Black).fg(Color::Gray);
        for y in win.y..win.y + win.height {
            for x in win.x..win.x + win.width {
                buf[(x, y)].set_style(bg);
            }
        }
        let x0 = win.x + 2;
        let y0 = win.y + 1;
        let board_area = Rect::new(x0, y0, board_w, board_h);
        let frame = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Gray).bg(Color::Black))
            .title(Span::styled(
                " TETRIS ",
                Style::default()
                    .fg(Color::White)
                    .bg(Color::Black)
                    .add_modifier(Modifier::BOLD),
            ));
        let inner = frame.inner(board_area);
        frame.render(board_area, buf);

        let g = self.game;
        let mut cells = g.board;
        if g.phase != Phase::GameOver {
            for (x, y) in g.current.board_cells() {
                if (0..WIDTH as i32).contains(&x) && (0..HEIGHT as i32).contains(&y) {
                    cells[y as usize][x as usize] = Some(g.current.kind);
                }
            }
        }
        for (row, line) in cells.iter().enumerate() {
            for (col, cell) in line.iter().enumerate() {
                let x = inner.x + (col * 2) as u16;
                let y = inner.y + row as u16;
                match cell {
                    Some(kind) => buf.set_string(x, y, "██", Style::default().fg(colour(*kind)).bg(Color::Black)),
                    None => buf.set_string(x, y, " ·", Style::default().fg(Color::DarkGray).bg(Color::Black)),
                }
            }
        }
        if g.phase != Phase::Running {
            let text = if g.phase == Phase::Paused {
                " PAUSED "
            } else {
                " GAME OVER "
            };
            let x = inner.x + (inner.width.saturating_sub(text.len() as u16)) / 2;
            let y = inner.y + inner.height / 2;
            buf.set_string(
                x,
                y,
                text,
                Style::default()
                    .fg(Color::Black)
                    .bg(Color::White)
                    .add_modifier(Modifier::BOLD),
            );
        }

        // Info: next piece, score, lines, level, keys.
        let ix = x0 + board_w + 2;
        let label = Style::default().fg(Color::Gray).bg(Color::Black);
        let value = Style::default()
            .fg(Color::White)
            .bg(Color::Black)
            .add_modifier(Modifier::BOLD);
        let mut y = y0;
        buf.set_string(ix, y, "Next", label);
        y += 1;
        let next = g.next;
        let mut preview = [[false; 4]; 2];
        for (cx, cy) in preview_cells(next) {
            preview[cy][cx] = true;
        }
        for (r, row) in preview.iter().enumerate() {
            for (c, on) in row.iter().enumerate() {
                let glyph = if *on { "██" } else { "  " };
                buf.set_string(
                    ix + (c * 2) as u16,
                    y + r as u16,
                    glyph,
                    Style::default().fg(colour(next)).bg(Color::Black),
                );
            }
        }
        y += 3;
        let rows = [
            ("Score", g.score.to_string()),
            ("Lines", g.lines.to_string()),
            ("Level", g.level().to_string()),
        ];
        for (name, val) in rows {
            buf.set_line(
                ix,
                y,
                &Line::from(vec![
                    Span::styled(format!("{:<7}", name), label),
                    Span::styled(val, value),
                ]),
                info_w,
            );
            y += 1;
        }
        y += 1;
        let keys = [
            "←/→  move",
            "↑    rotate",
            "↓    soft drop",
            "Space hard drop",
            "P    pause",
            "Enter new game",
            "Esc  quit",
        ];
        for k in keys {
            if y >= win.y + win.height - 2 {
                break;
            }
            buf.set_string(ix, y, k, label);
            y += 1;
        }

        let tribute = "A tribute to beloved DOS Navigator";
        let tx = win.x + (win.width.saturating_sub(tribute.len() as u16)) / 2;
        buf.set_string(
            tx,
            win.y + win.height - 1,
            tribute,
            Style::default().fg(Color::DarkGray).bg(Color::Black),
        );
    }
}

/// The next-piece preview cells in a 4×2 box.
fn preview_cells(kind: Kind) -> [(usize, usize); 4] {
    match kind {
        Kind::I => [(0, 0), (1, 0), (2, 0), (3, 0)],
        Kind::O => [(1, 0), (2, 0), (1, 1), (2, 1)],
        Kind::T => [(0, 0), (1, 0), (2, 0), (1, 1)],
        Kind::S => [(1, 0), (2, 0), (0, 1), (1, 1)],
        Kind::Z => [(0, 0), (1, 0), (1, 1), (2, 1)],
        Kind::J => [(0, 0), (1, 0), (2, 0), (2, 1)],
        Kind::L => [(0, 0), (1, 0), (2, 0), (0, 1)],
    }
}
