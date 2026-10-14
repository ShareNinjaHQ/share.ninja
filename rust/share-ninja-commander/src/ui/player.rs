//! The music player dialog: track line, FFT spectrum bars, progress, volume.
//! Pure rendering over [`crate::player::PlayerState`]; the spectrum bands are
//! computed by `PlayerState::tick` on the draw tick, not here.

use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::Span;
use ratatui::widgets::{Block, Borders, Clear, Widget};

use crate::player::{BANDS, PlayerState};
use crate::ui::dialog::{centered_rect, render_shadow};
use crate::ui::skin;

/// Bar body characters, one eighth per step.
const EIGHTHS: [char; 8] = ['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];
/// Spectrum rows inside the dialog.
const SPECTRUM_ROWS: u16 = 6;
/// Each band renders this many columns wide.
const BAND_WIDTH: u16 = 2;

pub struct PlayerDialog<'a> {
    state: &'a PlayerState,
}

impl<'a> PlayerDialog<'a> {
    pub fn new(state: &'a PlayerState) -> Self {
        Self { state }
    }
}

impl<'a> Widget for PlayerDialog<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let dialog_width = 64.min(area.width.saturating_sub(4));
        let dialog_height = 15.min(area.height.saturating_sub(2));
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

        let title_str = " Player ";
        let title_x = dialog_area.x + (dialog_area.width.saturating_sub(title_str.len() as u16)) / 2;
        buf.set_span(
            title_x,
            dialog_area.y,
            &Span::styled(title_str, skin::dialog_title()),
            title_str.len() as u16,
        );

        let shared = &self.state.shared;
        let centered = |buf: &mut Buffer, y: u16, text: &str, style: Style| {
            let w = (text.chars().count() as u16).min(inner.width);
            let x = inner.x + (inner.width.saturating_sub(w)) / 2;
            buf.set_span(x, y, &Span::styled(text, style), w);
        };

        // Track line: tags when probed, the filename until then / otherwise.
        let track = shared.tag_line().unwrap_or_else(|| self.state.title.clone());
        centered(
            buf,
            inner.y + 1,
            &track,
            skin::dialog_default().add_modifier(Modifier::BOLD),
        );
        if let Some(fmt) = shared.format_line() {
            centered(buf, inner.y + 2, &fmt, skin::dialog_default());
        }

        let spectrum_top = inner.y + 4;
        if let Some(err) = shared.error() {
            centered(buf, spectrum_top + SPECTRUM_ROWS / 2, &err, skin::error_default());
        } else if shared.format_line().is_none() {
            centered(
                buf,
                spectrum_top + SPECTRUM_ROWS / 2,
                "Loading…",
                skin::dialog_default(),
            );
        } else {
            self.render_spectrum(buf, inner, spectrum_top);
        }

        // Progress: "01:23 ━━━━━╸────── 04:05" (dashes only while the
        // duration is unknown — raw ADTS, VBR without a table).
        let pos_ms = shared.position_ms();
        let dur_ms = shared.duration_ms();
        let pos = format_time(pos_ms);
        let dur = if dur_ms > 0 {
            format_time(dur_ms)
        } else {
            "--:--".to_string()
        };
        let bar_w = inner.width.saturating_sub((pos.len() + dur.len() + 4) as u16) as usize;
        let filled = if dur_ms > 0 {
            (pos_ms as u128 * bar_w as u128 / dur_ms.max(1) as u128) as usize
        } else {
            0
        }
        .min(bar_w);
        let mut bar = String::with_capacity(bar_w * 3);
        for i in 0..bar_w {
            bar.push(if i < filled { '━' } else { '─' });
        }
        let progress = format!("{pos} {bar} {dur}");
        centered(buf, inner.y + 5 + SPECTRUM_ROWS, &progress, skin::dialog_default());

        // Volume + state.
        let volume = shared.volume_permille.load(std::sync::atomic::Ordering::Relaxed) / 10;
        let state = if shared.error().is_some() {
            "Error"
        } else if shared.is_done() {
            "Finished"
        } else if shared.paused.load(std::sync::atomic::Ordering::Relaxed) {
            "Paused"
        } else {
            "Playing"
        };
        let vol_cells = (volume / 10) as usize;
        let mut vol_bar = String::new();
        for i in 0..10 {
            vol_bar.push(if i < vol_cells { '▮' } else { '▯' });
        }
        centered(
            buf,
            inner.y + 6 + SPECTRUM_ROWS,
            &format!("{state}   Vol {vol_bar} {volume}%"),
            skin::dialog_default(),
        );

        centered(
            buf,
            inner.y + 8 + SPECTRUM_ROWS,
            "Space Pause · ←/→ ±10s · ↑/↓ Volume · Esc Close",
            skin::dialog_hot(),
        );
    }
}

impl<'a> PlayerDialog<'a> {
    /// Bottom-anchored eighth-block bars with a floating peak-hold cap.
    fn render_spectrum(&self, buf: &mut Buffer, inner: Rect, top: u16) {
        let total_w = BANDS as u16 * BAND_WIDTH;
        if inner.width < total_w || inner.height < SPECTRUM_ROWS {
            return;
        }
        let x0 = inner.x + (inner.width - total_w) / 2;
        let style = skin::dialog_focus();
        let peak_style = skin::dialog_hot();
        for (b, (&level, &peak)) in self.state.bands.iter().zip(self.state.peaks.iter()).enumerate() {
            let h = level * SPECTRUM_ROWS as f32;
            let peak_row = (peak * SPECTRUM_ROWS as f32).ceil() as u16;
            for row in 0..SPECTRUM_ROWS {
                // Rows are drawn top-down; `from_bottom` is this cell's index
                // counting up from the bar's base.
                let from_bottom = (SPECTRUM_ROWS - 1 - row) as f32;
                let fill = (h - from_bottom).clamp(0.0, 1.0);
                let (ch, st) = if fill >= 1.0 {
                    ('█', style)
                } else if fill > 0.0 {
                    (EIGHTHS[(fill * 8.0) as usize % 8], style)
                } else if peak_row > 0 && SPECTRUM_ROWS - 1 - row == peak_row - 1 && peak > level {
                    ('▁', peak_style)
                } else {
                    (' ', style)
                };
                for col in 0..BAND_WIDTH {
                    let x = x0 + b as u16 * BAND_WIDTH + col;
                    buf[(x, top + row)].set_char(ch).set_style(st);
                }
            }
        }
    }
}

fn format_time(ms: u64) -> String {
    let secs = ms / 1000;
    format!("{:02}:{:02}", secs / 60, secs % 60)
}
