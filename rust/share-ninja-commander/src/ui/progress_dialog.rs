//! Progress dialog widget for file operations

use super::dialog::{centered_rect, render_shadow};
use super::skin;
use ratatui::{
    buffer::Buffer,
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Clear, Gauge, Paragraph, Widget},
};

pub struct ProgressDialog<'a> {
    pub title: &'a str,
    pub source_path: &'a str,
    pub dest_path: &'a str,
    pub current_file: Option<&'a str>,
    pub bytes_copied: u64,
    pub total_bytes: u64,
    pub files_copied: u64,
    pub total_files: u64,
    pub elapsed_secs: u64,
}

impl<'a> ProgressDialog<'a> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        title: &'a str,
        source_path: &'a str,
        dest_path: &'a str,
        current_file: Option<&'a str>,
        bytes_copied: u64,
        total_bytes: u64,
        files_copied: u64,
        total_files: u64,
        elapsed_secs: u64,
    ) -> Self {
        Self {
            title,
            source_path,
            dest_path,
            current_file,
            bytes_copied,
            total_bytes,
            files_copied,
            total_files,
            elapsed_secs,
        }
    }
}

impl<'a> Widget for ProgressDialog<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let outer_width = ((area.width as f32 * 0.6) as u16).max(50);
        let outer_height = 14.min(area.height.saturating_sub(3));

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
        let title_len = title_str.len() as u16;
        let title_x = border_area.x + (border_area.width.saturating_sub(title_len)) / 2;
        buf.set_span(
            title_x,
            border_area.y,
            &Span::styled(&title_str, skin::dialog_title()),
            title_len,
        );

        let content = Rect::new(inner.x + 1, inner.y, inner.width.saturating_sub(2), inner.height);

        let content_chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(1), // 0: Source label
                Constraint::Length(1), // 1: Source path
                Constraint::Length(1), // 2: Dest label
                Constraint::Length(1), // 3: Dest path
                Constraint::Length(1), // 4: Spacer
                Constraint::Length(1), // 5: Total bytes gauge
                Constraint::Length(1), // 6: Files processed text
                Constraint::Length(1), // 7: Time/ETA/Speed
                Constraint::Min(0),
            ])
            .split(content);

        let source_label = Paragraph::new("Source:").style(skin::dialog_default());
        source_label.render(content_chunks[0], buf);

        // Show current file if available, otherwise show base source path.
        let src_display_path = self.current_file.unwrap_or(self.source_path);
        let src_display = truncate_path(src_display_path, content_chunks[1].width as usize);
        let source_path_widget = Paragraph::new(src_display).style(skin::dialog_hot());
        source_path_widget.render(content_chunks[1], buf);

        let dest_label = Paragraph::new("Destination:").style(skin::dialog_default());
        dest_label.render(content_chunks[2], buf);

        let dest_display = truncate_path(self.dest_path, content_chunks[3].width as usize);
        let dest_path_widget = Paragraph::new(dest_display).style(skin::dialog_hot());
        dest_path_widget.render(content_chunks[3], buf);

        // Total bytes progress gauge
        let bytes_percent = if self.total_bytes > 0 {
            ((self.bytes_copied as f64 / self.total_bytes as f64) * 100.0) as u16
        } else {
            0
        };
        let bytes_str = format_bytes(self.bytes_copied);
        let total_bytes_str = format_bytes(self.total_bytes);
        let bytes_label = format!("{} / {} ({}%)", bytes_str, total_bytes_str, bytes_percent);
        let bytes_gauge = Gauge::default()
            .gauge_style(Style::default().fg(Color::Cyan).bg(Color::Black))
            .percent(bytes_percent.min(100))
            .label(bytes_label);
        bytes_gauge.render(content_chunks[5], buf);

        // Files processed text
        let files_text = format!("Files processed: {} / {}", self.files_copied, self.total_files);
        let files_widget = Paragraph::new(files_text)
            .style(skin::dialog_default())
            .alignment(Alignment::Center);
        files_widget.render(content_chunks[6], buf);

        // Time/ETA/Speed line
        let (time_str, eta_str, speed_str) =
            calculate_time_eta_speed(self.bytes_copied, self.total_bytes, self.elapsed_secs);
        let time_line = format!("Time: {} ETA: {} ({})", time_str, eta_str, speed_str);
        let time_widget = Paragraph::new(time_line)
            .style(skin::dialog_default())
            .alignment(Alignment::Center);
        time_widget.render(content_chunks[7], buf);

        let sep_y = border_area.y + border_area.height - 3;
        if sep_y > border_area.y && sep_y < border_area.y + border_area.height - 1 {
            buf.set_string(border_area.x, sep_y, "├", border_style);
            for x in (border_area.x + 1)..(border_area.x + border_area.width - 1) {
                buf.set_string(x, sep_y, "─", border_style);
            }
            buf.set_string(border_area.x + border_area.width - 1, sep_y, "┤", border_style);
        }

        let btn_y = sep_y + 1;
        let btn_area = Rect::new(inner.x, btn_y, inner.width, 1);

        let buttons = Line::from(vec![Span::styled("[ Cancel ]", skin::dialog_focus())]);
        let buttons_widget = Paragraph::new(buttons)
            .style(skin::dialog_default())
            .alignment(Alignment::Center);
        buttons_widget.render(btn_area, buf);
    }
}

fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

fn truncate_path(path: &str, max_width: usize) -> String {
    if path.len() <= max_width {
        path.to_string()
    } else if max_width > 5 {
        let half = (max_width - 3) / 2;
        let tail = max_width - 3 - half;
        format!("{}...{}", &path[..half], &path[path.len() - tail..])
    } else {
        path.chars().take(max_width).collect()
    }
}

fn format_duration(secs: u64) -> String {
    let hours = secs / 3600;
    let minutes = (secs % 3600) / 60;
    let seconds = secs % 60;
    format!("{:02}:{:02}:{:02}", hours, minutes, seconds)
}

fn calculate_time_eta_speed(bytes_copied: u64, total_bytes: u64, elapsed_secs: u64) -> (String, String, String) {
    let time_str = format_duration(elapsed_secs);

    if elapsed_secs == 0 || bytes_copied == 0 {
        return (time_str, "--:--:--".to_string(), "-- B/s".to_string());
    }

    let speed = bytes_copied as f64 / elapsed_secs as f64;
    let speed_str = format!("{}/s", format_bytes(speed as u64));

    let remaining_bytes = total_bytes.saturating_sub(bytes_copied);
    if remaining_bytes == 0 {
        return (time_str, "00:00:00".to_string(), speed_str);
    }

    let eta_secs = (remaining_bytes as f64 / speed) as u64;
    let eta_str = format_duration(eta_secs);

    (time_str, eta_str, speed_str)
}
