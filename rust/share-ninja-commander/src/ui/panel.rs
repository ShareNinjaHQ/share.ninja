//! The panel frame and its list body — files, links, devices share one row
//! shape (`name │ size │ date`, the links adding `│ expires in │ source`,
//! the nearby devices `name │ owner │ device`; a symlink's name is marked
//! `~`) — plus a key/value details body.

use std::time::Instant;

use chrono::{DateTime, FixedOffset, Utc};
use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::{Color, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, ListState, Paragraph, StatefulWidget, Widget},
};

use share_ninja_core::StorageFileType;

use share_ninja_core::StorageObjectInfo;
use share_ninja_core::utils::device::device_card_label;

use super::skin;
use crate::app::{App, Cursor, FileList, LinkList, Panel, ViewMode};
use crate::events::PanelId;
use crate::types::{LinkRow, NearbyDevice, Space, StorageType};

/// One list row, already formatted: the name, then one cell per column of
/// the panel's [`Column`]s.
pub struct RowView {
    pub style: Style,
    pub name: String,
    pub cells: Vec<String>,
}

/// A fixed-width column right of the name: its header, its cell width, and
/// whether the cells are right-aligned (numbers) or left-aligned.
pub struct Column {
    pub title: &'static str,
    pub width: usize,
    pub right: bool,
}

const SIZE: Column = Column {
    title: "Size",
    width: 7,
    right: true,
};
/// "YYYY-MM-DD HH:MM:SS"
const DATE: Column = Column {
    title: "Date",
    width: 19,
    right: false,
};
/// The countdown to a link's `invalidate_at` (`expired` is the widest cell).
const EXPIRES: Column = Column {
    title: "Expires in",
    width: 10,
    right: true,
};
/// What serves a link (`cloud+P2P` is the widest cell).
const SOURCE: Column = Column {
    title: "Source",
    width: 9,
    right: false,
};

/// Whose device a nearby peer is: `mine`, the user's name, blank when anonymous.
const OWNER: Column = Column {
    title: "Owner",
    width: 16,
    right: false,
};
/// What a nearby peer is: its model, `<Browser> on <OS>`, its form factor, its OS.
const DEVICE: Column = Column {
    title: "Device",
    width: 18,
    right: false,
};

/// Files: `name │ size │ date`.
const FILE_COLUMNS: &[Column] = &[SIZE, DATE];
/// Links: the same, then the link's expiry and source.
const LINK_COLUMNS: &[Column] = &[SIZE, DATE, EXPIRES, SOURCE];
/// Nearby devices: whose and what.
const NEARBY_COLUMNS: &[Column] = &[OWNER, DEVICE];

/// A file listing's rows: marks and the cursor decide the style.
pub fn file_rows(files: &FileList, is_active: bool) -> Vec<RowView> {
    files
        .entries
        .iter()
        .enumerate()
        .map(|(idx, entry)| {
            // MC's markers: `~` a symlink, `*` an executable.
            let base_style = skin::file_style_for(&entry.key, entry.file_type, entry.attrs.posix_permissions);
            let marker = match entry.file_type {
                StorageFileType::Symlink => "~",
                StorageFileType::Dir => "",
                _ if entry.attrs.posix_permissions.is_some_and(|m| m & 0o111 != 0) => "*",
                _ => "",
            };
            let is_marked = files.marked.contains(&entry.key);
            let style = if is_marked && is_active && idx == files.cursor.index {
                skin::core_markselect()
            } else if is_marked {
                skin::core_marked()
            } else {
                base_style
            };
            let size = match files.shown_size(entry) {
                Some(bytes) => format_size(bytes),
                None => " <DIR>".to_string(),
            };
            RowView {
                style,
                name: format!("{}{}", marker, entry.key),
                cells: vec![
                    size,
                    entry.attrs.mtime.as_ref().map(format_modified).unwrap_or_default(),
                ],
            }
        })
        .collect()
}

/// The links list: one row per link — its size, creation time, the
/// countdown to its expiry and what serves it (see [`link_row`]).
pub fn link_rows(links: &LinkList) -> Vec<RowView> {
    let now = Utc::now().fixed_offset();
    links.rows.iter().map(|l| link_row(l, now)).collect()
}

/// A link's row, marked MC-style by its state: `!` nothing serves it (not
/// enterable), `x` expired, `>` P2P only (the owner has to be online); the
/// rows with a live source are bold like directories, the others plain.
fn link_row(l: &LinkRow, now: DateTime<FixedOffset>) -> RowView {
    let expired = l.invalidate_at.is_some_and(|at| at < now);
    let (marker, style) = if !l.has_source() {
        ("!", skin::core_default())
    } else if expired {
        ("x", skin::core_default())
    } else if !l.cloud {
        (">", skin::file_directory())
    } else {
        ("", skin::file_directory())
    };
    let source = match (l.cloud, l.p2p) {
        (true, true) => "cloud+P2P",
        (true, false) => "cloud",
        (false, true) => "P2P",
        (false, false) => "none",
    };
    RowView {
        style,
        name: format!("{}{}", marker, l.name),
        cells: vec![
            format_size(l.size),
            format_modified(&l.created_at),
            expires_in(l.invalidate_at, now),
            source.to_string(),
        ],
    }
}

/// The desktop's expiry countdown: `never`, `expired`, else the rounded
/// days / hours / minutes / seconds left.
fn expires_in(at: Option<DateTime<FixedOffset>>, now: DateTime<FixedOffset>) -> String {
    let Some(at) = at else {
        return "never".to_string();
    };
    let s = (at - now).num_seconds();
    if s < 0 {
        "expired".to_string()
    } else if s >= 86400 {
        format!("{} d", (s + 43200) / 86400)
    } else if s >= 3600 {
        format!("{} h", (s + 1800) / 3600)
    } else if s >= 60 {
        format!("{} m", (s + 30) / 60)
    } else {
        format!("{} s", s)
    }
}

/// The nearby devices: one dir-like row per device (see [`nearby_row`]).
pub fn nearby_rows(app: &App) -> Vec<RowView> {
    app.nearby.iter().map(|d| nearby_row(app, d)).collect()
}

/// A device's row: its name (with our offer's status), whose it is, what
/// it is.
fn nearby_row(app: &App, d: &NearbyDevice) -> RowView {
    let owner = if d.same_user == Some(true) {
        "mine".to_string()
    } else {
        d.display_name.clone().unwrap_or_default()
    };
    RowView {
        style: skin::file_directory(),
        name: app.nearby_row_label(d),
        cells: vec![owner, nearby_device_type(d)],
    }
}

/// The desktop's device line (the model, or `<Browser> on <OS>`); when
/// nothing of that is known, the reported form factor, else the OS.
fn nearby_device_type(d: &NearbyDevice) -> String {
    use share_ninja_core::models::ws_device_profile::{FormFactor, Os};
    let label = device_card_label(&d.device, d.product_id.as_deref());
    if !label.is_empty() {
        return label;
    }
    let kind = match d.device.form_factor {
        Some(FormFactor::Desktop) => "desktop",
        Some(FormFactor::Laptop) => "laptop",
        Some(FormFactor::Tablet) => "tablet",
        Some(FormFactor::Phone) => "phone",
        None => match d.device.os {
            Os::Mac => "macOS",
            Os::Win => "Windows",
            Os::Android => "Android",
            Os::Ios => "iOS",
            Os::Harmony => "HarmonyOS",
            Os::Other => "",
        },
    };
    kind.to_string()
}

/// A name in a `w`-wide cell, FAR style: one that does not fit is cut to
/// the cell and the column line next to it becomes `}` — the second item,
/// the glyph for that row's separator.
fn fit_name(name: &str, w: usize) -> (String, &'static str) {
    if name.chars().count() <= w {
        return (name.to_string(), "│");
    }
    (name.chars().take(w).collect(), "}")
}

/// The frame every panel content sits in: border, title, and — while a
/// listing is in flight — the spinner; else the inside is the caller's to
/// draw, given with the column past the title's last cell (the top
/// border's column marks keep off the title).
fn frame(
    panel: &Panel,
    is_active: bool,
    title: &str,
    name_width: usize,
    area: Rect,
    buf: &mut Buffer,
) -> Option<(Rect, u16)> {
    let title_style = if is_active {
        skin::panel_title_active()
    } else {
        skin::panel_title_inactive()
    };
    // Constrain the title to the name column, right-aligned with a left
    // ellipsis when it is too long.
    let full_title = format!(" {} ", title);
    let max_title_len = name_width.saturating_sub(1);
    let title = if full_title.chars().count() > max_title_len {
        let suffix: String = full_title
            .chars()
            .rev()
            .take(max_title_len.saturating_sub(3))
            .collect::<String>()
            .chars()
            .rev()
            .collect();
        format!("...{}", suffix)
    } else {
        full_title
    };
    let title_end = area.x + 1 + title.chars().count() as u16;
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(skin::panel_border())
        .style(skin::core_default())
        .title(Span::styled(title, title_style));
    let inner = block.inner(area);
    block.render(area, buf);

    if panel.loading {
        let elapsed_ms = panel
            .loading_started
            .map(|t: Instant| t.elapsed().as_millis() as usize)
            .unwrap_or(0);
        // Delay the indicator for 500 ms (quick storages), unless there is
        // nothing else to show.
        if elapsed_ms >= 500 || panel_is_empty(panel) {
            let spinner_chars = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];
            let spinner = spinner_chars[(elapsed_ms / 100) % spinner_chars.len()];
            let msg = format!(
                "{} Loading... [{}.{}s]",
                spinner,
                elapsed_ms / 1000,
                (elapsed_ms % 1000) / 100
            );
            let loading = Line::from(Span::styled(msg, Style::default().fg(Color::Yellow)));
            buf.set_line(inner.x, inner.y, &loading, inner.width);
            return None;
        }
    }
    Some((inner, title_end))
}

fn panel_is_empty(panel: &Panel) -> bool {
    match &panel.content {
        crate::app::Content::Files(f) => f.entries.is_empty(),
        crate::app::Content::Links(l) => l.rows.is_empty(),
        _ => true,
    }
}

/// A list body: rows in three columns, the cursor row highlighted on the
/// active panel, MC-style separators, the mini-status row for the cursor's
/// entry, and the bottom-border labels.
pub struct ListPanel<'a> {
    pub panel: &'a Panel,
    pub is_active: bool,
    pub title: String,
    /// The fixed columns right of the name, per content kind.
    pub columns: &'static [Column],
    pub rows: Vec<RowView>,
    pub cursor: Cursor,
    /// The mini-status row: the cursor's entry (a symlink as `-> target`).
    pub status: Option<RowView>,
    /// Bottom-left: the quick search prefix.
    pub quick_search: Option<&'a str>,
    /// Bottom-left when no quick search: the nearby device count.
    pub nearby: Option<String>,
    /// Bottom-centre: the marked entries summary.
    pub summary: Option<String>,
    /// Bottom-right: free / total space of what the panel sits on (MC).
    pub space: Option<Space>,
    /// The row layout: a file list's mode, `Full` for the other lists.
    pub view: ViewMode,
    /// FAR's sort indicator in the header's first cell (a file list).
    pub sort_letter: Option<char>,
}

impl<'a> ListPanel<'a> {
    /// The list panel for `id`'s content — rows and labels per kind.
    pub fn for_panel(app: &'a App, id: PanelId, side: &str) -> Self {
        use crate::app::Content;
        let panel = app.panel(id);
        let is_active = app.active_panel_id() == id;
        let cursor = app.cursor(id).unwrap_or_default();
        let mut status = None;
        let mut quick_search = None;
        let mut nearby = None;
        let mut summary = None;
        let mut space = None;
        let mut view = ViewMode::Full;
        let mut sort_letter = None;
        let mut columns = FILE_COLUMNS;
        let (title, rows) = match &panel.content {
            Content::Files(f) => {
                if !f.marked.is_empty() {
                    let total: u64 = f
                        .entries
                        .iter()
                        .filter(|e| f.marked.contains(&e.key))
                        .filter_map(|e| f.shown_size(e))
                        .sum();
                    let count = f.marked.len();
                    summary = Some(format!(
                        " {} in {} item{} ",
                        format_bytes_with_commas(total),
                        count,
                        if count == 1 { "" } else { "s" }
                    ));
                }
                quick_search = f.quick_search.as_deref();
                // A local volume's space comes with the listing; a share
                // shows the account's quota standing.
                space = match f.storage_type() {
                    Some(StorageType::Local) => f.space,
                    Some(StorageType::Ninja) => app.usage,
                    Some(StorageType::Archive) | None => None,
                };
                // The nearby device count is per agent: every share panel shows it.
                if f.storage_type() == Some(StorageType::Ninja) {
                    nearby = Some(format!(" Nearby: {} ", app.nearby.len()));
                }
                view = panel.view;
                sort_letter = Some(panel.sort.letter());
                let rows = file_rows(f, is_active);
                status = f.selected_entry().map(|e| status_row(f, e));
                // A local panel is titled by its absolute path alone; a share
                // by its name and the path inside it; a find's temporary list
                // by what was searched for.
                let title = if let Some(found) = &f.temporary {
                    format!("{} - Find: {}", panel.title(side), found.needle)
                } else if f.is_local() {
                    f.display_path()
                } else {
                    format!("{} - {}", panel.title(side), f.display_path())
                };
                (title, rows)
            }
            Content::Links(l) => {
                // The links live on the account's storage: its quota standing.
                space = app.usage;
                columns = LINK_COLUMNS;
                let rows = link_rows(l);
                status = l.selected().map(|r| link_row(r, Utc::now().fixed_offset()));
                (format!("{} - /", panel.title(side)), rows)
            }
            Content::Nearby(_) => {
                columns = NEARBY_COLUMNS;
                let rows = nearby_rows(app);
                status = app.nearby.get(cursor.index).map(|d| nearby_row(app, d));
                (format!("{} - /", panel.title(side)), rows)
            }
            _ => (panel.title(side), Vec::new()),
        };
        Self {
            panel,
            is_active,
            title,
            columns,
            rows,
            cursor,
            status,
            quick_search,
            nearby,
            summary,
            space,
            view,
            sort_letter,
        }
    }
}

/// The mini-status row for a file list's cursor entry: a symlink as
/// `-> target` (read off the disk for a local storage — the listing
/// carries no targets), a directory or file as its row.
fn status_row(files: &FileList, entry: &StorageObjectInfo) -> RowView {
    match entry.file_type {
        StorageFileType::Dir => RowView {
            style: skin::file_directory(),
            name: entry.key.clone(),
            cells: vec![
                files.shown_size(entry).map(format_size).unwrap_or_default(),
                entry.attrs.mtime.as_ref().map(format_modified).unwrap_or_default(),
            ],
        },
        StorageFileType::Symlink => {
            let target = files
                .storage
                .as_ref()
                .filter(|s| s.kind == StorageType::Local)
                .and_then(|s| {
                    let path = crate::local_path::join(&s.id, &files.key_of(&entry.key));
                    std::fs::read_link(path).ok()
                });
            RowView {
                style: skin::file_symlink(),
                name: match target {
                    Some(t) => format!("-> {}", t.display()),
                    None => format!("~{}", entry.key),
                },
                cells: vec![String::new(), String::new()],
            }
        }
        _ => RowView {
            style: skin::file_style_for(&entry.key, entry.file_type, entry.attrs.posix_permissions),
            name: entry.key.clone(),
            cells: vec![
                format_size(entry.size),
                entry.attrs.mtime.as_ref().map(format_modified).unwrap_or_default(),
            ],
        },
    }
}

impl<'a> Widget for ListPanel<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let border_style = skin::panel_border();
        let cols = self.columns;
        let fixed_cols: usize = cols.iter().map(|c| c.width + 1).sum(); // +1 each for its separator
        let inner_width = area.width.saturating_sub(2); // borders
        let name_width = inner_width.saturating_sub(fixed_cols as u16) as usize;

        let Some((inner_area, title_end)) = frame(self.panel, self.is_active, &self.title, name_width, area, buf)
        else {
            return;
        };

        // The column header row, then the list, then the mini-status
        // separator and row.
        let header_y = inner_area.y;
        let list_area = Rect::new(
            inner_area.x,
            inner_area.y + 1,
            inner_area.width,
            inner_area.height.saturating_sub(3),
        );
        let sep_y = inner_area.y + inner_area.height.saturating_sub(2);
        let status_y = inner_area.y + inner_area.height.saturating_sub(1);
        let bottom_y = area.y + area.height - 1;
        // Each column's separator line: border + name, then separator + cell.
        let mut col_xs: Vec<u16> = Vec::with_capacity(cols.len());
        let mut x = area.x + 1 + name_width as u16;
        for col in cols {
            col_xs.push(x);
            x += 1 + col.width as u16;
        }
        let sep_style = border_style;
        let col_style = skin::core_default();

        // A cell padded to its column, aligned per the column.
        let cell = |col: &Column, text: &str| -> String {
            if col.right {
                format!("{:>w$}", text, w = col.width)
            } else {
                format!("{:<w$}", text, w = col.width)
            }
        };
        let cell_text = |row: &RowView, i: usize| -> String { row.cells.get(i).cloned().unwrap_or_default() };

        // The full row shape, shared by the list and the mini-status.
        let row_line = |row: &RowView| -> Line<'static> {
            let (name, sep) = fit_name(&row.name, name_width);
            let mut spans = vec![
                Span::styled(format!("{:<width$}", name, width = name_width), row.style),
                Span::styled(sep, sep_style),
            ];
            for (i, col) in cols.iter().enumerate() {
                if i > 0 {
                    spans.push(Span::styled("│", sep_style));
                }
                spans.push(Span::styled(cell(col, &cell_text(row, i)), col_style));
            }
            Line::from(spans)
        };

        // The wide view: name │ size, the name taking the other columns' room too.
        let wide_name_w = name_width + cols.iter().skip(1).map(|c| c.width + 1).sum::<usize>();
        let wide_line = |row: &RowView| -> Line<'static> {
            let w = wide_name_w;
            let (name, sep) = fit_name(&row.name, w);
            let mut spans = vec![
                Span::styled(format!("{:<width$}", name, width = w), row.style),
                Span::styled(sep, sep_style),
            ];
            if let Some(col) = cols.first() {
                spans.push(Span::styled(cell(col, &cell_text(row, 0)), col_style));
            }
            Line::from(spans)
        };

        // The list's column lines, per view: they get ┬ on the top border and
        // ┴ on the mini-status separator.
        let columns = self.view.columns();
        let brief_col_w = (list_area.width as usize).saturating_sub(columns - 1) / columns;
        let seps: Vec<u16> = match self.view {
            ViewMode::Full => col_xs.clone(),
            ViewMode::Wide => vec![area.x + 1 + wide_name_w as u16],
            ViewMode::Brief | ViewMode::Medium => (1..columns)
                .map(|c| list_area.x + (c * (brief_col_w + 1)) as u16 - 1)
                .collect(),
        };

        // FAR's header: the sort letter in the first cell, the column titles
        // centred over their columns, the column lines running through.
        {
            let header_style = skin::core_header();
            let mut spans_x: Vec<(u16, String)> = Vec::new();
            let centred = |title: &str, x: u16, w: usize| -> (u16, String) {
                let pad = w.saturating_sub(title.chars().count()) / 2;
                (x + pad as u16, title.chars().take(w).collect())
            };
            match self.view {
                ViewMode::Full => {
                    spans_x.push(centred("Name", inner_area.x, name_width));
                    for (col, &cx) in cols.iter().zip(&col_xs) {
                        spans_x.push(centred(col.title, cx + 1, col.width));
                    }
                }
                ViewMode::Wide => {
                    spans_x.push(centred("Name", inner_area.x, wide_name_w));
                    if let Some(col) = cols.first() {
                        spans_x.push(centred(col.title, inner_area.x + wide_name_w as u16 + 1, col.width));
                    }
                }
                ViewMode::Brief | ViewMode::Medium => {
                    for c in 0..columns {
                        let x0 = list_area.x + (c * (brief_col_w + 1)) as u16;
                        spans_x.push(centred("Name", x0, brief_col_w));
                    }
                }
            }
            for (x, text) in spans_x {
                buf.set_string(x, header_y, &text, header_style);
            }
            if let Some(letter) = self.sort_letter {
                buf.set_string(inner_area.x, header_y, letter.to_string(), header_style);
            }
            for &cx in &seps {
                if cx < area.x + area.width - 1 {
                    buf.set_string(cx, header_y, "│", border_style);
                }
            }
        }

        if inner_area.height >= 2 {
            // Separator: ├───┴───┴───┤, the column lines end on it.
            buf.set_string(area.x, sep_y, "├", sep_style);
            for x in inner_area.x..inner_area.x + inner_area.width {
                buf.set_string(x, sep_y, "─", sep_style);
            }
            buf.set_string(area.x + area.width - 1, sep_y, "┤", sep_style);
            for &cx in &seps {
                if cx < area.x + area.width - 1 {
                    buf.set_string(cx, sep_y, "┴", sep_style);
                }
            }
            if let Some(status) = &self.status {
                let line = if status.name.starts_with("-> ") {
                    // A symlink's `-> target` runs across the columns.
                    let shown: String = status.name.chars().take(inner_area.width as usize).collect();
                    Line::from(Span::styled(shown, status.style))
                } else {
                    match self.view {
                        ViewMode::Full => row_line(status),
                        ViewMode::Wide => wide_line(status),
                        // No column lines to line up with: name, then the
                        // cells right-aligned.
                        ViewMode::Brief | ViewMode::Medium => {
                            let tail = cols
                                .iter()
                                .enumerate()
                                .map(|(i, col)| cell(col, &cell_text(status, i)))
                                .collect::<Vec<_>>()
                                .join("  ")
                                .trim_end()
                                .to_string();
                            let room = (inner_area.width as usize).saturating_sub(tail.chars().count() + 1);
                            let (name, sep) = fit_name(&status.name, room);
                            let gap = if sep == "}" { "}" } else { " " };
                            Line::from(vec![
                                Span::styled(format!("{:<width$}{}", name, gap, width = room), status.style),
                                Span::styled(tail, col_style),
                            ])
                        }
                    }
                };
                buf.set_line(inner_area.x, status_y, &line, inner_area.width);
            }
        }

        if self.rows.is_empty() {
            if let Some(error) = &self.panel.error {
                let display = middle_ellipsis(error, list_area.width as usize);
                let error_line = Line::from(Span::styled(display, skin::error_default()));
                buf.set_line(list_area.x, list_area.y, &error_line, list_area.width);
            } else if let Some(msg) = &self.panel.status_message {
                let status_line = Line::from(Span::styled(msg.as_str(), Style::default().fg(Color::Cyan)));
                buf.set_line(list_area.x, list_area.y, &status_line, list_area.width);
            } else {
                buf.set_line(list_area.x, list_area.y, &Line::from("(empty)"), list_area.width);
            }
        } else {
            match self.view {
                ViewMode::Full | ViewMode::Wide => {
                    // One row per entry: name │ size [│ date].
                    let wide = self.view == ViewMode::Wide;
                    let mut items: Vec<ListItem> = self
                        .rows
                        .iter()
                        .map(|row| ListItem::new(if wide { wide_line(row) } else { row_line(row) }))
                        .collect();
                    // An error shows as a row after the entries.
                    if let Some(error) = &self.panel.error {
                        let max_w = list_area.width.saturating_sub(1) as usize;
                        let display = format!(" {}", middle_ellipsis(error, max_w));
                        items.push(ListItem::new(Line::from(Span::styled(display, skin::error_default()))));
                    }
                    let list = if self.is_active {
                        List::new(items).highlight_style(skin::core_selected())
                    } else {
                        List::new(items)
                    };
                    let mut state = ListState::default()
                        .with_offset(self.cursor.scroll)
                        .with_selected(Some(self.cursor.index));
                    StatefulWidget::render(list, list_area, buf, &mut state);

                    // Column separators down to the mini-status line; the
                    // cursor row's invert to match MC. A row's `}` (a cut
                    // name) stays.
                    let selected_row = self.cursor.index.saturating_sub(self.cursor.scroll) as u16;
                    for row in 0..list_area.height {
                        let y = list_area.y + row;
                        let style = if self.is_active && row == selected_row {
                            skin::core_selected()
                        } else {
                            border_style
                        };
                        for &cx in &seps {
                            if cx < area.x + area.width - 1 {
                                let glyph = if buf[(cx, y)].symbol() == "}" { "}" } else { "│" };
                                buf.set_string(cx, y, glyph, style);
                            }
                        }
                    }
                }
                ViewMode::Brief | ViewMode::Medium => {
                    // Names in `columns` columns, filled column by column.
                    let rows_per_col = list_area.height as usize;
                    let col_w = brief_col_w;
                    for c in 1..columns {
                        let x = list_area.x + (c * (col_w + 1)) as u16 - 1;
                        for row in 0..list_area.height {
                            buf.set_string(x, list_area.y + row, "│", border_style);
                        }
                    }
                    for c in 0..columns {
                        let x0 = list_area.x + (c * (col_w + 1)) as u16;
                        for r in 0..rows_per_col {
                            let idx = self.cursor.scroll + c * rows_per_col + r;
                            let Some(row) = self.rows.get(idx) else { break };
                            let style = if self.is_active && idx == self.cursor.index {
                                skin::core_selected()
                            } else {
                                row.style
                            };
                            let (name, sep) = fit_name(&row.name, col_w);
                            let y = list_area.y + r as u16;
                            buf.set_string(x0, y, format!("{:<w$}", name, w = col_w), style);
                            if sep == "}" {
                                // On the column line, or the frame for the last column.
                                let x = if c + 1 < columns {
                                    x0 + col_w as u16
                                } else {
                                    area.x + area.width - 1
                                };
                                buf.set_string(x, y, "}", border_style);
                            }
                        }
                    }
                }
            }
        }
        // The column marks on the top border, off the title's cells.
        for &cx in &seps {
            if cx < area.x + area.width - 1 && cx >= title_end {
                buf.set_string(cx, area.y, "┬", sep_style);
            }
        }

        // Bottom border labels: quick search (else nearby count) left, the
        // marked summary centre, the space right.
        if let Some(q) = self.quick_search {
            let label = format!(" /{} ", q);
            let max = area.width.saturating_sub(4) as usize;
            let shown: String = label.chars().take(max).collect();
            buf.set_string(area.x + 2, bottom_y, &shown, skin::core_input());
        } else if let Some(label) = &self.nearby {
            buf.set_string(area.x + 2, bottom_y, label, border_style);
        }
        if let Some(summary) = &self.summary {
            let summary_len = summary.len() as u16;
            let summary_x = area.x + (area.width.saturating_sub(summary_len)) / 2;
            buf.set_string(summary_x, bottom_y, summary, skin::core_marked());
        }
        if let Some(space) = self.space {
            let label = format_space(space);
            let label_len = label.len() as u16;
            let label_x = area.x + area.width.saturating_sub(label_len + 1);
            buf.set_string(label_x, bottom_y, &label, border_style);
        }
    }
}

/// A key/value body (a nearby device's details).
pub struct DetailsPanel<'a> {
    pub panel: &'a Panel,
    pub is_active: bool,
    pub title: String,
    pub rows: Vec<(&'static str, String)>,
}

impl<'a> Widget for DetailsPanel<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let name_width = area.width.saturating_sub(2) as usize;
        let Some((inner, _)) = frame(self.panel, self.is_active, &self.title, name_width, area, buf) else {
            return;
        };
        if let Some(error) = &self.panel.error {
            let display = middle_ellipsis(error, inner.width as usize);
            buf.set_line(
                inner.x,
                inner.y,
                &Line::from(Span::styled(display, skin::error_default())),
                inner.width,
            );
            return;
        }
        let label_style = skin::get("editor", "editframeactive");
        let lines: Vec<Line> = self
            .rows
            .iter()
            .map(|(label, value)| {
                Line::from(vec![
                    Span::styled(format!(" {:<12} ", label), label_style),
                    Span::styled(value.clone(), skin::core_default()),
                ])
            })
            .collect();
        Paragraph::new(lines).render(inner, buf);
    }
}

/// MC's space label: `free / total (free%)`, compact units.
fn format_space(space: Space) -> String {
    let pct = (space.free * 100).checked_div(space.total).unwrap_or(0);
    format!(
        " {} / {} ({}%) ",
        format_compact(space.free),
        format_compact(space.total),
        pct
    )
}

/// MC-style compact size: `207G`, `12M`, `900K`, `512`.
pub fn format_compact(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["", "K", "M", "G", "T"];
    let mut v = bytes as f64;
    let mut unit = 0;
    while v >= 1000.0 && unit + 1 < UNITS.len() {
        v /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{}", bytes)
    } else if v < 10.0 {
        format!("{:.1}{}", v, UNITS[unit])
    } else {
        format!("{:.0}{}", v, UNITS[unit])
    }
}

/// Truncate a string in the middle with "..." if it exceeds max_width.
fn middle_ellipsis(s: &str, max_width: usize) -> String {
    let len = s.chars().count();
    if len <= max_width || max_width < 4 {
        return s.to_string();
    }
    let keep = max_width - 3; // room for "..."
    let left = keep.div_ceil(2);
    let right = keep / 2;
    let start: String = s.chars().take(left).collect();
    let end: String = s.chars().skip(len - right).collect();
    format!("{}...{}", start, end)
}

fn format_bytes_with_commas(bytes: u64) -> String {
    let s = bytes.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    let formatted: String = result.chars().rev().collect();
    format!("{} B", formatted)
}

/// A timestamp as "YYYY-MM-DD HH:MM:SS".
pub(super) fn format_modified<Tz: chrono::TimeZone>(dt: &chrono::DateTime<Tz>) -> String
where
    Tz::Offset: std::fmt::Display,
{
    dt.format("%Y-%m-%d %H:%M:%S").to_string()
}

pub(super) fn format_size(size: u64) -> String {
    if size < 1024 {
        format!("{} B", size)
    } else if size < 1024 * 1024 {
        format!("{:.0} KB", size as f64 / 1024.0)
    } else if size < 1024 * 1024 * 1024 {
        let mb = size as f64 / (1024.0 * 1024.0);
        if mb > 100.0 {
            format!("{:.0} MB", mb)
        } else {
            format!("{:.1} MB", mb)
        }
    } else {
        let gb = size as f64 / (1024.0 * 1024.0 * 1024.0);
        if gb > 100.0 {
            format!("{:.0} GB", gb)
        } else {
            format!("{:.1} GB", gb)
        }
    }
}
