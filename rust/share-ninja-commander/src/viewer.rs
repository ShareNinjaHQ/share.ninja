//! The built-in file viewer (F3): a byte-offset pager over any open storage,
//! local or share, that reads only the fixed-size aligned blocks the
//! viewport touches through `Engine::read_range` — a share file streams off
//! the warm transfer's source (P2P / S3 range reads) and is never copied.
//!
//! State and layout live here, free of ratatui: the event loop drives moves
//! and block arrivals, `ui::viewer` renders the computed rows. Text mode is
//! line-oriented from a byte offset like mcview (the top row may start
//! mid-line after a hex→text switch; one Up realigns), hex mode is 16-byte
//! rows. A move that needs a block not yet fetched parks in `pending` and
//! replays when the block lands.

use std::collections::{HashMap, HashSet};

use unicode_width::UnicodeWidthChar;

use crate::format::{self, Formatter};
use crate::syntax::{self, Class, Language};

/// Fetch/cache granularity. Taken from the app layer's preview block cache
/// (`share-ninja-app/src/preview_cache.rs`) for the same reason — big enough
/// to amortize a P2P round trip, small enough that a cold seek moves at most
/// half a megabyte before the first rows show. A judgment call there too,
/// not a measured optimum.
pub const BLOCK_SIZE: u64 = 512 * 1024;
/// A text line longer than this is broken here, so laying out a viewport
/// never needs more than `rows × MAX_LINE` bytes. Backward scans (Up, goto)
/// look at most this far back for the line start, so on a pathological
/// line the forward and backward breaks need not coincide.
pub const MAX_LINE: u64 = 64 * 1024;
/// Blocks kept on either side of the viewport's block; the rest are dropped
/// when a new block lands.
const KEEP_BLOCKS: u64 = 2;
/// Left/Right step in no-wrap text mode, in columns.
const HSCROLL_STEP: usize = 10;
/// The largest file the formatted view takes: it needs the whole file in
/// memory, and its output runs two to three times the input.
pub const MAX_FORMAT: u64 = 32 * 1024 * 1024;

/// The formatted view: off; every block on its way in; on, with the file's
/// own blocks kept for the way back.
#[derive(Debug)]
enum Format {
    Off,
    Loading,
    On(Raw),
}

/// The file as fetched, set aside while the formatted text is shown.
#[derive(Debug)]
struct Raw {
    blocks: HashMap<u64, Vec<u8>>,
    size: u64,
    top: u64,
}

/// A forward search's progress reports and its end.
#[derive(Debug)]
pub enum SearchOutcome {
    /// Scanned up to this byte offset.
    Progress(u64),
    Found(u64),
    NotFound,
}

/// A move waiting for a block to arrive (or applied directly when the bytes
/// are cached).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Move {
    Down(usize),
    Up(usize),
    Home,
    End,
    /// Put the row containing this byte at the top.
    Goto(u64),
}

/// One rendered row: its byte range and text pieces by class (syntax, the
/// search match).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    pub start: u64,
    /// Exclusive; for text rows the line's newline is not included.
    pub end: u64,
    pub spans: Vec<(String, Class)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LayoutKey {
    top: u64,
    hex: bool,
    wrap: bool,
    hscroll: usize,
    rows: usize,
    cols: usize,
    blocks_ver: u64,
    match_range: Option<(u64, u64)>,
}

/// The rows the viewport shows, computed once per distinct state.
#[derive(Debug)]
pub struct Layout {
    key: LayoutKey,
    pub rows: Vec<Row>,
    /// Where the row after the last visible one starts; `None` at EOF or
    /// when a block is missing before that point.
    pub next_top: Option<u64>,
    /// The first block the layout needed and did not have.
    pub missing: Option<u64>,
}

/// One decoded text cell: the character shown, the source bytes it stands
/// for (0 for the padding cells of an expanded tab) and the terminal
/// columns it occupies (2 for wide CJK / emoji, 0 for combining marks).
struct Cell {
    ch: char,
    nbytes: u8,
    width: u8,
}

/// Split a line's cells into rows of at most `cols` columns; a cell that
/// doesn't fit starts the next row (a lone cell wider than `cols` still
/// gets a row — the terminal clips it).
fn wrap_rows(cells: &[Cell], cols: usize) -> Vec<std::ops::Range<usize>> {
    let mut rows = Vec::new();
    let mut start = 0;
    let mut width = 0;
    for (i, c) in cells.iter().enumerate() {
        let w = c.width as usize;
        if width + w > cols && width > 0 {
            rows.push(start..i);
            start = i;
            width = 0;
        }
        width += w;
    }
    if start < cells.len() {
        rows.push(start..cells.len());
    }
    rows
}

/// The cells of a no-wrap row from column `hscroll` on, at most `cols`
/// columns, as their index range; a wide cell cut by either edge is left
/// out. Returns the bytes of the cells scrolled off the left too.
fn visible_cells(cells: &[Cell], hscroll: usize, cols: usize) -> (u64, std::ops::Range<usize>) {
    let mut col = 0usize;
    let mut skipped: u64 = 0;
    let mut first = None;
    let mut end = 0;
    for (i, c) in cells.iter().enumerate() {
        let w = c.width as usize;
        if col < hscroll || col + w <= hscroll {
            skipped += c.nbytes as u64;
            col += w;
            continue;
        }
        if col - hscroll + w > cols {
            break;
        }
        first.get_or_insert(i);
        end = i + 1;
        col += w;
    }
    let start = first.unwrap_or(end);
    (skipped, start..end)
}

pub struct ViewerState {
    /// Distinguishes this viewer's events from a closed one's.
    pub seq: u64,
    pub storage_id: String,
    pub path: String,
    /// A local storage's file: its size and blocks are fetched through the
    /// engine synchronously (the disk answers at once), so no frame waits
    /// on the event round trip. A share's come back as events.
    pub local: bool,
    pub title: String,
    /// The file's syntax, by its extension; `None` shows it plain.
    syntax: Option<&'static Language>,
    /// The file's pretty-printer, by its extension; `None` has no
    /// formatted view.
    formatter: Option<Formatter>,
    format: Format,
    /// `None` until `ViewerOpened` delivers it.
    pub size: Option<u64>,
    pub top: u64,
    pub hex: bool,
    pub wrap: bool,
    pub hscroll: usize,
    blocks: HashMap<u64, Vec<u8>>,
    blocks_ver: u64,
    inflight: HashSet<u64>,
    pub error: Option<String>,
    /// Transient message (search progress / outcome, goto parse errors).
    pub status: Option<String>,
    /// A move parked on the block it needs next; retried when any block
    /// lands, so it walks its way to the bytes it needs.
    pending: Option<(Move, u64)>,
    pub needle: Option<Vec<u8>>,
    pub search_running: bool,
    search_abort: Option<tokio::task::AbortHandle>,
    pub match_range: Option<(u64, u64)>,
    rows: usize,
    cols: usize,
    layout: Option<Layout>,
}

impl ViewerState {
    pub fn new(seq: u64, storage_id: String, path: String, local: bool, title: String) -> Self {
        let syntax = syntax::by_path(&path);
        let formatter = Formatter::by_path(&path);
        Self {
            seq,
            storage_id,
            path,
            local,
            title,
            syntax,
            formatter,
            format: Format::Off,
            size: None,
            top: 0,
            hex: false,
            wrap: false,
            hscroll: 0,
            blocks: HashMap::new(),
            blocks_ver: 0,
            inflight: HashSet::new(),
            error: None,
            status: None,
            pending: None,
            needle: None,
            search_running: false,
            search_abort: None,
            match_range: None,
            rows: 0,
            cols: 0,
            layout: None,
        }
    }

    pub fn set_size(&mut self, size: u64) {
        self.size = Some(size);
        self.layout = None;
    }

    /// The viewport in rows/columns; set by the draw before layout.
    pub fn set_viewport(&mut self, rows: usize, cols: usize) {
        if self.rows != rows || self.cols != cols {
            self.rows = rows;
            self.cols = cols;
            self.layout = None;
        }
    }

    pub fn rows(&self) -> usize {
        self.rows
    }

    // ── search task bookkeeping ────────────────────────────────────────────

    pub fn search_started(&mut self, abort: tokio::task::AbortHandle) {
        self.abort_search();
        self.search_abort = Some(abort);
        self.search_running = true;
    }

    /// Stop a running search; `true` if one was running.
    pub fn abort_search(&mut self) -> bool {
        let was = self.search_running;
        if let Some(h) = self.search_abort.take() {
            h.abort();
        }
        self.search_running = false;
        was
    }

    pub fn search_finished(&mut self) {
        self.search_abort = None;
        self.search_running = false;
    }

    // ── blocks ─────────────────────────────────────────────────────────────

    fn block_of(off: u64) -> u64 {
        off / BLOCK_SIZE
    }

    /// The blocks to fetch now: every one still missing while the
    /// formatted view loads; else what a parked move or the layout is stuck
    /// on, else one ahead of and one behind the viewport. Marks them in
    /// flight.
    pub fn plan_requests(&mut self) -> Vec<u64> {
        let Some(size) = self.size else { return Vec::new() };
        if size == 0 {
            return Vec::new();
        }
        let last_block = Self::block_of(size - 1);
        let mut want: Vec<u64> = Vec::new();
        if matches!(self.format, Format::Loading) {
            want.extend(0..=last_block);
        }
        if let Some((_, idx)) = self.pending {
            want.push(idx);
        }
        let layout = self.layout();
        if let Some(m) = layout.missing {
            want.push(m);
        } else {
            let after = layout
                .next_top
                .or_else(|| layout.rows.last().map(|r| r.end))
                .unwrap_or(self.top);
            let ahead = Self::block_of(after) + 1;
            if ahead <= last_block {
                want.push(ahead);
            }
            if self.top > 0 {
                let behind = Self::block_of(self.top - 1);
                want.push(behind);
                if behind > 0 {
                    want.push(behind - 1);
                }
            }
        }
        want.retain(|idx| *idx <= last_block && !self.blocks.contains_key(idx) && !self.inflight.contains(idx));
        want.sort_unstable();
        want.dedup();
        for idx in &want {
            self.inflight.insert(*idx);
        }
        want
    }

    /// A block fetch landed: keep it, drop the far-away ones, replay a
    /// parked move. While the formatted view loads every block is kept,
    /// and the last one in runs the formatter; a formatted view ignores
    /// what raw fetches still land.
    pub fn on_block(&mut self, idx: u64, result: Result<Vec<u8>, String>) {
        self.inflight.remove(&idx);
        if matches!(self.format, Format::On(_)) {
            return;
        }
        let bytes = match result {
            Ok(b) => b,
            Err(e) => {
                self.error = Some(e);
                self.format = Format::Off;
                return;
            }
        };
        let expected = self
            .size
            .map(|s| BLOCK_SIZE.min(s.saturating_sub(idx * BLOCK_SIZE)))
            .unwrap_or(BLOCK_SIZE);
        if (bytes.len() as u64) < expected {
            self.error = Some(format!(
                "short read at offset {}: got {} of {} bytes",
                idx * BLOCK_SIZE,
                bytes.len(),
                expected
            ));
            return;
        }
        self.blocks.insert(idx, bytes);
        if matches!(self.format, Format::Loading) {
            self.install_when_complete();
            return;
        }
        let cur = Self::block_of(self.top);
        self.blocks.retain(|k, _| k.abs_diff(cur) <= KEEP_BLOCKS);
        self.blocks_ver += 1;
        self.layout = None;
        if let Some((m, _)) = self.pending.take() {
            self.apply(m);
        }
    }

    // ── the formatted view ─────────────────────────────────────────────────

    /// Whether the formatted text is what the rows show.
    pub fn is_formatted(&self) -> bool {
        matches!(self.format, Format::On(_))
    }

    /// F2's cycle: raw → wrapped → formatted (a file with a formatter,
    /// within `MAX_FORMAT`) → raw. Hex mode only toggles the wrap flag.
    pub fn cycle_wrap(&mut self) {
        if self.is_formatted() {
            self.unformat();
            return;
        }
        if !self.hex && self.wrap && self.can_format() {
            self.wrap = false;
            self.format = Format::Loading;
            self.pending = None;
            self.layout = None;
            // Everything may be cached already: then there is no block
            // arrival to run the formatter.
            self.install_when_complete();
            return;
        }
        self.toggle_wrap();
    }

    /// Run the formatter once every block of the file is in.
    fn install_when_complete(&mut self) {
        let size = self.size.unwrap_or(0);
        let all = size == 0 || (0..=Self::block_of(size - 1)).all(|i| self.blocks.contains_key(&i));
        if all {
            self.install_formatted();
        }
    }

    /// F2's label: the state it goes to next.
    pub fn wrap_label(&self) -> &'static str {
        match self.format {
            Format::On(_) => "Unform",
            Format::Loading => "Format",
            Format::Off if self.wrap && !self.hex && self.can_format() => "Format",
            Format::Off if self.wrap => "Unwrap",
            Format::Off => "Wrap",
        }
    }

    fn can_format(&self) -> bool {
        self.formatter.is_some() && self.size.is_some_and(|s| s <= MAX_FORMAT)
    }

    /// Every block is in: the formatter's output becomes the block store,
    /// the file's own blocks wait in `Raw`.
    fn install_formatted(&mut self) {
        let Some(formatter) = self.formatter else {
            self.format = Format::Off;
            return;
        };
        let size = self.size.unwrap_or(0);
        let input = self.window(0, size).unwrap_or_default();
        let formatted = format::format(formatter, &input);
        let raw = Raw {
            blocks: std::mem::take(&mut self.blocks),
            size,
            top: self.top,
        };
        for (i, chunk) in formatted.bytes.chunks(BLOCK_SIZE as usize).enumerate() {
            self.blocks.insert(i as u64, chunk.to_vec());
        }
        self.size = Some(formatted.bytes.len() as u64);
        self.format = Format::On(raw);
        self.status = formatted.error_at.map(|at| {
            format!(
                "{}: not well-formed from byte {}, the rest shown as it is",
                formatter.label(),
                at
            )
        });
        self.top = 0;
        self.hscroll = 0;
        self.match_range = None;
        self.pending = None;
        self.blocks_ver += 1;
        self.layout = None;
    }

    /// Back to the file's own bytes, where the view was.
    fn unformat(&mut self) {
        let Format::On(raw) = std::mem::replace(&mut self.format, Format::Off) else {
            return;
        };
        self.blocks = raw.blocks;
        self.size = Some(raw.size);
        self.top = raw.top;
        self.hscroll = 0;
        self.match_range = None;
        self.pending = None;
        self.status = None;
        self.blocks_ver += 1;
        self.layout = None;
    }

    /// The first `needle` at or after `from` in the cached bytes — the
    /// formatted view's search, which has the whole text at hand.
    pub fn find_forward(&self, from: u64, needle: &[u8]) -> Option<u64> {
        let size = self.size?;
        if needle.is_empty() || from >= size {
            return None;
        }
        let mut pos = from;
        while pos < size {
            let chunk = self.window(pos, BLOCK_SIZE + needle.len() as u64 - 1).ok()?;
            if let Some(i) = chunk.windows(needle.len()).position(|w| w == needle) {
                return Some(pos + i as u64);
            }
            pos += BLOCK_SIZE;
        }
        None
    }

    /// Contiguous bytes `[off, off+len)` clamped to the file, or the first
    /// block index that is not cached.
    fn window(&self, off: u64, len: u64) -> Result<Vec<u8>, u64> {
        let size = self.size.unwrap_or(0);
        let end = off.saturating_add(len).min(size);
        if off >= end {
            return Ok(Vec::new());
        }
        let mut out = Vec::with_capacity((end - off) as usize);
        let mut pos = off;
        while pos < end {
            let idx = Self::block_of(pos);
            let block = self.blocks.get(&idx).ok_or(idx)?;
            let in_off = (pos - idx * BLOCK_SIZE) as usize;
            if in_off >= block.len() {
                break;
            }
            let take = ((end - pos) as usize).min(block.len() - in_off);
            out.extend_from_slice(&block[in_off..in_off + take]);
            pos += take as u64;
        }
        Ok(out)
    }

    // ── moves ──────────────────────────────────────────────────────────────

    /// Apply a move now if its bytes are cached, else park it until they
    /// arrive.
    pub fn apply(&mut self, m: Move) {
        let Some(size) = self.size else {
            self.pending = Some((m, 0));
            return;
        };
        self.status = None;
        if self.hex {
            self.apply_hex(m, size);
            return;
        }
        let res: Result<(), u64> = match m {
            Move::Home => {
                self.top = 0;
                self.hscroll = 0;
                Ok(())
            }
            Move::End => {
                self.top = size;
                self.layout = None;
                self.apply(Move::Up(self.rows.max(1)));
                Ok(())
            }
            Move::Goto(off) => {
                let off = off.min(size.saturating_sub(1));
                self.row_start_containing(off).map(|start| self.top = start)
            }
            Move::Up(n) => {
                let mut done = 0;
                let mut res = Ok(());
                while done < n && self.top > 0 {
                    match self.row_start_containing(self.top - 1) {
                        Ok(start) => {
                            self.top = start;
                            self.layout = None;
                            done += 1;
                        }
                        Err(idx) => {
                            res = Err(idx);
                            break;
                        }
                    }
                }
                if let Err(idx) = res {
                    self.pending = Some((Move::Up(n - done), idx));
                    self.layout = None;
                    return;
                }
                Ok(())
            }
            Move::Down(n) => {
                let layout = self.layout();
                let target = if n >= layout.rows.len() {
                    layout.next_top
                } else {
                    layout.rows.get(n).map(|r| r.start)
                };
                match (target, layout.missing) {
                    (Some(t), _) => {
                        self.top = t;
                        Ok(())
                    }
                    (None, Some(idx)) => Err(idx),
                    // EOF inside the viewport: nothing below.
                    (None, None) => Ok(()),
                }
            }
        };
        self.layout = None;
        if let Err(idx) = res {
            self.pending = Some((m, idx));
        }
    }

    fn apply_hex(&mut self, m: Move, size: u64) {
        let rows = self.rows.max(1) as u64;
        let last_row = size.saturating_sub(1) / 16 * 16;
        self.top = match m {
            Move::Home => 0,
            Move::End => last_row.saturating_sub((rows - 1) * 16),
            Move::Goto(off) => off.min(size.saturating_sub(1)) / 16 * 16,
            Move::Up(n) => self.top.saturating_sub(16 * n as u64),
            Move::Down(n) => (self.top + 16 * n as u64).min(last_row),
        };
        self.layout = None;
    }

    /// Hex is the file's own bytes: a formatted view goes first.
    pub fn toggle_hex(&mut self) {
        self.unformat();
        self.hex = !self.hex;
        if self.hex {
            self.top = self.top / 16 * 16;
        }
        self.pending = None;
        self.layout = None;
    }

    pub fn toggle_wrap(&mut self) {
        self.wrap = !self.wrap;
        if self.wrap {
            self.hscroll = 0;
        }
        self.pending = None;
        self.layout = None;
    }

    pub fn scroll_left(&mut self) {
        if !self.hex && !self.wrap {
            self.hscroll = self.hscroll.saturating_sub(HSCROLL_STEP);
            self.layout = None;
        }
    }

    pub fn scroll_right(&mut self) {
        if !self.hex && !self.wrap {
            self.hscroll += HSCROLL_STEP;
            self.layout = None;
        }
    }

    /// A search hit: highlight it and bring its row to the top.
    pub fn on_found(&mut self, off: u64, len: usize) {
        self.match_range = Some((off, off + len as u64));
        self.layout = None;
        self.apply(Move::Goto(off));
    }

    /// Where the next search continues from: past the current match, else
    /// the top of the view.
    pub fn search_from(&self) -> u64 {
        match self.match_range {
            Some((start, _)) => start + 1,
            None => self.top,
        }
    }

    // ── text geometry ──────────────────────────────────────────────────────

    /// End of the line starting at `pos` (the newline's offset, or where the
    /// line is broken) and where the next line starts.
    fn find_line_end(&self, pos: u64) -> Result<(u64, u64), u64> {
        let size = self.size.unwrap_or(0);
        let limit = pos.saturating_add(MAX_LINE).min(size);
        let mut p = pos;
        while p < limit {
            let idx = Self::block_of(p);
            let block = self.blocks.get(&idx).ok_or(idx)?;
            let base = idx * BLOCK_SIZE;
            let in_off = (p - base) as usize;
            let upto = ((limit - base) as usize).min(block.len());
            if in_off >= upto {
                // Block shorter than the size claims: EOF here.
                return Ok((p, p));
            }
            if let Some(i) = block[in_off..upto].iter().position(|b| *b == b'\n') {
                let nl = p + i as u64;
                return Ok((nl, nl + 1));
            }
            p = base + upto as u64;
            if upto < block.len() {
                break;
            }
        }
        Ok((limit, limit))
    }

    /// Start of the line containing byte `off`: scan back block by block
    /// for a newline, at most `MAX_LINE` bytes — only the blocks actually
    /// crossed are needed, so an Up just past a block boundary with the
    /// newline a few bytes back doesn't wait for the previous block.
    fn line_start_containing(&self, off: u64) -> Result<u64, u64> {
        let lo = off.saturating_sub(MAX_LINE);
        let mut p = off;
        while p > lo {
            let idx = Self::block_of(p - 1);
            let block = self.blocks.get(&idx).ok_or(idx)?;
            let base = idx * BLOCK_SIZE;
            let from = (lo.max(base) - base) as usize;
            let to = ((p - base) as usize).min(block.len());
            if from < to
                && let Some(i) = block[from..to].iter().rposition(|b| *b == b'\n')
            {
                return Ok(base + (from + i) as u64 + 1);
            }
            p = lo.max(base);
        }
        Ok(lo)
    }

    /// Start of the row that contains byte `off`: its line start, then in
    /// wrap mode the wrapped chunk holding `off`.
    fn row_start_containing(&self, off: u64) -> Result<u64, u64> {
        let ls = self.line_start_containing(off)?;
        if !self.wrap || self.cols == 0 {
            return Ok(ls);
        }
        let mut content = self.window(ls, off - ls + 1)?;
        match content.last() {
            Some(b'\n') => {
                content.pop();
            }
            // Finish the multibyte character byte `off` sits in: cut short it
            // would decode as a 1-column U+FFFD and could fit where the real
            // (2-column) character does not, moving the row boundary.
            Some(b) if b & 0x80 != 0 => {
                let tail = self.window(off + 1, 3)?;
                content.extend(tail.iter().take_while(|b| **b & 0xC0 == 0x80));
            }
            _ => {}
        }
        let cells = decode_line(&content);
        let Some(last) = wrap_rows(&cells, self.cols).pop() else {
            return Ok(ls);
        };
        let skipped: u64 = cells[..last.start].iter().map(|c| c.nbytes as u64).sum();
        Ok(ls + skipped)
    }

    // ── layout ─────────────────────────────────────────────────────────────

    /// The last computed layout, for rendering (the draw computes it via
    /// `layout()` first).
    pub fn current_layout(&self) -> Option<&Layout> {
        self.layout.as_ref()
    }

    /// The viewport's rows for the current state, recomputed only when
    /// something it depends on changed.
    pub fn layout(&mut self) -> &Layout {
        let key = LayoutKey {
            top: self.top,
            hex: self.hex,
            wrap: self.wrap,
            hscroll: self.hscroll,
            rows: self.rows,
            cols: self.cols,
            blocks_ver: self.blocks_ver,
            match_range: self.match_range,
        };
        if self.layout.as_ref().is_none_or(|l| l.key != key) {
            let (rows, next_top, missing) = if self.hex {
                self.layout_hex()
            } else {
                self.layout_text()
            };
            self.layout = Some(Layout {
                key,
                rows,
                next_top,
                missing,
            });
        }
        self.layout.as_ref().expect("set above")
    }

    fn layout_text(&self) -> (Vec<Row>, Option<u64>, Option<u64>) {
        let size = self.size.unwrap_or(0);
        let mut rows: Vec<Row> = Vec::new();
        let mut pos = self.top;
        let mut missing = None;
        // The lexer runs on from the viewport's top, line to line.
        let mut lex = syntax::State::default();
        // One row past the viewport gives the next top for Down.
        while rows.len() <= self.rows && pos < size {
            let (line_end, next) = match self.find_line_end(pos) {
                Ok(v) => v,
                Err(idx) => {
                    missing = Some(idx);
                    break;
                }
            };
            let mut content = match self.window(pos, line_end - pos) {
                Ok(c) => c,
                Err(idx) => {
                    missing = Some(idx);
                    break;
                }
            };
            if content.last() == Some(&b'\r') {
                content.pop();
            }
            let cells = decode_line(&content);
            let classes = match self.syntax {
                Some(lang) => {
                    let chars: Vec<char> = cells.iter().map(|c| c.ch).collect();
                    syntax::classes(lang, &chars, &mut lex)
                }
                None => vec![Class::Plain; cells.len()],
            };
            if self.wrap && self.cols > 0 {
                if cells.is_empty() {
                    rows.push(Row {
                        start: pos,
                        end: line_end,
                        spans: Vec::new(),
                    });
                }
                let mut start = pos;
                for range in wrap_rows(&cells, self.cols) {
                    let chunk = &cells[range.clone()];
                    let nbytes: u64 = chunk.iter().map(|c| c.nbytes as u64).sum();
                    rows.push(Row {
                        start,
                        end: start + nbytes,
                        spans: self.spans_for(chunk, &classes[range], start),
                    });
                    start += nbytes;
                }
            } else {
                let (skipped, range) = visible_cells(&cells, self.hscroll, self.cols);
                let spans = self.spans_for(&cells[range.clone()], &classes[range], pos + skipped);
                rows.push(Row {
                    start: pos,
                    end: line_end,
                    spans,
                });
            }
            if next == line_end && line_end < size {
                // Broken at MAX_LINE: the next row continues the same line.
                pos = line_end;
            } else {
                pos = next;
            }
            if next == pos && pos >= size {
                break;
            }
        }
        let next_top = if missing.is_none() {
            rows.get(self.rows).map(|r| r.start)
        } else {
            None
        };
        rows.truncate(self.rows);
        (rows, next_top, missing)
    }

    /// Group cells into runs by class: the search match where the cell's
    /// bytes fall inside it, else the cell's syntax class.
    fn spans_for(&self, cells: &[Cell], classes: &[Class], start: u64) -> Vec<(String, Class)> {
        let mut spans: Vec<(String, Class)> = Vec::new();
        let mut off = start;
        for (c, class) in cells.iter().zip(classes) {
            let hit = self
                .match_range
                .is_some_and(|(a, b)| c.nbytes > 0 && off >= a && off < b);
            let class = if hit { Class::Match } else { *class };
            match spans.last_mut() {
                Some((s, k)) if *k == class => s.push(c.ch),
                _ => spans.push((c.ch.to_string(), class)),
            }
            off += c.nbytes as u64;
        }
        spans
    }

    fn layout_hex(&self) -> (Vec<Row>, Option<u64>, Option<u64>) {
        let size = self.size.unwrap_or(0);
        let mut rows: Vec<Row> = Vec::new();
        let mut missing = None;
        for r in 0..self.rows as u64 {
            let off = self.top + 16 * r;
            if off >= size {
                break;
            }
            let bytes = match self.window(off, 16) {
                Ok(b) => b,
                Err(idx) => {
                    missing = Some(idx);
                    break;
                }
            };
            let mut spans: Vec<(String, Class)> = vec![(format!("{:08X}  ", off), Class::Plain)];
            let push = |spans: &mut Vec<(String, Class)>, s: String, hit: bool| {
                let class = if hit { Class::Match } else { Class::Plain };
                match spans.last_mut() {
                    Some((t, k)) if *k == class => t.push_str(&s),
                    _ => spans.push((s, class)),
                }
            };
            for i in 0..16usize {
                let hit = self
                    .match_range
                    .is_some_and(|(a, b)| off + (i as u64) >= a && off + (i as u64) < b);
                match bytes.get(i) {
                    Some(b) => push(&mut spans, format!("{:02X}", b), hit && i < bytes.len()),
                    None => push(&mut spans, "  ".to_string(), false),
                }
                push(
                    &mut spans,
                    if i == 7 { "  ".to_string() } else { " ".to_string() },
                    false,
                );
            }
            push(&mut spans, " ".to_string(), false);
            for (i, b) in bytes.iter().enumerate() {
                let hit = self
                    .match_range
                    .is_some_and(|(a, c)| off + (i as u64) >= a && off + (i as u64) < c);
                let ch = if (0x20..0x7f).contains(b) { *b as char } else { '.' };
                push(&mut spans, ch.to_string(), hit);
            }
            rows.push(Row {
                start: off,
                end: off + bytes.len() as u64,
                spans,
            });
        }
        let after = self.top + 16 * self.rows as u64;
        let next_top = if missing.is_none() && after < size {
            Some(after)
        } else {
            None
        };
        (rows, next_top, missing)
    }
}

/// Decode one line's bytes into cells: tabs expand to the next 8-column
/// stop, other control characters show as `.`, invalid UTF-8 as U+FFFD.
/// Columns are terminal columns (`unicode-width`), not characters.
fn decode_line(bytes: &[u8]) -> Vec<Cell> {
    let mut cells: Vec<Cell> = Vec::with_capacity(bytes.len());
    let mut col = 0usize;
    for chunk in bytes.utf8_chunks() {
        for ch in chunk.valid().chars() {
            match ch {
                '\t' => {
                    let k = 8 - col % 8;
                    cells.push(Cell {
                        ch: ' ',
                        nbytes: 1,
                        width: 1,
                    });
                    for _ in 1..k {
                        cells.push(Cell {
                            ch: ' ',
                            nbytes: 0,
                            width: 1,
                        });
                    }
                    col += k;
                }
                c if c.is_control() => {
                    cells.push(Cell {
                        ch: '.',
                        nbytes: c.len_utf8() as u8,
                        width: 1,
                    });
                    col += 1;
                }
                c => {
                    let width = c.width().unwrap_or(1) as u8;
                    cells.push(Cell {
                        ch: c,
                        nbytes: c.len_utf8() as u8,
                        width,
                    });
                    col += width as usize;
                }
            }
        }
        let invalid = chunk.invalid();
        if !invalid.is_empty() {
            cells.push(Cell {
                ch: '\u{FFFD}',
                nbytes: invalid.len() as u8,
                width: 1,
            });
            col += 1;
        }
    }
    cells
}

/// Parse a goto target: `NN%`, `0x…` hex, or a decimal byte offset.
pub fn parse_goto(input: &str, size: u64) -> Result<u64, String> {
    let s = input.trim();
    if s.is_empty() {
        return Err("empty offset".to_string());
    }
    if let Some(pct) = s.strip_suffix('%') {
        let p: f64 = pct.trim().parse().map_err(|_| format!("not a percentage: {s}"))?;
        if !(0.0..=100.0).contains(&p) {
            return Err(format!("percentage out of range: {s}"));
        }
        return Ok((size as f64 * p / 100.0) as u64);
    }
    if let Some(hex) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
        return u64::from_str_radix(hex, 16).map_err(|_| format!("not a hex offset: {s}"));
    }
    s.parse::<u64>().map_err(|_| format!("not an offset: {s}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn viewer(data: &[u8], rows: usize, cols: usize) -> ViewerState {
        let mut v = ViewerState::new(1, "s".into(), "f".into(), false, "f".into());
        v.set_size(data.len() as u64);
        v.set_viewport(rows, cols);
        for (idx, chunk) in data.chunks(BLOCK_SIZE as usize).enumerate() {
            v.on_block(idx as u64, Ok(chunk.to_vec()));
        }
        v
    }

    fn texts(v: &mut ViewerState) -> Vec<String> {
        v.layout()
            .rows
            .iter()
            .map(|r| r.spans.iter().map(|(s, _)| s.as_str()).collect())
            .collect()
    }

    #[test]
    fn text_rows_and_down_up() {
        let mut v = viewer(b"one\ntwo\r\nthree\n\nfive", 2, 80);
        assert_eq!(texts(&mut v), vec!["one", "two"]);
        assert_eq!(v.layout().next_top, Some(9));
        v.apply(Move::Down(1));
        assert_eq!(v.top, 4);
        assert_eq!(texts(&mut v), vec!["two", "three"]);
        v.apply(Move::Down(2));
        assert_eq!(texts(&mut v), vec!["", "five"]);
        // EOF is already inside the viewport: a page down goes nowhere,
        // a line down still runs the last line up to the top.
        v.apply(Move::Down(5));
        assert_eq!(v.top, 15);
        v.apply(Move::Down(1));
        assert_eq!(v.top, 16);
        v.apply(Move::Up(1));
        assert_eq!(v.top, 15);
        v.apply(Move::Up(1));
        assert_eq!(v.top, 9);
        v.apply(Move::Up(10));
        assert_eq!(v.top, 0);
        v.apply(Move::End);
        assert_eq!(texts(&mut v), vec!["", "five"]);
    }

    #[test]
    fn wrap_splits_lines_and_up_lands_on_chunks() {
        let mut v = viewer(b"abcdefgh\nij", 3, 3);
        v.toggle_wrap();
        assert_eq!(texts(&mut v), vec!["abc", "def", "gh"]);
        assert_eq!(v.layout().next_top, Some(9));
        v.apply(Move::Down(1));
        assert_eq!(v.top, 3);
        v.apply(Move::Down(2));
        assert_eq!(v.top, 9);
        v.apply(Move::Up(1));
        assert_eq!(v.top, 6);
        v.apply(Move::Up(1));
        assert_eq!(v.top, 3);
        v.apply(Move::Goto(7));
        assert_eq!(v.top, 6);
    }

    #[test]
    fn wrap_and_hscroll_count_terminal_columns() {
        // 日本語テキスト: 7 chars, 3 bytes and 2 columns each.
        let data = "日本語テキスト\nab".as_bytes();
        let mut v = viewer(data, 6, 5);
        v.toggle_wrap();
        assert_eq!(texts(&mut v), vec!["日本", "語テ", "キス", "ト", "ab"]);
        let starts: Vec<u64> = v.layout().rows.iter().map(|r| r.start).collect();
        assert_eq!(starts, vec![0, 6, 12, 18, 22]);
        v.apply(Move::Goto(13));
        assert_eq!(v.top, 12);
        v.apply(Move::Up(1));
        assert_eq!(v.top, 6);
        // No wrap: scrolling 2 columns right drops the first wide char whole,
        // and a wide char cut by the right edge is left out.
        v.toggle_wrap();
        v.apply(Move::Home);
        v.set_viewport(6, 3);
        v.hscroll = 2;
        assert_eq!(texts(&mut v)[0], "本");
        // Combining marks take no column: "é" as e + U+0301 fits in 1.
        let cells = decode_line("e\u{301}x".as_bytes());
        let widths: Vec<u8> = cells.iter().map(|c| c.width).collect();
        assert_eq!(widths, vec![1, 0, 1]);
        assert_eq!(wrap_rows(&cells, 1).len(), 2);
    }

    #[test]
    fn tabs_controls_and_invalid_utf8() {
        let cells = decode_line(b"a\tb\x01\xff\xc3\xa9");
        let s: String = cells.iter().map(|c| c.ch).collect();
        assert_eq!(s, "a       b.\u{FFFD}é");
        let n: u32 = cells.iter().map(|c| c.nbytes as u32).sum();
        assert_eq!(n, 7);
    }

    #[test]
    fn hex_rows() {
        let data: Vec<u8> = (0u8..40).collect();
        let mut v = viewer(&data, 2, 80);
        v.toggle_hex();
        let t = texts(&mut v);
        assert!(t[0].starts_with("00000000  00 01 02 03 04 05 06 07  08 09 0A 0B 0C 0D 0E 0F  "));
        assert_eq!(v.layout().next_top, Some(32));
        v.apply(Move::End);
        assert_eq!(v.top, 16);
        v.apply(Move::Goto(37));
        assert_eq!(v.top, 32);
        assert_eq!(texts(&mut v).len(), 1);
    }

    #[test]
    fn missing_block_parks_the_move() {
        let mut v = ViewerState::new(1, "s".into(), "f".into(), false, "f".into());
        let size = BLOCK_SIZE * 2 + 10;
        v.set_size(size);
        v.set_viewport(2, 80);
        assert_eq!(v.plan_requests(), vec![0]);
        v.apply(Move::End);
        // Parked: the tail block isn't here.
        assert_eq!(v.top, size);
        assert_eq!(v.plan_requests(), vec![1, 2]);
        let mut tail = vec![b'x'; 10];
        tail[4] = b'\n';
        v.on_block(2, Ok(tail));
        // One line up landed on the tail's newline; the second is parked on
        // block 1, which is what gets asked for next.
        assert_eq!(v.top, BLOCK_SIZE * 2 + 5);
        assert_eq!(texts(&mut v), vec!["xxxxx"]);
        assert_eq!(v.pending, Some((Move::Up(1), 1)));
        // Block 1 is already in flight from the first plan: nothing new.
        assert_eq!(v.plan_requests(), Vec::<u64>::new());
        let mut mid = vec![b'y'; BLOCK_SIZE as usize];
        mid[BLOCK_SIZE as usize - 3] = b'\n';
        v.on_block(1, Ok(mid));
        assert_eq!(v.top, BLOCK_SIZE * 2 - 2);
        assert_eq!(v.pending, None);
        assert_eq!(texts(&mut v), vec!["yyxxxx", "xxxxx"]);
    }

    #[test]
    fn short_block_is_an_error() {
        let mut v = ViewerState::new(1, "s".into(), "f".into(), false, "f".into());
        v.set_size(BLOCK_SIZE * 3);
        v.on_block(0, Ok(vec![0; 10]));
        assert!(v.error.as_deref().is_some_and(|e| e.contains("short read")));
    }

    #[test]
    fn match_highlight_spans() {
        let mut v = viewer(b"hello world\n", 1, 80);
        v.on_found(6, 5);
        let rows = &v.layout().rows;
        assert_eq!(
            rows[0].spans,
            vec![("hello ".to_string(), Class::Plain), ("world".to_string(), Class::Match)]
        );
        assert_eq!(v.search_from(), 7);
    }

    #[test]
    fn syntax_spans_by_extension() {
        let mut v = ViewerState::new(1, "s".into(), "d/f.py".into(), false, "f.py".into());
        v.set_size(12);
        v.set_viewport(2, 80);
        v.on_block(0, Ok(b"x = 'a' # c\n".to_vec()));
        assert_eq!(
            v.layout().rows[0].spans,
            vec![
                ("x = ".to_string(), Class::Plain),
                ("'a'".to_string(), Class::Str),
                (" ".to_string(), Class::Plain),
                ("# c".to_string(), Class::Comment),
            ]
        );
        // The match wins over the syntax class.
        v.on_found(4, 3);
        assert_eq!(v.layout().rows[0].spans[1], ("'a'".to_string(), Class::Match));
    }

    #[test]
    fn f2_cycles_raw_wrap_formatted_and_back() {
        let data = br#"{"a":[1,2],"b":"x"}"#;
        let mut v = ViewerState::new(1, "s".into(), "d/f.json".into(), false, "f.json".into());
        v.set_size(data.len() as u64);
        v.set_viewport(10, 80);
        v.on_block(0, Ok(data.to_vec()));
        assert_eq!(v.wrap_label(), "Wrap");
        v.cycle_wrap();
        assert!(v.wrap);
        assert_eq!(v.wrap_label(), "Format");
        v.cycle_wrap();
        // Every block was in: formatted at once.
        assert!(v.is_formatted());
        assert!(!v.wrap);
        assert_eq!(v.wrap_label(), "Unform");
        assert_eq!(texts(&mut v), ["{", "  \"a\": [", "    1,", "    2", "  ],", "  \"b\": \"x\"", "}"]);
        // Search runs on the formatted text.
        assert_eq!(v.find_forward(0, b"\"b\""), Some(31));
        v.cycle_wrap();
        assert!(!v.is_formatted());
        assert_eq!(v.size, Some(data.len() as u64));
        assert_eq!(texts(&mut v), [String::from_utf8_lossy(data)]);
    }

    #[test]
    fn a_file_without_a_formatter_only_wraps() {
        let mut v = viewer(b"abc\n", 2, 80);
        v.cycle_wrap();
        assert_eq!(v.wrap_label(), "Unwrap");
        v.cycle_wrap();
        assert!(!v.wrap && !v.is_formatted());
    }

    #[test]
    fn goto_parsing() {
        assert_eq!(parse_goto("50%", 200).unwrap(), 100);
        assert_eq!(parse_goto("0x10", 200).unwrap(), 16);
        assert_eq!(parse_goto(" 42 ", 200).unwrap(), 42);
        assert!(parse_goto("", 200).is_err());
        assert!(parse_goto("120%", 200).is_err());
        assert!(parse_goto("zz", 200).is_err());
    }
}
