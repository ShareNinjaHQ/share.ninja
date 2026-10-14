//! MC skin theme — parsed from ini into a HashMap.
//!
//! The default skin is embedded via include_str!. External skins can be
//! loaded at runtime from `/opt/homebrew/share/mc/skins/` (or similar).
//! A RwLock allows hot-swapping the active skin.

use ratatui::style::{Color, Modifier, Style};
use std::collections::HashMap;
use std::path::Path;
use std::sync::RwLock;

const DEFAULT_SKIN_INI: &str = include_str!("default.ini");
const MC_SKINS_DIR: &str = "/opt/homebrew/share/mc/skins";

type SkinMap = HashMap<String, HashMap<String, String>>;

struct SkinState {
    map: SkinMap,
    name: String,
}

fn skin_state() -> &'static RwLock<SkinState> {
    use std::sync::OnceLock;
    static STATE: OnceLock<RwLock<SkinState>> = OnceLock::new();
    STATE.get_or_init(|| {
        RwLock::new(SkinState {
            map: parse_ini(DEFAULT_SKIN_INI),
            name: "< default >".to_string(),
        })
    })
}

fn with_skin<R>(f: impl FnOnce(&SkinMap) -> R) -> R {
    let guard = skin_state().read().unwrap();
    f(&guard.map)
}

/// Load the embedded default skin.
pub fn load_default_skin() {
    let mut guard = skin_state().write().unwrap();
    guard.map = parse_ini(DEFAULT_SKIN_INI);
    guard.name = "< default >".to_string();
}

/// Load a skin from a file path. Returns Err if the file cannot be read.
pub fn load_skin_from_file(path: &Path) -> Result<(), String> {
    let content = std::fs::read_to_string(path).map_err(|e| format!("Failed to read skin: {}", e))?;
    let name = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("unknown")
        .to_string();
    let mut guard = skin_state().write().unwrap();
    guard.map = parse_ini(&content);
    guard.name = name;
    Ok(())
}

/// Name of the currently loaded skin.
pub fn current_skin_name() -> String {
    skin_state().read().unwrap().name.clone()
}

/// List available skins: (display_name, Option<path>).
/// The first entry is always "< default >" with None path.
pub fn available_skins() -> Vec<(String, Option<std::path::PathBuf>)> {
    let mut skins = vec![("< default >".to_string(), None)];
    let dir = Path::new(MC_SKINS_DIR);
    if let Ok(entries) = std::fs::read_dir(dir) {
        let mut files: Vec<_> = entries
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("ini"))
            .collect();
        files.sort_by_key(|e| e.file_name());
        for entry in files {
            let path = entry.path();
            let name = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("unknown")
                .to_string();
            skins.push((name, Some(path)));
        }
    }
    skins
}

fn parse_ini(input: &str) -> SkinMap {
    let mut map = SkinMap::new();
    let mut current_section = String::new();
    for line in input.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if line.starts_with('[') && line.ends_with(']') {
            // Normalise section names to lowercase for consistent lookups
            current_section = line[1..line.len() - 1].to_lowercase();
            map.entry(current_section.clone()).or_default();
        } else if let Some((key, value)) = line.split_once('=') {
            let key = key.trim().to_string();
            let value = value.trim().to_string();
            if !value.is_empty() {
                map.entry(current_section.clone()).or_default().insert(key, value);
            }
        }
    }
    map
}

/// A skin colour name to a terminal colour. The 16 classic names map to
/// fixed cells of the 256-colour cube (the console palette FAR paints: dark
/// blue `#000087`, cyan `#008787`, light gray `#bcbcbc`, ...) rather than
/// to the ANSI palette slots, so the panels look the same in every
/// terminal instead of taking the colour scheme's idea of "blue".
fn parse_color(name: &str) -> Color {
    let lower = name.to_lowercase();
    // Handle 256-color "colorNNN" syntax
    if let Some(num_str) = lower.strip_prefix("color")
        && let Ok(n) = num_str.parse::<u8>()
    {
        return Color::Indexed(n);
    }
    match lower.as_str() {
        "default" => Color::Reset,
        "black" => Color::Indexed(16),
        "red" => Color::Indexed(124),
        "green" => Color::Indexed(34),
        "brown" => Color::Indexed(130),
        "yellow" => Color::Indexed(227),
        "blue" => Color::Indexed(18),
        "magenta" => Color::Indexed(127),
        "cyan" => Color::Indexed(30),
        "lightgray" => Color::Indexed(250),
        "white" => Color::Indexed(231),
        "gray" | "darkgray" => Color::Indexed(244),
        "brightred" | "lightred" => Color::Indexed(203),
        "brightgreen" | "lightgreen" => Color::Indexed(83),
        "brightyellow" | "lightyellow" => Color::Indexed(227),
        "brightblue" | "lightblue" => Color::Indexed(63),
        "brightmagenta" | "lightmagenta" => Color::Indexed(207),
        "brightcyan" | "lightcyan" => Color::Indexed(87),
        _ => Color::Reset,
    }
}

/// Look up any section/key from the skin ini, with core._default_ as fallback
pub fn get(section: &str, key: &str) -> Style {
    get_style(section, key).unwrap_or_else(core_default)
}

fn get_style(section: &str, key: &str) -> Option<Style> {
    with_skin(|map| {
        let sec = map.get(section)?;
        let val = sec.get(key)?;
        let parts: Vec<&str> = val.split(';').collect();
        let fg = parts.first().filter(|s| !s.is_empty()).map(|s| parse_color(s));
        let bg = parts.get(1).filter(|s| !s.is_empty()).map(|s| parse_color(s));
        let mut style = Style::default();
        if let Some(fg) = fg {
            style = style.fg(fg);
        }
        if let Some(bg) = bg {
            style = style.bg(bg);
        }
        Some(style)
    })
}

// Helper: resolve a filehighlight entry, inheriting bg from core._default_
fn file_style(key: &str) -> Style {
    let core_bg = get_style("core", "_default_").and_then(|s| s.bg).unwrap_or(Color::Blue);
    get_style("filehighlight", key)
        .map(|s| if s.bg.is_none() { s.bg(core_bg) } else { s })
        .unwrap_or_else(|| Style::default().bg(core_bg))
}

// === Public API — all colors from embedded skin ini ===

// [core]
pub fn core_default() -> Style {
    get("core", "_default_")
}
pub fn core_selected() -> Style {
    get("core", "selected")
}
pub fn core_marked() -> Style {
    get("core", "marked")
}
pub fn core_markselect() -> Style {
    get("core", "markselect")
}
pub fn core_header() -> Style {
    get("core", "header")
}
pub fn core_input() -> Style {
    get("core", "input")
}

// [filehighlight]
pub fn file_directory() -> Style {
    file_style("directory").add_modifier(Modifier::BOLD)
}
pub fn file_symlink() -> Style {
    file_style("symlink")
}
pub fn file_executable() -> Style {
    file_style("executable")
}
pub fn file_regular() -> Style {
    core_default()
}

/// MC's file highlight class of a regular file, by name — the groups of
/// MC's `filehighlight.ini` (`temp`, `archive`, `doc`, `source`, `media`,
/// `graph`, `database`, `core`); `None` = plain.
pub fn file_class(name: &str) -> Option<&'static str> {
    let lower = name.to_lowercase();
    if name == "core" || (lower.starts_with("core.") && lower[5..].chars().all(|c| c.is_ascii_digit())) {
        return Some("core");
    }
    let ext = match lower.rsplit_once('.') {
        Some((stem, ext)) if !stem.is_empty() => ext,
        _ => "",
    };
    if name.starts_with('#')
        || name.ends_with('~')
        || matches!(ext, "swp" | "bak" | "old" | "orig" | "rej" | "pyc" | "pyo" | "tmp")
    {
        return Some("temp");
    }
    const ARCHIVE: &[&str] = &[
        "gz", "bz2", "tar", "tgz", "tbz", "tbz2", "txz", "tzst", "taz", "z", "xz", "lz", "lzma", "lz4", "zst", "zip",
        "rar", "7z", "jar", "war", "ace", "arj", "cab", "cpio", "lha", "lzh", "iso", "dmg", "deb", "rpm", "apk", "ipa",
        "xpi", "whl",
    ];
    const DOC: &[&str] = &[
        "txt", "doc", "docx", "xls", "xlsx", "ppt", "pptx", "odt", "ods", "odp", "abw", "pdf", "rtf", "ps", "eps",
        "dvi", "djvu", "chm", "epub", "mobi", "fb2", "md", "markdown", "rst", "tex", "man", "info", "nfo",
    ];
    const SOURCE: &[&str] = &[
        "c", "h", "cc", "hh", "cpp", "hpp", "cxx", "hxx", "m", "mm", "asm", "s", "awk", "sed", "sh", "bash", "zsh",
        "ksh", "csh", "fish", "pl", "pm", "py", "rb", "php", "lua", "java", "kt", "kts", "scala", "groovy", "js",
        "jsx", "mjs", "cjs", "ts", "tsx", "go", "rs", "swift", "cs", "vb", "el", "lisp", "scm", "clj", "hs", "ml",
        "mli", "erl", "ex", "exs", "f", "f90", "for", "pas", "d", "dart", "r", "jl", "nim", "zig", "v", "vhd", "vhdl",
        "tcl", "sql", "xml", "xsl", "xslt", "html", "htm", "css", "scss", "less", "json", "yaml", "yml", "toml", "ini",
        "cfg", "conf", "mk", "cmake", "in", "am", "ac", "m4", "po", "pot", "diff", "patch", "proto", "gradle", "vue",
        "svelte",
    ];
    const MEDIA: &[&str] = &[
        "mp3", "mp2", "ogg", "oga", "opus", "flac", "wav", "aiff", "aif", "m4a", "aac", "wma", "mid", "midi", "mod",
        "xm", "s3m", "it", "mpg", "mpeg", "mp4", "m4v", "mkv", "avi", "mov", "wmv", "flv", "webm", "ogv", "ogm", "3gp",
        "ts", "vob", "rm", "rmvb", "asf",
    ];
    const GRAPH: &[&str] = &[
        "jpg", "jpeg", "jpe", "png", "gif", "bmp", "tif", "tiff", "svg", "svgz", "webp", "ico", "icns", "xpm", "xbm",
        "pbm", "pgm", "ppm", "pnm", "heic", "heif", "avif", "psd", "xcf", "raw", "cr2", "nef", "dng",
    ];
    const DATABASE: &[&str] = &[
        "db", "db3", "sqlite", "sqlite3", "mdb", "accdb", "dbf", "odb", "fdb", "gdb",
    ];
    let groups: [(&str, &[&str]); 6] = [
        ("archive", ARCHIVE),
        ("doc", DOC),
        ("source", SOURCE),
        ("media", MEDIA),
        ("graph", GRAPH),
        ("database", DATABASE),
    ];
    groups
        .iter()
        .find(|(_, exts)| exts.contains(&ext))
        .map(|(class, _)| *class)
}

/// The row style of a listed object: directory / symlink / executable /
/// MC's name classes / plain.
pub fn file_style_for(name: &str, kind: share_ninja_core::StorageFileType, mode: Option<i64>) -> Style {
    use share_ninja_core::StorageFileType;
    match kind {
        StorageFileType::Dir => file_directory(),
        StorageFileType::Symlink => file_symlink(),
        _ => {
            if mode.is_some_and(|m| m & 0o111 != 0) {
                return file_executable();
            }
            match file_class(name) {
                Some(class) => file_style(class),
                None => file_regular(),
            }
        }
    }
}

// [dialog]
pub fn dialog_default() -> Style {
    get("dialog", "_default_")
}
pub fn dialog_focus() -> Style {
    get("dialog", "dfocus")
}
pub fn dialog_title() -> Style {
    get("dialog", "dtitle")
}
pub fn dialog_hot() -> Style {
    get("dialog", "dhotnormal")
}

// [error]
pub fn error_default() -> Style {
    get("error", "_default_")
}
pub fn error_title() -> Style {
    get("error", "errdtitle")
}
pub fn error_focus() -> Style {
    get("error", "errdfocus")
}
pub fn error_hot() -> Style {
    get("error", "errdhotnormal")
}
pub fn error_hot_focus() -> Style {
    get("error", "errdhotfocus")
}

// [core]
pub fn shadow() -> Style {
    get("core", "shadow")
}

// [buttonbar]
pub fn buttonbar_hotkey() -> Style {
    get("buttonbar", "hotkey")
}
pub fn buttonbar_button() -> Style {
    get("buttonbar", "button")
}

// [statusbar]
pub fn statusbar_default() -> Style {
    get("statusbar", "_default_")
}

// Panel border: fg from core.selected bg, bg from core._default_ bg
pub fn panel_border() -> Style {
    let sel = core_selected();
    let def = core_default();
    Style::default()
        .fg(sel.bg.unwrap_or(def.fg.unwrap_or(Color::Reset)))
        .bg(def.bg.unwrap_or(Color::Reset))
}

// Active title: core.selected + bold
pub fn panel_title_active() -> Style {
    core_selected().add_modifier(Modifier::BOLD)
}

// Inactive title: same as core._default_
pub fn panel_title_inactive() -> Style {
    core_default()
}

// [viewer]
pub fn viewer_default() -> Style {
    get("viewer", "_default_")
}

/// The viewer's syntax colours, `[syntax]`: a foreground per class. MC's
/// skins have no such section (its syntax files carry the colours), so a
/// skin without one gets the embedded default's.
pub fn syntax(class: crate::syntax::Class) -> Style {
    use crate::syntax::Class;
    let key = match class {
        Class::Comment => "comment",
        Class::Str => "string",
        Class::Keyword => "keyword",
        Class::Number => "number",
        Class::Plain | Class::Match => return Style::default(),
    };
    get_style("syntax", key).unwrap_or_else(|| default_skin_style("syntax", key).unwrap_or_default())
}

/// A style off the embedded default skin, whatever skin is loaded.
fn default_skin_style(section: &str, key: &str) -> Option<Style> {
    use std::sync::OnceLock;
    static DEFAULT: OnceLock<SkinMap> = OnceLock::new();
    let map = DEFAULT.get_or_init(|| parse_ini(DEFAULT_SKIN_INI));
    let val = map.get(section)?.get(key)?;
    let fg = val.split(';').next().filter(|s| !s.is_empty())?;
    Some(Style::default().fg(parse_color(fg)))
}
pub fn viewer_bold() -> Style {
    get("viewer", "viewbold")
}
pub fn viewer_underline() -> Style {
    get("viewer", "viewunderline")
}
pub fn viewer_selected() -> Style {
    get("viewer", "viewselected")
}

// [menu]
pub fn menu_default() -> Style {
    get("menu", "_default_")
}
pub fn menu_selected() -> Style {
    get("menu", "menusel")
}

// Raw string lookup (for [lines] characters etc.)
fn get_raw(section: &str, key: &str) -> Option<String> {
    with_skin(|map| map.get(section)?.get(key).cloned())
}

// [lines] — box-drawing characters from the skin
pub fn line_horiz() -> String {
    get_raw("lines", "horiz").unwrap_or_else(|| "─".to_string())
}
pub fn line_vert() -> String {
    get_raw("lines", "vert").unwrap_or_else(|| "│".to_string())
}
pub fn line_lefttop() -> String {
    get_raw("lines", "lefttop").unwrap_or_else(|| "┌".to_string())
}
pub fn line_righttop() -> String {
    get_raw("lines", "righttop").unwrap_or_else(|| "┐".to_string())
}
pub fn line_leftbottom() -> String {
    get_raw("lines", "leftbottom").unwrap_or_else(|| "└".to_string())
}
pub fn line_rightbottom() -> String {
    get_raw("lines", "rightbottom").unwrap_or_else(|| "┘".to_string())
}
pub fn line_leftmiddle() -> String {
    get_raw("lines", "leftmiddle").unwrap_or_else(|| "├".to_string())
}
pub fn line_rightmiddle() -> String {
    get_raw("lines", "rightmiddle").unwrap_or_else(|| "┤".to_string())
}
pub fn line_topmiddle() -> String {
    get_raw("lines", "topmiddle").unwrap_or_else(|| "┬".to_string())
}
pub fn line_bottommiddle() -> String {
    get_raw("lines", "bottommiddle").unwrap_or_else(|| "┴".to_string())
}
