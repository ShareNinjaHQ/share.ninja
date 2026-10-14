//! The viewer's pretty-printers for one-line JSON and XML/HTML: streaming,
//! whitespace only — every content byte stays as in the file (numbers are
//! not re-serialized, keys keep their order); what changes is the
//! whitespace between tokens. Where the input stops making sense the rest
//! is copied through untouched and the offset reported.

const INDENT: &[u8] = b"  ";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Formatter {
    Json,
    Xml,
}

impl Formatter {
    /// The formatter for `path`, by its extension.
    pub fn by_path(path: &str) -> Option<Formatter> {
        let ext = std::path::Path::new(path).extension()?.to_str()?.to_ascii_lowercase();
        match ext.as_str() {
            "json" => Some(Formatter::Json),
            "xml" | "html" | "htm" | "xhtml" | "svg" | "xsl" | "xslt" | "plist" | "vue" => Some(Formatter::Xml),
            _ => None,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Formatter::Json => "JSON",
            Formatter::Xml => "XML",
        }
    }
}

/// A formatter's output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Formatted {
    pub bytes: Vec<u8>,
    /// Where the input stopped making sense; from there it is copied
    /// through as it is.
    pub error_at: Option<u64>,
}

pub fn format(formatter: Formatter, input: &[u8]) -> Formatted {
    match formatter {
        Formatter::Json => json(input),
        Formatter::Xml => xml(input),
    }
}

fn newline(out: &mut Vec<u8>, depth: usize) {
    out.push(b'\n');
    for _ in 0..depth {
        out.extend_from_slice(INDENT);
    }
}

/// JSON: one member per line, `: ` after a key, empty `{}` / `[]` kept as
/// they are. Strings are copied whole, escapes included. A closer with
/// nothing open, or a string with no end, is where formatting stops.
fn json(input: &[u8]) -> Formatted {
    let mut out = Vec::with_capacity(input.len() + input.len() / 2);
    let mut depth = 0usize;
    let mut i = 0;
    let n = input.len();
    while i < n {
        let b = input[i];
        match b {
            b' ' | b'\t' | b'\r' | b'\n' => i += 1,
            b'"' => {
                let Some(end) = json_string_end(input, i) else {
                    return passthrough(out, input, i);
                };
                out.extend_from_slice(&input[i..end]);
                i = end;
            }
            b'{' | b'[' => {
                out.push(b);
                i += 1;
                // An empty container stays on its line.
                let next = input[i..].iter().position(|c| !c.is_ascii_whitespace()).map(|p| i + p);
                if let Some(j) = next
                    && input[j] == (if b == b'{' { b'}' } else { b']' })
                {
                    out.push(input[j]);
                    i = j + 1;
                    continue;
                }
                depth += 1;
                newline(&mut out, depth);
            }
            b'}' | b']' => {
                if depth == 0 {
                    return passthrough(out, input, i);
                }
                depth -= 1;
                newline(&mut out, depth);
                out.push(b);
                i += 1;
            }
            b',' => {
                out.push(b);
                newline(&mut out, depth);
                i += 1;
            }
            b':' => {
                out.extend_from_slice(b": ");
                i += 1;
            }
            _ => {
                out.push(b);
                i += 1;
            }
        }
    }
    finish(out, None)
}

/// Past the closing quote of the string opening at `at`, or `None` at EOF.
fn json_string_end(input: &[u8], at: usize) -> Option<usize> {
    let mut i = at + 1;
    while i < input.len() {
        match input[i] {
            b'\\' => i += 2,
            b'"' => return Some(i + 1),
            _ => i += 1,
        }
    }
    None
}

/// HTML elements with no closing tag: they open nothing.
const VOID: &[&str] = &[
    "area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "param", "source", "track", "wbr",
];

/// XML / HTML: one tag per line indented by depth, text nodes trimmed onto
/// their own line; comments, CDATA, processing instructions and doctypes
/// pass through whole. A tag with no `>` is where formatting stops.
fn xml(input: &[u8]) -> Formatted {
    let mut out = Vec::with_capacity(input.len() + input.len() / 2);
    let mut depth = 0usize;
    let mut i = 0;
    let n = input.len();
    let mut first = true;
    let mut line = |out: &mut Vec<u8>, depth: usize| {
        if first {
            first = false;
            for _ in 0..depth {
                out.extend_from_slice(INDENT);
            }
        } else {
            newline(out, depth);
        }
    };
    while i < n {
        if input[i] != b'<' {
            // Text up to the next tag, trimmed.
            let end = input[i..].iter().position(|b| *b == b'<').map_or(n, |p| i + p);
            let text = trim(&input[i..end]);
            if !text.is_empty() {
                line(&mut out, depth);
                out.extend_from_slice(text);
            }
            i = end;
            continue;
        }
        let Some(end) = tag_end(input, i) else {
            return passthrough(out, input, i);
        };
        let tag = &input[i..end];
        let closing = tag.starts_with(b"</");
        let special = tag.starts_with(b"<!") || tag.starts_with(b"<?");
        let self_closing = tag.ends_with(b"/>") || (!closing && !special && VOID.contains(&tag_name(tag).as_str()));
        if closing {
            depth = depth.saturating_sub(1);
        }
        line(&mut out, depth);
        out.extend_from_slice(tag);
        if !closing && !special && !self_closing {
            depth += 1;
        }
        i = end;
    }
    finish(out, None)
}

/// Past the `>` of the tag opening at `at`: quotes inside an ordinary tag
/// hide a `>`, a comment / CDATA runs to its own terminator.
fn tag_end(input: &[u8], at: usize) -> Option<usize> {
    let rest = &input[at..];
    let terminator: &[u8] = if rest.starts_with(b"<!--") {
        b"-->"
    } else if rest.starts_with(b"<![CDATA[") {
        b"]]>"
    } else {
        b">"
    };
    if terminator.len() > 1 {
        let skip = if terminator == b"-->" { 4 } else { 9 };
        return rest[skip..]
            .windows(terminator.len())
            .position(|w| w == terminator)
            .map(|p| at + skip + p + terminator.len());
    }
    let mut quote: Option<u8> = None;
    for (j, b) in rest.iter().enumerate().skip(1) {
        match quote {
            Some(q) if *b == q => quote = None,
            Some(_) => {}
            None => match *b {
                b'"' | b'\'' => quote = Some(*b),
                b'>' => return Some(at + j + 1),
                _ => {}
            },
        }
    }
    None
}

/// The lower-cased element name of an opening tag (`<br class=x>` → `br`).
fn tag_name(tag: &[u8]) -> String {
    tag[1..]
        .iter()
        .take_while(|b| b.is_ascii_alphanumeric() || **b == b'-' || **b == b':')
        .map(|b| b.to_ascii_lowercase() as char)
        .collect()
}

fn trim(bytes: &[u8]) -> &[u8] {
    let start = bytes.iter().position(|b| !b.is_ascii_whitespace()).unwrap_or(bytes.len());
    let end = bytes.iter().rposition(|b| !b.is_ascii_whitespace()).map_or(start, |p| p + 1);
    &bytes[start..end]
}

/// Formatting stops at `at`: the rest as it is, on a line of its own (an
/// indent already begun is kept).
fn passthrough(mut out: Vec<u8>, input: &[u8], at: usize) -> Formatted {
    let indent = out.iter().rev().take_while(|b| **b == b' ').count();
    let before = out.len() - indent;
    if before > 0 && out[before - 1] != b'\n' {
        out.push(b'\n');
    }
    out.extend_from_slice(&input[at..]);
    finish(out, Some(at as u64))
}

fn finish(mut out: Vec<u8>, error_at: Option<u64>) -> Formatted {
    if !out.is_empty() && out.last() != Some(&b'\n') {
        out.push(b'\n');
    }
    Formatted { bytes: out, error_at }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(f: &Formatted) -> String {
        String::from_utf8(f.bytes.clone()).unwrap()
    }

    #[test]
    fn json_one_liner() {
        let f = json(br#"{"a":1,"b":[true,{"c":"x,\"y\":z"}],"e":{},"f":[]}"#);
        assert_eq!(
            s(&f),
            "{\n  \"a\": 1,\n  \"b\": [\n    true,\n    {\n      \"c\": \"x,\\\"y\\\":z\"\n    }\n  ],\n  \"e\": {},\n  \"f\": []\n}\n"
        );
        assert_eq!(f.error_at, None);
    }

    #[test]
    fn json_stops_at_a_stray_closer_and_keeps_the_rest() {
        let f = json(b"[1]] tail");
        assert_eq!(s(&f), "[\n  1\n]\n] tail\n");
        assert_eq!(f.error_at, Some(3));
        let f = json(b"{\"open");
        assert_eq!(f.error_at, Some(1));
        assert_eq!(s(&f), "{\n  \"open\n");
    }

    #[test]
    fn xml_one_liner() {
        let f = xml(b"<?xml version=\"1.0\"?><a x=\"1>2\"><b>t</b><br><!-- c --><c/></a>trail");
        assert_eq!(
            s(&f),
            "<?xml version=\"1.0\"?>\n<a x=\"1>2\">\n  <b>\n    t\n  </b>\n  <br>\n  <!-- c -->\n  <c/>\n</a>\ntrail\n"
        );
        assert_eq!(f.error_at, None);
    }

    #[test]
    fn xml_unterminated_tag_passes_through() {
        let f = xml(b"<a><b attr=\"x");
        assert_eq!(s(&f), "<a>\n<b attr=\"x\n");
        assert_eq!(f.error_at, Some(3));
    }

    #[test]
    fn formatter_by_extension() {
        assert_eq!(Formatter::by_path("a/b.JSON"), Some(Formatter::Json));
        assert_eq!(Formatter::by_path("x.html"), Some(Formatter::Xml));
        assert_eq!(Formatter::by_path("x.py"), None);
    }
}
