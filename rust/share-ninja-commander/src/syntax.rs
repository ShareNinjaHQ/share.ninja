//! Syntax classes for the viewer's text rows: a per-line lexer over a
//! per-language spec that is plain data — comment markers, string quotes,
//! keywords, numbers — the language picked by the file's extension. The
//! lexer's state (an open block comment or multi-line string) carries from
//! line to line within one layout, from the viewport's top: what opened
//! above the viewport is not seen and stays plain until it closes.

use std::path::Path;

/// What a run of a row is drawn as. `Match` is the search hit and wins
/// over the syntax classes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Class {
    #[default]
    Plain,
    Match,
    Comment,
    Str,
    Keyword,
    Number,
}

/// A string kind: its opening and closing marks, whether a backslash
/// escapes the next char inside, whether it may run across lines.
#[derive(Clone, Copy, Debug)]
pub struct Quote {
    pub open: &'static str,
    pub close: &'static str,
    pub escapes: bool,
    pub multiline: bool,
}

const fn q(open: &'static str, close: &'static str, escapes: bool, multiline: bool) -> Quote {
    Quote {
        open,
        close,
        escapes,
        multiline,
    }
}

pub struct Language {
    pub line_comments: &'static [&'static str],
    pub block_comment: Option<(&'static str, &'static str)>,
    /// Longest opening mark first where one is a prefix of another.
    pub quotes: &'static [Quote],
    pub keywords: &'static [&'static str],
    pub numbers: bool,
    /// Markdown: `#` heading lines and the ``` fenced blocks, nothing else.
    pub markdown: bool,
    /// Markup (HTML, XML): tags with their attribute strings, `<!-- -->`
    /// comments; the text between tags is plain.
    pub markup: bool,
}

/// The lexer between lines.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum State {
    #[default]
    Normal,
    BlockComment,
    /// Inside `Language::quotes[i]`.
    Quoted(usize),
    /// Markup: inside a tag, after its name.
    Tag,
}

const NONE: Language = Language {
    line_comments: &[],
    block_comment: None,
    quotes: &[],
    keywords: &[],
    numbers: false,
    markdown: false,
    markup: false,
};

const DQ: Quote = q("\"", "\"", true, false);
const SQ: Quote = q("'", "'", true, false);
const C_LIKE: &[&str] = &["//"];
const C_BLOCK: Option<(&str, &str)> = Some(("/*", "*/"));

pub static JSON: Language = Language {
    quotes: &[DQ],
    keywords: &["true", "false", "null"],
    numbers: true,
    ..NONE
};

pub static PYTHON: Language = Language {
    line_comments: &["#"],
    quotes: &[q("\"\"\"", "\"\"\"", true, true), q("'''", "'''", true, true), DQ, SQ],
    keywords: &[
        "False", "None", "True", "and", "as", "assert", "async", "await", "break", "class", "continue", "def",
        "del", "elif", "else", "except", "finally", "for", "from", "global", "if", "import", "in", "is", "lambda",
        "nonlocal", "not", "or", "pass", "raise", "return", "try", "while", "with", "yield", "self",
    ],
    numbers: true,
    ..NONE
};

pub static RUST: Language = Language {
    line_comments: C_LIKE,
    block_comment: C_BLOCK,
    quotes: &[q("\"", "\"", true, true)],
    keywords: &[
        "as", "async", "await", "break", "const", "continue", "crate", "dyn", "else", "enum", "extern", "false",
        "fn", "for", "if", "impl", "in", "let", "loop", "match", "mod", "move", "mut", "pub", "ref", "return",
        "self", "Self", "static", "struct", "super", "trait", "true", "type", "unsafe", "use", "where", "while",
    ],
    numbers: true,
    ..NONE
};

pub static JAVASCRIPT: Language = Language {
    line_comments: C_LIKE,
    block_comment: C_BLOCK,
    quotes: &[DQ, SQ, q("`", "`", true, true)],
    keywords: &[
        "async", "await", "break", "case", "catch", "class", "const", "continue", "debugger", "default", "delete",
        "do", "else", "enum", "export", "extends", "false", "finally", "for", "function", "if", "implements",
        "import", "in", "instanceof", "interface", "let", "new", "null", "of", "return", "static", "super",
        "switch", "this", "throw", "true", "try", "type", "typeof", "undefined", "var", "void", "while", "with",
        "yield",
    ],
    numbers: true,
    ..NONE
};

pub static SHELL: Language = Language {
    line_comments: &["#"],
    quotes: &[DQ, q("'", "'", false, true)],
    keywords: &[
        "if", "then", "else", "elif", "fi", "for", "while", "until", "do", "done", "case", "esac", "in", "function",
        "select", "return", "exit", "local", "export", "readonly", "declare", "set", "unset", "shift", "source",
        "alias", "break", "continue",
    ],
    numbers: false,
    ..NONE
};

pub static TOML: Language = Language {
    line_comments: &["#"],
    quotes: &[q("\"\"\"", "\"\"\"", true, true), q("'''", "'''", false, true), DQ, q("'", "'", false, false)],
    keywords: &["true", "false"],
    numbers: true,
    ..NONE
};

pub static YAML: Language = Language {
    line_comments: &["#"],
    quotes: &[DQ, q("'", "'", false, false)],
    keywords: &["true", "false", "null", "yes", "no"],
    numbers: true,
    ..NONE
};

pub static C: Language = Language {
    line_comments: C_LIKE,
    block_comment: C_BLOCK,
    quotes: &[DQ, SQ],
    keywords: &[
        "auto", "bool", "break", "case", "char", "class", "const", "constexpr", "continue", "default", "delete",
        "do", "double", "else", "enum", "explicit", "extern", "false", "float", "for", "goto", "if", "inline",
        "int", "long", "namespace", "new", "nullptr", "operator", "private", "protected", "public", "return",
        "short", "signed", "sizeof", "static", "struct", "switch", "template", "this", "throw", "true", "try",
        "typedef", "typename", "union", "unsigned", "using", "virtual", "void", "volatile", "while",
    ],
    numbers: true,
    ..NONE
};

pub static GO: Language = Language {
    line_comments: C_LIKE,
    block_comment: C_BLOCK,
    quotes: &[DQ, q("`", "`", false, true), SQ],
    keywords: &[
        "break", "case", "chan", "const", "continue", "default", "defer", "else", "fallthrough", "for", "func",
        "go", "goto", "if", "import", "interface", "map", "package", "range", "return", "select", "struct",
        "switch", "type", "var", "true", "false", "nil",
    ],
    numbers: true,
    ..NONE
};

pub static MARKDOWN: Language = Language {
    markdown: true,
    ..NONE
};

/// HTML and XML alike: the attribute quotes take no escapes and may run
/// across lines, as may a tag.
pub static MARKUP: Language = Language {
    block_comment: Some(("<!--", "-->")),
    quotes: &[q("\"", "\"", false, true), q("'", "'", false, true)],
    markup: true,
    ..NONE
};

/// The language of `path`, by its extension.
pub fn by_path(path: &str) -> Option<&'static Language> {
    let ext = Path::new(path).extension()?.to_str()?.to_ascii_lowercase();
    Some(match ext.as_str() {
        "json" => &JSON,
        "py" | "pyi" => &PYTHON,
        "rs" => &RUST,
        "js" | "mjs" | "cjs" | "jsx" | "ts" | "tsx" => &JAVASCRIPT,
        "sh" | "bash" | "zsh" => &SHELL,
        "toml" => &TOML,
        "yaml" | "yml" => &YAML,
        "c" | "h" | "cc" | "cpp" | "cxx" | "hh" | "hpp" => &C,
        "go" => &GO,
        "md" | "markdown" => &MARKDOWN,
        "html" | "htm" | "xhtml" | "xml" | "svg" | "xsl" | "xslt" | "plist" | "vue" => &MARKUP,
        _ => return None,
    })
}

/// One line's chars classified, the lexer carrying `state` in and out.
pub fn classes(lang: &Language, chars: &[char], state: &mut State) -> Vec<Class> {
    let n = chars.len();
    let mut out = vec![Class::Plain; n];
    if lang.markdown {
        markdown_line(chars, &mut out, state);
        return out;
    }
    if lang.markup {
        markup_line(lang, chars, &mut out, state);
        return out;
    }
    let mut i = 0;
    while i < n {
        match *state {
            State::BlockComment => {
                let close = lang.block_comment.map(|(_, c)| c).unwrap_or("");
                if !close.is_empty() && starts_with(chars, i, close) {
                    fill(&mut out, i, close.chars().count(), Class::Comment);
                    i += close.chars().count();
                    *state = State::Normal;
                } else {
                    out[i] = Class::Comment;
                    i += 1;
                }
            }
            State::Quoted(k) => {
                let quote = lang.quotes[k];
                if quote.escapes && chars[i] == '\\' {
                    fill(&mut out, i, 2, Class::Str);
                    i += 2;
                } else if starts_with(chars, i, quote.close) {
                    let len = quote.close.chars().count();
                    fill(&mut out, i, len, Class::Str);
                    i += len;
                    *state = State::Normal;
                } else {
                    out[i] = Class::Str;
                    i += 1;
                }
            }
            // `Tag` is the markup lexer's; here it reads as normal text.
            State::Normal | State::Tag => {
                if lang.line_comments.iter().any(|m| starts_with(chars, i, m)) {
                    fill(&mut out, i, n - i, Class::Comment);
                    break;
                }
                if let Some((open, _)) = lang.block_comment
                    && starts_with(chars, i, open)
                {
                    let len = open.chars().count();
                    fill(&mut out, i, len, Class::Comment);
                    i += len;
                    *state = State::BlockComment;
                    continue;
                }
                if let Some(k) = lang.quotes.iter().position(|q| starts_with(chars, i, q.open)) {
                    let len = lang.quotes[k].open.chars().count();
                    fill(&mut out, i, len, Class::Str);
                    i += len;
                    *state = State::Quoted(k);
                    continue;
                }
                let c = chars[i];
                let after_word = i > 0 && is_word(chars[i - 1]);
                if is_word_start(c) && !after_word {
                    let end = (i..n).find(|&j| !is_word(chars[j])).unwrap_or(n);
                    let word: String = chars[i..end].iter().collect();
                    if lang.keywords.contains(&word.as_str()) {
                        fill(&mut out, i, end - i, Class::Keyword);
                    }
                    i = end;
                    continue;
                }
                if lang.numbers && c.is_ascii_digit() && !after_word {
                    let end = (i..n).find(|&j| !(is_word(chars[j]) || chars[j] == '.')).unwrap_or(n);
                    fill(&mut out, i, end - i, Class::Number);
                    i = end;
                    continue;
                }
                i += 1;
            }
        }
    }
    // A one-line string left open ends with its line.
    if let State::Quoted(k) = *state
        && !lang.quotes[k].multiline
    {
        *state = State::Normal;
    }
    out
}

/// Markdown: a heading line is a keyword, a ``` fence line and the lines
/// between two fences are a comment (a code region).
fn markdown_line(chars: &[char], out: &mut [Class], state: &mut State) {
    let n = chars.len();
    if starts_with(chars, 0, "```") {
        fill(out, 0, n, Class::Comment);
        *state = match *state {
            State::BlockComment => State::Normal,
            _ => State::BlockComment,
        };
        return;
    }
    match *state {
        State::BlockComment => fill(out, 0, n, Class::Comment),
        _ if chars.first() == Some(&'#') => fill(out, 0, n, Class::Keyword),
        _ => {}
    }
}

/// Markup: `<name` / `</name` / `<?xml` / `<!DOCTYPE` and the closing `>`
/// or `/>` are keywords, quoted attribute values strings, `<!-- -->` a
/// comment; the text between tags is plain. A tag or a quoted value may
/// run across lines.
fn markup_line(lang: &Language, chars: &[char], out: &mut [Class], state: &mut State) {
    let n = chars.len();
    let mut i = 0;
    while i < n {
        match *state {
            State::BlockComment => {
                let close = lang.block_comment.map(|(_, c)| c).unwrap_or("");
                if !close.is_empty() && starts_with(chars, i, close) {
                    fill(out, i, close.chars().count(), Class::Comment);
                    i += close.chars().count();
                    *state = State::Normal;
                } else {
                    out[i] = Class::Comment;
                    i += 1;
                }
            }
            State::Quoted(k) => {
                let close = lang.quotes[k].close;
                if starts_with(chars, i, close) {
                    fill(out, i, close.chars().count(), Class::Str);
                    i += close.chars().count();
                    *state = State::Tag;
                } else {
                    out[i] = Class::Str;
                    i += 1;
                }
            }
            State::Tag => {
                if let Some(k) = lang.quotes.iter().position(|q| starts_with(chars, i, q.open)) {
                    let len = lang.quotes[k].open.chars().count();
                    fill(out, i, len, Class::Str);
                    i += len;
                    *state = State::Quoted(k);
                } else if chars[i] == '>' {
                    // `/>` and `?>` close with their slash / question mark.
                    let from = if i > 0 && matches!(chars[i - 1], '/' | '?') { i - 1 } else { i };
                    fill(out, from, i + 1 - from, Class::Keyword);
                    i += 1;
                    *state = State::Normal;
                } else {
                    i += 1;
                }
            }
            _ => {
                if let Some((open, _)) = lang.block_comment
                    && starts_with(chars, i, open)
                {
                    let len = open.chars().count();
                    fill(out, i, len, Class::Comment);
                    i += len;
                    *state = State::BlockComment;
                    continue;
                }
                if chars[i] == '<' && chars.get(i + 1).is_some_and(|c| c.is_alphabetic() || matches!(c, '/' | '?' | '!')) {
                    let mut end = i + 1;
                    if matches!(chars[end], '/' | '?' | '!') {
                        end += 1;
                    }
                    while end < n && (chars[end].is_alphanumeric() || matches!(chars[end], '-' | ':' | '_' | '.')) {
                        end += 1;
                    }
                    fill(out, i, end - i, Class::Keyword);
                    i = end;
                    *state = State::Tag;
                    continue;
                }
                i += 1;
            }
        }
    }
}

fn starts_with(chars: &[char], at: usize, mark: &str) -> bool {
    mark.chars().enumerate().all(|(j, m)| chars.get(at + j) == Some(&m))
}

fn fill(out: &mut [Class], at: usize, len: usize, class: Class) {
    let end = (at + len).min(out.len());
    for c in &mut out[at..end] {
        *c = class;
    }
}

fn is_word_start(c: char) -> bool {
    c.is_alphabetic() || c == '_'
}

fn is_word(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(lang: &Language, line: &str, state: &mut State) -> String {
        let chars: Vec<char> = line.chars().collect();
        classes(lang, &chars, state)
            .iter()
            .map(|c| match c {
                Class::Plain => '.',
                Class::Match => 'M',
                Class::Comment => 'c',
                Class::Str => 's',
                Class::Keyword => 'k',
                Class::Number => 'n',
            })
            .collect()
    }

    #[test]
    fn python_keywords_strings_numbers_comments() {
        let mut st = State::default();
        assert_eq!(run(&PYTHON, "if x == \"a#b\": # c", &mut st), "kk......sssss..ccc");
        assert_eq!(run(&PYTHON, "elif_x = 0x1f + 3.5", &mut st), ".........nnnn...nnn");
        assert_eq!(st, State::Normal);
    }

    #[test]
    fn triple_quotes_carry_across_lines_and_one_line_quotes_do_not() {
        let mut st = State::default();
        assert_eq!(run(&PYTHON, "s = \"\"\"doc", &mut st), "....ssssss");
        assert_eq!(st, State::Quoted(0));
        assert_eq!(run(&PYTHON, "end\"\"\" x", &mut st), "ssssss..");
        assert_eq!(st, State::Normal);
        assert_eq!(run(&PYTHON, "t = 'open", &mut st), "....sssss");
        assert_eq!(st, State::Normal);
    }

    #[test]
    fn block_comments_and_escapes() {
        let mut st = State::default();
        assert_eq!(run(&RUST, "let a = \"q\\\"x\"; /* c", &mut st), "kkk.....ssssss..cccc");
        assert_eq!(st, State::BlockComment);
        assert_eq!(run(&RUST, "more */ fn", &mut st), "ccccccc.kk");
        assert_eq!(st, State::Normal);
    }

    #[test]
    fn json_and_shell() {
        let mut st = State::default();
        assert_eq!(run(&JSON, "{\"k\": [true, 12]}", &mut st), ".sss...kkkk..nn..");
        let mut st = State::default();
        assert_eq!(run(&SHELL, "if [ -f '$x' ]; then # y", &mut st), "kk......ssss....kkkk.ccc");
    }

    #[test]
    fn markdown_headings_and_fences() {
        let mut st = State::default();
        assert_eq!(run(&MARKDOWN, "# Title", &mut st), "kkkkkkk");
        assert_eq!(run(&MARKDOWN, "```rust", &mut st), "ccccccc");
        assert_eq!(run(&MARKDOWN, "let x;", &mut st), "cccccc");
        assert_eq!(run(&MARKDOWN, "```", &mut st), "ccc");
        assert_eq!(run(&MARKDOWN, "text", &mut st), "....");
    }

    #[test]
    fn markup_tags_attributes_comments_and_text() {
        let mut st = State::default();
        assert_eq!(run(&MARKUP, "<a href=\"x\">it's</a> <!-- c", &mut st), "kk......sssk....kkkk.cccccc");
        assert_eq!(st, State::BlockComment);
        assert_eq!(run(&MARKUP, "end --> <br/>", &mut st), "ccccccc.kkkkk");
        assert_eq!(st, State::Normal);
        // A tag across lines, its attribute value too.
        assert_eq!(run(&MARKUP, "<?xml version='1.0", &mut st), "kkkkk.........ssss");
        assert_eq!(st, State::Quoted(1));
        assert_eq!(run(&MARKUP, "' ?>", &mut st), "s.kk");
        assert_eq!(st, State::Normal);
    }

    #[test]
    fn languages_by_extension() {
        assert!(std::ptr::eq(by_path("a/b.PY").unwrap(), &PYTHON));
        assert!(std::ptr::eq(by_path("x.tsx").unwrap(), &JAVASCRIPT));
        assert!(std::ptr::eq(by_path("index.html").unwrap(), &MARKUP));
        assert!(std::ptr::eq(by_path("pom.xml").unwrap(), &MARKUP));
        assert!(by_path("README").is_none());
        assert!(by_path("bin.exe").is_none());
    }
}
