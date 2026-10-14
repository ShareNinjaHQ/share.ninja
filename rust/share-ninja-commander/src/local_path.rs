//! Local paths as the panels see them: a local storage is opened on a
//! **volume root** (`/` on unix, `C:\` on Windows — the storage's id) and
//! addresses everything under it by a `/`-separated **key**. These helpers
//! split, join and show such paths natively per platform.

use std::path::{Component, MAIN_SEPARATOR_STR, Path, PathBuf};

/// Split an absolute local path into its volume root and the key inside it:
/// `/Users/me/x` → (`/`, `Users/me/x`); `C:\Users\me` → (`C:\`, `Users/me`).
pub fn split(path: &Path) -> (String, String) {
    let mut root = PathBuf::new();
    let mut parts: Vec<String> = Vec::new();
    for c in path.components() {
        match c {
            Component::Prefix(_) | Component::RootDir => root.push(c.as_os_str()),
            Component::CurDir | Component::ParentDir => {}
            Component::Normal(n) => parts.push(n.to_string_lossy().into_owned()),
        }
    }
    if root.as_os_str().is_empty() {
        root.push(MAIN_SEPARATOR_STR);
    }
    (root.to_string_lossy().into_owned(), parts.join("/"))
}

/// The volume the local panels start on and the home directory's key in
/// it (the volume root itself when there is no home).
pub fn home_volume() -> (String, String) {
    let home = dirs::home_dir().unwrap_or_else(|| PathBuf::from(MAIN_SEPARATOR_STR));
    split(&home)
}

/// The path of `key` on the volume rooted at `root`.
pub fn join(root: &str, key: &str) -> PathBuf {
    if key.is_empty() {
        PathBuf::from(root)
    } else {
        Path::new(root).join(key)
    }
}

/// The `/`-separated key of a path relative to its volume root.
pub fn key(rel: &Path) -> String {
    rel.components()
        .filter_map(|c| match c {
            Component::Normal(n) => Some(n.to_string_lossy().into_owned()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("/")
}

/// A local path as the user reads it: native separators; `~`-shortened on
/// unix (Windows shows the drive path in full, as its shells do).
pub fn display(path: &Path) -> String {
    let s = path.to_string_lossy();
    if cfg!(windows) {
        return s.replace('/', "\\");
    }
    if let Some(home) = dirs::home_dir() {
        let home = home.to_string_lossy();
        if s == home {
            return "~".to_string();
        }
        if let Some(suffix) = s.strip_prefix(home.as_ref())
            && suffix.starts_with('/')
        {
            return format!("~{}", suffix);
        }
    }
    s.into_owned()
}

/// The command line prompt for commands running in `cwd`: `~/dir$ ` on
/// unix, `C:\dir>` on Windows.
pub fn prompt(cwd: &Path) -> String {
    if cfg!(windows) {
        format!("{}>", display(cwd))
    } else {
        format!("{}$ ", display(cwd))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_and_key_roundtrip() {
        if cfg!(windows) {
            assert_eq!(
                split(Path::new(r"C:\Users\me")),
                ("C:\\".to_string(), "Users/me".to_string())
            );
            assert_eq!(split(Path::new(r"D:\")), ("D:\\".to_string(), String::new()));
            assert_eq!(key(Path::new(r"a\b\c")), "a/b/c");
            assert_eq!(display(&join("C:\\", "Users/me")), r"C:\Users\me");
            assert_eq!(display(&join("C:\\", "")), r"C:\");
        } else {
            assert_eq!(split(Path::new("/Users/me")), ("/".to_string(), "Users/me".to_string()));
            assert_eq!(split(Path::new("/")), ("/".to_string(), String::new()));
            assert_eq!(join("/", "Users/me"), PathBuf::from("/Users/me"));
            assert_eq!(join("/", ""), PathBuf::from("/"));
        }
        assert_eq!(key(Path::new("a/b/c")), "a/b/c");
        assert_eq!(key(Path::new("")), "");
    }
}
