//! Selection semantics for partial downloads (docs/core/PartialDownloads.md):
//! an include-list of share-relative keys. `None` = the whole share (including
//! files the uploader adds later); a dir entry covers everything under it,
//! also including later arrivals; a file entry covers only itself. There is no
//! explicit exclude — the UI expands "everything but X" into includes.

/// Whether selection root `root` covers `key` — boundary-safe prefix match:
/// `a/b` covers `a/b` and `a/b/c`, but not `a/bc`.
pub fn covers(root: &str, key: &str) -> bool {
    key == root || key.strip_prefix(root).is_some_and(|rest| rest.starts_with('/'))
}

/// Whether a normalized selection covers `key`. `None` covers everything;
/// an empty list covers nothing.
pub fn selection_covers(selection: Option<&[String]>, key: &str) -> bool {
    match selection {
        None => true,
        Some(roots) => roots.iter().any(|r| covers(r, key)),
    }
}

/// Canonical form applied on every selection write, before persist and before
/// handing it to the engine: trim trailing slashes, drop duplicates and any
/// entry covered by another entry (ancestor wins), sort. An entry for the
/// share root (`""`) collapses the whole selection to `None` (whole share).
/// `Some(vec![])` stays as-is — a valid "nothing selected".
pub fn normalize_selection(selection: Option<Vec<String>>) -> Option<Vec<String>> {
    let roots = selection?;
    let mut cleaned: Vec<String> = Vec::with_capacity(roots.len());
    for r in roots {
        let r = r.trim_end_matches('/');
        if r.is_empty() {
            return None;
        }
        cleaned.push(r.to_string());
    }
    cleaned.sort();
    cleaned.dedup();
    let mut out: Vec<String> = Vec::with_capacity(cleaned.len());
    for r in cleaned {
        // Sorted order puts an ancestor before its descendants, but not
        // necessarily adjacent to them (`a` < `a!` < `a/b`), so check against
        // every kept root, not just the last one.
        if !out.iter().any(|kept| covers(kept, &r)) {
            out.push(r);
        }
    }
    Some(out)
}

/// The inverse of the UI's uncheck expansion: when every child of `parent`
/// (full keys, the listing of that level) is covered by `selection`, the
/// children fold into `parent`. `None` when nothing folds. The caller
/// normalizes — a folded share root collapses the selection to "whole share".
pub fn collapse_into_parent(selection: &[String], parent: &str, children: &[String]) -> Option<Vec<String>> {
    if children.is_empty() || !children.iter().all(|c| selection_covers(Some(selection), c)) {
        return None;
    }
    let mut out: Vec<String> = selection
        .iter()
        .filter(|s| !children.iter().any(|c| covers(c, s)))
        .cloned()
        .collect();
    out.push(parent.to_string());
    Some(out)
}

// ── Complete dirs ──────────────────────────────────────────────────────────
// A download's persisted "fully landed" dir set (docs/core/PartialDownloads.md
// "Complete dirs"): maximal dirs whose whole subtree a finished run has
// landed, collapsed upward — a dir whose subdirs and own entries are all
// complete stands in for all of them; a file selection root that landed is
// an entry too (its own key). `""` is a legal entry meaning the share
// root, i.e. everything. Unlike a selection there is no `None`: an empty list
// means "nothing known", which is also every legacy record's value.

/// One scope's outcome from a finished run — reported by
/// `TransferItem::dir_completion` per selection root (or `""` for the whole
/// share). `dirs` are the maximal complete dirs under `scope` (the scope
/// itself when it landed whole); `complete` is whether `scope` did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopeCompletion {
    pub scope: String,
    pub complete: bool,
    pub dirs: Vec<String>,
}

/// `covers` with the share root as a legal, all-covering root.
pub(crate) fn root_covers(root: &str, key: &str) -> bool {
    root.is_empty() || covers(root, key)
}

/// Whether `dirs` (a normalized complete-dir list) places `key` inside a
/// fully landed subtree.
pub fn complete_dirs_cover(dirs: &[String], key: &str) -> bool {
    dirs.iter().any(|d| root_covers(d, key))
}

/// Canonical form: trim trailing slashes, drop duplicates and any entry
/// covered by another (ancestor wins), sort. A share-root entry collapses the
/// list to `[""]`.
pub fn normalize_complete_dirs(dirs: Vec<String>) -> Vec<String> {
    let mut cleaned: Vec<String> = Vec::with_capacity(dirs.len());
    for d in dirs {
        let d = d.trim_end_matches('/');
        if d.is_empty() {
            return vec![String::new()];
        }
        cleaned.push(d.to_string());
    }
    cleaned.sort();
    cleaned.dedup();
    let mut out: Vec<String> = Vec::with_capacity(cleaned.len());
    for d in cleaned {
        if !out.iter().any(|kept| covers(kept, &d)) {
            out.push(d);
        }
    }
    out
}

/// Fold a finished run's per-scope outcomes into the persisted list. Each
/// scope the run enumerated whole replaces what was known under it; a scope
/// that did not land whole also invalidates its ancestors (a descendant is
/// incomplete, so they cannot be complete either). Scopes the run did not
/// touch keep their entries — landed bytes don't vanish on deselect.
pub fn merge_complete_dirs(old: &[String], runs: &[ScopeCompletion]) -> Vec<String> {
    let mut out: Vec<String> = old
        .iter()
        .filter(|d| {
            !runs
                .iter()
                .any(|r| root_covers(&r.scope, d) || (!r.complete && root_covers(d, &r.scope)))
        })
        .cloned()
        .collect();
    for r in runs {
        out.extend(r.dirs.iter().cloned());
    }
    normalize_complete_dirs(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    fn scope(scope: &str, complete: bool, dirs: &[&str]) -> ScopeCompletion {
        ScopeCompletion {
            scope: scope.to_string(),
            complete,
            dirs: v(dirs),
        }
    }

    #[test]
    fn collapse_folds_a_fully_covered_level() {
        let sel = v(&["a", "b", "c"]);
        let kids = v(&["a", "b", "c"]);
        assert_eq!(collapse_into_parent(&sel, "", &kids), Some(v(&[""])));
        assert_eq!(normalize_selection(collapse_into_parent(&sel, "", &kids)), None);
        // One unselected sibling: nothing folds.
        assert_eq!(collapse_into_parent(&sel, "", &v(&["a", "b", "c", "d"])), None);
        // Deeper level: the folded children leave, the parent joins, unrelated
        // roots stay; an entry *under* a child counts as covering it only if
        // the child itself is covered.
        let sel = v(&["x/1", "x/2", "z.txt"]);
        assert_eq!(
            collapse_into_parent(&sel, "x", &v(&["x/1", "x/2"])),
            Some(v(&["z.txt", "x"]))
        );
        assert_eq!(collapse_into_parent(&v(&["x/1/deep"]), "x", &v(&["x/1", "x/2"])), None);
        // An empty listing never folds (an empty dir is not "all covered").
        assert_eq!(collapse_into_parent(&sel, "x", &[]), None);
    }

    #[test]
    fn complete_dirs_cover_root_and_boundaries() {
        assert!(complete_dirs_cover(&v(&[""]), "anything/at/all"));
        assert!(complete_dirs_cover(&v(&[""]), ""));
        assert!(!complete_dirs_cover(&[], "a"));
        let dirs = v(&["a/b", "c"]);
        assert!(complete_dirs_cover(&dirs, "a/b"));
        assert!(complete_dirs_cover(&dirs, "a/b/x/y.txt"));
        assert!(complete_dirs_cover(&dirs, "c"));
        assert!(!complete_dirs_cover(&dirs, "a"));
        assert!(!complete_dirs_cover(&dirs, "a/bc"));
        assert!(!complete_dirs_cover(&dirs, ""));
    }

    #[test]
    fn normalize_complete_dirs_collapses_and_sorts() {
        assert_eq!(
            normalize_complete_dirs(v(&["a/b/c", "z/", "a", "a/b", "a", "a!"])),
            v(&["a", "a!", "z"])
        );
        assert_eq!(normalize_complete_dirs(v(&["a", ""])), v(&[""]));
        assert_eq!(normalize_complete_dirs(v(&["/"])), v(&[""]));
        assert!(normalize_complete_dirs(vec![]).is_empty());
    }

    #[test]
    fn merge_replaces_under_scope_and_keeps_others() {
        let old = v(&["a/b", "a/c", "d"]);
        // A rerun of scope `a` found `a/b` still complete, `a/c` no longer
        // (new remote file under it) and nothing new: `a/c` goes, `d` stays.
        let out = merge_complete_dirs(&old, &[scope("a", false, &["a/b"])]);
        assert_eq!(out, v(&["a/b", "d"]));
        // Scope landed whole: its entries collapse into the scope.
        let out = merge_complete_dirs(&old, &[scope("a", true, &["a"])]);
        assert_eq!(out, v(&["a", "d"]));
    }

    #[test]
    fn merge_incomplete_scope_invalidates_ancestors() {
        // `a` was complete; a rerun of `a/b` (say, re-selected after a remote
        // change) did not land whole — `a` can't be complete anymore, and the
        // complete sibling subtree under `a/b` is what's left.
        let out = merge_complete_dirs(&v(&["a", "z"]), &[scope("a/b", false, &["a/b/ok"])]);
        assert_eq!(out, v(&["a/b/ok", "z"]));
        // A complete rerun of a sub-scope leaves the ancestor's claim standing.
        let out = merge_complete_dirs(&v(&["a", "z"]), &[scope("a/b", true, &["a/b"])]);
        assert_eq!(out, v(&["a", "z"]));
    }

    #[test]
    fn merge_whole_share_scope() {
        // Whole-share run landed everything: one root entry, nothing else.
        let out = merge_complete_dirs(&v(&["a", "b/c"]), &[scope("", true, &[""])]);
        assert_eq!(out, v(&[""]));
        // Whole-share run with a hole: every old entry is superseded by the
        // run's own view.
        let out = merge_complete_dirs(&v(&["a", "b/c"]), &[scope("", false, &["b"])]);
        assert_eq!(out, v(&["b"]));
        // An old root claim survives only a whole-share rerun that confirms it.
        let out = merge_complete_dirs(&v(&[""]), &[scope("x", false, &[])]);
        assert!(out.is_empty());
        let out = merge_complete_dirs(&v(&[""]), &[scope("x", true, &["x"])]);
        assert_eq!(out, v(&[""]));
    }

    #[test]
    fn merge_with_no_runs_is_identity() {
        assert_eq!(merge_complete_dirs(&v(&["b", "a"]), &[]), v(&["a", "b"]));
    }

    #[test]
    fn covers_is_boundary_safe() {
        assert!(covers("a/b", "a/b"));
        assert!(covers("a/b", "a/b/c"));
        assert!(covers("a/b", "a/b/c/d"));
        assert!(!covers("a/b", "a/bc"));
        assert!(!covers("a/b", "a"));
        assert!(!covers("a/b", "b"));
    }

    #[test]
    fn selection_covers_none_and_empty() {
        assert!(selection_covers(None, "anything"));
        assert!(!selection_covers(Some(&[]), "anything"));
        let sel = vec!["docs".to_string(), "a/b.txt".to_string()];
        assert!(selection_covers(Some(&sel), "docs/report.pdf"));
        assert!(selection_covers(Some(&sel), "a/b.txt"));
        assert!(!selection_covers(Some(&sel), "a/c.txt"));
    }

    #[test]
    fn normalize_drops_covered_and_dupes() {
        let sel = normalize_selection(Some(vec![
            "a/b/c".into(),
            "a".into(),
            "a/b".into(),
            "a".into(),
            "z.txt".into(),
        ]));
        assert_eq!(sel, Some(vec!["a".to_string(), "z.txt".to_string()]));
    }

    #[test]
    fn normalize_handles_non_adjacent_ancestor() {
        // `a!` sorts between `a` and `a/b`; `a/b` must still collapse into `a`.
        let sel = normalize_selection(Some(vec!["a".into(), "a!".into(), "a/b".into()]));
        assert_eq!(sel, Some(vec!["a".to_string(), "a!".to_string()]));
    }

    #[test]
    fn normalize_root_entry_means_whole_share() {
        assert_eq!(normalize_selection(Some(vec!["a".into(), "".into()])), None);
        assert_eq!(normalize_selection(Some(vec!["/".into()])), None);
        assert_eq!(normalize_selection(None), None);
    }

    #[test]
    fn normalize_trims_trailing_slash_and_keeps_empty() {
        assert_eq!(
            normalize_selection(Some(vec!["docs/".into()])),
            Some(vec!["docs".to_string()])
        );
        assert_eq!(normalize_selection(Some(vec![])), Some(vec![]));
    }
}
