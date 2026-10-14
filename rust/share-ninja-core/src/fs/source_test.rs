use super::*;
use std::collections::HashSet;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

static TMP_SEQ: AtomicU64 = AtomicU64::new(0);

/// RAII temp dir under the system temp, removed on drop.
struct TmpDir(PathBuf);
impl TmpDir {
    fn new() -> Self {
        let n = TMP_SEQ.fetch_add(1, Ordering::Relaxed);
        let mut p = std::env::temp_dir();
        p.push(format!("fssrc_test_{}_{}", std::process::id(), n));
        std::fs::create_dir_all(&p).unwrap();
        TmpDir(p)
    }
    fn path(&self) -> &Path {
        &self.0
    }
}
impl Drop for TmpDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Build a `share/` tree (`share/a.txt`, `share/sub/b.txt`) and return its path.
fn build_share(root: &Path) -> PathBuf {
    let share = root.join("share");
    std::fs::create_dir_all(share.join("sub")).unwrap();
    std::fs::write(share.join("a.txt"), b"hello").unwrap();
    std::fs::write(share.join("sub/b.txt"), b"world").unwrap();
    share
}

async fn drain(src: &FsSource, path: &str, recursive: bool) -> Vec<StorageObjectInfo> {
    let mut rx = src.get_dir(path, recursive, None, None).await;
    let mut items = Vec::new();
    while let Some(batch) = rx.recv().await {
        items.extend(batch.expect("get_dir batch error"));
    }
    items
}

fn keys(items: &[StorageObjectInfo]) -> HashSet<String> {
    items.iter().map(|s| s.key.clone()).collect()
}
fn type_of(items: &[StorageObjectInfo], key: &str) -> Option<StorageFileType> {
    items.iter().find(|s| s.key == key).map(|s| s.file_type)
}

#[tokio::test]
async fn get_dir_root_recursive_lists_folder_as_node() {
    let tmp = TmpDir::new();
    let share = build_share(tmp.path());
    let src = FsSource::new(vec![share]);

    let items = drain(&src, "", true).await;
    let k = keys(&items);
    assert!(k.contains("share"), "root dir node present: {k:?}");
    assert!(k.contains("share/a.txt"));
    assert!(k.contains("share/sub"));
    assert!(k.contains("share/sub/b.txt"));
    assert_eq!(type_of(&items, "share"), Some(StorageFileType::Dir));
    assert_eq!(type_of(&items, "share/a.txt"), Some(StorageFileType::Regular));
    assert_eq!(type_of(&items, "share/sub"), Some(StorageFileType::Dir));
}

#[tokio::test]
async fn get_dir_non_recursive_is_immediate_only() {
    let tmp = TmpDir::new();
    let share = build_share(tmp.path());
    let src = FsSource::new(vec![share]);

    let k = keys(&drain(&src, "share", false).await);
    assert!(k.contains("share/a.txt"));
    assert!(k.contains("share/sub"));
    assert!(!k.contains("share/sub/b.txt"), "non-recursive must not descend: {k:?}");
}

#[tokio::test]
async fn search_pages_at_page_limit() {
    let tmp = TmpDir::new();
    let share = tmp.path().join("share");
    std::fs::create_dir_all(&share).unwrap();
    for i in 0..5 {
        std::fs::write(share.join(format!("match_{i}.txt")), b"x").unwrap();
    }
    std::fs::write(share.join("other.dat"), b"x").unwrap();
    let src = FsSource::new(vec![share]);

    let q = StorageSearchQuery {
        name_match: Some("match_".into()),
    };
    let mut opts = SearchOptions {
        path_scope: Some("share".into()),
        recursive: Some(true),
        page_limit: Some(2),
        ..Default::default()
    };
    let res = src.search(&q, &opts).await.unwrap();
    assert_eq!(res.items.len(), 2, "a page is page_limit entries");
    let token = res.next_page_token.expect("a full page parks the walk under a token");
    assert!(res.items.iter().all(|s| s.key.contains("match_")));

    let mut all = res.items;
    opts.page_token = Some(token);
    let mut pages = 1;
    loop {
        let res = src.search(&q, &opts).await.unwrap();
        pages += 1;
        assert!(pages <= 10, "paging must terminate");
        assert!(res.items.len() <= 2, "never more than page_limit");
        all.extend(res.items);
        match res.next_page_token {
            Some(t) => opts.page_token = Some(t),
            None => break,
        }
    }
    let unique: HashSet<String> = all.iter().map(|s| s.key.clone()).collect();
    assert_eq!(
        unique.len(),
        5,
        "pages resume the walk: no repeats, no gaps: {unique:?}"
    );
    assert_eq!(all.len(), 5);
}

#[tokio::test]
async fn search_matches_basenames_case_insensitively() {
    let tmp = TmpDir::new();
    let share = tmp.path().join("share");
    std::fs::create_dir_all(share.join("Sub")).unwrap();
    std::fs::write(share.join("Sub/README.md"), b"x").unwrap();
    std::fs::write(share.join("readme_too.txt"), b"x").unwrap();
    std::fs::write(share.join("Übung.txt"), b"x").unwrap(); // non-ASCII path
    std::fs::write(share.join("other.dat"), b"x").unwrap();
    let src = FsSource::new(vec![share]);

    let opts = SearchOptions {
        path_scope: None,
        recursive: Some(true),
        page_limit: Some(100),
        ..Default::default()
    };

    let q = StorageSearchQuery {
        name_match: Some("ReadMe".into()),
    };
    let res = src.search(&q, &opts).await.unwrap();
    let k = keys(&res.items);
    assert!(k.contains("share/Sub/README.md"), "ASCII case-folded match: {k:?}");
    assert!(k.contains("share/readme_too.txt"));
    assert_eq!(res.items.len(), 2, "no false positives: {k:?}");
    assert!(res.next_page_token.is_none());

    let q = StorageSearchQuery {
        name_match: Some("übUNG".into()),
    };
    let res = src.search(&q, &opts).await.unwrap();
    assert_eq!(keys(&res.items), HashSet::from(["share/Übung.txt".to_string()]));

    let q = StorageSearchQuery {
        name_match: Some("no_such_name".into()),
    };
    let res = src.search(&q, &opts).await.unwrap();
    assert!(res.items.is_empty());
    assert!(res.next_page_token.is_none());
}

/// A subdirectory the walk can't open (no permission) is stepped over: the
/// search still returns every readable match instead of failing — while a
/// plain recursive listing of the same tree still reports the error.
#[cfg(unix)]
#[tokio::test]
async fn search_skips_unreadable_subdirectories() {
    use std::os::unix::fs::PermissionsExt;
    let tmp = TmpDir::new();
    let share = tmp.path().join("share");
    std::fs::create_dir_all(share.join("locked")).unwrap();
    std::fs::create_dir_all(share.join("open")).unwrap();
    std::fs::write(share.join("locked/match_hidden.txt"), b"x").unwrap();
    std::fs::write(share.join("open/match_b.txt"), b"x").unwrap();
    std::fs::write(share.join("match_a.txt"), b"x").unwrap();
    std::fs::set_permissions(share.join("locked"), std::fs::Permissions::from_mode(0o000)).unwrap();
    // root reads through any mode bits — nothing to test there.
    if std::fs::read_dir(share.join("locked")).is_ok() {
        std::fs::set_permissions(share.join("locked"), std::fs::Permissions::from_mode(0o755)).unwrap();
        return;
    }
    let src = FsSource::new(vec![share.clone()]);

    let opts = SearchOptions {
        path_scope: None,
        recursive: Some(true),
        page_limit: Some(100),
        ..Default::default()
    };
    let q = StorageSearchQuery {
        name_match: Some("match".into()),
    };
    let res = src.search(&q, &opts).await;
    let mut rx = src.get_dir("", true, None, None).await;
    let mut listing_failed = false;
    while let Some(batch) = rx.recv().await {
        listing_failed |= batch.is_err();
    }
    // Restore before asserting so TmpDir can clean up.
    std::fs::set_permissions(share.join("locked"), std::fs::Permissions::from_mode(0o755)).unwrap();

    let res = res.expect("search survives an unreadable subdirectory");
    assert_eq!(
        keys(&res.items),
        HashSet::from(["share/match_a.txt".to_string(), "share/open/match_b.txt".to_string()])
    );
    assert!(listing_failed, "a listing must not silently drop a subtree");
}

#[tokio::test]
async fn search_pages_resume_the_same_walk() {
    let tmp = TmpDir::new();
    let share = tmp.path().join("share");
    std::fs::create_dir_all(share.join("sub")).unwrap();
    for i in 0..5 {
        std::fs::write(share.join(format!("match_{i}.txt")), b"x").unwrap();
        std::fs::write(share.join("sub").join(format!("match_sub_{i}.txt")), b"x").unwrap();
    }
    std::fs::write(share.join("other.dat"), b"x").unwrap();
    let src = FsSource::new(vec![share]);

    // A 1-byte page budget forces one item per page (the first item always
    // ships regardless of budget), so every page cut + token resume runs.
    let q = StorageSearchQuery {
        name_match: Some("match_".into()),
    };
    let mut opts = SearchOptions {
        path_scope: Some("share".into()),
        recursive: Some(true),
        page_limit: Some(100),
        max_response_bytes: Some(1),
        ..Default::default()
    };

    let mut all = Vec::new();
    let mut pages = 0;
    loop {
        let res = src.search(&q, &opts).await.unwrap();
        pages += 1;
        assert!(pages <= 20, "paging must terminate");
        all.extend(res.items);
        match res.next_page_token {
            Some(t) => opts.page_token = Some(t),
            None => break,
        }
    }
    assert!(pages >= 10, "1-byte budget ⇒ one item per page, got {pages}");
    assert_eq!(all.len(), 10);
    assert!(all.iter().all(|s| s.key.contains("match_")));
    let unique: HashSet<String> = all.iter().map(|s| s.key.clone()).collect();
    assert_eq!(unique.len(), 10, "resume must not repeat or skip entries");
}

#[tokio::test]
async fn search_with_unknown_page_token_errors() {
    let tmp = TmpDir::new();
    let share = build_share(tmp.path());
    let src = FsSource::new(vec![share]);

    let q = StorageSearchQuery {
        name_match: Some("a".into()),
    };
    let opts = SearchOptions {
        page_token: Some("s999".into()),
        ..Default::default()
    };
    let err = src.search(&q, &opts).await.unwrap_err();
    assert!(err.to_string().contains("cursor expired"), "got: {err}");
}

#[cfg(unix)]
#[tokio::test]
async fn symlinks_are_listed_not_resolved() {
    use std::os::unix::fs::symlink;
    let tmp = TmpDir::new();
    let share = build_share(tmp.path());
    symlink(share.join("a.txt"), share.join("flink")).unwrap(); // → file
    symlink(share.join("sub"), share.join("dlink")).unwrap(); // → dir
    symlink(share.join("nope"), share.join("broken")).unwrap(); // → missing
    let src = FsSource::new(vec![share]);

    let items = drain(&src, "", true).await;
    let k = keys(&items);
    assert_eq!(type_of(&items, "share/flink"), Some(StorageFileType::Symlink));
    assert_eq!(type_of(&items, "share/dlink"), Some(StorageFileType::Symlink));
    assert_eq!(
        type_of(&items, "share/broken"),
        Some(StorageFileType::Symlink),
        "broken link is listed, not an error"
    );
    assert!(
        !k.contains("share/dlink/b.txt"),
        "a symlinked dir must not be descended into: {k:?}"
    );
}

/// Every block of a streamed stat walk, in order. The source publishes running
/// totals as it goes; the last block is the answer.
async fn dir_stats_blocks(src: &FsSource, path: &str) -> Vec<StorageDirStats> {
    let mut rx = src.get_dir_stats_streamed(path).await;
    let mut blocks = Vec::new();
    while let Some(block) = rx.recv().await {
        blocks.push(block.expect("dir stats block"));
    }
    blocks
}

/// The completed totals of a streamed stat walk.
async fn drain_dir_stats(src: &FsSource, path: &str) -> StorageDirStats {
    *dir_stats_blocks(src, path)
        .await
        .last()
        .expect("dir stats produced no block")
}

/// A walk that finishes fast still yields its total — a consumer must never see
/// an empty stream.
#[tokio::test]
async fn dir_stats_streamed_always_yields_a_final_block() {
    let tmp = TmpDir::new();
    let share = build_share(tmp.path());
    let src = FsSource::new(vec![share]);

    let blocks = dir_stats_blocks(&src, "").await;
    assert!(!blocks.is_empty(), "a walk must publish at least its total");
    let last = blocks.last().unwrap();
    assert_eq!(last.number_of_files, 2);
    assert_eq!(last.total_size, 10);
}

/// Blocks are cumulative running totals, never deltas — so the last one alone
/// is the complete answer.
#[tokio::test]
async fn dir_stats_streamed_blocks_are_cumulative() {
    let tmp = TmpDir::new();
    let share = tmp.path().join("share");
    std::fs::create_dir_all(&share).unwrap();
    for i in 0..2000 {
        std::fs::write(share.join(format!("f{i}.bin")), b"xy").unwrap();
    }
    let src = FsSource::new(vec![share]);

    let blocks = dir_stats_blocks(&src, "").await;
    for pair in blocks.windows(2) {
        assert!(
            pair[1].number_of_files >= pair[0].number_of_files && pair[1].total_size >= pair[0].total_size,
            "totals must never go backwards: {:?} then {:?}",
            pair[0],
            pair[1]
        );
    }
    let last = blocks.last().unwrap();
    assert_eq!(last.number_of_files, 2000);
    assert_eq!(last.total_size, 4000, "2 bytes each");
}

/// An unknown key fails the stream rather than reporting a zeroed total that
/// would silently pass for "empty directory".
#[tokio::test]
async fn dir_stats_streamed_missing_path_errors() {
    let tmp = TmpDir::new();
    let share = build_share(tmp.path());
    let src = FsSource::new(vec![share]);

    let mut rx = src.get_dir_stats_streamed("nope").await;
    let first = rx.recv().await.expect("a block must arrive");
    assert!(first.is_err(), "missing path must surface as an error");
}

/// Dropping the receiver cancels the blocking walk instead of statting a whole
/// tree nobody is waiting for.
#[tokio::test]
async fn dir_stats_streamed_dropping_receiver_cancels() {
    let tmp = TmpDir::new();
    let share = build_share(tmp.path());
    let src = FsSource::new(vec![share]);

    let rx = src.get_dir_stats_streamed("").await;
    drop(rx);
    // The producer runs on a blocking thread; give it a moment to observe the
    // closed channel and unwind. Nothing to assert but "no panic / no hang" —
    // a `blocking_send` on a dropped receiver that wasn't handled would abort
    // the test process.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
}

#[tokio::test]
async fn get_dir_stats_counts_contents() {
    let tmp = TmpDir::new();
    let share = build_share(tmp.path()); // a.txt(5) + sub/ + sub/b.txt(5)
    let src = FsSource::new(vec![share]);

    let s = drain_dir_stats(&src, "").await;
    assert_eq!(s.number_of_files, 2);
    assert_eq!(
        s.number_of_folders, 1,
        "the `sub` dir; the share root itself isn't counted"
    );
    assert_eq!(s.number_of_symlinks, 0);
    assert_eq!(s.total_size, 10, "5 + 5");
}

#[cfg(unix)]
#[tokio::test]
async fn get_dir_stats_counts_symlinks_without_following() {
    use std::os::unix::fs::symlink;
    let tmp = TmpDir::new();
    let share = build_share(tmp.path());
    symlink(share.join("sub"), share.join("dlink")).unwrap(); // → dir (must NOT descend)
    symlink(share.join("a.txt"), share.join("flink")).unwrap(); // → file
    let src = FsSource::new(vec![share]);

    let s = drain_dir_stats(&src, "").await;
    assert_eq!(s.number_of_files, 2, "symlinked dir not descended ⇒ no extra files");
    assert_eq!(s.number_of_folders, 1);
    assert_eq!(s.number_of_symlinks, 2, "dlink + flink counted, not followed");
}

/// Scale test: 10k+ entries of mixed types. Verifies exact per-type counts,
/// that `meta()` drains the same walk as `get_dir`, that symlinked dirs are
/// not descended, and that a bounded batch size streams in real batches.
#[tokio::test]
async fn scale_10k_mixed_types() {
    const N_DIRS: usize = 100;
    const FILES_PER_DIR: usize = 100;
    const TOP_FILES: usize = 50;

    let tmp = TmpDir::new();
    let share = tmp.path().join("share");
    std::fs::create_dir_all(&share).unwrap();

    // N_DIRS subdirs, each with FILES_PER_DIR regular files.
    for d in 0..N_DIRS {
        let dir = share.join(format!("d{d:03}"));
        std::fs::create_dir_all(&dir).unwrap();
        for f in 0..FILES_PER_DIR {
            std::fs::write(dir.join(format!("f{f:03}.bin")), b"x").unwrap();
        }
    }
    // Top-level regular files.
    for f in 0..TOP_FILES {
        std::fs::write(share.join(format!("top{f:03}.txt")), b"x").unwrap();
    }

    // Symlinks: to a file, to a dir (must NOT be descended), and broken.
    let (n_file_links, n_dir_links, n_broken): (usize, usize, usize);
    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;
        n_file_links = 30;
        n_dir_links = 10;
        n_broken = 5;
        for i in 0..n_file_links {
            symlink(share.join("top000.txt"), share.join(format!("fl{i}"))).unwrap();
        }
        for i in 0..n_dir_links {
            symlink(share.join("d000"), share.join(format!("dl{i}"))).unwrap();
        }
        for i in 0..n_broken {
            symlink(share.join(format!("missing{i}")), share.join(format!("bl{i}"))).unwrap();
        }
    }
    #[cfg(not(unix))]
    {
        n_file_links = 0;
        n_dir_links = 0;
        n_broken = 0;
    }

    let src = FsSource::new(vec![share]);

    // Full recursive listing via the streamed walk.
    let items = drain(&src, "", true).await;

    let exp_regular = N_DIRS * FILES_PER_DIR + TOP_FILES; // 10_050
    let exp_dirs = 1 + N_DIRS; // "share" node + the d* dirs = 101
    let exp_symlinks = n_file_links + n_dir_links + n_broken;

    let mut c_reg = 0usize;
    let mut c_dir = 0usize;
    let mut c_sym = 0usize;
    for s in &items {
        match s.file_type {
            StorageFileType::Regular => c_reg += 1,
            StorageFileType::Dir => c_dir += 1,
            StorageFileType::Symlink => c_sym += 1,
            _ => {}
        }
    }
    assert_eq!(c_reg, exp_regular, "regular file count");
    assert_eq!(c_dir, exp_dirs, "dir count incl. the share node");
    assert_eq!(c_sym, exp_symlinks, "symlink count");
    assert!(items.len() > 10_000, "10k+ entries, got {}", items.len());

    // Symlinked dirs must not be descended: no entry under any dl{i}/.
    for i in 0..n_dir_links {
        let prefix = format!("share/dl{i}/");
        assert!(
            !items.iter().any(|s| s.key.starts_with(&prefix)),
            "symlinked dir dl{i} was descended"
        );
    }

    // Bounded batch size streams real batches: every batch but the last is
    // exactly the limit, and the total matches the full listing.
    let mut rx = src.get_dir("", true, None, Some(500)).await;
    let mut batches = 0usize;
    let mut total = 0usize;
    let mut prev_len: Option<usize> = None;
    while let Some(b) = rx.recv().await {
        let b = b.unwrap();
        if let Some(p) = prev_len {
            assert_eq!(p, 500, "a non-final batch must be full ({} entries)", p);
        }
        prev_len = Some(b.len());
        total += b.len();
        batches += 1;
    }
    assert!(batches > 1, "10k+ entries at batch 500 ⇒ many batches, got {batches}");
    assert_eq!(total, items.len(), "batched total must equal the full listing");
}

#[tokio::test]
async fn same_named_file_roots_get_unique_keys() {
    let tmp = TmpDir::new();
    let a = tmp.path().join("a");
    let b = tmp.path().join("b");
    std::fs::create_dir_all(&a).unwrap();
    std::fs::create_dir_all(&b).unwrap();
    std::fs::write(a.join("doc.pdf"), b"first").unwrap();
    std::fs::write(b.join("doc.pdf"), b"second!").unwrap();
    let roots = vec![a.join("doc.pdf"), b.join("doc.pdf")];

    let src = FsSource::new(roots.clone());
    let items = drain(&src, "", true).await;
    let k = keys(&items);
    assert_eq!(k, HashSet::from(["doc.pdf".to_string(), "doc 1.pdf".to_string()]));

    // The key↔entry binding follows pick order: sizes prove each key lists
    // its own file, and resolution returns that same file.
    let size_of = |key: &str| items.iter().find(|s| s.key == key).unwrap().size;
    assert_eq!(size_of("doc.pdf"), 5);
    assert_eq!(size_of("doc 1.pdf"), 7);
    let root_keys = unique_root_keys(&roots);
    assert_eq!(find_file_path_in_keyed(&roots, &root_keys, "doc.pdf"), Some(roots[0].clone()));
    assert_eq!(find_file_path_in_keyed(&roots, &root_keys, "doc 1.pdf"), Some(roots[1].clone()));
}

#[tokio::test]
async fn same_named_dir_roots_get_unique_prefixes() {
    let tmp = TmpDir::new();
    let a = tmp.path().join("x/share");
    let b = tmp.path().join("y/share");
    std::fs::create_dir_all(&a).unwrap();
    std::fs::create_dir_all(&b).unwrap();
    std::fs::write(a.join("f.txt"), b"one").unwrap();
    std::fs::write(b.join("f.txt"), b"three").unwrap();

    let src = FsSource::new(vec![a, b]);
    let items = drain(&src, "", true).await;
    let k = keys(&items);
    assert_eq!(
        k,
        HashSet::from([
            "share".to_string(),
            "share/f.txt".to_string(),
            "share 1".to_string(),
            "share 1/f.txt".to_string(),
        ])
    );

    // Subtree listing and key resolution both bind "share 1" to the second root.
    let sub = drain(&src, "share 1", true).await;
    assert_eq!(keys(&sub), HashSet::from(["share 1/f.txt".to_string()]));
    assert_eq!(src.get_attrs("share 1/f.txt").await.unwrap().size, 5);
    assert_eq!(src.get_attrs("share/f.txt").await.unwrap().size, 3);
}

/// The walk carries each entry's permission bits (the panels' executable
/// marker), on both the bulk and the readdir paths, and so does a single
/// `get_attrs`.
#[cfg(unix)]
#[tokio::test]
async fn get_dir_carries_posix_permissions() {
    use std::os::unix::fs::PermissionsExt;
    let tmp = TmpDir::new();
    let share = build_share(tmp.path());
    std::fs::write(share.join("run.sh"), b"#!/bin/sh\n").unwrap();
    std::fs::set_permissions(share.join("run.sh"), std::fs::Permissions::from_mode(0o755)).unwrap();
    std::fs::set_permissions(share.join("a.txt"), std::fs::Permissions::from_mode(0o644)).unwrap();
    let src = FsSource::new(vec![share]);

    let items = drain(&src, "", true).await;
    let mode_of = |key: &str| {
        items
            .iter()
            .find(|s| s.key == key)
            .and_then(|s| s.attrs.posix_permissions)
    };
    assert_eq!(mode_of("share/run.sh"), Some(0o755));
    assert_eq!(mode_of("share/a.txt"), Some(0o644));
    assert!(mode_of("share/sub").is_some(), "directories carry their mode too");

    let one = src.get_attrs("share/run.sh").await.expect("get_attrs");
    assert_eq!(one.attrs.posix_permissions, Some(0o755));
}

/// A regular file's etag is `mtime_ns-size`, the same string from a listing
/// and from `get_attrs` (the receiver's thumbnail cache keys on it), and it
/// moves when the file does. Dirs and symlinks carry none.
#[tokio::test]
async fn regular_files_carry_a_stable_etag() {
    let tmp = TmpDir::new();
    let share = build_share(tmp.path());
    #[cfg(unix)]
    std::os::unix::fs::symlink("a.txt", share.join("link")).unwrap();
    let src = FsSource::new(vec![share.clone()]);

    let items = drain(&src, "", true).await;
    let etag_of = |key: &str| items.iter().find(|s| s.key == key).and_then(|s| s.etag.clone());
    let listed = etag_of("share/a.txt").expect("a regular file has an etag");
    assert!(listed.ends_with("-5"), "mtime_ns-size, got {listed}");
    assert!(!listed.starts_with("0-"), "the walk carries the real mtime, got {listed}");
    assert_eq!(etag_of("share/sub"), None, "a dir has none");
    #[cfg(unix)]
    assert_eq!(etag_of("share/link"), None, "a symlink has none");

    let one = src.get_attrs("share/a.txt").await.expect("get_attrs");
    assert_eq!(one.etag.as_deref(), Some(listed.as_str()), "listing and get_attrs agree");

    std::fs::write(share.join("a.txt"), b"hello, again").unwrap();
    let moved = src.get_attrs("share/a.txt").await.expect("get_attrs").etag;
    assert_ne!(moved.as_deref(), Some(listed.as_str()), "a rewritten file gets a new etag");
}

/// Scripted thumbnailer: the file's extension picks the outcome, and the
/// "image" records what it was asked for.
struct FakeGenerator;

#[async_trait]
impl PreviewGenerator for FakeGenerator {
    fn max_px(&self) -> u32 {
        256
    }

    async fn generate(&self, file: &str, px: u32) -> Result<Option<PreviewImage>, String> {
        if file.ends_with(".slow") {
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        }
        if file.ends_with(".err") {
            return Err("decoder blew up".into());
        }
        if file.ends_with(".bin") {
            return Ok(None);
        }
        Ok(Some(PreviewImage {
            bytes: format!("{file}@{px}").into_bytes(),
            mime_type: "image/png".into(),
            etag: None,
        }))
    }
}

async fn previews(src: &FsSource, objects: &[&str], px: u32) -> Vec<Result<PreviewImage, StorageError>> {
    let objects: Vec<String> = objects.iter().map(|s| s.to_string()).collect();
    let mut out: Vec<Option<Result<PreviewImage, StorageError>>> = objects.iter().map(|_| None).collect();
    let mut rx = src.get_previews(objects, px).await.expect("request accepted");
    while let Some((index, result)) = rx.recv().await {
        assert!(out[index].is_none(), "index {index} answered twice");
        out[index] = Some(result);
    }
    out.into_iter().map(|r| r.expect("every index answered")).collect()
}

/// Every object of a batch is answered exactly once, each with its own
/// outcome; the requested size is capped to the generator's.
#[tokio::test]
async fn get_previews_answers_every_object() {
    let tmp = TmpDir::new();
    let share = build_share(tmp.path());
    std::fs::write(share.join("x.bin"), b"\0").unwrap();
    std::fs::write(share.join("y.err"), b"\0").unwrap();
    #[cfg(unix)]
    std::os::unix::fs::symlink("a.txt", share.join("link")).unwrap();
    let src = FsSource::new(vec![share.clone()]).with_preview_generator(Some(Arc::new(FakeGenerator)));
    assert!(src.serves_previews());

    let got = previews(
        &src,
        &["share/a.txt", "share/sub", "share/missing.txt", "share/x.bin", "share/y.err", "share/../a.txt"],
        1000,
    )
    .await;
    let image = got[0].as_ref().expect("a regular file previews");
    assert_eq!(image.mime_type, "image/png");
    let asked = String::from_utf8(image.bytes.clone()).unwrap();
    assert_eq!(asked, format!("{}@256", share.join("a.txt").display()), "real path, px capped");
    let listed = src.get_attrs("share/a.txt").await.unwrap().etag;
    assert!(listed.is_some() && image.etag == listed, "the preview names the version the listing reports");
    assert!(matches!(got[1], Err(StorageError::NoPreview(_))), "a dir has no preview");
    assert!(matches!(got[2], Err(StorageError::NoSuchKey(_))));
    assert!(matches!(got[3], Err(StorageError::NoPreview(_))), "the generator's Ok(None)");
    assert!(matches!(got[4], Err(StorageError::Other(_))), "a generator error is transient");
    assert!(matches!(got[5], Err(StorageError::NoSuchKey(_))), "traversal never resolves");

    #[cfg(unix)]
    {
        let got = previews(&src, &["share/link"], 64).await;
        assert!(matches!(got[0], Err(StorageError::NoPreview(_))), "a symlink is never opened");
    }

    assert!(previews(&src, &[], 64).await.is_empty(), "an empty batch just closes");
}

/// Without a generator the source serves no previews: not advertised, and
/// the request as a whole is refused.
#[tokio::test]
async fn get_previews_without_a_generator_is_refused() {
    let tmp = TmpDir::new();
    let share = build_share(tmp.path());
    let src = FsSource::new(vec![share]);
    assert!(!src.serves_previews());
    assert!(matches!(
        src.get_previews(vec!["share/a.txt".into()], 64).await,
        Err(StorageError::NotImplemented(_))
    ));
}

/// A generation that outlives the item timeout fails that item alone — the
/// rest of the batch is answered, so the wire never goes quiet for long.
#[tokio::test(start_paused = true)]
async fn get_previews_times_a_stuck_item_out() {
    let tmp = TmpDir::new();
    let share = build_share(tmp.path());
    std::fs::write(share.join("z.slow"), b"\0").unwrap();
    let src = FsSource::new(vec![share]).with_preview_generator(Some(Arc::new(FakeGenerator)));

    let got = previews(&src, &["share/z.slow", "share/a.txt"], 64).await;
    assert!(matches!(&got[0], Err(StorageError::Other(m)) if m.contains("timed out")));
    assert!(got[1].is_ok());
}
