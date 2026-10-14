//! `MultiSource`'s cache of what the peer rendered (docs/core/previews.md). It
//! exists because a preview from the peer costs a round trip, and rows of a
//! list are recycled: a row asks again on every scroll-back. It lives and dies
//! with its `MultiSource`; every image keeps the etag the peer rendered it from
//! (`PreviewImage::etag`).

use std::collections::HashMap;
use std::sync::Mutex;

use super::{PreviewImage, StorageError};

/// Cache budget, encoded bytes. Rows of a list are recycled, so a row asks
/// again on every scroll-back; what is kept is the encoded image — the rows on
/// screen hold the decoded ones.
const BUDGET_BYTES: usize = 16 * 1024 * 1024;

/// One answer of a `get_previews` call: `(index into its objects, result)`.
pub(crate) type Answer = (usize, Result<PreviewImage, StorageError>);

/// `image == None`: the peer has no preview for this version of the file.
struct Cached {
    image: Option<PreviewImage>,
    size: usize,
    last_used: u64,
}

#[derive(Default)]
struct State {
    /// path → px → entry.
    cache: HashMap<String, HashMap<u32, Cached>>,
    used: usize,
    tick: u64,
}

#[derive(Default)]
pub(crate) struct PreviewState {
    state: Mutex<State>,
}

impl PreviewState {
    /// Split a call into what the cache answers now and the indices it cannot.
    pub(crate) fn cached(&self, objects: &[String], px: u32) -> (Vec<Answer>, Vec<usize>) {
        let mut state = self.state.lock().unwrap();
        state.tick += 1;
        let tick = state.tick;
        let mut hits = Vec::new();
        let mut missing = Vec::new();
        for (index, object) in objects.iter().enumerate() {
            match state.cache.get_mut(object).and_then(|by_px| by_px.get_mut(&px)) {
                Some(entry) => {
                    entry.last_used = tick;
                    let result = match &entry.image {
                        Some(image) => Ok(image.clone()),
                        None => Err(StorageError::NoPreview(object.clone())),
                    };
                    hits.push((index, result));
                }
                None => missing.push(index),
            }
        }
        (hits, missing)
    }

    /// Keep what the peer answered for `(object, px)`: an image, or `None` for
    /// "no preview". Least recently used entries leave once over budget.
    pub(crate) fn store(&self, object: &str, px: u32, image: Option<PreviewImage>) {
        let mut state = self.state.lock().unwrap();
        state.tick += 1;
        let size = object.len() + image.as_ref().map(|i| i.bytes.len()).unwrap_or(0);
        let entry = Cached {
            image,
            size,
            last_used: state.tick,
        };
        if let Some(old) = state.cache.entry(object.to_string()).or_default().insert(px, entry) {
            state.used -= old.size;
        }
        state.used += size;
        while state.used > BUDGET_BYTES {
            let oldest = state
                .cache
                .iter()
                .flat_map(|(path, by_px)| by_px.iter().map(move |(px, e)| (e.last_used, path.clone(), *px)))
                .min();
            let Some((_, path, px)) = oldest else {
                break;
            };
            state.evict(&path, px);
        }
    }

}

impl State {
    fn evict(&mut self, path: &str, px: u32) {
        let Some(by_px) = self.cache.get_mut(path) else {
            return;
        };
        if let Some(entry) = by_px.remove(&px) {
            self.used -= entry.size;
        }
        if by_px.is_empty() {
            self.cache.remove(path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn image(bytes: &[u8], etag: &str) -> PreviewImage {
        PreviewImage {
            bytes: bytes.to_vec(),
            mime_type: "image/png".into(),
            etag: Some(etag.into()),
        }
    }

    fn paths(keys: &[&str]) -> Vec<String> {
        keys.iter().map(|k| k.to_string()).collect()
    }

    /// Hits are answered per `(path, px)`, a cached "no preview" as
    /// `NoPreview`; everything else is reported missing.
    #[test]
    fn cached_splits_hits_from_misses() {
        let loader = PreviewState::default();
        loader.store("a.png", 64, Some(image(b"A", "v1")));
        loader.store("b.bin", 64, None);

        let (hits, missing) = loader.cached(&paths(&["a.png", "b.bin", "c.png"]), 64);
        assert_eq!(missing, [2]);
        assert!(matches!(&hits[0], (0, Ok(i)) if i.bytes == b"A"));
        assert!(matches!(&hits[1], (1, Err(StorageError::NoPreview(_)))));

        let (hits, missing) = loader.cached(&paths(&["a.png"]), 128);
        assert!(hits.is_empty() && missing == [0], "another px is another entry");
    }

    /// Over budget, the least recently used entries leave first.
    #[test]
    fn store_evicts_least_recently_used_over_budget() {
        let loader = PreviewState::default();
        let big = vec![0u8; BUDGET_BYTES / 2 - 64];
        loader.store("one", 64, Some(image(&big, "v1")));
        loader.store("two", 64, Some(image(&big, "v1")));
        let _ = loader.cached(&paths(&["one"]), 64); // touch: "two" is now the oldest
        loader.store("three", 64, Some(image(&big, "v1")));

        let (hits, missing) = loader.cached(&paths(&["one", "two", "three"]), 64);
        assert_eq!(missing, [1]);
        assert_eq!(hits.len(), 2);
    }
}
