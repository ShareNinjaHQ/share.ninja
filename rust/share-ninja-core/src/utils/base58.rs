//! Base58 encoding/decoding (Bitcoin alphabet: no 0, O, I, l).
//!
//! Used for claim tokens in share-link fragments, where the string is read
//! and re-typed by humans — the alphabet has no confusable glyphs. Mirrored
//! in TS as `sharon-lib/src/utils/base58.ts`; keep the two in lockstep.

/// Encode bytes to base58 (Bitcoin alphabet).
pub fn to_base58(data: &[u8]) -> String {
    bs58::encode(data).into_string()
}

/// Decode base58 (Bitcoin alphabet).
pub fn from_base58(s: &str) -> std::result::Result<Vec<u8>, bs58::decode::Error> {
    bs58::decode(s).into_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let data: Vec<u8> = (0u8..12).collect();
        let encoded = to_base58(&data);
        assert_eq!(from_base58(&encoded).unwrap(), data);
    }

    /// Known vector so the TS mirror can assert the exact same mapping.
    #[test]
    fn known_vector() {
        assert_eq!(to_base58(b"Hello World!"), "2NEpo7TZRRrLZSi2U");
        // Leading zero bytes map to leading '1's.
        assert_eq!(to_base58(&[0, 0, 1]), "112");
    }

    #[test]
    fn rejects_confusable_chars() {
        assert!(from_base58("O0Il").is_err());
    }
}
