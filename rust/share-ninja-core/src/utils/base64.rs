//! Base64 encoding/decoding utilities with URL-safe variant

use base64::{Engine, engine::general_purpose};

/// Encode bytes to URL-safe base64 without padding (special variant)
pub fn to_base64_special(data: &[u8]) -> String {
    general_purpose::URL_SAFE_NO_PAD.encode(data)
}

/// Decode URL-safe base64 without padding (special variant)
pub fn from_base64_special(base64: &str) -> std::result::Result<Vec<u8>, base64::DecodeError> {
    general_purpose::URL_SAFE_NO_PAD.decode(base64)
}

/// Decode standard base64
pub fn from_base64(base64: &str) -> std::result::Result<Vec<u8>, base64::DecodeError> {
    general_purpose::STANDARD.decode(base64)
}

/// Encode to standard base64
pub fn to_base64(data: &[u8]) -> String {
    general_purpose::STANDARD.encode(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip_special() {
        let data = b"Hello, World!";
        let encoded = to_base64_special(data);
        let decoded = from_base64_special(&encoded).unwrap();
        assert_eq!(data.as_slice(), decoded.as_slice());
    }

    #[test]
    fn test_roundtrip_standard() {
        let data = b"Hello, World!";
        let encoded = to_base64(data);
        let decoded = from_base64(&encoded).unwrap();
        assert_eq!(data.as_slice(), decoded.as_slice());
    }
}
