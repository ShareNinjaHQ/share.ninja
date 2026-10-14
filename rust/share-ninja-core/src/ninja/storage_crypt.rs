//! Storage encryption utilities using AES-GCM

use aes_gcm::{
    Aes256Gcm, Nonce,
    aead::{Aead, KeyInit, Payload},
};
use bytes::{Bytes, BytesMut};
use hkdf::Hkdf;
use sha2::Sha256;
use std::collections::VecDeque;
use thiserror::Error;
use tokio::sync::mpsc;

use crate::transfers::StorageError;

pub const GCM_CHUNK_SIZE: usize = 64 * 1024;
pub const GCM_TAG_SIZE: usize = 16;

#[derive(Error, Debug)]
pub enum CryptError {
    #[error("Encryption error: {0}")]
    Encryption(String),

    #[error("Decryption error: {0}")]
    Decryption(String),

    #[error("Key derivation error: {0}")]
    KeyDerivation(String),
}

/// Encrypted range calculation result
#[derive(Debug, Clone)]
pub struct EncryptedRangeInfo {
    pub encrypted_offset: u64,
    pub encrypted_length: u64,
    pub first_chunk_index: u64,
    pub slice_start: usize,
    pub slice_end: usize,
}

/// Calculate encrypted range for partial reads
pub fn calc_encrypted_range(offset: u64, length: u64, chunk_size: usize, total_size: u64) -> EncryptedRangeInfo {
    let chunk = chunk_size as u64;
    let chunk_with_tag = chunk_size + GCM_TAG_SIZE;

    // Which chunks contain the requested bytes? (mirror TS `calcEncryptedRange`)
    let first_chunk_index = offset / chunk;
    let last_byte = (offset + length - 1).min(total_size.saturating_sub(1));
    let last_chunk_index = last_byte / chunk;
    let total_chunks = total_size.div_ceil(chunk); // ceil

    let encrypted_offset = first_chunk_index * chunk_with_tag as u64;

    // The final chunk of a file is usually smaller than `chunk_size`; when the
    // requested range reaches it, account for the partial size so we don't
    // overshoot the blob (TS uses `totalPlaintextSize % chunkSize`). Ignoring
    // `total_size` (as the old code did) over-reads the tail and breaks the
    // last-chunk GCM boundary.
    let encrypted_length = if total_chunks > 0 && last_chunk_index == total_chunks - 1 {
        let last_chunk_plain = match total_size % chunk {
            0 => chunk,
            r => r,
        };
        (last_chunk_index - first_chunk_index) * chunk_with_tag as u64 + (last_chunk_plain + GCM_TAG_SIZE as u64)
    } else {
        (last_chunk_index - first_chunk_index + 1) * chunk_with_tag as u64
    };

    let slice_start = (offset % chunk) as usize;
    let slice_end = slice_start + length as usize;

    EncryptedRangeInfo {
        encrypted_offset,
        encrypted_length,
        first_chunk_index,
        slice_start,
        slice_end,
    }
}

/// Storage encryption/decryption utilities. Stateless — `Clone`/`Copy` so the
/// streaming producer tasks can hold their own handle cheaply.
#[derive(Clone, Copy)]
pub struct StorageCrypt {}

impl StorageCrypt {
    pub fn new() -> Self {
        Self {}
    }

    pub fn cleanup(&mut self) {}

    /// Derive encryption key and IV from seed
    pub fn derive_seed_keys(seed: &[u8]) -> Result<(Vec<u8>, Vec<u8>), CryptError> {
        let hk = Hkdf::<Sha256>::new(None, seed);

        let mut iv = vec![0u8; 12];
        hk.expand(b"blob:iv", &mut iv)
            .map_err(|e| CryptError::KeyDerivation(e.to_string()))?;

        let mut key = vec![0u8; 32];
        hk.expand(b"blob:key", &mut key)
            .map_err(|e| CryptError::KeyDerivation(e.to_string()))?;

        Ok((iv, key))
    }

    /// Derive encryption key for journal encryption from master key (IV is random, prepended to data)
    pub fn derive_journal_key(master_key: &[u8]) -> Result<Vec<u8>, CryptError> {
        let hk = Hkdf::<Sha256>::new(None, master_key);

        let mut key = vec![0u8; 32];
        hk.expand(b"journal:key", &mut key)
            .map_err(|e| CryptError::KeyDerivation(e.to_string()))?;

        Ok(key)
    }

    /// Encrypt data using AES-256-GCM. `aad` (additional authenticated data) is
    /// bound into the auth tag — pass `None` for an empty AAD (identical tag to
    /// the no-AAD case). TS binds `aadForFile`/`aadForPart`/`aadForZip` here.
    pub fn encrypt_data_gcm(
        &self,
        data: &[u8],
        iv: &[u8],
        key: &[u8],
        aad: Option<&[u8]>,
    ) -> Result<Vec<u8>, CryptError> {
        let cipher = Aes256Gcm::new_from_slice(key).map_err(|e| CryptError::Encryption(e.to_string()))?;

        let nonce = Nonce::try_from(iv).map_err(|_| CryptError::Encryption("invalid nonce length".to_string()))?;

        cipher
            .encrypt(
                &nonce,
                Payload {
                    msg: data,
                    aad: aad.unwrap_or(&[]),
                },
            )
            .map_err(|e| CryptError::Encryption(e.to_string()))
    }

    /// Decrypt data using AES-256-GCM. `aad` must match what was used to encrypt.
    pub fn decrypt_data_gcm(
        &self,
        data: &[u8],
        iv: &[u8],
        key: &[u8],
        aad: Option<&[u8]>,
    ) -> Result<Vec<u8>, CryptError> {
        let cipher = Aes256Gcm::new_from_slice(key).map_err(|e| CryptError::Decryption(e.to_string()))?;

        let nonce = Nonce::try_from(iv).map_err(|_| CryptError::Decryption("invalid nonce length".to_string()))?;

        cipher
            .decrypt(
                &nonce,
                Payload {
                    msg: data,
                    aad: aad.unwrap_or(&[]),
                },
            )
            .map_err(|e| CryptError::Decryption(e.to_string()))
    }

    /// Encrypt data in chunks for large files. The same `aad` is bound into
    /// every chunk's auth tag (matching the TS CryptoWorker, which passes the
    /// same `additionalData` on each chunk).
    pub fn encrypt_chunked_gcm(
        &self,
        data: &[u8],
        iv: &[u8],
        key: &[u8],
        chunk_size: usize,
        aad: Option<&[u8]>,
    ) -> Result<Vec<u8>, CryptError> {
        let cipher = Aes256Gcm::new_from_slice(key).map_err(|e| CryptError::Encryption(e.to_string()))?;
        let aad = aad.unwrap_or(&[]);

        let mut result = Vec::new();

        for (chunk_index, chunk) in data.chunks(chunk_size).enumerate() {
            let chunk_nonce = self.derive_chunk_nonce(iv, chunk_index as u64);
            let nonce = Nonce::try_from(chunk_nonce.as_slice())
                .map_err(|_| CryptError::Encryption("invalid nonce length".to_string()))?;

            let encrypted = cipher
                .encrypt(&nonce, Payload { msg: chunk, aad })
                .map_err(|e| CryptError::Encryption(e.to_string()))?;

            result.extend_from_slice(&encrypted);
        }

        Ok(result)
    }

    /// Decrypt chunked data. `aad` must match what was used to encrypt and is
    /// applied to every chunk.
    pub fn decrypt_chunked_gcm(
        &self,
        data: &[u8],
        iv: &[u8],
        key: &[u8],
        chunk_size: usize,
        aad: Option<&[u8]>,
    ) -> Result<Vec<u8>, CryptError> {
        let cipher = Aes256Gcm::new_from_slice(key).map_err(|e| CryptError::Decryption(e.to_string()))?;
        let aad = aad.unwrap_or(&[]);

        let chunk_with_tag = chunk_size + GCM_TAG_SIZE;
        let mut result = Vec::new();

        for (chunk_index, chunk) in data.chunks(chunk_with_tag).enumerate() {
            let chunk_nonce = self.derive_chunk_nonce(iv, chunk_index as u64);
            let nonce = Nonce::try_from(chunk_nonce.as_slice())
                .map_err(|_| CryptError::Decryption("invalid nonce length".to_string()))?;

            let decrypted = cipher
                .decrypt(&nonce, Payload { msg: chunk, aad })
                .map_err(|e| CryptError::Decryption(e.to_string()))?;

            result.extend_from_slice(&decrypted);
        }

        Ok(result)
    }

    /// Stream-decrypt a chunked GCM range: consumes the encrypted byte stream for a
    /// range (fetched via `get_streamed` using the range from
    /// `calc_encrypted_range`) and forwards the requested plaintext window to `out`
    /// chunk-by-chunk — it never materializes the whole encrypted range nor the whole
    /// plaintext. Mirrors TS `decryptChunkedRangeStreamedGCM`.
    ///
    /// The cipher is built once and reused for every chunk, so this issues exactly one
    /// AES-GCM decrypt per chunk — the same count as the whole-buffer path. Incoming
    /// network buffers are re-framed into encrypted GCM chunks (`chunk_size + tag`);
    /// the only copy is for a chunk that straddles two network buffers — aligned or
    /// large reads are sliced in place via `Bytes`.
    ///
    /// Sends `Ok(window)` for each plaintext slice and returns the terminal `Err` (an
    /// upstream stream error or a decrypt failure) for the caller to forward once;
    /// returns `Ok(())` early if `out`'s receiver is gone.
    #[allow(clippy::too_many_arguments)]
    pub async fn decrypt_chunked_range_streamed_gcm(
        &self,
        mut enc_rx: mpsc::Receiver<Result<Vec<u8>, StorageError>>,
        iv: &[u8],
        key: &[u8],
        chunk_size: usize,
        first_chunk_index: u64,
        slice_start: usize,
        slice_end: usize,
        aad: Option<&[u8]>,
        out: &mpsc::Sender<Result<Vec<u8>, StorageError>>,
    ) -> Result<(), StorageError> {
        let cipher = Aes256Gcm::new_from_slice(key).map_err(|e| StorageError::Other(e.to_string()))?;
        let aad = aad.unwrap_or(&[]);
        let enc_chunk_size = chunk_size + GCM_TAG_SIZE;

        // Encrypted bytes received but not yet assembled into a full chunk. Drained
        // eagerly, so this holds < enc_chunk_size bytes between iterations.
        let mut buffered: VecDeque<Bytes> = VecDeque::new();
        let mut buffered_len = 0usize;

        let mut chunk_in_range = 0u64; // index of the next chunk within the range
        let mut plain_pos = 0usize; // plaintext bytes produced across the range so far

        while let Some(item) = enc_rx.recv().await {
            let buf = item?;
            if buf.is_empty() {
                continue;
            }
            buffered_len += buf.len();
            buffered.push_back(Bytes::from(buf));
            // Emit every complete chunk now buffered; the tail (< one chunk) waits.
            while buffered_len >= enc_chunk_size {
                let enc = take_front(&mut buffered, &mut buffered_len, enc_chunk_size);
                let win = self
                    .decrypt_range_window(
                        &cipher,
                        iv,
                        aad,
                        first_chunk_index,
                        &mut chunk_in_range,
                        &mut plain_pos,
                        slice_start,
                        slice_end,
                        &enc,
                    )
                    .map_err(|e| StorageError::Other(e.to_string()))?;
                if let Some(win) = win
                    && out.send(Ok(win)).await.is_err()
                {
                    return Ok(());
                }
            }
        }
        // Whatever remains is the file's final (possibly smaller) chunk.
        if buffered_len > 0 {
            let remaining = buffered_len;
            let enc = take_front(&mut buffered, &mut buffered_len, remaining);
            let win = self
                .decrypt_range_window(
                    &cipher,
                    iv,
                    aad,
                    first_chunk_index,
                    &mut chunk_in_range,
                    &mut plain_pos,
                    slice_start,
                    slice_end,
                    &enc,
                )
                .map_err(|e| StorageError::Other(e.to_string()))?;
            if let Some(win) = win {
                let _ = out.send(Ok(win)).await;
            }
        }
        Ok(())
    }

    /// Decrypt one assembled encrypted chunk and return its intersection with
    /// `[slice_start, slice_end)`, or `None` when the chunk falls fully outside it.
    /// Advances `chunk_in_range` (for the chunk's nonce) and `plain_pos` (plaintext
    /// produced so far). Mirrors the TS `decryptWindow` closure.
    #[allow(clippy::too_many_arguments)]
    fn decrypt_range_window(
        &self,
        cipher: &Aes256Gcm,
        iv: &[u8],
        aad: &[u8],
        first_chunk_index: u64,
        chunk_in_range: &mut u64,
        plain_pos: &mut usize,
        slice_start: usize,
        slice_end: usize,
        enc: &[u8],
    ) -> Result<Option<Vec<u8>>, CryptError> {
        let chunk_nonce = self.derive_chunk_nonce(iv, first_chunk_index + *chunk_in_range);
        *chunk_in_range += 1;
        let nonce = Nonce::try_from(chunk_nonce.as_slice())
            .map_err(|_| CryptError::Decryption("invalid nonce length".to_string()))?;
        let decrypted = cipher
            .decrypt(&nonce, Payload { msg: enc, aad })
            .map_err(|e| CryptError::Decryption(e.to_string()))?;

        let start = *plain_pos;
        let win_start = slice_start.max(start) - start;
        let win_end = slice_end.min(start + decrypted.len()).saturating_sub(start);
        *plain_pos += decrypted.len();
        if win_end > win_start {
            Ok(Some(decrypted[win_start..win_end].to_vec()))
        } else {
            Ok(None)
        }
    }

    /// Derive nonce for a specific chunk by incrementing the base IV (big-endian)
    fn derive_chunk_nonce(&self, base_iv: &[u8], chunk_index: u64) -> Vec<u8> {
        let mut nonce = base_iv.to_vec();
        if nonce.len() < 12 {
            nonce.resize(12, 0);
        }

        // Increment IV by chunk_index (big-endian counter style, matching TypeScript)
        let mut carry = chunk_index;
        for i in (0..12).rev() {
            if carry == 0 {
                break;
            }
            let sum = nonce[i] as u64 + (carry & 0xff);
            nonce[i] = (sum & 0xff) as u8;
            carry = (carry >> 8) + (sum >> 8);
        }

        nonce
    }
}

/// Take exactly `n` bytes off the front of `buffered`. Zero-copy when the head
/// buffer alone covers them; one allocation when `n` straddles buffers. Callers only
/// ask for `n` once at least `n` bytes are buffered. Mirrors the TS `take` closure.
fn take_front(buffered: &mut VecDeque<Bytes>, buffered_len: &mut usize, n: usize) -> Bytes {
    let head_len = buffered[0].len();
    if head_len == n {
        *buffered_len -= n;
        return buffered.pop_front().unwrap();
    }
    if head_len > n {
        let mut head = buffered.pop_front().unwrap();
        let taken = head.split_to(n);
        buffered.push_front(head);
        *buffered_len -= n;
        return taken;
    }
    let mut out = BytesMut::with_capacity(n);
    while out.len() < n {
        let need = n - out.len();
        let mut b = buffered.pop_front().unwrap();
        if b.len() <= need {
            out.extend_from_slice(&b);
        } else {
            out.extend_from_slice(&b.split_to(need));
            buffered.push_front(b);
        }
    }
    *buffered_len -= n;
    out.freeze()
}

impl Default for StorageCrypt {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encrypt_decrypt_gcm() {
        let crypt = StorageCrypt::new();
        let data = b"Hello, World!";
        let key = [0u8; 32];
        let iv = [0u8; 12];

        let encrypted = crypt.encrypt_data_gcm(data, &iv, &key, None).unwrap();
        let decrypted = crypt.decrypt_data_gcm(&encrypted, &iv, &key, None).unwrap();

        assert_eq!(data.as_slice(), decrypted.as_slice());
    }

    #[test]
    fn test_chunked_encrypt_decrypt() {
        let crypt = StorageCrypt::new();
        let data = vec![0u8; 200_000]; // Larger than one chunk
        let key = [0u8; 32];
        let iv = [0u8; 12];

        let encrypted = crypt
            .encrypt_chunked_gcm(&data, &iv, &key, GCM_CHUNK_SIZE, None)
            .unwrap();
        let decrypted = crypt
            .decrypt_chunked_gcm(&encrypted, &iv, &key, GCM_CHUNK_SIZE, None)
            .unwrap();

        assert_eq!(data, decrypted);
    }

    #[test]
    fn test_aad_must_match() {
        let crypt = StorageCrypt::new();
        let data = vec![7u8; 100_000]; // spans two chunks
        let key = [0u8; 32];
        let iv = [0u8; 12];
        let aad = b"blob-name|100000".as_slice();

        let encrypted = crypt
            .encrypt_chunked_gcm(&data, &iv, &key, GCM_CHUNK_SIZE, Some(aad))
            .unwrap();

        // Right AAD decrypts.
        let ok = crypt
            .decrypt_chunked_gcm(&encrypted, &iv, &key, GCM_CHUNK_SIZE, Some(aad))
            .unwrap();
        assert_eq!(data, ok);

        // Wrong / missing AAD fails the auth tag (this is the interop guarantee).
        assert!(
            crypt
                .decrypt_chunked_gcm(&encrypted, &iv, &key, GCM_CHUNK_SIZE, None)
                .is_err()
        );
        assert!(
            crypt
                .decrypt_chunked_gcm(&encrypted, &iv, &key, GCM_CHUNK_SIZE, Some(b"wrong".as_slice()))
                .is_err()
        );
    }

    #[test]
    fn test_calc_encrypted_range_last_partial_chunk() {
        // 100_000 bytes, 64 KiB chunks → chunk0 = 64KiB+16, chunk1 = 35_536+16.
        let total = 100_000u64;
        let rng = calc_encrypted_range(0, total, GCM_CHUNK_SIZE, total);
        let full = (GCM_CHUNK_SIZE + GCM_TAG_SIZE) as u64;
        let last_plain = total - GCM_CHUNK_SIZE as u64; // 35_536
        assert_eq!(rng.encrypted_length, full + (last_plain + GCM_TAG_SIZE as u64));
        // A request past EOF clamps to the real last chunk, not a full one.
        let rng2 = calc_encrypted_range(0, total + 50_000, GCM_CHUNK_SIZE, total);
        assert_eq!(rng2.encrypted_length, rng.encrypted_length);
    }

    /// Stream-decrypt must equal the whole-buffer decrypt regardless of how the
    /// encrypted range is re-framed by the network (here: tiny 7-byte buffers that
    /// never align to a GCM chunk), for both a full read and an interior sub-range.
    #[tokio::test]
    async fn test_decrypt_chunked_range_streamed_matches_buffered() {
        let crypt = StorageCrypt::new();
        let total = 200_000usize; // 3 full 64 KiB chunks + a partial tail
        let data: Vec<u8> = (0..total).map(|i| (i * 31 + 7) as u8).collect();
        let key = [3u8; 32];
        let iv = [0u8; 12];
        let aad = b"blob|200000".as_slice();

        let encrypted = crypt
            .encrypt_chunked_gcm(&data, &iv, &key, GCM_CHUNK_SIZE, Some(aad))
            .unwrap();

        // (offset, length) windows: full file, a slice spanning a chunk boundary,
        // and a slice fully inside one interior chunk.
        for &(offset, length) in &[(0u64, total as u64), (60_000, 80_000), (70_000, 1_000)] {
            let rng = calc_encrypted_range(offset, length, GCM_CHUNK_SIZE, total as u64);
            let range_bytes =
                &encrypted[rng.encrypted_offset as usize..(rng.encrypted_offset + rng.encrypted_length) as usize];

            // Feed the range into the streamer in deliberately misaligned 7-byte buffers.
            let (enc_tx, enc_rx) = mpsc::channel::<Result<Vec<u8>, StorageError>>(8);
            let range_owned = range_bytes.to_vec();
            tokio::spawn(async move {
                for piece in range_owned.chunks(7) {
                    if enc_tx.send(Ok(piece.to_vec())).await.is_err() {
                        return;
                    }
                }
            });

            let (out_tx, mut out_rx) = mpsc::channel::<Result<Vec<u8>, StorageError>>(8);
            let collector = tokio::spawn(async move {
                let mut got = Vec::new();
                while let Some(item) = out_rx.recv().await {
                    got.extend_from_slice(&item.unwrap());
                }
                got
            });

            crypt
                .decrypt_chunked_range_streamed_gcm(
                    enc_rx,
                    &iv,
                    &key,
                    GCM_CHUNK_SIZE,
                    rng.first_chunk_index,
                    rng.slice_start,
                    rng.slice_end,
                    Some(aad),
                    &out_tx,
                )
                .await
                .unwrap();
            drop(out_tx);

            let got = collector.await.unwrap();
            let want = &data[offset as usize..(offset + length) as usize];
            assert_eq!(got.as_slice(), want, "offset={offset} length={length}");
        }
    }

    #[test]
    fn test_derive_seed_keys() {
        let seed = [1u8; 32];
        let (iv, key) = StorageCrypt::derive_seed_keys(&seed).unwrap();

        assert_eq!(iv.len(), 12);
        assert_eq!(key.len(), 32);
    }
}
