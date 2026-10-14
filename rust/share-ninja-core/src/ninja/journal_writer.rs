//! NinjaJournalWriter - Manages journal entries for NinjaStorageV2

use protobuf::Message;
use sha2::{Digest, Sha256};
use std::time::{Duration, Instant};

use crate::transfers::{StorageDest, StorageError, StorageInfo, StorageStatInfo};

use super::storage_crypt::{GCM_CHUNK_SIZE, StorageCrypt};
use share_ninja_proto::journal_proto::{
    AddDirOp, AddFileOp, AddPartOp, AddSymlinkOp, AddZipOp, BeginMultipartOp, CancelMultipartOp, CompleteMultipartOp,
    DeleteDirOp, DeleteFileOp, Journal, JournalOp, journal_op,
};

/// NinjaJournalWriter manages journal entries and writes them to storage
pub struct NinjaJournalWriter {
    info: StorageInfo,
    master_key: Option<Vec<u8>>,
    ops: Vec<JournalOp>,
    prev_hash: Option<Vec<u8>>,
    storage_crypt: StorageCrypt,
    /// Wall-clock of the last journal flush (TS `lastTS`). Used by the
    /// time-based batching threshold in [`write`](Self::write).
    last_ts: Instant,
    /// True whenever `info` differs from what storage#info last persisted
    /// (stats changed or the journal pointer moved). Lets a forced write with
    /// no pending ops skip the redundant storage#info PUT. Mirrors TS
    /// `NinjaJournalWriter.infoDirty`.
    info_dirty: bool,
}

impl NinjaJournalWriter {
    pub fn new(info: StorageInfo, master_key: Option<Vec<u8>>, prev_hash: Option<Vec<u8>>) -> Self {
        Self {
            info,
            master_key,
            ops: Vec::new(),
            prev_hash,
            storage_crypt: StorageCrypt::new(),
            last_ts: Instant::now(),
            info_dirty: true,
        }
    }

    pub fn add_file_op(&mut self, op: AddFileOp) {
        let mut journal_op = JournalOp::new();
        journal_op.msg = Some(journal_op::Msg::AddFile(op));
        self.ops.push(journal_op);
    }

    pub fn add_dir_op(&mut self, op: AddDirOp) {
        let mut journal_op = JournalOp::new();
        journal_op.msg = Some(journal_op::Msg::AddDir(op));
        self.ops.push(journal_op);
    }

    pub fn add_symlink_op(&mut self, op: AddSymlinkOp) {
        let mut journal_op = JournalOp::new();
        journal_op.msg = Some(journal_op::Msg::AddSymlink(op));
        self.ops.push(journal_op);
    }

    pub fn add_zip_op(&mut self, op: AddZipOp) {
        let mut journal_op = JournalOp::new();
        journal_op.msg = Some(journal_op::Msg::AddZip(op));
        self.ops.push(journal_op);
    }

    pub fn delete_file_op(&mut self, op: DeleteFileOp) {
        let mut journal_op = JournalOp::new();
        journal_op.msg = Some(journal_op::Msg::DeleteFile(op));
        self.ops.push(journal_op);
    }

    pub fn delete_dir_op(&mut self, op: DeleteDirOp) {
        let mut journal_op = JournalOp::new();
        journal_op.msg = Some(journal_op::Msg::DeleteDir(op));
        self.ops.push(journal_op);
    }

    pub fn begin_multipart_op(&mut self, op: BeginMultipartOp) {
        let mut journal_op = JournalOp::new();
        journal_op.msg = Some(journal_op::Msg::BeginMultipart(op));
        self.ops.push(journal_op);
    }

    pub fn add_part_op(&mut self, op: AddPartOp) {
        let mut journal_op = JournalOp::new();
        journal_op.msg = Some(journal_op::Msg::AddPart(op));
        self.ops.push(journal_op);
    }

    pub fn complete_multipart_op(&mut self, op: CompleteMultipartOp) {
        let mut journal_op = JournalOp::new();
        journal_op.msg = Some(journal_op::Msg::CompleteMultipart(op));
        self.ops.push(journal_op);
    }

    pub fn cancel_multipart_op(&mut self, op: CancelMultipartOp) {
        let mut journal_op = JournalOp::new();
        journal_op.msg = Some(journal_op::Msg::CancelMultipart(op));
        self.ops.push(journal_op);
    }

    pub fn update_stats(&mut self, stats: StorageStatInfo) {
        self.info.stats = stats;
        self.info_dirty = true;
    }

    fn encrypt_journal(&self, data: &[u8]) -> Result<Vec<u8>, StorageError> {
        if let Some(ref master_key) = self.master_key {
            let encryption_key =
                StorageCrypt::derive_journal_key(master_key).map_err(|e| StorageError::Other(e.to_string()))?;
            // Generate random 12-byte IV
            let iv: [u8; 12] = rand::random();
            // The journal uses no AAD (TS binds AAD only on blob/part/zip data).
            // Mirror TS `encryptDataWorkerGCM`, which always passes
            // `chunkSize: GCM_CHUNK_SIZE` → the CryptoWorker chunks the journal
            // into 64 KiB GCM chunks (per-chunk IV = baseIV + index, own tag).
            // Single-shot only matched TS for journals <= 64 KiB; larger ones
            // produced an incompatible layout that failed the auth tag on read.
            let encrypted = self
                .storage_crypt
                .encrypt_chunked_gcm(data, &iv, &encryption_key, GCM_CHUNK_SIZE, None)
                .map_err(|e| StorageError::Other(e.to_string()))?;
            // Prepend IV to encrypted data
            let mut result = Vec::with_capacity(12 + encrypted.len());
            result.extend_from_slice(&iv);
            result.extend_from_slice(&encrypted);
            Ok(result)
        } else {
            Ok(data.to_vec())
        }
    }

    pub fn decrypt_journal(master_key: Option<&[u8]>, data: &[u8]) -> Result<Vec<u8>, StorageError> {
        if let Some(key) = master_key {
            if data.len() < 12 {
                return Err(StorageError::Other("Journal data too short".to_string()));
            }
            // Extract IV from first 12 bytes
            let iv = &data[..12];
            let encrypted_data = &data[12..];
            let encryption_key =
                StorageCrypt::derive_journal_key(key).map_err(|e| StorageError::Other(e.to_string()))?;
            let crypt = StorageCrypt::new();
            // Mirror TS `decryptDataWorkerGCM` (chunked, GCM_CHUNK_SIZE). For
            // journals <= 64 KiB this is byte-identical to single-shot, so
            // previously-written small journals still decrypt.
            crypt
                .decrypt_chunked_gcm(encrypted_data, iv, &encryption_key, GCM_CHUNK_SIZE, None)
                .map_err(|e| StorageError::Other(e.to_string()))
        } else {
            Ok(data.to_vec())
        }
    }

    pub async fn write<S: StorageDest + ?Sized>(&mut self, storage: &S, force: bool) -> Result<(), StorageError> {
        match self.prepare_write(force)? {
            Some(prepared) => {
                let res = prepared.upload(storage).await;
                self.finish_upload(&prepared, res.is_ok());
                res
            }
            None => Ok(()),
        }
    }

    /// Report how the upload of `prepared` ended. Landed: the ops it carried
    /// leave the queue, and the journal number, hash chain and timer move on.
    /// Failed: nothing moves — the next flush writes the same ops again (plus
    /// any queued since) under the same number — and storage#info is due
    /// again. Uploads are one at a time (the caller serializes them).
    pub fn finish_upload(&mut self, prepared: &PreparedJournalWrite, ok: bool) {
        if !ok {
            self.info_dirty = true;
            return;
        }
        if let Some(hash) = &prepared.hash {
            self.ops.drain(..prepared.n_ops);
            self.info.last_journal_num = prepared.journal_num;
            self.prev_hash = Some(hash.clone());
            // TS addWrite resets lastTS whenever a journal is actually written.
            self.last_ts = Instant::now();
        }
    }

    /// Test-only: backdate the batching timer so the next non-forced write flushes.
    #[cfg(test)]
    pub(crate) fn make_due(&mut self) {
        if let Some(ts) = Instant::now().checked_sub(Duration::from_secs(61)) {
            self.last_ts = ts;
        }
    }

    /// The fast half of a flush: decide whether one is due, build the journal
    /// from the queued ops, encrypt it and render `storage#info` — everything
    /// except the network writes. Callers run this under their own lock and
    /// then `upload()` the result WITHOUT holding it, so concurrent writers
    /// keep committing while the two PUTs are in flight. Nothing moves here:
    /// `finish_upload` advances the writer once the upload landed.
    pub fn prepare_write(&mut self, force: bool) -> Result<Option<PreparedJournalWrite>, StorageError> {
        // Mirror TS NinjaJournal.write: flush only when forced, when 1000+ ops
        // have queued, or when there's >=1 op and >60s have elapsed since the
        // last flush. Below that it's a no-op — ops keep accumulating in memory
        // and get batched into a single journal file instead of one file per op.
        let should_flush = force
            || self.ops.len() >= 1000
            || (!self.ops.is_empty() && self.last_ts.elapsed() > Duration::from_secs(60));
        if !should_flush {
            return Ok(None);
        }

        let wrote_journal = !self.ops.is_empty();
        if !wrote_journal && !self.info_dirty {
            tracing::debug!("meta unchanged, skipping write");
            return Ok(None);
        }

        let mut journal_num = self.info.last_journal_num;
        let mut journal_blob: Option<(String, Vec<u8>)> = None;
        let mut hash: Option<Vec<u8>> = None;
        if wrote_journal {
            // Mirror TS NinjaJournal.addWrite: the FIRST journal is number 0,
            // then +1 each subsequent write. `last_journal_num` is the 0-based
            // index of the journal just written (not a count), so it must stay
            // `None -> 0`, not `unwrap_or(0) + 1` (which skipped journal 0 and
            // left `lastJournalNum` off by one vs TS-written shares).
            let num = match journal_num {
                None => 0,
                Some(n) => n + 1,
            };
            journal_num = Some(num);

            let mut journal = Journal::new();
            journal.version = 2;
            journal.prev_sha256 = self.prev_hash.clone();
            journal.ops = self.ops.clone();

            let journal_bin = journal
                .write_to_bytes()
                .map_err(|e| StorageError::Other(format!("Failed to encode journal: {}", e)))?;
            hash = Some(Sha256::digest(&journal_bin).to_vec());

            let encrypted_journal = self.encrypt_journal(&journal_bin)?;
            journal_blob = Some((format!("meta/journal-{:06}", num), encrypted_journal));
        }

        // Always write storage#info
        let info_json = serde_json::json!({
            "version": self.info.version,
            "ninjaParts": self.info.ninja_parts,
            "zipBatches": self.info.zip_batches,
            "isEncrypted": self.info.is_encrypted,
            "lastJournalNum": journal_num,
            "stats": {
                "metaState": self.info.stats.meta_state.as_i32(),
                "totalSize": self.info.stats.total_size,
            }
        });
        self.info_dirty = false;

        Ok(Some(PreparedJournalWrite {
            journal: journal_blob,
            info: info_json.to_string().into_bytes(),
            n_ops: self.ops.len(),
            journal_num,
            hash,
        }))
    }
}

/// A journal flush prepared by [`NinjaJournalWriter::prepare_write`]: the
/// encrypted journal blob (when ops were pending) plus the rendered
/// `storage#info`. `upload` is the slow network half — run it without holding
/// the ninja state lock.
pub struct PreparedJournalWrite {
    journal: Option<(String, Vec<u8>)>,
    info: Vec<u8>,
    /// What `finish_upload` applies once this landed: the ops it carried
    /// (the head of the queue), the journal number and the new chain hash.
    n_ops: usize,
    journal_num: Option<u32>,
    hash: Option<Vec<u8>>,
}

impl PreparedJournalWrite {
    pub async fn upload<S: StorageDest + ?Sized>(&self, storage: &S) -> Result<(), StorageError> {
        if let Some((journal_key, encrypted_journal)) = &self.journal {
            tracing::info!("Writing journal ops {}", journal_key);
            storage.put_file(journal_key, encrypted_journal, None, None).await?;
        }
        tracing::info!("Writing storage info");
        storage.put_file("storage#info", &self.info, None, None).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::storage_crypt::GCM_TAG_SIZE;
    use super::*;

    fn writer(master_key: Option<Vec<u8>>) -> NinjaJournalWriter {
        NinjaJournalWriter::new(StorageInfo::default(), master_key, None)
    }

    /// A journal larger than one GCM chunk must round-trip. This is the
    /// regression: TS encrypts the journal chunked (64 KiB chunks, per-chunk
    /// IV + tag), so single-shot Rust output could not be decrypted by TS and
    /// vice versa once the journal exceeded 64 KiB.
    #[test]
    fn large_journal_round_trips() {
        let master_key = vec![0x42u8; 32];
        let w = writer(Some(master_key.clone()));

        // 200_000 bytes spans 4 chunks (3 full + a partial).
        let data = vec![0xABu8; 200_000];
        let enc = w.encrypt_journal(&data).unwrap();
        let dec = NinjaJournalWriter::decrypt_journal(Some(&master_key), &enc).unwrap();
        assert_eq!(data, dec);
    }

    /// The on-disk layout must be the chunked one (12-byte IV + each chunk
    /// carrying its own tag), NOT single-shot (one trailing tag). If this ever
    /// fails, Rust has diverged from the TS CryptoWorker again.
    #[test]
    fn large_journal_uses_chunked_layout() {
        let master_key = vec![0x07u8; 32];
        let w = writer(Some(master_key));

        let len = 200_000usize;
        let data = vec![0u8; len];
        let enc = w.encrypt_journal(&data).unwrap();

        let num_chunks = len.div_ceil(GCM_CHUNK_SIZE); // 4
        let chunked_len = 12 + len + num_chunks * GCM_TAG_SIZE; // IV + plaintext + per-chunk tags
        let single_shot_len = 12 + len + GCM_TAG_SIZE; // IV + plaintext + one tag
        assert_eq!(enc.len(), chunked_len);
        assert_ne!(enc.len(), single_shot_len);
    }

    /// Journals <= 64 KiB are byte-identical to the old single-shot output
    /// (one chunk, chunk-0 IV == base IV), so journals written before this fix
    /// still decrypt. Verify by re-encrypting single-shot with the same IV.
    #[test]
    fn small_journal_matches_single_shot() {
        let master_key = vec![0x11u8; 32];
        let w = writer(Some(master_key.clone()));

        let data = vec![0x5Au8; 1024];
        let enc = w.encrypt_journal(&data).unwrap();

        let iv = &enc[..12];
        let journal_key = StorageCrypt::derive_journal_key(&master_key).unwrap();
        let single_shot = StorageCrypt::new()
            .encrypt_data_gcm(&data, iv, &journal_key, None)
            .unwrap();
        assert_eq!(&enc[12..], single_shot.as_slice());
    }

    #[test]
    fn no_master_key_is_passthrough() {
        let w = writer(None);
        let data = vec![1u8, 2, 3, 4];
        let enc = w.encrypt_journal(&data).unwrap();
        assert_eq!(enc, data);
        let dec = NinjaJournalWriter::decrypt_journal(None, &enc).unwrap();
        assert_eq!(dec, data);
    }
}
