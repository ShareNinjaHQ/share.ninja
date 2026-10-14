//! NinjaStorageV2 - Encrypted storage with journaling

use async_trait::async_trait;
use protobuf::Message;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::sync::{Mutex, RwLock, mpsc};
use uuid::Uuid;
use zip::write::SimpleFileOptions;

use crate::transfers::{
    ChangeType, SearchOptions, SearchResult, Storage, StorageChange, StorageDest, StorageDestMultipart, StorageError,
    StorageFileAttrs, StorageFileType, StorageInfo, StorageMeta, StorageMetaState, StorageMultipartUpload,
    StorageObjectInfo, StoragePartInfo, StoragePutConditions, StoragePutResult, StorageSearchQuery, StorageSource,
    StorageSourceType, StorageStatInfo, reject_conditions,
};
use futures::StreamExt;

use super::journal_writer::NinjaJournalWriter;
use super::storage_crypt::{GCM_CHUNK_SIZE, StorageCrypt, calc_encrypted_range};
use share_ninja_proto::journal_proto::{
    AddDirOp, AddFileOp, AddPartOp, AddSymlinkOp, AddZipOp, BeginMultipartOp, CancelMultipartOp, CompleteMultipartOp,
    DeleteDirOp, DeleteFileOp, FileMeta, Journal, ZipOp, journal_op,
};

const BLOB_PREFIX: &str = "blobs/";

// AAD (additional authenticated data) helpers — mirror TS `aadForFile` /
// `aadForPart` / `aadForZip` in NinjaStorageV2.ts. The AES-GCM auth tag binds
// each blob/part/zip entry to these bytes, so they MUST match TS byte-for-byte
// (UTF-8 of `key|size`, `key|partNumber|offset|size`, `nameInZip|size`) for
// cross-implementation decryption to succeed.
fn aad_for_file(storage_key: &str, size: u64) -> Vec<u8> {
    format!("{}|{}", storage_key, size).into_bytes()
}

fn aad_for_part(storage_key: &str, part_number: u32, offset: u64, size: u64) -> Vec<u8> {
    format!("{}|{}|{}|{}", storage_key, part_number, offset, size).into_bytes()
}

fn aad_for_zip(name_in_zip: &str, size: u64) -> Vec<u8> {
    format!("{}|{}", name_in_zip, size).into_bytes()
}

/// The AAD key is the un-prefixed blob name. `encrypt` uses the bare name;
/// journal replay prepends `BLOB_PREFIX` to `storage_key`, so strip it back off
/// at read time (TS `getLow` does the same).
fn aad_key(storage_key: &str) -> &str {
    storage_key.strip_prefix(BLOB_PREFIX).unwrap_or(storage_key)
}

/// Build a journal `FileMeta` from optional `StorageFileAttrs`, mirroring TS
/// `fileAttrsFromAttrs` (which returns `undefined` when `attrs` is undefined).
/// `mtime` is stored as nanoseconds (TS: `getTime() * 1_000_000`).
fn file_meta_from_attrs(attrs: Option<&StorageFileAttrs>) -> ::protobuf::MessageField<FileMeta> {
    let attrs = match attrs {
        Some(a) => a,
        None => return ::protobuf::MessageField::none(),
    };
    if attrs.mtime.is_none()
        && attrs.posix_permissions.is_none()
        && attrs.email_subject.is_none()
        && attrs.gdrive_file_id.is_none()
        && attrs.gdrive_mime_type.is_none()
    {
        return ::protobuf::MessageField::none();
    }
    ::protobuf::MessageField::some(FileMeta {
        mtime: attrs.mtime.map(|t| t.timestamp_millis() * 1_000_000),
        posix_permissions: attrs.posix_permissions,
        email_subject: attrs.email_subject.clone(),
        gdrive_file_id: attrs.gdrive_file_id.clone(),
        gdrive_mime_type: attrs.gdrive_mime_type.clone(),
        special_fields: Default::default(),
    })
}

/// Apply a journal `FileMeta` onto `StorageFileAttrs`, mirroring TS `setMeta`.
fn apply_meta_to_attrs(meta: &FileMeta, attrs: &mut StorageFileAttrs) {
    attrs.email_subject = meta.email_subject.clone();
    attrs.gdrive_file_id = meta.gdrive_file_id.clone();
    attrs.gdrive_mime_type = meta.gdrive_mime_type.clone();
    attrs.posix_permissions = meta.posix_permissions;
    if let Some(ns) = meta.mtime {
        attrs.mtime = chrono::DateTime::from_timestamp_millis(ns / 1_000_000);
    }
}

/// Parts tracking element
/// Backend-private pointer to one encrypted blob on the inner storage — the
/// payload the TS `NinjaStorageObjectInfo` subclass carries. Never exposed on
/// the shared `StorageObjectInfo`.
#[derive(Debug, Clone)]
struct NinjaBlobRef {
    /// Prefixed key on the inner storage (`""` = no blob, e.g. an empty file).
    storage_key: String,
    seed: Option<Vec<u8>>,
    /// Plaintext size (AAD binding + encrypted-range calc).
    size: u64,
    /// Zip-batched entries: member name inside the batch blob.
    name_in_zip: Option<String>,
}

/// One journaled part of a multipart file: its blob plus coordinates.
#[derive(Debug, Clone)]
struct NinjaPart {
    number: u32,
    offset: u64,
    size: u64,
    is_last: bool,
    storage_key: String,
    seed: Option<Vec<u8>>,
}

struct NinjaPartsElem {
    parts: Vec<NinjaPart>,
    is_complete: bool,
}

/// Result of [`NinjaStorageV2::put_blob`]: the bare blob name plus its seed and
/// plaintext size.
struct BlobUpload {
    storage_key: String,
    seed: Option<Vec<u8>>,
    size: u64,
}

/// NinjaStorageV2 parameters
#[derive(Debug, Clone)]
pub struct NinjaStorageV2Params {
    pub master_key: Option<Vec<u8>>,
    pub use_zip_batches: bool,
    pub zip_batch_size: usize,
    pub zip_max_file_size: usize,
    pub zip_max_files: usize,
    pub expand_zip_batches: bool,
    pub preload_zip_batches: bool,
    pub prefix: String,
}

impl Default for NinjaStorageV2Params {
    fn default() -> Self {
        Self {
            master_key: None,
            use_zip_batches: true,
            zip_batch_size: 10 * 1024 * 1024,
            zip_max_file_size: 1024 * 1024,
            zip_max_files: 1000,
            expand_zip_batches: true,
            preload_zip_batches: false,
            prefix: String::new(),
        }
    }
}

/// Zip batch writer for accumulating small files
struct NinjaZipWriter {
    /// One writer for the whole batch: entries are appended in place and the
    /// central directory is written once, by `finish` in `flush_zip`.
    zip_writer: zip::ZipWriter<std::io::Cursor<Vec<u8>>>,
    zip_blob_name: String,
    zip_journal: Vec<ZipOp>,
    zip_length: usize,
    zip_n_files: usize,
    zip_name_counter: u32,
    storage_crypt: StorageCrypt,
}

impl NinjaZipWriter {
    fn new() -> Self {
        Self {
            zip_writer: zip::ZipWriter::new(std::io::Cursor::new(Vec::new())),
            zip_blob_name: Uuid::new_v4().to_string(),
            zip_journal: Vec::new(),
            zip_length: 0,
            zip_n_files: 0,
            zip_name_counter: 0,
            storage_crypt: StorageCrypt::new(),
        }
    }

    fn add(
        &mut self,
        object: &str,
        data: &[u8],
        is_symlink: bool,
        attrs: Option<&StorageFileAttrs>,
    ) -> Result<(), StorageError> {
        let name_in_zip = format!("{:06}", self.zip_name_counter);
        self.zip_name_counter += 1;
        let data_len = data.len();

        let seed: [u8; 32] = rand::random();
        let (iv, encryption_key) =
            StorageCrypt::derive_seed_keys(&seed).map_err(|e| StorageError::Other(e.to_string()))?;

        // TS chunk-encrypts zip entries (64 KiB) with `aadForZip` — match both.
        let aad = aad_for_zip(&name_in_zip, data_len as u64);
        let enc_data = self
            .storage_crypt
            .encrypt_chunked_gcm(data, &iv, &encryption_key, GCM_CHUNK_SIZE, Some(&aad))
            .map_err(|e| StorageError::Other(e.to_string()))?;

        let options = SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);

        self.zip_writer
            .start_file(&name_in_zip, options)
            .map_err(|e| StorageError::Other(e.to_string()))?;
        self.zip_writer
            .write_all(&enc_data)
            .map_err(|e| StorageError::Other(e.to_string()))?;

        let zip_op = ZipOp {
            key: object.to_string(),
            name_in_zip,
            is_symlink,
            seed: Some(seed.to_vec()),
            size: data_len as i64,
            meta: file_meta_from_attrs(attrs),
            special_fields: Default::default(),
        };
        self.zip_journal.push(zip_op);
        self.zip_length += data_len;
        self.zip_n_files += 1;

        Ok(())
    }
}

/// Mutable state shared across `&self` dest methods, behind a single Mutex.
struct NinjaWriteState {
    storage_info: Option<StorageInfo>,
    journal_writer: Option<NinjaJournalWriter>,
    zip_writer: Option<NinjaZipWriter>,
    last_read_journal_num: i32,
    parts: HashMap<String, NinjaPartsElem>,
    /// Read-path payload per logical key (complete files, symlinks, zip
    /// entries) — populated alongside `cached_meta` by the journal apply and by
    /// this session's writes.
    blob_refs: HashMap<String, NinjaBlobRef>,
    prev_journal_hash: Option<Vec<u8>>,
    cached_meta: Option<StorageMeta>,
}

impl NinjaWriteState {
    /// Mirror one of our own writes into the cached meta so `get_attrs` answers
    /// reflect this session's writes — `apply_journal_diff` only surfaces them
    /// once the remote journal is re-read (idempotent: `update` replaces by key).
    fn note_write(&mut self, soi: StorageObjectInfo) {
        if let Some(ref mut m) = self.cached_meta {
            m.update(soi);
        }
    }

    fn note_delete(&mut self, key: &str) {
        if let Some(ref mut m) = self.cached_meta {
            m.delete(key);
        }
    }
}

/// A first read's own copy of everything a journal replay builds; installed
/// into `NinjaWriteState` in one go once the whole log is read.
struct FreshReplay {
    meta: StorageMeta,
    parts: HashMap<String, NinjaPartsElem>,
    blob_refs: HashMap<String, NinjaBlobRef>,
    prev_journal_hash: Option<Vec<u8>>,
    last_read_journal_num: i32,
}

/// Single-flight cache of fetched zip-batch blobs, keyed by storage key: a per-key
/// `OnceCell` lets concurrent reads of the same batch share one download. Mirrors the
/// TS `zipReaders` map (which caches the in-flight promise).
type ZipBlobCache = Arc<Mutex<HashMap<String, Arc<tokio::sync::OnceCell<Arc<Vec<u8>>>>>>>;

/// AAD derivation callback for `put_blob`: blob name → AAD bytes
/// (`aad_for_file` / `aad_for_part` with the coords already bound).
type AadFn = dyn Fn(&str) -> Vec<u8> + Send + Sync;

/// A part read keeps the presigned GET URLs of the next `PRESIGN_WINDOW` parts
/// minted ahead of the stream (`Storage::presign_urls`), refilling once fewer
/// than `PRESIGN_REFILL` hinted parts remain ahead — one small mint per few
/// parts, never at a part boundary. Mirrors TS `PRESIGN_WINDOW`/`PRESIGN_REFILL`.
const PRESIGN_WINDOW: usize = 10;
const PRESIGN_REFILL: usize = 5;

/// NinjaStorageV2 - Encrypted storage with journaling support
pub struct NinjaStorageV2 {
    storage: Arc<RwLock<dyn StorageSourceAndDest>>,
    // `Arc` so the streaming `get_streamed` producer task can observe cancellation.
    is_cancelled: Arc<AtomicBool>,
    params: NinjaStorageV2Params,
    storage_crypt: StorageCrypt,
    state: Mutex<NinjaWriteState>,
    /// Single-flight cache of fetched zip-batch blobs (see [`ZipBlobCache`]).
    zip_readers: ZipBlobCache,
    /// Serializes journal flush *uploads* (journal blob + `storage#info`), which
    /// run outside the state lock — see [`Self::journal_write`]. Held only by
    /// the task actually flushing; op-queuing never waits on it.
    journal_flush: Mutex<()>,
    /// Last successful reads of the inner storage's byte counters, keyed by
    /// getter — returned when the non-blocking `try_read` hits lock
    /// contention, so a sample repeats the previous value instead of
    /// contributing 0. `last_sent_progress` is `None` until the inner has
    /// answered once (a local inner never does).
    last_total_recv: AtomicU64,
    last_total_sent: AtomicU64,
    last_sent_progress: std::sync::Mutex<Option<u64>>,
    data_source_id: Option<String>,
}

/// Combined trait for storage that supports both source and dest operations
pub trait StorageSourceAndDest: StorageSource + StorageDest + StorageDestMultipart {}
impl<T: StorageSource + StorageDest + StorageDestMultipart> StorageSourceAndDest for T {}

impl NinjaStorageV2 {
    pub fn new(params: NinjaStorageV2Params, storage: Arc<RwLock<dyn StorageSourceAndDest>>) -> Self {
        Self {
            storage,
            is_cancelled: Arc::new(AtomicBool::new(false)),
            params,
            storage_crypt: StorageCrypt::new(),
            state: Mutex::new(NinjaWriteState {
                storage_info: None,
                journal_writer: None,
                zip_writer: None,
                last_read_journal_num: -1,
                parts: HashMap::new(),
                blob_refs: HashMap::new(),
                prev_journal_hash: None,
                cached_meta: None,
            }),
            zip_readers: Arc::new(Mutex::new(HashMap::new())),
            journal_flush: Mutex::new(()),
            last_total_recv: AtomicU64::new(0),
            last_total_sent: AtomicU64::new(0),
            last_sent_progress: std::sync::Mutex::new(None),
            data_source_id: None,
        }
    }

    /// Non-blocking sample of an inner-storage byte counter with a
    /// last-known-good fallback (see the `last_*` field docs).
    fn sample_inner(&self, cache: &AtomicU64, get: impl FnOnce(&dyn StorageSourceAndDest) -> u64) -> u64 {
        match self.storage.try_read() {
            Ok(g) => {
                let v = get(&*g);
                cache.store(v, Ordering::Relaxed);
                v
            }
            Err(_) => cache.load(Ordering::Relaxed),
        }
    }

    /// Flush the journal writer if due (or `force`d), uploading OUTSIDE the
    /// state lock. Holding the state lock across the journal + `storage#info`
    /// PUTs convoyed every concurrent writer for the duration of two network
    /// writes — visible as the progress bar dropping tens of MB every ~5 s
    /// while completed parts queued for their `dst_bytes_written` credit.
    ///
    /// `journal_flush` keeps uploads single-flight and ordered (journal N
    /// lands before N+1): the flusher takes it first, then briefly takes the
    /// state lock to prepare. Non-forced callers that find it busy just leave
    /// their ops queued for the next flush; forced callers wait, so a final
    /// or pause flush never returns with ops still pending. Lock order is
    /// always `journal_flush` → `state`, never the reverse.
    async fn journal_write(&self, force: bool) -> Result<(), StorageError> {
        let _flush = if force {
            self.journal_flush.lock().await
        } else {
            match self.journal_flush.try_lock() {
                Ok(guard) => guard,
                Err(_) => return Ok(()),
            }
        };

        let prepared = {
            let mut state = self.state.lock().await;
            match state.journal_writer.as_mut() {
                Some(jw) => jw.prepare_write(force)?,
                None => return Ok(()),
            }
        };

        if let Some(prepared) = prepared {
            let res = {
                let storage = self.storage.read().await;
                prepared.upload(&*storage).await
            };
            if let Some(jw) = self.state.lock().await.journal_writer.as_mut() {
                jw.finish_upload(&prepared, res.is_ok());
            }
            res?;
        }
        Ok(())
    }

    async fn update_storage_info(&self) -> Result<StorageMetaState, StorageError> {
        tracing::debug!("updating storage info");
        let info_key = format!("{}storage#info", self.params.prefix);

        let data = {
            let storage = self.storage.read().await;
            storage.get(&info_key, None, None).await
        };
        match data {
            Ok(data) => {
                tracing::debug!("storage info found");
                let utf8_string = String::from_utf8(data).map_err(|e| StorageError::Other(e.to_string()))?;
                let obj: serde_json::Value =
                    serde_json::from_str(&utf8_string).map_err(|e| StorageError::Other(e.to_string()))?;
                tracing::debug!("storage info parsed: {:?}", obj);
                let info = StorageInfo {
                    version: obj["version"].as_u64().map(|v| v as u32),
                    stats: StorageStatInfo {
                        // Mirror TS: read stats.metaState. Fall back to the legacy
                        // stats.isComplete bool for stores written before metaState existed.
                        meta_state: {
                            let s = &obj["stats"];
                            if let Some(ms) = s["metaState"].as_i64() {
                                StorageMetaState::from_i32(ms as i32)
                            } else if s["isComplete"].as_bool().unwrap_or(false) {
                                StorageMetaState::Complete
                            } else {
                                StorageMetaState::Incomplete
                            }
                        },
                        total_size: obj["stats"]["totalSize"].as_u64(),
                    },
                    ninja_parts: obj["ninjaParts"].as_bool(),
                    zip_batches: obj["zipBatches"].as_bool(),
                    is_encrypted: obj["isEncrypted"].as_bool(),
                    last_journal_num: obj["lastJournalNum"].as_u64().map(|v| v as u32),
                };

                if info.version != Some(2) {
                    return Err(StorageError::UnsupportedVersion(
                        info.version.map(|v| v.to_string()).unwrap_or_else(|| "none".into()),
                    ));
                }

                let is_complete = info.stats.is_complete();
                self.state.lock().await.storage_info = Some(info);

                if is_complete {
                    Ok(StorageMetaState::Complete)
                } else {
                    Ok(StorageMetaState::Incomplete)
                }
            }
            Err(StorageError::NoSuchKey(_)) => {
                tracing::debug!("Storage info not found, creating new");
                let info = StorageInfo {
                    version: Some(2),
                    zip_batches: Some(true),
                    ninja_parts: Some(true),
                    is_encrypted: Some(self.params.master_key.is_some()),
                    stats: StorageStatInfo {
                        meta_state: StorageMetaState::Unknown,
                        ..Default::default()
                    },
                    last_journal_num: None,
                };
                let mut state = self.state.lock().await;
                state.journal_writer = Some(NinjaJournalWriter::new(
                    info.clone(),
                    self.params.master_key.clone(),
                    None,
                ));
                state.storage_info = Some(info);
                Ok(StorageMetaState::Incomplete)
            }
            Err(e) => Err(e),
        }
    }

    /// Stream the plaintext of one blob/part `[offset,len)` into `tx` — the Rust
    /// analog of TS `getLowStreamed`. Encrypted blobs stream-decrypt the requested
    /// range chunk-by-chunk via `decrypt_chunked_range_streamed_gcm` (no materialize);
    /// plaintext blobs forward the inner source's stream chunk-by-chunk. Associated
    /// (not `&self`) so the spawned `get_streamed` producer can call it with cloned
    /// handles. Returns the terminal `Err` (the caller forwards it to `tx`).
    /// `part` = (number, offset) when the blob is one multipart part — switches
    /// the AAD binding to `aadForPart`.
    #[allow(clippy::too_many_arguments)] // associated fn: every handle is passed in
    async fn stream_low(
        storage: &Arc<RwLock<dyn StorageSourceAndDest>>,
        crypt: &StorageCrypt,
        master_key: Option<&[u8]>,
        blob: &NinjaBlobRef,
        part: Option<(u32, u64)>,
        offset: Option<u64>,
        length: Option<u64>,
        tx: &mpsc::Sender<Result<Vec<u8>, StorageError>>,
    ) -> Result<(), StorageError> {
        if master_key.is_some()
            && let Some(seed) = blob.seed.as_ref()
        {
            let (iv, encryption_key) =
                StorageCrypt::derive_seed_keys(seed).map_err(|e| StorageError::Other(e.to_string()))?;

            let off = offset.unwrap_or(0);
            let len = length.unwrap_or(blob.size);
            let rng = calc_encrypted_range(off, len, GCM_CHUNK_SIZE, blob.size);

            // AAD binds this blob to its key+size (and part coords). Mirror TS
            // `getLow`: `aadForPart` when the blob is a part, else `aadForFile`,
            // keyed on the un-prefixed blob name.
            let key = aad_key(&blob.storage_key);
            let aad = match part {
                Some((pn, po)) => aad_for_part(key, pn, po, blob.size),
                None => aad_for_file(key, blob.size),
            };

            let enc_rx = storage
                .read()
                .await
                .get_streamed(
                    &blob.storage_key,
                    Some(rng.encrypted_offset),
                    Some(rng.encrypted_length),
                )
                .await;

            // Stream-decrypt the range chunk-by-chunk into `tx` — never materialize the
            // whole encrypted range nor the whole plaintext (TS `getLowStreamed`).
            crypt
                .decrypt_chunked_range_streamed_gcm(
                    enc_rx,
                    &iv,
                    &encryption_key,
                    GCM_CHUNK_SIZE,
                    rng.first_chunk_index,
                    rng.slice_start,
                    rng.slice_end,
                    Some(&aad),
                    tx,
                )
                .await
        } else {
            // Plaintext passthrough: forward the inner source's chunks as they land.
            let mut rx = storage
                .read()
                .await
                .get_streamed(&blob.storage_key, offset, length)
                .await;
            while let Some(item) = rx.recv().await {
                let is_err = item.is_err();
                if tx.send(item).await.is_err() || is_err {
                    break;
                }
            }
            Ok(())
        }
    }

    /// Encrypt + upload a blob to the inner storage. Does not touch
    /// NinjaStorageV2's own mutable state; safe to call concurrently from
    /// multiple `&self` dest methods. Returns the *bare* blob name (the form the
    /// journal stores) plus seed/size.
    async fn put_blob(
        &self,
        object: &str,
        data: &[u8],
        encrypt: bool,
        aad_fn: Option<&AadFn>,
    ) -> Result<BlobUpload, StorageError> {
        let blob_name = Uuid::new_v4().to_string();
        let data_len = data.len() as u64;

        tracing::debug!("putblob {} -> {} data {}", object, blob_name, data.len());

        let mut seed_out: Option<Vec<u8>> = None;

        if encrypt && self.params.master_key.is_some() {
            let seed = rand::random::<[u8; 32]>().to_vec();
            let (iv, encryption_key) =
                StorageCrypt::derive_seed_keys(&seed).map_err(|e| StorageError::Other(e.to_string()))?;

            // AAD binds the blob to its (un-prefixed) name; the caller supplies
            // the exact TS shape (`aadForFile`/`aadForPart`).
            let aad = aad_fn.map(|f| f(&blob_name));
            let enc_data = self
                .storage_crypt
                .encrypt_chunked_gcm(data, &iv, &encryption_key, GCM_CHUNK_SIZE, aad.as_deref())
                .map_err(|e| StorageError::Other(e.to_string()))?;

            let storage = self.storage.read().await;
            storage
                .put_file(&format!("{}{}", BLOB_PREFIX, blob_name), &enc_data, None, None)
                .await?;
            seed_out = Some(seed);
        } else {
            let storage = self.storage.read().await;
            storage
                .put_file(&format!("{}{}", BLOB_PREFIX, blob_name), data, None, None)
                .await?;
        }

        Ok(BlobUpload {
            storage_key: blob_name,
            seed: seed_out,
            size: data_len,
        })
    }

    fn check_complete_part_list(parts: &[NinjaPart]) -> (bool, u64) {
        let mut sz = 0u64;
        let mut last_part_num: Option<u32> = None;

        for p in parts {
            if p.is_last {
                last_part_num = Some(p.number);
            }
            sz += p.size;
        }

        let is_ok = last_part_num.is_some() && parts.len() as u32 == last_part_num.unwrap();
        (is_ok, sz)
    }

    fn is_complete_spi_part_list(&self, parts: &[StoragePartInfo]) -> (bool, u64) {
        let mut sz = 0u64;
        let mut last_part_num: Option<u32> = None;

        for p in parts {
            if p.is_last == Some(true) {
                last_part_num = p.part_number;
            }
            sz += p.size.unwrap_or(0);
        }

        let is_ok = last_part_num.is_some() && parts.len() as u32 == last_part_num.unwrap();
        (is_ok, sz)
    }

    /// Flush pending zip batch (if any) to a blob and journal it.
    /// Takes `&self`; uses the state mutex briefly.
    async fn flush_zip(&self) -> Result<(), StorageError> {
        // Take the writer out of state so we can do the blob upload without holding the lock.
        let zip_writer = {
            let mut state = self.state.lock().await;
            state.zip_writer.take()
        };
        let Some(zip_writer) = zip_writer else { return Ok(()) };

        tracing::debug!("flushing zip, sz {}", zip_writer.zip_length);

        let zip_data = zip_writer
            .zip_writer
            .finish()
            .map_err(|e| StorageError::Other(e.to_string()))?
            .into_inner();
        let len = zip_data.len();
        // The zip blob itself is not encrypted (each entry inside already is), so no AAD.
        let blob_up = self.put_blob(&zip_writer.zip_blob_name, &zip_data, false, None).await?;
        tracing::debug!(
            "wrote zip batch {} sz {} {}",
            zip_writer.zip_blob_name,
            len,
            blob_up.storage_key
        );

        let zip_op = AddZipOp {
            storage_key: blob_up.storage_key,
            size: len as i64,
            ops: zip_writer.zip_journal,
            special_fields: Default::default(),
        };

        {
            let mut state = self.state.lock().await;
            if let Some(ref mut jw) = state.journal_writer {
                jw.add_zip_op(zip_op);
            }
        }
        self.journal_write(true).await?;

        Ok(())
    }

    /// Add a file to the current zip batch, flushing when limits hit.
    async fn add_to_zip(
        &self,
        object: &str,
        data: &[u8],
        is_symlink: bool,
        attrs: Option<&StorageFileAttrs>,
    ) -> Result<(), StorageError> {
        let needs_flush = {
            let mut state = self.state.lock().await;
            if state.zip_writer.is_none() {
                state.zip_writer = Some(NinjaZipWriter::new());
            }
            let zip_writer = state.zip_writer.as_mut().unwrap();
            zip_writer.add(object, data, is_symlink, attrs)?;

            zip_writer.zip_length >= self.params.zip_batch_size || zip_writer.zip_n_files >= self.params.zip_max_files
        };

        if needs_flush {
            self.flush_zip().await?;
        }

        Ok(())
    }

    /// Read a file from a ZIP batch (immutable version for StorageSource::get)
    /// Fetch (or reuse cached) the zip batch blob, extract `name_in_zip`, decrypt and
    /// window it. Zip entries are random-access so the entry is decrypted whole and
    /// then sliced (TS "zip random-access entries are decrypted whole; window the
    /// result"). Associated so the `get_streamed` producer can call it with cloned
    /// handles.
    async fn get_from_zip_bytes(
        storage: &Arc<RwLock<dyn StorageSourceAndDest>>,
        crypt: &StorageCrypt,
        zip_readers: &ZipBlobCache,
        key: &str,
        blob: &NinjaBlobRef,
        offset: Option<u64>,
        length: Option<u64>,
    ) -> Result<Vec<u8>, StorageError> {
        let name_in_zip = blob
            .name_in_zip
            .as_ref()
            .ok_or_else(|| StorageError::Other("No name_in_zip for zip entry".into()))?;

        tracing::debug!(
            "get_from_zip key={} storage_key={} name_in_zip={}",
            key,
            blob.storage_key,
            name_in_zip
        );

        // Single-flight per batch: grab/insert this key's OnceCell under the map lock
        // (held only long enough to clone the cell, never across the fetch), then
        // init it exactly once — concurrent same-batch readers await that one
        // download; a failed fetch leaves the cell empty so a later read retries.
        let cell = {
            let mut map = zip_readers.lock().await;
            map.entry(blob.storage_key.clone())
                .or_insert_with(|| Arc::new(tokio::sync::OnceCell::new()))
                .clone()
        };
        let zip_data = cell
            .get_or_try_init(|| async {
                let data = storage.read().await.get(&blob.storage_key, None, None).await?;
                Ok::<Arc<Vec<u8>>, StorageError>(Arc::new(data))
            })
            .await?
            .clone();

        tracing::debug!("loaded zip blob {} size {}", blob.storage_key, zip_data.len());

        let cursor = std::io::Cursor::new(zip_data.as_ref().as_slice());
        let mut archive =
            zip::ZipArchive::new(cursor).map_err(|e| StorageError::Other(format!("Failed to open zip: {}", e)))?;

        let mut file = archive
            .by_name(name_in_zip)
            .map_err(|e| StorageError::Other(format!("File not found in zip {}: {}", name_in_zip, e)))?;

        let mut enc_data = Vec::new();
        file.read_to_end(&mut enc_data)
            .map_err(|e| StorageError::Other(format!("Failed to read zip data: {}", e)))?;

        tracing::debug!("zip entry {} enc_data size {}", name_in_zip, enc_data.len());

        Self::decrypt_and_slice_zip_entry(crypt, &enc_data, blob, offset, length)
    }

    fn decrypt_and_slice_zip_entry(
        crypt: &StorageCrypt,
        enc_data: &[u8],
        blob: &NinjaBlobRef,
        offset: Option<u64>,
        length: Option<u64>,
    ) -> Result<Vec<u8>, StorageError> {
        let seed = blob
            .seed
            .as_ref()
            .ok_or_else(|| StorageError::Other("No seed for zip entry".into()))?;

        let (iv, encryption_key) =
            StorageCrypt::derive_seed_keys(seed).map_err(|e| StorageError::Other(e.to_string()))?;

        // Zip entries are chunk-encrypted (64 KiB) with `aadForZip` — TS
        // `decryptDataWorkerGCM` passes chunkSize + AAD. Always chunked; the old
        // overhead-based chunked-vs-single heuristic is gone now that both sides
        // agree on the scheme.
        let name_in_zip = blob
            .name_in_zip
            .as_deref()
            .ok_or_else(|| StorageError::Other("No name_in_zip for zip entry".into()))?;
        let aad = aad_for_zip(name_in_zip, blob.size);
        let data = crypt
            .decrypt_chunked_gcm(enc_data, &iv, &encryption_key, GCM_CHUNK_SIZE, Some(&aad))
            .map_err(|e| StorageError::Other(e.to_string()))?;

        if offset.is_some() || length.is_some() {
            // Clamped to the entry like any read: a start at/after the end
            // is an empty read.
            let start = (offset.unwrap_or(0) as usize).min(data.len());
            let end = length
                .map(|l| start.saturating_add(l as usize))
                .unwrap_or(data.len())
                .min(data.len());
            Ok(data[start..end].to_vec())
        } else {
            Ok(data)
        }
    }

    /// Resolve a logical key to its SOI via the cached meta, building the meta on
    /// demand when it hasn't been read yet. Mirrors TS `getStreamed`'s
    /// `(await this.meta()).getAttrs(key)`.
    async fn resolve_soi(&self, key: &str) -> Result<StorageObjectInfo, StorageError> {
        self.ensure_meta().await?;
        let state = self.state.lock().await;
        state
            .cached_meta
            .as_ref()
            .and_then(|m| m.get_attrs(key))
            .ok_or_else(|| StorageError::NoSuchKey(key.to_string()))
    }

    /// Cache + return the full meta (built from `storage#info` + journals) —
    /// what `get_dir` / `search` stream from. Mirrors TS `NinjaStorageV2.readMeta`
    /// returning the cached `_meta`; `&self` because the cache lives behind the
    /// `state` mutex.
    async fn cached_read_meta(&self) -> Result<StorageMeta, StorageError> {
        self.ensure_meta().await?;
        Ok(self.state.lock().await.cached_meta.clone().unwrap())
    }

    /// Build the meta once per session if nobody has yet. Everything answered
    /// from the replay side-state (`state.parts`, `blob_refs`) must call this
    /// first — the journal replay is what fills it, and that only runs here.
    async fn ensure_meta(&self) -> Result<(), StorageError> {
        let need_read = self.state.lock().await.cached_meta.is_none();
        if need_read {
            self.build_meta().await?;
        }
        Ok(())
    }

    /// The first read: replay the whole journal log into a `FreshReplay` and
    /// install it — meta, side-state, hash chain and journal writer together —
    /// so no reader ever sees a partial meta. A concurrent first read that
    /// installed its own meanwhile wins; this one is dropped.
    async fn build_meta(&self) -> Result<(), StorageError> {
        tracing::debug!("reading metadata");
        // Mirrors TS `new StorageMeta(metaState===Complete, storageInfo.stats)`:
        // seed the stat from `storage#info` (carries metaState + totalSize).
        let meta = {
            let state = self.state.lock().await;
            match state.storage_info {
                Some(ref info) => StorageMeta::with_stat(info.stats.clone()),
                None => StorageMeta::default(),
            }
        };
        let mut fresh = FreshReplay {
            meta,
            parts: HashMap::new(),
            blob_refs: HashMap::new(),
            prev_journal_hash: None,
            last_read_journal_num: -1,
        };

        self.apply_journal_diff(Some(&mut fresh)).await?;

        let mut state = self.state.lock().await;
        if state.cached_meta.is_some() {
            return Ok(());
        }
        // Extend, not replace: the side-state also carries this session's own
        // writes.
        state.parts.extend(fresh.parts);
        state.blob_refs.extend(fresh.blob_refs);
        if let Some(ref info) = state.storage_info {
            state.journal_writer = Some(NinjaJournalWriter::new(
                info.clone(),
                self.params.master_key.clone(),
                fresh.prev_journal_hash.clone(),
            ));
        }
        state.prev_journal_hash = fresh.prev_journal_hash;
        state.last_read_journal_num = fresh.last_read_journal_num;
        state.cached_meta = Some(fresh.meta);
        Ok(())
    }

    /// Re-read `storage#info` and fold any journals written since the last read
    /// into the cached meta, returning `(meta, changes)` — the Rust analog of TS
    /// `refreshMeta`. `changes` is the per-op list applied this round.
    pub async fn refresh_meta(&self) -> Result<(StorageMeta, Vec<StorageChange>), StorageError> {
        tracing::debug!("refreshing metadata");
        self.update_storage_info().await?;

        // No cached meta yet → fall back to a full read (mirrors the first
        // `cached_read_meta()` call), which also installs the journal writer + hash chain.
        // A baseline read has no incremental changes to report.
        let have_cached = self.state.lock().await.cached_meta.is_some();
        if !have_cached {
            return Ok((self.cached_read_meta().await?, Vec::new()));
        }

        // Only the journals written since `last_read_journal_num`, applied in
        // place: the cached meta stays readable throughout.
        let changes = self.apply_journal_diff(None).await?;

        let mut guard = self.state.lock().await;
        let state = &mut *guard;
        let is_complete = state
            .storage_info
            .as_ref()
            .map(|i| i.stats.is_complete())
            .unwrap_or(false);
        let meta = state
            .cached_meta
            .as_mut()
            .ok_or_else(|| StorageError::Other("refresh without a cached meta".into()))?;
        meta.set_is_complete_meta(is_complete);
        Ok((meta.clone(), changes))
    }

    /// Apply journals `last_read+1 ..= last_journal_num` to the live state
    /// (`fresh` = None, a refresh) or to a first read's `fresh` copy, returning
    /// the per-op change list (Add/Modify/Delete) of a refresh; a first read
    /// reports none (no per-entry clones on the bulk read).
    ///
    /// Journals are fetched through a bounded read-ahead window (`PREFETCH`) so the
    /// round-trips overlap, but they are decrypted, hash-chain verified and applied
    /// strictly in order — the hash chain and op application depend on the previous
    /// journal, the fetch+decrypt do not. Mirrors the TS prefetch loop.
    ///
    /// A journal is the unit of atomicity, as the cursor and the hash chain count
    /// journals: each is fetched, decrypted and parsed with nothing touched, then
    /// applied whole by `apply_journal` — live, under one hold of `state`, no
    /// await inside. A failed fetch or a dropped future leaves the state at the
    /// previous journal, and the next refresh resumes exactly there.
    async fn apply_journal_diff(&self, mut fresh: Option<&mut FreshReplay>) -> Result<Vec<StorageChange>, StorageError> {
        let (last_journal_num, is_encrypted, start_journal) = {
            let state = self.state.lock().await;
            let last_journal_num = match &state.storage_info {
                Some(info) => match info.last_journal_num {
                    Some(n) => n,
                    None => {
                        tracing::debug!("don't have journal, skipping");
                        return Ok(Vec::new());
                    }
                },
                None => return Ok(Vec::new()),
            };

            let is_encrypted = state
                .storage_info
                .as_ref()
                .and_then(|i| i.is_encrypted)
                .unwrap_or(false);

            // TS: `for (i = lastReadJournalNum + 1; i <= lastJournalNum; i++)`
            // with `lastReadJournalNum` starting at -1 — i.e. journals are
            // 0-based and the first read starts at journal 0. (Was `max(1, …)`,
            // which skipped journal-000000.)
            let last_read = match fresh {
                Some(ref fresh) => fresh.last_read_journal_num,
                None => state.last_read_journal_num,
            };
            let start_journal = (last_read + 1).max(0) as u32;
            (last_journal_num, is_encrypted, start_journal)
        };

        let master_key = self.params.master_key.clone();
        let mut changes: Vec<StorageChange> = Vec::new();

        if start_journal > last_journal_num {
            return Ok(changes);
        }

        // Bounded read-ahead: up to PREFETCH journal fetches in flight, yielded
        // back in ascending order by `buffered`. The storage read guard is held
        // for the whole apply loop — only shared reads happen under it, and
        // applying a journal takes `state`, never `storage`, so there is no
        // self-deadlock.
        const PREFETCH: usize = 8;
        let storage = self.storage.read().await;
        let mut journals = futures::stream::iter(start_journal..=last_journal_num)
            .map(|i| {
                let storage = &*storage;
                async move {
                    tracing::debug!("reading journal {}", i);
                    let journal_key = format!("meta/journal-{:06}", i);
                    (i, storage.get(&journal_key, None, None).await)
                }
            })
            .buffered(PREFETCH);

        while let Some((i, journal_data)) = journals.next().await {
            let journal_data = journal_data?;
            let decrypted_data = if is_encrypted {
                NinjaJournalWriter::decrypt_journal(master_key.as_deref(), &journal_data)?
            } else {
                journal_data
            };

            let journal = Journal::parse_from_bytes(&decrypted_data)
                .map_err(|e| StorageError::Other(format!("Failed to decode journal: {}", e)))?;

            tracing::debug!("journal version {} with {} ops", journal.version, journal.ops.len());

            let hash = Sha256::digest(&decrypted_data).to_vec();
            match fresh {
                Some(ref mut fresh) => Self::apply_journal(
                    &mut fresh.meta,
                    &mut fresh.parts,
                    &mut fresh.blob_refs,
                    &mut fresh.prev_journal_hash,
                    &mut fresh.last_read_journal_num,
                    i,
                    &journal,
                    hash,
                    None,
                )?,
                None => {
                    let mut guard = self.state.lock().await;
                    let state = &mut *guard;
                    let meta = state
                        .cached_meta
                        .as_mut()
                        .ok_or_else(|| StorageError::Other("refresh without a cached meta".into()))?;
                    Self::apply_journal(
                        meta,
                        &mut state.parts,
                        &mut state.blob_refs,
                        &mut state.prev_journal_hash,
                        &mut state.last_read_journal_num,
                        i,
                        &journal,
                        hash,
                        Some(&mut changes),
                    )?;
                }
            }
        }

        drop(journals);
        drop(storage);

        Ok(changes)
    }

    /// Apply one parsed journal whole — hash chain checked first, then every op,
    /// then the cursor and the chain advanced together. A journal at or below
    /// the cursor was applied already by a concurrent refresh and is skipped.
    /// Sync on purpose: nothing can interleave with or cut short a journal.
    #[allow(clippy::too_many_arguments)] // the five replay fields, from live state or a FreshReplay
    fn apply_journal(
        meta: &mut StorageMeta,
        parts: &mut HashMap<String, NinjaPartsElem>,
        blob_refs: &mut HashMap<String, NinjaBlobRef>,
        prev_journal_hash: &mut Option<Vec<u8>>,
        last_read_journal_num: &mut i32,
        num: u32,
        journal: &Journal,
        hash: Vec<u8>,
        mut changes: Option<&mut Vec<StorageChange>>,
    ) -> Result<(), StorageError> {
        if *last_read_journal_num >= num as i32 {
            return Ok(());
        }
        if let Some(ref prev_sha) = journal.prev_sha256
            && let Some(ref expected_hash) = *prev_journal_hash
            && prev_sha != expected_hash
        {
            return Err(StorageError::Other(format!("journal hash mismatch at journal {}", num)));
        }

        for op in &journal.ops {
            if let Some(ref msg) = op.msg {
                Self::apply_journal_op(meta, parts, blob_refs, msg, changes.as_deref_mut())?;
            }
        }

        *prev_journal_hash = Some(hash);
        *last_read_journal_num = num as i32;
        Ok(())
    }

    ///
    /// `changes`, when `Some`, accumulates the Add/Modify/Delete events for this op
    /// (mirroring the TS `applyJournalDiff` change list). It is `None` on the
    /// initial bulk read so no per-entry clones are paid there.
    fn apply_journal_op(
        result: &mut StorageMeta,
        parts: &mut HashMap<String, NinjaPartsElem>,
        blob_refs: &mut HashMap<String, NinjaBlobRef>,
        msg: &journal_op::Msg,
        mut changes: Option<&mut Vec<StorageChange>>,
    ) -> Result<(), StorageError> {
        match msg {
            journal_op::Msg::AddFile(op) => {
                let mut soi = StorageObjectInfo::new(&op.key);
                soi.size = op.size as u64;
                soi.file_type = StorageFileType::Regular;
                if let Some(m) = op.meta.as_ref() {
                    apply_meta_to_attrs(m, &mut soi.attrs);
                }
                // Read-path payload (re-prefix the bare journal blob name).
                blob_refs.insert(
                    op.key.clone(),
                    NinjaBlobRef {
                        storage_key: op
                            .storage_key
                            .as_ref()
                            .map(|k| format!("{}{}", BLOB_PREFIX, k))
                            .unwrap_or_default(),
                        seed: op.seed.clone(),
                        size: op.size as u64,
                        name_in_zip: None,
                    },
                );
                if let Some(c) = changes.as_deref_mut() {
                    c.push(StorageChange {
                        change_type: ChangeType::Add,
                        item: soi.clone(),
                    });
                }
                result.update(soi);
            }
            journal_op::Msg::AddDir(op) => {
                let mut soi = StorageObjectInfo::new(&op.key);
                soi.file_type = StorageFileType::Dir;
                if let Some(m) = op.meta.as_ref() {
                    apply_meta_to_attrs(m, &mut soi.attrs);
                }
                if let Some(c) = changes.as_deref_mut() {
                    c.push(StorageChange {
                        change_type: ChangeType::Add,
                        item: soi.clone(),
                    });
                }
                result.update(soi);
            }
            journal_op::Msg::AddSymlink(op) => {
                let mut soi = StorageObjectInfo::new(&op.key);
                soi.file_type = StorageFileType::Symlink;
                soi.size = op.size as u64;
                if let Some(m) = op.meta.as_ref() {
                    apply_meta_to_attrs(m, &mut soi.attrs);
                }
                // Read-path payload, mirroring the AddFile arm — the target blob
                // is fetched and decrypted (aadForFile) like a file's.
                blob_refs.insert(
                    op.key.clone(),
                    NinjaBlobRef {
                        storage_key: if op.storage_key.is_empty() {
                            String::new()
                        } else {
                            format!("{}{}", BLOB_PREFIX, op.storage_key)
                        },
                        seed: op.seed.clone(),
                        size: op.size as u64,
                        name_in_zip: None,
                    },
                );
                if let Some(c) = changes.as_deref_mut() {
                    c.push(StorageChange {
                        change_type: ChangeType::Add,
                        item: soi.clone(),
                    });
                }
                result.update(soi);
            }
            journal_op::Msg::AddZip(op) => {
                for zip_op in &op.ops {
                    let mut soi = StorageObjectInfo::new(&zip_op.key);
                    soi.file_type = if zip_op.is_symlink {
                        StorageFileType::Symlink
                    } else {
                        StorageFileType::Regular
                    };
                    soi.size = zip_op.size as u64;
                    if let Some(m) = zip_op.meta.as_ref() {
                        apply_meta_to_attrs(m, &mut soi.attrs);
                    }
                    blob_refs.insert(
                        zip_op.key.clone(),
                        NinjaBlobRef {
                            storage_key: format!("{}{}", BLOB_PREFIX, op.storage_key),
                            seed: zip_op.seed.clone(),
                            size: zip_op.size as u64,
                            name_in_zip: Some(zip_op.name_in_zip.clone()),
                        },
                    );
                    // TS emits Modify for zip entries.
                    if let Some(c) = changes.as_deref_mut() {
                        c.push(StorageChange {
                            change_type: ChangeType::Modify,
                            item: soi.clone(),
                        });
                    }
                    result.update(soi);
                }
            }
            journal_op::Msg::DeleteFile(op) => match result.get_attrs(&op.key) {
                Some(itm) if itm.file_type == StorageFileType::Regular => {
                    result.delete(&op.key);
                    blob_refs.remove(&op.key);
                    if let Some(c) = changes.as_deref_mut() {
                        c.push(StorageChange {
                            change_type: ChangeType::Delete,
                            item: itm,
                        });
                    }
                }
                _ => tracing::debug!("file not found {} for delete", op.key),
            },
            journal_op::Msg::DeleteDir(op) => match result.get_attrs(&op.key) {
                Some(itm) if itm.file_type == StorageFileType::Dir => {
                    result.delete(&op.key);
                    if let Some(c) = changes.as_deref_mut() {
                        c.push(StorageChange {
                            change_type: ChangeType::Delete,
                            item: itm,
                        });
                    }
                }
                _ => tracing::debug!("dir not found {} for delete", op.key),
            },
            journal_op::Msg::AddPart(op) => {
                tracing::debug!("ninjasrc {}#{}", op.key, op.part_number);
                let part = NinjaPart {
                    number: op.part_number as u32,
                    offset: op.offset as u64,
                    size: op.size as u64,
                    is_last: op.is_last,
                    storage_key: format!("{}{}", BLOB_PREFIX, op.storage_key),
                    seed: op.seed.clone(),
                };

                let Some(elem) = parts.get_mut(&op.key) else {
                    // Mirrors TS: a part with no BeginMultipartOp is a corrupt
                    // journal, not something to patch a file entry around.
                    return Err(StorageError::Other(format!(
                        "addPart without beginMultipart {}",
                        op.key
                    )));
                };
                elem.parts.push(part.clone());

                if let Some(mut main_soi) = result.get_attrs(&op.key) {
                    if main_soi.parts.is_none() {
                        main_soi.parts = Some(Vec::new());
                    }
                    main_soi.parts.as_mut().unwrap().push(StoragePartInfo {
                        part_number: Some(op.part_number as u32),
                        offset: Some(op.offset as u64),
                        size: Some(op.size as u64),
                        ..Default::default()
                    });
                    if let Some(c) = changes.as_deref_mut() {
                        c.push(StorageChange {
                            change_type: ChangeType::Modify,
                            item: main_soi.clone(),
                        });
                    }
                    result.update(main_soi);
                }
            }
            journal_op::Msg::CompleteMultipart(op) => {
                let (ok, sz) = {
                    if let Some(elem) = parts.get_mut(&op.key) {
                        elem.parts.sort_by_key(|p| p.number);
                        Self::check_complete_part_list(&elem.parts)
                    } else {
                        (false, 0)
                    }
                };

                if !ok {
                    return Err(StorageError::Other(format!("multipart not complete {}", op.key)));
                }

                if let Some(mut main_soi) = result.get_attrs(&op.key) {
                    if let Some(ref mut parts) = main_soi.parts {
                        parts.sort_by_key(|p| p.part_number.unwrap_or(0));
                    }
                    main_soi.is_complete = true;
                    main_soi.size = sz;
                    if let Some(c) = changes.as_deref_mut() {
                        c.push(StorageChange {
                            change_type: ChangeType::Modify,
                            item: main_soi.clone(),
                        });
                    }
                    result.update(main_soi);
                }

                if let Some(elem) = parts.get_mut(&op.key) {
                    elem.is_complete = true;
                }
            }
            journal_op::Msg::CancelMultipart(op) => {
                tracing::debug!("cancel multipart {}", op.key);
                parts.remove(&op.key);
                if let Some(itm) = result.get_attrs(&op.key) {
                    result.delete(&op.key);
                    if let Some(c) = changes.as_deref_mut() {
                        c.push(StorageChange {
                            change_type: ChangeType::Delete,
                            item: itm,
                        });
                    }
                }
            }
            journal_op::Msg::BeginMultipart(op) => {
                tracing::debug!("begin multipart {}", op.key);
                parts.insert(
                    op.key.clone(),
                    NinjaPartsElem {
                        parts: Vec::new(),
                        is_complete: false,
                    },
                );
                // Mirror TS: create the main entry now (declared size when the
                // op carries one, else 0 with AddPart accumulating) so its
                // attrs are recorded.
                if result.get_attrs(&op.key).is_none() {
                    let mut main_soi = StorageObjectInfo::new(&op.key);
                    main_soi.file_type = StorageFileType::Regular;
                    main_soi.size = op.size.unwrap_or(0) as u64;
                    main_soi.is_complete = false;
                    main_soi.parts = Some(Vec::new());
                    if let Some(m) = op.meta.as_ref() {
                        apply_meta_to_attrs(m, &mut main_soi.attrs);
                    }
                    if let Some(c) = changes {
                        c.push(StorageChange {
                            change_type: ChangeType::Add,
                            item: main_soi.clone(),
                        });
                    }
                    result.update(main_soi);
                }
            }
            _ => {
                tracing::warn!("Unknown journal op encountered, skipping");
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Storage for NinjaStorageV2 {
    fn cleanup(&mut self) {
        self.storage_crypt.cleanup();
        // Cascade to the wrapped S3 storage (drops its cached client). Sync
        // trait method — take the write lock in a spawned task.
        let storage = self.storage.clone();
        tokio::spawn(async move {
            storage.write().await.cleanup();
        });
    }

    async fn connect(&mut self) -> Result<StorageMetaState, StorageError> {
        {
            let mut storage = self.storage.write().await;
            storage.connect().await?;
        }
        self.update_storage_info().await
    }

    async fn cancel(&self) -> Result<(), StorageError> {
        tracing::debug!("Cancel NinjaStorageV2");
        self.is_cancelled.store(true, Ordering::Relaxed);
        let storage = self.storage.read().await;
        storage.cancel().await
    }

    /// Stats straight from `storage#info` — mirrors TS `metaStats`. A trait
    /// override (not inherent) so `dyn Storage` callers get the real metaState,
    /// not the base default.
    async fn meta_stats(&self) -> Result<StorageStatInfo, StorageError> {
        let state = self.state.lock().await;
        let info = state
            .storage_info
            .as_ref()
            .ok_or_else(|| StorageError::Other("bad state".into()))?;
        Ok(StorageStatInfo {
            meta_state: info.stats.meta_state,
            total_size: info.stats.total_size,
        })
    }

    /// Stream the cached meta's entries — TS `NinjaStorageV2.getDir` delegating to
    /// `meta.getDir`.
    async fn get_dir(
        &self,
        path: &str,
        recursive: bool,
        start_after: Option<&str>,
        max_batch_size: Option<usize>,
    ) -> mpsc::Receiver<Result<Vec<StorageObjectInfo>, StorageError>> {
        if start_after.is_some() {
            // A V2 store is never a V3 backend; its journal-built meta has no key order.
            let (tx, rx) = mpsc::channel(1);
            let _ = tx
                .send(Err(StorageError::NotImplemented("V2 getDir start_after".into())))
                .await;
            return rx;
        }
        match self.cached_read_meta().await {
            Ok(meta) => meta.get_dir(path, recursive, max_batch_size),
            Err(e) => {
                let (tx, rx) = mpsc::channel(1);
                let _ = tx.send(Err(e)).await;
                rx
            }
        }
    }

    /// Single-entry lookup against the cached meta — TS `NinjaStorageV2.getAttrs`
    /// delegating to `meta.getAttrs` (throwing "No such file" when absent).
    async fn get_attrs(&self, path: &str) -> Result<StorageObjectInfo, StorageError> {
        self.resolve_soi(path).await
    }

    async fn update_meta(&self) -> Result<Vec<StorageChange>, StorageError> {
        tracing::debug!("updating metadata");
        // Apply any journals written since the last read so newly-added files
        // surface; return the change set so the engine can detect progress.
        let (_meta, changes) = self.refresh_meta().await?;
        Ok(changes)
    }

    /// Mirror TS `NinjaStorageV2.search`: delegate to `meta.search` over the
    /// cached read meta.
    async fn search(&self, query: &StorageSearchQuery, options: &SearchOptions) -> Result<SearchResult, StorageError> {
        self.cached_read_meta().await?.search(query, options)
    }

    /// Delegate byte accounting to the inner storage (the real S3/P2P source doing
    /// the wire transfers) so totals see through the encryption layer, mirroring
    /// the TS delegation. `try_read` keeps these sync methods non-blocking; a
    /// contended sample returns the last-known-good value ([`Self::sample_inner`]).
    fn total_bytes_recv(&self) -> u64 {
        self.sample_inner(&self.last_total_recv, |s| s.total_bytes_recv())
    }

    fn total_bytes_sent(&self) -> u64 {
        self.sample_inner(&self.last_total_sent, |s| s.total_bytes_sent())
    }

    /// The inner wire's progress, ciphertext as it goes out: bytes queued in
    /// a zip batch are not on it until the batch blob is — which is exactly
    /// what the bar should say. Journal/info blobs ride the same counter (a
    /// little over the plaintext total; the engine clamps).
    fn bytes_sent_progress(&self) -> Option<u64> {
        let mut last = self.last_sent_progress.lock().unwrap();
        match self.storage.try_read() {
            Ok(g) => {
                *last = g.bytes_sent_progress();
                *last
            }
            Err(_) => *last,
        }
    }

    fn data_source_id(&self) -> Option<&str> {
        self.data_source_id.as_deref()
    }

    fn set_data_source_id(&mut self, id: String) {
        self.data_source_id = Some(id);
    }
}

#[async_trait]
impl StorageSource for NinjaStorageV2 {
    fn source_name(&self) -> &str {
        "NinjaV2"
    }

    fn source_type(&self) -> StorageSourceType {
        StorageSourceType::NinjaV2
    }

    /// Streaming read — THE source primitive (TS `getStreamed`). Dispatches on the
    /// object kind: a multipart file streams part-by-part (one `stream_low` per
    /// covering part); a zip entry decrypts whole and yields windowed; a direct blob
    /// goes through `stream_low`. Snapshots the parts under the state lock, then runs
    /// a spawned producer with cloned handles so chunks flow as they're decrypted
    /// rather than being materialized first.
    async fn get_streamed(
        &self,
        key: &str,
        offset: Option<u64>,
        length: Option<u64>,
    ) -> mpsc::Receiver<Result<Vec<u8>, StorageError>> {
        let (tx, rx) = mpsc::channel(4);

        // Resolve the logical key via the cached meta (built on demand) — the
        // existence check, mirroring TS `getStreamed`'s `meta().getAttrs(key)`,
        // and the declared length for the multipart branch. The read payload
        // itself comes from the backend-private side-state.
        let soi_size = match self.resolve_soi(key).await {
            Ok(soi) => soi.size,
            Err(e) => {
                let _ = tx.send(Err(e)).await;
                return rx;
            }
        };

        let (multipart_parts, blob) = {
            let state = self.state.lock().await;
            (
                state.parts.get(key).map(|elem| (elem.parts.clone(), elem.is_complete)),
                state.blob_refs.get(key).cloned(),
            )
        };
        let key = key.to_string();
        let storage = self.storage.clone();
        let crypt = self.storage_crypt;
        let master_key = self.params.master_key.clone();
        let zip_readers = self.zip_readers.clone();
        let is_cancelled = self.is_cancelled.clone();

        tokio::spawn(async move {
            let mk = master_key.as_deref();
            let res: Result<(), StorageError> = async {
                if let Some((parts, is_complete)) = multipart_parts {
                    tracing::debug!("partget {} off {:?} len {:?}", key, offset, length);
                    // The object's length is the SOI's size — declared by
                    // `BeginMultipartOp.size`, fixed by CompleteMultipart —
                    // never derived from the parts seen so far: a live
                    // multipart lands its parts out of order, so any
                    // parts-based figure lies about what exists.
                    let full_len = soi_size;
                    let real_off = offset.unwrap_or(0);
                    if real_off >= full_len {
                        if is_complete {
                            // Ranges are clamped to the object, like a file
                            // read or an HTTP Range: readers ask in fixed
                            // aligned blocks (the preview block cache) and
                            // expect the tail block short, not an error. A
                            // start at/after the end is an empty read.
                            return Ok(());
                        }
                        // Still uploading: never an empty read that could
                        // pass for EOF.
                        return Err(StorageError::NoPartError(format!(
                            "No part covers offset {} for {} (still uploading, len {})",
                            real_off, key, full_len
                        )));
                    }
                    let end = match length {
                        Some(l) => real_off.saturating_add(l).min(full_len),
                        None => full_len,
                    };

                    let mut sorted = parts.clone();
                    sorted.sort_by_key(|p| p.offset);
                    // The parts this read crosses, in stream order — the
                    // presign window (PRESIGN_WINDOW) runs over these.
                    let span: Vec<_> = sorted
                        .iter()
                        .filter(|p| p.offset < end && p.offset + p.size > real_off)
                        .collect();
                    let mut hinted = 0usize;

                    let mut pos = real_off;
                    while pos < end {
                        // An error, never an early end: a closed stream reads
                        // as the whole range served.
                        if is_cancelled.load(Ordering::Relaxed) {
                            return Err(StorageError::Cancelled);
                        }
                        let part = sorted
                            .iter()
                            .find(|p| p.offset <= pos && p.offset + p.size > pos)
                            .ok_or_else(|| {
                                StorageError::NoPartError(format!("No part covers offset {} for {}", pos, key))
                            })?;
                        let span_idx = span.iter().position(|p| p.offset == part.offset).unwrap_or(0);
                        if hinted < span_idx + PRESIGN_REFILL {
                            let upto = (span_idx + PRESIGN_WINDOW).min(span.len());
                            if hinted < upto {
                                let keys: Vec<String> =
                                    span[hinted..upto].iter().map(|p| p.storage_key.clone()).collect();
                                let storage = storage.clone();
                                let key = key.clone();
                                tokio::spawn(async move {
                                    if let Err(e) = storage.read().await.presign_urls(&keys).await {
                                        tracing::debug!("presign window failed for {}: {}", key, e);
                                    }
                                });
                            }
                            hinted = span_idx + PRESIGN_WINDOW;
                        }
                        let off_in_part = pos - part.offset;
                        let to_read = (part.size - off_in_part).min(end - pos);
                        let part_blob = NinjaBlobRef {
                            storage_key: part.storage_key.clone(),
                            seed: part.seed.clone(),
                            size: part.size,
                            name_in_zip: None,
                        };
                        Self::stream_low(
                            &storage,
                            &crypt,
                            mk,
                            &part_blob,
                            Some((part.number, part.offset)),
                            Some(off_in_part),
                            Some(to_read),
                            &tx,
                        )
                        .await?;
                        pos += to_read;
                    }
                    Ok(())
                } else if let Some(blob) = blob {
                    if blob.name_in_zip.is_some() {
                        tracing::debug!("zipget {} from {}", key, blob.storage_key);
                        let data =
                            Self::get_from_zip_bytes(&storage, &crypt, &zip_readers, &key, &blob, offset, length)
                                .await?;
                        let _ = tx.send(Ok(data)).await;
                        Ok(())
                    } else {
                        tracing::debug!("directget {}", key);
                        Self::stream_low(&storage, &crypt, mk, &blob, None, offset, length, &tx).await
                    }
                } else {
                    Err(StorageError::Other(format!("no blob data for {}", key)))
                }
            }
            .await;
            if let Err(e) = res {
                let _ = tx.send(Err(e)).await;
            }
        });

        rx
    }

    async fn get_symlink(&self, object: &str) -> Result<String, StorageError> {
        let data = self.get(object, None, None).await?;
        String::from_utf8(data).map_err(|e| StorageError::Other(e.to_string()))
    }
}

#[async_trait]
impl StorageDest for NinjaStorageV2 {
    fn is_sequential(&self) -> bool {
        false
    }

    async fn put_file(
        &self,
        object: &str,
        data: &[u8],
        attrs: Option<&StorageFileAttrs>,
        conditions: Option<&StoragePutConditions>,
    ) -> Result<StoragePutResult, StorageError> {
        // Preconditions are an object-store notion; a V2 store is a filesystem
        // over journaled blobs and has nothing to compare them against.
        reject_conditions(conditions)?;
        if self.params.use_zip_batches && !data.is_empty() && data.len() < self.params.zip_max_file_size {
            tracing::debug!("Adding file to zip {} sz {}", object, data.len());
            self.add_to_zip(object, data, false, attrs).await?;
            let mut soi = StorageObjectInfo::new(object);
            soi.size = data.len() as u64;
            if let Some(a) = attrs {
                soi.attrs = a.clone();
            }
            self.state.lock().await.note_write(soi);
            return Ok(StoragePutResult {
                etag: Some(object.to_string()),
                crc32: None,
                sha256: None,
                storage_key: object.to_string(),
            });
        }

        // Encrypt + upload blob without holding the state lock.
        let data_len = data.len() as u64;
        let aad_fn = move |blob_name: &str| aad_for_file(blob_name, data_len);
        let blob_up = self.put_blob(object, data, !data.is_empty(), Some(&aad_fn)).await?;

        // Now journal the operation under the state lock.
        let op = if !data.is_empty() {
            AddFileOp {
                key: object.to_string(),
                storage_key: Some(blob_up.storage_key.clone()),
                size: blob_up.size as i64,
                seed: blob_up.seed.clone(),
                meta: file_meta_from_attrs(attrs),
                ..Default::default()
            }
        } else {
            AddFileOp {
                key: object.to_string(),
                size: 0,
                meta: file_meta_from_attrs(attrs),
                ..Default::default()
            }
        };

        {
            let mut state = self.state.lock().await;
            if let Some(ref mut jw) = state.journal_writer {
                jw.add_file_op(op);
            }

            // Mirror what apply_journal_op builds for AddFile: generic meta entry
            // plus the read-path payload.
            let mut soi = StorageObjectInfo::new(object);
            soi.size = blob_up.size;
            if let Some(a) = attrs {
                soi.attrs = a.clone();
            }
            state.blob_refs.insert(
                object.to_string(),
                NinjaBlobRef {
                    storage_key: if data.is_empty() {
                        String::new()
                    } else {
                        format!("{}{}", BLOB_PREFIX, blob_up.storage_key)
                    },
                    seed: blob_up.seed.clone(),
                    size: blob_up.size,
                    name_in_zip: None,
                },
            );
            state.note_write(soi);
        }
        self.journal_write(false).await?;

        Ok(StoragePutResult {
            etag: Some(blob_up.storage_key.clone()),
            crc32: None,
            sha256: None,
            storage_key: blob_up.storage_key,
        })
    }

    async fn put_dir(&self, object: &str, attrs: Option<&StorageFileAttrs>) -> Result<StoragePutResult, StorageError> {
        {
            let mut state = self.state.lock().await;
            if let Some(ref mut jw) = state.journal_writer {
                jw.add_dir_op(AddDirOp {
                    key: object.to_string(),
                    meta: file_meta_from_attrs(attrs),
                    ..Default::default()
                });
            }

            let mut soi = StorageObjectInfo::new(object);
            soi.file_type = StorageFileType::Dir;
            if let Some(a) = attrs {
                soi.attrs = a.clone();
            }
            state.note_write(soi);
        }
        self.journal_write(false).await?;

        Ok(StoragePutResult {
            etag: Some(object.to_string()),
            crc32: None,
            sha256: None,
            storage_key: object.to_string(),
        })
    }

    async fn put_symlink(
        &self,
        object: &str,
        target: &str,
        attrs: Option<&StorageFileAttrs>,
    ) -> Result<StoragePutResult, StorageError> {
        if self.params.use_zip_batches {
            tracing::debug!("Adding symlink to zip {} -> {}", object, target);
            self.add_to_zip(object, target.as_bytes(), true, attrs).await?;
            let mut soi = StorageObjectInfo::new(object);
            soi.file_type = StorageFileType::Symlink;
            soi.size = target.len() as u64;
            if let Some(a) = attrs {
                soi.attrs = a.clone();
            }
            self.state.lock().await.note_write(soi);
            return Ok(StoragePutResult {
                etag: Some(object.to_string()),
                crc32: None,
                sha256: None,
                storage_key: object.to_string(),
            });
        }

        let target_bytes = target.as_bytes();
        let data_len = target_bytes.len() as u64;
        let aad_fn = move |blob_name: &str| aad_for_file(blob_name, data_len);
        let blob_up = self.put_blob(object, target_bytes, true, Some(&aad_fn)).await?;

        {
            let mut state = self.state.lock().await;
            if let Some(ref mut jw) = state.journal_writer {
                jw.add_symlink_op(AddSymlinkOp {
                    key: object.to_string(),
                    storage_key: blob_up.storage_key.clone(),
                    size: data_len as i64,
                    seed: blob_up.seed.clone(),
                    meta: file_meta_from_attrs(attrs),
                    ..Default::default()
                });
            }

            // Mirror what apply_journal_op builds for AddSymlink: generic meta entry
            // plus the read-path payload.
            let mut meta_soi = StorageObjectInfo::new(object);
            meta_soi.file_type = StorageFileType::Symlink;
            meta_soi.size = data_len;
            if let Some(a) = attrs {
                meta_soi.attrs = a.clone();
            }
            state.blob_refs.insert(
                object.to_string(),
                NinjaBlobRef {
                    storage_key: format!("{}{}", BLOB_PREFIX, blob_up.storage_key),
                    seed: blob_up.seed.clone(),
                    size: data_len,
                    name_in_zip: None,
                },
            );
            state.note_write(meta_soi);
        }
        self.journal_write(false).await?;

        Ok(StoragePutResult {
            etag: Some(object.to_string()),
            crc32: None,
            sha256: None,
            storage_key: blob_up.storage_key,
        })
    }

    async fn del_file(&self, object: &str) -> Result<(), StorageError> {
        tracing::debug!("Delete file {}", object);

        {
            let mut state = self.state.lock().await;
            if let Some(ref mut jw) = state.journal_writer {
                jw.delete_file_op(DeleteFileOp {
                    key: object.to_string(),
                    special_fields: Default::default(),
                });
            }
            state.blob_refs.remove(object);
            state.note_delete(object);
        }
        self.journal_write(false).await?;

        Ok(())
    }

    async fn del_dir(&self, object: &str) -> Result<(), StorageError> {
        tracing::debug!("Delete dir {}", object);

        {
            let mut state = self.state.lock().await;
            if let Some(ref mut jw) = state.journal_writer {
                jw.delete_dir_op(DeleteDirOp {
                    key: object.to_string(),
                    special_fields: Default::default(),
                });
            }
            state.note_delete(object);
        }
        self.journal_write(false).await?;

        Ok(())
    }

    async fn update_stats(&self, stats: &StorageStatInfo) -> Result<(), StorageError> {
        let mut flush = false;
        {
            let mut state = self.state.lock().await;
            if let Some(ref mut info) = state.storage_info {
                info.stats = stats.clone();
            }

            if let Some(ref mut jw) = state.journal_writer {
                jw.update_stats(stats.clone());
                // Complete is only ever stamped right before the transfer engine's
                // final flush — defer to it so the last journal and the Complete
                // info land in a single write instead of two storage#info PUTs.
                flush = stats.meta_state != StorageMetaState::Complete;
            }
        }
        if flush {
            self.journal_write(true).await?;
        }

        Ok(())
    }

    async fn flush(&self) -> Result<(), StorageError> {
        self.flush_zip().await?;
        self.journal_write(true).await
    }

    async fn close(&self) -> Result<(), StorageError> {
        tracing::info!("Close NinjaStorageV2");
        self.flush().await
    }

    async fn finalize(&self) -> Result<(), StorageError> {
        tracing::debug!("Finalize NinjaStorageV2 - marking upload complete");
        self.flush_zip().await?;

        {
            let mut state = self.state.lock().await;
            if let Some(ref mut info) = state.storage_info {
                info.stats.meta_state = StorageMetaState::Complete;
            }

            let stats_snapshot = state.storage_info.as_ref().map(|i| i.stats.clone());
            if let Some(ref mut jw) = state.journal_writer
                && let Some(stats) = stats_snapshot
            {
                jw.update_stats(stats);
            }
        }
        self.journal_write(true).await?;

        Ok(())
    }
}

#[async_trait]
impl StorageDestMultipart for NinjaStorageV2 {
    async fn list_multiparts_all(&self) -> Result<Vec<StorageMultipartUpload>, StorageError> {
        // `state.parts` is filled by the journal replay — make sure it ran,
        // whatever the caller asked before (resume calls this first).
        self.ensure_meta().await?;
        let state = self.state.lock().await;
        let mut result = Vec::new();
        for (key, value) in &state.parts {
            if !value.is_complete {
                result.push(StorageMultipartUpload {
                    upload_id: Some(key.clone()),
                    plain_key: Some(key.clone()),
                    ..Default::default()
                });
            }
        }
        Ok(result)
    }

    async fn list_parts_all(&self, upload: &StorageMultipartUpload) -> Result<Vec<StoragePartInfo>, StorageError> {
        let key = upload
            .upload_id
            .as_ref()
            .ok_or_else(|| StorageError::Other("No upload ID".into()))?;

        self.ensure_meta().await?;
        let state = self.state.lock().await;
        let elem = state
            .parts
            .get(key)
            .ok_or_else(|| StorageError::Other(format!("No multipart for {}", key)))?;

        let mut result: Vec<StoragePartInfo> = elem
            .parts
            .iter()
            .map(|p| StoragePartInfo {
                etag: Some(key.clone()),
                part_number: Some(p.number),
                is_last: Some(p.is_last),
                size: Some(p.size),
                ..Default::default()
            })
            .collect();

        result.sort_by_key(|p| p.part_number);
        Ok(result)
    }

    async fn begin_multipart(
        &self,
        object: &str,
        size: u64,
        attrs: Option<&StorageFileAttrs>,
    ) -> Result<StorageMultipartUpload, StorageError> {
        {
            let mut state = self.state.lock().await;
            // Duplicate-begin guard for *in-progress* uploads only — a completed
            // elem stays in the map to serve reads and may be begun anew.
            if state.parts.get(object).map(|e| !e.is_complete).unwrap_or(false) {
                return Err(StorageError::Other(format!("Already have multipart for {}", object)));
            }

            // Mirror TS beginMultipart: journal a BeginMultipartOp carrying the file
            // metadata so the main entry (and its attrs) survive a journal replay,
            // and the declared length so readers see the real size from the start.
            if let Some(ref mut jw) = state.journal_writer {
                jw.begin_multipart_op(BeginMultipartOp {
                    key: object.to_string(),
                    meta: file_meta_from_attrs(attrs),
                    size: Some(size as i64),
                    ..Default::default()
                });
            }

            state.parts.insert(
                object.to_string(),
                NinjaPartsElem {
                    parts: Vec::new(),
                    is_complete: false,
                },
            );

            let mut soi = StorageObjectInfo::new(object);
            soi.is_complete = false;
            soi.size = size;
            if let Some(a) = attrs {
                soi.attrs = a.clone();
            }
            state.note_write(soi);
        }
        self.journal_write(false).await?;

        Ok(StorageMultipartUpload {
            plain_key: Some(object.to_string()),
            upload_id: Some(object.to_string()),
            ..Default::default()
        })
    }

    async fn put_part(
        &self,
        part_number: u32,
        upload: &StorageMultipartUpload,
        offset: u64,
        data: &[u8],
        is_last_part: bool,
    ) -> Result<StoragePartInfo, StorageError> {
        let key = upload
            .plain_key
            .as_ref()
            .ok_or_else(|| StorageError::Other("No plain key".into()))?
            .clone();

        // Check that we have the multipart first (brief lock)
        {
            let state = self.state.lock().await;
            if !state.parts.contains_key(&key) {
                return Err(StorageError::Other(format!("No begin multipart for {}", key)));
            }
        }

        let len = data.len() as u64;
        // Blob upload — no state lock held, runs in parallel with other put_parts.
        let aad_fn = move |blob_name: &str| aad_for_part(blob_name, part_number, offset, len);
        let blob_up = self.put_blob(&key, data, true, Some(&aad_fn)).await?;

        // Journal + parts-table update under the state lock; the journal
        // flush itself (if due) uploads outside it via `journal_write`.
        {
            let mut state = self.state.lock().await;
            if let Some(ref mut jw) = state.journal_writer {
                jw.add_part_op(AddPartOp {
                    key: key.clone(),
                    storage_key: blob_up.storage_key.clone(),
                    size: len as i64,
                    part_number: part_number as i32,
                    offset: offset as i64,
                    is_last: is_last_part,
                    seed: blob_up.seed.clone(),
                    ..Default::default()
                });
            }

            // Prefixed like the journal-replay arm, so a read-back of this part in
            // the same session resolves the blob.
            let part = NinjaPart {
                number: part_number,
                offset,
                size: len,
                is_last: is_last_part,
                storage_key: format!("{}{}", BLOB_PREFIX, blob_up.storage_key),
                seed: blob_up.seed,
            };
            if let Some(elem) = state.parts.get_mut(&key) {
                elem.parts.push(part);
            }
        }
        self.journal_write(false).await?;

        Ok(StoragePartInfo {
            etag: Some(key.to_string()),
            part_number: Some(part_number),
            offset: Some(offset),
            size: Some(len),
            is_last: Some(is_last_part),
            ..Default::default()
        })
    }

    async fn cancel_multipart(&self, upload: &StorageMultipartUpload) -> Result<(), StorageError> {
        let key = upload
            .plain_key
            .as_ref()
            .ok_or_else(|| StorageError::Other("No plain key".into()))?;

        tracing::debug!("Cancel multipart {}", key);
        {
            let mut state = self.state.lock().await;
            state.parts.remove(key);

            if let Some(ref mut jw) = state.journal_writer {
                jw.cancel_multipart_op(CancelMultipartOp {
                    key: key.clone(),
                    special_fields: Default::default(),
                });
            }
        }
        self.journal_write(false).await?;

        Ok(())
    }

    async fn complete_multipart(
        &self,
        upload: &StorageMultipartUpload,
        parts: &[StoragePartInfo],
    ) -> Result<String, StorageError> {
        let key = upload
            .plain_key
            .as_ref()
            .ok_or_else(|| StorageError::Other("No plain key".into()))?;

        tracing::debug!("Complete multipart {}", key);

        let (is_ok, sz) = self.is_complete_spi_part_list(parts);
        if !is_ok {
            return Err(StorageError::Other("Not all parts are complete".into()));
        }

        {
            let mut state = self.state.lock().await;
            // Keep the parts (marked complete) — completed ninja files are read
            // part-by-part, and this is what the journal-replay arm does too.
            // list_multiparts_all filters on !is_complete.
            if let Some(elem) = state.parts.get_mut(key) {
                elem.parts.sort_by_key(|p| p.number);
                elem.is_complete = true;
            }

            if let Some(ref mut jw) = state.journal_writer {
                jw.complete_multipart_op(CompleteMultipartOp {
                    key: key.clone(),
                    ..Default::default()
                });
            }

            let mut soi = state
                .cached_meta
                .as_ref()
                .and_then(|m| m.get_attrs(key))
                .unwrap_or_else(|| StorageObjectInfo::new(key.as_str()));
            soi.size = sz;
            soi.is_complete = true;
            state.note_write(soi);
        }
        self.journal_write(false).await?;

        Ok(upload.upload_id.clone().unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transfers::{StorageChange, StorageDest, StorageDestMultipart, StoragePutResult};

    /// In-memory source+dest backing NinjaStorageV2 — the rust analog of the TS
    /// FakeBackend in NinjaStorageV2.test.ts. Round-trips exercise the private
    /// blob_refs/parts read path in both its session-mirror and journal-replay
    /// population modes.
    struct MemStorage {
        files: Mutex<HashMap<String, Vec<u8>>>,
        /// Sleep injected into meta writes (journal / storage#info) — lets the
        /// convoy test hold a journal flush "on the wire" while other writers run.
        meta_put_delay: Option<std::time::Duration>,
        /// Sleep (ms) injected into journal reads, switchable mid-test — lets a
        /// refresh sit on its journal fetch while other reads run.
        journal_get_delay_ms: AtomicU64,
        /// Fail the next journal PUT once.
        fail_next_journal_put: AtomicBool,
    }

    impl MemStorage {
        fn new() -> Self {
            Self {
                files: Mutex::new(HashMap::new()),
                meta_put_delay: None,
                journal_get_delay_ms: AtomicU64::new(0),
                fail_next_journal_put: AtomicBool::new(false),
            }
        }

        fn with_meta_delay(delay: std::time::Duration) -> Self {
            Self {
                files: Mutex::new(HashMap::new()),
                meta_put_delay: Some(delay),
                journal_get_delay_ms: AtomicU64::new(0),
                fail_next_journal_put: AtomicBool::new(false),
            }
        }
    }

    #[async_trait]
    impl Storage for MemStorage {
        async fn connect(&mut self) -> Result<StorageMetaState, StorageError> {
            Ok(StorageMetaState::Unknown)
        }
        async fn cancel(&self) -> Result<(), StorageError> {
            Ok(())
        }
        async fn update_meta(&self) -> Result<Vec<StorageChange>, StorageError> {
            Ok(Vec::new())
        }
        fn data_source_id(&self) -> Option<&str> {
            None
        }
        fn set_data_source_id(&mut self, _id: String) {}
    }

    #[async_trait]
    impl StorageSource for MemStorage {
        fn source_name(&self) -> &str {
            "Mem"
        }
        async fn get_streamed(
            &self,
            object: &str,
            offset: Option<u64>,
            length: Option<u64>,
        ) -> mpsc::Receiver<Result<Vec<u8>, StorageError>> {
            let (tx, rx) = mpsc::channel(1);
            let delay_ms = self.journal_get_delay_ms.load(Ordering::Relaxed);
            if delay_ms > 0 && object.contains("meta/journal") {
                tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
            }
            let item = {
                let files = self.files.lock().await;
                match files.get(object) {
                    Some(data) => {
                        let start = (offset.unwrap_or(0) as usize).min(data.len());
                        let end = length.map(|l| start + l as usize).unwrap_or(data.len()).min(data.len());
                        Ok(data[start..end].to_vec())
                    }
                    None => Err(StorageError::NoSuchKey(object.to_string())),
                }
            };
            let _ = tx.send(item).await;
            rx
        }
        async fn get_symlink(&self, _object: &str) -> Result<String, StorageError> {
            Err(StorageError::NotImplemented("symlink".into()))
        }
    }

    #[async_trait]
    impl StorageDest for MemStorage {
        fn is_sequential(&self) -> bool {
            false
        }
        fn is_sequential_multipart(&self) -> bool {
            false
        }
        async fn put_file(
            &self,
            object: &str,
            data: &[u8],
            _attrs: Option<&StorageFileAttrs>,
            _: Option<&StoragePutConditions>,
        ) -> Result<StoragePutResult, StorageError> {
            if let Some(delay) = self.meta_put_delay
                && (object.contains("meta/journal") || object.contains("storage#info"))
            {
                tokio::time::sleep(delay).await;
            }
            if object.contains("meta/journal") && self.fail_next_journal_put.swap(false, Ordering::Relaxed) {
                return Err(StorageError::S3(format!("injected PUT failure {}", object)));
            }
            self.files.lock().await.insert(object.to_string(), data.to_vec());
            Ok(StoragePutResult {
                etag: None,
                crc32: None,
                sha256: None,
                storage_key: object.to_string(),
            })
        }
        async fn put_dir(
            &self,
            object: &str,
            _attrs: Option<&StorageFileAttrs>,
        ) -> Result<StoragePutResult, StorageError> {
            Ok(StoragePutResult {
                etag: None,
                crc32: None,
                sha256: None,
                storage_key: object.to_string(),
            })
        }
        async fn put_symlink(
            &self,
            object: &str,
            _target: &str,
            _attrs: Option<&StorageFileAttrs>,
        ) -> Result<StoragePutResult, StorageError> {
            Ok(StoragePutResult {
                etag: None,
                crc32: None,
                sha256: None,
                storage_key: object.to_string(),
            })
        }
        async fn del_file(&self, object: &str) -> Result<(), StorageError> {
            self.files.lock().await.remove(object);
            Ok(())
        }
        async fn del_dir(&self, _object: &str) -> Result<(), StorageError> {
            Ok(())
        }
        async fn update_stats(&self, _stats: &StorageStatInfo) -> Result<(), StorageError> {
            Ok(())
        }
        async fn close(&self) -> Result<(), StorageError> {
            Ok(())
        }
    }

    #[async_trait]
    impl StorageDestMultipart for MemStorage {
        async fn list_multiparts_all(&self) -> Result<Vec<StorageMultipartUpload>, StorageError> {
            Ok(Vec::new())
        }
        async fn list_parts_all(&self, _upload: &StorageMultipartUpload) -> Result<Vec<StoragePartInfo>, StorageError> {
            Ok(Vec::new())
        }
        async fn begin_multipart(
            &self,
            _object: &str,
            _size: u64,
            _attrs: Option<&StorageFileAttrs>,
        ) -> Result<StorageMultipartUpload, StorageError> {
            Err(StorageError::NotImplemented("multipart".into()))
        }
        async fn put_part(
            &self,
            _part_number: u32,
            _upload: &StorageMultipartUpload,
            _offset: u64,
            _data: &[u8],
            _is_last_part: bool,
        ) -> Result<StoragePartInfo, StorageError> {
            Err(StorageError::NotImplemented("multipart".into()))
        }
        async fn cancel_multipart(&self, _upload: &StorageMultipartUpload) -> Result<(), StorageError> {
            Err(StorageError::NotImplemented("multipart".into()))
        }
        async fn complete_multipart(
            &self,
            _upload: &StorageMultipartUpload,
            _parts: &[StoragePartInfo],
        ) -> Result<String, StorageError> {
            Err(StorageError::NotImplemented("multipart".into()))
        }
    }

    fn params(zip: bool) -> NinjaStorageV2Params {
        NinjaStorageV2Params {
            master_key: Some(vec![7u8; 32]),
            use_zip_batches: zip,
            ..Default::default()
        }
    }

    async fn new_ninja(mem: Arc<RwLock<dyn StorageSourceAndDest>>, zip: bool) -> NinjaStorageV2 {
        let mut ninja = NinjaStorageV2::new(params(zip), mem);
        ninja.connect().await.unwrap();
        ninja.cached_read_meta().await.unwrap();
        ninja
    }

    fn spi(number: u32, offset: u64, size: u64, is_last: bool) -> StoragePartInfo {
        StoragePartInfo {
            part_number: Some(number),
            offset: Some(offset),
            size: Some(size),
            is_last: Some(is_last),
            ..Default::default()
        }
    }

    /// Encrypted blob write → same-session read (session mirror) and
    /// fresh-instance read (journal replay), full + ranged.
    #[tokio::test]
    async fn blob_round_trip_session_and_replay() {
        let mem: Arc<RwLock<dyn StorageSourceAndDest>> = Arc::new(RwLock::new(MemStorage::new()));
        let data: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();

        {
            let ninja = new_ninja(mem.clone(), false).await;
            ninja.put_file("dir/a.bin", &data, None, None).await.unwrap();

            let got = ninja.get("dir/a.bin", None, None).await.unwrap();
            assert_eq!(got, data, "same-session full read");
            let got = ninja.get("dir/a.bin", Some(100_000), Some(1000)).await.unwrap();
            assert_eq!(got, &data[100_000..101_000], "same-session ranged read");

            ninja.flush().await.unwrap();
        }

        let ninja2 = new_ninja(mem.clone(), false).await;
        let soi = ninja2.get_attrs("dir/a.bin").await.unwrap();
        assert!(soi.is_complete);
        assert_eq!(soi.size, data.len() as u64);
        let got = ninja2.get("dir/a.bin", None, None).await.unwrap();
        assert_eq!(got, data, "replayed full read");
    }

    /// A refresh that fails part-way (a journal not readable yet) keeps the
    /// cached meta and the journals it did apply; the next refresh resumes at
    /// the missing one — no hash-chain mismatch, no rebuild from a partial
    /// replay.
    #[tokio::test]
    async fn failed_refresh_keeps_meta_and_resumes() {
        let mem_store = Arc::new(RwLock::new(MemStorage::new()));
        let mem: Arc<RwLock<dyn StorageSourceAndDest>> = mem_store.clone();
        let writer = new_ninja(mem.clone(), false).await;
        writer.put_file("a.txt", b"a", None, None).await.unwrap();
        writer.flush().await.unwrap();

        let reader = new_ninja(mem.clone(), false).await;
        assert!(reader.get_attrs("a.txt").await.is_ok());

        writer.put_file("b.txt", b"b", None, None).await.unwrap();
        writer.flush().await.unwrap();
        writer.put_file("c.txt", b"c", None, None).await.unwrap();
        writer.flush().await.unwrap();

        // c's journal is not there yet; b's is.
        let store = mem_store.read().await;
        let mut journals: Vec<String> = store
            .files
            .lock()
            .await
            .keys()
            .filter(|k| k.contains("meta/journal-"))
            .cloned()
            .collect();
        journals.sort();
        assert_eq!(journals.len(), 3);
        let hidden_key = journals[2].clone();
        let hidden = store.files.lock().await.remove(&hidden_key).unwrap();
        drop(store);

        assert!(reader.refresh_meta().await.is_err());
        assert!(reader.get_attrs("a.txt").await.is_ok(), "cached meta survives a failed refresh");
        assert!(reader.get_attrs("b.txt").await.is_ok(), "journals applied before the failure stay");

        mem_store.read().await.files.lock().await.insert(hidden_key, hidden);
        let (_meta, changes) = reader.refresh_meta().await.unwrap();
        assert!(changes.iter().any(|c| c.item.key == "c.txt"));
        for key in ["a.txt", "b.txt", "c.txt"] {
            assert!(reader.get_attrs(key).await.is_ok(), "{key} after the resumed refresh");
        }
    }

    /// A read while a refresh sits on its journal fetch answers from the cached
    /// meta — the refresh never takes it out, so known files never go missing.
    #[tokio::test]
    async fn read_during_refresh_sees_cached_meta() {
        let mem_store = Arc::new(RwLock::new(MemStorage::new()));
        let mem: Arc<RwLock<dyn StorageSourceAndDest>> = mem_store.clone();
        let writer = new_ninja(mem.clone(), false).await;
        writer.put_file("a.txt", b"a", None, None).await.unwrap();
        writer.flush().await.unwrap();
        let reader = new_ninja(mem.clone(), false).await;

        writer.put_file("b.txt", b"b", None, None).await.unwrap();
        writer.flush().await.unwrap();

        mem_store.read().await.journal_get_delay_ms.store(300, Ordering::Relaxed);
        let (refreshed, read) = tokio::join!(reader.refresh_meta(), async {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            reader.get_attrs("a.txt").await
        });
        refreshed.unwrap();
        assert!(read.is_ok(), "a.txt missing while the refresh ran: {:?}", read.err());
        assert!(reader.get_attrs("b.txt").await.is_ok());
    }

    /// A journal PUT that fails loses nothing: the next flush writes the same
    /// ops again under the same number, and a fresh reader sees every file.
    #[tokio::test]
    async fn failed_journal_put_is_written_by_the_next_flush() {
        let mem_store = Arc::new(RwLock::new(MemStorage::new()));
        let mem: Arc<RwLock<dyn StorageSourceAndDest>> = mem_store.clone();
        let writer = new_ninja(mem.clone(), false).await;
        writer.put_file("a.txt", b"a", None, None).await.unwrap();
        mem_store.read().await.fail_next_journal_put.store(true, Ordering::Relaxed);
        assert!(writer.flush().await.is_err());

        writer.put_file("b.txt", b"b", None, None).await.unwrap();
        writer.flush().await.unwrap();

        let reader = new_ninja(mem.clone(), false).await;
        for key in ["a.txt", "b.txt"] {
            assert!(reader.get_attrs(key).await.is_ok(), "{key} after the retried flush");
        }
    }

    /// A read cut by `cancel` ends in an error, not in an early clean end that
    /// would pass for the whole range.
    #[tokio::test]
    async fn cancelled_multipart_read_is_an_error() {
        let mem: Arc<RwLock<dyn StorageSourceAndDest>> = Arc::new(RwLock::new(MemStorage::new()));
        let ninja = new_ninja(mem.clone(), false).await;
        let p1 = vec![1u8; 70_000];
        let p2 = vec![2u8; 30_000];
        let total = (p1.len() + p2.len()) as u64;
        let up = ninja.begin_multipart("big.bin", total, None).await.unwrap();
        ninja.put_part(1, &up, 0, &p1, false).await.unwrap();
        ninja.put_part(2, &up, p1.len() as u64, &p2, true).await.unwrap();
        ninja
            .complete_multipart(
                &up,
                &[
                    spi(1, 0, p1.len() as u64, false),
                    spi(2, p1.len() as u64, p2.len() as u64, true),
                ],
            )
            .await
            .unwrap();

        ninja.cancel().await.unwrap();
        assert!(ninja.get("big.bin", None, None).await.is_err());
    }

    /// A zip entry read clamps to the entry: a start past the end is an empty
    /// read, a length past the end stops at it — no slice panic.
    #[test]
    fn zip_entry_slice_clamps_to_the_entry() {
        let crypt = StorageCrypt::new();
        let data = b"zipped-entry-data";
        let seed = [3u8; 32];
        let (iv, key) = StorageCrypt::derive_seed_keys(&seed).unwrap();
        let aad = aad_for_zip("000000", data.len() as u64);
        let enc = crypt
            .encrypt_chunked_gcm(data, &iv, &key, GCM_CHUNK_SIZE, Some(&aad))
            .unwrap();
        let blob = NinjaBlobRef {
            storage_key: String::new(),
            seed: Some(seed.to_vec()),
            size: data.len() as u64,
            name_in_zip: Some("000000".into()),
        };

        let past = NinjaStorageV2::decrypt_and_slice_zip_entry(&crypt, &enc, &blob, Some(100), Some(10)).unwrap();
        assert!(past.is_empty());
        let tail = NinjaStorageV2::decrypt_and_slice_zip_entry(&crypt, &enc, &blob, Some(12), Some(100)).unwrap();
        assert_eq!(tail, b"-data");
    }

    /// Zip-batched write → flush → fresh-instance read (zip entries resolve via
    /// journal-replayed blob refs).
    #[tokio::test]
    async fn zip_batch_round_trip_via_replay() {
        let mem: Arc<RwLock<dyn StorageSourceAndDest>> = Arc::new(RwLock::new(MemStorage::new()));
        let data = b"zipped-entry-data".to_vec();

        {
            let ninja = new_ninja(mem.clone(), true).await;
            ninja.put_file("z/x.txt", &data, None, None).await.unwrap();
            ninja.flush().await.unwrap();
        }

        let ninja2 = new_ninja(mem.clone(), true).await;
        let soi = ninja2.get_attrs("z/x.txt").await.unwrap();
        assert_eq!(soi.size, data.len() as u64);
        let got = ninja2.get("z/x.txt", None, None).await.unwrap();
        assert_eq!(got, data);
    }

    /// A zip batch grows linearly with its entries: one `ZipWriter` per batch,
    /// the central directory written once at flush. (Re-opening the archive per
    /// entry left every previous central directory behind as garbage — quadratic
    /// in the number of files.) Every entry reads back from a fresh instance.
    #[tokio::test]
    async fn zip_batch_blob_size_is_linear_in_entries() {
        let mem_store = Arc::new(RwLock::new(MemStorage::new()));
        let mem: Arc<RwLock<dyn StorageSourceAndDest>> = mem_store.clone();
        const N: usize = 200;
        const ENTRY: usize = 1000;
        // Per entry: local header + central directory entry (+ zip64 extras) for a
        // 6-char name, plus the 16-byte GCM tag — well under 256 bytes.
        const PER_ENTRY_OVERHEAD: usize = 256;

        {
            let ninja = new_ninja(mem.clone(), true).await;
            for i in 0..N {
                let data = vec![(i % 251) as u8; ENTRY];
                ninja.put_file(&format!("z/{i}.bin"), &data, None, None).await.unwrap();
            }
            ninja.flush().await.unwrap();
        }

        let blobs: Vec<usize> = mem_store
            .read()
            .await
            .files
            .lock()
            .await
            .iter()
            .filter(|(k, _)| k.starts_with(BLOB_PREFIX))
            .map(|(_, v)| v.len())
            .collect();
        assert_eq!(blobs.len(), 1, "all {N} entries land in one batch");
        assert!(
            blobs[0] <= N * (ENTRY + PER_ENTRY_OVERHEAD) + 1024,
            "zip batch blob is {} bytes for {} entries of {} bytes",
            blobs[0],
            N,
            ENTRY
        );

        let ninja2 = new_ninja(mem.clone(), true).await;
        for i in 0..N {
            let got = ninja2.get(&format!("z/{i}.bin"), None, None).await.unwrap();
            assert_eq!(got, vec![(i % 251) as u8; ENTRY], "entry {i}");
        }
    }

    /// Multipart write → same-session read (covers the prefixed part
    /// storage_key) and fresh-instance replay read across a part boundary.
    #[tokio::test]
    async fn multipart_round_trip_session_and_replay() {
        let mem: Arc<RwLock<dyn StorageSourceAndDest>> = Arc::new(RwLock::new(MemStorage::new()));
        let p1 = vec![1u8; 70_000];
        let p2 = vec![2u8; 30_000];
        let mut full = p1.clone();
        full.extend_from_slice(&p2);

        {
            let ninja = new_ninja(mem.clone(), false).await;
            let up = ninja.begin_multipart("big.bin", full.len() as u64, None).await.unwrap();
            ninja.put_part(1, &up, 0, &p1, false).await.unwrap();
            ninja.put_part(2, &up, p1.len() as u64, &p2, true).await.unwrap();
            ninja
                .complete_multipart(
                    &up,
                    &[
                        spi(1, 0, p1.len() as u64, false),
                        spi(2, p1.len() as u64, p2.len() as u64, true),
                    ],
                )
                .await
                .unwrap();

            let got = ninja.get("big.bin", None, None).await.unwrap();
            assert_eq!(got, full, "same-session multipart read");

            ninja.flush().await.unwrap();
        }

        let ninja2 = new_ninja(mem.clone(), false).await;
        let soi = ninja2.get_attrs("big.bin").await.unwrap();
        assert!(soi.is_complete);
        assert_eq!(soi.size, full.len() as u64);
        // read across the part boundary
        let got = ninja2.get("big.bin", Some(60_000), Some(20_000)).await.unwrap();
        assert_eq!(got, &full[60_000..80_000], "replayed cross-part ranged read");
        // A fixed-size block straddling the end is served short (the preview
        // block cache reads aligned 512 KiB blocks); a start at the end is empty.
        let got = ninja2.get("big.bin", Some(90_000), Some(524_288)).await.unwrap();
        assert_eq!(got, &full[90_000..], "tail block clamped to the object end");
        let got = ninja2.get("big.bin", Some(100_000), Some(10)).await.unwrap();
        assert!(got.is_empty(), "read at EOF is empty");
    }

    /// A live multipart lands its parts out of order. Reading an existing
    /// part past a gap must yield its bytes; reading inside the gap, or past
    /// the declared end while incomplete, is `NoPartError` — never an empty
    /// stream (which the engine would record as a 0-byte part, a permanent
    /// hole).
    #[tokio::test]
    async fn incomplete_multipart_reads_out_of_order_parts_and_refuses_gaps() {
        let mem: Arc<RwLock<dyn StorageSourceAndDest>> = Arc::new(RwLock::new(MemStorage::new()));
        let ninja = new_ninja(mem.clone(), false).await;
        let p1 = vec![1u8; 1000];
        let p3 = vec![3u8; 1000];
        let up = ninja.begin_multipart("big.bin", 3000, None).await.unwrap();
        ninja.put_part(1, &up, 0, &p1, false).await.unwrap();
        ninja.put_part(3, &up, 2000, &p3, false).await.unwrap(); // part 2 not there yet

        let got = ninja.get("big.bin", Some(2000), Some(1000)).await.unwrap();
        assert_eq!(got, p3, "an existing part past a gap reads back whole");
        let got = ninja.get("big.bin", Some(0), Some(1000)).await.unwrap();
        assert_eq!(got, p1);

        let err = ninja.get("big.bin", Some(1000), Some(1000)).await.unwrap_err();
        assert!(matches!(err, StorageError::NoPartError(_)), "gap: {err:?}");
        let err = ninja.get("big.bin", Some(3000), Some(1000)).await.unwrap_err();
        assert!(
            matches!(err, StorageError::NoPartError(_)),
            "past the declared end while incomplete: {err:?}"
        );
    }

    /// Resume asks a fresh session for its open multiparts before anything
    /// else: the answer must come from the journal, not from whether some
    /// earlier call happened to build the meta.
    #[tokio::test]
    async fn open_multiparts_listed_before_any_other_call() {
        let mem: Arc<RwLock<dyn StorageSourceAndDest>> = Arc::new(RwLock::new(MemStorage::new()));
        {
            let ninja = new_ninja(mem.clone(), false).await;
            let up = ninja.begin_multipart("big.bin", 3000, None).await.unwrap();
            ninja.put_part(1, &up, 0, &[1u8; 1000], false).await.unwrap();
            ninja.flush().await.unwrap();
        }

        // Not `new_ninja`: that helper reads the meta eagerly, which is the
        // very thing this test must not rely on.
        let mut ninja2 = NinjaStorageV2::new(params(false), mem.clone());
        ninja2.connect().await.unwrap();
        let open = ninja2.list_multiparts_all().await.unwrap();
        assert_eq!(open.len(), 1, "the unfinished multipart is listed first thing");
        assert_eq!(open[0].plain_key.as_deref(), Some("big.bin"));
        let parts = ninja2.list_parts_all(&open[0]).await.unwrap();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].part_number, Some(1));
    }

    /// `BeginMultipartOp.size`: the SOI carries the declared full length from
    /// the start — never a sum/extent of the parts seen so far — both in the
    /// writing session and after a journal replay, and reads refuse the
    /// not-yet-landed range up to it.
    #[tokio::test]
    async fn declared_multipart_size_from_begin_and_across_replay() {
        let mem: Arc<RwLock<dyn StorageSourceAndDest>> = Arc::new(RwLock::new(MemStorage::new()));
        let ninja = new_ninja(mem.clone(), false).await;
        let p1 = vec![1u8; 1000];
        let p3 = vec![3u8; 1000];
        let up = ninja.begin_multipart("big.bin", 4000, None).await.unwrap();
        ninja.put_part(1, &up, 0, &p1, false).await.unwrap();
        ninja.put_part(3, &up, 2000, &p3, false).await.unwrap();

        let soi = ninja.get_attrs("big.bin").await.unwrap();
        assert!(!soi.is_complete);
        assert_eq!(soi.size, 4000, "declared length from begin, not a part sum");

        // Landed bytes read back; everything not landed — the gap, the range
        // past the extent, the range past the declared end — is NoPartError.
        let got = ninja.get("big.bin", Some(2000), Some(1000)).await.unwrap();
        assert_eq!(got, p3);
        for off in [1000u64, 3000, 4000] {
            let err = ninja.get("big.bin", Some(off), Some(1000)).await.unwrap_err();
            assert!(matches!(err, StorageError::NoPartError(_)), "off {off}: {err:?}");
        }

        ninja.flush().await.unwrap();
        let ninja2 = new_ninja(mem.clone(), false).await;
        let soi = ninja2.get_attrs("big.bin").await.unwrap();
        assert!(!soi.is_complete);
        assert_eq!(soi.size, 4000, "declared length survives replay");
        let got = ninja2.get("big.bin", Some(2000), Some(1000)).await.unwrap();
        assert_eq!(got, p3);
    }

    /// A due journal flush must not convoy concurrent writers: the journal +
    /// storage#info PUTs run outside the state lock, so a put_part finishing
    /// its blob while the flush is on the wire commits immediately instead of
    /// queueing behind two network writes (the progress-bar stall/snap-back
    /// seen in the field every ~5 s of a big upload).
    #[tokio::test]
    async fn journal_flush_does_not_block_concurrent_put_part() {
        use std::time::{Duration, Instant};

        let meta_delay = Duration::from_millis(400);
        let mem: Arc<RwLock<dyn StorageSourceAndDest>> = Arc::new(RwLock::new(MemStorage::with_meta_delay(meta_delay)));
        let ninja = Arc::new(new_ninja(mem.clone(), false).await);

        let up = ninja.begin_multipart("big.bin", 3000, None).await.unwrap();
        ninja.put_part(1, &up, 0, &[1u8; 1000], false).await.unwrap();

        // Backdate the batching timer so the next non-forced write flushes.
        {
            let mut state = ninja.state.lock().await;
            state.journal_writer.as_mut().unwrap().make_due();
        }

        // Part 2 triggers the flush and pays its two slow meta uploads.
        let flusher = {
            let ninja = ninja.clone();
            let up = up.clone();
            tokio::spawn(async move {
                ninja.put_part(2, &up, 1000, &[2u8; 1000], false).await.unwrap();
            })
        };

        // Let part 2 reach the upload phase of its flush.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Part 3 must commit while that flush is still on the wire.
        let t0 = Instant::now();
        ninja.put_part(3, &up, 2000, &[3u8; 1000], true).await.unwrap();
        let elapsed = t0.elapsed();

        flusher.await.unwrap();
        assert!(
            elapsed < Duration::from_millis(300),
            "concurrent put_part convoyed behind the journal flush: {:?}",
            elapsed
        );

        // The flush itself really happened.
        let journal = {
            let storage = mem.read().await;
            storage.get("meta/journal-000000", None, None).await
        };
        assert!(journal.is_ok(), "journal blob was uploaded");
    }
}
