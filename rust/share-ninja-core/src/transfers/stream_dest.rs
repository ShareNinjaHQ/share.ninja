//! StreamDest - writes transfer output to a stream

use async_trait::async_trait;
use std::collections::HashMap;
use tokio::sync::Mutex;

use super::{
    Storage, StorageChange, StorageDest, StorageDestMultipart, StorageError, StorageFileAttrs, StorageMetaState,
    StorageMultipartUpload, StorageObjectInfo, StoragePartInfo, StoragePutConditions, StoragePutResult,
    StorageStatInfo, reject_conditions,
};

struct StreamState {
    current_part: u32,
    parts: Vec<StoragePartInfo>,
    part_data: HashMap<u32, Vec<u8>>,
    output_buffer: Vec<u8>,
    is_closed: bool,
    /// Session-write mirror: a fresh stream has no pre-existing content, but the
    /// engine's multi-pass loop (live incomplete sources) re-checks every key —
    /// the object already streamed must answer `get_attrs` or it'd be sent twice.
    written: HashMap<String, StorageObjectInfo>,
}

/// Stream destination for writing transfer output
pub struct StreamDest {
    #[allow(dead_code)]
    window_size: u64,
    data_source_id: Option<String>,
    state: Mutex<StreamState>,
}

impl StreamDest {
    pub fn new(window_size: Option<u64>) -> Self {
        Self {
            window_size: window_size.unwrap_or(64 * 1024 * 1024),
            data_source_id: None,
            state: Mutex::new(StreamState {
                current_part: 1,
                parts: Vec::new(),
                part_data: HashMap::new(),
                output_buffer: Vec::new(),
                is_closed: false,
                written: HashMap::new(),
            }),
        }
    }

    pub async fn take_output(&self) -> Vec<u8> {
        let mut s = self.state.lock().await;
        std::mem::take(&mut s.output_buffer)
    }

    pub async fn output(&self) -> Vec<u8> {
        self.state.lock().await.output_buffer.clone()
    }
}

#[async_trait]
impl Storage for StreamDest {
    async fn connect(&mut self) -> Result<StorageMetaState, StorageError> {
        Ok(StorageMetaState::Incomplete)
    }

    async fn cancel(&self) -> Result<(), StorageError> {
        tracing::debug!("Cancel StreamDest");
        Ok(())
    }

    async fn update_meta(&self) -> Result<Vec<StorageChange>, StorageError> {
        Ok(Vec::new())
    }

    async fn get_attrs(&self, path: &str) -> Result<StorageObjectInfo, StorageError> {
        self.state
            .lock()
            .await
            .written
            .get(path)
            .cloned()
            .ok_or_else(|| StorageError::NoSuchKey(path.to_string()))
    }

    fn data_source_id(&self) -> Option<&str> {
        self.data_source_id.as_deref()
    }

    fn set_data_source_id(&mut self, id: String) {
        self.data_source_id = Some(id);
    }
}

#[async_trait]
impl StorageDest for StreamDest {
    fn is_sequential(&self) -> bool {
        true
    }

    async fn put_file(
        &self,
        object: &str,
        data: &[u8],
        _attrs: Option<&StorageFileAttrs>,
        conditions: Option<&StoragePutConditions>,
    ) -> Result<StoragePutResult, StorageError> {
        reject_conditions(conditions)?;
        tracing::debug!("put file {} sz {}", object, data.len());
        let mut s = self.state.lock().await;
        s.output_buffer.extend_from_slice(data);
        s.is_closed = true;
        let mut soi = StorageObjectInfo::new(object);
        soi.size = data.len() as u64;
        s.written.insert(object.to_string(), soi);
        Ok(StoragePutResult {
            etag: None,
            crc32: None,
            sha256: None,
            storage_key: object.to_string(),
        })
    }

    async fn put_dir(
        &self,
        _object: &str,
        _attrs: Option<&StorageFileAttrs>,
    ) -> Result<StoragePutResult, StorageError> {
        Err(StorageError::NotImplemented(
            "put_dir not supported for StreamDest".into(),
        ))
    }

    async fn put_symlink(
        &self,
        _object: &str,
        _target: &str,
        _attrs: Option<&StorageFileAttrs>,
    ) -> Result<StoragePutResult, StorageError> {
        Err(StorageError::NotImplemented(
            "put_symlink not supported for StreamDest".into(),
        ))
    }

    async fn del_file(&self, _object: &str) -> Result<(), StorageError> {
        Err(StorageError::NotImplemented(
            "del_file not supported for StreamDest".into(),
        ))
    }

    async fn del_dir(&self, _object: &str) -> Result<(), StorageError> {
        Err(StorageError::NotImplemented(
            "del_dir not supported for StreamDest".into(),
        ))
    }

    async fn update_stats(&self, _info: &StorageStatInfo) -> Result<(), StorageError> {
        Ok(())
    }

    async fn close(&self) -> Result<(), StorageError> {
        self.state.lock().await.is_closed = true;
        Ok(())
    }
}

#[async_trait]
impl StorageDestMultipart for StreamDest {
    async fn list_multiparts_all(&self) -> Result<Vec<StorageMultipartUpload>, StorageError> {
        Ok(Vec::new())
    }

    async fn list_parts_all(&self, _upload: &StorageMultipartUpload) -> Result<Vec<StoragePartInfo>, StorageError> {
        Ok(self.state.lock().await.parts.clone())
    }

    async fn begin_multipart(
        &self,
        object: &str,
        _size: u64,
        _attrs: Option<&StorageFileAttrs>,
    ) -> Result<StorageMultipartUpload, StorageError> {
        let mut soi = StorageObjectInfo::new(object);
        soi.is_complete = false;
        self.state.lock().await.written.insert(object.to_string(), soi);
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
        _is_last_part: bool,
    ) -> Result<StoragePartInfo, StorageError> {
        let soi = StoragePartInfo {
            part_number: Some(part_number),
            offset: Some(offset),
            size: Some(data.len() as u64),
            ..Default::default()
        };

        let key = upload.plain_key.as_deref().unwrap_or("unknown");

        let mut s = self.state.lock().await;
        s.parts.push(soi.clone());

        if s.current_part == part_number {
            tracing::debug!("Writing directly {}#{}", key, part_number);
            s.output_buffer.extend_from_slice(data);

            let mut next_part = part_number + 1;
            while let Some(queued_data) = s.part_data.remove(&next_part) {
                tracing::debug!("Continue writing directly {}#{}", key, next_part);
                s.output_buffer.extend_from_slice(&queued_data);
                next_part += 1;
            }
            s.current_part = next_part;
        } else {
            tracing::debug!("Can't write now, putting {}#{} to queue", key, part_number);
            s.part_data.insert(part_number, data.to_vec());
        }

        Ok(soi)
    }

    async fn cancel_multipart(&self, upload: &StorageMultipartUpload) -> Result<(), StorageError> {
        tracing::debug!("Cancel multipart {}", upload.plain_key.as_deref().unwrap_or("unknown"));
        let mut s = self.state.lock().await;
        s.parts.clear();
        s.part_data.clear();
        Ok(())
    }

    async fn complete_multipart(
        &self,
        upload: &StorageMultipartUpload,
        _parts: &[StoragePartInfo],
    ) -> Result<String, StorageError> {
        tracing::debug!(
            "Complete multipart {}",
            upload.plain_key.as_deref().unwrap_or("unknown")
        );
        let key = upload.plain_key.clone().unwrap_or_default();
        let mut s = self.state.lock().await;
        s.is_closed = true;
        let mut soi = StorageObjectInfo::new(&key);
        soi.size = s.parts.iter().filter_map(|p| p.size).sum();
        s.written.insert(key, soi);
        Ok(upload.upload_id.clone().unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The stream's own ordering: an early out-of-order part queues until its
    /// predecessor lands, then the queue drains in one go.
    #[tokio::test]
    async fn parts_write_in_order_regardless_of_arrival() {
        let dest = StreamDest::new(None);
        let upload = dest.begin_multipart("file.bin", 0, None).await.unwrap();

        dest.put_part(2, &upload, 4, b"5678", false).await.unwrap();
        assert!(dest.output().await.is_empty(), "part 2 must wait for part 1");
        dest.put_part(1, &upload, 0, b"1234", false).await.unwrap();
        assert_eq!(dest.output().await, b"12345678", "queued part flushed in order");
        dest.put_part(3, &upload, 8, b"9AB", true).await.unwrap();
        assert_eq!(dest.output().await, b"123456789AB");
        assert_eq!(dest.list_parts_all(&upload).await.unwrap().len(), 3);

        dest.complete_multipart(&upload, &[]).await.unwrap();
        let soi = dest.get_attrs("file.bin").await.unwrap();
        assert_eq!(soi.size, 11);
        assert!(soi.is_complete, "completed multipart answers as a complete entry");

        assert_eq!(dest.take_output().await, b"123456789AB");
        assert!(dest.output().await.is_empty(), "take_output drains the buffer");
    }
}
