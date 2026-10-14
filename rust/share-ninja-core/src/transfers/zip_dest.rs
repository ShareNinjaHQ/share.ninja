//! ZipDest - writes transfer output to a ZIP archive

use async_trait::async_trait;
use std::io::{Cursor, Write};
use tokio::sync::Mutex;
use zip::ZipWriter;
use zip::write::SimpleFileOptions;

use super::{
    Storage, StorageChange, StorageDest, StorageDestMultipart, StorageError, StorageFileAttrs, StorageFileType,
    StorageMetaState, StorageMultipartUpload, StorageObjectInfo, StoragePartInfo, StoragePutConditions,
    StoragePutResult, StorageStatInfo, reject_conditions,
};

/// ZIP destination for writing transfer output as a ZIP archive
pub struct ZipDest {
    data_source_id: Option<String>,
    state: Mutex<ZipState>,
}

struct ZipState {
    zip_writer: Option<ZipWriter<Cursor<Vec<u8>>>>,
    /// Session-write mirror: a fresh zip has no pre-existing content, but the
    /// engine's multi-pass loop (live incomplete sources) re-checks every key —
    /// entries already added must answer `get_attrs` or they'd be added twice.
    written: std::collections::HashMap<String, StorageObjectInfo>,
}

impl ZipDest {
    pub fn new() -> Self {
        let buffer = Cursor::new(Vec::new());
        let zip_writer = ZipWriter::new(buffer);

        Self {
            data_source_id: None,
            state: Mutex::new(ZipState {
                zip_writer: Some(zip_writer),
                written: std::collections::HashMap::new(),
            }),
        }
    }

    pub async fn finish(&self) -> Result<Vec<u8>, StorageError> {
        let mut s = self.state.lock().await;
        let writer = s
            .zip_writer
            .take()
            .ok_or_else(|| StorageError::Other("ZIP writer already finished".into()))?;
        let cursor = writer.finish().map_err(|e| StorageError::Other(e.to_string()))?;
        Ok(cursor.into_inner())
    }
}

impl Default for ZipDest {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Storage for ZipDest {
    async fn connect(&mut self) -> Result<StorageMetaState, StorageError> {
        Ok(StorageMetaState::Incomplete)
    }

    async fn cancel(&self) -> Result<(), StorageError> {
        tracing::debug!("Cancel ZipDest");
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
impl StorageDest for ZipDest {
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
        tracing::debug!("ZIP put file {} sz {}", object, data.len());
        let mut s = self.state.lock().await;
        let writer = s
            .zip_writer
            .as_mut()
            .ok_or_else(|| StorageError::Other("ZIP writer not available".into()))?;
        let options = SimpleFileOptions::default().compression_method(zip::CompressionMethod::Deflated);

        writer
            .start_file(object, options)
            .map_err(|e| StorageError::Other(e.to_string()))?;

        writer.write_all(data).map_err(StorageError::Io)?;

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

    async fn put_dir(&self, object: &str, _attrs: Option<&StorageFileAttrs>) -> Result<StoragePutResult, StorageError> {
        use std::borrow::Cow;
        tracing::debug!("ZIP put dir {}", object);

        let mut s = self.state.lock().await;
        let writer = s
            .zip_writer
            .as_mut()
            .ok_or_else(|| StorageError::Other("ZIP writer not available".into()))?;
        let dir_name: Cow<str> = if object.ends_with('/') {
            Cow::Borrowed(object)
        } else {
            Cow::Owned(format!("{}/", object))
        };

        let options = SimpleFileOptions::default();
        writer
            .add_directory(dir_name.as_ref(), options)
            .map_err(|e| StorageError::Other(e.to_string()))?;

        let mut soi = StorageObjectInfo::new(object);
        soi.file_type = StorageFileType::Dir;
        s.written.insert(object.to_string(), soi);

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
        target: &str,
        attrs: Option<&StorageFileAttrs>,
    ) -> Result<StoragePutResult, StorageError> {
        tracing::debug!("ZIP put symlink {} -> {}", object, target);
        let result = self.put_file(object, target.as_bytes(), attrs, None).await?;
        // put_file recorded it as Regular; a symlink source item must match on
        // the multi-pass type check.
        let mut soi = StorageObjectInfo::new(object);
        soi.file_type = StorageFileType::Symlink;
        soi.size = target.len() as u64;
        self.state.lock().await.written.insert(object.to_string(), soi);
        Ok(result)
    }

    async fn del_file(&self, _object: &str) -> Result<(), StorageError> {
        Err(StorageError::NotImplemented(
            "del_file not supported for ZipDest".into(),
        ))
    }

    async fn del_dir(&self, _object: &str) -> Result<(), StorageError> {
        Err(StorageError::NotImplemented("del_dir not supported for ZipDest".into()))
    }

    async fn update_stats(&self, _info: &StorageStatInfo) -> Result<(), StorageError> {
        Ok(())
    }

    async fn close(&self) -> Result<(), StorageError> {
        tracing::debug!("ZipDest closed");
        Ok(())
    }
}

#[async_trait]
impl StorageDestMultipart for ZipDest {
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
        Err(StorageError::NotImplemented(
            "Multipart not supported for ZipDest".into(),
        ))
    }

    async fn put_part(
        &self,
        _part_number: u32,
        _upload: &StorageMultipartUpload,
        _offset: u64,
        _data: &[u8],
        _is_last_part: bool,
    ) -> Result<StoragePartInfo, StorageError> {
        Err(StorageError::NotImplemented(
            "Multipart not supported for ZipDest".into(),
        ))
    }

    async fn cancel_multipart(&self, _upload: &StorageMultipartUpload) -> Result<(), StorageError> {
        Ok(())
    }

    async fn complete_multipart(
        &self,
        _upload: &StorageMultipartUpload,
        _parts: &[StoragePartInfo],
    ) -> Result<String, StorageError> {
        Err(StorageError::NotImplemented(
            "Multipart not supported for ZipDest".into(),
        ))
    }
}
