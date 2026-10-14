//! Chunked file streaming shared by every local source (`FsSource`, `LocalFs`,
//! a landed `FsDest`): open once, seek to `offset`, push fixed-size chunks
//! down a bounded channel instead of buffering the range in RAM. Dropping the
//! receiver cancels the producer.

use std::io::SeekFrom;
use std::path::PathBuf;

use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::mpsc;

use crate::StorageError;

const CHUNK: usize = 5 * 1024 * 1024;

/// Stream `[offset, offset+length)` (`None` length ⇒ to EOF) of `path`.
pub(crate) fn stream_file(
    path: PathBuf,
    offset: Option<u64>,
    length: Option<u64>,
) -> mpsc::Receiver<Result<Vec<u8>, StorageError>> {
    let (tx, rx) = mpsc::channel(4);
    tokio::spawn(async move {
        match fs::File::open(&path).await {
            Ok(file) => pump_file(file, offset, length, tx).await,
            Err(e) => {
                let _ = tx.send(Err(e.into())).await;
            }
        }
    });
    rx
}

/// The producer body over an already-open file (a source that opens through
/// something other than a path — an Android `content://` fd — lands here).
pub(crate) async fn pump_file(
    mut file: fs::File,
    offset: Option<u64>,
    length: Option<u64>,
    tx: mpsc::Sender<Result<Vec<u8>, StorageError>>,
) {
    if let Some(off) = offset
        && let Err(e) = file.seek(SeekFrom::Start(off)).await
    {
        let _ = tx.send(Err(e.into())).await;
        return;
    }
    let mut remaining = length;
    loop {
        let want = match remaining {
            Some(0) => break,
            Some(r) => (r as usize).min(CHUNK),
            None => CHUNK,
        };
        let mut buf = vec![0u8; want];
        let n = match file.read(&mut buf).await {
            Ok(0) => break, // EOF
            Ok(n) => n,
            Err(e) => {
                let _ = tx.send(Err(e.into())).await;
                return;
            }
        };
        buf.truncate(n);
        if let Some(r) = remaining.as_mut() {
            *r -= n as u64;
        }
        if tx.send(Ok(buf)).await.is_err() {
            break; // receiver dropped → cancel
        }
    }
}
