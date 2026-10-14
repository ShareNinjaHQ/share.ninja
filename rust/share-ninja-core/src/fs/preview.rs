//! The `get_previews` body shared by the local sources (`FsSource`, `FsDest`):
//! they resolve each key to a file, this runs the injected generator over the
//! batch. See docs/core/previews.md.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;

use crate::StorageError;
use crate::transfers::{PreviewGenerator, PreviewImage};

/// Longest one generation may take. The P2P client gives up on a response
/// when no message arrives for 30 s, and that failure closes its channel
/// mid-stream; every item of a batch starts at once, so bounding each item
/// bounds every gap between messages.
const ITEM_TIMEOUT: Duration = Duration::from_secs(20);

/// Generate one preview per target, each answered once as `(index, result)` in
/// completion order. `targets` is index-aligned with the request: the file
/// locator and the etag of the version behind it, or the error the key
/// already resolved to. How many generations run at once is the platform
/// generator's business — nothing is limited here.
pub(crate) fn spawn_previews(
    generator: Arc<dyn PreviewGenerator>,
    targets: Vec<Result<(String, Option<String>), StorageError>>,
    px: u32,
) -> mpsc::Receiver<(usize, Result<PreviewImage, StorageError>)> {
    let (tx, rx) = mpsc::channel(4);
    let px = px.min(generator.max_px());
    for (index, target) in targets.into_iter().enumerate() {
        let tx = tx.clone();
        let generator = generator.clone();
        tokio::spawn(async move {
            let result = match target {
                Ok((file, etag)) => generate(generator.as_ref(), &file, px)
                    .await
                    .map(|image| PreviewImage { etag, ..image }),
                Err(e) => Err(e),
            };
            let _ = tx.send((index, result)).await;
        });
    }
    rx
}

async fn generate(generator: &dyn PreviewGenerator, file: &str, px: u32) -> Result<PreviewImage, StorageError> {
    match tokio::time::timeout(ITEM_TIMEOUT, generator.generate(file, px)).await {
        Ok(Ok(Some(image))) => Ok(image),
        Ok(Ok(None)) => Err(StorageError::NoPreview(file.to_string())),
        Ok(Err(e)) => Err(StorageError::Other(format!("preview of {file}: {e}"))),
        Err(_) => Err(StorageError::Other(format!("preview of {file}: timed out"))),
    }
}
