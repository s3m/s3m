use super::{Stream, try_stream_part};
use anyhow::{Context, Result};

/// Replay the staged final part with the same budget and progress as full parts.
pub(super) async fn upload_final_part(stream: &Stream<'_>) -> Result<String> {
    try_stream_part(stream)
        .await
        .context("Failed to upload final part")
}
