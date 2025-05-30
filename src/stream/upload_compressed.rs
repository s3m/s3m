use crate::{
    cli::globals::GlobalArgs,
    s3::S3,
    stream::{
        complete_multipart_upload, compress_chunk, create_initial_stream, get_key,
        initiate_multipart_upload, maybe_upload_part, setup_progress, upload_final_part,
        write_to_stream, Stream, STDIN_BUFFER_SIZE,
    },
};
use anyhow::{anyhow, Result};
use futures::stream::TryStreamExt;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};
use tokio::fs::File;
use tokio_util::codec::{BytesCodec, FramedRead};

/// Read file in chunks of 512MB
/// # Errors
/// Will return an error if the upload fails
#[allow(clippy::too_many_arguments)]
pub async fn stream_compressed(
    s3: &S3,
    object_key: &str,
    acl: Option<String>,
    meta: Option<BTreeMap<String, String>>,
    quiet: bool,
    tmp_dir: PathBuf,
    globals: GlobalArgs,
    file_path: &Path,
) -> Result<String> {
    // use .zst extension if compress option is set
    let key = get_key(object_key, true, globals.encrypt);

    // Add Content-Type application/zstd
    let mut meta = meta.unwrap_or_default();
    meta.insert("Content-Type".to_string(), "application/zstd".to_string());

    // S3 setup
    let upload_id = initiate_multipart_upload(s3, &key, acl, meta).await?;

    let progress_sender = setup_progress(quiet, None).await;

    // Create initial stream
    let first_stream: Stream = create_initial_stream(
        &upload_id,
        &tmp_dir,
        &key,
        s3,
        progress_sender,
        &globals,
        None,
    )?;

    let file = File::open(file_path).await?;

    // The accumulator for try_fold is a tuple: (UploadStream, EncryptorBE32).
    // After the fold, we map the Result to extract only the UploadStream state.
    let mut stream = FramedRead::new(file, BytesCodec::new())
        .map_err(|e| anyhow!("Error reading file chunk: {}", e))
        .try_fold(
            first_stream,
            |mut current_upload_state_acc, chunk| async move {
                // Compress the current chunk
                let compress_data = compress_chunk(chunk).await?;

                // Write the encrypted chunk to our internal buffer/temp file
                write_to_stream(&mut current_upload_state_acc, &compress_data)
                    .map_err(|e| anyhow!("Error writing chunk to stream: {}", e))?;

                // Check if a part needs to be uploaded to S3
                maybe_upload_part(&mut current_upload_state_acc, STDIN_BUFFER_SIZE).await?;

                Ok(current_upload_state_acc) // Return updated accumulator
            },
        )
        .await?;

    // Upload final part and complete multipart upload
    let final_etag = upload_final_part(&mut stream, &key, &upload_id, s3, &globals).await?;

    stream.etags.push(final_etag);

    // Close channel if it exists
    if let Some(sender) = stream.channel.take() {
        drop(sender);
    }

    complete_multipart_upload(s3, &key, &upload_id, stream.etags).await
}
