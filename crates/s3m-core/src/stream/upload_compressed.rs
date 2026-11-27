use crate::stream::{
    FileStreamUpload, InitialStreamParams, STDIN_BUFFER_SIZE, Stream, compress_chunk,
    create_initial_stream, finish_stream_upload, get_key, maybe_upload_part, setup_stream_progress,
    with_multipart_upload, write_to_stream,
};
use anyhow::{Result, anyhow};
use futures::stream::TryStreamExt;
use tokio::fs::File;
use tokio_util::codec::{BytesCodec, FramedRead};

/// Read file in chunks of 512MB
///
/// Returns the upload's report: `ETag: "..."` and a line for each response header
/// of the completion (`Version ID`, `Server-side encryption`, `Expiration`, ...).
///
/// # Errors
/// Will return an error if the upload fails
pub async fn stream_compressed(request: FileStreamUpload<'_>) -> Result<String> {
    let FileStreamUpload {
        s3,
        object_key,
        acl,
        meta,
        quiet,
        tmp_dir,
        globals,
        file_path,
    } = request;

    // use .zst extension if compress option is set
    let key = get_key(object_key, globals.compress, globals.encrypt);

    // Add Content-Type application/zstd
    let mut meta = meta.unwrap_or_default();
    meta.insert("Content-Type".to_string(), "application/zstd".to_string());

    with_multipart_upload(
        s3,
        &key,
        acl,
        meta,
        globals.object_lock.clone(),
        async |upload_id| {
            let progress_sender = setup_stream_progress(quiet).await;

            // Create initial stream
            let first_stream: Stream = create_initial_stream(InitialStreamParams {
                upload_id,
                tmp_dir: &tmp_dir,
                key: &key,
                s3,
                progress_sender,
                globals: &globals,
                header_data: None,
            })?;

            let file = File::open(file_path).await?;

            // The accumulator for try_fold is a tuple: (UploadStream, EncryptorBE32).
            // After the fold, we map the Result to extract only the UploadStream state.
            let stream = FramedRead::new(file, BytesCodec::new())
                .map_err(|e| anyhow!("Error reading file chunk: {e}"))
                .try_fold(
                    first_stream,
                    |mut current_upload_state_acc, chunk| async move {
                        // Compress the current chunk
                        let compress_data = compress_chunk(chunk).await?;

                        // Write the encrypted chunk to our internal buffer/temp file
                        write_to_stream(&mut current_upload_state_acc, &compress_data)
                            .map_err(|e| anyhow!("Error writing chunk to stream: {e}"))?;

                        // Check if a part needs to be uploaded to S3
                        maybe_upload_part(&mut current_upload_state_acc, STDIN_BUFFER_SIZE).await?;

                        Ok(current_upload_state_acc) // Return updated accumulator
                    },
                )
                .await?;

            finish_stream_upload(stream).await
        },
    )
    .await
}
