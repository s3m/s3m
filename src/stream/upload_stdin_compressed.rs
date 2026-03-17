use crate::{
    cli::globals::GlobalArgs,
    s3::S3,
    stream::{
        InitialStreamParams, STDIN_BUFFER_SIZE, Stream, complete_multipart_upload, compress_chunk,
        create_initial_stream, get_key, initiate_multipart_upload, maybe_upload_part,
        setup_stream_progress, upload_final_part, write_to_stream,
    },
};
use anyhow::{Result, anyhow};
use futures::stream::TryStreamExt;
use std::{collections::BTreeMap, path::PathBuf};
use tokio::io::stdin;
use tokio_util::codec::{BytesCodec, FramedRead};

/// Read from STDIN, compress the data using zstd, and upload in chunks
/// # Errors
/// Will return an error if the upload fails
pub async fn stream_stdin_compressed(
    s3: &S3,
    object_key: &str,
    acl: Option<String>,
    meta: Option<BTreeMap<String, String>>,
    quiet: bool,
    tmp_dir: PathBuf,
    globals: GlobalArgs,
) -> Result<String> {
    // use .zst extension
    let key = get_key(object_key, globals.compress, globals.encrypt);

    let mut meta = meta.unwrap_or_default();
    meta.insert("Content-Type".to_string(), "application/zstd".to_string());

    // S3 setup
    let upload_id = initiate_multipart_upload(s3, &key, acl, meta).await?;

    let progress_sender = setup_stream_progress(quiet).await;

    // Create initial stream
    let first_stream: Stream = create_initial_stream(InitialStreamParams {
        upload_id: &upload_id,
        tmp_dir: &tmp_dir,
        key: &key,
        s3,
        progress_sender,
        globals: &globals,
        header_data: None,
    })?;

    let mut stream = FramedRead::new(stdin(), BytesCodec::new())
        .map_err(|e| anyhow!("Error reading STDIN chunk: {e}"))
        .try_fold(
            first_stream,
            |mut current_upload_state_acc, chunk| async move {
                // Compress the current chunk
                let data = compress_chunk(chunk).await?;

                // Write the compressed chunk to our internal buffer/temp file
                write_to_stream(&mut current_upload_state_acc, &data)
                    .map_err(|e| anyhow!("Error writing compressed chunk to stream: {e}"))?;

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
