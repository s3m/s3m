use crate::stream::{
    FileStreamUpload, InitialStreamParams, STDIN_BUFFER_SIZE, Stream, create_initial_stream,
    create_nonce_header, encrypt_chunk, finish_stream_upload, get_key, init_encryption,
    maybe_upload_part, setup_stream_progress, with_multipart_upload, write_to_stream,
};
use aead_stream::EncryptorBE32;
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
pub async fn stream_encrypted(request: FileStreamUpload<'_>) -> Result<String> {
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

    // Validate encryption key early
    let encryption_key = globals
        .enc_key
        .as_ref()
        .ok_or_else(|| anyhow!("Encryption key is required"))?;

    // use .enc extension
    let key = get_key(object_key, globals.compress, globals.encrypt);

    // Add Content-Type application/zstd
    let mut meta = meta.unwrap_or_default();
    meta.insert(
        "Content-Type".to_string(),
        "application/vnd.s3m.encrypted".to_string(),
    );

    with_multipart_upload(
        s3,
        &key,
        acl,
        meta,
        globals.object_lock.clone(),
        async |upload_id| {
            let progress_sender = setup_stream_progress(quiet).await;

            // Initialize encryption
            let (cipher, nonce_bytes) = init_encryption(encryption_key)?;
            let encryptor = EncryptorBE32::from_aead(cipher, (&nonce_bytes).into());

            let nonce_header = create_nonce_header(&nonce_bytes);

            // Create initial stream with nonce header
            let first_stream: Stream = create_initial_stream(InitialStreamParams {
                upload_id,
                tmp_dir: &tmp_dir,
                key: &key,
                s3,
                progress_sender,
                globals: &globals,
                header_data: Some(&nonce_header),
            })?;

            let file = File::open(file_path)
                .await
                .map_err(|e| anyhow!("Failed to open file '{}': {}", file_path.display(), e))?;

            // The accumulator for try_fold is a tuple: (UploadStream, EncryptorBE32).
            // After the fold, we map the Result to extract only the UploadStream state.
            let stream = FramedRead::new(file, BytesCodec::new())
                .map_err(|e| anyhow!("Error reading file chunk: {e}")) // Convert io::Error to anyhow::Error
                .try_fold(
                    (first_stream, encryptor), // Initial accumulator tuple (encryptor is moved here)
                    |(mut current_upload_state_acc, mut current_encryptor_acc), chunk| async move {
                        // Encrypt the current chunk
                        let encrypted_data = encrypt_chunk(&mut current_encryptor_acc, &chunk)
                            .map_err(|e| anyhow!("Failed to encrypt chunk: {e}"))?;

                        // Write the encrypted chunk to our internal buffer/temp file
                        write_to_stream(&mut current_upload_state_acc, &encrypted_data).map_err(
                            |e| anyhow!("Failed to write encrypted chunk to upload stream: {e}"),
                        )?;

                        // Check if a part needs to be uploaded to S3
                        maybe_upload_part(&mut current_upload_state_acc, STDIN_BUFFER_SIZE).await?;

                        Ok((current_upload_state_acc, current_encryptor_acc)) // Return updated accumulator
                    },
                )
                .await // This results in Result<(UploadStream, EncryptorBE32), Error>
                .map(|(final_stream_state, _)| final_stream_state)?; // Extract only UploadStream from Ok variant

            finish_stream_upload(stream).await
        },
    )
    .await
}
