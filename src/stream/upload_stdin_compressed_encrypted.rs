use crate::{
    cli::globals::GlobalArgs,
    s3::S3,
    stream::{
        STDIN_BUFFER_SIZE, Stream, complete_multipart_upload, compress_chunk,
        create_initial_stream, create_nonce_header, encrypt_chunk, get_key, init_encryption,
        initiate_multipart_upload, maybe_upload_part, setup_progress, upload_final_part,
        write_to_stream,
    },
};
use anyhow::{Result, anyhow};
use chacha20poly1305::aead::stream::EncryptorBE32;
use futures::stream::TryStreamExt;
use std::{collections::BTreeMap, path::PathBuf};
use tokio::io::stdin;
use tokio_util::codec::{BytesCodec, FramedRead};

/// Read from STDIN, compress the data and encrypt it
///
/// # Errors
/// Will return an error if the upload fails
pub async fn stream_stdin_compressed_encrypted(
    s3: &S3,
    object_key: &str,
    acl: Option<String>,
    meta: Option<BTreeMap<String, String>>,
    quiet: bool,
    tmp_dir: PathBuf,
    globals: GlobalArgs,
) -> Result<String> {
    // Validate encryption key early
    let encryption_key = globals
        .enc_key
        .as_ref()
        .ok_or_else(|| anyhow!("Encryption key is required"))?;

    // use .zst extension if compress option is set
    let key = get_key(object_key, globals.compress, globals.encrypt);

    let mut meta = meta.unwrap_or_default();
    meta.insert(
        "Content-Type".to_string(),
        "application/vnd.s3m.encrypted".to_string(),
    );

    // S3 setup
    let upload_id = initiate_multipart_upload(s3, &key, acl, meta).await?;

    let progress_sender = setup_progress(quiet, None).await;

    // Initialize encryption
    let (cipher, nonce_bytes) = init_encryption(encryption_key)?;
    let encryptor = EncryptorBE32::from_aead(cipher, (&nonce_bytes).into());

    let nonce_header = create_nonce_header(&nonce_bytes);

    // Create initial stream
    let first_stream: Stream = create_initial_stream(
        &upload_id,
        &tmp_dir,
        &key,
        s3,
        progress_sender,
        &globals,
        Some(&nonce_header),
    )?;

    let mut stream = FramedRead::new(stdin(), BytesCodec::new())
        .map_err(|e| anyhow!("Error reading STDIN chunk: {}", e))
        .try_fold(
            (first_stream, encryptor), // Initial accumulator tuple (encryptor is moved here)
            |(mut current_upload_state_acc, mut current_encryptor_acc), chunk| async move {
                // Compress the current chunk
                let compress_data = compress_chunk(chunk).await?;

                // Encrypt the current chunk
                let encrypted_data = encrypt_chunk(&mut current_encryptor_acc, &compress_data)
                    .map_err(|e| anyhow!("Failed to encrypt chunk: {}", e))?;

                // Write the encrypted chunk to our internal buffer/temp file
                write_to_stream(&mut current_upload_state_acc, &encrypted_data).map_err(|e| {
                    anyhow!("Failed to write encrypted chunk to upload stream: {}", e)
                })?;

                // Check if a part needs to be uploaded to S3
                maybe_upload_part(&mut current_upload_state_acc, STDIN_BUFFER_SIZE).await?;

                Ok((current_upload_state_acc, current_encryptor_acc)) // Return updated accumulator
            },
        )
        .await // This results in Result<(UploadStream, EncryptorBE32), Error>
        .map(|(final_stream_state, _)| final_stream_state)?; // Extract only UploadStream from Ok variant

    // Upload final part and complete multipart upload
    let final_etag = upload_final_part(&mut stream, &key, &upload_id, s3, &globals).await?;

    stream.etags.push(final_etag);

    // Close channel if it exists
    if let Some(sender) = stream.channel.take() {
        drop(sender);
    }

    complete_multipart_upload(s3, &key, &upload_id, stream.etags).await
}
