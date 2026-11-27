use crate::{
    s3::RequestOptions,
    s3::S3,
    stream::{
        InitialStreamParams, STDIN_BUFFER_SIZE, Stream, create_initial_stream, create_nonce_header,
        encrypt_chunk, finish_stream_upload, get_key, init_encryption, maybe_upload_part,
        setup_stream_progress, with_multipart_upload, write_to_stream,
    },
};
use aead_stream::EncryptorBE32;
use anyhow::{Result, anyhow};
use futures::stream::TryStreamExt;
use std::{collections::BTreeMap, path::PathBuf};
use tokio::io::stdin;
use tokio_util::codec::{BytesCodec, FramedRead};

/// Read from STDIN and encrypt the data.
///
/// Returns the upload's report: `ETag: "..."` and a line for each response header
/// of the completion (`Version ID`, `Server-side encryption`, `Expiration`, ...).
///
/// # Errors
/// Will return an error if the upload fails.
pub async fn stream_stdin_encrypted(
    s3: &S3,
    object_key: &str,
    acl: Option<String>,
    meta: Option<BTreeMap<String, String>>,
    quiet: bool,
    tmp_dir: PathBuf,
    globals: RequestOptions,
) -> Result<String> {
    let encryption_key = globals
        .enc_key
        .as_ref()
        .ok_or_else(|| anyhow!("Encryption key is required"))?;

    let key = get_key(object_key, globals.compress, globals.encrypt);

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

            let (cipher, nonce_bytes) = init_encryption(encryption_key)?;
            let encryptor = EncryptorBE32::from_aead(cipher, (&nonce_bytes).into());
            let nonce_header = create_nonce_header(&nonce_bytes);

            let first_stream: Stream = create_initial_stream(InitialStreamParams {
                upload_id,
                tmp_dir: &tmp_dir,
                key: &key,
                s3,
                progress_sender,
                globals: &globals,
                header_data: Some(&nonce_header),
            })?;

            let stream = FramedRead::new(stdin(), BytesCodec::new())
                .map_err(|e| anyhow!("Error reading STDIN chunk: {e}"))
                .try_fold(
                    (first_stream, encryptor),
                    |(mut current_upload_state_acc, mut current_encryptor_acc), chunk| async move {
                        let encrypted_data = encrypt_chunk(&mut current_encryptor_acc, &chunk)
                            .map_err(|e| anyhow!("Failed to encrypt chunk: {e}"))?;

                        write_to_stream(&mut current_upload_state_acc, &encrypted_data).map_err(
                            |e| anyhow!("Failed to write encrypted chunk to upload stream: {e}"),
                        )?;

                        maybe_upload_part(&mut current_upload_state_acc, STDIN_BUFFER_SIZE).await?;

                        Ok((current_upload_state_acc, current_encryptor_acc))
                    },
                )
                .await
                .map(|(final_stream_state, _)| final_stream_state)?;

            finish_stream_upload(stream).await
        },
    )
    .await
}
