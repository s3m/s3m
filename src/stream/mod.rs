pub mod db;
pub mod iterator;
pub mod part;
pub mod upload_compressed;
pub mod upload_compressed_encrypted;
pub mod upload_default;
pub mod upload_encrypted;
pub mod upload_multipart;
pub mod upload_stdin;
pub mod upload_stdin_compressed_encrypted;

use crate::{
    cli::{globals::GlobalArgs, progressbar::Bar},
    s3::{actions, S3},
};
use anyhow::{anyhow, Context as _, Result};
use bytes::BytesMut;
use bytesize::ByteSize;
use chacha20poly1305::{
    aead::{stream::EncryptorBE32, KeyInit},
    ChaCha20Poly1305,
};
use crossbeam::channel::{unbounded, Sender};
use rand::{rng, RngCore};
use ring::digest::{Context, SHA256};
use secrecy::ExposeSecret;
use std::{
    cmp::min,
    collections::BTreeMap,
    io::{Cursor, Write},
    path::{Path, PathBuf},
};
use tempfile::{Builder, NamedTempFile};
use tokio::time::{sleep, Duration};
use tokio::{io::Error, task};
use zstd::stream::encode_all;

// 512MB  to upload 5TB (the current max object size)
const STDIN_BUFFER_SIZE: usize = 1_024 * 1_024 * 512;

pub struct Stream<'a> {
    tmp_file: NamedTempFile,
    count: usize,
    etags: Vec<String>,
    key: &'a str,
    part_number: u16,
    s3: &'a S3,
    upload_id: &'a str,
    sha: ring::digest::Context,
    md5: md5::Context,
    channel: Option<Sender<usize>>,
    tmp_dir: PathBuf,
    throttle: Option<usize>,
    retries: u32,
}

// return the key with the .zst, .enc, or .zst.enc extension based on the flags
fn get_key(key: &str, compress: bool, encrypt: bool) -> String {
    let path = Path::new(key);
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();

    match (compress, encrypt) {
        (true, true) => {
            // If extension is already "zst.enc" add ".zst.enc" only if not present
            if ext == "zst.enc" {
                key.to_string()
            } else {
                format!("{key}.zst.enc")
            }
        }

        (true, false) => {
            // Only compress: add ".zst" if not already "zst"
            if ext == "zst" {
                key.to_string()
            } else {
                format!("{key}.zst")
            }
        }

        (false, true) => {
            // Only encrypt: add ".enc" if not already "enc"
            if ext == "enc" {
                key.to_string()
            } else {
                format!("{key}.enc")
            }
        }

        (false, false) => key.to_string(),
    }
}

async fn try_stream_part(part: &Stream<'_>) -> Result<String> {
    let mut etag = String::new();

    let digest_sha = part.sha.clone().finish();
    let digest_md5 = part.md5.clone().compute();

    // Create globals only to pass the throttle
    let globals = GlobalArgs {
        throttle: part.throttle,
        retries: part.retries,
        compress: false,
        encrypt: false,
        enc_key: None,
    };

    for attempt in 1..=part.retries {
        let backoff_time = 2u64.pow(attempt - 1);
        if attempt > 1 {
            log::warn!(
                "Error streaming part number {}, retrying in {} seconds",
                part.part_number,
                backoff_time
            );

            sleep(Duration::from_secs(backoff_time)).await;
        }

        let action = actions::StreamPart::new(
            part.key,
            part.tmp_file.path(),
            part.part_number,
            part.upload_id,
            part.count,
            (digest_sha.as_ref(), digest_md5.as_ref()),
            part.channel.clone(),
        );

        match action.request(part.s3, &globals).await {
            Ok(e) => {
                etag = e;

                log::info!("Uploaded part: {}, etag: {}", part.part_number, etag);

                break;
            }

            Err(e) => {
                log::error!(
                    "Error uploading part number {}, attempt {}/{} failed: {}",
                    part.part_number,
                    attempt,
                    part.retries,
                    e
                );

                if attempt == part.retries {
                    return Err(anyhow::anyhow!(
                        "Error uploading part number {}, {}",
                        part.part_number,
                        e
                    ));
                }

                continue;
            }
        }
    }

    Ok(etag)
}

/// Compresses a chunk of bytes using Zstandard (zstd), offloading the work to a blocking thread.
///
/// # Errors
/// Returns an error if compression fails or the thread panics.
pub async fn compress_chunk(bytes: BytesMut) -> Result<Vec<u8>> {
    let input = bytes.freeze(); // Safe to send across threads

    task::spawn_blocking(move || {
        encode_all(Cursor::new(input), 0).context("failed to compress with zstd")
    })
    .await
    .context("compression task panicked or was cancelled")?
}

pub fn init_encryption(
    encryption_key: &secrecy::SecretString,
) -> Result<(ChaCha20Poly1305, [u8; 7])> {
    let cipher = ChaCha20Poly1305::new(encryption_key.expose_secret().as_bytes().into());

    // Generate a random nonce of 7 bytes
    let mut nonce_bytes = [0u8; 7];
    rng().fill_bytes(&mut nonce_bytes);

    Ok((cipher, nonce_bytes))
}

/// Initialize multipart upload
async fn initiate_multipart_upload(
    s3: &S3,
    key: &str,
    acl: Option<String>,
    meta: BTreeMap<String, String>,
) -> Result<String> {
    let action = actions::CreateMultipartUpload::new(key, acl, Some(meta), None);
    let response = action.request(s3).await?;
    Ok(response.upload_id)
}

async fn setup_progress(
    quiet: bool,
    size: Option<u64>,
) -> Option<crossbeam::channel::Sender<usize>> {
    if quiet {
        return None;
    }

    let (sender, receiver) = unbounded::<usize>();

    if let Some(size) = size {
        if let Some(pb) = Bar::new(size).progress {
            tokio::spawn(async move {
                let mut uploaded = 0;

                while let Ok(bytes_count) = receiver.recv() {
                    let new = min(uploaded + bytes_count as u64, size);
                    uploaded = new;
                    pb.set_position(new);
                }

                log::debug!("Progress channel closed — total uploaded: {}", uploaded);

                pb.finish();
            });
        }
    } else if let Some(pb) = Bar::new_spinner_stream().progress {
        tokio::spawn(async move {
            let mut uploaded = 0;

            while let Ok(bytes_count) = receiver.recv() {
                uploaded += bytes_count;
                pb.set_message(ByteSize(uploaded as u64).to_string());
            }

            log::debug!("Progress channel closed — total uploaded: {}", uploaded);

            pb.finish();
        });
    }

    Some(sender)
}

/// Create the initial stream with nonce header
#[allow(clippy::too_many_arguments)]
fn create_initial_stream<'a>(
    upload_id: &'a str,
    tmp_dir: &Path,
    key: &'a str,
    s3: &'a S3,
    progress_sender: Option<crossbeam::channel::Sender<usize>>,
    globals: &GlobalArgs,
    header_data: Option<&[u8]>,
) -> Result<Stream<'a>> {
    let mut tmp_file = Builder::new()
        .prefix(upload_id)
        .suffix(".s3m")
        .tempfile_in(tmp_dir)?;

    let mut sha_context = Context::new(&SHA256);
    let mut md5_context = md5::Context::new();
    let mut count = 0;

    if let Some(header) = header_data {
        tmp_file.write_all(header)?;
        sha_context.update(header);
        md5_context.consume(header);
        count = header.len();
    }

    Ok(Stream {
        tmp_file,
        count,
        etags: Vec::new(),
        key,
        part_number: 1,
        s3,
        upload_id,
        sha: sha_context,
        md5: md5_context,
        channel: progress_sender.clone(),
        tmp_dir: tmp_dir.to_path_buf(),
        throttle: globals.throttle,
        retries: globals.retries,
    })
}

/// Write data to stream and update counters/hashes
pub fn write_to_stream(stream: &mut Stream<'_>, data: &[u8]) -> Result<()> {
    stream.tmp_file.write_all(data)?;
    stream.sha.update(data);
    stream.md5.consume(data);
    stream.count += data.len();
    Ok(())
}

/// Create encryption nonce header
pub fn create_nonce_header(nonce_bytes: &[u8; 7]) -> Vec<u8> {
    [&[7_u8], nonce_bytes.as_slice()].concat()
}

/// Encrypt a chunk of data
pub fn encrypt_chunk(
    encryptor: &mut EncryptorBE32<ChaCha20Poly1305>,
    chunk: &[u8],
) -> Result<Vec<u8>> {
    let mut encrypted_chunk = chunk.to_vec();
    encryptor
        .encrypt_next_in_place(&[], &mut encrypted_chunk)
        .map_err(|e| anyhow!("Encryption error: {e}"))?;

    // Format: [encrypted_data_length(4 bytes)][encrypted_data]
    let encrypted_len = encrypted_chunk.len() as u32;
    let mut result = Vec::with_capacity(4 + encrypted_chunk.len());
    result.extend_from_slice(&encrypted_len.to_be_bytes());
    result.extend_from_slice(&encrypted_chunk);
    Ok(result)
}

/// Check if we need to upload current part and start a new one
pub async fn maybe_upload_part(stream: &mut Stream<'_>, buffer_size: usize) -> Result<(), Error> {
    if stream.count >= buffer_size {
        let etag = try_stream_part(stream)
            .await
            .map_err(|e| Error::other(format!("Error streaming part: {e}")))?;

        stream.etags.push(etag);

        log::debug!(
            "Part {} uploaded, total bytes: {}, etag: {}",
            stream.part_number,
            stream.count,
            stream.etags.last().unwrap_or(&"".to_string())
        );

        // Reset for next part
        stream.tmp_file = Builder::new()
            .prefix(stream.upload_id)
            .suffix(".s3m")
            .tempfile_in(&stream.tmp_dir)?;

        stream.count = 0;
        stream.part_number += 1;
        stream.sha = Context::new(&SHA256);
        stream.md5 = md5::Context::new();
    }
    Ok(())
}

/// Complete multipart upload
async fn complete_multipart_upload(
    s3: &S3,
    key: &str,
    upload_id: &str,
    etags: Vec<String>,
) -> Result<String> {
    let parts: BTreeMap<u16, actions::Part> = etags
        .into_iter()
        .enumerate()
        .map(|(index, etag)| {
            let part_number = (index + 1) as u16;
            (
                part_number,
                actions::Part {
                    etag,
                    number: part_number,
                    checksum: None,
                },
            )
        })
        .collect();

    let action = actions::CompleteMultipartUpload::new(key, upload_id, parts, None);

    let response = action.request(s3).await?;

    Ok(response.e_tag)
}

/// Upload the final part
async fn upload_final_part(
    stream: &mut Stream<'_>,
    key: &str,
    upload_id: &str,
    s3: &S3,
    globals: &GlobalArgs,
) -> Result<String> {
    let digest_sha = stream.sha.clone().finish();
    let digest_md5 = stream.md5.clone().compute();

    let action = actions::StreamPart::new(
        key,
        stream.tmp_file.path(),
        stream.part_number,
        upload_id,
        stream.count,
        (digest_sha.as_ref(), digest_md5.as_ref()),
        stream.channel.clone(),
    );

    action
        .request(s3, globals)
        .await
        .map_err(|e| anyhow!("Failed to upload final part: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::{Credentials, Region, S3};
    use secrecy::SecretString;

    #[test]
    fn test_get_key() {
        let test_cases = vec![
            ("test", false, "test"),
            ("test", true, "test.zst"),
            ("test.txt", false, "test.txt"),
            ("test.txt", true, "test.txt.zst"),
            ("test.ZST", false, "test.ZST"),
            ("test.ZST", true, "test.ZST"),
            ("testzst", true, "testzst.zst"),
        ];
        for (key, compress, expected) in test_cases {
            assert_eq!(get_key(key, compress, false), expected);
        }
    }

    #[test]
    fn test_try_stream_part() {
        let s3 = S3::new(
            &Credentials::new(
                "AKIAIOSFODNN7EXAMPLE",
                &SecretString::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into()),
            ),
            &"us-west-1".parse::<Region>().unwrap(),
            Some("awsexamplebucket1".to_string()),
            false,
        );

        let part = Stream {
            tmp_file: NamedTempFile::new().unwrap(),
            count: 0,
            etags: Vec::new(),
            key: "test",
            part_number: 1,
            s3: &s3,
            upload_id: "test",
            sha: ring::digest::Context::new(&ring::digest::SHA256),
            md5: md5::Context::new(),
            channel: None,
            tmp_dir: PathBuf::new(),
            throttle: None,
            retries: 1,
        };

        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(try_stream_part(&part));
        assert!(result.is_err());
    }
}
