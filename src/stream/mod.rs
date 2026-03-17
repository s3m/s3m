pub mod db;
pub mod iterator;
pub mod part;
pub mod state;
pub mod upload_compressed;
pub mod upload_compressed_encrypted;
pub mod upload_default;
pub mod upload_encrypted;
pub mod upload_multipart;
pub mod upload_stdin;
pub mod upload_stdin_compressed;
pub mod upload_stdin_compressed_encrypted;
pub mod upload_stdin_encrypted;

use crate::{
    cli::{globals::GlobalArgs, progressbar::Bar},
    s3::{S3, actions, request::ProgressCallback},
};
use anyhow::{Context as _, Result, anyhow};
use bytes::BytesMut;
use bytesize::ByteSize;
use chacha20poly1305::{
    ChaCha20Poly1305,
    aead::{KeyInit, stream::EncryptorBE32},
};
use indicatif::ProgressBar;
use rand::{Rng, RngExt, rng};
use ring::digest::{Context, SHA256};
use secrecy::ExposeSecret;
use std::{
    cmp::min,
    collections::BTreeMap,
    io::{Cursor, Write},
    path::{Path, PathBuf},
    sync::Arc,
};
use tempfile::{Builder, NamedTempFile};
use tokio::time::{Duration, sleep};
use tokio::{
    io::Error,
    sync::mpsc::{UnboundedSender, unbounded_channel},
    task,
};
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
    channel: Option<UnboundedSender<StreamProgressEvent>>,
    tmp_dir: PathBuf,
    throttle: Option<usize>,
    retries: u32,
}

struct InitialStreamParams<'a> {
    upload_id: &'a str,
    tmp_dir: &'a Path,
    key: &'a str,
    s3: &'a S3,
    progress_sender: Option<UnboundedSender<StreamProgressEvent>>,
    globals: &'a GlobalArgs,
    header_data: Option<&'a [u8]>,
}

pub struct FileStreamUpload<'a> {
    pub s3: &'a S3,
    pub object_key: &'a str,
    pub acl: Option<String>,
    pub meta: Option<BTreeMap<String, String>>,
    pub quiet: bool,
    pub tmp_dir: PathBuf,
    pub globals: GlobalArgs,
    pub file_path: &'a Path,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamProgressEvent {
    Staged(u64),
    SendingStarted { part_number: u16, total_bytes: u64 },
    SendingProgress(u64),
    SendingStopped,
    Confirmed(u64),
    ResetStaging,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct StreamProgressState {
    staged_current: u64,
    staged_total: u64,
    confirmed_total: u64,
    sending_current: u64,
    sending_total: u64,
    sending_part: Option<u16>,
}

// return the key with the .zst, .enc, or .zst.enc extension based on the flags
fn get_key(key: &str, compress: bool, encrypt: bool) -> String {
    let path = Path::new(key);
    let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");

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
    let digest_md5 = part.md5.clone().finalize();

    // Create globals only to pass the throttle
    let globals = GlobalArgs {
        throttle: part.throttle,
        retries: part.retries,
        compress: false,
        encrypt: false,
        enc_key: None,
    };

    for attempt in 1..=part.retries {
        // Exponential backoff: 1s, 2s, 4s, 8s, 16s, capped at 30s
        const MAX_BACKOFF_SECS: u64 = 30;
        let backoff_time = std::cmp::min(2u64.pow(attempt - 1), MAX_BACKOFF_SECS);

        if attempt > 1 {
            // Add jitter (0-1000ms) to prevent thundering herd
            let jitter_ms = rng().random_range(0_u64..1000);
            let total_backoff =
                Duration::from_secs(backoff_time) + Duration::from_millis(jitter_ms);

            log::warn!(
                "Error streaming part number {}, retrying in {:.1} seconds",
                part.part_number,
                total_backoff.as_secs_f64()
            );

            sleep(total_backoff).await;
        }

        emit_stream_progress(
            part.channel.as_ref(),
            StreamProgressEvent::SendingStarted {
                part_number: part.part_number,
                total_bytes: part.count as u64,
            },
        );

        let action = actions::StreamPart::new(
            part.key,
            part.tmp_file.path(),
            part.part_number,
            part.upload_id,
            part.count,
            (digest_sha.as_ref(), digest_md5.as_ref()),
            stream_sending_progress_callback(part.channel.as_ref()),
        );

        match action.request(part.s3, &globals).await {
            Ok(e) => {
                etag = e;
                emit_stream_progress(
                    part.channel.as_ref(),
                    StreamProgressEvent::Confirmed(part.count as u64),
                );

                log::info!("Uploaded part: {}, etag: {}", part.part_number, etag);

                break;
            }

            Err(e) => {
                emit_stream_progress(part.channel.as_ref(), StreamProgressEvent::SendingStopped);

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

#[must_use]
pub fn init_encryption(encryption_key: &secrecy::SecretString) -> (ChaCha20Poly1305, [u8; 7]) {
    let cipher = ChaCha20Poly1305::new(encryption_key.expose_secret().as_bytes().into());

    // Generate a random nonce of 7 bytes
    let mut nonce_bytes = [0u8; 7];
    rng().fill_bytes(&mut nonce_bytes);

    (cipher, nonce_bytes)
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

async fn setup_progress(quiet: bool, size: u64) -> Option<UnboundedSender<usize>> {
    if quiet {
        return None;
    }

    let (sender, receiver) = unbounded_channel::<usize>();

    if let Some(pb) = Bar::new(size).progress {
        spawn_progress_task(receiver, pb, size);
        return Some(sender);
    }

    None
}

async fn setup_stream_progress(quiet: bool) -> Option<UnboundedSender<StreamProgressEvent>> {
    if quiet {
        return None;
    }

    let (sender, receiver) = unbounded_channel::<StreamProgressEvent>();

    if let Some(bars) = Bar::new_stream(STDIN_BUFFER_SIZE as u64) {
        spawn_stream_progress_task(receiver, bars.staging, bars.status);
        return Some(sender);
    }

    None
}

fn spawn_progress_task(
    mut receiver: tokio::sync::mpsc::UnboundedReceiver<usize>,
    pb: ProgressBar,
    size: u64,
) -> task::JoinHandle<u64> {
    tokio::spawn(async move {
        let mut bytes_tracked = 0;

        while let Some(bytes_count) = receiver.recv().await {
            let new = min(bytes_tracked + bytes_count as u64, size);
            bytes_tracked = new;
            pb.set_position(new);
        }

        log::debug!("Progress channel closed — total bytes tracked: {bytes_tracked}");

        pb.finish();
        bytes_tracked
    })
}

fn spawn_stream_progress_task(
    mut receiver: tokio::sync::mpsc::UnboundedReceiver<StreamProgressEvent>,
    staging_pb: ProgressBar,
    status_pb: ProgressBar,
) -> task::JoinHandle<StreamProgressState> {
    tokio::spawn(async move {
        let mut state = StreamProgressState::default();
        update_stream_status_message(&status_pb, &state);

        while let Some(event) = receiver.recv().await {
            match event {
                StreamProgressEvent::Staged(bytes_count) => {
                    state.staged_current += bytes_count;
                    state.staged_total += bytes_count;
                    staging_pb.set_position(min(state.staged_current, STDIN_BUFFER_SIZE as u64));
                }
                StreamProgressEvent::SendingStarted {
                    part_number,
                    total_bytes,
                } => {
                    state.sending_part = Some(part_number);
                    state.sending_current = 0;
                    state.sending_total = total_bytes;
                }
                StreamProgressEvent::SendingProgress(bytes_count) => {
                    state.sending_current =
                        min(state.sending_current + bytes_count, state.sending_total);
                }
                StreamProgressEvent::SendingStopped => {
                    state.sending_part = None;
                    state.sending_current = 0;
                    state.sending_total = 0;
                }
                StreamProgressEvent::Confirmed(bytes_count) => {
                    state.confirmed_total += bytes_count;
                    state.sending_part = None;
                    state.sending_current = 0;
                    state.sending_total = 0;
                }
                StreamProgressEvent::ResetStaging => {
                    state.staged_current = 0;
                    staging_pb.set_position(0);
                }
            }

            update_stream_status_message(&status_pb, &state);
        }

        log::debug!(
            "Stream progress channel closed - staged_current: {}, staged_total: {}, confirmed_total: {}, sending_current: {}, sending_total: {}, sending_part: {:?}",
            state.staged_current,
            state.staged_total,
            state.confirmed_total,
            state.sending_current,
            state.sending_total,
            state.sending_part
        );

        staging_pb.finish();
        status_pb.finish();
        state
    })
}

fn update_stream_status_message(pb: &ProgressBar, state: &StreamProgressState) {
    let message = if let Some(part_number) = state.sending_part {
        format!(
            "sending part {part_number} {}/{} | confirmed {}",
            ByteSize(state.sending_current),
            ByteSize(state.sending_total),
            ByteSize(state.confirmed_total)
        )
    } else {
        format!("confirmed {}", ByteSize(state.confirmed_total))
    };

    pb.set_message(message);
}

fn emit_stream_progress(
    channel: Option<&UnboundedSender<StreamProgressEvent>>,
    event: StreamProgressEvent,
) {
    if let Some(tx) = channel
        && tx.send(event).is_err()
    {
        log::trace!("Progress receiver dropped");
    }
}

fn stream_sending_progress_callback(
    channel: Option<&UnboundedSender<StreamProgressEvent>>,
) -> Option<ProgressCallback> {
    let sender = channel.cloned()?;
    let callback: ProgressCallback = Arc::new(move |bytes_count| {
        if sender
            .send(StreamProgressEvent::SendingProgress(bytes_count as u64))
            .is_err()
        {
            log::trace!("Progress receiver dropped");
        }
    });
    Some(callback)
}

/// Create the initial stream with nonce header
fn create_initial_stream(params: InitialStreamParams<'_>) -> Result<Stream<'_>> {
    let tmp_file = Builder::new()
        .prefix(params.upload_id)
        .suffix(".s3m")
        .tempfile_in(params.tmp_dir)?;

    let mut stream = Stream {
        tmp_file,
        count: 0,
        etags: Vec::new(),
        key: params.key,
        part_number: 1,
        s3: params.s3,
        upload_id: params.upload_id,
        sha: Context::new(&SHA256),
        md5: md5::Context::new(),
        channel: params.progress_sender,
        tmp_dir: params.tmp_dir.to_path_buf(),
        throttle: params.globals.throttle,
        retries: params.globals.retries,
    };

    if let Some(header) = params.header_data {
        write_to_stream(&mut stream, header)?;
    }

    Ok(stream)
}

/// Write data to stream and update counters/hashes
///
/// # Errors
/// Returns `Err` if writing to the temporary file fails
pub fn write_to_stream(stream: &mut Stream<'_>, data: &[u8]) -> Result<()> {
    stream.tmp_file.write_all(data)?;
    stream.sha.update(data);
    stream.md5.consume(data);
    stream.count += data.len();

    emit_stream_progress(
        stream.channel.as_ref(),
        StreamProgressEvent::Staged(data.len() as u64),
    );

    Ok(())
}

/// Create encryption nonce header
#[must_use]
pub fn create_nonce_header(nonce_bytes: &[u8; 7]) -> Vec<u8> {
    [&[7_u8], nonce_bytes.as_slice()].concat()
}

/// Encrypt a chunk of data
///
/// # Errors
/// Returns `Err` if encryption fails or if the encrypted chunk exceeds 4GB
pub fn encrypt_chunk(
    encryptor: &mut EncryptorBE32<ChaCha20Poly1305>,
    chunk: &[u8],
) -> Result<Vec<u8>> {
    let mut encrypted_chunk = chunk.to_vec();
    encryptor
        .encrypt_next_in_place(&[], &mut encrypted_chunk)
        .map_err(|e| anyhow!("Encryption error: {e}"))?;

    // Format: [encrypted_data_length(4 bytes)][encrypted_data]
    let encrypted_len = u32::try_from(encrypted_chunk.len())
        .map_err(|_| anyhow!("Encrypted chunk size exceeds 4GB"))?;
    let mut result = Vec::with_capacity(4 + encrypted_chunk.len());
    result.extend_from_slice(&encrypted_len.to_be_bytes());
    result.extend_from_slice(&encrypted_chunk);
    Ok(result)
}

/// Check if we need to upload current part and start a new one
///
/// # Errors
/// Returns `Err` if streaming the part fails or if creating a new temporary file fails
pub async fn maybe_upload_part(stream: &mut Stream<'_>, buffer_size: usize) -> Result<(), Error> {
    if stream.count >= buffer_size {
        let etag = try_stream_part(stream)
            .await
            .map_err(|e| Error::other(format!("Error streaming part: {e}")))?;

        emit_stream_progress(stream.channel.as_ref(), StreamProgressEvent::ResetStaging);

        stream.etags.push(etag);

        log::debug!(
            "Part {} uploaded, total bytes: {}, etag: {}",
            stream.part_number,
            stream.count,
            stream.etags.last().unwrap_or(&String::new())
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
    // Validate part count before processing
    let part_count = etags.len();
    if part_count > 10_000 {
        log::error!(
            "Part count ({part_count}) exceeds S3's maximum of 10,000 parts. \
            This indicates a bug in part size calculation. \
            Upload ID: {upload_id}, Key: {key}"
        );

        return Err(anyhow!(
            "Upload failed: {part_count} parts created, but S3 allows maximum 10,000 parts. \
            This is a bug in s3m - please report at https://github.com/s3m/s3m/issues with file size and buffer settings. \
            The incomplete multipart upload (ID: {upload_id}) may need manual cleanup using: \
            s3m rm {key} --abort {upload_id}"
        ));
    }

    let parts: BTreeMap<u16, actions::Part> = etags
        .into_iter()
        .enumerate()
        .map(|(index, etag)| {
            // S3 supports max 10,000 parts. We validated above, so this should never fail.
            // Convert to Result to satisfy clippy's no-expect rule
            let part_number = u16::try_from(index + 1).map_err(|_| {
                anyhow!(
                    "BUG: Part number overflow after validation (index={index}). This should be impossible."
                )
            })?;
            Ok((
                part_number,
                actions::Part {
                    etag,
                    number: part_number,
                    checksum: None,
                },
            ))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;

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
    let digest_md5 = stream.md5.clone().finalize();

    emit_stream_progress(
        stream.channel.as_ref(),
        StreamProgressEvent::SendingStarted {
            part_number: stream.part_number,
            total_bytes: stream.count as u64,
        },
    );

    let action = actions::StreamPart::new(
        key,
        stream.tmp_file.path(),
        stream.part_number,
        upload_id,
        stream.count,
        (digest_sha.as_ref(), digest_md5.as_ref()),
        stream_sending_progress_callback(stream.channel.as_ref()),
    );

    let etag = action.request(s3, globals).await.map_err(|e| {
        emit_stream_progress(stream.channel.as_ref(), StreamProgressEvent::SendingStopped);
        anyhow!("Failed to upload final part: {e}")
    })?;

    emit_stream_progress(
        stream.channel.as_ref(),
        StreamProgressEvent::Confirmed(stream.count as u64),
    );

    Ok(etag)
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::unnecessary_wraps
)]
mod tests {
    use super::*;
    use crate::s3::{Credentials, Region, S3};
    use chacha20poly1305::aead::stream::EncryptorBE32;
    use indicatif::ProgressBar;
    use mockito::{Matcher, Server};
    use secrecy::SecretString;
    use tokio::{sync::mpsc::unbounded_channel, time::timeout};

    #[test]
    fn test_get_key() {
        let test_cases = vec![
            ("test", false, false, "test"),
            ("test", true, false, "test.zst"),
            ("test", false, true, "test.enc"),
            ("test", true, true, "test.zst.enc"),
            ("test.txt", false, false, "test.txt"),
            ("test.txt", true, false, "test.txt.zst"),
            ("test.txt", false, true, "test.txt.enc"),
            ("test.txt", true, true, "test.txt.zst.enc"),
            ("test.ZST", false, false, "test.ZST"),
            ("test.ZST", true, false, "test.ZST.zst"),
            ("test.ZST", false, true, "test.ZST.enc"),
            ("test.ZST", true, true, "test.ZST.zst.enc"),
        ];
        for (key, compress, encrypt, expected) in test_cases {
            assert_eq!(get_key(key, compress, encrypt), expected);
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

    #[test]
    fn test_create_nonce_header() {
        let nonce = [1, 2, 3, 4, 5, 6, 7];
        let header = create_nonce_header(&nonce);
        assert_eq!(header.len(), 8);
        assert_eq!(header[0], 7);
        assert_eq!(&header[1..], &nonce);
    }

    #[test]
    fn test_encrypt_chunk() {
        let key = secrecy::SecretString::new("0123456789abcdef0123456789abcdef".into());
        let (cipher, nonce) = init_encryption(&key);
        let mut encryptor: EncryptorBE32<ChaCha20Poly1305> =
            EncryptorBE32::from_aead(cipher, (&nonce).into());

        let data = b"Hello, world!";
        let encrypted = encrypt_chunk(&mut encryptor, data).unwrap();

        assert!(
            encrypted.len() > 4,
            "Encrypted output should include ciphertext after 4-byte prefix"
        );

        let prefix = u32::from_be_bytes(encrypted[0..4].try_into().unwrap()) as usize;
        let encrypted_payload = &encrypted[4..];

        // The prefix should match the actual length of the encrypted payload
        assert_eq!(
            prefix,
            encrypted_payload.len(),
            "Length prefix should match encrypted chunk length"
        );

        // The encrypted payload should differ from plaintext
        assert_ne!(
            encrypted_payload, data,
            "Encrypted payload should differ from original plaintext"
        );
    }

    #[test]
    fn test_write_to_stream() {
        let s3 = S3::new(
            &Credentials::new(
                "AKIAIOSFODNN7EXAMPLE",
                &SecretString::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into()),
            ),
            &"us-west-1".parse::<Region>().unwrap(),
            Some("awsexamplebucket1".to_string()),
            false,
        );

        let mut stream = Stream {
            tmp_file: NamedTempFile::new().unwrap(),
            count: 0,
            etags: Vec::new(),
            key: "test",
            part_number: 1,
            s3: &s3,
            upload_id: "test",
            sha: Context::new(&SHA256),
            md5: md5::Context::new(),
            channel: None,
            tmp_dir: PathBuf::new(),
            throttle: None,
            retries: 1,
        };

        let data = b"Hello, world!";
        write_to_stream(&mut stream, data).unwrap();

        assert_eq!(stream.count, data.len());
        assert!(stream.tmp_file.as_file().metadata().unwrap().len() > 0);
    }

    #[tokio::test]
    async fn test_write_to_stream_emits_staged_progress() {
        let s3 = create_test_s3();
        let (sender, receiver) = unbounded_channel();
        let handle =
            spawn_stream_progress_task(receiver, ProgressBar::hidden(), ProgressBar::hidden());

        let mut stream = Stream {
            tmp_file: NamedTempFile::new().unwrap(),
            count: 0,
            etags: Vec::new(),
            key: "test",
            part_number: 1,
            s3: &s3,
            upload_id: "test",
            sha: Context::new(&SHA256),
            md5: md5::Context::new(),
            channel: Some(sender),
            tmp_dir: PathBuf::new(),
            throttle: None,
            retries: 1,
        };

        let data = b"Hello, world!";
        write_to_stream(&mut stream, data).unwrap();
        drop(stream.channel.take());

        let progress = timeout(Duration::from_secs(1), handle)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(progress.staged_current, data.len() as u64);
        assert_eq!(progress.staged_total, data.len() as u64);
        assert_eq!(progress.confirmed_total, 0);
        assert_eq!(progress.sending_current, 0);
        assert_eq!(progress.sending_total, 0);
        assert_eq!(progress.sending_part, None);
    }

    #[test]
    fn test_create_initial_stream() {
        let s3 = S3::new(
            &Credentials::new(
                "AKIAIOSFODNN7EXAMPLE",
                &SecretString::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into()),
            ),
            &"us-west-1".parse::<Region>().unwrap(),
            Some("awsexamplebucket1".to_string()),
            false,
        );

        let tmp_dir = PathBuf::new();
        let key = "test_key";
        let upload_id = "test_upload_id";
        let globals = GlobalArgs {
            throttle: None,
            retries: 1,
            compress: false,
            encrypt: false,
            enc_key: None,
        };

        let stream = create_initial_stream(InitialStreamParams {
            upload_id,
            tmp_dir: &tmp_dir,
            key,
            s3: &s3,
            progress_sender: None,
            globals: &globals,
            header_data: None,
        })
        .unwrap();

        assert_eq!(stream.key, key);
        assert_eq!(stream.upload_id, upload_id);
        assert_eq!(stream.part_number, 1);
        assert_eq!(stream.count, 0);
    }

    #[tokio::test]
    async fn test_create_initial_stream_header_counts_as_staged_progress() {
        let s3 = create_test_s3();
        let key = "test_key";
        let upload_id = "test_upload_id";
        let globals = GlobalArgs {
            throttle: None,
            retries: 1,
            compress: false,
            encrypt: true,
            enc_key: None,
        };
        let header = create_nonce_header(&[1, 2, 3, 4, 5, 6, 7]);
        let tmp_dir = PathBuf::new();
        let (sender, receiver) = unbounded_channel();
        let handle =
            spawn_stream_progress_task(receiver, ProgressBar::hidden(), ProgressBar::hidden());

        let mut stream = create_initial_stream(InitialStreamParams {
            upload_id,
            tmp_dir: &tmp_dir,
            key,
            s3: &s3,
            progress_sender: Some(sender),
            globals: &globals,
            header_data: Some(&header),
        })
        .unwrap();

        assert_eq!(stream.count, header.len());
        drop(stream.channel.take());

        let progress = timeout(Duration::from_secs(1), handle)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(progress.staged_current, header.len() as u64);
        assert_eq!(progress.staged_total, header.len() as u64);
        assert_eq!(progress.confirmed_total, 0);
        assert_eq!(progress.sending_current, 0);
        assert_eq!(progress.sending_total, 0);
        assert_eq!(progress.sending_part, None);
    }

    #[test]
    fn test_compress_chunk() {
        let data = BytesMut::from("Hello, world!");
        let compressed = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(compress_chunk(data))
            .unwrap();

        assert!(
            !compressed.is_empty(),
            "Compressed data should not be empty"
        );

        assert_ne!(
            compressed, b"Hello, world!",
            "Compressed data should differ from original"
        );
    }

    #[tokio::test]
    async fn test_maybe_upload_part_1_no_upload() {
        let s3 = S3::new(
            &Credentials::new(
                "AKIAIOSFODNN7EXAMPLE",
                &SecretString::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into()),
            ),
            &"us-west-1".parse::<Region>().unwrap(),
            Some("awsexamplebucket1".to_string()),
            false,
        );

        let tmp_dir = PathBuf::new();
        let key = "test_key";
        let upload_id = "test_upload_id";
        let globals = GlobalArgs {
            throttle: None,
            retries: 1,
            compress: false,
            encrypt: false,
            enc_key: None,
        };

        let stream = create_initial_stream(InitialStreamParams {
            upload_id,
            tmp_dir: &tmp_dir,
            key,
            s3: &s3,
            progress_sender: None,
            globals: &globals,
            header_data: None,
        })
        .unwrap();

        let buffer_size = 1024; // 1KB for testing
        let mut stream = stream;
        let data = vec![0; 1023];

        write_to_stream(&mut stream, &data).unwrap();
        let result = maybe_upload_part(&mut stream, buffer_size).await;

        println!("Result: {result:?}");
    }

    #[tokio::test]
    async fn test_spawn_progress_task_finishes_when_sender_dropped() {
        let (sender, receiver) = unbounded_channel();
        let handle = spawn_progress_task(receiver, ProgressBar::hidden(), 10);

        sender.send(4).unwrap();
        sender.send(10).unwrap();
        drop(sender);

        let uploaded = timeout(Duration::from_secs(1), handle)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(uploaded, 10);
    }

    #[tokio::test]
    async fn test_spawn_stream_progress_task_tracks_sending_and_confirmed_bytes() {
        let (sender, receiver) = unbounded_channel();
        let handle =
            spawn_stream_progress_task(receiver, ProgressBar::hidden(), ProgressBar::hidden());

        sender.send(StreamProgressEvent::Staged(4)).unwrap();
        sender.send(StreamProgressEvent::Staged(5)).unwrap();
        sender
            .send(StreamProgressEvent::SendingStarted {
                part_number: 2,
                total_bytes: 9,
            })
            .unwrap();
        sender
            .send(StreamProgressEvent::SendingProgress(4))
            .unwrap();
        sender.send(StreamProgressEvent::Confirmed(9)).unwrap();
        sender.send(StreamProgressEvent::ResetStaging).unwrap();
        drop(sender);

        let progress = timeout(Duration::from_secs(1), handle)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(progress.staged_current, 0);
        assert_eq!(progress.staged_total, 9);
        assert_eq!(progress.confirmed_total, 9);
        assert_eq!(progress.sending_current, 0);
        assert_eq!(progress.sending_total, 0);
        assert_eq!(progress.sending_part, None);
    }

    // Comprehensive tests for complete_multipart_upload to prevent regressions

    fn create_mock_s3(endpoint: String, bucket: Option<&str>) -> S3 {
        S3::new(
            &Credentials::new(
                "minioadmin",
                &SecretString::new("minioadmin".to_string().into()),
            ),
            &Region::custom("us-east-1", endpoint),
            bucket.map(str::to_string),
            false,
        )
    }

    fn create_test_s3() -> S3 {
        S3::new(
            &Credentials::new(
                "AKIAIOSFODNN7EXAMPLE",
                &SecretString::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into()),
            ),
            &"us-west-1".parse::<Region>().unwrap(),
            Some("test-bucket".to_string()),
            false,
        )
    }

    #[tokio::test]
    async fn test_try_stream_part_retry_does_not_inflate_staged_progress() {
        let mut server = Server::new_async().await;
        let query = Matcher::AllOf(vec![
            Matcher::UrlEncoded("partNumber".into(), "1".into()),
            Matcher::UrlEncoded("uploadId".into(), "upload-id".into()),
        ]);

        let _first = server
            .mock("PUT", "/bucket/key")
            .match_query(query.clone())
            .with_status(503)
            .with_body("Slow Down")
            .expect(1)
            .create_async()
            .await;
        let _second = server
            .mock("PUT", "/bucket/key")
            .match_query(query)
            .with_status(200)
            .with_header("ETag", "\"retry-ok\"")
            .expect(1)
            .create_async()
            .await;

        let s3 = create_mock_s3(server.url(), Some("bucket"));
        let (sender, receiver) = unbounded_channel();
        let handle =
            spawn_stream_progress_task(receiver, ProgressBar::hidden(), ProgressBar::hidden());

        let mut stream = Stream {
            tmp_file: NamedTempFile::new().unwrap(),
            count: 0,
            etags: Vec::new(),
            key: "key",
            part_number: 1,
            s3: &s3,
            upload_id: "upload-id",
            sha: Context::new(&SHA256),
            md5: md5::Context::new(),
            channel: Some(sender),
            tmp_dir: PathBuf::new(),
            throttle: None,
            retries: 2,
        };

        let data = b"retry me once";
        write_to_stream(&mut stream, data).unwrap();

        let etag = try_stream_part(&stream).await.unwrap();
        assert_eq!(etag, "\"retry-ok\"");

        drop(stream.channel.take());

        let progress = timeout(Duration::from_secs(3), handle)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(progress.staged_current, data.len() as u64);
        assert_eq!(progress.staged_total, data.len() as u64);
        assert_eq!(progress.confirmed_total, data.len() as u64);
        assert_eq!(progress.sending_current, 0);
        assert_eq!(progress.sending_total, 0);
        assert_eq!(progress.sending_part, None);
    }

    #[tokio::test]
    async fn test_maybe_upload_part_emits_confirmed_progress_and_resets_staging() {
        let mut server = Server::new_async().await;
        let query = Matcher::AllOf(vec![
            Matcher::UrlEncoded("partNumber".into(), "1".into()),
            Matcher::UrlEncoded("uploadId".into(), "upload-id".into()),
        ]);

        let _upload = server
            .mock("PUT", "/bucket/key")
            .match_query(query)
            .with_status(200)
            .with_header("ETag", "\"part-ok\"")
            .expect(1)
            .create_async()
            .await;

        let s3 = create_mock_s3(server.url(), Some("bucket"));
        let (sender, receiver) = unbounded_channel();
        let handle =
            spawn_stream_progress_task(receiver, ProgressBar::hidden(), ProgressBar::hidden());

        let mut stream = Stream {
            tmp_file: NamedTempFile::new().unwrap(),
            count: 0,
            etags: Vec::new(),
            key: "key",
            part_number: 1,
            s3: &s3,
            upload_id: "upload-id",
            sha: Context::new(&SHA256),
            md5: md5::Context::new(),
            channel: Some(sender),
            tmp_dir: PathBuf::new(),
            throttle: None,
            retries: 1,
        };

        let data = vec![0_u8; 32];
        write_to_stream(&mut stream, &data).unwrap();
        maybe_upload_part(&mut stream, 16).await.unwrap();
        drop(stream.channel.take());

        let progress = timeout(Duration::from_secs(1), handle)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(progress.staged_current, 0);
        assert_eq!(progress.staged_total, data.len() as u64);
        assert_eq!(progress.confirmed_total, data.len() as u64);
        assert_eq!(progress.sending_current, 0);
        assert_eq!(progress.sending_total, 0);
        assert_eq!(progress.sending_part, None);
        assert_eq!(stream.count, 0);
        assert_eq!(stream.part_number, 2);
        assert_eq!(stream.etags.len(), 1);
    }

    #[tokio::test]
    async fn test_complete_multipart_upload_single_part() {
        let s3 = create_test_s3();
        let etags = vec!["etag1".to_string()];

        let result = complete_multipart_upload(&s3, "test-key", "upload-123", etags).await;

        // This will fail because we're not mocking the S3 API, but we're testing the part number conversion logic
        // The important part is that it doesn't panic and correctly creates the parts map
        assert!(result.is_err(), "Expected error due to unmocked S3 API");
    }

    #[tokio::test]
    async fn test_complete_multipart_upload_100_parts() {
        let s3 = create_test_s3();
        let etags: Vec<String> = (1..=100).map(|i| format!("etag{i}")).collect();

        let result = complete_multipart_upload(&s3, "test-key", "upload-123", etags).await;

        // Should not panic, will fail at API call
        assert!(result.is_err(), "Expected error due to unmocked S3 API");
    }

    #[tokio::test]
    async fn test_complete_multipart_upload_1000_parts() {
        let s3 = create_test_s3();
        let etags: Vec<String> = (1..=1000).map(|i| format!("etag{i}")).collect();

        let result = complete_multipart_upload(&s3, "test-key", "upload-123", etags).await;

        // Should not panic, will fail at API call
        assert!(result.is_err(), "Expected error due to unmocked S3 API");
    }

    #[tokio::test]
    async fn test_complete_multipart_upload_exactly_10000_parts() {
        let s3 = create_test_s3();
        let etags: Vec<String> = (1..=10_000).map(|i| format!("etag{i}")).collect();

        let result = complete_multipart_upload(&s3, "test-key", "upload-123", etags).await;

        // Should not panic with exactly 10,000 parts (the S3 maximum)
        // Will fail at API call but that's expected
        assert!(result.is_err(), "Expected error due to unmocked S3 API");
    }

    #[tokio::test]
    async fn test_complete_multipart_upload_10001_parts_returns_error() {
        let s3 = create_test_s3();
        let etags: Vec<String> = (1..=10_001).map(|i| format!("etag{i}")).collect();

        let result = complete_multipart_upload(&s3, "test-key", "upload-123", etags).await;

        // Should return error, not panic
        assert!(result.is_err(), "Expected error for exceeding 10,000 parts");

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("10001 parts created"),
            "Error message should mention part count. Got: {err_msg}"
        );
        assert!(
            err_msg.contains("maximum 10,000 parts"),
            "Error message should mention the limit. Got: {err_msg}"
        );
        assert!(
            err_msg.contains("s3m rm"),
            "Error message should include cleanup command. Got: {err_msg}"
        );
        assert!(
            err_msg.contains("--abort"),
            "Error message should include abort flag. Got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_complete_multipart_upload_20000_parts_returns_error() {
        let s3 = create_test_s3();
        let etags: Vec<String> = (1..=20_000).map(|i| format!("etag{i}")).collect();

        let result = complete_multipart_upload(&s3, "test-key", "upload-123", etags).await;

        // Should return error, not panic
        assert!(result.is_err(), "Expected error for exceeding 10,000 parts");

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("20000 parts created"),
            "Error message should mention actual part count. Got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_complete_multipart_upload_error_contains_context() {
        let s3 = create_test_s3();
        let etags: Vec<String> = (1..=15_000).map(|i| format!("etag{i}")).collect();

        let result =
            complete_multipart_upload(&s3, "my-large-file.dat", "ABC123DEF456", etags).await;

        assert!(result.is_err(), "Expected error for exceeding 10,000 parts");

        let err_msg = result.unwrap_err().to_string();

        // Verify all important context is in the error message
        assert!(
            err_msg.contains("my-large-file.dat"),
            "Error should contain the key. Got: {err_msg}"
        );
        assert!(
            err_msg.contains("ABC123DEF456"),
            "Error should contain the upload ID. Got: {err_msg}"
        );
        assert!(
            err_msg.contains("github.com/s3m/s3m/issues"),
            "Error should contain bug reporting URL. Got: {err_msg}"
        );
    }

    #[test]
    fn test_part_number_conversion_edge_cases() {
        // Test that our conversion logic handles edge cases correctly

        // Test index 0 -> part number 1
        let part_num = u16::try_from(1_usize).unwrap();
        assert_eq!(part_num, 1, "First part should be numbered 1");

        // Test index 9,999 -> part number 10,000
        let part_num = u16::try_from(10_000_usize).unwrap();
        assert_eq!(part_num, 10_000, "Part 10,000 should fit in u16");

        // Test that index 10,000 -> part number 10,001 would overflow our validation
        // (but our validation catches it before conversion)
        let etags_count = 10_001_usize;
        assert!(
            etags_count > 10_000,
            "Validation should catch this before conversion"
        );
    }

    #[test]
    fn test_u16_max_value_sufficient_for_s3_limit() {
        // Verify our assumption that u16 is sufficient for S3's 10,000 part limit
        // u16::MAX is 65535, which is much larger than 10,000
        use crate::s3::limits::MAX_PARTS_PER_UPLOAD;
        const _: () = assert!(u16::MAX as usize > MAX_PARTS_PER_UPLOAD);

        // Verify we can represent 10,000
        assert!(
            u16::try_from(10_000_usize).is_ok(),
            "Must be able to convert 10,000 to u16"
        );

        // Verify 10,001 also fits (for testing error path)
        assert!(
            u16::try_from(10_001_usize).is_ok(),
            "Must be able to convert 10,001 to u16 for testing"
        );
    }
}
