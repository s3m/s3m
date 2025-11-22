use crate::s3::checksum::{
    Checksum, ChecksumAlgorithm,
    hasher::{ChecksumHasher, Md5Hasher, Sha256Hasher},
};
use anyhow::{Result, anyhow};
use base64ct::{Base64, Encoding};
use bytes::Bytes;
use futures::stream::TryStreamExt;
use std::{io::SeekFrom, path::Path};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt},
    sync::mpsc,
    task,
};
use tokio_util::codec::{BytesCodec, FramedRead};

async fn compute_hash(
    mut rx: mpsc::Receiver<Bytes>,
    mut hasher: Box<dyn ChecksumHasher>,
) -> Result<Bytes> {
    while let Some(bytes) = rx.recv().await {
        hasher.update(&bytes);
    }

    Ok(hasher.finalize())
}

/// Compute the SHA256 and MD5 hashes of a file
/// # Errors
/// Will return an error if the file cannot be opened or read
#[allow(clippy::format_collect)]
pub async fn sha256_md5_digest(file_path: &Path) -> Result<(Bytes, Bytes, usize)> {
    let file = File::open(file_path).await?;

    // Buffer size is 256KB
    let mut stream = FramedRead::with_capacity(file, BytesCodec::new(), 1024 * 256);

    let mut length: usize = 0;

    // Create broadcast channels for MD5 and SHA256 hashes
    let (md5_tx, md5_rx) = mpsc::channel::<Bytes>(64);
    let (sha256_tx, sha256_rx) = mpsc::channel::<Bytes>(64);

    // Spawn tasks for MD5 and SHA256 hash calculations
    let md5_task = task::spawn(compute_hash(md5_rx, Box::<Md5Hasher>::default()));
    let sha256_task = task::spawn(compute_hash(sha256_rx, Box::<Sha256Hasher>::default()));

    // Read bytes from the stream and send them to the tasks
    while let Some(bytes) = stream.try_next().await? {
        let bytes_clone: Bytes = bytes.clone().into();

        md5_tx
            .send(bytes_clone.clone())
            .await
            .map_err(|err| anyhow!("Error sending to MD5 channel: {err}"))?;

        sha256_tx
            .send(bytes_clone.clone())
            .await
            .map_err(|err| anyhow!("Error sending to SHA256 channel: {err}"))?;

        length += &bytes.len();
    }

    drop(md5_tx);
    drop(sha256_tx);

    // Wait for both tasks to complete concurrently
    let (md5_hash, sha256_hash) = tokio::try_join!(md5_task, sha256_task)?;

    Ok((sha256_hash?, md5_hash?, length))
}

/// Compute the SHA256 and MD5 hashes of a file chunk
/// # Errors
/// Will return an error if the file cannot be opened or read
#[allow(clippy::format_collect)]
pub async fn sha256_md5_digest_multipart(
    file_path: &Path,
    seek: u64,
    chunk: u64,
    mut algorithm: Option<&mut Checksum>,
) -> Result<(Bytes, Bytes, usize, Option<Checksum>)> {
    let mut file = File::open(file_path).await?;

    // Seek to the start position
    file.seek(SeekFrom::Start(seek)).await?;

    // Take the chunk
    let file = file.take(chunk);

    // Buffer size is 256KB,
    let mut stream = FramedRead::with_capacity(file, BytesCodec::new(), 1024 * 256);

    let mut length: usize = 0;

    // Create broadcast channels for MD5 and SHA256 hashes
    let (md5_tx, md5_rx) = mpsc::channel::<Bytes>(64);
    let (sha256_tx, sha256_rx) = mpsc::channel::<Bytes>(64);

    // Spawn tasks for MD5 and SHA256 hash calculations
    let md5_task = task::spawn(compute_hash(md5_rx, Box::<Md5Hasher>::default()));
    let sha256_task = task::spawn(compute_hash(sha256_rx, Box::<Sha256Hasher>::default()));

    let mut hasher_channel: Option<(mpsc::Sender<Bytes>, task::JoinHandle<Result<Bytes>>)> = None;

    if let Some(ref mut hasher) = algorithm {
        // Create a channel and spawn the task for the hasher if the algorithm is not SHA256
        // because SHA256 is already calculated and can be reused
        if hasher.algorithm != ChecksumAlgorithm::Sha256 {
            let (hasher_tx, hasher_rx) = mpsc::channel::<Bytes>(64);
            let hasher_task = task::spawn(compute_hash(hasher_rx, hasher.hasher()));

            hasher_channel = Some((hasher_tx, hasher_task));
        }
    }

    while let Some(bytes) = stream.try_next().await? {
        let bytes_clone: Bytes = bytes.clone().into();

        md5_tx
            .send(bytes_clone.clone())
            .await
            .map_err(|err| anyhow!("Error sending to MD5 channel: {err}"))?;

        sha256_tx
            .send(bytes_clone.clone())
            .await
            .map_err(|err| anyhow!("Error sending to SHA256 channel: {err}"))?;

        if let Some((ref hasher_tx, _)) = hasher_channel {
            hasher_tx
                .send(bytes_clone)
                .await
                .map_err(|err| anyhow!("Error sending to hasher channel: {err}"))?;
        }

        length += &bytes.len();
    }

    drop(md5_tx);
    drop(sha256_tx);

    let (md5_hash, sha256_hash, hasher_result) = tokio::join!(
        md5_task,
        sha256_task,
        hasher_channel.map_or_else(
            || task::spawn(async { Ok(Bytes::default()) }),
            |(_, hasher_task)| hasher_task
        )
    );

    let sha256_hash = sha256_hash??;
    let md5_hash = md5_hash??;

    if let Some(checksum) = algorithm {
        checksum.checksum = match checksum.algorithm {
            ChecksumAlgorithm::Sha256 => Base64::encode_string(&sha256_hash),
            _ => match hasher_result {
                Ok(hasher_result) => Base64::encode_string(&hasher_result?),
                _ => return Err(anyhow!("Error calculating checksum")),
            },
        };

        return Ok((sha256_hash, md5_hash, length, Some(checksum.clone())));
    }

    Ok((sha256_hash, md5_hash, length, None))
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::unnecessary_wraps,
    clippy::format_collect
)]
mod tests {
    use super::*;
    use crate::s3::checksum::ChecksumAlgorithm;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_sha256_md5_digest() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"hello world").unwrap();
        let (sha256, md5, length) = sha256_md5_digest(file.path()).await.unwrap();
        assert_eq!(
            sha256
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>(),
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
        assert_eq!(
            md5.iter().map(|b| format!("{b:02x}")).collect::<String>(),
            "5eb63bbbe01eeed093cb22bb8f5acdc3"
        );
        assert_eq!(length, 11);
    }

    #[tokio::test]
    async fn test_sha256_md5_digest_multipart() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"hello world").unwrap();
        let (sha256, md5, length, _) = sha256_md5_digest_multipart(file.path(), 0, 5, None)
            .await
            .unwrap();
        assert_eq!(
            sha256
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>(),
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
        assert_eq!(
            md5.iter().map(|b| format!("{b:02x}")).collect::<String>(),
            "5d41402abc4b2a76b9719d911017c592"
        );
        assert_eq!(length, 5);
    }

    #[tokio::test]
    async fn test_sha256_md5_digest_multipart_sha256() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"hello world").unwrap();
        let mut checksum = Checksum::new(ChecksumAlgorithm::Sha256);
        let (sha256, md5, length, _) =
            sha256_md5_digest_multipart(file.path(), 0, 5, Some(&mut checksum))
                .await
                .unwrap();
        assert_eq!(
            sha256
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>(),
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
        assert_eq!(
            md5.iter().map(|b| format!("{b:02x}")).collect::<String>(),
            "5d41402abc4b2a76b9719d911017c592"
        );
        assert_eq!(length, 5);
    }

    struct Test {
        algorithm: ChecksumAlgorithm,
        expected_sha256: &'static str,
        expected_md5: &'static str,
        expected_checksum: &'static str,
    }

    #[tokio::test]
    async fn test_sha256_md5_digest_multipart_with_checksum() {
        let tests = [
            Test {
                algorithm: ChecksumAlgorithm::Sha256,
                expected_sha256: "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
                expected_md5: "5d41402abc4b2a76b9719d911017c592",
                // echo -n "hello" | openssl sha256 -binary | base64
                expected_checksum: "LPJNul+wow4m6DsqxbninhsWHlwfp0JecwQzYpOLmCQ=",
            },
            Test {
                algorithm: ChecksumAlgorithm::Md5,
                expected_sha256: "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
                expected_md5: "5d41402abc4b2a76b9719d911017c592",
                // echo -n "hello" | openssl md5 -binary | base64
                expected_checksum: "XUFAKrxLKna5cZ2REBfFkg==",
            },
            Test {
                algorithm: ChecksumAlgorithm::Sha1,
                expected_sha256: "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
                expected_md5: "5d41402abc4b2a76b9719d911017c592",
                // echo -n "hello" | openssl sha1 -binary | base64
                expected_checksum: "qvTGHdzF6KLavt4PO0gs2a6pQ00=",
            },
            Test {
                algorithm: ChecksumAlgorithm::Crc32,
                expected_sha256: "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
                expected_md5: "5d41402abc4b2a76b9719d911017c592",
                // python3 -c "import base64, binascii; print(base64.b64encode(binascii.crc32('hello'.encode('utf-8')).to_bytes(4, 'big')).decode('utf-8'))"
                expected_checksum: "NhCmhg==",
            },
            Test {
                algorithm: ChecksumAlgorithm::Crc32c,
                expected_sha256: "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
                expected_md5: "5d41402abc4b2a76b9719d911017c592",
                // python3 -c "import base64, crc32c; print(base64.b64encode(crc32c.crc32c('hello'.encode('utf-8')).to_bytes(4, 'big')).decode('utf-8'))"
                expected_checksum: "mnG7TA==",
            },
        ];

        for test in &tests {
            let mut file = NamedTempFile::new().unwrap();
            file.write_all(b"hello world").unwrap();
            let mut checksum = Checksum::new(test.algorithm.clone());
            let (sha256, md5, length, r_checksum) =
                sha256_md5_digest_multipart(file.path(), 0, 5, Some(&mut checksum))
                    .await
                    .unwrap();
            let get_hex =
                |bytes: Bytes| bytes.iter().map(|b| format!("{b:02x}")).collect::<String>();

            assert_eq!(length, 5);
            assert_eq!(get_hex(sha256), test.expected_sha256);
            assert_eq!(get_hex(md5), test.expected_md5);
            assert_eq!(checksum.checksum, test.expected_checksum);
            assert_eq!(r_checksum.unwrap().checksum, test.expected_checksum);
        }
    }
}
