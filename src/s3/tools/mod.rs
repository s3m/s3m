use anyhow::Result;
use base64ct::{Base64, Encoding};
use futures::stream::TryStreamExt;
use ring::{
    digest,
    digest::{Context, SHA256},
    hmac,
};
use std::{fmt::Write, io::prelude::*, io::SeekFrom, path::Path};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt},
};
use tokio_util::codec::{BytesCodec, FramedRead};

/// # Errors
///
/// Will return `Err` if can not open the file
pub async fn sha256_md5_digest(file_path: &Path) -> Result<(digest::Digest, md5::Digest, usize)> {
    let file = File::open(file_path).await?;
    let mut stream = FramedRead::with_capacity(file, BytesCodec::new(), 1024 * 256);
    let mut context_sha = Context::new(&SHA256);
    let mut context_md5 = md5::Context::new();
    let mut length: usize = 0;
    while let Some(bytes) = stream.try_next().await? {
        context_sha.update(&bytes);
        context_md5.consume(&bytes);
        length += &bytes.len();
    }
    let digest_sha = context_sha.finish();
    let digest_md5 = context_md5.compute();
    Ok((digest_sha, digest_md5, length))
}

/// # Errors
///
/// Will return `Err` if can not open the file
pub async fn sha256_md5_digest_multipart(
    file_path: &str,
    seek: u64,
    chunk: u64,
) -> Result<(digest::Digest, md5::Digest, usize)> {
    let mut file = File::open(file_path).await?;

    file.seek(SeekFrom::Start(seek)).await?;

    let file = file.take(chunk);
    let mut stream = FramedRead::with_capacity(file, BytesCodec::new(), 1024 * 256);
    let mut context_sha = Context::new(&SHA256);
    let mut context_md5 = md5::Context::new();
    let mut length: usize = 0;

    while let Some(bytes) = stream.try_next().await? {
        context_sha.update(&bytes);
        context_md5.consume(&bytes);
        length += &bytes.len();
    }

    let digest_sha = context_sha.finish();
    let digest_md5 = context_md5.compute();

    Ok((digest_sha, digest_md5, length))
}

#[must_use]
pub fn sha256_digest(input: impl AsRef<[u8]>) -> digest::Digest {
    digest::digest(&digest::SHA256, input.as_ref())
}

pub fn base64_md5(input: impl AsRef<[u8]>) -> String {
    let md5_digest = md5::compute(input);
    Base64::encode_string(md5_digest.as_ref())
}

#[must_use]
pub fn sha256_hmac(key: &[u8], msg: &[u8]) -> hmac::Tag {
    let s_key = hmac::Key::new(hmac::HMAC_SHA256, key);
    hmac::sign(&s_key, msg)
}

#[must_use]
pub fn write_hex_bytes(bytes: &[u8]) -> String {
    let mut s = String::new();
    for byte in bytes {
        write!(&mut s, "{byte:02x}").expect("Unable to write");
    }
    s
}

/// # Errors
///
/// Will return `Err` if can not open the file
pub fn blake3(file_path: &str) -> Result<String> {
    let mut file = std::fs::File::open(file_path)?;
    let mut hasher = blake3::Hasher::new();
    let mut buf = [0_u8; 65536];

    while let Ok(size) = file.read(&mut buf[..]) {
        if size == 0 {
            break;
        }
        hasher.update(&buf[0..size]);
    }

    Ok(hasher.finalize().to_hex().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_sha256_md5_digest() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"hello world").unwrap();
        let (digest_sha, digest_md5, length) = sha256_md5_digest(&file.path()).await.unwrap();
        assert_eq!(
            write_hex_bytes(digest_sha.as_ref()),
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
        assert_eq!(
            write_hex_bytes(digest_md5.as_ref()),
            "5eb63bbbe01eeed093cb22bb8f5acdc3"
        );
        assert_eq!(length, 11);
    }

    #[tokio::test]
    async fn test_sha256_md5_digest_multipart() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"hello world").unwrap();
        let (digest_sha, digest_md5, length) =
            sha256_md5_digest_multipart(&file.path().to_str().unwrap(), 0, 5)
                .await
                .unwrap();
        assert_eq!(
            write_hex_bytes(digest_sha.as_ref()),
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
        assert_eq!(
            write_hex_bytes(digest_md5.as_ref()),
            "5d41402abc4b2a76b9719d911017c592"
        );
        assert_eq!(length, 5);
    }

    #[test]
    fn test_sha256_digest() {
        let digest = sha256_digest(b"hello world");
        assert_eq!(
            write_hex_bytes(digest.as_ref()),
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
    }

    #[test]
    fn test_base64_md5() {
        let md5 = base64_md5(b"hello world");
        assert_eq!(md5, "XrY7u+Ae7tCTyyK7j1rNww==");
    }

    #[test]
    fn test_sha256_hmac() {
        let key = b"key";
        let msg = b"The quick brown fox jumps over the lazy dog";
        let tag = sha256_hmac(key, msg);
        assert_eq!(
            write_hex_bytes(tag.as_ref()),
            "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8"
        );
    }

    #[test]
    fn test_write_hex_bytes() {
        let bytes = b"hello world";
        assert_eq!(write_hex_bytes(bytes), "68656c6c6f20776f726c64");
    }

    #[test]
    fn test_blake3() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"hello world").unwrap();
        let hash = blake3(&file.path().to_str().unwrap()).unwrap();
        assert_eq!(
            hash,
            "d74981efa70a0c880b8d8c1985d075dbcbf679b99a5f9914e5aaf96b831a9e24"
        );
    }
}
