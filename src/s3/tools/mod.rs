use anyhow::Result;
use base64ct::{Base64, Encoding};
use futures::stream::TryStreamExt;
use ring::{
    digest,
    digest::{Context, SHA256},
    hmac,
};
use std::fmt::Write;
use std::io::prelude::*;
use std::io::SeekFrom;
use std::path::Path;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
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
        write!(&mut s, "{:02x}", byte).expect("Unable to write");
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
