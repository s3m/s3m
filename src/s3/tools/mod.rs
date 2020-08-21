use futures::stream::TryStreamExt;
use ring::{
    digest,
    digest::{Context, SHA256},
    hmac,
};
use std::error::Error;
use std::fmt::Write;
use std::io::prelude::*;
use std::io::BufReader;
use std::io::SeekFrom;
use tokio::fs::File;
use tokio::prelude::*;
use tokio_util::codec::{BytesCodec, FramedRead};

/// # Errors
///
/// Will return `Err` if can not open the file
pub async fn sha256_md5_digest(file_path: &str) -> Result<(String, String, usize), Box<dyn Error>> {
    let file = File::open(file_path).await?;
    let mut stream = FramedRead::new(file, BytesCodec::new());
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
    Ok((
        write_hex_bytes(digest_sha.as_ref()),
        base64::encode(digest_md5.as_ref()),
        length,
    ))
}

/// # Errors
///
/// Will return `Err` if can not open the file
pub async fn sha256_md5_multipart(
    file_path: &str,
    seek: u64,
    chunk: u64,
) -> Result<(String, String, usize), Box<dyn Error>> {
    let mut file = File::open(file_path).await?;
    file.seek(SeekFrom::Start(seek)).await?;
    let file = file.take(chunk);
    let mut stream = FramedRead::new(file, BytesCodec::new());
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
    Ok((
        write_hex_bytes(digest_sha.as_ref()),
        base64::encode(digest_md5.as_ref()),
        length,
    ))
}

#[must_use]
pub fn sha256_digest_string(string: &str) -> String {
    write_hex_bytes(digest::digest(&digest::SHA256, string.as_bytes()).as_ref())
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
pub fn blake3(file_path: &str) -> Result<String, Box<dyn Error>> {
    let file = std::fs::File::open(file_path)?;
    let mut reader = BufReader::new(file);
    let mut hasher = blake3::Hasher::new();
    let mut buf: [u8; 65536] = [0; 65536];
    while let Ok(size) = reader.read(&mut buf[..]) {
        if size == 0 {
            break;
        }
        hasher.update(&buf[0..size]);
    }
    Ok(hasher.finalize().to_hex().to_string())
}
