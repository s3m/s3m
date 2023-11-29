use anyhow::Result;
use base64ct::{Base64, Encoding};
use ring::{digest, hmac};
use std::{fmt::Write, io::prelude::*, path::Path};

#[must_use]
pub fn sha256_digest(input: impl AsRef<[u8]>) -> digest::Digest {
    digest::digest(&digest::SHA256, input.as_ref())
}

#[must_use]
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
pub fn blake3(file: &Path) -> Result<String> {
    let mut file = std::fs::File::open(file)?;
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
        let hash = blake3(&file.path()).unwrap();
        assert_eq!(
            hash,
            "d74981efa70a0c880b8d8c1985d075dbcbf679b99a5f9914e5aaf96b831a9e24"
        );
    }
}
