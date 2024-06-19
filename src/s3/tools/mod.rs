use anyhow::{anyhow, Result};
use base64ct::{Base64, Encoding};
use ring::{digest, hmac};
use std::{fmt::Write, io::prelude::*, path::Path};

const MAX_PART_SIZE: u64 = 5_368_709_120;
const MAX_PARTS_PER_UPLOAD: usize = 10_000;

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

// Calculate part size for multipart upload (max 5 GB)
// <https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html>
/// # Errors
/// Will return `Err` if the file size exceeds 5 TB
/// or if the part size exceeds 5 GB
pub fn calculate_part_size(file_size: u64, buf_size: u64) -> Result<u64> {
    log::info!("file size: {}, buf size: {}", file_size, buf_size);

    let mut part_size = buf_size.max(1);

    while {
        let calculated_parts = (file_size.saturating_add(part_size).saturating_sub(1)) / part_size;
        log::debug!(
            "Calculated parts: {}, Part size: {}, Max parts: {}",
            calculated_parts,
            part_size,
            MAX_PARTS_PER_UPLOAD
        );
        calculated_parts > MAX_PARTS_PER_UPLOAD as u64
    } {
        part_size = part_size.saturating_mul(2);
    }

    if part_size > MAX_PART_SIZE {
        log::error!("max part size 5 GB");
        return Err(anyhow!("max part size 5 GB"));
    }

    log::info!("part size: {}", part_size);

    Ok(part_size)
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
    use crate::stream::iterator::PartIterator;
    use std::io::Write;
    use tempfile::NamedTempFile;

    const MAX_FILE_SIZE: u64 = 5_497_558_138_880;

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

    #[test]
    fn test_calculate_part_size() {
        let file_size = 5 * 1024 * 1024 * 1000 * 1000;
        let buf_size = 10 * 1024 * 1024;
        let part_size = calculate_part_size(file_size, buf_size).unwrap();
        assert!(part_size <= MAX_PART_SIZE);
        assert_eq!(part_size, 671_088_640);
    }

    #[test]
    fn test_calculate_part_size_10000parts() {
        let file_size = 5 * 1024 * 1024 * 1000 * 1000;
        let buf_size = 512 * 1024 * 1024;
        let part_size = calculate_part_size(file_size, buf_size).unwrap();
        assert!(part_size <= MAX_PART_SIZE);
        assert_eq!(part_size, buf_size);
    }

    #[test]
    fn test_calculate_part_size_within_limits() {
        let result = calculate_part_size(1_000_000, 1_000);
        assert!(result.is_ok());
    }

    #[test]
    fn test_calculate_part_size_exceeds_max_part_size() {
        let result = calculate_part_size(1_000_000, MAX_PART_SIZE + 1);
        assert!(result.is_err());
        assert_eq!(result.err().unwrap().to_string(), "max part size 5 GB");
    }

    #[test]
    fn test_calculate_part_size_5tb_part_512mb() {
        let file_size = 5 * 1024 * 1024 * 1000 * 1000;
        let buf_size = 1;
        let result = calculate_part_size(file_size, buf_size).unwrap();
        assert_eq!(result, MAX_PART_SIZE / 10);
    }

    #[test]
    fn test_calculate_part_size_5tb_part_1gb() {
        let file_size = 5 * 1024 * 1024 * 1000 * 1000;
        let buf_size = 1 * 1024 * 1024 * 1024;
        let result = calculate_part_size(file_size, buf_size).unwrap();
        assert_eq!(result, MAX_PART_SIZE / 5);
    }

    #[test]
    fn test_calculate_part_size_max_part_size() {
        let file_size = 5 * 1024 * 1024 * 1000 * 1000;
        let part_size = calculate_part_size(file_size, MAX_PART_SIZE).unwrap();
        assert_eq!(part_size, MAX_PART_SIZE);
        let (number, seek, chunk) = PartIterator::new(file_size, part_size).last().unwrap();
        assert_eq!(file_size, seek + chunk);
        assert!(usize::from(number) <= MAX_PARTS_PER_UPLOAD);
    }

    #[test]
    fn test_calculate_part_size_max_part_size_512() {
        let file_size = 5 * 1024 * 1024 * 1000 * 1000;
        let buf_size = 512 * 1024 * 1024;
        let part_size = calculate_part_size(file_size, buf_size).unwrap();
        assert_eq!(part_size, buf_size);
        let (number, seek, chunk) = PartIterator::new(file_size, part_size).last().unwrap();
        assert_eq!(file_size, seek + chunk);
        assert!(usize::from(number) <= MAX_PARTS_PER_UPLOAD);
    }

    #[test]
    fn test_calculate_part_size_max_part_per_upload() {
        let buf_size = 549755814;
        let part_size = calculate_part_size(MAX_FILE_SIZE, buf_size).unwrap();
        let (number, seek, chunk) = PartIterator::new(MAX_FILE_SIZE, part_size).last().unwrap();
        assert_eq!(MAX_FILE_SIZE, seek + chunk);
        assert!(usize::from(number) <= MAX_PARTS_PER_UPLOAD);
    }

    #[test]
    fn test_calculate_part_size_max_part_per_upload_1() {
        let buf_size = 1;
        let part_size = calculate_part_size(MAX_FILE_SIZE, buf_size).unwrap();
        let (number, seek, chunk) = PartIterator::new(MAX_FILE_SIZE, part_size).last().unwrap();
        assert_eq!(MAX_FILE_SIZE, seek + chunk);
        assert!(usize::from(number) <= MAX_PARTS_PER_UPLOAD);
    }

    #[test]
    fn test_calculate_part_size_max_part_per_upload_2() {
        let part_size = calculate_part_size(15, 4).unwrap();
        let mut parts = PartIterator::new(15, part_size);
        assert_eq!(parts.next(), Some((1, 0, 4)));
        assert_eq!(parts.next(), Some((2, 4, 4)));
        assert_eq!(parts.next(), Some((3, 8, 4)));
        assert_eq!(parts.next(), Some((4, 12, 3)));
        assert_eq!(parts.next(), None);
    }

    #[test]
    fn test_calculate_part_size_max_part_per_upload_3() {
        let part_size = calculate_part_size(15, 5).unwrap();
        let mut parts = PartIterator::new(15, part_size);
        assert_eq!(parts.next(), Some((1, 0, 5)));
        assert_eq!(parts.next(), Some((2, 5, 5)));
        assert_eq!(parts.next(), Some((3, 10, 5)));
        assert_eq!(parts.next(), None);
    }

    #[test]
    fn test_calculate_part_size_max_part_per_upload_4() {
        let buf_size = 52428800;
        let part_size = calculate_part_size(524288000000, buf_size).unwrap();
        let (number, seek, chunk) = PartIterator::new(524288000000, part_size).last().unwrap();
        assert_eq!(524288000000, seek + chunk);
        assert!(usize::from(number) <= MAX_PARTS_PER_UPLOAD);
    }

    #[test]
    fn test_calculate_part_size_10k_parts() {
        // Use a file size that results in exactly 10,000 parts
        let file_size = 100_000_000; // Assume a fixed file size
        let buf_size = 10_000; // Assume a fixed buffer size

        let part_size = calculate_part_size(file_size, buf_size).unwrap();

        // Ensure that the calculated part size leads to exactly 10,000 parts
        assert_eq!((file_size + part_size - 1) / part_size, 10_000);
    }

    #[test]
    fn test_calculate_part_size_less_than_10k_parts() {
        // Use a file size that results in less than 10,000 parts
        let file_size = 90_000_000; // Assume a fixed file size
        let buf_size = 10_000; // Set buf_size to a value that ensures less than 10,000 parts

        let part_size = calculate_part_size(file_size, buf_size).unwrap();

        // Ensure that the calculated part size results in less than 10,000 parts
        assert!((file_size + part_size - 1) / part_size < 10_000);
    }

    #[test]
    fn test_calculate_part_size_buffer_size_0() {
        let part_size = calculate_part_size(MAX_FILE_SIZE, 0).unwrap();
        let (number, seek, chunk) = PartIterator::new(MAX_FILE_SIZE, part_size).last().unwrap();
        assert_eq!(MAX_FILE_SIZE, seek + chunk);
        assert!(usize::from(number) <= MAX_PARTS_PER_UPLOAD);
        assert!((MAX_FILE_SIZE + part_size - 1) / part_size < 10_000);
    }
}
