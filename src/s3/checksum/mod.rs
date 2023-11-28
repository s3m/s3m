use anyhow::Result;
use base64ct::{Base64, Encoding};
use crc32c::Crc32cHasher;
use crc32fast::Hasher;
use ring::digest::{Context, SHA1_FOR_LEGACY_USE_ONLY, SHA256};
use std::{fs::File, hash::Hasher as _, io::Read};

#[derive(Debug, Eq, PartialEq)]
pub enum ChecksumAlgorithm {
    Crc32,
    Crc32c,
    Sha1,
    Sha256,
}

impl ChecksumAlgorithm {
    pub fn from_str(algorithm: &str) -> Option<Self> {
        match algorithm {
            "crc32" => Some(Self::Crc32),
            "crc32c" => Some(Self::Crc32c),
            "sha1" => Some(Self::Sha1),
            "sha256" => Some(Self::Sha256),
            _ => None,
        }
    }
}

pub struct Checksum {
    pub algorithm: ChecksumAlgorithm,
}

impl Checksum {
    pub const fn new(algorithm: ChecksumAlgorithm) -> Self {
        Self { algorithm }
    }

    pub fn calculate(&self, file_path: &str) -> Result<String> {
        match self.algorithm {
            ChecksumAlgorithm::Crc32 => self.calculate_crc32(file_path),
            ChecksumAlgorithm::Crc32c => self.calculate_crc32c(file_path),
            ChecksumAlgorithm::Sha1 => self.calculate_sha1(file_path),
            ChecksumAlgorithm::Sha256 => self.calculate_sha256(file_path),
        }
    }

    fn calculate_crc32(&self, file_path: &str) -> Result<String> {
        let mut file = File::open(file_path)?;
        let mut buf = [0_u8; 65_536];

        let mut hasher = Hasher::new();
        while let Ok(size) = file.read(&mut buf[..]) {
            if size == 0 {
                break;
            }
            hasher.update(&buf[0..size]);
        }

        Ok(Base64::encode_string(&hasher.finalize().to_be_bytes()))
    }

    fn calculate_crc32c(&self, file_path: &str) -> Result<String> {
        let mut file = File::open(file_path)?;
        let mut buf = [0_u8; 65_536];

        let mut hasher = Crc32cHasher::new(Default::default());
        while let Ok(size) = file.read(&mut buf[..]) {
            if size == 0 {
                break;
            }
            hasher.write(&buf[0..size]);
        }

        Ok(Base64::encode_string(&hasher.finish().to_be_bytes()))
    }

    fn calculate_sha1(&self, file_path: &str) -> Result<String> {
        let mut file = File::open(file_path)?;
        let mut buf = [0_u8; 65_536];

        let mut context = Context::new(&SHA1_FOR_LEGACY_USE_ONLY);
        while let Ok(size) = file.read(&mut buf[..]) {
            if size == 0 {
                break;
            }
            context.update(&buf[0..size]);
        }

        Ok(Base64::encode_string(context.finish().as_ref()))
    }

    fn calculate_sha256(&self, file_path: &str) -> Result<String> {
        let mut file = File::open(file_path)?;
        let mut buf = [0_u8; 65_536];

        let mut context = Context::new(&SHA256);
        while let Ok(size) = file.read(&mut buf[..]) {
            if size == 0 {
                break;
            }
            context.update(&buf[0..size]);
        }

        Ok(Base64::encode_string(context.finish().as_ref()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_crc32() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"hello world").unwrap();
        let checksum = Checksum::new(ChecksumAlgorithm::Crc32);
        let result = checksum.calculate(&file.path().to_string_lossy()).unwrap();
        assert_eq!(result, "DUoRhQ==");
    }

    #[test]
    fn test_crc32c() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"hello world").unwrap();
        let checksum = Checksum::new(ChecksumAlgorithm::Crc32c);
        let result = checksum.calculate(&file.path().to_string_lossy()).unwrap();
        assert_eq!(result, "AAAAAMmUZao=");
    }

    #[test]
    fn test_sha1() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"hello world").unwrap();
        let checksum = Checksum::new(ChecksumAlgorithm::Sha1);
        let result = checksum.calculate(&file.path().to_string_lossy()).unwrap();
        assert_eq!(result, "Kq5sNclPz7QV2+lfQIuc6R7oRu0=");
    }

    #[test]
    fn test_sha256() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"hello world").unwrap();
        let checksum = Checksum::new(ChecksumAlgorithm::Sha256);
        let result = checksum.calculate(&file.path().to_string_lossy()).unwrap();
        assert_eq!(result, "uU0nuZNNPgilLlLX2n2r+sSE7+N6U4DukIj3rOLvzek=");
    }

    #[test]
    fn test_from_str() {
        assert_eq!(
            ChecksumAlgorithm::from_str("crc32"),
            Some(ChecksumAlgorithm::Crc32)
        );
        assert_eq!(
            ChecksumAlgorithm::from_str("crc32c"),
            Some(ChecksumAlgorithm::Crc32c)
        );
        assert_eq!(
            ChecksumAlgorithm::from_str("sha1"),
            Some(ChecksumAlgorithm::Sha1)
        );
        assert_eq!(
            ChecksumAlgorithm::from_str("sha256"),
            Some(ChecksumAlgorithm::Sha256)
        );
        assert_eq!(ChecksumAlgorithm::from_str("md5"), None);
    }
}
