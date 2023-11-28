use anyhow::Result;
use base64ct::{Base64, Encoding};
use crc32c::{crc32c, crc32c_append};
use crc32fast::Hasher;
use ring::digest::{Context, SHA1_FOR_LEGACY_USE_ONLY, SHA256};
use std::{fs::File, io::Read, str::FromStr};

#[derive(Debug, Eq, PartialEq)]
pub enum ChecksumAlgorithm {
    Crc32,
    Crc32c,
    Sha1,
    Sha256,
    None,
}

impl ChecksumAlgorithm {
    pub const fn as_amz(&self) -> &'static str {
        match self {
            Self::Crc32 => "x-amz-checksum-crc32",
            Self::Crc32c => "x-amz-checksum-crc32c",
            Self::Sha1 => "x-amz-checksum-sha1",
            Self::Sha256 => "x-amz-checksum-sha256",
            Self::None => "",
        }
    }
}

impl FromStr for ChecksumAlgorithm {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "crc32" => Ok(Self::Crc32),
            "crc32c" => Ok(Self::Crc32c),
            "sha1" => Ok(Self::Sha1),
            "sha256" => Ok(Self::Sha256),
            _ => Err(()),
        }
    }
}

#[derive(Debug)]
pub struct Checksum {
    pub algorithm: ChecksumAlgorithm,
    pub checksum: String,
}

impl Checksum {
    pub const fn new(algorithm: ChecksumAlgorithm) -> Self {
        Self {
            algorithm,
            checksum: String::new(),
        }
    }

    pub fn calculate(&mut self, file_path: &str) -> Result<String> {
        match self.algorithm {
            ChecksumAlgorithm::Crc32 => self.calculate_crc32(file_path),
            ChecksumAlgorithm::Crc32c => self.calculate_crc32c(file_path),
            ChecksumAlgorithm::Sha1 => self.calculate_sha1(file_path),
            ChecksumAlgorithm::Sha256 => self.calculate_sha256(file_path),
            ChecksumAlgorithm::None => Ok(String::new()),
        }
    }

    fn calculate_crc32(&mut self, file_path: &str) -> Result<String> {
        let mut file = File::open(file_path)?;
        let mut buf = [0_u8; 65_536];

        let mut hasher = Hasher::new();
        while let Ok(size) = file.read(&mut buf[..]) {
            if size == 0 {
                break;
            }
            hasher.update(&buf[0..size]);
        }

        self.checksum = Base64::encode_string(&hasher.finalize().to_be_bytes());
        Ok(self.checksum.clone())
    }

    fn calculate_crc32c(&mut self, file_path: &str) -> Result<String> {
        let mut file = File::open(file_path)?;
        let mut buf = [0_u8; 65_536];

        let mut hasher: u32 = crc32c(&[]);
        while let Ok(size) = file.read(&mut buf[..]) {
            if size == 0 {
                break;
            }
            hasher = crc32c_append(hasher, &buf[0..size]);
        }

        self.checksum = Base64::encode_string(&hasher.to_be_bytes());
        Ok(self.checksum.clone())
    }

    fn calculate_sha1(&mut self, file_path: &str) -> Result<String> {
        let mut file = File::open(file_path)?;
        let mut buf = [0_u8; 65_536];

        let mut context = Context::new(&SHA1_FOR_LEGACY_USE_ONLY);
        while let Ok(size) = file.read(&mut buf[..]) {
            if size == 0 {
                break;
            }
            context.update(&buf[0..size]);
        }

        self.checksum = Base64::encode_string(context.finish().as_ref());
        Ok(self.checksum.clone())
    }

    fn calculate_sha256(&mut self, file_path: &str) -> Result<String> {
        let mut file = File::open(file_path)?;
        let mut buf = [0_u8; 65_536];

        let mut context = Context::new(&SHA256);
        while let Ok(size) = file.read(&mut buf[..]) {
            if size == 0 {
                break;
            }
            context.update(&buf[0..size]);
        }

        self.checksum = Base64::encode_string(context.finish().as_ref());
        Ok(self.checksum.clone())
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
        let mut checksum = Checksum::new(ChecksumAlgorithm::Crc32);
        let result = checksum.calculate(&file.path().to_string_lossy()).unwrap();
        assert_eq!(result, "DUoRhQ==");
    }

    #[test]
    fn test_crc32c() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"hello world").unwrap();
        let mut checksum = Checksum::new(ChecksumAlgorithm::Crc32c);
        let result = checksum.calculate(&file.path().to_string_lossy()).unwrap();
        assert_eq!(result, "yZRlqg==");
    }

    #[test]
    fn test_sha1() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"hello world").unwrap();
        let mut checksum = Checksum::new(ChecksumAlgorithm::Sha1);
        let result = checksum.calculate(&file.path().to_string_lossy()).unwrap();
        assert_eq!(result, "Kq5sNclPz7QV2+lfQIuc6R7oRu0=");
    }

    #[test]
    fn test_sha256() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"hello world").unwrap();
        let mut checksum = Checksum::new(ChecksumAlgorithm::Sha256);
        let result = checksum.calculate(&file.path().to_string_lossy()).unwrap();
        assert_eq!(result, "uU0nuZNNPgilLlLX2n2r+sSE7+N6U4DukIj3rOLvzek=");
    }

    #[test]
    fn test_from_str() {
        assert_eq!(
            "crc32".parse::<ChecksumAlgorithm>(),
            Ok(ChecksumAlgorithm::Crc32)
        );

        assert_eq!(
            "crc32c".parse::<ChecksumAlgorithm>(),
            Ok(ChecksumAlgorithm::Crc32c)
        );

        assert_eq!(
            "sha1".parse::<ChecksumAlgorithm>(),
            Ok(ChecksumAlgorithm::Sha1)
        );

        assert_eq!(
            "sha256".parse::<ChecksumAlgorithm>(),
            Ok(ChecksumAlgorithm::Sha256)
        );

        assert_eq!("md5".parse::<ChecksumAlgorithm>(), Err(()));
    }
}
