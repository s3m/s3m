use anyhow::Result;
use base64ct::{Base64, Encoding};
use bincode::{Decode, Encode};
use bytes::Bytes;
use futures::stream::TryStreamExt;
use serde::{Deserialize, Serialize};
use std::{io::SeekFrom, path::Path, str::FromStr};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt},
};
use tokio_util::codec::{BytesCodec, FramedRead};

pub mod hasher;
use self::hasher::{
    ChecksumHasher, Crc32Hasher, Crc32cHasher, Md5Hasher, Sha1Hasher, Sha256Hasher,
};

pub mod digest;

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, Encode, Decode)]
pub enum ChecksumAlgorithm {
    Crc32,
    Crc32c,
    Md5,
    Sha1,
    Sha256,
}

impl ChecksumAlgorithm {
    #[must_use]
    pub const fn as_amz(&self) -> &'static str {
        match self {
            Self::Crc32 => "x-amz-checksum-crc32",
            Self::Crc32c => "x-amz-checksum-crc32c",
            Self::Sha1 => "x-amz-checksum-sha1",
            Self::Sha256 => "x-amz-checksum-sha256",
            Self::Md5 => unimplemented!(),
        }
    }

    #[must_use]
    pub const fn as_algorithm(&self) -> &'static str {
        match self {
            Self::Crc32 => "CRC32",
            Self::Crc32c => "CRC32C",
            Self::Sha1 => "SHA1",
            Self::Sha256 => "SHA256",
            Self::Md5 => unimplemented!(),
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

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct Checksum {
    pub algorithm: ChecksumAlgorithm,
    pub checksum: String, // Base64 encoded
}

impl Checksum {
    #[must_use]
    pub const fn new(algorithm: ChecksumAlgorithm) -> Self {
        Self {
            algorithm,
            checksum: String::new(),
        }
    }

    #[must_use]
    pub fn hasher(&self) -> Box<dyn ChecksumHasher> {
        let hasher: Box<dyn ChecksumHasher> = match self.algorithm {
            ChecksumAlgorithm::Crc32 => Box::new(Crc32Hasher::new()),
            ChecksumAlgorithm::Crc32c => Box::new(Crc32cHasher::new()),
            ChecksumAlgorithm::Md5 => Box::new(Md5Hasher::new()),
            ChecksumAlgorithm::Sha1 => Box::new(Sha1Hasher::new()),
            ChecksumAlgorithm::Sha256 => Box::new(Sha256Hasher::new()),
        };
        hasher
    }

    /// # Errors
    /// Will return an error if the file cannot be opened
    pub async fn calculate(&mut self, file: &Path) -> Result<String> {
        let mut hasher: Box<dyn ChecksumHasher> = match self.algorithm {
            ChecksumAlgorithm::Crc32 => Box::new(Crc32Hasher::new()),
            ChecksumAlgorithm::Crc32c => Box::new(Crc32cHasher::new()),
            ChecksumAlgorithm::Md5 => Box::new(Md5Hasher::new()),
            ChecksumAlgorithm::Sha1 => Box::new(Sha1Hasher::new()),
            ChecksumAlgorithm::Sha256 => Box::new(Sha256Hasher::new()),
        };

        // Buffer size is 256KB
        let mut stream =
            FramedRead::with_capacity(File::open(file).await?, BytesCodec::new(), 1024 * 256);

        while let Some(bytes) = stream.try_next().await? {
            hasher.update(&bytes);
        }

        self.checksum = Base64::encode_string(&hasher.finalize());
        Ok(self.checksum.clone())
    }

    /// # Errors
    /// Will return an error if the file cannot be opened
    pub fn digest(&mut self, data: &[u8]) -> Result<String> {
        let mut hasher: Box<dyn ChecksumHasher> = match self.algorithm {
            ChecksumAlgorithm::Crc32 => Box::new(Crc32Hasher::new()),
            ChecksumAlgorithm::Crc32c => Box::new(Crc32cHasher::new()),
            ChecksumAlgorithm::Md5 => Box::new(Md5Hasher::new()),
            ChecksumAlgorithm::Sha1 => Box::new(Sha1Hasher::new()),
            ChecksumAlgorithm::Sha256 => Box::new(Sha256Hasher::new()),
        };

        hasher.update(data);

        self.checksum = Base64::encode_string(&hasher.finalize());
        Ok(self.checksum.clone())
    }
}

/// # Errors
/// Will return an error if the file cannot be opened
pub async fn sha256_md5_digest(file_path: &Path) -> Result<(Bytes, Bytes, usize)> {
    let mut md5hasher = Checksum::new(ChecksumAlgorithm::Md5).hasher();
    let mut sha256hasher = Checksum::new(ChecksumAlgorithm::Sha256).hasher();

    // Buffer size is 256KB
    let mut stream =
        FramedRead::with_capacity(File::open(file_path).await?, BytesCodec::new(), 1024 * 256);

    let mut length: usize = 0;

    while let Some(bytes) = stream.try_next().await? {
        md5hasher.update(&bytes);
        sha256hasher.update(&bytes);
        length += &bytes.len();
    }

    Ok((sha256hasher.finalize(), md5hasher.finalize(), length))
}

/// # Errors
/// Will return an error if the file cannot be opened
pub async fn sha256_md5_digest_multipart(
    file_path: &Path,
    seek: u64,
    chunk: u64,
    mut algorithm: Option<&mut Checksum>,
) -> Result<(Bytes, Bytes, usize, Option<Checksum>)> {
    let mut md5hasher = Checksum::new(ChecksumAlgorithm::Md5).hasher();
    let mut sha256hasher = Checksum::new(ChecksumAlgorithm::Sha256).hasher();
    let mut hasher = algorithm.as_mut().map(|checksum| checksum.hasher());

    let mut file = File::open(file_path).await?;

    // Seek to the start position
    file.seek(SeekFrom::Start(seek)).await?;

    // Take the chunk
    let file = file.take(chunk);

    // Buffer size is 256KB,
    let mut stream = FramedRead::with_capacity(file, BytesCodec::new(), 1024 * 256);

    let mut length: usize = 0;

    while let Some(bytes) = stream.try_next().await? {
        md5hasher.update(&bytes);
        sha256hasher.update(&bytes);
        if let Some(ref mut hasher) = hasher {
            hasher.update(&bytes);
        }
        length += &bytes.len();
    }

    let digest = hasher.map(|hasher| Base64::encode_string(&hasher.finalize()));

    if let Some(checksum) = algorithm {
        // Modify the contents of the Checksum struct with the output of the hash
        if let Some(d) = digest.as_ref() {
            checksum.checksum.clone_from(d);
        }

        return Ok((
            sha256hasher.finalize(),
            md5hasher.finalize(),
            length,
            Some(checksum.clone()),
        ));
    }

    Ok((sha256hasher.finalize(), md5hasher.finalize(), length, None))
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64ct::{Base64, Encoding};
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_crc32() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"hello world").unwrap();
        let mut checksum = Checksum::new(ChecksumAlgorithm::Crc32);
        let result = checksum.calculate(file.path()).await.unwrap();
        assert_eq!(result, "DUoRhQ==");
    }

    #[tokio::test]
    async fn test_crc32c() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"hello world").unwrap();
        let mut checksum = Checksum::new(ChecksumAlgorithm::Crc32c);
        let result = checksum.calculate(file.path()).await.unwrap();
        assert_eq!(result, "yZRlqg==");
    }

    #[tokio::test]
    async fn test_md5() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"hello world").unwrap();
        let mut checksum = Checksum::new(ChecksumAlgorithm::Md5);
        let result = checksum.calculate(file.path()).await.unwrap();
        assert_eq!(result, "XrY7u+Ae7tCTyyK7j1rNww==");
    }

    #[tokio::test]
    async fn test_sha1() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"hello world").unwrap();
        let mut checksum = Checksum::new(ChecksumAlgorithm::Sha1);
        let result = checksum.calculate(file.path()).await.unwrap();
        assert_eq!(result, "Kq5sNclPz7QV2+lfQIuc6R7oRu0=");
    }

    #[tokio::test]
    async fn test_sha256() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"hello world").unwrap();
        let mut checksum = Checksum::new(ChecksumAlgorithm::Sha256);
        let result = checksum.calculate(file.path()).await.unwrap();
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

    #[test]
    fn test_as_amz() {
        assert_eq!(ChecksumAlgorithm::Crc32.as_amz(), "x-amz-checksum-crc32");
        assert_eq!(ChecksumAlgorithm::Crc32c.as_amz(), "x-amz-checksum-crc32c");
        assert_eq!(ChecksumAlgorithm::Sha1.as_amz(), "x-amz-checksum-sha1");
        assert_eq!(ChecksumAlgorithm::Sha256.as_amz(), "x-amz-checksum-sha256");
    }

    #[test]
    fn test_hasher() {
        let mut hasher = Checksum::new(ChecksumAlgorithm::Crc32).hasher();
        hasher.update(b"hello world");
        let result = Box::new(hasher).finalize();
        assert_eq!(Base64::encode_string(&result), "DUoRhQ==");

        let mut hasher = Checksum::new(ChecksumAlgorithm::Crc32c).hasher();
        hasher.update(b"hello world");
        let result = Box::new(hasher).finalize();
        assert_eq!(Base64::encode_string(&result), "yZRlqg==");

        let mut hasher = Checksum::new(ChecksumAlgorithm::Md5).hasher();
        hasher.update(b"hello world");
        let result = Box::new(hasher).finalize();
        assert_eq!(
            format!("{:x}", result),
            Bytes::from("5eb63bbbe01eeed093cb22bb8f5acdc3")
        );

        let mut hasher = Checksum::new(ChecksumAlgorithm::Sha1).hasher();
        hasher.update(b"hello world");
        let result = Box::new(hasher).finalize();
        assert_eq!(
            format!("{:x}", result),
            Bytes::from("2aae6c35c94fcfb415dbe95f408b9ce91ee846ed")
        );

        let mut hasher = Checksum::new(ChecksumAlgorithm::Sha256).hasher();
        hasher.update(b"hello world");
        let result = Box::new(hasher).finalize();
        assert_eq!(
            format!("{:x}", result),
            Bytes::from("b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9")
        );
    }

    #[tokio::test]
    async fn test_sha256_md5_digest() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"hello world").unwrap();
        let (sha256, md5, length) = sha256_md5_digest(file.path()).await.unwrap();
        assert_eq!(
            Base64::encode_string(&sha256),
            "uU0nuZNNPgilLlLX2n2r+sSE7+N6U4DukIj3rOLvzek="
        );
        assert_eq!(Base64::encode_string(&md5), "XrY7u+Ae7tCTyyK7j1rNww==");
        assert_eq!(length, 11);
    }

    #[tokio::test]
    async fn test_sha256_md5_digest_multipart() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"hello world").unwrap();
        let (sha256, md5, length, checksum) = sha256_md5_digest_multipart(file.path(), 0, 11, None)
            .await
            .unwrap();
        assert_eq!(
            Base64::encode_string(&sha256),
            "uU0nuZNNPgilLlLX2n2r+sSE7+N6U4DukIj3rOLvzek="
        );
        assert_eq!(Base64::encode_string(&md5), "XrY7u+Ae7tCTyyK7j1rNww==");
        assert_eq!(length, 11);
        assert!(checksum.is_none());
    }

    #[tokio::test]
    async fn test_sha256_md5_digest_multipart_wtih_checksum() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"hello world").unwrap();
        let mut checksum = Some(Checksum::new(ChecksumAlgorithm::Sha256));
        let (sha256, md5, length, checksum) =
            sha256_md5_digest_multipart(file.path(), 0, 11, checksum.as_mut())
                .await
                .unwrap();
        assert_eq!(
            Base64::encode_string(&sha256),
            "uU0nuZNNPgilLlLX2n2r+sSE7+N6U4DukIj3rOLvzek="
        );
        assert_eq!(Base64::encode_string(&md5), "XrY7u+Ae7tCTyyK7j1rNww==");
        assert_eq!(length, 11);
        assert_eq!(checksum.clone().unwrap().algorithm.as_algorithm(), "SHA256");
        assert_eq!(
            checksum.unwrap().checksum,
            "uU0nuZNNPgilLlLX2n2r+sSE7+N6U4DukIj3rOLvzek="
        );
    }
}
