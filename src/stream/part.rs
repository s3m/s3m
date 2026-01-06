use crate::s3::checksum::Checksum;
use rkyv::{Archive, Deserialize, Serialize};

#[derive(Debug, Default, Clone, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct Part {
    etag: String,
    number: u16,
    checksum: Option<Checksum>,
    seek: u64,
    chunk: u64,
}

impl Part {
    #[must_use]
    pub const fn new(number: u16, seek: u64, chunk: u64, checksum: Option<Checksum>) -> Self {
        Self {
            number,
            seek,
            chunk,
            etag: String::new(),
            checksum,
        }
    }

    #[must_use]
    pub fn set_etag(mut self, etag: String) -> Self {
        self.etag = etag;
        self
    }

    #[must_use]
    pub fn set_checksum(mut self, checksum: Option<Checksum>) -> Self {
        self.checksum = checksum;
        self
    }

    #[must_use]
    pub fn get_etag(&self) -> &str {
        &self.etag
    }

    #[must_use]
    pub const fn get_number(&self) -> u16 {
        self.number
    }

    #[must_use]
    pub fn get_checksum(&self) -> Option<Checksum> {
        self.checksum.clone()
    }

    #[must_use]
    pub const fn get_seek(&self) -> u64 {
        self.seek
    }

    #[must_use]
    pub const fn get_chunk(&self) -> u64 {
        self.chunk
    }
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
    use crate::s3::checksum::{Checksum, ChecksumAlgorithm};
    use rkyv::{from_bytes, rancor::Error as RkyvError, to_bytes};

    #[test]
    fn test_part() {
        let part = Part::new(1, 0, 0, None);
        assert_eq!(part.get_number(), 1);
        assert_eq!(part.get_seek(), 0);
        assert_eq!(part.get_chunk(), 0);
        assert_eq!(part.get_etag(), "");
        assert!(part.get_checksum().is_none());
    }

    #[test]
    fn test_part_set_etag() {
        let part = Part::new(1, 0, 0, None).set_etag("etag".to_string());
        assert_eq!(part.get_etag(), "etag");
    }

    #[test]
    fn test_part_set_checksum() {
        let checksum = Checksum::new(ChecksumAlgorithm::Crc32c);
        let part = Part::new(1, 0, 0, None).set_checksum(Some(checksum.clone()));
        assert_eq!(
            part.get_checksum().unwrap().algorithm,
            ChecksumAlgorithm::Crc32c
        );
        assert_eq!(part.get_checksum().unwrap().checksum, "");
    }

    #[test]
    fn test_part_rkyv_serialize_deserialize_basic() {
        let part = Part::new(1, 1024, 4096, None);
        let bytes = to_bytes::<RkyvError>(&part).unwrap();
        let deserialized: Part = from_bytes::<Part, RkyvError>(&bytes).unwrap();

        assert_eq!(deserialized.get_number(), 1);
        assert_eq!(deserialized.get_seek(), 1024);
        assert_eq!(deserialized.get_chunk(), 4096);
        assert_eq!(deserialized.get_etag(), "");
        assert!(deserialized.get_checksum().is_none());
    }

    #[test]
    fn test_part_rkyv_serialize_deserialize_with_etag() {
        let part = Part::new(42, 0, 5_242_880, None)
            .set_etag("\"d41d8cd98f00b204e9800998ecf8427e\"".to_string());
        let bytes = to_bytes::<RkyvError>(&part).unwrap();
        let deserialized: Part = from_bytes::<Part, RkyvError>(&bytes).unwrap();

        assert_eq!(deserialized.get_number(), 42);
        assert_eq!(
            deserialized.get_etag(),
            "\"d41d8cd98f00b204e9800998ecf8427e\""
        );
    }

    #[test]
    fn test_part_rkyv_serialize_deserialize_with_checksum() {
        let mut checksum = Checksum::new(ChecksumAlgorithm::Sha256);
        checksum.checksum = "uU0nuZNNPgilLlLX2n2r+sSE7+N6U4DukIj3rOLvzek=".to_string();
        let part = Part::new(100, 52_428_800, 5_242_880, Some(checksum));
        let bytes = to_bytes::<RkyvError>(&part).unwrap();
        let deserialized: Part = from_bytes::<Part, RkyvError>(&bytes).unwrap();

        assert_eq!(deserialized.get_number(), 100);
        assert_eq!(deserialized.get_seek(), 52_428_800);
        assert_eq!(deserialized.get_chunk(), 5_242_880);
        let checksum = deserialized.get_checksum().unwrap();
        assert_eq!(checksum.algorithm, ChecksumAlgorithm::Sha256);
        assert_eq!(
            checksum.checksum,
            "uU0nuZNNPgilLlLX2n2r+sSE7+N6U4DukIj3rOLvzek="
        );
    }

    #[test]
    fn test_part_rkyv_serialize_deserialize_full() {
        let mut checksum = Checksum::new(ChecksumAlgorithm::Crc32c);
        checksum.checksum = "yZRlqg==".to_string();
        let part = Part::new(9999, u64::MAX, u64::MAX - 1, Some(checksum))
            .set_etag("\"abc123\"".to_string());
        let bytes = to_bytes::<RkyvError>(&part).unwrap();
        let deserialized: Part = from_bytes::<Part, RkyvError>(&bytes).unwrap();

        assert_eq!(deserialized.get_number(), 9999);
        assert_eq!(deserialized.get_seek(), u64::MAX);
        assert_eq!(deserialized.get_chunk(), u64::MAX - 1);
        assert_eq!(deserialized.get_etag(), "\"abc123\"");
        let checksum = deserialized.get_checksum().unwrap();
        assert_eq!(checksum.algorithm, ChecksumAlgorithm::Crc32c);
        assert_eq!(checksum.checksum, "yZRlqg==");
    }

    #[test]
    fn test_part_rkyv_roundtrip_all_checksum_algorithms() {
        for algorithm in [
            ChecksumAlgorithm::Crc32,
            ChecksumAlgorithm::Crc32c,
            ChecksumAlgorithm::Sha1,
            ChecksumAlgorithm::Sha256,
            ChecksumAlgorithm::Md5,
        ] {
            let mut checksum = Checksum::new(algorithm.clone());
            checksum.checksum = "test_checksum_value".to_string();
            let part = Part::new(1, 0, 1024, Some(checksum));
            let bytes = to_bytes::<RkyvError>(&part).unwrap();
            let deserialized: Part = from_bytes::<Part, RkyvError>(&bytes).unwrap();

            let recovered_checksum = deserialized.get_checksum().unwrap();
            assert_eq!(recovered_checksum.algorithm, algorithm);
            assert_eq!(recovered_checksum.checksum, "test_checksum_value");
        }
    }
}
