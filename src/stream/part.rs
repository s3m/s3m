use crate::s3::checksum::Checksum;
use bincode::{Decode, Encode};

#[derive(Debug, Default, Clone, Encode, Decode)]
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
}
