use crate::s3::checksum::Checksum;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Default, Clone)]
pub struct Part {
    etag: String,
    number: u16,
    checksum: Option<Checksum>,
    seek: u64,
    chunk: u64,
}

impl Part {
    #[must_use]
    pub fn new(number: u16, seek: u64, chunk: u64, checksum: Option<Checksum>) -> Self {
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
