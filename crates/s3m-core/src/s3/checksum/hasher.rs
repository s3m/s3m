use bytes::Bytes;
use crc32c::{crc32c, crc32c_append};
use crc32fast::Hasher;
use ring::digest::{Context, SHA1_FOR_LEGACY_USE_ONLY, SHA256};

/// A trait for checksum calculation.
pub trait ChecksumHasher: Send + Sync {
    fn new() -> Self
    where
        Self: Sized;

    fn update(&mut self, bytes: &[u8]);

    fn finalize(self: Box<Self>) -> Bytes;
}

// CRC32
pub struct Crc32Hasher(Hasher);

impl ChecksumHasher for Crc32Hasher {
    fn new() -> Self {
        Self::default()
    }

    fn update(&mut self, bytes: &[u8]) {
        self.0.update(bytes);
    }

    fn finalize(self: Box<Self>) -> Bytes {
        Bytes::from(self.0.finalize().to_be_bytes().to_vec())
    }
}

impl Default for Crc32Hasher {
    fn default() -> Self {
        Self(crc32fast::Hasher::new())
    }
}

// CRC32C
pub struct Crc32cHasher(u32);

impl ChecksumHasher for Crc32cHasher {
    fn new() -> Self {
        Self::default()
    }

    fn update(&mut self, bytes: &[u8]) {
        self.0 = crc32c_append(self.0, bytes);
    }

    fn finalize(self: Box<Self>) -> Bytes {
        Bytes::from(self.0.to_be_bytes().to_vec())
    }
}

impl Default for Crc32cHasher {
    fn default() -> Self {
        Self(crc32c(&[]))
    }
}

// Md5
pub struct Md5Hasher(md5::Context);

impl ChecksumHasher for Md5Hasher {
    fn new() -> Self {
        Self::default()
    }

    fn update(&mut self, bytes: &[u8]) {
        self.0.consume(bytes);
    }

    fn finalize(self: Box<Self>) -> Bytes {
        Bytes::from(self.0.finalize().0.to_vec())
    }
}

impl Default for Md5Hasher {
    fn default() -> Self {
        Self(md5::Context::new())
    }
}

// SHA1
pub struct Sha1Hasher(Context);

impl ChecksumHasher for Sha1Hasher {
    fn new() -> Self {
        Self::default()
    }

    fn update(&mut self, bytes: &[u8]) {
        self.0.update(bytes);
    }

    fn finalize(self: Box<Self>) -> Bytes {
        Bytes::from(self.0.finish().as_ref().to_vec())
    }
}

impl Default for Sha1Hasher {
    fn default() -> Self {
        Self(Context::new(&SHA1_FOR_LEGACY_USE_ONLY))
    }
}

// SHA256
pub struct Sha256Hasher(Context);

impl ChecksumHasher for Sha256Hasher {
    fn new() -> Self {
        Self::default()
    }

    fn update(&mut self, bytes: &[u8]) {
        self.0.update(bytes);
    }

    fn finalize(self: Box<Self>) -> Bytes {
        Bytes::from(self.0.finish().as_ref().to_vec())
    }
}

impl Default for Sha256Hasher {
    fn default() -> Self {
        Self(Context::new(&SHA256))
    }
}
