use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Default, Clone)]
pub struct Part {
    etag: String,
    number: u16,
    seek: u64,
    chunk: u64,
}

impl Part {
    #[must_use]
    pub const fn new(number: u16, seek: u64, chunk: u64) -> Self {
        Self {
            number,
            seek,
            chunk,
            etag: String::new(),
        }
    }

    #[must_use]
    pub const fn set_etag(&self, etag: String) -> Self {
        Self { etag, ..*self }
    }

    #[must_use]
    pub fn get_etag(&self) -> String {
        self.etag.clone()
    }

    #[must_use]
    pub const fn get_number(&self) -> u16 {
        self.number
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
