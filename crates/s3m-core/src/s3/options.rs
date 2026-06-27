use crate::s3::object_lock::ObjectLock;
use secrecy::SecretString;
use std::convert::TryFrom;

/// Per-transfer options shared across S3 upload/download actions.
///
/// These tune how a request behaves (bandwidth throttling, retry budget) and
/// whether the payload is transformed in flight (compression, encryption).
/// They are deliberately part of the `s3` module — not the CLI — so external
/// consumers of the library can drive uploads/downloads without depending on
/// any command-line types.
#[derive(Debug, Clone, Default)]
pub struct RequestOptions {
    pub throttle: Option<usize>,
    pub retries: u32,
    pub compress: bool,
    pub encrypt: bool,
    pub enc_key: Option<SecretString>,
    /// Object Lock (WORM) settings applied to uploads, when set.
    pub object_lock: Option<ObjectLock>,
}

impl RequestOptions {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            throttle: None,
            retries: 3,
            compress: false,
            encrypt: false,
            enc_key: None,
            object_lock: None,
        }
    }

    pub fn set_retries(&mut self, retries: usize) {
        self.retries = u32::try_from(retries).unwrap_or(3);
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

    #[test]
    fn test_request_options() {
        let mut options = RequestOptions::new();
        assert_eq!(options.throttle, None);
        assert_eq!(options.retries, 3);
        assert!(!options.compress);
        assert!(!options.encrypt);

        options.throttle = Some(10);
        assert_eq!(options.throttle, Some(10));

        options.set_retries(5);
        assert_eq!(options.retries, 5);

        options.compress = true;
        assert!(options.compress);

        options.encrypt = true;
        assert!(options.encrypt);
    }
}
