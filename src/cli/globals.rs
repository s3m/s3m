//! CLI-facing alias for [`crate::s3::options::RequestOptions`].
//!
//! The type lives in the `s3` module so the library can be consumed without
//! pulling in CLI concerns; the binary keeps referring to it as `GlobalArgs`.
pub use crate::s3::options::RequestOptions as GlobalArgs;
