//! `s3m-core` — the reusable S3 client and streaming engine behind the `s3m`
//! CLI.
//!
//! This crate is intentionally free of any command-line concerns so it can be
//! embedded by other applications. The two top-level modules are:
//!
//! - [`s3`]: the S3 client ([`S3`]), credentials, region, signing, and the
//!   typed request [`Error`](s3::Error) returned by every action.
//! - [`stream`]: resumable multipart uploads plus the compression/encryption
//!   transfer pipelines.

pub mod progressbar;
pub mod s3;
pub mod stream;

// Curated re-exports: the core building blocks a consumer needs without
// reaching into deep module paths.
pub use crate::s3::error::Result;
pub use crate::s3::{ApiError, Credentials, Error, Region, RequestOptions, S3};
