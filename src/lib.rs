pub mod cli;

// Re-export the core library so existing `s3m::s3::…` / `s3m::stream::…`
// paths keep resolving for the binary, integration tests, and benches, and so
// the CLI's internal `crate::s3` / `crate::stream` references continue to work.
pub use s3m_core::{progressbar, s3, stream};

// Curated top-level re-exports mirroring `s3m_core`.
pub use s3m_core::{
    ApiError, Credentials, Error, ObjectLock, ObjectLockMode, Region, RequestOptions, Result, S3,
};
