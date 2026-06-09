//! Typed errors for the `s3` module.
//!
//! Public S3 actions return [`Result<T>`] (alias for `Result<T, Error>`) so
//! that consumers of the library can react programmatically to failures, for
//! example distinguishing a missing object (`NoSuchKey` / `404`) from an
//! authorization failure (`AccessDenied` / `403`) or a transport error.
//!
//! Internally the crate still uses `anyhow` freely; [`Error`] implements
//! `std::error::Error`, so any `s3::Error` converts into `anyhow::Error` via
//! `?` at the binary boundary, and `anyhow::Error` produced by internal
//! helpers converts into [`Error::Other`].

use std::fmt;

/// Structured representation of an error response returned by the S3 service.
#[derive(Debug, Clone)]
pub struct ApiError {
    /// HTTP status code of the response.
    pub status: u16,
    /// S3 error `Code` (e.g. `NoSuchKey`, `AccessDenied`), when present.
    pub code: Option<String>,
    /// S3 error `Message`, when present.
    pub message: Option<String>,
    /// Full human-readable detail (status code, request ids, code, message).
    pub details: String,
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.details)
    }
}

/// Errors returned by the public API of the `s3` module.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An error response returned by the S3 service (non-2xx status).
    #[error("{0}")]
    Api(ApiError),

    /// HTTP / transport-level failure.
    #[error(transparent)]
    Http(#[from] reqwest::Error),

    /// Local I/O failure (e.g. reading a file to upload, writing a download).
    #[error(transparent)]
    Io(#[from] std::io::Error),

    /// XML deserialization failure while parsing an S3 response.
    #[error("XML parse error: {0}")]
    Xml(#[from] quick_xml::DeError),

    /// XML serialization failure while building an S3 request body.
    #[error("XML serialize error: {0}")]
    XmlSerialize(#[from] quick_xml::SeError),

    /// A response header was not valid UTF-8 / could not be read as a string.
    #[error("invalid header value: {0}")]
    Header(#[from] http::header::ToStrError),

    /// An unsupported / malformed HTTP method.
    #[error("invalid HTTP method: {0}")]
    Method(#[from] http::method::InvalidMethod),

    /// An invalid endpoint / URL.
    #[error("invalid URL: {0}")]
    Url(#[from] url::ParseError),

    /// A request header name could not be constructed.
    #[error("invalid header name: {0}")]
    HeaderName(#[from] http::header::InvalidHeaderName),

    /// A request header value could not be constructed.
    #[error("invalid header value: {0}")]
    HeaderValue(#[from] http::header::InvalidHeaderValue),

    /// Any other error originating inside the `s3` module (signing,
    /// validation, decoding, and internal helpers).
    #[error("{0}")]
    Other(String),
}

impl Error {
    /// The S3 error `Code` (e.g. `"NoSuchKey"`) if this is an API error.
    #[must_use]
    pub fn code(&self) -> Option<&str> {
        match self {
            Self::Api(e) => e.code.as_deref(),
            _ => None,
        }
    }

    /// The HTTP status code if this is an API error.
    #[must_use]
    pub const fn status(&self) -> Option<u16> {
        match self {
            Self::Api(e) => Some(e.status),
            _ => None,
        }
    }

    /// `true` if this is a `404 Not Found` / `NoSuchKey`-style API error.
    #[must_use]
    pub fn is_not_found(&self) -> bool {
        match self {
            Self::Api(e) => e.status == 404 || e.code.as_deref() == Some("NoSuchKey"),
            _ => false,
        }
    }
}

impl From<anyhow::Error> for Error {
    fn from(e: anyhow::Error) -> Self {
        Self::Other(format!("{e:#}"))
    }
}

impl From<tokio::task::JoinError> for Error {
    fn from(e: tokio::task::JoinError) -> Self {
        Self::Other(format!("background task failed: {e}"))
    }
}

/// Convenience alias used throughout the `s3` module's public API.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests {
    use super::*;

    #[test]
    fn test_api_error_display_and_accessors() {
        let err = Error::Api(ApiError {
            status: 404,
            code: Some("NoSuchKey".to_string()),
            message: Some("The specified key does not exist.".to_string()),
            details: "HTTP Status Code: 404 Not Found\nCode: NoSuchKey\n".to_string(),
        });

        assert_eq!(err.status(), Some(404));
        assert_eq!(err.code(), Some("NoSuchKey"));
        assert!(err.is_not_found());
        assert!(err.to_string().contains("NoSuchKey"));
    }

    #[test]
    fn test_other_has_no_code_or_status() {
        let err = Error::Other("boom".to_string());
        assert_eq!(err.code(), None);
        assert_eq!(err.status(), None);
        assert!(!err.is_not_found());
        assert_eq!(err.to_string(), "boom");
    }

    #[test]
    fn test_from_anyhow() {
        let err: Error = anyhow::anyhow!("internal failure").into();
        assert!(matches!(err, Error::Other(_)));
        assert!(err.to_string().contains("internal failure"));
    }
}
