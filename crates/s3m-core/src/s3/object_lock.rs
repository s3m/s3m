//! S3 Object Lock (WORM) per-object settings.
//!
//! Object Lock lets you store objects using a write-once-read-many (WORM)
//! model: an object version can be protected from deletion/overwrite either
//! until a fixed date (retention) or until a legal hold is removed.
//!
//! This type carries the per-upload settings and knows how to turn them into
//! the `x-amz-object-lock-*` request headers consumed by
//! [`PutObject`](crate::s3::actions::PutObject) and
//! [`CreateMultipartUpload`](crate::s3::actions::CreateMultipartUpload).
//!
//! Object Lock requires the target bucket to have been created with Object Lock
//! enabled (see [`CreateBucket`](crate::s3::actions::CreateBucket)); otherwise
//! S3 rejects the upload.
//!
//! <https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html>

use crate::s3::error::Error;
use std::{collections::BTreeMap, str::FromStr};

/// Retention mode applied to an Object Lock protected object version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectLockMode {
    /// Privileged users with `s3:BypassGovernanceRetention` can still delete.
    Governance,
    /// No one can overwrite or delete the version until retention expires.
    Compliance,
}

impl ObjectLockMode {
    /// The value S3 expects in the `x-amz-object-lock-mode` header.
    #[must_use]
    pub const fn as_amz(self) -> &'static str {
        match self {
            Self::Governance => "GOVERNANCE",
            Self::Compliance => "COMPLIANCE",
        }
    }
}

impl FromStr for ObjectLockMode {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_uppercase().as_str() {
            "GOVERNANCE" => Ok(Self::Governance),
            "COMPLIANCE" => Ok(Self::Compliance),
            other => Err(Error::Other(format!(
                "invalid object lock mode {other:?}, expected GOVERNANCE or COMPLIANCE"
            ))),
        }
    }
}

/// Per-upload Object Lock settings.
///
/// `retention` carries the mode together with the retain-until date (an
/// already-validated RFC 3339 / ISO 8601 UTC string); the two are inseparable
/// at the S3 API. `legal_hold` is independent: `Some(true)` turns it on,
/// `Some(false)` off, `None` leaves it unset.
#[derive(Debug, Clone, Default)]
pub struct ObjectLock {
    pub retention: Option<(ObjectLockMode, String)>,
    pub legal_hold: Option<bool>,
}

impl ObjectLock {
    /// `true` when nothing is set, i.e. no Object Lock headers would be emitted.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.retention.is_none() && self.legal_hold.is_none()
    }

    /// Insert the `x-amz-object-lock-*` headers implied by this config into `map`.
    pub fn apply<'a>(&'a self, map: &mut BTreeMap<&'a str, &'a str>) {
        if let Some((mode, retain_until)) = &self.retention {
            map.insert("x-amz-object-lock-mode", mode.as_amz());
            map.insert("x-amz-object-lock-retain-until-date", retain_until.as_str());
        }

        if let Some(hold) = self.legal_hold {
            map.insert(
                "x-amz-object-lock-legal-hold",
                if hold { "ON" } else { "OFF" },
            );
        }
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
    fn test_as_amz() {
        assert_eq!(ObjectLockMode::Governance.as_amz(), "GOVERNANCE");
        assert_eq!(ObjectLockMode::Compliance.as_amz(), "COMPLIANCE");
    }

    #[test]
    fn test_from_str() {
        assert_eq!(
            "GOVERNANCE".parse::<ObjectLockMode>().unwrap(),
            ObjectLockMode::Governance
        );
        assert_eq!(
            "compliance".parse::<ObjectLockMode>().unwrap(),
            ObjectLockMode::Compliance
        );
        assert!("bogus".parse::<ObjectLockMode>().is_err());
    }

    #[test]
    fn test_is_empty() {
        assert!(ObjectLock::default().is_empty());
        assert!(
            !ObjectLock {
                legal_hold: Some(false),
                ..Default::default()
            }
            .is_empty()
        );
    }

    #[test]
    fn test_apply_retention() {
        let ol = ObjectLock {
            retention: Some((
                ObjectLockMode::Compliance,
                "2027-01-01T00:00:00Z".to_string(),
            )),
            legal_hold: None,
        };
        let mut map = BTreeMap::new();
        ol.apply(&mut map);
        assert_eq!(map.get("x-amz-object-lock-mode"), Some(&"COMPLIANCE"));
        assert_eq!(
            map.get("x-amz-object-lock-retain-until-date"),
            Some(&"2027-01-01T00:00:00Z")
        );
        assert_eq!(map.get("x-amz-object-lock-legal-hold"), None);
    }

    #[test]
    fn test_apply_legal_hold() {
        let on = ObjectLock {
            retention: None,
            legal_hold: Some(true),
        };
        let mut map = BTreeMap::new();
        on.apply(&mut map);
        assert_eq!(map.get("x-amz-object-lock-legal-hold"), Some(&"ON"));

        let off = ObjectLock {
            retention: None,
            legal_hold: Some(false),
        };
        let mut map = BTreeMap::new();
        off.apply(&mut map);
        assert_eq!(map.get("x-amz-object-lock-legal-hold"), Some(&"OFF"));
    }

    #[test]
    fn test_apply_empty() {
        let empty = ObjectLock::default();
        let mut map = BTreeMap::new();
        empty.apply(&mut map);
        assert!(map.is_empty());
    }
}
