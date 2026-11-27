use secrecy::{ExposeSecret, SecretString};
use std::env;

#[derive(Clone)]
pub struct Credentials {
    // AWS_ACCESS_KEY_ID
    key: String,
    // AWS_SECRET_ACCESS_KEY
    secret: SecretString,
}

/// What to show for an access key id in debug output: whether one is set, never the
/// id itself. Debug output ends up in bug reports and CI logs
/// (<https://github.com/s3m/s3m/issues/87>).
#[must_use]
pub const fn masked_access_key(key: &str) -> &'static str {
    if key.is_empty() {
        "<empty>"
    } else {
        "<redacted>"
    }
}

impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credentials")
            .field("key", &masked_access_key(&self.key))
            .field("secret", &self.secret)
            .finish()
    }
}

impl Credentials {
    /// Creates new credentials with the following priority:
    /// 1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
    /// 2. Passed parameters (`access`, `secret`)
    ///
    /// This allows environment variables to override configuration file values,
    /// which is useful for temporary credential overrides and CI/CD environments.
    #[must_use]
    pub fn new(access: &str, secret: &SecretString) -> Self {
        Self::with_overrides(
            access,
            secret,
            env::var("AWS_ACCESS_KEY_ID").ok(),
            env::var("AWS_SECRET_ACCESS_KEY").ok(),
        )
    }

    /// The credentials, with each value replaced by its override when there is one.
    /// Separate from [`Credentials::new`] so that tests don't have to change the
    /// environment of the process, which every other test reads.
    fn with_overrides(
        access: &str,
        secret: &SecretString,
        access_override: Option<String>,
        secret_override: Option<String>,
    ) -> Self {
        let access_key = access_override.unwrap_or_else(|| access.to_string());
        let secret_key = secret_override.unwrap_or_else(|| secret.expose_secret().to_string());
        Self {
            key: access_key,
            secret: SecretString::new(secret_key.into()),
        }
    }

    /// Get a reference to the access key ID.
    #[must_use]
    pub fn aws_access_key_id(&self) -> &str {
        &self.key
    }

    /// Get a reference to the secret access key.
    #[must_use]
    pub fn aws_secret_access_key(&self) -> &str {
        self.secret.expose_secret()
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

    // These tests don't set AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY: the environment
    // belongs to the whole test process, and the tests that sign requests with fixed
    // keys read it at the same time (one of them failed that way once). Reading the
    // real variables is covered by the CLI test
    // `environment_credentials_replace_the_config_keys`, which runs s3m as a child.
    #[test]
    fn test_credentials() {
        let creds =
            Credentials::with_overrides("access", &SecretString::new("secret".into()), None, None);
        assert_eq!(creds.aws_access_key_id(), "access");
        assert_eq!(creds.aws_secret_access_key(), "secret");
    }

    #[test]
    fn test_credentials_overrides() {
        let secret = SecretString::new("secret".into());
        let creds = Credentials::with_overrides(
            "access",
            &secret,
            Some("env-access".to_string()),
            Some("env-secret".to_string()),
        );
        assert_eq!(creds.aws_access_key_id(), "env-access");
        assert_eq!(creds.aws_secret_access_key(), "env-secret");

        // Each value on its own.
        let creds =
            Credentials::with_overrides("access", &secret, Some("env-access".to_string()), None);
        assert_eq!(creds.aws_access_key_id(), "env-access");
        assert_eq!(creds.aws_secret_access_key(), "secret");
    }

    // #87: the debug output of the credentials shows neither the access key id nor
    // the secret, only whether a key is set.
    #[test]
    fn test_debug_hides_the_keys() {
        let creds = Credentials::with_overrides(
            "AKIAIOSFODNN7EXAMPLE",
            &SecretString::new("wJalrXUtnFEMI".into()),
            None,
            None,
        );
        let shown = format!("{creds:#?}");
        assert!(!shown.contains("AKIAIOSFODNN7EXAMPLE"), "{shown}");
        assert!(!shown.contains("wJalrXUtnFEMI"), "{shown}");
        assert!(shown.contains("<redacted>"), "{shown}");

        let none = Credentials::with_overrides("", &SecretString::new("".into()), None, None);
        assert!(format!("{none:?}").contains("<empty>"));
    }
}
