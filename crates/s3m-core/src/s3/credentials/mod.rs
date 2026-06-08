use secrecy::{ExposeSecret, SecretString};
use std::env;

#[derive(Clone, Debug)]
pub struct Credentials {
    // AWS_ACCESS_KEY_ID
    key: String,
    // AWS_SECRET_ACCESS_KEY
    secret: SecretString,
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
        let access_key = env::var("AWS_ACCESS_KEY_ID").unwrap_or_else(|_| access.to_string());
        let secret_key = env::var("AWS_SECRET_ACCESS_KEY")
            .unwrap_or_else(|_| secret.expose_secret().to_string());
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

    #[test]
    fn test_credentials() {
        let creds = Credentials::new("access", &SecretString::new("secret".into()));
        assert_eq!(creds.aws_access_key_id(), "access");
        assert_eq!(creds.aws_secret_access_key(), "secret");
    }

    #[test]
    fn test_credentials_env() {
        temp_env::with_vars(
            [
                ("AWS_ACCESS_KEY_ID", Some("env-access")),
                ("AWS_SECRET_ACCESS_KEY", Some("env-secret")),
            ],
            || {
                let creds = Credentials::new("access", &SecretString::new("secret".into()));
                assert_eq!(creds.aws_access_key_id(), "env-access");
                assert_eq!(creds.aws_secret_access_key(), "env-secret");
            },
        );
    }
}
