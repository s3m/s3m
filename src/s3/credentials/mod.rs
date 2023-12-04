use std::env;

#[derive(Clone, Debug)]
pub struct Credentials {
    // AWS_ACCESS_KEY_ID
    key: String,
    // AWS_SECRET_ACCESS_KEY
    secret: String,
}

impl Credentials {
    // TODO
    // give priority to passed keys and then env
    #[must_use]
    pub fn new(access: &str, secret: &str) -> Self {
        let access_key = env::var("AWS_ACCESS_KEY_ID").unwrap_or_else(|_| access.to_string());
        let secret_key = env::var("AWS_SECRET_ACCESS_KEY").unwrap_or_else(|_| secret.to_string());
        Self {
            key: access_key,
            secret: secret_key,
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
        &self.secret
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_credentials() {
        let creds = Credentials::new("access", "secret");
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
                let creds = Credentials::new("access", "secret");
                assert_eq!(creds.aws_access_key_id(), "env-access");
                assert_eq!(creds.aws_secret_access_key(), "env-secret");
            },
        );
    }
}
