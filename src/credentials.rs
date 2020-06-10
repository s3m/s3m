use std::env;

#[derive(Clone, Debug)]
pub struct Credentials {
    // AWS_ACCESS_KEY_ID
    key: String,
    // AWS_SECRET_ACCESS_KEY
    secret: String,
}

impl Credentials {
    #[must_use]
    pub fn new(access: &str, secret: &str) -> Self {
        let access_key = env::var("AWS_ACCESS_KEY_ID").unwrap_or(access.to_string());
        let secret_key = env::var("AWS_SECRET_ACCESS_KEY").unwrap_or(secret.to_string());
        Self {
            key: access_key,
            secret: secret_key,
        }
    }

    /// Get a reference to the access key ID.
    pub fn aws_access_key_id(&self) -> &str {
        &self.key
    }

    /// Get a reference to the secret access key.
    pub fn aws_secret_access_key(&self) -> &str {
        &self.secret
    }
}
