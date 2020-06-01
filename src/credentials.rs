use std::env;

#[derive(Debug)]
pub struct Credentials {
    // AWS_ACCESS_KEY_ID
    access_key: String,
    // AWS_SECRET_ACCESS_KEY
    secret_key: String,
}

impl Credentials {
    #[must_use]
    pub fn new(access: String, secret: String) -> Self {
        let access_key = env::var("AWS_ACCESS_KEY_ID").unwrap_or(access);
        let secret_key = env::var("AWS_SECRET_ACCESS_KEY").unwrap_or(secret);
        Self {
            access_key: access_key,
            secret_key: secret_key,
        }
    }
}
