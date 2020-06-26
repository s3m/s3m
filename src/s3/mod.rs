pub mod credentials;
pub mod region;
pub mod signature;

pub use self::{credentials::Credentials, region::Region, signature::Signature};

#[derive(Debug)]
pub struct S3 {
    // AWS Credentials
    pub credentials: Credentials,
    // AWS Region
    pub region: Region,
}

impl S3 {
    #[must_use]
    pub fn new(credentials: &Credentials, region: &Region) -> Self {
        Self {
            credentials: credentials.clone(),
            region: region.clone(),
        }
    }
}
