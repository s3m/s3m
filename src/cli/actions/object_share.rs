use crate::{
    cli::actions::Action,
    s3::{Signature, S3},
};
use anyhow::Result;
use reqwest::Method;

pub fn handle(s3: &S3, action: Action) -> Result<()> {
    if let Action::ShareObject { key, expire } = action {
        let url =
            Signature::new(s3, "s3", Method::from_bytes(b"GET")?)?.presigned_url(&key, expire)?;
        println!("{url}");
    }

    Ok(())
}
