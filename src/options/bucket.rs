use crate::s3::{actions, S3};
use anyhow::Result;

pub async fn make_bucket(s3: &S3, acl: &str) -> Result<()> {
    let action = actions::CreateBucket::new(acl);
    let rs = action.request(s3).await?;
    for (key, value) in rs {
        println!("{key}: {value}");
    }
    Ok(())
}
