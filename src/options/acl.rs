use crate::s3::{actions, S3};
use anyhow::Result;

pub async fn acl(s3: &S3, key: &str, acl: Option<String>) -> Result<()> {
    if let Some(acl) = acl {
        let action = actions::PutObjectAcl::new(key, &acl);
        action.request(s3).await?;
    } else {
        let action = actions::GetObjectAcl::new(key);
        let res = action.request(s3).await?.text().await?;
        // TODO pretty print
        // let acl: AccessControlPolicy = from_str(&res)?;
        println!("{}", res);
    }
    Ok(())
}
