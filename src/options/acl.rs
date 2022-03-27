use crate::{
    s3::responses::AccessControlPolicy,
    s3::{actions, S3},
};
use anyhow::Result;
use serde_xml_rs::from_str;

pub async fn acl(s3: &S3, key: &str, acl: Option<String>) -> Result<()> {
    if let Some(acl) = acl {
        let action = actions::PutObjectAcl::new(key, &acl);
        action.request(s3).await?;
    } else {
        let action = actions::GetObjectAcl::new(key);
        let res = action.request(s3).await?.text().await?;
        println!("{}", res);
        let acl: AccessControlPolicy = from_str(&res)?;
        println!("{:#?}", acl);
    }
    Ok(())
}
