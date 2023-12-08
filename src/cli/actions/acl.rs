use crate::{
    cli::actions::Action,
    s3::{actions, S3},
};
use anyhow::Result;

/// # Errors
/// Will return an error if the action fails
pub async fn handle(s3: &S3, action: Action) -> Result<()> {
    if let Action::ACL { key, acl } = action {
        if let Some(acl) = acl {
            let action = actions::PutObjectAcl::new(&key, &acl);
            action.request(s3).await?;
        } else {
            let action = actions::GetObjectAcl::new(&key);
            let res = action.request(s3).await?.text().await?;
            println!("{res}");
        }
    }

    Ok(())
}
