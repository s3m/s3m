use crate::{
    cli::actions::Action,
    s3::{actions, S3},
};
use anyhow::Result;

/// # Errors
/// Will return an error if the action fails
pub async fn handle(s3: &S3, action: Action) -> Result<()> {
    if let Action::CreateBucket { acl } = action {
        let action = actions::CreateBucket::new(&acl);
        let rs = action.request(s3).await?;
        for (key, value) in rs {
            println!("{key}: {value}");
        }
    }

    Ok(())
}
