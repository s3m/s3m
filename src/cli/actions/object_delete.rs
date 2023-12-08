use crate::{
    cli::actions::Action,
    s3::{actions, S3},
};
use anyhow::Result;

/// # Errors
/// Will return an error if the action fails
pub async fn handle(s3: &S3, action: Action) -> Result<()> {
    if let Action::DeleteObject {
        key,
        upload_id,
        bucket,
    } = action
    {
        if bucket {
            // delete bucket
            let action = actions::DeleteBucket::new();
            action.request(s3).await?;
        } else if upload_id.is_empty() {
            // delete object
            let action = actions::DeleteObject::new(&key);
            action.request(s3).await?;
        } else {
            let action = actions::AbortMultipartUpload::new(&key, &upload_id);
            action.request(s3).await?;
        }
    }

    Ok(())
}
