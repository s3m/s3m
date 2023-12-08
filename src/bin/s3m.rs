use anyhow::Result;
use s3m::cli::{actions, actions::Action, start};

#[tokio::main]
async fn main() -> Result<()> {
    let (s3, action, globals) = start()?;

    match action {
        Action::ACL { .. } => {
            actions::acl::handle(&s3, action).await?;
        }

        Action::CreateBucket { .. } => {
            actions::bucket::handle(&s3, action).await?;
        }

        Action::ListObjects { .. } => {
            actions::object_list::handle(&s3, action).await?;
        }

        Action::DeleteObject { .. } => {
            actions::object_delete::handle(&s3, action).await?;
        }

        Action::GetObject { .. } => {
            actions::object_get::handle(&s3, action, globals).await?;
        }

        Action::ShareObject { .. } => {
            actions::object_share::handle(&s3, action)?;
        }

        Action::PutObject { .. } => {
            actions::object_put::handle(&s3, action, globals).await?;
        }
    }

    Ok(())
}
