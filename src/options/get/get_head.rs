use crate::s3::{actions, S3};
use anyhow::Result;
use colored::Colorize;

pub async fn get_head(s3: S3, key: String) -> Result<()> {
    let action = actions::HeadObject::new(&key);
    let headers = action.request(&s3).await?;
    let mut i = 0;
    for k in headers.keys() {
        i = k.len();
    }
    i += 1;
    for (k, v) in headers {
        println!("{:<width$} {}", format!("{}:", k).green(), v, width = i)
    }
    Ok(())
}
