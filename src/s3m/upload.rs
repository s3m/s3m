use crate::s3::actions;
use crate::s3::S3;
use std::error;

pub async fn upload(s3: S3, key: String, file: String) -> Result<String, Box<dyn error::Error>> {
    let action = actions::PutObject::new(key, file);
    Ok(action.request(s3).await?)
}

pub async fn multipart_upload(
    s3: S3,
    key: String,
    file: String,
) -> Result<(), Box<dyn error::Error>> {
    let action = actions::CreateMultipartUpload::new(key);
    let response = action.request(s3).await?;
    println!("upload_id: {}, file: {}", response.upload_id, file);
    Ok(())
}
