use anyhow::Result;
use mockito::{Matcher, Server};
use s3m_core::{
    Credentials, Region, RequestOptions, S3,
    stream::{
        db::Db,
        upload_multipart::{MultipartUploadRequest, upload_multipart},
    },
};
use secrecy::SecretString;

#[tokio::test]
async fn default_options_attempt_file_part_once_and_do_not_complete_on_failure() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let file = dir.path().join("payload");
    let data = b"part payload";
    tokio::fs::write(&file, data).await?;
    let size = tokio::fs::metadata(&file).await?.len();
    let mut server = Server::new_async().await;
    let s3 = S3::new(
        &Credentials::new("test-access", &SecretString::new("test-secret".into())),
        &Region::custom("us-east-1", server.url()),
        Some("bucket".into()),
        false,
    );
    let db = Db::new(&s3, "key", "test-checksum", 0, dir.path())?;
    db.save_upload_id("upload-id")?;
    let upload = server
        .mock("PUT", "/bucket/key")
        .match_query(Matcher::AllOf(vec![
            Matcher::UrlEncoded("partNumber".into(), "1".into()),
            Matcher::UrlEncoded("uploadId".into(), "upload-id".into()),
        ]))
        .match_body(data.to_vec())
        .with_status(403)
        .with_body("<Error><Code>AccessDenied</Code><Message>Part denied</Message></Error>")
        .expect(1)
        .create_async()
        .await;
    let complete = server
        .mock("POST", "/bucket/key")
        .match_query(Matcher::UrlEncoded("uploadId".into(), "upload-id".into()))
        .expect(0)
        .create_async()
        .await;

    let result = upload_multipart(MultipartUploadRequest {
        s3: &s3,
        key: "key",
        file: &file,
        file_size: size,
        chunk_size: size,
        sdb: &db,
        acl: None,
        meta: None,
        quiet: true,
        additional_checksum: None,
        max_requests: 1,
        globals: RequestOptions::default(),
    })
    .await;

    upload.assert_async().await;
    complete.assert_async().await;
    assert!(result.is_err(), "a failed part must fail the upload");
    assert_eq!(db.db_parts()?.len(), 1, "failed part remains resumable");
    assert!(db.db_uploaded()?.is_empty());
    Ok(())
}
