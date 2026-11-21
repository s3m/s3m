//! Final streaming parts honor the same total-attempt budget as buffered parts.

use anyhow::{Context, Result};
use mockito::{Matcher, Server};
use s3m_core::{
    s3::{Credentials, Region, RequestOptions, S3},
    stream::{FileStreamUpload, upload_compressed::stream_compressed},
};
use secrecy::SecretString;
use std::{
    io::Write,
    process::{Command, Stdio},
};
use tempfile::tempdir;

#[tokio::test]
async fn default_options_upload_final_payload_before_completion() -> Result<()> {
    for options in [
        RequestOptions::default(),
        RequestOptions {
            retries: 0,
            ..RequestOptions::new()
        },
    ] {
        for succeeds in [true, false] {
            let mut server = Server::new_async().await;
            let home = tempdir()?;
            let input = home.path().join("payload");
            std::fs::write(&input, b"default options must upload these bytes")?;
            let s3 = S3::new(
                &Credentials::new("dummy", &SecretString::new("dummy-secret".into())),
                &Region::custom("us-east-1", server.url()),
                Some("bucket".into()),
                false,
            );
            let initiate = server
                .mock("POST", "/bucket/key")
                .match_query(Matcher::UrlEncoded("uploads".into(), String::new()))
                .with_body("<InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>key</Key><UploadId>default-upload</UploadId></InitiateMultipartUploadResult>")
                .expect(1)
                .create_async()
                .await;
            let upload = server
                .mock("PUT", "/bucket/key")
                .match_query(Matcher::AllOf(vec![
                    Matcher::UrlEncoded("partNumber".into(), "1".into()),
                    Matcher::UrlEncoded("uploadId".into(), "default-upload".into()),
                ]))
                .with_status(if succeeds { 200 } else { 503 })
                .with_body("<Error><Code>ServiceUnavailable</Code></Error>")
                .with_header("etag", "stored-part")
                .expect(1)
                .create_async()
                .await;
            let complete = server
                .mock("POST", "/bucket/key")
                .match_query(Matcher::UrlEncoded("uploadId".into(), "default-upload".into()))
                .match_body(if succeeds {
                    Matcher::Regex("<ETag>stored-part</ETag>".into())
                } else {
                    Matcher::Any
                })
                .with_body("<CompleteMultipartUploadResult><Location>local</Location><Bucket>bucket</Bucket><Key>key</Key><ETag>completed</ETag></CompleteMultipartUploadResult>")
                .expect(usize::from(succeeds))
                .create_async()
                .await;
            let result = stream_compressed(FileStreamUpload {
                s3: &s3,
                object_key: "key",
                acl: None,
                meta: None,
                quiet: true,
                tmp_dir: home.path().to_path_buf(),
                globals: options.clone(),
                file_path: &input,
            })
            .await;

            initiate.assert_async().await;
            upload.assert_async().await;
            complete.assert_async().await;
            if succeeds {
                assert_eq!(result?, "completed");
            } else {
                assert!(
                    result.is_err(),
                    "failed zero-budget PUT must stop completion"
                );
            }
        }
    }
    Ok(())
}

#[test]
fn final_pipe_replays_exact_bytes_within_total_attempt_budget() -> Result<()> {
    for (budget, failures, succeeds) in [
        (0, 1, false),
        (1, 1, false),
        (3, 3, false),
        (3, 1, true),
        (3, 2, true),
    ] {
        let mut server = Server::new();
        let home = tempdir()?;
        let config = home.path().join("config.yml");
        std::fs::write(
            &config,
            format!(
                "hosts:\n  s3:\n    endpoint: {}\n    access_key: dummy\n    secret_key: dummy-secret\n",
                server.url()
            ),
        )?;
        let payload = (0..=255_u8).cycle().take(65_537).collect::<Vec<_>>();
        let initiate = server
            .mock("POST", "/bucket/key")
            .match_query(Matcher::UrlEncoded("uploads".into(), String::new()))
            .with_body("<InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>key</Key><UploadId>retry-upload</UploadId></InitiateMultipartUploadResult>")
            .expect(1)
            .create();
        let failed = server
            .mock("PUT", "/bucket/key")
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("partNumber".into(), "1".into()),
                Matcher::UrlEncoded("uploadId".into(), "retry-upload".into()),
            ]))
            .match_body(payload.clone())
            .with_status(503)
            .with_body("<Error><Code>ServiceUnavailable</Code></Error>")
            .expect(failures)
            .create();
        let success = succeeds.then(|| {
            server
                .mock("PUT", "/bucket/key")
                .match_query(Matcher::AllOf(vec![
                    Matcher::UrlEncoded("partNumber".into(), "1".into()),
                    Matcher::UrlEncoded("uploadId".into(), "retry-upload".into()),
                ]))
                .match_body(payload.clone())
                .with_header("etag", "replayed-part")
                .expect(1)
                .create()
        });
        let complete = server
            .mock("POST", "/bucket/key")
            .match_query(Matcher::UrlEncoded("uploadId".into(), "retry-upload".into()))
            .match_body(Matcher::Regex("<ETag>replayed-part</ETag>".into()))
            .with_body("<CompleteMultipartUploadResult><Location>local</Location><Bucket>bucket</Bucket><Key>key</Key><ETag>completed</ETag></CompleteMultipartUploadResult>")
            .expect(usize::from(succeeds))
            .create();
        let mut child = Command::new(env!("CARGO_BIN_EXE_s3m"))
            .env("HOME", home.path())
            .env("USERPROFILE", home.path())
            .args(["--config"])
            .arg(&config)
            .args(["--retries", &budget.to_string(), "--pipe", "s3/bucket/key"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        child
            .stdin
            .take()
            .context("piped stdin")?
            .write_all(&payload)?;
        let output = child.wait_with_output()?;
        assert_eq!(
            output.status.success(),
            succeeds,
            "budget {budget}, failures {failures}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        if succeeds {
            assert!(String::from_utf8_lossy(&output.stdout).contains("completed"));
        } else {
            assert!(
                String::from_utf8_lossy(&output.stderr).contains("Failed to upload final part")
            );
            assert!(!String::from_utf8_lossy(&output.stdout).contains("ETag:"));
        }
        initiate.assert();
        failed.assert();
        if let Some(success) = success {
            success.assert();
        }
        complete.assert();
    }
    Ok(())
}
