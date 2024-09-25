pub mod db;
pub mod iterator;
pub mod part;
pub mod upload_compressed;
pub mod upload_default;
pub mod upload_multipart;
pub mod upload_stdin;

use crate::{
    cli::globals::GlobalArgs,
    s3::{actions::StreamPart, S3},
};
use anyhow::Result;
use crossbeam::channel::Sender;
use std::path::{Path, PathBuf};
use tempfile::NamedTempFile;
use tokio::time::{sleep, Duration};

// 512MB  to upload 5TB (the current max object size)
const STDIN_BUFFER_SIZE: usize = 1_024 * 1_024 * 512;

struct Stream<'a> {
    tmp_file: NamedTempFile,
    count: usize,
    etags: Vec<String>,
    key: &'a str,
    part_number: u16,
    s3: &'a S3,
    upload_id: &'a str,
    sha: ring::digest::Context,
    md5: md5::Context,
    channel: Option<Sender<usize>>,
    tmp_dir: PathBuf,
    throttle: Option<usize>,
    retries: u32,
}

// return the key with the .zst extension if compress option is set
fn get_key(key: &str, compress: bool) -> String {
    if compress
        && !Path::new(key)
            .extension()
            .map_or(false, |ext| ext.eq_ignore_ascii_case("zst"))
    {
        return format!("{key}.zst");
    }
    key.to_string()
}

async fn try_stream_part(part: &Stream<'_>) -> Result<String> {
    let mut etag = String::new();

    let digest_sha = part.sha.clone().finish();
    let digest_md5 = part.md5.clone().compute();

    // Create globals only to pass the throttle
    let globals = GlobalArgs {
        throttle: part.throttle,
        retries: part.retries,
        compress: false,
    };

    for attempt in 1..=part.retries {
        let backoff_time = 2u64.pow(attempt - 1);
        if attempt > 1 {
            log::warn!(
                "Error streaming part number {}, retrying in {} seconds",
                part.part_number,
                backoff_time
            );

            sleep(Duration::from_secs(backoff_time)).await;
        }

        let action = StreamPart::new(
            part.key,
            part.tmp_file.path(),
            part.part_number,
            part.upload_id,
            part.count,
            (digest_sha.as_ref(), digest_md5.as_ref()),
            part.channel.clone(),
        );

        match action.request(part.s3, &globals).await {
            Ok(e) => {
                etag = e;

                log::info!("Uploaded part: {}, etag: {}", part.part_number, etag);

                break;
            }

            Err(e) => {
                log::error!(
                    "Error uploading part number {}, attempt {}/{} failed: {}",
                    part.part_number,
                    attempt,
                    part.retries,
                    e
                );

                if attempt == part.retries {
                    return Err(anyhow::anyhow!(
                        "Error uploading part number {}, {}",
                        part.part_number,
                        e
                    ));
                }

                continue;
            }
        }
    }

    Ok(etag)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::{Credentials, Region, S3};
    use secrecy::SecretString;

    #[test]
    fn test_get_key() {
        let test_cases = vec![
            ("test", false, "test"),
            ("test", true, "test.zst"),
            ("test.txt", false, "test.txt"),
            ("test.txt", true, "test.txt.zst"),
            ("test.ZST", false, "test.ZST"),
            ("test.ZST", true, "test.ZST"),
            ("testzst", true, "testzst.zst"),
        ];
        for (key, compress, expected) in test_cases {
            assert_eq!(get_key(key, compress), expected);
        }
    }

    #[test]
    fn test_try_stream_part() {
        let s3 = S3::new(
            &Credentials::new(
                "AKIAIOSFODNN7EXAMPLE",
                &SecretString::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into()),
            ),
            &"us-west-1".parse::<Region>().unwrap(),
            Some("awsexamplebucket1".to_string()),
            false,
        );

        let part = Stream {
            tmp_file: NamedTempFile::new().unwrap(),
            count: 0,
            etags: Vec::new(),
            key: "test",
            part_number: 1,
            s3: &s3,
            upload_id: "test",
            sha: ring::digest::Context::new(&ring::digest::SHA256),
            md5: md5::Context::new(),
            channel: None,
            tmp_dir: PathBuf::new(),
            throttle: None,
            retries: 1,
        };

        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(try_stream_part(&part));
        assert!(result.is_err());
    }
}
