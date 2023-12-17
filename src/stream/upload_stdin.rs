use crate::{
    cli::{globals::GlobalArgs, progressbar::Bar},
    s3::{actions, actions::StreamPart, S3},
};
use anyhow::Result;
use bytes::BytesMut;
use crossbeam::channel::{unbounded, Sender};
use futures::stream::TryStreamExt;
use ring::digest::{Context, SHA256};
use std::{collections::BTreeMap, io::Write, path::PathBuf};
use tempfile::{Builder, NamedTempFile};
use tokio::{
    io::{stdin, Error, ErrorKind},
    time::{sleep, Duration},
};
use tokio_util::codec::{BytesCodec, FramedRead};

// 512MB
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

/// Read from STDIN, since the size is unknown we use the max chunk size = 512MB, to handle the max supported file object of 5TB
/// # Errors
/// Will return an error if the upload fails
pub async fn stream(
    s3: &S3,
    key: &str,
    acl: Option<String>,
    meta: Option<BTreeMap<String, String>>,
    quiet: bool,
    tmp_dir: PathBuf,
    globals: GlobalArgs,
) -> Result<String> {
    // Initiate Multipart Upload - request an Upload ID
    let action = actions::CreateMultipartUpload::new(key, acl, meta, None);
    let response = action.request(s3).await?;
    let upload_id = response.upload_id;
    let (sender, receiver) = unbounded::<usize>();
    let channel = if quiet { None } else { Some(sender) };

    // Upload parts progress bar
    let pb = if quiet {
        Bar::default()
    } else {
        Bar::new_spinner_stream()
    };

    if !quiet {
        // Spawn a new thread to update the progress bar
        if let Some(pb) = pb.progress.clone() {
            tokio::spawn(async move {
                let mut uploaded = 0;
                while let Ok(i) = receiver.recv() {
                    uploaded += i;
                    pb.set_message(bytesize::to_string(uploaded as u64, true));
                }
                pb.finish();
            });
        }
    };

    let first_stream = Stream {
        tmp_file: Builder::new()
            .prefix(&upload_id)
            .suffix(".s3m")
            .tempfile_in(&tmp_dir)?,
        count: 0,
        etags: Vec::new(),
        key,
        part_number: 1,
        s3,
        upload_id: &upload_id,
        sha: Context::new(&SHA256),
        md5: md5::Context::new(),
        channel,
        tmp_dir,
        throttle: globals.throttle,
        retries: globals.retries,
    };

    // try_fold will pass writer to fold_fn until there are no more bytes to read.
    // FrameRead return a stream of Result<BytesMut, io::Error>.
    // let mut last_stream = FramedRead::new(stdin(), BytesCodec::new())
    //     .try_fold(first_stream, fold_fn)
    //     .await?;
    let mut last_stream = FramedRead::new(stdin(), BytesCodec::new())
        .try_fold(first_stream, |part, bytes| {
            fold_fn(part, bytes, STDIN_BUFFER_SIZE)
        })
        .await?;

    // Calculate sha256 and md5 for the last part
    let digest_sha = last_stream.sha.finish();
    let digest_md5 = last_stream.md5.compute();

    // upload last part
    let action = actions::StreamPart::new(
        key,
        last_stream.tmp_file.path(),
        last_stream.part_number,
        &upload_id,
        last_stream.count,
        (digest_sha.as_ref(), digest_md5.as_ref()),
        last_stream.channel,
    );

    let etag = action.request(s3, &globals).await?;

    last_stream.etags.push(etag);

    // https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html
    let uploaded: BTreeMap<u16, actions::Part> = last_stream
        .etags
        .into_iter()
        .zip(1..)
        .map(|(etag, number)| {
            (
                number,
                actions::Part {
                    etag,
                    number,
                    checksum: None,
                },
            )
        })
        .collect();

    let action = actions::CompleteMultipartUpload::new(key, &upload_id, uploaded, None);

    let rs = action.request(s3).await?;

    Ok(rs.e_tag)
}

// try to read/parse only once on the same, so in the loop calculate the sha256, md5 and get the
// length, this should speed up things and consume less resources
// TODO crossbeam channel to get progress bar but for now pv could be used, for example:
// cat file | pv | s3m
async fn fold_fn(
    mut part: Stream<'_>,
    bytes: BytesMut,
    buffer_size: usize,
) -> Result<Stream, Error> {
    // when data is bigger than 512MB, upload a part
    if part.count >= buffer_size {
        let etag = try_stream_part(&part)
            .await
            .map_err(|e| Error::new(ErrorKind::Other, format!("Error streaming part: {e}")))?;

        // delete and create new tmp file
        part.etags.push(etag);

        // close, delete tmp file and create a new one
        part.tmp_file.close()?;
        part.tmp_file = Builder::new()
            .prefix(part.upload_id)
            .suffix(".s3m")
            .tempfile_in(&part.tmp_dir)?;

        // set count to the current bytes len and increment part number
        part.count = bytes.len();
        part.part_number += 1;
        part.sha = Context::new(&SHA256);
        part.md5 = md5::Context::new();

        log::debug!(
            "New part number {}, bytes: {}",
            part.part_number,
            part.count
        );
    } else {
        // increment the count
        part.count += bytes.len();

        log::debug!(
            "Adding {} bytes to part number {}",
            bytes.len(),
            part.part_number
        );
    }

    part.tmp_file.write_all(&bytes)?;
    part.sha.update(&bytes);
    part.md5.consume(&bytes);

    Ok(part)
}

async fn try_stream_part(part: &Stream<'_>) -> Result<String> {
    let mut etag = String::new();

    let digest_sha = part.sha.clone().finish();
    let digest_md5 = part.md5.clone().compute();

    // Create globals only to pass the throttle
    let globals = GlobalArgs {
        throttle: part.throttle,
        retries: part.retries,
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
    use crate::s3::{
        checksum::{Checksum, ChecksumAlgorithm},
        {Credentials, Region, S3},
    };
    use base64ct::{Base64, Encoding};
    use secrecy::Secret;
    use std::io::Write;
    use tempfile::{tempdir, Builder};
    use tokio::fs::File;

    #[tokio::test]
    async fn test_fold_fn() -> Result<()> {
        // env_logger::init();
        // TODO need to refactor to test StreamPart and Stream
        let mut tmp_file = Builder::new().prefix("test").suffix(".s3m").tempfile()?;
        let chunk_size_bytes = 1 * 1024 * 1024;
        let total_size_bytes = 10 * 1024 * 1024;
        let buffer: Vec<u8> = vec![0; chunk_size_bytes];
        let num_chunks = total_size_bytes / chunk_size_bytes;
        for _ in 0..num_chunks {
            // Write the buffer to the file for each chunk
            tmp_file.write_all(&buffer)?;
        }
        let mut checksum = Checksum::new(ChecksumAlgorithm::Sha256);
        let hash = checksum.calculate(tmp_file.path()).await?;
        let decoded = Base64::decode_vec(&hash)
            .unwrap()
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect::<String>();

        let s3 = S3::new(
            &Credentials::new(
                "AKIAIOSFODNN7EXAMPLE",
                &Secret::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string()),
            ),
            &"us-west-1".parse::<Region>().unwrap(),
            Some("awsexamplebucket1".to_string()),
            false,
        );

        let tmp_dir = tempdir()?;

        let first_stream = Stream {
            tmp_file: Builder::new().prefix("test").suffix(".s3m").tempfile()?,
            count: 0,
            etags: Vec::new(),
            key: "test",
            part_number: 1,
            s3: &s3,
            upload_id: "test",
            sha: Context::new(&SHA256),
            md5: md5::Context::new(),
            channel: None,
            tmp_dir: tmp_dir.path().to_path_buf(),
            throttle: None,
            retries: 0,
        };

        let file = File::open(tmp_file).await?;

        let last_stream = FramedRead::new(file, BytesCodec::new())
            .try_fold(first_stream, |part, bytes| {
                fold_fn(part, bytes, STDIN_BUFFER_SIZE)
            })
            .await?;

        assert_eq!(last_stream.count, total_size_bytes);

        let digest_sha1 = last_stream
            .sha
            .finish()
            .as_ref()
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect::<String>();

        assert_eq!(decoded, digest_sha1);

        Ok(())
    }
}
