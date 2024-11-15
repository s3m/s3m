use crate::{
    cli::{globals::GlobalArgs, progressbar::Bar},
    s3::{actions, S3},
    stream::{get_key, try_stream_part, Stream, STDIN_BUFFER_SIZE},
};
use anyhow::Result;
use bytes::BytesMut;
use crossbeam::channel::unbounded;
use futures::stream::TryStreamExt;
use ring::digest::{Context, SHA256};
use std::{collections::BTreeMap, io::Write, path::PathBuf};
use tempfile::Builder;
use tokio::io::{stdin, Error, ErrorKind};
use tokio_util::codec::{BytesCodec, FramedRead};
use zstd::stream::encode_all;

/// Read from STDIN, since the size is unknown we use the max chunk size = 512MB, to handle the max supported file object of 5TB
/// # Errors
/// Will return an error if the upload fails
#[allow(clippy::too_many_arguments)]
pub async fn stream(
    s3: &S3,
    object_key: &str,
    acl: Option<String>,
    meta: Option<BTreeMap<String, String>>,
    quiet: bool,
    tmp_dir: PathBuf,
    globals: GlobalArgs,
    compress: bool,
) -> Result<String> {
    // use .zst extension if compress option is set
    let key = get_key(object_key, compress);

    let mut meta = meta.unwrap_or_default();

    if compress {
        meta.insert("Content-Type".to_string(), "application/zstd".to_string());
    }

    // Initiate Multipart Upload - request an Upload ID
    let action = actions::CreateMultipartUpload::new(&key, acl, Some(meta), None);
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
        key: &key,
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
    let mut last_stream = FramedRead::new(stdin(), BytesCodec::new())
        .try_fold(first_stream, |part, bytes| {
            fold_fn(part, bytes, STDIN_BUFFER_SIZE, compress)
        })
        .await?;

    // Calculate sha256 and md5 for the last part
    let digest_sha = last_stream.sha.finish();
    let digest_md5 = last_stream.md5.compute();

    // upload last part
    let action = actions::StreamPart::new(
        &key,
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

    let action = actions::CompleteMultipartUpload::new(&key, &upload_id, uploaded, None);

    let rs = action.request(s3).await?;

    Ok(rs.e_tag)
}

// read/parse only once in the loop, calculate the sha256, md5 and get the
// length, this should speed up things and consume less resources
async fn fold_fn(
    mut part: Stream<'_>,
    bytes: BytesMut,
    buffer_size: usize,
    compress: bool,
) -> Result<Stream, Error> {
    // compress data using zstd if option is set
    let data = if compress {
        encode_all(&*bytes, 0)
            .map_err(|e| Error::new(ErrorKind::Other, format!("Error compressing data: {e}")))?
    } else {
        bytes.to_vec()
    };

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
        part.count = data.len();
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
        part.count += data.len();

        log::debug!(
            "Adding {} bytes to part number {}",
            data.len(),
            part.part_number
        );
    }

    part.tmp_file.write_all(&data)?;
    part.sha.update(&data);
    part.md5.consume(&data);

    Ok(part)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::{
        checksum::{Checksum, ChecksumAlgorithm},
        {Credentials, Region, S3},
    };
    use base64ct::{Base64, Encoding};
    use secrecy::SecretString;
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
                &SecretString::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into()),
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
                fold_fn(part, bytes, STDIN_BUFFER_SIZE, false)
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
