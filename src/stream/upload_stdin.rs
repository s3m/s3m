use crate::{
    cli::progressbar::Bar,
    s3::{actions, S3},
};
use anyhow::Result;
use bytes::BytesMut;
use crossbeam::channel::{unbounded, Sender};
use futures::stream::TryStreamExt;
use ring::digest::{Context, SHA256};
use std::{collections::BTreeMap, io::Write, path::PathBuf};
use tempfile::{Builder, NamedTempFile};
use tokio::io::{stdin, Error, ErrorKind};
use tokio_util::codec::{BytesCodec, FramedRead};

// 512MB
const BUFFER_SIZE: usize = 1_024 * 1_024 * 512;

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
}

/// Read from STDIN, since the size is unknown we use the max chunk size = 512MB, to handle the max supported file object of 5TB
pub async fn stream(
    s3: &S3,
    key: &str,
    acl: Option<String>,
    meta: Option<BTreeMap<String, String>>,
    quiet: bool,
    tmp_dir: PathBuf,
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
    };

    // try_fold will pass writer to fold_fn until there are no more bytes to read.
    // FrameRead return a stream of Result<BytesMut, io::Error>.
    let mut last_stream = FramedRead::new(stdin(), BytesCodec::new())
        .try_fold(first_stream, fold_fn)
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

    let etag = action.request(s3).await?;

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
async fn fold_fn(mut part: Stream<'_>, bytes: BytesMut) -> Result<Stream<'_>, Error> {
    if part.count >= BUFFER_SIZE {
        // when data is bigger than 512MB, upload a part
        // calculate sha256 and md5
        let digest_sha = part.sha.finish();
        let digest_md5 = part.md5.compute();

        // upload a part
        let action = actions::StreamPart::new(
            part.key,
            part.tmp_file.path(),
            part.part_number,
            part.upload_id,
            part.count,
            (digest_sha.as_ref(), digest_md5.as_ref()),
            part.channel.clone(),
        );

        let etag = action
            .request(part.s3)
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

        // reset counters
        part.count = 0;
        part.part_number += 1;
        part.sha = Context::new(&SHA256);
        part.md5 = md5::Context::new();
    } else {
        // update counters and write to tmp file
        part.count += bytes.len();
        part.tmp_file.write_all(&bytes)?;
        part.sha.update(&bytes);
        part.md5.consume(&bytes);
    }

    Ok(part)
}
