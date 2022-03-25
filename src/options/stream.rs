use crate::s3::{actions, S3};
use crate::s3m::progressbar::Bar;
use anyhow::Result;
use bytes::BytesMut;
use crossbeam::channel::{unbounded, Sender};
use futures::stream::TryStreamExt;
use std::collections::BTreeMap;
use std::io::Write;
use tempfile::{Builder, NamedTempFile};
use tokio::io::stdin;
use tokio_util::codec::{BytesCodec, FramedRead};

// 512MB
const BUFFER_SIZE: usize = 1024 * 1024 * 512;

#[derive(Debug)]
struct Stream<'a> {
    tmp_file: NamedTempFile,
    count: usize,
    etags: Vec<String>,
    key: &'a str,
    part_number: u16,
    s3: &'a S3,
    upload_id: &'a str,
    channel: Option<Sender<usize>>,
}

/// Read from STDIN, since the size is unknown we use the max chunk size = 512MB, to handle the max supported file object of 5TB
pub async fn stream(s3: &S3, key: &str, quiet: bool) -> Result<String> {
    // Initiate Multipart Upload - request an Upload ID
    let action = actions::CreateMultipartUpload::new(key);
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
        if let Some(pb) = pb.progress.clone() {
            tokio::spawn(async move {
                let mut count = 0;
                while let Ok(i) = receiver.recv() {
                    count += i;
                    pb.set_message(bytesize::to_string(count as u64, true));
                    //                 if (count % BUFFER_SIZE) == 0 {
                    //                    pb.disable_steady_tick();
                    //               }
                }
                pb.finish();
            });
        }
    };

    let first_stream = Stream {
        tmp_file: Builder::new()
            .prefix(&upload_id)
            .suffix(".s3m")
            .tempfile()?,
        count: 0,
        etags: Vec::new(),
        key,
        part_number: 1,
        s3,
        upload_id: &upload_id,
        channel,
    };

    let mut count = 0;
    // try_fold will pass writer to fold_fn until there are no more bytes to read.
    // FrameRead return a stream of Result<BytesMut, Error>.
    // TODO get the sha256_md5_digest here to prevent reading twice
    let mut last_stream = FramedRead::new(stdin(), BytesCodec::new())
        .inspect_ok(|chunk| {
            if let Some(pb) = &pb.progress {
                count += chunk.len();
                pb.set_message(bytesize::to_string(count as u64, true));
            }
        })
        .try_fold(first_stream, fold_fn)
        .await?;
    let action = actions::StreamPart::new(
        key,
        last_stream.tmp_file.path(),
        last_stream.part_number,
        &upload_id,
        None,
    );
    let etag = action.request(s3).await?;
    last_stream.etags.push(etag);

    // https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html
    let uploaded: BTreeMap<u16, actions::Part> = last_stream
        .etags
        .into_iter()
        .zip(1..)
        .map(|(etag, number)| (number, actions::Part { etag, number }))
        .collect();

    let action = actions::CompleteMultipartUpload::new(key, &upload_id, uploaded);
    let rs = action.request(s3).await?;
    Ok(rs.e_tag)
}

async fn fold_fn<'a>(mut part: Stream<'a>, bytes: BytesMut) -> Result<Stream<'a>, std::io::Error> {
    part.count += bytes.len();
    // chunk size 512MB
    if part.count + bytes.len() >= BUFFER_SIZE {
        let action = actions::StreamPart::new(
            part.key,
            part.tmp_file.path(),
            part.part_number,
            part.upload_id,
            part.channel.clone(),
        );
        // TODO handle unwrap
        let etag = action.request(part.s3).await.unwrap();
        // delete and create new file
        part.etags.push(etag);
        part.tmp_file.close()?;
        part.tmp_file = Builder::new()
            .prefix(part.upload_id)
            .suffix(".s3m")
            .tempfile()?;
        part.count = 0;
        part.part_number += 1;
    } else {
        part.tmp_file.write_all(&bytes)?;
    }
    Ok(part)
}
