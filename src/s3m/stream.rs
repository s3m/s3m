use crate::s3::{actions, S3};
use anyhow::Result;
use bytes::{BufMut, BytesMut};
use futures::stream::TryStreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use std::collections::BTreeMap;
use tokio::io::stdin;
use tokio_util::codec::{BytesCodec, FramedRead};

enum StreamWriter<'a> {
    Init {
        buf_size: usize,
        key: &'a str,
        s3: &'a S3,
        upload_id: &'a str,
        pb: &'a ProgressBar,
    },
    Uploading {
        buf_size: usize,
        buffer: BytesMut,
        etags: Vec<String>,
        key: &'a str,
        part_number: u16,
        s3: &'a S3,
        upload_id: &'a str,
        count: usize,
        pb: &'a ProgressBar,
    },
}

pub async fn stream<'a>(s3: &'a S3, key: &'a str, buf_size: usize) -> Result<String> {
    // Initiate Multipart Upload - request an Upload ID
    let action = actions::CreateMultipartUpload::new(key);
    let response = action.request(s3).await?;

    let pb = ProgressBar::new_spinner();
    pb.enable_steady_tick(200);
    pb.set_style(
        ProgressStyle::default_spinner()
            .tick_strings(&[
                "\u{2801}", "\u{2802}", "\u{2804}", "\u{2840}", "\u{2880}", "\u{2820}", "\u{2810}",
                "\u{2808}", "",
            ])
            .template("{spinner:.green}  {msg}"),
    );

    // initialize writer
    // TODO use references instead of copy the values
    let writer = StreamWriter::Init {
        buf_size,
        key,
        s3,
        upload_id: &response.upload_id,
        pb: &pb,
    };

    // try_fold will pass writer to fold_fn until there are no more bytes to
    // read. FrameRead return a stream of Result<BytesMut, Error>.
    let result = FramedRead::new(stdin(), BytesCodec::new())
        .try_fold(writer, fold_fn)
        .await?;

    // compleat the multipart upload
    match result {
        StreamWriter::Uploading {
            buf_size: _,
            buffer,
            mut etags,
            key,
            part_number,
            s3,
            upload_id,
            count,
            pb,
        } => {
            pb.finish_and_clear();
            println!(
                "Uploaded Bytes: {}",
                bytesize::to_string(count as u64, true)
            );

            let action = actions::StreamPart::new(key, buffer.freeze(), part_number, upload_id);
            let etag = action.request(s3).await.unwrap();
            etags.push(etag);

            // https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html
            let uploaded: BTreeMap<u16, actions::Part> = etags
                .into_iter()
                .zip(1..)
                .map(|(etag, number)| (number, actions::Part { etag, number }))
                .collect();

            let action = actions::CompleteMultipartUpload::new(key, upload_id, uploaded);
            let rs = action.request(s3).await?;
            Ok(format!("ETag: {}", rs.e_tag))
        }
        _ => todo!(),
    }
}

async fn fold_fn<'a>(
    writer: StreamWriter<'_>,
    bytes: BytesMut,
) -> Result<StreamWriter<'_>, std::io::Error> {
    let writer = match writer {
        StreamWriter::Init {
            buf_size,
            key,
            s3,
            upload_id,
            pb,
        } => StreamWriter::Uploading {
            buf_size,
            buffer: BytesMut::with_capacity(buf_size),
            etags: Vec::new(),
            key,
            part_number: 1,
            s3,
            upload_id,
            count: 0,
            pb,
        },
        _ => writer,
    };
    match writer {
        StreamWriter::Uploading {
            buf_size,
            mut buffer,
            mut etags,
            key,
            part_number,
            s3,
            upload_id,
            mut count,
            pb,
        } => {
            // if buffer size > buf_size create another buffer and upload the previous one
            if buffer.len() + bytes.len() >= buf_size {
                count += bytes.len();
                pb.set_message(&bytesize::to_string(count as u64, true));

                let mut new_buf = BytesMut::with_capacity(buf_size);
                new_buf.put(bytes);

                // upload the old buffer
                let action = actions::StreamPart::new(key, buffer.freeze(), part_number, upload_id);
                // TODO remove unwrap
                let etag = action.request(s3).await.unwrap();
                etags.push(etag);

                // loop again until buffer is full
                Ok(StreamWriter::Uploading {
                    buf_size,
                    buffer: new_buf,
                    etags,
                    key,
                    part_number: part_number + 1,
                    s3,
                    upload_id,
                    count,
                    pb,
                })
            } else {
                count += bytes.len();
                pb.set_message(&bytesize::to_string(count as u64, true));
                buffer.put(bytes);
                Ok(StreamWriter::Uploading {
                    buf_size,
                    buffer,
                    etags,
                    key,
                    part_number,
                    s3,
                    upload_id,
                    count,
                    pb,
                })
            }
        }
        _ => todo!(), // this should never happen
    }
}
