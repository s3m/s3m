use crate::s3::{actions, S3};
use crate::s3m::progressbar::Bar;
use anyhow::Result;
use bytes::{BufMut, BytesMut};
use futures::stream::TryStreamExt;
use std::collections::BTreeMap;
use tokio::io::stdin;
use tokio_util::codec::{BytesCodec, FramedRead};

// 512MB
const BUFFER_SIZE: usize = 1024 * 1024 * 512;

enum StreamWriter<'a> {
    Init {
        key: &'a str,
        s3: &'a S3,
        upload_id: &'a str,
    },
    Uploading {
        buffer: BytesMut,
        etags: Vec<String>,
        key: &'a str,
        part_number: u16,
        s3: &'a S3,
        upload_id: &'a str,
    },
}

/// Read from STDIN, since the size is unknown we use the max chunk size = 512MB, to handle the
/// max supported file object of 5TB
pub async fn stream(s3: &S3, key: &str, quiet: bool) -> Result<String> {
    // Initiate Multipart Upload - request an Upload ID
    let action = actions::CreateMultipartUpload::new(key);
    let response = action.request(s3).await?;

    // Upload parts progress bar
    let pb = if quiet {
        Bar::default()
    } else {
        Bar::new_spinner_stream()
    };

    // initialize writer
    let writer = StreamWriter::Init {
        key,
        s3,
        upload_id: &response.upload_id,
    };

    let mut count = 0;
    // try_fold will pass writer to fold_fn until there are no more bytes to read.
    // FrameRead return a stream of Result<BytesMut, Error>.
    let stream = FramedRead::new(stdin(), BytesCodec::new())
        .inspect_ok(|chunk| {
            if let Some(pb) = &pb.progress {
                count += chunk.len();
                pb.set_message(bytesize::to_string(count as u64, true));
            }
        })
        .try_fold(writer, fold_fn)
        .await?;

    // complete the multipart upload
    match stream {
        StreamWriter::Uploading {
            buffer,
            key,
            mut etags,
            part_number,
            s3,
            upload_id,
        } => {
            if let Some(pb) = &pb.progress {
                pb.set_message(bytesize::to_string(count as u64, true));
            }
            let stream = buffer.freeze();
            let action = actions::StreamPart::new(key, stream, part_number, upload_id);
            let etag = action.request(s3).await?;
            etags.push(etag);

            // https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html
            let uploaded: BTreeMap<u16, actions::Part> = etags
                .into_iter()
                .zip(1..)
                .map(|(etag, number)| (number, actions::Part { etag, number }))
                .collect();

            let action = actions::CompleteMultipartUpload::new(key, upload_id, uploaded);
            let rs = action.request(s3).await?;
            Ok(rs.e_tag)
        }
        _ => panic!(),
    }
}
async fn fold_fn<'a>(
    writer: StreamWriter<'_>,
    bytes: BytesMut,
) -> Result<StreamWriter<'_>, std::io::Error> {
    // in the first interaction Init will only match and initialize the StreamWriter
    // then Uploading will only match until it consumes all data from STDIN
    let writer = match writer {
        StreamWriter::Init { key, s3, upload_id } => StreamWriter::Uploading {
            buffer: BytesMut::with_capacity(BUFFER_SIZE),
            etags: Vec::new(),
            key,
            part_number: 1,
            s3,
            upload_id,
        },
        _ => writer,
    };

    match writer {
        StreamWriter::Uploading {
            key,
            mut buffer,
            mut etags,
            part_number,
            s3,
            upload_id,
        } => {
            // if buffer size > buf_size create another buffer and upload the previous one
            if buffer.len() + bytes.len() >= BUFFER_SIZE {
                let mut new_buf = BytesMut::with_capacity(BUFFER_SIZE);
                new_buf.put(bytes);

                // upload the old buffer
                let action = actions::StreamPart::new(key, buffer.freeze(), part_number, upload_id);
                // TODO remove unwrap
                let etag = action.request(s3).await.unwrap();
                etags.push(etag);

                // loop again until buffer is full
                Ok(StreamWriter::Uploading {
                    buffer: new_buf,
                    etags,
                    key,
                    part_number: part_number + 1,
                    s3,
                    upload_id,
                })
            } else {
                buffer.put(bytes);
                Ok(StreamWriter::Uploading {
                    buffer,
                    etags,
                    key,
                    part_number,
                    s3,
                    upload_id,
                })
            }
        }
        _ => panic!(),
    }
}
