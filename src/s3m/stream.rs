use crate::s3::{actions, S3};
use anyhow::Result;
use bytes::BytesMut;
use futures::stream::TryStreamExt;
use std::collections::BTreeMap;
use tokio::io::stdin;
use tokio::prelude::*;
use tokio_util::codec::{BytesCodec, FramedRead};

enum StreamWriter {
    Init {
        buf_size: usize,
        key: String,
        s3: S3,
        upload_id: String,
    },
    Uploading {
        buf_size: usize,
        buffer: Vec<u8>,
        etags: Vec<String>,
        key: String,
        part_number: u16,
        s3: S3,
        upload_id: String,
    },
}

pub async fn stream(s3: &S3, key: &str, buf_size: usize) -> Result<String> {
    // Initiate Multipart Upload - request an Upload ID
    let action = actions::CreateMultipartUpload::new(key);
    let response = action.request(s3).await?;

    let writer = StreamWriter::Init {
        buf_size,
        key: key.to_string(),
        s3: s3.clone(),
        upload_id: response.upload_id,
    };

    // try_fold will pass writer to fold_fn until there are no more bytes to
    // read.  FrameRead return a stream of Result<BytesMut, Error>.
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
        } => {
            let action = actions::StreamPart::new(&key, buffer, part_number, &upload_id);
            let etag = action.request(&s3).await.unwrap();
            etags.push(etag);

            let uploaded: BTreeMap<u16, actions::Part> = etags
                .into_iter()
                .zip(1..)
                .map(|(etag, number)| (number, actions::Part { etag, number }))
                .collect();
            let action = actions::CompleteMultipartUpload::new(&key, &upload_id, uploaded);
            let rs = action.request(&s3).await?;
            Ok(format!("ETag: {}", rs.e_tag))
        }
        _ => todo!(),
    }
}

async fn fold_fn(writer: StreamWriter, bytes: BytesMut) -> Result<StreamWriter, std::io::Error> {
    let writer = match writer {
        StreamWriter::Init {
            buf_size,
            key,
            s3,
            upload_id,
        } => StreamWriter::Uploading {
            buf_size,
            buffer: Vec::with_capacity(buf_size),
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
            buf_size,
            mut buffer,
            mut etags,
            key,
            part_number,
            s3,
            upload_id,
        } => {
            if buffer.len() + bytes.len() >= buf_size {
                let mut new_buf = Vec::with_capacity(buf_size);
                new_buf.write_all(&bytes).await?;
                let action = actions::StreamPart::new(&key, buffer, part_number, &upload_id);
                let etag = action.request(&s3).await.unwrap();
                etags.push(etag);
                Ok(StreamWriter::Uploading {
                    buf_size,
                    buffer: new_buf,
                    etags,
                    key,
                    part_number: part_number + 1,
                    s3,
                    upload_id,
                })
            } else {
                buffer.write_all(&bytes).await?;
                Ok(StreamWriter::Uploading {
                    buf_size,
                    buffer,
                    etags,
                    key,
                    part_number,
                    s3,
                    upload_id,
                })
            }
        }
        _ => todo!(),
    }
}
