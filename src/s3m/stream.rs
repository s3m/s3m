use crate::s3::{actions, S3};
use anyhow::Result;
use bytes::BytesMut;
use futures::stream::TryStreamExt;
use tokio::io::stdin;
use tokio::prelude::*;
use tokio_util::codec::{BytesCodec, FramedRead};

// const BUFFER_SIZE: usize = 1024 * 1024 * 5;

enum StreamWriter {
    Init {
        buf_size: usize,
        key: String,
        upload_id: String,
    },
    Uploading {
        key: String,
        upload_id: String,
        buf_size: usize,
        part_number: u16,
        etags: Vec<String>,
        buffer: Vec<u8>,
    },
}

pub async fn stream(s3: &S3, key: &str) -> Result<()> {
    // Initiate Multipart Upload - request an Upload ID
    let action = actions::CreateMultipartUpload::new(key);
    let response = action.request(s3).await?;

    let writer = StreamWriter::Init {
        buf_size: 1024 * 1024 * 10,
        upload_id: response.upload_id,
        key: key.to_string(),
    };

    // Turn an AsyncRead into a stream of Result<BytesMut, Error>.
    let result = FramedRead::new(stdin(), BytesCodec::new())
        .try_fold(writer, fold_fn)
        .await?;

    match result {
        StreamWriter::Uploading {
            key,
            upload_id,
            buf_size,
            part_number,
            buffer,
            etags,
        } => {
            println!("remaining: {}", buffer.len());
        }
        _ => todo!(),
    }

    Ok(())
}

async fn fold_fn(writer: StreamWriter, bytes: BytesMut) -> Result<StreamWriter, std::io::Error> {
    let writer = match writer {
        StreamWriter::Init {
            buf_size,
            key,
            upload_id,
        } => StreamWriter::Uploading {
            buf_size,
            buffer: Vec::with_capacity(buf_size),
            etags: Vec::new(),
            key,
            part_number: 1,
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
            upload_id,
        } => match buffer.len() + bytes.len() >= buf_size {
            true => {
                let mut new_buf = Vec::with_capacity(buf_size);
                new_buf.write_all(&bytes).await?;

                Ok(StreamWriter::Uploading {
                    buf_size,
                    buffer: new_buf,
                    etags,
                    key,
                    part_number: part_number + 1,
                    upload_id,
                })
            }
            false => {
                buffer.write_all(&bytes).await?;
                Ok(StreamWriter::Uploading {
                    buf_size,
                    buffer,
                    etags,
                    key,
                    part_number,
                    upload_id,
                })
            }
        },
        _ => todo!(),
    }
}
