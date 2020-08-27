use anyhow::{anyhow, Result};
use bytes::BytesMut;
use futures::stream::TryStreamExt;
use tokio::io::stdin;
use tokio::prelude::*;
use tokio_util::codec::{BytesCodec, FramedRead};

// const BUFFER_SIZE: usize = 1024 * 1024 * 5;

enum StreamWriter {
    Init {
        buf_size: usize,
    },
    Uploading {
        upload_id: String,
        buf_size: usize,
        part_count: u16,
        etags: Vec<String>,
        buffer: Vec<u8>,
    },
}

pub async fn dispatcher() -> Result<()> {
    let writer = StreamWriter::Init {
        buf_size: 1024 * 1024 * 10,
    };
    // Turn an AsyncRead into a stream of Result<BytesMut, Error>.
    let result = FramedRead::new(stdin(), BytesCodec::new())
        .try_fold(writer, fold_fn)
        .await?;

    match result {
        StreamWriter::Uploading {
            upload_id,
            buf_size,
            part_count,
            buffer,
            etags,
        } => {
            println!("remaining: {}", buffer.len());
        }
        _ => {
            todo!();
        }
    }

    Ok(())
}

async fn fold_fn(writer: StreamWriter, bytes: BytesMut) -> Result<StreamWriter, std::io::Error> {
    let writer = match writer {
        StreamWriter::Init { buf_size } => {
            println!("first call in fold");

            StreamWriter::Uploading {
                upload_id: "foo".to_string(),
                buf_size,
                part_count: 1,
                buffer: Vec::with_capacity(buf_size),
                etags: Vec::new(),
            }
        }
        _ => writer,
    };
    match writer {
        StreamWriter::Uploading {
            upload_id,
            buf_size,
            part_count,
            mut buffer,
            etags,
        } => match buffer.len() + bytes.len() >= buf_size {
            true => {
                let mut new_buf = Vec::with_capacity(buf_size);
                new_buf.write_all(&bytes).await?;
                println!("old buff: {}", buffer.len());
                Ok(StreamWriter::Uploading {
                    upload_id,
                    buf_size,
                    part_count,
                    buffer: new_buf,
                    etags,
                })
            }
            false => {
                buffer.write_all(&bytes).await?;
                Ok(StreamWriter::Uploading {
                    upload_id,
                    buf_size,
                    part_count,
                    buffer,
                    etags,
                })
            }
        },
        _ => panic!(),
    }
}
