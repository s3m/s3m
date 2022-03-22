use crate::s3::{actions, S3};
use crate::s3m::progressbar::Bar;
use anyhow::Result;
use bytes::BytesMut;
use futures::stream::TryStreamExt;
use std::collections::BTreeMap;
use std::io::Write;
use tempfile::{Builder, NamedTempFile};
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
    Streaming {
        tmp_file: NamedTempFile,
        count: usize,
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
        StreamWriter::Streaming {
            tmp_file,
            count,
            key,
            mut etags,
            part_number,
            s3,
            upload_id,
        } => {
            if let Some(pb) = &pb.progress {
                pb.set_message(bytesize::to_string(count as u64, true));
            }
            //let action = actions::StreamPart::new(key, stream, part_number, upload_id);
            //let etag = action.request(s3).await?;
            //etags.push(etag);

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
    writer: StreamWriter<'a>,
    bytes: BytesMut,
) -> Result<StreamWriter<'a>, std::io::Error> {
    // in the first interaction Init will only match and initialize the StreamWriter
    // then Streaming will only match until it consumes all data from STDIN
    let writer = match writer {
        StreamWriter::Init { key, s3, upload_id } => {
            let named_tempfile = Builder::new().prefix(upload_id).suffix(".s3m").tempfile()?;
            StreamWriter::Streaming {
                tmp_file: named_tempfile,
                count: 0,
                etags: Vec::new(),
                key,
                part_number: 1,
                s3,
                upload_id,
            }
        }
        _ => writer,
    };

    match writer {
        StreamWriter::Streaming {
            mut tmp_file,
            key,
            mut count,
            mut etags,
            part_number,
            s3,
            upload_id,
        } => {
            count += bytes.len();

            // if buffer size > buf_size create another buffer and upload the previous one
            if count + bytes.len() >= BUFFER_SIZE {
                // upload the old buffer
                //                let action = actions::StreamPart::new(key, file, part_number, upload_id);
                // TODO remove unwrap
                //               let etag = action.request(s3).await.unwrap();
                //              etags.push(etag);

                // loop again until buffer is full

                println!("flush: {:#?}", tmp_file);
                tmp_file.close()?;
                Ok(StreamWriter::Streaming {
                    tmp_file: Builder::new().prefix(upload_id).suffix(".s3m").tempfile()?,
                    count: 0,
                    etags,
                    key,
                    part_number: part_number + 1,
                    s3,
                    upload_id,
                })
            } else {
                tmp_file.write_all(&bytes)?;
                Ok(StreamWriter::Streaming {
                    tmp_file,
                    count,
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
