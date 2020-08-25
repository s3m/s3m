use anyhow::Result;
use futures::stream::TryStreamExt;
use tokio::fs::File;
use tokio::io::stdin;
use tokio::prelude::*;
use tokio_util::codec::{BytesCodec, FramedRead};

pub async fn prebuffer(buffer: u64) -> Result<()> {
    let mut i: usize = 0;
    let mut size: u64 = 0;
    let mut stream = FramedRead::new(stdin(), BytesCodec::new());
    loop {
        let mut f = File::create(format!("/tmp/c/chunk_{}", i)).await?;
        while let Some(bytes) = stream.try_next().await? {
            size += bytes.len() as u64;
            f.write_all(&bytes).await?;
            if size % buffer == 0 {
                i += 1;
                break;
            }
        }
        if stream.try_next().await?.is_none() {
            break;
        }
    }
    Ok(())
}
