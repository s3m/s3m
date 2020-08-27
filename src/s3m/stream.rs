use anyhow::Result;
use futures::stream::TryStreamExt;
use tokio::io::stdin;
use tokio::io::BufReader;
use tokio::prelude::*;
use tokio_util::codec::{BytesCodec, FramedRead};

const BUFFER_SIZE: usize = 536_870_912;

// https://stackoverflow.com/q/41069865/1135424
// https://users.rust-lang.org/t/reading-from-stdin-performance/2025
pub async fn dispatcher() -> Result<()> {
    let mut i: usize = 0;
    let mut size: usize = 0;
    let mut stream = FramedRead::new(stdin(), BytesCodec::new());
    loop {
        let mut in_memory_part = Vec::with_capacity(24);
        while let Some(bytes) = stream.try_next().await? {
            size += bytes.len();
            in_memory_part.write_all(&bytes).await?;
            if size % BUFFER_SIZE == 0 {
                i += 1;
                break;
            }
        }
        println!("size: {}, i: {}", size, i);
        if stream.try_next().await?.is_none() {
            break;
        }
    }
    Ok(())
}
