use anyhow::Result;
use futures::stream::TryStreamExt;
use tokio::io::stdin;
use tokio::prelude::*;
use tokio_util::codec::{BytesCodec, FramedRead};

const BUFFER_SIZE: usize = 1024 * 1024 * 5;

// https://stackoverflow.com/q/41069865/1135424
// https://users.rust-lang.org/t/reading-from-stdin-performance/2025
pub async fn dispatcher() -> Result<()> {
    todo!();
}
