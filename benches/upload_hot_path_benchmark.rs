#![allow(clippy::expect_used, clippy::missing_panics_doc)]

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use futures::stream::TryStreamExt;
use ring::digest::{Context, SHA256};
use std::{
    path::{Path, PathBuf},
    sync::OnceLock,
};
use tokio::{fs::File, runtime::Runtime, sync::mpsc::unbounded_channel};
use tokio_util::codec::{BytesCodec, FramedRead};

const FILE_SIZE_BYTES: usize = 16 * 1024 * 1024;
const CHUNK_SIZE_BYTES: usize = 128 * 1024;

fn fixture_path() -> &'static Path {
    static PATH: OnceLock<PathBuf> = OnceLock::new();

    PATH.get_or_init(|| {
        let path = std::env::temp_dir().join("s3m-upload-hot-path-benchmark.bin");

        if !path.exists() {
            let mut data = vec![0_u8; FILE_SIZE_BYTES];
            for (i, byte) in data.iter_mut().enumerate() {
                *byte = u8::try_from(i % 251).expect("value must fit in u8");
            }
            std::fs::write(&path, data).expect("Failed to create benchmark fixture");
        }

        path
    })
    .as_path()
}

async fn read_and_report_progress(path: &Path) -> usize {
    let file = File::open(path)
        .await
        .expect("Failed to open benchmark file");
    let mut stream = FramedRead::with_capacity(file, BytesCodec::new(), CHUNK_SIZE_BYTES);

    let (sender, mut receiver) = unbounded_channel::<usize>();
    let consumer = tokio::spawn(async move {
        let mut total = 0usize;
        while let Some(bytes) = receiver.recv().await {
            total += bytes;
        }
        total
    });

    while let Some(bytes) = stream.try_next().await.expect("Failed to read chunk") {
        sender
            .send(bytes.len())
            .expect("Progress receiver dropped unexpectedly");
    }
    drop(sender);

    consumer.await.expect("Progress consumer panicked")
}

async fn read_hash_and_report_progress(path: &Path) -> (usize, usize) {
    let file = File::open(path)
        .await
        .expect("Failed to open benchmark file");
    let mut stream = FramedRead::with_capacity(file, BytesCodec::new(), CHUNK_SIZE_BYTES);

    let (sender, mut receiver) = unbounded_channel::<usize>();
    let consumer = tokio::spawn(async move {
        let mut total = 0usize;
        while let Some(bytes) = receiver.recv().await {
            total += bytes;
        }
        total
    });

    let mut sha = Context::new(&SHA256);
    let mut md5 = md5::Context::new();

    while let Some(bytes) = stream.try_next().await.expect("Failed to read chunk") {
        sha.update(&bytes);
        md5.consume(&bytes);
        sender
            .send(bytes.len())
            .expect("Progress receiver dropped unexpectedly");
    }
    drop(sender);

    let uploaded = consumer.await.expect("Progress consumer panicked");
    let _sha = sha.finish();
    let _md5 = md5.finalize();

    (uploaded, CHUNK_SIZE_BYTES)
}

fn bench_upload_hot_path(c: &mut Criterion) {
    let rt = Runtime::new().expect("Failed to create Tokio runtime");
    let path = fixture_path();

    let mut group = c.benchmark_group("upload_hot_path");
    group.throughput(Throughput::Bytes(FILE_SIZE_BYTES as u64));

    group.bench_with_input(
        BenchmarkId::new("read_progress", "16MiB"),
        &path,
        |b, path| {
            b.iter(|| rt.block_on(read_and_report_progress(path)));
        },
    );

    group.bench_with_input(
        BenchmarkId::new("read_hash_progress", "16MiB"),
        &path,
        |b, path| {
            b.iter(|| rt.block_on(read_hash_and_report_progress(path)));
        },
    );

    group.finish();
}

criterion_group!(benches, bench_upload_hot_path);
criterion_main!(benches);
