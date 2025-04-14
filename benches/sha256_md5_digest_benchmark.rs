use criterion::{
    BenchmarkId, Criterion, {criterion_group, criterion_main},
};
use s3m::s3::checksum::digest::sha256_md5_digest;
use std::path::Path;
use tokio::runtime::Runtime;

async fn bench_sha256_md5_digest(path: &Path) {
    let _ = async {
        sha256_md5_digest(path)
            .await
            .expect("Failed to compute hash");
    }
    .await;
}

pub fn from_elem(c: &mut Criterion) {
    let file = Path::new("README.md");

    if !file.exists() {
        panic!("File does not exist");
    }

    // Create a Tokio runtime manually
    let rt = Runtime::new().expect("Failed to create Tokio runtime");

    c.bench_with_input(
        BenchmarkId::new("input", file.display().to_string()),
        &file,
        |b, file| {
            b.iter(|| {
                rt.block_on(async {
                    // Call the async function within the runtime
                    bench_sha256_md5_digest(file).await
                });
            });
        },
    );
}

criterion_group!(benches, from_elem);
criterion_main!(benches);
