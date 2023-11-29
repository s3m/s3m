use criterion::{
    async_executor::FuturesExecutor,
    BenchmarkId, Criterion, {criterion_group, criterion_main},
};
use s3m::s3::checksum::digest::sha256_md5_digest;
use std::path::Path;

async fn bench_sha256_md5_digest(path: &Path) {
    let _ = async {
        sha256_md5_digest(path)
            .await
            .expect("Failed to compute hash");
    };
}

pub fn from_elem(c: &mut Criterion) {
    let file = Path::new("README.md");

    if !file.exists() {
        panic!("File does not exist");
    }

    c.bench_with_input(
        BenchmarkId::new("input", file.display().to_string()),
        &file,
        |b, file| {
            b.to_async(FuturesExecutor).iter(move || async move {
                for _ in 0..1_000_000 {
                    bench_sha256_md5_digest(file).await;
                }
            });
        },
    );
}

criterion_group!(benches, from_elem);
criterion_main!(benches);
