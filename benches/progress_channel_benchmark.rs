#![allow(clippy::expect_used, clippy::missing_panics_doc)]

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use tokio::{runtime::Runtime, sync::mpsc::unbounded_channel};

fn bench_progress_channel(c: &mut Criterion) {
    let rt = Runtime::new().expect("Failed to create Tokio runtime");

    let mut group = c.benchmark_group("progress_channel");
    for messages in [1_000_usize, 10_000, 100_000] {
        group.bench_with_input(
            BenchmarkId::from_parameter(messages),
            &messages,
            |b, messages| {
                b.iter(|| {
                    rt.block_on(async {
                        let (sender, mut receiver) = unbounded_channel::<usize>();

                        let consumer = tokio::spawn(async move {
                            let mut total = 0usize;
                            while let Some(bytes) = receiver.recv().await {
                                total += bytes;
                            }
                            total
                        });

                        for _ in 0..*messages {
                            sender.send(128 * 1024).expect("progress receiver dropped");
                        }
                        drop(sender);

                        consumer.await.expect("progress consumer panicked")
                    });
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_progress_channel);
criterion_main!(benches);
