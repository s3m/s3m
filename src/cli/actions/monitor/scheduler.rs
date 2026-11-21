use futures::{Stream, StreamExt};
use std::future::Future;

pub(super) async fn collect<F: Future>(
    checks: impl Stream<Item = F>,
    number: u8,
) -> Vec<F::Output> {
    checks
        .buffer_unordered(usize::from(number.max(1)))
        .collect()
        .await
}

#[cfg(test)]
mod tests {
    use super::collect;
    use anyhow::{Result, anyhow};
    use futures::{FutureExt, StreamExt, channel::oneshot, stream};
    use std::{cell::Cell, pin::pin};

    #[test]
    fn monitor_scheduler_applies_number_before_polling_checks() -> Result<()> {
        for (number, expected) in [(0, 1), (1, 1), (2, 2)] {
            let started = Cell::new(0);
            let (first, first_ready) = oneshot::channel::<()>();
            let (second, second_ready) = oneshot::channel::<()>();
            let checks = stream::iter([first_ready, second_ready]).map(|ready| async {
                started.set(started.get() + 1);
                ready.await
            });
            let mut results = pin!(collect(checks, number));

            // A full poll reaches Pending with the admitted checks held by
            // independent gates. No HTTP or OS scheduling races this count.
            assert!(results.as_mut().now_or_never().is_none());
            assert_eq!(started.get(), expected, "number={number}");

            first
                .send(())
                .map_err(|()| anyhow!("first check dropped"))?;
            second
                .send(())
                .map_err(|()| anyhow!("second check dropped"))?;
            let completed = results
                .as_mut()
                .now_or_never()
                .ok_or_else(|| anyhow!("released checks did not complete"))?;
            assert_eq!(completed.len(), 2);
            for result in completed {
                result?;
            }
        }
        Ok(())
    }
}
