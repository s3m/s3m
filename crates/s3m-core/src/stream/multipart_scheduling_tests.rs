use super::{Bar, await_tasks};
use anyhow::{Result, anyhow};
use futures::{FutureExt, channel::oneshot, stream::FuturesUnordered};
use std::pin::pin;

#[test]
fn multipart_scheduler_holds_a_full_slot_until_its_request_finishes() -> Result<()> {
    for max_requests in [1, 2] {
        let (release, held) = oneshot::channel::<()>();
        let mut tasks = FuturesUnordered::new();
        tasks.push(async { held.await.map(|()| 1).map_err(anyhow::Error::from) });
        let progress = Bar::default();
        let mut admission = pin!(await_tasks(&mut tasks, &progress, 5, max_requests));

        let first_poll = admission.as_mut().now_or_never();
        if max_requests == 1 {
            assert!(first_poll.is_none(), "a full slot must block admission");
            release
                .send(())
                .map_err(|()| anyhow!("held request dropped"))?;
            admission
                .as_mut()
                .now_or_never()
                .ok_or_else(|| anyhow!("finished request did not release its slot"))??;
        } else {
            first_poll.ok_or_else(|| anyhow!("a spare slot blocked admission"))??;
            release
                .send(())
                .map_err(|()| anyhow!("queued request dropped"))?;
        }
    }
    Ok(())
}
