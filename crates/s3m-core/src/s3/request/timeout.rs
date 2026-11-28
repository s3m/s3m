use super::transport::Activity;
use crate::s3::error::Error;
use bytes::Bytes;
use http_body::{Body as HttpBody, Frame, SizeHint};
use reqwest::{Body, Response, ResponseBuilderExt};
use std::{
    future::Future,
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tokio::time::{Instant, Sleep, sleep_until};

const IDLE_TIMEOUT: Duration = Duration::from_secs(60);

#[cfg(test)]
tokio::task_local! {
    pub(crate) static TEST_IDLE_TIMEOUT: Duration;
}

pub(super) fn idle_interval() -> Duration {
    #[cfg(test)]
    if let Ok(duration) = TEST_IDLE_TIMEOUT.try_with(|duration| *duration) {
        return duration;
    }
    IDLE_TIMEOUT
}

pub(super) fn stalled(endpoint: &str, phase: &'static str) -> Error {
    Error::ResponseTimeout {
        endpoint: endpoint.to_string(),
        phase,
    }
}

pub(super) fn timed_response(
    response: http::Response<Body>,
    url: url::Url,
    endpoint: String,
    idle: Duration,
    activity: Option<Arc<Activity>>,
) -> Response {
    let mut builder = http::Response::builder().url(url);
    let (mut parts, body) = response.into_parts();
    // Reqwest stores the URL separately from the HTTP response parts.
    if let Some(url_extension) = builder.extensions_mut() {
        parts.extensions.extend(std::mem::take(url_extension));
    }
    Response::from(http::Response::from_parts(
        parts,
        Body::wrap(IdleBody {
            body,
            endpoint,
            idle,
            activity,
            timer: None,
        }),
    ))
}

struct IdleBody {
    body: Body,
    endpoint: String,
    idle: Duration,
    activity: Option<Arc<Activity>>,
    timer: Option<Pin<Box<Sleep>>>,
}

impl HttpBody for IdleBody {
    type Data = Bytes;
    type Error = Box<dyn std::error::Error + Send + Sync>;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<std::result::Result<Frame<Bytes>, Self::Error>>> {
        if let Poll::Ready(frame) = Pin::new(&mut self.body).poll_frame(cx) {
            self.timer = None;
            return Poll::Ready(frame.map(|frame| frame.map_err(Into::into)));
        }
        // Start the idle interval only while the caller is waiting for data.
        let idle = self.idle;
        let transport_deadline = self
            .activity
            .as_deref()
            .and_then(Activity::last_read)
            .map(|io| io + idle);
        let timer = self
            .timer
            .get_or_insert_with(|| Box::pin(sleep_until(Instant::now() + idle)));
        if let Some(deadline) = transport_deadline
            && deadline > timer.deadline()
        {
            timer.as_mut().reset(deadline);
        }
        if timer.as_mut().poll(cx).is_pending() {
            return Poll::Pending;
        }
        let error = io::Error::new(
            io::ErrorKind::TimedOut,
            stalled(&self.endpoint, "response body"),
        );
        Poll::Ready(Some(Err(error.into())))
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.body.size_hint()
    }
}
