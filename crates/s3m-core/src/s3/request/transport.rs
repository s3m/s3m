use super::timeout::{idle_interval, stalled, timed_response};
use crate::s3::{
    endpoint_authority,
    error::{Error, Result},
};
use bytes::Bytes;
use http::{Uri, header::PROXY_AUTHORIZATION, uri::Scheme};
use http_body::{Body as HttpBody, Frame, SizeHint};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::{
    client::{
        legacy::{
            Client,
            connect::{CaptureConnection, capture_connection},
        },
        proxy::matcher::Matcher,
    },
    rt::{TokioExecutor, TokioIo, TokioTimer},
};
use reqwest::{Body, RequestBuilder, Response};
use rustls_platform_verifier::BuilderVerifierExt;
use socket::{HttpStream, Progress};
use std::{
    error::Error as StdError,
    fmt,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, PoisonError},
    task::{Context, Poll},
    time::Duration,
};
use tokio::time::{Instant, error::Elapsed, sleep_until, timeout};
use tower_service::Service;

mod proxy;
pub(super) mod socket;
pub(super) use socket::Activity;

// The AWS CLI defaults to 60 seconds for socket connect and read.
// <https://docs.aws.amazon.com/cli/latest/reference/s3/ls.html#global-options>
pub(crate) const CONNECT_TIMEOUT: Duration = Duration::from_secs(60);

#[cfg(test)]
tokio::task_local! {
    pub(crate) static TEST_CONNECT_TIMEOUT: Duration;
}

fn connect_timeout() -> Duration {
    #[cfg(test)]
    if let Ok(duration) = TEST_CONNECT_TIMEOUT.try_with(|duration| *duration) {
        return duration;
    }
    CONNECT_TIMEOUT
}

#[derive(Clone)]
pub(crate) struct Transport {
    client: Client<ConnectDeadline, UploadBody>,
    proxy: Arc<Matcher>,
}

impl fmt::Debug for Transport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Transport").finish_non_exhaustive()
    }
}

impl Transport {
    #[allow(clippy::expect_used)]
    pub(crate) fn new() -> Self {
        // Match reqwest's platform trust and TLS provider selection.
        let provider = rustls::crypto::CryptoProvider::get_default()
            .cloned()
            .unwrap_or_else(|| Arc::new(rustls::crypto::aws_lc_rs::default_provider()));
        let tls = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .and_then(BuilderVerifierExt::with_platform_verifier)
            .expect("initialize S3 TLS client")
            .with_no_client_auth();
        Self::build(tls, Matcher::from_system())
    }

    pub(crate) fn build(tls: rustls::ClientConfig, proxy: Matcher) -> Self {
        let proxy = Arc::new(proxy);
        let route = proxy::Route {
            tcp: socket::Connector::new(),
            proxy: Arc::clone(&proxy),
            proxy_tls: Arc::new(tls.clone()),
        };
        let connector = HttpsConnectorBuilder::new()
            .with_tls_config(tls)
            .https_or_http()
            .enable_http1()
            .wrap_connector(route);
        let client = Client::builder(TokioExecutor::new())
            .pool_timer(TokioTimer::new())
            .build(ConnectDeadline(connector));
        Self { client, proxy }
    }

    pub(super) async fn send(&self, request: RequestBuilder) -> Result<Response> {
        let request = request.build()?;
        let url = request.url().clone();
        let endpoint = endpoint_authority(&url).to_string();
        let transport_error = |source: hyper_util::client::legacy::Error| {
            if source.source().is_some_and(<dyn StdError>::is::<Elapsed>) {
                return stalled(&endpoint, "connect");
            }
            Error::Transport {
                endpoint: endpoint.clone(),
                source: Box::new(source),
            }
        };
        log::trace!("Sending S3 request to {endpoint}");
        let mut request: http::Request<Body> = request.try_into()?;
        let has_upload = !request.body().is_end_stream();
        let upload = Arc::new(Mutex::new(UploadProgress::new()));
        let capture = Arc::new(capture_connection(&mut request));
        let mut request = request.map(|body| UploadBody {
            body,
            upload: Arc::clone(&upload),
            capture: Arc::clone(&capture),
        });
        if request.uri().scheme() == Some(&Scheme::HTTP)
            && let Some(proxy) = self.proxy.intercept(request.uri())
            && let Some(auth) = proxy.basic_auth()
        {
            request
                .headers_mut()
                .entry(PROXY_AUTHORIZATION)
                .or_insert_with(|| {
                    let mut auth = auth.clone();
                    auth.set_sensitive(true);
                    auth
                });
        }
        let response = self.client.request(request);
        tokio::pin!(response);

        let idle = idle_interval();
        let connect = connect_timeout();
        let upload = has_upload.then_some(upload.as_ref());
        let response = tokio::select! {
            biased;
            response = &mut response => response.map_err(transport_error)?,
            phase = wait_for_idle(&capture, idle, connect, upload) => {
                if let Some(connected) = capture.connection_metadata().as_ref() {
                    connected.poison();
                }
                return Err(stalled(&endpoint, phase));
            }
        };
        Ok(timed_hyper_response(response, url, endpoint, idle))
    }
}

#[derive(Clone)]
struct ConnectDeadline(HttpsConnector<proxy::Route>);

impl Service<Uri> for ConnectDeadline {
    type Response = TokioIo<HttpStream<<HttpsConnector<proxy::Route> as Service<Uri>>::Response>>;
    type Error = Box<dyn StdError + Send + Sync>;
    type Future =
        Pin<Box<dyn Future<Output = std::result::Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        self.0.poll_ready(cx)
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        // Hyper can move this future to the background when an idle connection
        // wins checkout. Keep the same deadline through proxy and origin TLS.
        let connecting = timeout(connect_timeout(), self.0.call(uri));
        Box::pin(async move { Ok(HttpStream::new(connecting.await??)) })
    }
}

fn timed_hyper_response(
    response: http::Response<hyper::body::Incoming>,
    url: url::Url,
    endpoint: String,
    idle: Duration,
) -> Response {
    let (mut parts, body) = response.into_parts();
    let activity = parts.extensions.remove::<Arc<Activity>>();
    let response = http::Response::from_parts(parts, Body::wrap(body));
    timed_response(response, url, endpoint, idle, activity)
}

enum UploadState {
    Ready(Instant),
    WaitingForSource,
    Released,
}

struct UploadProgress {
    state: UploadState,
    written_before: Option<u64>,
}

impl UploadProgress {
    fn new() -> Self {
        Self {
            state: UploadState::Ready(Instant::now()),
            written_before: None,
        }
    }
}

struct UploadBody {
    body: Body,
    upload: Arc<Mutex<UploadProgress>>,
    capture: Arc<CaptureConnection>,
}

impl HttpBody for UploadBody {
    type Data = Bytes;
    type Error = reqwest::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<reqwest::Result<Frame<Bytes>>>> {
        let result = Pin::new(&mut self.body).poll_frame(cx);
        let mut upload = self.upload.lock().unwrap_or_else(PoisonError::into_inner);
        if result.is_pending() {
            upload.state = UploadState::WaitingForSource;
        } else if matches!(upload.state, UploadState::WaitingForSource) {
            upload.state = UploadState::Ready(Instant::now());
        }
        result
    }

    fn is_end_stream(&self) -> bool {
        if self.body.is_end_stream() {
            return true;
        }
        let mut upload = self.upload.lock().unwrap_or_else(PoisonError::into_inner);
        if upload.written_before.is_none() {
            // Hyper captures the checked-out socket before asking about the body
            // to encode the headers. Sample here before the request can write.
            upload.written_before = captured_activity(&self.capture)
                .and_then(|activity| activity.progress())
                .map(|progress| progress.written);
        }
        false
    }

    fn size_hint(&self) -> SizeHint {
        self.body.size_hint()
    }
}

impl Drop for UploadBody {
    fn drop(&mut self) {
        self.upload
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .state = UploadState::Released;
    }
}

fn captured_activity(capture: &CaptureConnection) -> Option<Arc<Activity>> {
    let mut extensions = http::Extensions::new();
    capture
        .connection_metadata()
        .as_ref()?
        .get_extras(&mut extensions);
    extensions.remove()
}

async fn wait_for_idle(
    capture: &CaptureConnection,
    idle: Duration,
    connect: Duration,
    upload: Option<&Mutex<UploadProgress>>,
) -> &'static str {
    let mut last_progress = Instant::now();
    let mut previous_socket: Option<Arc<Activity>> = None;
    let mut upload_watch = UploadWatch::default();
    let mut reconnecting = None;
    let interval = Duration::from_secs(1).min(idle / 4).min(connect / 4);
    loop {
        let now = Instant::now();
        let Some((tcp, progress)) = captured_activity(capture)
            .and_then(|tcp| tcp.progress().map(|progress| (tcp, progress)))
        else {
            let deadline = *reconnecting.get_or_insert(now) + connect;
            if now >= deadline {
                return "connect";
            }
            sleep_until(deadline.min(now + interval)).await;
            continue;
        };
        if !previous_socket
            .as_ref()
            .is_some_and(|prev| Arc::ptr_eq(prev, &tcp))
        {
            last_progress = now;
            upload_watch = UploadWatch::default();
            previous_socket = Some(Arc::clone(&tcp));
        }
        reconnecting = None;
        if let Some(io) = progress.read_at.max(progress.written_at)
            && io > last_progress
        {
            last_progress = io;
            upload_watch.recovered_at = None;
        }
        let (phase, deadline) =
            upload.map_or(("response headers", last_progress + idle), |upload| {
                let upload = upload.lock().unwrap_or_else(PoisonError::into_inner);
                upload_watch.deadline(&upload, &tcp, progress, idle, now, &mut last_progress)
            });
        if now >= deadline {
            return phase;
        }
        sleep_until(deadline.min(now + interval)).await;
    }
}

#[derive(Default)]
struct UploadWatch {
    previous_queue: Option<u64>,
    sample_missing: bool,
    recovered_at: Option<Instant>,
    drained_at: Option<u64>,
}

impl UploadWatch {
    fn deadline(
        &mut self,
        upload: &UploadProgress,
        tcp: &Activity,
        progress: Progress,
        idle: Duration,
        now: Instant,
        last_progress: &mut Instant,
    ) -> (&'static str, Instant) {
        // Once the upload is drained, TLS control writes cannot put the
        // request back in the upload phase. More HTTP writes reopen the check.
        let queue = if self.drained_at == Some(progress.written) {
            Some(0)
        } else {
            tcp.queued()
        };
        if let Some(bytes) = queue {
            if self.previous_queue.is_some_and(|before| bytes < before) {
                *last_progress = now;
                self.recovered_at = None;
            } else if self.sample_missing {
                // Give the recovered baseline time to decrease. Repeated
                // sampling failures alone cannot renew this grace period.
                self.recovered_at.get_or_insert(now);
            }
        }
        self.previous_queue = queue;
        self.sample_missing = queue.is_none();

        let written = upload
            .written_before
            .map_or(0, |before| progress.written.saturating_sub(before));
        let phase = match upload.state {
            UploadState::Released if queue == Some(0) => {
                if written > 0 {
                    self.drained_at = Some(progress.written);
                }
                "response headers"
            }
            UploadState::WaitingForSource if queue.is_none_or(|bytes| bytes == 0) => {
                // Waiting for local input or pacing is outside the network deadline.
                *last_progress = now;
                "upload"
            }
            UploadState::Ready(since) => {
                *last_progress = (*last_progress).max(since);
                "upload"
            }
            _ => "upload",
        };
        let may_be_draining = match upload.state {
            UploadState::Ready(_) => written > 0,
            UploadState::Released => true,
            UploadState::WaitingForSource => false,
        };
        let budget = if queue.is_none() && may_be_draining {
            // TCP can deliver buffered bytes while a streaming write is
            // blocked, as well as after Hyper has released the body.
            let tail = Duration::from_secs(tcp.possible_tail_bytes(written).div_ceil(1024));
            (idle * 5 + tail).min(Duration::from_mins(15))
        } else {
            idle
        };
        // An unavailable sample already has a separate allowance. Apply the
        // recovery grace only while a sample is available.
        let start = if queue.is_some() {
            self.recovered_at
                .map_or(*last_progress, |at| at.max(*last_progress))
        } else {
            *last_progress
        };
        (phase, start + budget)
    }
}
