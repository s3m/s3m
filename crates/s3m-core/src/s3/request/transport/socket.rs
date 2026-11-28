use hyper_util::{
    client::legacy::connect::{Connected, Connection, HttpConnector},
    rt::TokioIo,
};
use socket2::{SockRef, Socket};
use std::{
    future::Future,
    io::{self, IoSlice},
    pin::Pin,
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpStream,
    time::Instant,
};
use tower_service::Service;

#[cfg(test)]
pub(crate) mod test_dns;

#[cfg(test)]
tokio::task_local! {
    pub(crate) static TEST_QUEUE_UNAVAILABLE: ();
    pub(super) static TEST_QUEUE_SAMPLE: std::cell::Cell<Option<u64>>;
}

#[derive(Clone)]
pub(super) struct Connector {
    #[cfg(not(test))]
    http: HttpConnector,
    #[cfg(test)]
    http: HttpConnector<test_dns::Resolver>,
    pub forward: bool,
}

impl Connector {
    pub(super) fn new() -> Self {
        #[cfg(not(test))]
        let mut http = HttpConnector::new();
        #[cfg(test)]
        let mut http = HttpConnector::new_with_resolver(test_dns::Resolver::new());
        http.enforce_http(false);
        http.set_nodelay(true);
        Self {
            http,
            forward: false,
        }
    }
}

impl Service<http::Uri> for Connector {
    type Response = TokioIo<TrackedTcp>;
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output = io::Result<Self::Response>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, uri: http::Uri) -> Self::Future {
        let connecting = self.http.call(uri);
        let forward = self.forward;
        Box::pin(async move {
            let socket = connecting.await.map_err(io::Error::other)?.into_inner();
            let monitor = SockRef::from(&socket).try_clone().ok();
            Ok(TokioIo::new(TrackedTcp {
                socket,
                forward,
                activity: Arc::new(Activity {
                    monitor: Mutex::new(monitor),
                    progress: Mutex::new(Some(Progress::default())),
                    socket_written: AtomicU64::new(0),
                    #[cfg(test)]
                    stale: std::sync::atomic::AtomicBool::new(false),
                }),
            }))
        })
    }
}

pub(in crate::s3::request) struct Activity {
    monitor: Mutex<Option<Socket>>,
    progress: Mutex<Option<Progress>>,
    socket_written: AtomicU64,
    #[cfg(test)]
    pub(super) stale: std::sync::atomic::AtomicBool,
}

#[derive(Clone, Copy, Default)]
pub(super) struct Progress {
    pub read_at: Option<Instant>,
    pub written_at: Option<Instant>,
    pub written: u64,
}

impl Activity {
    pub(in crate::s3::request) fn last_read(&self) -> Option<Instant> {
        self.progress().and_then(|progress| progress.read_at)
    }

    pub(super) fn progress(&self) -> Option<Progress> {
        *self.progress.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn update_progress(&self, update: impl FnOnce(&mut Progress)) {
        if let Some(progress) = self
            .progress
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_mut()
        {
            update(progress);
        }
    }

    pub(super) fn queued(&self) -> Option<u64> {
        #[cfg(test)]
        if let Ok(sample) = TEST_QUEUE_SAMPLE.try_with(std::cell::Cell::get) {
            return sample;
        }
        #[cfg(test)]
        if TEST_QUEUE_UNAVAILABLE.try_with(|()| ()).is_ok() {
            return None;
        }
        self.monitor
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .and_then(|socket| queued(socket, self.socket_written.load(Ordering::Relaxed)))
    }

    pub(super) fn possible_tail_bytes(&self, written: u64) -> u64 {
        let capacity = self
            .monitor
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .and_then(|socket| socket.send_buffer_size().ok());
        capacity.map_or(written, |capacity| written.min(capacity as u64))
    }
}

/// Attributes socket writes to HTTP output for the per-request drain estimate.
pub(super) struct HttpStream<T> {
    io: TokioIo<T>,
    activity: Option<Arc<Activity>>,
    flushing_http: bool,
}

impl<T: Connection> HttpStream<T> {
    pub(super) fn new(io: T) -> TokioIo<Self> {
        let mut extensions = http::Extensions::new();
        io.connected().get_extras(&mut extensions);
        TokioIo::new(Self {
            io: TokioIo::new(io),
            activity: extensions.remove(),
            flushing_http: false,
        })
    }
}

impl<T: Connection> Connection for HttpStream<T> {
    fn connected(&self) -> Connected {
        self.io.connected()
    }
}

impl<T: hyper::rt::Read + Unpin> AsyncRead for HttpStream<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_read(cx, buffer)
    }
}

impl<T: hyper::rt::Write + Unpin> HttpStream<T> {
    fn track_write<R>(
        &mut self,
        poll: impl FnOnce(Pin<&mut TokioIo<T>>) -> Poll<io::Result<R>>,
    ) -> Poll<io::Result<R>> {
        let io = Pin::new(&mut self.io);
        let Some(activity) = self.activity.as_ref().filter(|_| self.flushing_http) else {
            return poll(io);
        };
        let before = activity.socket_written.load(Ordering::Relaxed);
        let result = poll(io);
        let bytes = activity.socket_written.load(Ordering::Relaxed) - before;
        if bytes > 0 {
            activity.update_progress(|progress| progress.written += bytes);
        }
        result
    }
}

impl<T: hyper::rt::Write + Unpin> AsyncWrite for HttpStream<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.flushing_http |= !bytes.is_empty();
        self.track_write(|io| io.poll_write(cx, bytes))
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.flushing_http |= bytes.iter().any(|slice| !slice.is_empty());
        self.track_write(|io| io.poll_write_vectored(cx, bytes))
    }

    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let result = self.track_write(|io| io.poll_flush(cx));
        if result.is_ready() {
            self.flushing_http = false;
        }
        result
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}

pub(super) struct TrackedTcp {
    socket: TcpStream,
    forward: bool,
    activity: Arc<Activity>,
}

impl TrackedTcp {
    fn track_write(&self, result: Poll<io::Result<usize>>) -> Poll<io::Result<usize>> {
        if let Poll::Ready(Ok(bytes)) = result
            && bytes > 0
        {
            self.activity
                .socket_written
                .fetch_add(bytes as u64, Ordering::Relaxed);
            self.activity
                .update_progress(|progress| progress.written_at = Some(Instant::now()));
        }
        result
    }
}

impl Drop for TrackedTcp {
    fn drop(&mut self) {
        *self
            .activity
            .progress
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = None;
        // The duplicate is for querying statistics only. It must not keep a
        // closed or canceled HTTP connection alive.
        self.activity
            .monitor
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
    }
}

impl Connection for TrackedTcp {
    fn connected(&self) -> Connected {
        self.socket
            .connected()
            .proxy(self.forward)
            .extra(Arc::clone(&self.activity))
    }
}

impl AsyncRead for TrackedTcp {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        // Deliver EOF exactly when Hyper polls the checked-out connection.
        #[cfg(test)]
        if self.activity.stale.swap(false, Ordering::Relaxed) {
            return Poll::Ready(Ok(()));
        }
        let before = buffer.filled().len();
        let result = Pin::new(&mut self.socket).poll_read(cx, buffer);
        if buffer.filled().len() > before {
            // TLS can wait for the rest of a record before returning any HTTP data.
            self.activity
                .update_progress(|progress| progress.read_at = Some(Instant::now()));
        }
        result
    }
}

impl AsyncWrite for TrackedTcp {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let result = Pin::new(&mut self.socket).poll_write(cx, bytes);
        self.track_write(result)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let result = Pin::new(&mut self.socket).poll_write_vectored(cx, bytes);
        self.track_write(result)
    }

    fn is_write_vectored(&self) -> bool {
        self.socket.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket).poll_shutdown(cx)
    }
}

// Socket writes can return before TCP sends or acknowledges the bytes. A
// shrinking queue is progress even when Hyper has released the request body.
#[cfg(any(target_os = "linux", target_os = "freebsd"))]
fn queued(socket: &Socket, _: u64) -> Option<u64> {
    use std::os::fd::AsRawFd as _;
    let mut bytes = 0_i32;
    #[cfg(target_os = "linux")]
    let request = libc::TIOCOUTQ;
    #[cfg(target_os = "freebsd")]
    let request = libc::FIONWRITE;
    // SAFETY: Both ioctls take a writable int on a live socket.
    let result = unsafe { libc::ioctl(socket.as_raw_fd(), request, &raw mut bytes) };
    (result == 0).then(|| u64::try_from(bytes).ok()).flatten()
}

#[cfg(target_os = "macos")]
fn queued(socket: &Socket, _: u64) -> Option<u64> {
    use std::{mem, os::fd::AsRawFd as _};
    let mut bytes = 0_i32;
    let size = libc::socklen_t::try_from(mem::size_of_val(&bytes)).ok()?;
    let mut length = size;
    // SAFETY: SO_NWRITE takes a writable int and socklen_t on a live socket.
    let result = unsafe {
        libc::getsockopt(
            socket.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_NWRITE,
            (&raw mut bytes).cast(),
            &raw mut length,
        )
    };
    (result == 0 && length == size)
        .then(|| u64::try_from(bytes).ok())
        .flatten()
}

#[cfg(windows)]
fn queued(socket: &Socket, written: u64) -> Option<u64> {
    use std::{mem, os::windows::io::AsRawSocket as _, ptr};
    use windows_sys::Win32::Networking::WinSock::{SIO_TCP_INFO, TCP_INFO_v0, WSAIoctl};

    let socket = usize::try_from(socket.as_raw_socket()).ok()?;
    let version = 0_u32;
    let mut info = TCP_INFO_v0::default();
    let size = u32::try_from(mem::size_of_val(&info)).ok()?;
    let mut length = 0;
    // SAFETY: the input and output buffers match SIO_TCP_INFO version 0.
    let result = unsafe {
        WSAIoctl(
            socket,
            SIO_TCP_INFO,
            (&raw const version).cast(),
            4,
            (&raw mut info).cast(),
            size,
            &raw mut length,
            ptr::null_mut(),
            None,
        )
    };
    if result != 0 || length != size {
        return None;
    }
    // Discount retransmissions so they cannot be mistaken for acknowledgements
    // if Windows includes them in BytesOut.
    let acknowledged = info
        .BytesOut
        .saturating_sub(u64::from(info.BytesInFlight) + u64::from(info.BytesRetrans));
    written.checked_sub(acknowledged)
}

#[cfg(not(any(
    target_os = "linux",
    target_os = "macos",
    target_os = "freebsd",
    windows
)))]
fn queued(_: &Socket, _: u64) -> Option<u64> {
    None
}

#[cfg(test)]
mod tests {
    use super::{
        super::{UploadProgress, UploadState, UploadWatch},
        *,
    };
    use std::time::Duration;

    #[test]
    fn timeout_fallback_caps_a_stalled_streaming_put() {
        let body_ready = Instant::now();
        let last_write = body_ready + Duration::from_secs(30);
        let progress = Progress {
            written_at: Some(last_write),
            written: 1024 * 1024,
            ..Progress::default()
        };
        // A ready body stalled after a partial write. With no socket statistics,
        // the 1 MiB tail makes the uncapped allowance longer than 15 minutes.
        let activity = Activity {
            monitor: Mutex::new(None),
            progress: Mutex::new(Some(progress)),
            socket_written: AtomicU64::new(progress.written),
            stale: std::sync::atomic::AtomicBool::new(false),
        };
        let upload = UploadProgress {
            state: UploadState::Ready(body_ready),
            written_before: Some(0),
        };
        let mut last_progress = last_write;
        let (phase, deadline) = UploadWatch::default().deadline(
            &upload,
            &activity,
            progress,
            Duration::from_secs(60),
            last_write + Duration::from_secs(5),
            &mut last_progress,
        );
        assert_eq!(phase, "upload");
        assert_eq!(
            deadline - last_write,
            Duration::from_mins(15),
            "a stalled streaming PUT must use the fallback cap from its last write"
        );
    }
}
