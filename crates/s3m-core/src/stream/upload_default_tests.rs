use super::{UploadRequest, upload};
use crate::{
    s3::{
        Credentials, Error, Region, RequestOptions, S3,
        actions::PutObject,
        request::{TEST_IDLE_TIMEOUT, TEST_QUEUE_UNAVAILABLE, TEST_RESOLVE, Transport},
    },
    stream::db::Db,
};
use anyhow::{Context, Result, ensure};
use rustls::{ClientConfig, RootCertStore, ServerConfig, pki_types::PrivatePkcs8KeyDer};
use secrecy::SecretString;
use std::{
    cell::Cell,
    io::{self, Write as _},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, UNIX_EPOCH},
};
use tempfile::TempDir;
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::mpsc::unbounded_channel,
    time::{Instant, timeout},
};
use tokio_rustls::TlsAcceptor;

const ETAG: &str = "\"stored\"";
const KEY: &str = "key";
const DEADLINE: Duration = Duration::from_secs(10);
const READ_IDLE: Duration = Duration::from_millis(500);
const STALLED_SERVICE_UNAVAILABLE: &[u8] =
    b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 100\r\n\r\n<Error>";

struct Fixture {
    _dir: TempDir,
    file: PathBuf,
    file_blake3: String,
    size: u64,
    s3: S3,
    db: Db,
}

impl Fixture {
    fn new(endpoint: String, body: &[u8]) -> Result<Self> {
        let dir = tempfile::tempdir()?;
        let file = dir.path().join("body");
        std::fs::write(&file, body)?;
        let file_blake3 = blake3::hash(body).to_hex().to_string();
        let s3 = S3::new(
            &Credentials::new("test-access", &SecretString::new("test-secret".into())),
            &Region::custom("us-east-1", endpoint),
            Some("bucket".into()),
            false,
        );
        let db = Db::new(&s3, KEY, &file_blake3, 0, dir.path())?;
        Ok(Self {
            _dir: dir,
            file,
            file_blake3,
            size: body.len() as u64,
            s3,
            db,
        })
    }

    fn request(&self) -> UploadRequest<'_> {
        UploadRequest {
            s3: &self.s3,
            key: KEY,
            file: &self.file,
            file_size: self.size,
            file_mtime: None,
            file_blake3: &self.file_blake3,
            sdb: &self.db,
            acl: None,
            meta: None,
            quiet: true,
            additional_checksum: None,
            globals: RequestOptions {
                retries: 2,
                ..RequestOptions::default()
            },
        }
    }
}

async fn read_put_headers(listener: &TcpListener) -> Result<BufReader<TcpStream>> {
    let (socket, _) = listener.accept().await?;
    read_headers(socket).await
}

/// A loopback listener whose accepted sockets advertise a small receive window, so an
/// upload to a slow reader waits in the client's send queue, where s3m sees it drain.
/// Without it, macOS loopback buffers a whole MiB on the receiving side at once, and the
/// client can't tell a slow reader from a stuck one (the documented limit).
fn small_window_listener() -> Result<TcpListener> {
    let socket = socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::STREAM, None)?;
    socket.set_recv_buffer_size(4096)?;
    socket.bind(&std::net::SocketAddr::from(([127, 0, 0, 1], 0)).into())?;
    socket.listen(16)?;
    socket.set_nonblocking(true)?;
    Ok(TcpListener::from_std(socket.into())?)
}

async fn read_headers<T: AsyncRead + Unpin>(socket: T) -> Result<BufReader<T>> {
    let mut socket = BufReader::new(socket);
    let mut line = String::new();
    socket.read_line(&mut line).await?;
    assert_eq!(line, "PUT /bucket/key HTTP/1.1\r\n");
    loop {
        line.clear();
        ensure!(
            socket.read_line(&mut line).await? != 0,
            "incomplete PUT headers"
        );
        if line == "\r\n" {
            return Ok(socket);
        }
    }
}

async fn reply_stored(mut socket: impl AsyncWrite + Unpin) -> Result<()> {
    let reply = format!("HTTP/1.1 200 OK\r\nETag: {ETAG}\r\nContent-Length: 0\r\n\r\n");
    socket.write_all(reply.as_bytes()).await?;
    Ok(())
}

async fn read_test_put(listener: &TcpListener) -> Result<BufReader<TcpStream>> {
    let mut socket = read_put_headers(listener).await?;
    let mut bytes = [0; 4];
    socket.read_exact(&mut bytes).await?;
    assert_eq!(&bytes, b"body");
    Ok(socket)
}

async fn retry_after_response_stalls(reply: Option<&'static [u8]>) -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let fixture = Fixture::new(format!("http://{}", listener.local_addr()?), b"body")?;
    let server = tokio::spawn(async move {
        let mut first = read_test_put(&listener).await?;
        if let Some(reply) = reply {
            first.write_all(reply).await?;
        }
        let second = read_test_put(&listener).await?;
        reply_stored(second).await?;
        drop(first);
        Ok::<_, anyhow::Error>(())
    });

    let result = timeout(
        DEADLINE,
        TEST_IDLE_TIMEOUT.scope(READ_IDLE, upload(fixture.request())),
    )
    .await
    .context("PUT must retry a stalled response before the test watchdog")
    .and_then(|result| result);
    if result.is_err() {
        server.abort();
    }
    let report = result?;
    server.await??;
    assert_eq!(report, format!("ETag: {ETAG}\n"));
    assert_eq!(fixture.db.cached_etag()?.as_deref(), Some(ETAG));
    Ok(())
}

#[tokio::test]
async fn timeout_retries_put_when_the_server_sends_no_headers() -> Result<()> {
    retry_after_response_stalls(None).await
}

#[tokio::test]
async fn timeout_retries_put_when_a_503_body_stalls() -> Result<()> {
    retry_after_response_stalls(Some(STALLED_SERVICE_UNAVAILABLE)).await
}

#[tokio::test]
async fn timeout_preserves_the_status_of_a_stalled_error_body() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = listener.local_addr()?;
    let fixture = Fixture::new(format!("http://{endpoint}"), b"body")?;
    let server = async {
        let mut socket = read_test_put(&listener).await?;
        socket.write_all(STALLED_SERVICE_UNAVAILABLE).await?;
        std::future::pending::<Result<()>>().await
    };
    let mut request = fixture.request();
    request.globals.retries = 1;
    let result = tokio::select! {
        result = timeout(DEADLINE, TEST_IDLE_TIMEOUT.scope(READ_IDLE, upload(request))) => {
            result.context("a stalled 503 body must fail before the test watchdog")?
        }
        result = server => { result?; anyhow::bail!("the server stopped stalling the body") }
    };
    let error = result.err().context("the PUT must fail with HTTP 503")?;
    assert_eq!(
        error.downcast_ref::<Error>().and_then(Error::status),
        Some(503),
        "{error}"
    );
    assert!(
        error
            .to_string()
            .contains(&format!("Timed out reading response body from {endpoint}")),
        "the stalled error body must identify its endpoint and port: {error}"
    );
    assert!(fixture.db.cached_etag()?.is_none());
    Ok(())
}

#[tokio::test]
async fn timeout_allows_a_slow_receiver_to_drain_the_buffered_put_tail() -> Result<()> {
    slow_receiver(
        1024 * 1024,
        1024,
        Duration::from_millis(5),
        Duration::from_secs(2),
    )
    .await
}

#[tokio::test]
async fn timeout_allows_a_slow_receiver_without_queue_statistics() -> Result<()> {
    TEST_QUEUE_UNAVAILABLE
        .scope(
            (),
            slow_receiver(48 * 1024, 512, Duration::from_millis(40), READ_IDLE),
        )
        .await
        .context("a failed queue sample must allow a slowly draining upload to finish")
}

async fn slow_receiver(size: usize, chunk: usize, delay: Duration, idle: Duration) -> Result<()> {
    let listener = small_window_listener()?;
    let payload = vec![42; size];
    let fixture = Fixture::new(format!("http://{}", listener.local_addr()?), &payload)?;
    let server = tokio::spawn(async move {
        let mut socket = read_put_headers(&listener).await?;
        let mut block = vec![0; chunk];
        for expected in payload.chunks_exact(chunk) {
            tokio::time::sleep(delay).await;
            socket.read_exact(&mut block).await?;
            assert_eq!(block, expected);
        }
        reply_stored(socket).await
    });

    let start = Instant::now();
    let mut request = fixture.request();
    request.globals.retries = 1;
    let result = timeout(
        Duration::from_secs(30),
        TEST_IDLE_TIMEOUT.scope(idle, upload(request)),
    )
    .await
    .context("the slow loopback receiver must finish within the watchdog")
    .and_then(|result| result);
    if result.is_err() {
        server.abort();
    }
    let report = result.context("TCP send queue progress must keep the PUT alive")?;
    server.await??;
    assert!(start.elapsed() > idle);
    assert_eq!(report, format!("ETag: {ETAG}\n"));
    Ok(())
}

#[tokio::test]
async fn timeout_retries_a_put_when_the_receiver_stops_reading() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let payload = vec![42; 16 * 1024 * 1024];
    let fixture = Fixture::new(format!("http://{}", listener.local_addr()?), &payload)?;
    let server = tokio::spawn(async move {
        // Keep the first TCP connection open without consuming its request body.
        let first = read_put_headers(&listener).await?;
        let mut second = read_put_headers(&listener).await?;
        let mut received = vec![0; payload.len()];
        second.read_exact(&mut received).await?;
        assert!(received == payload, "the retry must send the entire body");
        reply_stored(second).await?;
        drop(first);
        Ok::<_, anyhow::Error>(())
    });

    let result = timeout(
        DEADLINE,
        TEST_IDLE_TIMEOUT.scope(Duration::from_secs(1), upload(fixture.request())),
    )
    .await
    .context("a backpressured PUT must time out and retry before the watchdog")
    .and_then(|result| result);
    if result.is_err() {
        server.abort();
    }
    let report = result?;
    server.await??;
    assert_eq!(report, format!("ETag: {ETAG}\n"));
    assert_eq!(fixture.db.cached_etag()?.as_deref(), Some(ETAG));
    Ok(())
}

#[tokio::test]
async fn timeout_allows_a_throttled_put_longer_than_the_response_idle_interval() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let body = vec![42; 384 * 1024];
    let fixture = Fixture::new(format!("http://{}", listener.local_addr()?), &body)?;
    let server = tokio::spawn(async move {
        let mut socket = read_put_headers(&listener).await?;
        let mut received = vec![0; body.len()];
        socket.read_exact(&mut received).await?;
        assert_eq!(received, body);
        reply_stored(socket).await
    });
    let mut request = fixture.request();
    request.globals.retries = 1;
    // 128 KiB frames at 128 KiB/s leave one second between frames.
    request.globals.throttle = Some(128);
    let start = Instant::now();
    let report = timeout(
        DEADLINE,
        TEST_IDLE_TIMEOUT.scope(READ_IDLE, upload(request)),
    )
    .await?
    .context("a paced request body must finish before the response timer starts")?;
    server.await??;
    assert!(start.elapsed() > READ_IDLE);
    assert_eq!(report, format!("ETag: {ETAG}\n"));
    Ok(())
}

#[tokio::test]
async fn redirects_do_not_follow_put_as_get_or_cache_an_etag() -> Result<()> {
    let mut server = mockito::Server::new_async().await;
    let fixture = Fixture::new(server.url(), b"body")?;
    let redirect = server
        .mock("PUT", "/bucket/key")
        .with_status(303)
        .with_header("location", "/receipt")
        .with_header("etag", "\"redirect\"")
        .create_async()
        .await;
    let receipt = server
        .mock("GET", "/receipt")
        .with_status(200)
        .with_header("etag", "\"receipt\"")
        .expect(0)
        .create_async()
        .await;

    let result = upload(fixture.request()).await;
    let cached = fixture.db.cached_etag()?;
    assert!(
        result.is_err() && cached.is_none(),
        "a PUT redirect must fail without caching an ETag: result={result:?}, cached={cached:?}"
    );
    let error = result.err().context("the PUT redirect must fail")?;
    assert!(error.to_string().contains("303"), "{error}");
    assert!(
        error.to_string().contains("destination is /receipt"),
        "{error}"
    );
    redirect.assert_async().await;
    receipt.assert_async().await;
    Ok(())
}

async fn rejected_redirect(status: usize, code: &str) -> Result<()> {
    let mut server = mockito::Server::new_async().await;
    let fixture = Fixture::new(server.url(), b"body")?;
    let redirect = server
        .mock("PUT", "/bucket/key")
        .with_status(status)
        .with_body(format!(
            "<Error><Code>{code}</Code><Endpoint>bucket.example.test</Endpoint></Error>"
        ))
        .create_async()
        .await;

    let error = upload(fixture.request())
        .await
        .err()
        .context("a PUT redirect must fail")?;
    redirect.assert_async().await;
    let message = error.to_string();
    assert!(message.contains(&status.to_string()), "{message}");
    assert!(message.contains(code), "{message}");
    assert!(message.contains("is bucket.example.test"), "{message}");
    assert_eq!(
        message.contains("Update the host's endpoint or region"),
        status == 301,
        "{message}"
    );
    assert_eq!(
        message.contains("for this request"),
        status == 307,
        "{message}"
    );
    assert!(fixture.db.cached_etag()?.is_none());
    Ok(())
}

#[tokio::test]
async fn redirects_report_the_permanent_endpoint_in_s3_xml() -> Result<()> {
    rejected_redirect(301, "PermanentRedirect").await
}

#[tokio::test]
async fn redirects_report_the_temporary_endpoint_in_s3_xml() -> Result<()> {
    rejected_redirect(307, "TemporaryRedirect").await
}

#[tokio::test]
async fn redirects_are_permanent_even_with_a_transient_s3_code() -> Result<()> {
    rejected_redirect(307, "RequestTimeout").await
}

#[tokio::test]
async fn a_temporary_dns_failure_retries_the_put() -> Result<()> {
    let mut server = mockito::Server::new_async().await;
    let address = server.socket_address();
    let fixture = Fixture::new(format!("http://retry.invalid:{}", address.port()), b"body")?;
    let lookups = Arc::new(AtomicUsize::new(0));
    let attempts = Arc::clone(&lookups);
    let resolve = Box::new(
        move |name: &hyper_util::client::legacy::connect::dns::Name| {
            assert_eq!(name.as_str(), "retry.invalid");
            if attempts.fetch_add(1, Ordering::Relaxed) == 0 {
                // The system resolver reports EAI_AGAIN as a wrapped I/O error.
                return Err(io::Error::other("Temporary failure in name resolution"));
            }
            Ok(address)
        },
    );
    let stored = server
        .mock("PUT", "/bucket/key")
        .match_body("body")
        .with_status(200)
        .with_header("etag", ETAG)
        .expect(1)
        .create_async()
        .await;

    let result = timeout(
        DEADLINE,
        TEST_RESOLVE.scope(resolve, upload(fixture.request())),
    )
    .await?;
    assert_eq!(
        lookups.load(Ordering::Relaxed),
        2,
        "temporary DNS failure must retry: {result:?}"
    );
    assert_eq!(result?, format!("ETag: {ETAG}\n"));
    stored.assert_async().await;
    Ok(())
}

fn test_tls() -> Result<(TlsAcceptor, ClientConfig)> {
    let rcgen::CertifiedKey { cert, signing_key } =
        rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_string()])?;
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let mut roots = RootCertStore::empty();
    roots.add(cert.der().clone())?;
    let server = ServerConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()?
        .with_no_client_auth()
        .with_single_cert(
            vec![cert.der().clone()],
            PrivatePkcs8KeyDer::from(signing_key.serialize_der()).into(),
        )?;
    let client = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()?
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok((TlsAcceptor::from(Arc::new(server)), client))
}

#[tokio::test]
async fn a_reset_during_the_tls_handshake_retries_the_put() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let mut fixture = Fixture::new(format!("https://{}", listener.local_addr()?), b"body")?;
    let (acceptor, config) = test_tls()?;
    fixture.s3.set_test_transport(Transport::build(
        config,
        hyper_util::client::proxy::matcher::Matcher::builder().build(),
    ));
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await?;
        assert_eq!(socket.read_u8().await?, 22, "TLS handshake record");
        socket.set_zero_linger()?;
        drop(socket);

        let (socket, _) = listener.accept().await?;
        let mut socket = read_headers(acceptor.accept(socket).await?).await?;
        let mut body = [0; 4];
        socket.read_exact(&mut body).await?;
        assert_eq!(&body, b"body");
        reply_stored(socket).await
    });

    let result = timeout(DEADLINE, upload(fixture.request())).await?;
    assert!(
        result.is_ok(),
        "a reset during the TLS handshake must retry: {result:?}"
    );
    assert_eq!(result?, format!("ETag: {ETAG}\n"));
    server.await??;
    Ok(())
}

#[tokio::test]
async fn an_invalid_tls_certificate_is_tried_once() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let fixture = Fixture::new(format!("https://{}", listener.local_addr()?), b"body")?;
    let (acceptor, _) = test_tls()?;
    let attempts = Cell::new(0);
    let reject_handshake = async {
        loop {
            let (socket, _) = listener.accept().await?;
            attempts.set(attempts.get() + 1);
            assert!(
                acceptor.accept(socket).await.is_err(),
                "the self-signed certificate is untrusted"
            );
        }
        #[allow(unreachable_code)]
        Ok::<(), io::Error>(())
    };
    let result = tokio::select! {
        result = timeout(DEADLINE, upload(fixture.request())) => result?,
        result = reject_handshake => { result?; anyhow::bail!("TLS server stopped before the upload") },
    };
    let error = result
        .err()
        .context("an untrusted certificate must fail TLS negotiation")?;
    assert!(
        matches!(error.downcast_ref::<crate::s3::Error>(), Some(crate::s3::Error::Transport { source, .. }) if source.downcast_ref::<hyper_util::client::legacy::Error>().is_some_and(hyper_util::client::legacy::Error::is_connect))
    );
    assert_eq!(
        attempts.get(),
        1,
        "an untrusted certificate must fail on the first attempt"
    );
    assert!(fixture.db.cached_etag()?.is_none());
    Ok(())
}

#[tokio::test]
async fn retry_after_a_connection_drops_during_put() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let body = vec![42; 256 * 1024];
    let fixture = Fixture::new(format!("http://{}", listener.local_addr()?), &body)?;
    let server = tokio::spawn(async move {
        let mut first = read_put_headers(&listener).await?;
        assert_eq!(first.read_u8().await?, 42);
        drop(first);

        let mut second = read_put_headers(&listener).await?;
        let mut stored = vec![0; body.len()];
        second.read_exact(&mut stored).await?;
        assert_eq!(stored, body);
        reply_stored(second).await
    });

    let report = timeout(DEADLINE, upload(fixture.request())).await??;
    server.await??;
    assert_eq!(report, format!("ETag: {ETAG}\n"));
    assert_eq!(fixture.db.cached_etag()?.as_deref(), Some(ETAG));
    Ok(())
}

#[tokio::test]
async fn a_plain_http_peer_on_an_https_endpoint_is_tried_once() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let fixture = Fixture::new(format!("https://{}", listener.local_addr()?), b"body")?;
    let attempts = Cell::new(0);
    let result = tokio::select! {
        result = timeout(DEADLINE, upload(fixture.request())) => result?,
        result = respond_with_plain_http(&listener, &attempts) => { result?; anyhow::bail!("server stopped before the upload") },
    };

    let error = result
        .err()
        .context("an HTTP peer must fail TLS negotiation")?;
    let error = error
        .downcast_ref::<crate::s3::Error>()
        .context("S3 error")?;
    assert!(
        matches!(error, crate::s3::Error::Transport { source, .. } if source.downcast_ref::<hyper_util::client::legacy::Error>().is_some_and(hyper_util::client::legacy::Error::is_connect))
    );
    assert_eq!(
        attempts.get(),
        1,
        "a non-TLS peer must fail on the first attempt"
    );
    assert!(fixture.db.cached_etag()?.is_none());
    Ok(())
}

#[tokio::test]
async fn cloudflare_522_retries_the_put() -> Result<()> {
    assert_cloudflare_retry(522).await
}

#[tokio::test]
async fn cloudflare_524_retries_the_put() -> Result<()> {
    assert_cloudflare_retry(524).await
}

async fn assert_cloudflare_retry(status: usize) -> Result<()> {
    let mut server = mockito::Server::new_async().await;
    let fixture = Fixture::new(server.url(), b"body")?;
    let gateway = server
        .mock("PUT", "/bucket/key")
        .with_status(status)
        .with_body("<html>Cloudflare gateway error</html>")
        .expect(1)
        .create_async()
        .await;
    let stored = server
        .mock("PUT", "/bucket/key")
        .with_status(200)
        .with_header("etag", ETAG)
        .expect(1)
        .create_async()
        .await;

    let result = upload(fixture.request()).await;
    assert!(
        result.is_ok(),
        "Cloudflare HTTP {status} must retry: {result:?}"
    );
    assert_eq!(result?, format!("ETag: {ETAG}\n"));
    gateway.assert_async().await;
    stored.assert_async().await;
    Ok(())
}

#[tokio::test]
async fn http_501_is_tried_once() -> Result<()> {
    let mut server = mockito::Server::new_async().await;
    let fixture = Fixture::new(server.url(), b"body")?;
    let unsupported = server
        .mock("PUT", "/bucket/key")
        .with_status(501)
        .with_body("<Error><Code>NotImplemented</Code></Error>")
        .expect(1)
        .create_async()
        .await;
    let stored = server
        .mock("PUT", "/bucket/key")
        .with_status(200)
        .with_header("etag", ETAG)
        .expect(0)
        .create_async()
        .await;

    let result = upload(fixture.request()).await;
    let error = result
        .err()
        .context("HTTP 501 must fail on the first attempt")?;
    assert!(
        matches!(error.downcast_ref::<crate::s3::Error>(), Some(crate::s3::Error::Api(error)) if error.status == 501)
    );
    assert!(fixture.db.cached_etag()?.is_none());
    unsupported.assert_async().await;
    stored.assert_async().await;
    Ok(())
}

#[tokio::test]
async fn an_append_during_the_put_fails_after_success_without_caching_the_etag() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let fixture = Fixture::new(format!("http://{}", listener.local_addr()?), b"body")?;
    let file = fixture.file.clone();
    // Preserve mtime so the regression exercises the post-PUT size check.
    let mtime = UNIX_EPOCH + Duration::from_secs(10);
    // Windows needs write access to change a file's times.
    std::fs::OpenOptions::new()
        .write(true)
        .open(&file)?
        .set_modified(mtime)?;
    let server = tokio::spawn(async move {
        let mut socket = read_put_headers(&listener).await?;
        let mut stored = [0; 4];
        socket.read_exact(&mut stored).await?;
        assert_eq!(&stored, b"body");
        let mut file = std::fs::OpenOptions::new().append(true).open(file)?;
        file.write_all(b" appended")?;
        file.set_modified(mtime)?;
        reply_stored(socket).await
    });

    let result = timeout(DEADLINE, upload(fixture.request())).await?;
    server.await??;
    let error = result
        .err()
        .context("a successful PUT must fail locally after the source grows")?;
    assert!(error.to_string().contains("the file changed during the upload: size changed from 4 to 13 bytes. Run the upload again."), "{error}");
    assert!(fixture.db.cached_etag()?.is_none());
    Ok(())
}

async fn respond_with_plain_http(listener: &TcpListener, attempts: &Cell<usize>) -> Result<()> {
    loop {
        let (mut socket, _) = listener.accept().await?;
        attempts.set(attempts.get() + 1);
        // Consume the TLS record so closing the socket does not reset it with unread data.
        assert_eq!(socket.read_u8().await?, 22);
        let _version = socket.read_u16().await?;
        let mut record = vec![0; usize::from(socket.read_u16().await?)];
        socket.read_exact(&mut record).await?;
        socket
            .write_all(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n")
            .await?;
        socket.shutdown().await?;
    }
}

#[tokio::test]
async fn the_first_put_rejects_a_changed_size_or_digest() -> Result<()> {
    let mut server = mockito::Server::new_async().await;
    let unexpected = server
        .mock("PUT", "/bucket/key")
        .expect(0)
        .create_async()
        .await;
    let fixture = Fixture::new(server.url(), b"body")?;

    for (rewrite, reason) in [
        (b"same".as_slice(), "BLAKE3 checksum changed"),
        (b"new size", "size changed"),
    ] {
        std::fs::write(&fixture.file, rewrite)?;
        let error = upload(fixture.request())
            .await
            .err()
            .context("a changed file must fail")?
            .to_string();
        assert!(
            error.contains("the file changed during the upload") && error.contains(reason),
            "{error}"
        );
        assert!(error.contains("Run the upload again."), "{error}");
        assert!(fixture.db.cached_etag()?.is_none());
    }
    unexpected.assert_async().await;
    Ok(())
}

#[tokio::test]
async fn progress_channel_closes_before_the_put_response() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let fixture = Fixture::new(format!("http://{}", listener.local_addr()?), b"body")?;
    let (sender, mut progress) = unbounded_channel();
    let server = tokio::spawn(async move {
        let mut socket = read_put_headers(&listener).await?;
        let mut stored = [0; 4];
        socket.read_exact(&mut stored).await?;
        assert_eq!(&stored, b"body");
        assert_eq!(progress.recv().await, Some(stored.len()));
        assert_eq!(progress.recv().await, None);
        reply_stored(socket).await
    });

    let put = PutObject::new(KEY, &fixture.file, None, None, Some(sender), None, None);
    let response = timeout(
        DEADLINE,
        put.request(&fixture.s3, RequestOptions::default()),
    )
    .await??;
    server.await??;
    assert_eq!(response.get("ETag").map(String::as_str), Some(ETAG));
    Ok(())
}
