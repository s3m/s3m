use super::{UploadRequest, upload};
use crate::{
    s3::{Credentials, Region, RequestOptions, S3, actions::PutObject},
    stream::db::Db,
};
use anyhow::{Context, Result, ensure};
use reqwest::{
    Client,
    dns::{Addrs, Name, Resolve, Resolving},
};
use rustls::{ServerConfig, pki_types::PrivatePkcs8KeyDer};
use secrecy::SecretString;
use std::{
    cell::Cell,
    io::{self, Write as _},
    net::SocketAddr,
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
    time::timeout,
};
use tokio_rustls::TlsAcceptor;

const ETAG: &str = "\"stored\"";
const KEY: &str = "key";
const DEADLINE: Duration = Duration::from_secs(10);

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

#[derive(Debug)]
struct FlakyDns {
    loopback: SocketAddr,
    lookups: AtomicUsize,
}

impl Resolve for FlakyDns {
    fn resolve(&self, _: Name) -> Resolving {
        let failed = self.lookups.fetch_add(1, Ordering::Relaxed) == 0;
        let address = self.loopback;
        Box::pin(async move {
            if failed {
                // The system resolver reports EAI_AGAIN as a wrapped I/O error.
                return Err(io::Error::other("Temporary failure in name resolution").into());
            }
            Ok(Box::new(std::iter::once(address)) as Addrs)
        })
    }
}

#[tokio::test]
async fn a_temporary_dns_failure_retries_the_put() -> Result<()> {
    let mut server = mockito::Server::new_async().await;
    let address = server.socket_address();
    let mut fixture = Fixture::new(format!("http://retry.invalid:{}", address.port()), b"body")?;
    let resolver = Arc::new(FlakyDns {
        loopback: address,
        lookups: AtomicUsize::new(0),
    });
    fixture.s3.set_test_client(
        Client::builder()
            .no_proxy()
            .dns_resolver(Arc::clone(&resolver))
            .build()?,
    );
    let stored = server
        .mock("PUT", "/bucket/key")
        .match_body("body")
        .with_status(200)
        .with_header("etag", ETAG)
        .expect(1)
        .create_async()
        .await;

    let result = timeout(DEADLINE, upload(fixture.request())).await?;
    assert_eq!(
        resolver.lookups.load(Ordering::Relaxed),
        2,
        "temporary DNS failure must retry: {result:?}"
    );
    assert_eq!(result?, format!("ETag: {ETAG}\n"));
    stored.assert_async().await;
    Ok(())
}

#[tokio::test]
async fn a_remote_http2_throttle_retries_the_put() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let mut fixture = Fixture::new(format!("http://{}", listener.local_addr()?), b"body")?;
    fixture.s3.set_test_client(
        Client::builder()
            .no_proxy()
            .http2_prior_knowledge()
            .build()?,
    );
    let (stored, stored_body) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await?;
        let mut connection = h2::server::handshake(socket).await?;
        let (_, mut response) = connection.accept().await.context("first HTTP/2 PUT")??;
        response.send_reset(h2::Reason::ENHANCE_YOUR_CALM);
        let (request, mut response) = connection.accept().await.context("retried HTTP/2 PUT")??;
        let handler = tokio::spawn(async move {
            assert_eq!(request.method(), reqwest::Method::PUT);
            let mut received = Vec::new();
            let mut body = request.into_body();
            while let Some(chunk) = body.data().await {
                received.extend_from_slice(&chunk?);
            }
            let reply = http::Response::builder()
                .status(200)
                .header("etag", ETAG)
                .body(())?;
            response.send_response(reply, true)?;
            let _ = stored.send(received);
            Ok::<_, anyhow::Error>(())
        });
        while connection.accept().await.is_some() {}
        handler.await??;
        Ok::<_, anyhow::Error>(())
    });

    let result = timeout(DEADLINE, upload(fixture.request())).await?;
    assert!(
        result.is_ok(),
        "remote HTTP/2 throttling must retry: {result:?}"
    );
    assert_eq!(result?, format!("ETag: {ETAG}\n"));
    assert_eq!(timeout(DEADLINE, stored_body).await??, b"body");
    server.abort();
    Ok(())
}

fn test_tls() -> Result<(TlsAcceptor, reqwest::Certificate)> {
    let rcgen::CertifiedKey { cert, signing_key } =
        rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_string()])?;
    let config = ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()?
    .with_no_client_auth()
    .with_single_cert(
        vec![cert.der().clone()],
        PrivatePkcs8KeyDer::from(signing_key.serialize_der()).into(),
    )?;
    Ok((
        TlsAcceptor::from(Arc::new(config)),
        reqwest::Certificate::from_der(cert.der())?,
    ))
}

#[tokio::test]
async fn a_reset_during_the_tls_handshake_retries_the_put() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let mut fixture = Fixture::new(format!("https://{}", listener.local_addr()?), b"body")?;
    let (acceptor, cert) = test_tls()?;
    fixture.s3.set_test_client(
        Client::builder()
            .no_proxy()
            .tls_certs_only([cert])
            .build()?,
    );
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
        matches!(error.downcast_ref::<crate::s3::Error>(), Some(crate::s3::Error::Http(error)) if error.is_connect())
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
    assert!(matches!(error, crate::s3::Error::Http(error) if error.is_connect()));
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
