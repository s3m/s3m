//!  S3 signature v4
//! <https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html>

use crate::s3::{
    S3,
    error::{Error, Result},
};
use bytes::Bytes;
use futures::stream::{self, TryStreamExt};
use reqwest::{
    Client, RequestBuilder, Response,
    header::{HeaderMap, HeaderName, HeaderValue},
};
use std::{
    collections::BTreeMap,
    io, iter,
    path::Path,
    sync::{Arc, Mutex, PoisonError},
};
use tokio::{
    fs::File,
    io::{AsyncRead, AsyncReadExt, AsyncSeekExt, SeekFrom},
};
use tokio_util::codec::{BytesCodec, FramedRead};
use upload_stream::paced;
use url::Url;

mod timeout;
mod transport;
mod upload_stream;

#[cfg(test)]
pub(crate) use timeout::TEST_IDLE_TIMEOUT;
pub(crate) use transport::{CONNECT_TIMEOUT, Transport};
#[cfg(test)]
pub(crate) use transport::{
    TEST_UPLOAD_TRACE,
    socket::{TEST_QUEUE_UNAVAILABLE, test_dns::TEST_RESOLVE},
};

const DEFAULT_FRAMED_CHUNK_SIZE_BYTES: usize = 1024 * 128;

static APP_USER_AGENT: &str = concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"),);

pub type ProgressCallback = Arc<dyn Fn(usize) + Send + Sync + 'static>;

pub(crate) fn is_transient_send_error(error: &reqwest::Error) -> bool {
    // AWS SDKs retry connection, timeout and other transient transport failures.
    // <https://docs.aws.amazon.com/sdkref/latest/guide/feature-retry-behavior.html>
    // Local read errors are returned separately by send_file.
    (error.is_connect() || error.is_request() || error.is_timeout())
        && !has_permanent_tls_error(error)
}

pub(crate) fn has_permanent_tls_error(error: &dyn std::error::Error) -> bool {
    iter::successors(error.source(), |&cause| {
        // hyper-rustls uses io::Error::other, whose source can skip the inner error.
        cause
            .downcast_ref::<io::Error>()
            .and_then(io::Error::get_ref)
            .map(|inner| inner as &(dyn std::error::Error + 'static))
            .or_else(|| cause.source())
    })
    .any(|cause| {
        cause
            .downcast_ref::<rustls::Error>()
            .is_some_and(is_permanent_tls_error)
    })
}

fn is_permanent_tls_error(error: &rustls::Error) -> bool {
    use rustls::{AlertDescription as Alert, Error, InvalidMessage};
    matches!(
        error,
        Error::InvalidCertificate(_)
            | Error::NoCertificatesPresented
            | Error::UnsupportedNameType
            | Error::InvalidMessage(
                InvalidMessage::InvalidContentType | InvalidMessage::UnknownProtocolVersion
            )
            | Error::AlertReceived(
                Alert::BadCertificate
                    | Alert::UnsupportedCertificate
                    | Alert::CertificateRevoked
                    | Alert::CertificateExpired
                    | Alert::CertificateUnknown
                    | Alert::UnknownCA
                    | Alert::BadCertificateStatusResponse
                    | Alert::CertificateRequired
            )
    )
}

fn headers_for_log(headers: &HeaderMap) -> HeaderMap {
    let mut headers = headers.clone();
    for (name, value) in &mut headers {
        if name == reqwest::header::AUTHORIZATION || name == "x-amz-security-token" {
            *value = HeaderValue::from_static("<redacted>");
        }
    }
    headers
}

pub(crate) struct Sender<'a> {
    client: &'a Client,
    transport: Option<&'a Transport>,
}

impl<'a> From<&'a S3> for Sender<'a> {
    fn from(s3: &'a S3) -> Self {
        Self {
            client: s3.client(),
            transport: Some(s3.transport()),
        }
    }
}

impl<'a> From<&'a Client> for Sender<'a> {
    fn from(client: &'a Client) -> Self {
        Self {
            client,
            transport: None,
        }
    }
}

async fn send(request: RequestBuilder, transport: Option<&Transport>) -> Result<Response> {
    if let Some(transport) = transport {
        transport.send(request).await
    } else {
        Ok(request.send().await?)
    }
}

pub struct MultipartRequest<'a> {
    pub client: &'a Client,
    pub url: Url,
    pub method: reqwest::Method,
    pub headers: &'a BTreeMap<String, String>,
    pub file: &'a Path,
    pub seek: u64,
    pub chunk: u64,
    pub throttle: Option<usize>,
}

/// # Errors
///
/// Will return `Err` if can not make the request
pub async fn request(
    client: &Client,
    url: Url,
    method: reqwest::Method,
    headers: &BTreeMap<String, String>,
    file: Option<&Path>,
    progress: Option<ProgressCallback>,
    throttle: Option<usize>,
) -> Result<Response> {
    send_request(
        client.into(),
        url,
        method,
        headers,
        file,
        progress,
        throttle,
    )
    .await
}

pub(crate) async fn send_request(
    sender: Sender<'_>,
    url: Url,
    method: reqwest::Method,
    headers: &BTreeMap<String, String>,
    file: Option<&Path>,
    progress: Option<ProgressCallback>,
    throttle: Option<usize>,
) -> Result<Response> {
    request_with_file(
        sender,
        url,
        method,
        headers,
        file.map(|path| (path, None)),
        progress,
        throttle,
    )
    .await
}

pub(crate) async fn request_with_file(
    sender: Sender<'_>,
    url: Url,
    method: reqwest::Method,
    headers: &BTreeMap<String, String>,
    file: Option<(&Path, Option<blake3::Hash>)>,
    progress: Option<ProgressCallback>,
    throttle: Option<usize>,
) -> Result<Response> {
    let headers = headers
        .iter()
        .map(|(k, v)| Ok((k.parse::<HeaderName>()?, v.parse::<HeaderValue>()?)))
        .collect::<Result<HeaderMap>>()?;

    log::debug!(
        "HTTP method: {method}, Headers: {:#?}",
        headers_for_log(&headers)
    );

    let length = headers
        .get(reqwest::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok());
    let request = sender
        .client
        .request(method, url)
        .header(reqwest::header::USER_AGENT, APP_USER_AGENT)
        .headers(headers);

    if let Some((file, expected_blake3)) = file {
        send_file(
            request,
            File::open(file).await?,
            length,
            expected_blake3,
            progress,
            throttle,
            sender.transport,
        )
        .await
    } else {
        send(request, sender.transport).await
    }
}

async fn send_file(
    request: RequestBuilder,
    file: impl AsyncRead + Unpin + Send + 'static,
    length: Option<u64>,
    expected_blake3: Option<blake3::Hash>,
    progress: Option<ProgressCallback>,
    throttle: Option<usize>,
    transport: Option<&Transport>,
) -> Result<Response> {
    let file = file.take(length.unwrap_or(u64::MAX));
    let chunks =
        FramedRead::with_capacity(file, BytesCodec::new(), DEFAULT_FRAMED_CHUNK_SIZE_BYTES);
    let digest = expected_blake3.map(|expected| (blake3::Hasher::new(), expected));
    let stream = stream::try_unfold(
        (chunks, length, digest),
        |(mut chunks, remaining, mut digest)| async move {
            match chunks.try_next().await? {
                Some(chunk) => {
                    let remaining = remaining.map(|bytes| bytes - chunk.len() as u64);
                    if let Some((hasher, expected)) = &mut digest {
                        hasher.update(&chunk);
                        // Withhold the final bytes if the source changed after signing.
                        if remaining == Some(0) && hasher.finalize() != *expected {
                            return Err(source_changed(
                                "BLAKE3 checksum changed since it was read",
                            ));
                        }
                    }
                    Ok(Some((chunk, (chunks, remaining, digest))))
                }
                None if remaining.unwrap_or(0) > 0 => Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "upload file ended before the signed content length",
                )
                .into()),
                None => Ok(None),
            }
        },
    );

    // Preserve the local read error when the transport reports a failed request.
    let read_error = Arc::new(Mutex::new(None));
    let body_error = Arc::clone(&read_error);
    let stream = stream
        .inspect_ok(move |chunk| {
            if let Some(callback) = &progress {
                log::trace!("Sending {} bytes", chunk.len());
                callback(chunk.len());
            }
        })
        .map_err(move |error| {
            *body_error.lock().unwrap_or_else(PoisonError::into_inner) = Some(error);
            io::Error::other("could not read upload body")
        });
    let body = reqwest::Body::wrap_stream(paced(stream, throttle));
    let response = Box::pin(send(request.body(body), transport)).await;
    if let Some(error) = read_error
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .take()
    {
        return Err(error);
    }
    response
}

pub(crate) fn source_changed(change: &str) -> Error {
    Error::Other(format!(
        "the file changed during the upload: {change}. Run the upload again."
    ))
}

/// # Errors
///
/// Will return `Err` if can not make the request
pub async fn multipart_upload(request: MultipartRequest<'_>) -> Result<Response> {
    send_part(request, None).await
}

pub(crate) async fn send_part(
    request: MultipartRequest<'_>,
    transport: Option<&Transport>,
) -> Result<Response> {
    let headers = request
        .headers
        .iter()
        .map(|(k, v)| Ok((k.parse::<HeaderName>()?, v.parse::<HeaderValue>()?)))
        .collect::<Result<HeaderMap>>()?;

    log::debug!(
        "HTTP method: {}, Headers: {:#?}",
        request.method,
        headers_for_log(&headers)
    );

    // async read
    let mut file = File::open(request.file).await?;

    file.seek(SeekFrom::Start(request.seek)).await?;

    let file = file.take(request.chunk);

    log::debug!("Chunk size: {}", request.chunk);

    let stream =
        FramedRead::with_capacity(file, BytesCodec::new(), DEFAULT_FRAMED_CHUNK_SIZE_BYTES);

    let stream = paced(stream, request.throttle);
    let request = request
        .client
        .request(request.method, request.url)
        .header(reqwest::header::USER_AGENT, APP_USER_AGENT)
        .headers(headers);

    send(request.body(reqwest::Body::wrap_stream(stream)), transport).await
}

/// # Errors
///
/// Will return `Err` if can not make the request
pub async fn upload(
    client: &Client,
    url: Url,
    method: reqwest::Method,
    headers: &BTreeMap<String, String>,
    body: Bytes,
) -> Result<Response> {
    send_payload(client.into(), url, method, headers, body).await
}

pub(crate) async fn send_payload(
    sender: Sender<'_>,
    url: Url,
    method: reqwest::Method,
    headers: &BTreeMap<String, String>,
    body: Bytes,
) -> Result<Response> {
    let headers = headers
        .iter()
        .map(|(k, v)| Ok((k.parse::<HeaderName>()?, v.parse::<HeaderValue>()?)))
        .collect::<Result<HeaderMap>>()?;

    log::debug!(
        "HTTP method: {method}, Headers: {:#?}",
        headers_for_log(&headers)
    );

    let request = sender
        .client
        .request(method, url)
        .header(reqwest::header::USER_AGENT, APP_USER_AGENT)
        .headers(headers)
        .body(body);

    send(request, sender.transport).await
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::unnecessary_wraps,
    clippy::cast_precision_loss,
    clippy::large_stack_arrays,
    clippy::missing_errors_doc,
    clippy::float_cmp
)]
mod tests {
    use super::*;
    use super::{transport::TEST_CONNECT_TIMEOUT, upload_stream::calculate_duration_per_chunk};
    use crate::s3::{
        Credentials, Error, Region, RequestOptions, S3,
        actions::{GetObject, HeadObject},
    };
    use mockito::Server;
    use reqwest::StatusCode;
    use std::{
        cell::Cell,
        pin::Pin,
        task::{Context, Poll},
    };
    use tokio::time::Duration;
    use tokio::{
        io::{AsyncBufReadExt, AsyncWriteExt, BufReader, ReadBuf},
        net::{TcpListener, TcpStream},
        time::{Instant, sleep, timeout},
    };

    fn loopback_s3(endpoint: String) -> S3 {
        S3::new(
            &Credentials::new("", &secrecy::SecretString::new("".into())),
            &Region::custom("us-east-1", endpoint),
            Some("bucket".into()),
            true,
        )
    }

    async fn read_request_headers(socket: &mut TcpStream) {
        let mut reader = BufReader::new(socket);
        let mut line = String::new();
        loop {
            assert!(reader.read_line(&mut line).await.unwrap() > 0);
            if line == "\r\n" {
                return;
            }
            line.clear();
        }
    }

    async fn finish_response(mut socket: TcpStream) {
        socket.shutdown().await.unwrap();
        // Keep the read side open until the client has finished with the connection.
        timeout(
            Duration::from_secs(5),
            tokio::io::copy(&mut socket, &mut tokio::io::sink()),
        )
        .await
        .expect("the client must close after the response")
        .unwrap();
    }

    #[tokio::test]
    async fn caller_supplied_client_controls_the_read_timeout() {
        for (read_timeout, should_succeed) in [
            (Duration::from_secs(3), true),
            (Duration::from_millis(100), false),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}/key", listener.local_addr().unwrap());
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                while !request.ends_with(b"\r\n\r\n") {
                    request.push(socket.read_u8().await.unwrap());
                }
                sleep(Duration::from_millis(600)).await;
                let _ = socket
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                    .await;
            });
            let client = Client::builder()
                .no_proxy()
                .read_timeout(read_timeout)
                .build()
                .unwrap();
            let result = timeout(
                Duration::from_secs(10),
                TEST_IDLE_TIMEOUT.scope(
                    Duration::from_millis(250),
                    request(
                        &client,
                        url.parse().unwrap(),
                        reqwest::Method::GET,
                        &BTreeMap::new(),
                        None,
                        None,
                        None,
                    ),
                ),
            )
            .await
            .unwrap();
            if should_succeed {
                assert!(
                    result.is_ok(),
                    "caller read timeout must allow the delayed response: {result:?}"
                );
            } else {
                assert!(matches!(result, Err(Error::Http(error)) if error.is_timeout()));
            }
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn timeout_bounds_a_stalled_tls_handshake_on_loopback() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let s3 = loopback_s3(format!("https://{addr}"));
        let received_client_hello = Cell::new(false);
        let server = async {
            let (mut socket, _) = listener.accept().await.unwrap();
            assert_eq!(
                socket.read_u8().await.unwrap(),
                0x16,
                "expected a TLS handshake record"
            );
            received_client_hello.set(true);
            std::future::pending::<()>().await;
        };
        let head = HeadObject::new("key", None);
        let result = tokio::select! {
            result = timeout(
                Duration::from_secs(10),
                TEST_CONNECT_TIMEOUT.scope(Duration::from_millis(500), head.request(&s3))
            ) => result,
            () = server => unreachable!("the TLS server never replies"),
        };
        let error = result
            .expect("the connector must time out before the test watchdog")
            .expect_err("a stalled TLS handshake must fail");
        assert!(received_client_hello.get());
        assert!(
            matches!(&error, Error::ResponseTimeout { endpoint, phase: "connect" }
                if endpoint == &addr.to_string()),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn timeout_allows_a_steady_download_longer_than_the_response_idle_interval() {
        let idle = Duration::from_millis(500);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let s3 = loopback_s3(format!("http://{addr}"));
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_request_headers(&mut socket).await;
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 8\r\nConnection: close\r\n\r\n")
                .await
                .unwrap();
            for byte in b"abcdefgh" {
                sleep(Duration::from_millis(100)).await;
                socket.write_u8(*byte).await.unwrap();
            }
            finish_response(socket).await;
        });

        let start = Instant::now();
        let bytes = timeout(
            Duration::from_secs(10),
            TEST_IDLE_TIMEOUT.scope(idle, async {
                let response = GetObject::new("key", None)
                    .request(&s3, &RequestOptions::default())
                    .await
                    .unwrap();
                assert_eq!(response.content_length(), Some(8));
                assert_eq!(response.url().as_str(), format!("http://{addr}/bucket/key"));
                assert_eq!(response.remote_addr(), Some(addr));
                response
                    .bytes()
                    .await
                    .expect("each download chunk arrives within the response idle interval")
            }),
        )
        .await
        .unwrap();
        assert_eq!(bytes.as_ref(), b"abcdefgh");
        assert!(start.elapsed() > idle);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn timeout_resets_on_partial_response_headers() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let s3 = loopback_s3(format!("http://{}", listener.local_addr().unwrap()));
        let idle = Duration::from_millis(500);
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_request_headers(&mut socket).await;
            socket.write_all(b"HTTP/1.1 200 OK\r\n").await.unwrap();
            for _ in 0..6 {
                sleep(Duration::from_millis(125)).await;
                socket.write_all(b"X-Progress: yes\r\n").await.unwrap();
            }
            socket
                .write_all(b"Content-Length: 0\r\nConnection: close\r\n\r\n")
                .await
                .unwrap();
            finish_response(socket).await;
        });
        let result = timeout(
            Duration::from_secs(10),
            TEST_IDLE_TIMEOUT.scope(idle, HeadObject::new("key", None).request(&s3)),
        )
        .await
        .unwrap();
        assert!(
            result.is_ok(),
            "individual header reads must reset the idle interval: {result:?}"
        );
        server.await.unwrap();
    }

    struct FailingReader;

    impl AsyncRead for FailingReader {
        fn poll_read(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            _: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Ready(Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "local read failure",
            )))
        }
    }

    #[tokio::test]
    async fn returns_the_local_read_error_from_a_managed_upload() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/test", listener.local_addr().unwrap());
        let s3 = loopback_s3(url.clone());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            socket.read_to_end(&mut bytes).await.unwrap();
        });
        let request = s3
            .client()
            .put(url)
            .header(reqwest::header::CONTENT_LENGTH, 8);
        let reader = b"part".as_slice().chain(FailingReader);

        let error = timeout(
            Duration::from_secs(10),
            send_file(
                request,
                reader,
                Some(8),
                None,
                None,
                None,
                Some(s3.transport()),
            ),
        )
        .await
        .unwrap()
        .unwrap_err();
        server.abort();
        let _ = server.await;
        assert!(matches!(error, Error::Io(error)
            if error.kind() == io::ErrorKind::ConnectionReset && error.to_string() == "local read failure"));
    }

    #[test]
    fn hides_credential_headers_only_in_logs() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "Authorization",
            HeaderValue::from_static("Signature=secret"),
        );
        headers.insert(
            "x-amz-security-token",
            HeaderValue::from_static("session token"),
        );
        headers.append(
            "x-amz-security-token",
            HeaderValue::from_static("another token"),
        );
        headers.insert("content-length", HeaderValue::from_static("10"));

        let logged = headers_for_log(&headers);
        assert_eq!(logged["authorization"], "<redacted>");
        assert_eq!(logged.get_all("x-amz-security-token").iter().count(), 2);
        assert!(
            logged
                .get_all("x-amz-security-token")
                .iter()
                .all(|value| value == "<redacted>")
        );
        assert_eq!(logged["content-length"], "10");
        assert_eq!(headers["authorization"], "Signature=secret");
        assert_eq!(headers["x-amz-security-token"], "session token");
    }

    #[test]
    fn test_calculate_duration_per_chunk() {
        let chunk_size = 1024 * 128;
        // (KB/s, expected delay between 128 KiB chunks)
        let cases = [
            (1, Duration::from_secs(128)),
            (3, Duration::from_nanos(42_666_666_666)),
            (1024, Duration::from_millis(125)),
            (102_400, Duration::from_micros(1_250)),
            // KB/s values whose byte rate overflows usize mean "no delay"
            (usize::MAX / 1000, Duration::ZERO),
            (usize::MAX, Duration::ZERO),
        ];

        for (bandwidth_kb_per_sec, expected) in cases {
            assert_eq!(
                calculate_duration_per_chunk(bandwidth_kb_per_sec, chunk_size),
                expected,
                "{bandwidth_kb_per_sec} KB/s"
            );
        }
    }

    #[tokio::test]
    async fn test_upload_get() {
        let mut server = Server::new_async().await;
        let _m = server
            .mock("GET", "/test")
            .with_status(200)
            .match_header("User-Agent", APP_USER_AGENT)
            .match_body("test data")
            .create_async()
            .await;

        let headers = BTreeMap::new();
        let body = Bytes::from("test data");
        let client = Client::new();

        let url = format!("{}/test", server.url()).parse::<Url>().unwrap();

        let response = upload(&client, url, reqwest::Method::GET, &headers, body).await;

        assert!(response.is_ok());
        let response = response.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_upload_post() {
        let mut server = Server::new_async().await;
        let _m = server
            .mock("POST", "/test")
            .with_status(200)
            .match_header("User-Agent", APP_USER_AGENT)
            .match_body("test data")
            .create_async()
            .await;

        let headers = BTreeMap::new();
        let body = Bytes::from("test data");
        let client = Client::new();

        let url = format!("{}/test", server.url()).parse::<Url>().unwrap();

        let response = upload(&client, url, reqwest::Method::POST, &headers, body).await;

        println!("Response: {response:?}");

        assert!(response.is_ok());
        let response = response.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_upload_reuses_same_client_for_multiple_requests() {
        let mut server = Server::new_async().await;
        let _first = server
            .mock("POST", "/first")
            .with_status(200)
            .match_header("User-Agent", APP_USER_AGENT)
            .match_body("first")
            .create_async()
            .await;
        let _second = server
            .mock("POST", "/second")
            .with_status(200)
            .match_header("User-Agent", APP_USER_AGENT)
            .match_body("second")
            .create_async()
            .await;

        let client = Client::new();
        let headers = BTreeMap::new();

        let first_url = format!("{}/first", server.url()).parse::<Url>().unwrap();
        let second_url = format!("{}/second", server.url()).parse::<Url>().unwrap();

        let first = upload(
            &client,
            first_url,
            reqwest::Method::POST,
            &headers,
            Bytes::from("first"),
        )
        .await
        .unwrap();
        let second = upload(
            &client,
            second_url,
            reqwest::Method::POST,
            &headers,
            Bytes::from("second"),
        )
        .await
        .unwrap();

        assert_eq!(first.status(), StatusCode::OK);
        assert_eq!(second.status(), StatusCode::OK);
    }
}
