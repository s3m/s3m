use super::socket::{Connector, TrackedTcp};
use http::{Uri, uri::Scheme};
use hyper_rustls::{HttpsConnector, MaybeHttpsStream};
use hyper_util::{
    client::{legacy::connect::proxy::Tunnel, proxy::matcher::Matcher},
    rt::TokioIo,
};
use rustls::ClientConfig;
use std::{
    error::Error,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tower_service::Service;

#[derive(Clone)]
pub(super) struct Route {
    pub tcp: Connector,
    pub proxy: Arc<Matcher>,
    pub proxy_tls: Arc<ClientConfig>,
}

impl Service<Uri> for Route {
    type Response = MaybeHttpsStream<TokioIo<TrackedTcp>>;
    type Error = Box<dyn Error + Send + Sync>;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, dst: Uri) -> Self::Future {
        let Some(proxy) = self.proxy.intercept(&dst) else {
            let connecting = self.tcp.call(dst);
            return Box::pin(async { Ok(MaybeHttpsStream::Http(connecting.await?)) });
        };

        // A TLS proxy speaks HTTP/1.1 for forwarding and CONNECT. The outer
        // HTTPS connector negotiates the origin's protocol through the tunnel.
        let mut tcp = self.tcp.clone();
        tcp.forward = dst.scheme() == Some(&Scheme::HTTP);
        let mut connector = HttpsConnector::from((tcp, Arc::clone(&self.proxy_tls)));
        if dst.scheme() == Some(&Scheme::HTTPS) {
            let mut tunnel = Tunnel::new(proxy.uri().clone(), connector);
            if let Some(auth) = proxy.basic_auth() {
                tunnel = tunnel.with_auth(auth.clone());
            }
            return Box::pin(async move { Ok(tunnel.call(dst).await?) });
        }
        connector.call(proxy.uri().clone())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::super::socket::TEST_QUEUE_SAMPLE;
    use super::{
        super::{TEST_CONNECT_TIMEOUT, Transport, UploadBody, UploadProgress, capture_connection},
        *,
    };
    use crate::s3::request::{TEST_IDLE_TIMEOUT, TEST_QUEUE_UNAVAILABLE};
    use rustls::{RootCertStore, ServerConfig, pki_types::PrivatePkcs8KeyDer};
    use std::{
        cell::Cell,
        future::pending,
        io::Write as _,
        sync::{
            Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };
    use tokio::{
        io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader},
        net::TcpListener,
        sync::oneshot,
        time::{Duration, Instant, sleep, timeout},
    };
    use tokio_rustls::TlsAcceptor;

    async fn headers(socket: &mut BufReader<impl AsyncRead + Unpin>) -> String {
        let mut request = String::new();
        loop {
            let mut line = String::new();
            assert!(socket.read_line(&mut line).await.unwrap() > 0);
            if line == "\r\n" {
                return request;
            }
            request.push_str(&line);
        }
    }

    fn tls() -> (ClientConfig, TlsAcceptor) {
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let rcgen::CertifiedKey { cert, signing_key } =
            rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_string()]).unwrap();
        let mut roots = RootCertStore::empty();
        roots.add(cert.der().clone()).unwrap();
        let client = ClientConfig::builder_with_provider(Arc::clone(&provider))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let server = ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(
                vec![cert.der().clone()],
                PrivatePkcs8KeyDer::from(signing_key.serialize_der()).into(),
            )
            .unwrap();
        (client, TlsAcceptor::from(Arc::new(server)))
    }

    const OK: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";

    #[tokio::test]
    async fn timeout_allows_a_put_when_queue_sampling_recovers() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/key", listener.local_addr().unwrap());
        let transport = Transport::build(tls().0, Matcher::builder().build());
        let (send, start) = oneshot::channel();
        let body = reqwest::Body::wrap_stream(futures::stream::once(async {
            start.await.unwrap();
            Ok::<_, std::io::Error>(vec![42; 64 * 1024])
        }));
        let put = reqwest::Client::new()
            .put(url)
            .header("content-length", 64 * 1024)
            .body(body);
        let idle = Duration::from_millis(500);
        let server = async {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = BufReader::new(socket);
            assert!(headers(&mut socket).await.starts_with("PUT /key "));
            sleep(idle / 2).await;
            TEST_QUEUE_SAMPLE.with(|sample| sample.set(None));
            send.send(()).unwrap();
            socket.read_exact(&mut vec![0; 16 * 1024]).await.unwrap();
            sleep(idle * 2).await;
            // The recovered positive sample cannot be compared with the old zero.
            TEST_QUEUE_SAMPLE.with(|sample| sample.set(Some(48 * 1024)));
            sleep(idle / 2).await;
            socket.read_exact(&mut vec![0; 48 * 1024]).await.unwrap();
            let _ = socket.write_all(OK).await;
        };
        let response = timeout(Duration::from_secs(10), async {
            TEST_QUEUE_SAMPLE
                .scope(
                    Cell::new(Some(0)),
                    TEST_IDLE_TIMEOUT
                        .scope(idle, async { tokio::join!(transport.send(put), server).0 }),
                )
                .await
        })
        .await
        .unwrap();
        assert_eq!(
            response
                .expect("a recovered queue sample must let the healthy PUT finish")
                .status(),
            200
        );
    }

    async fn fallback_wait(first_size: Option<usize>, last_size: usize) -> Duration {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/key", listener.local_addr().unwrap());
        let transport = Transport::build(tls().0, Matcher::builder().build());
        let client = reqwest::Client::new();
        let (drained, last_body_received) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = BufReader::new(socket);
            if let Some(size) = first_size {
                assert!(headers(&mut socket).await.starts_with("PUT /key "));
                socket.read_exact(&mut vec![0; size]).await.unwrap();
                socket.write_all(OK).await.unwrap();
            }
            assert!(headers(&mut socket).await.starts_with("PUT /key "));
            socket.read_exact(&mut vec![0; last_size]).await.unwrap();
            drained.send(()).unwrap();
            pending::<()>().await;
        });
        if let Some(size) = first_size {
            let response = timeout(
                Duration::from_secs(5),
                transport.send(client.put(&url).body(vec![0; size])),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(response.status(), 200);
        }
        let put = client.put(url).body(vec![0; last_size]);
        let request =
            tokio::spawn(
                async move { TEST_QUEUE_UNAVAILABLE.scope((), transport.send(put)).await },
            );
        timeout(Duration::from_secs(5), last_body_received)
            .await
            .expect("the last PUT must reuse the same connection")
            .unwrap();
        // The peer has read the entire fixed-length body and sends no reply.
        // No request writes or response bytes remain to extend the watchdog.
        tokio::time::pause();
        let start = Instant::now();
        let result = timeout(Duration::from_mins(30), request)
            .await
            .unwrap()
            .unwrap();
        let elapsed = start.elapsed();
        tokio::time::resume();
        server.abort();
        assert!(
            matches!(
                result,
                Err(crate::s3::Error::ResponseTimeout {
                    phase: "upload",
                    ..
                })
            ),
            "{result:?}"
        );
        elapsed
    }

    #[tokio::test]
    async fn timeout_fallback_counts_only_the_current_put() {
        let fresh = fallback_wait(None, 1).await;
        let reused = fallback_wait(Some(1024 * 1024), 1).await;
        println!("fresh socket: {fresh:?}; reused after 1 MiB PUT: {reused:?}");
        assert!(
            fresh.abs_diff(reused) <= Duration::from_secs(2),
            "an earlier PUT must not enlarge the next allowance: {fresh:?} vs {reused:?}"
        );
    }

    #[tokio::test]
    async fn timeout_fallback_caps_a_large_buffered_tail() {
        let waited = fallback_wait(None, 1024 * 1024).await;
        println!("large buffered PUT: {waited:?}");
        assert!(
            waited <= Duration::from_secs(15 * 60 + 2),
            "a buffered upload must time out within the fallback cap: {waited:?}"
        );
    }

    #[tokio::test]
    async fn timeout_progress_allows_a_slow_streaming_put_without_queue_samples() {
        const CHUNK: usize = 128 * 1024;
        const CHUNKS: usize = 128;
        const SIZE: usize = CHUNK * CHUNKS;
        let listener = tokio::net::TcpSocket::new_v4().unwrap();
        listener.set_recv_buffer_size(32 * 1024).unwrap();
        listener.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let listener = listener.listen(1).unwrap();
        let url = format!("http://{}/key", listener.local_addr().unwrap());
        let transport = Transport::build(tls().0, Matcher::builder().build());
        let chunks_polled = Arc::new(AtomicUsize::new(0));
        let polled = Arc::clone(&chunks_polled);
        let body = reqwest::Body::wrap_stream(futures::stream::iter((0..CHUNKS).map(move |_| {
            polled.fetch_add(1, Ordering::Relaxed);
            Ok::<_, std::io::Error>(vec![0; CHUNK])
        })));
        let put = reqwest::Client::new()
            .put(url)
            .header("content-length", SIZE)
            .body(body);
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = BufReader::new(socket);
            assert!(headers(&mut socket).await.starts_with("PUT /key "));
            let mut received = 0;
            let mut pace = tokio::time::interval(Duration::from_millis(250));
            let mut buf = [0; 1024];
            // Stay backpressured beyond the production idle interval, then
            // drain at full speed so the test can finish promptly.
            for _ in 0..276 {
                pace.tick().await;
                socket.read_exact(&mut buf).await.unwrap();
                received += buf.len();
            }
            let polled = chunks_polled.load(Ordering::Relaxed);
            assert!(polled < CHUNKS, "the upload must still be streaming");
            println!(
                "paced receiver read {received} bytes with {polled}/{CHUNKS} source chunks polled"
            );
            let rest = (SIZE - received) as u64;
            assert_eq!(
                tokio::io::copy(&mut (&mut socket).take(rest), &mut tokio::io::sink())
                    .await
                    .unwrap(),
                rest
            );
            socket.write_all(OK).await.unwrap();
        });
        let response = timeout(
            Duration::from_secs(100),
            TEST_QUEUE_UNAVAILABLE.scope((), transport.send(put)),
        )
        .await
        .unwrap()
        .expect("a progressing streaming PUT must finish with queue samples unavailable");
        assert_eq!(response.status(), 200);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn timeout_resets_on_partial_tls_response_headers() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("https://{}/key", listener.local_addr().unwrap());
        let (config, acceptor) = tls();
        let transport = Transport::build(config, Matcher::builder().build());
        let server = async {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = BufReader::new(acceptor.accept(socket).await.unwrap());
            headers(&mut socket).await;
            for line in [
                "HTTP/1.1 200 OK\r\n",
                "Content-Length: 0\r\n",
                "X-Progress: 1\r\n",
                "X-Progress: 2\r\n",
                "X-Progress: 3\r\n",
                "\r\n",
            ] {
                sleep(Duration::from_millis(100)).await;
                socket.write_all(line.as_bytes()).await.unwrap();
                socket.flush().await.unwrap();
            }
        };
        let get = reqwest::Client::new().get(url);
        let (response, ()) = timeout(Duration::from_secs(5), async {
            tokio::join!(
                TEST_IDLE_TIMEOUT.scope(Duration::from_millis(300), transport.send(get)),
                server
            )
        })
        .await
        .unwrap();
        assert_eq!(response.unwrap().status(), 200);
    }

    #[tokio::test]
    async fn timeout_progress_counts_tls_key_updates() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("https://{}/key", listener.local_addr().unwrap());
        let (config, acceptor) = tls();
        let transport = Transport::build(config, Matcher::builder().build());
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = BufReader::new(acceptor.accept(socket).await.unwrap());
            headers(&mut socket).await;
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\n")
                .await
                .unwrap();
            socket.flush().await.unwrap();
            for _ in 0..8 {
                sleep(Duration::from_millis(100)).await;
                socket.get_mut().get_mut().1.refresh_traffic_keys().unwrap();
                socket.flush().await.unwrap();
            }
            socket.write_all(b"x").await.unwrap();
            socket.shutdown().await.unwrap();
            // The client can send TLS key updates in reply to the server's updates.
            let tcp = socket.get_mut().get_mut().0;
            timeout(
                Duration::from_secs(5),
                tokio::io::copy(tcp, &mut tokio::io::sink()),
            )
            .await
            .expect("the client must close after the TLS response")
            .unwrap();
        });
        let request = reqwest::Client::new().get(url);
        let body = timeout(
            Duration::from_secs(5),
            TEST_IDLE_TIMEOUT.scope(Duration::from_millis(400), async {
                transport.send(request).await.unwrap().bytes().await
            }),
        )
        .await
        .unwrap()
        .expect("incoming TLS key updates keep the response body alive");
        assert_eq!(body.as_ref(), b"x");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn timeout_progress_counts_partial_tls_records() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("https://{}/key", listener.local_addr().unwrap());
        let (config, acceptor) = tls();
        let transport = Transport::build(config, Matcher::builder().build());
        let payload = vec![42; 16 * 1024];
        let server_payload = payload.clone();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = BufReader::new(acceptor.accept(socket).await.unwrap());
            assert!(headers(&mut socket).await.starts_with("GET /key "));
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 16384\r\n\r\n")
                .await
                .unwrap();
            socket.flush().await.unwrap();

            // Produce one record, then drip its ciphertext directly over TCP.
            let (tcp, tls) = socket.get_mut().get_mut();
            tls.writer().write_all(&server_payload).unwrap();
            let mut record = Vec::new();
            while tls.wants_write() {
                tls.write_tls(&mut record).unwrap();
            }
            let [kind, _, _, high, low] = *record.first_chunk::<5>().unwrap();
            assert_eq!(kind, 23, "expected a TLS application-data record");
            assert_eq!(
                record.len(),
                5 + usize::from(u16::from_be_bytes([high, low]))
            );
            for part in record.chunks(1024) {
                sleep(Duration::from_millis(50)).await;
                tcp.write_all(part).await.unwrap();
            }
        });
        let request = reqwest::Client::new().get(url);
        let body = timeout(
            Duration::from_secs(5),
            TEST_IDLE_TIMEOUT.scope(Duration::from_millis(400), async {
                transport.send(request).await.unwrap().bytes().await
            }),
        )
        .await
        .unwrap()
        .expect("an arriving TLS record must keep the response body alive");
        assert_eq!(body.as_ref(), payload);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn timeout_bounds_the_background_spare_tls_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("https://{}/key", listener.local_addr().unwrap());
        let (config, acceptor) = tls();
        let transport = Transport::build(config, Matcher::builder().build());
        let (seen, ready) = oneshot::channel();
        let (completed, foreground_done) = oneshot::channel();
        let budget = Duration::from_secs(1);
        let server = tokio::spawn(async move {
            let (first, _) = listener.accept().await.unwrap();
            let mut first = BufReader::new(acceptor.accept(first).await.unwrap());
            assert!(headers(&mut first).await.starts_with("HEAD /key "));
            seen.send(()).unwrap();
            let (mut spare, _) = listener.accept().await.unwrap();
            assert_eq!(
                spare.read_u8().await.unwrap(),
                0x16,
                "spare TLS ClientHello"
            );
            first.write_all(OK).await.unwrap();
            first.flush().await.unwrap();
            assert!(headers(&mut first).await.starts_with("HEAD /key "));
            first.write_all(OK).await.unwrap();
            first.flush().await.unwrap();
            foreground_done.await.unwrap();
            let mut hello = Vec::new();
            timeout(budget * 4, spare.read_to_end(&mut hello))
                .await
                .expect("the background spare TLS socket must close at the connect deadline")
                .unwrap();
        });
        let client = reqwest::Client::new();
        let second = async {
            ready.await.unwrap();
            transport.send(client.head(&url)).await
        };
        let (first, second) = timeout(
            Duration::from_secs(10),
            TEST_CONNECT_TIMEOUT.scope(budget, async {
                tokio::join!(transport.send(client.head(&url)), second)
            }),
        )
        .await
        .unwrap();
        assert_eq!(first.unwrap().status(), 200);
        assert_eq!(second.unwrap().status(), 200);
        completed.send(()).unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn recovers_an_unstarted_request_on_a_stale_pooled_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/key", listener.local_addr().unwrap());
        let transport = Transport::build(tls().0, Matcher::builder().build());
        let server = tokio::spawn(async move {
            let (first, _) = listener.accept().await.unwrap();
            let mut first = BufReader::new(first);
            assert!(headers(&mut first).await.starts_with("HEAD /key "));
            first.write_all(OK).await.unwrap();
            let (fresh, _) = listener.accept().await.unwrap();
            assert_eq!(
                first.read(&mut [0]).await.unwrap(),
                0,
                "no second request on the stale socket"
            );
            let mut fresh = BufReader::new(fresh);
            assert!(headers(&mut fresh).await.starts_with("HEAD /key "));
            fresh.write_all(b"HTTP/1.1 200 OK\r\n").await.unwrap();
            for _ in 0..6 {
                sleep(Duration::from_millis(125)).await;
                fresh.write_all(b"X-Progress: yes\r\n").await.unwrap();
            }
            fresh.write_all(b"Content-Length: 0\r\n\r\n").await.unwrap();
        });
        let mut seed = http::Request::builder()
            .method("HEAD")
            .uri(&url)
            .body(reqwest::Body::from(bytes::Bytes::new()))
            .unwrap();
        let capture = Arc::new(capture_connection(&mut seed));
        let seed = seed.map(|body| UploadBody {
            body,
            upload: Arc::new(Mutex::new(UploadProgress::new())),
            capture,
        });
        let response = timeout(Duration::from_secs(10), transport.client.request(seed))
            .await
            .unwrap()
            .unwrap();
        let stale = response
            .extensions()
            .get::<Arc<super::super::Activity>>()
            .unwrap();
        tokio::task::yield_now().await;
        stale.stale.store(true, Ordering::Relaxed);
        let client = reqwest::Client::new();
        let result = timeout(
            Duration::from_secs(10),
            TEST_IDLE_TIMEOUT.scope(
                Duration::from_millis(500),
                transport.send(client.head(&url)),
            ),
        )
        .await
        .unwrap();
        assert!(
            !stale.stale.load(Ordering::Relaxed),
            "Hyper must encounter the stale connection"
        );
        assert!(
            result.is_ok(),
            "the unstarted HEAD must retry on a fresh connection: {result:?}"
        );
        assert_eq!(result.unwrap().status(), 200);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn proxy_forwards_an_absolute_http_target_with_authorization() {
        let proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/bucket/key", origin.local_addr().unwrap());
        let matcher = Matcher::builder()
            .all(format!(
                "http://test:password@{}",
                proxy.local_addr().unwrap()
            ))
            .build();
        let transport = Transport::build(tls().0, matcher);
        let server = async {
            let (socket, _) = proxy.accept().await.unwrap();
            let mut socket = BufReader::new(socket);
            let request = headers(&mut socket).await;
            assert!(request.starts_with(&format!("GET {url} HTTP/1.1\r\n")));
            assert!(request.contains("proxy-authorization: Basic dGVzdDpwYXNzd29yZA==\r\n"));
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                .await
                .unwrap();
        };
        let (response, ()) = timeout(Duration::from_secs(5), async {
            tokio::join!(transport.send(reqwest::Client::new().get(&url)), server)
        })
        .await
        .expect("the HTTP proxy must receive the request before the watchdog");
        assert_eq!(response.unwrap().status(), 200);
    }

    #[tokio::test]
    async fn proxy_tunnels_https_without_sending_proxy_authorization_to_origin() {
        let proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = origin.local_addr().unwrap();
        let matcher = Matcher::builder()
            .all(format!(
                "http://test:password@{}",
                proxy.local_addr().unwrap()
            ))
            .build();
        let (config, acceptor) = tls();
        let transport = Transport::build(config, matcher);
        let server = async {
            let (socket, _) = proxy.accept().await.unwrap();
            let mut socket = BufReader::new(socket);
            let request = headers(&mut socket).await;
            assert!(request.starts_with(&format!("CONNECT {addr} HTTP/1.1\r\n")));
            assert!(request.contains("Proxy-Authorization: Basic dGVzdDpwYXNzd29yZA==\r\n"));
            socket
                .write_all(b"HTTP/1.1 200 Connection established\r\n\r\n")
                .await
                .unwrap();
            let mut tls = BufReader::new(acceptor.accept(socket.into_inner()).await.unwrap());
            let request = headers(&mut tls).await;
            assert!(request.starts_with("GET /bucket/key HTTP/1.1\r\n"));
            assert!(!request.to_ascii_lowercase().contains("proxy-authorization"));
            tls.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                .await
                .unwrap();
        };
        let request = reqwest::Client::new().get(format!("https://{addr}/bucket/key"));
        let (response, ()) = timeout(Duration::from_secs(5), async {
            tokio::join!(transport.send(request), server)
        })
        .await
        .expect("the proxy must receive CONNECT before the watchdog");
        assert_eq!(response.unwrap().status(), 200);
    }
}
