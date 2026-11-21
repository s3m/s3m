#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::unnecessary_wraps
)]

mod common;

use common::get_s3m_binary;
use mockito::{Matcher, Server};
use std::fmt::Write as _;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::process::Command;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;
use tempfile::NamedTempFile;

/// `LastModified` values that stay inside / outside every configured age
/// window no matter when the test runs, so no assertion depends on the clock.
const FRESH: &str = "2999-01-01T00:00:00.000Z";
const STALE: &str = "2000-01-01T00:00:00.000Z";

fn run_s3m_with_config(config_path: &std::path::Path, args: &[&str]) -> std::process::Output {
    Command::new(get_s3m_binary())
        .arg("--config")
        .arg(config_path)
        .args(args)
        .output()
        .unwrap()
}

fn create_monitor_config(
    endpoint: &str,
    access_key: &str,
    secret_key: &str,
    buckets_yaml: &str,
) -> NamedTempFile {
    let config = format!(
        r"---
hosts:
  s3:
    endpoint: {endpoint}
    access_key: {access_key}
    secret_key: {secret_key}
    buckets:
{buckets_yaml}
"
    );

    let mut file = NamedTempFile::new().unwrap();
    file.write_all(config.as_bytes()).unwrap();
    file.flush().unwrap();
    file
}

fn list_objects_xml(bucket: &str, prefix: &str, entries: &[(&str, u64, &str)]) -> String {
    let mut contents = String::new();
    for (key, size, last_modified) in entries {
        let _ = write!(
            contents,
            "<Contents><Key>{key}</Key><LastModified>{last_modified}</LastModified><ETag>\"etag-{key}\"</ETag><Size>{size}</Size><StorageClass>STANDARD</StorageClass></Contents>"
        );
    }

    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Name>{bucket}</Name><Prefix>{prefix}</Prefix><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>{contents}</ListBucketResult>"#
    )
}

fn error_xml(code: &str, message: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><Error><Code>{code}</Code><Message>{message}</Message><RequestId>request-id</RequestId></Error>"#
    )
}

fn listing(
    server: &mut Server,
    prefix: &str,
    entries: &[(&str, u64, &str)],
    hits: usize,
) -> mockito::Mock {
    server
        .mock("GET", "/bucket-a")
        .match_query(Matcher::AllOf(vec![
            Matcher::UrlEncoded("list-type".into(), "2".into()),
            Matcher::UrlEncoded("prefix".into(), prefix.into()),
        ]))
        .with_status(200)
        .with_header("content-type", "application/xml")
        .with_body(list_objects_xml("bucket-a", prefix, entries))
        .expect(hits)
        .create()
}

/// Mocks that fail the test (on `assert`) if the binary sends any request.
fn forbid_requests(server: &mut Server) -> Vec<mockito::Mock> {
    ["GET", "HEAD", "PUT", "POST", "DELETE"]
        .into_iter()
        .map(|method| {
            server
                .mock(method, Matcher::Any)
                .with_status(500)
                .expect(0)
                .create()
        })
        .collect()
}

const DAILY_RULE: &str = r"      bucket-a:
        - prefix: backups/daily
          suffix: .log
          age: 1d
          size: 30720";

fn prometheus(exists: u8, error: u8, mismatch: u8) -> String {
    let labels = r#"{host="s3",bucket="bucket-a",prefix="backups/daily",suffix=".log"}"#;
    format!(
        "# HELP s3m_object_exists Object exists within the configured age window\n\
         # TYPE s3m_object_exists gauge\n\
         s3m_object_exists{labels} {exists}\n\
         # HELP s3m_check_error S3 API call failed\n\
         # TYPE s3m_check_error gauge\n\
         s3m_check_error{labels} {error}\n\
         # HELP s3m_size_mismatch Object size is below the configured minimum\n\
         # TYPE s3m_size_mismatch gauge\n\
         s3m_size_mismatch{labels} {mismatch}\n"
    )
}

#[test]
fn test_monitor_prometheus_output_default() {
    let mut server = Server::new();
    let list = listing(
        &mut server,
        "backups/daily",
        &[("backups/daily-2026-03-16.log", 40_960, FRESH)],
        3,
    );

    let config = create_monitor_config(&server.url(), "minioadmin", "minioadmin", DAILY_RULE);

    // Default, explicit long and short format flags render identical metrics.
    for args in [
        &["monitor", "s3"][..],
        &["monitor", "s3", "--format", "prometheus"],
        &["monitor", "s3", "-f", "prometheus"],
    ] {
        let output = run_s3m_with_config(config.path(), args);
        assert!(output.status.success(), "{args:?}: {output:?}");
        assert_eq!(String::from_utf8_lossy(&output.stdout), prometheus(1, 0, 0));
    }
    list.assert();
}

#[test]
fn test_monitor_exit_on_check_failure_returns_non_zero_after_printing_metrics() {
    let mut server = Server::new();
    let _list = server
        .mock("GET", "/bucket-a")
        .match_query(Matcher::AllOf(vec![
            Matcher::UrlEncoded("list-type".into(), "2".into()),
            Matcher::UrlEncoded("prefix".into(), "backups/daily".into()),
        ]))
        .with_status(200)
        .with_header("content-type", "application/xml")
        .with_body(list_objects_xml("bucket-a", "backups/daily", &[]))
        .create();

    let config = create_monitor_config(
        &server.url(),
        "minioadmin",
        "minioadmin",
        r"      bucket-a:
        - prefix: backups/daily
          age: 1d",
    );

    let output = run_s3m_with_config(config.path(), &["monitor", "s3", "--exit-on-check-failure"]);

    assert!(!output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stdout.contains("# HELP s3m_object_exists"));
    assert!(
        stdout
            .contains(r#"s3m_object_exists{host="s3",bucket="bucket-a",prefix="backups/daily"} 0"#)
    );
    assert!(
        stdout.contains(r#"s3m_check_error{host="s3",bucket="bucket-a",prefix="backups/daily"} 0"#)
    );
    assert!(stderr.contains("one or more monitor checks failed"));
}

#[test]
fn test_monitor_exit_policy_for_stale_and_undersized_objects() {
    // (listing, expected metrics): a stale-only object does not exist in the
    // window; a fresh but too-small object is a size mismatch.
    let cases = [
        (
            ("backups/daily-old.log", 40_960, STALE),
            prometheus(0, 0, 0),
        ),
        (
            ("backups/daily-small.log", 1_024, FRESH),
            prometheus(1, 0, 1),
        ),
    ];

    for (entry, metrics) in cases {
        let mut server = Server::new();
        let list = listing(&mut server, "backups/daily", &[entry], 2);
        let config = create_monitor_config(&server.url(), "minioadmin", "minioadmin", DAILY_RULE);

        let report = run_s3m_with_config(config.path(), &["monitor", "s3"]);
        assert_eq!(report.status.code(), Some(0), "{entry:?}: {report:?}");
        assert_eq!(String::from_utf8_lossy(&report.stdout), metrics);

        let strict =
            run_s3m_with_config(config.path(), &["monitor", "s3", "--exit-on-check-failure"]);
        assert_eq!(strict.status.code(), Some(1), "{entry:?}: {strict:?}");
        assert_eq!(String::from_utf8_lossy(&strict.stdout), metrics);
        assert!(
            String::from_utf8_lossy(&strict.stderr).contains("one or more monitor checks failed")
        );
        list.assert();
    }
}

#[test]
fn test_monitor_influxdb_output() {
    let mut server = Server::new();
    let list = listing(&mut server, "logs/", &[("logs/app.log", 2_048, FRESH)], 2);

    let config = create_monitor_config(
        &server.url(),
        "minioadmin",
        "minioadmin",
        r"      bucket-a:
        - prefix: logs/
          suffix: .log
          age: 1h
          size: 1024",
    );

    for args in [
        &["monitor", "s3", "--format", "influxdb"][..],
        &["monitor", "s3", "-f", "influxdb"],
    ] {
        let output = run_s3m_with_config(config.path(), args);
        assert!(output.status.success(), "{args:?}: {output:?}");
        assert_eq!(
            String::from_utf8_lossy(&output.stdout),
            "s3m,host=s3,bucket=bucket-a,prefix=logs/,suffix=.log error=0i,exist=1i,size_mismatch=0i\n"
        );
    }
    list.assert();
}

#[test]
fn test_monitor_prometheus_reports_check_error_without_failure_flag() {
    let mut server = Server::new();
    let _list = server
        .mock("GET", "/bucket-a")
        .match_query(Matcher::AllOf(vec![
            Matcher::UrlEncoded("list-type".into(), "2".into()),
            Matcher::UrlEncoded("prefix".into(), "logs/".into()),
        ]))
        .with_status(404)
        .with_header("content-type", "application/xml")
        .with_body(error_xml(
            "NoSuchBucket",
            "The specified bucket does not exist",
        ))
        .create();

    let config = create_monitor_config(
        &server.url(),
        "minioadmin",
        "minioadmin",
        r"      bucket-a:
        - prefix: logs/
          age: 60s",
    );

    let output = run_s3m_with_config(config.path(), &["monitor", "s3"]);

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains(r#"s3m_object_exists{host="s3",bucket="bucket-a",prefix="logs/"} 0"#));
    assert!(stdout.contains(r#"s3m_check_error{host="s3",bucket="bucket-a",prefix="logs/"} 1"#));
    assert!(stdout.contains(r#"s3m_size_mismatch{host="s3",bucket="bucket-a",prefix="logs/"} 0"#));
}

#[test]
fn test_monitor_reports_unsupported_age_as_check_error() {
    let mut server = Server::new();
    let list = listing(&mut server, "logs/", &[("logs/app.log", 2_048, STALE)], 0);
    // Parses as seconds, but reaches before the earliest representable date,
    // so the rule fails before any listing.
    let config = create_monitor_config(
        &server.url(),
        "minioadmin",
        "minioadmin",
        r"      bucket-a:
        - prefix: logs/
          age: 100000000d",
    );

    let output = run_s3m_with_config(config.path(), &["monitor", "s3", "-f", "influxdb"]);

    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        "s3m,host=s3,bucket=bucket-a,prefix=logs/ error=1i,exist=0i,size_mismatch=0i\n"
    );
    list.assert();
}

#[test]
fn test_monitor_rejects_empty_rule_list() {
    let server = Server::new();
    let config = create_monitor_config(
        &server.url(),
        "minioadmin",
        "minioadmin",
        "      bucket-a: []",
    );

    let output = run_s3m_with_config(config.path(), &["monitor", "s3"]);

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("No monitor rules configured"));
}

#[test]
fn test_monitor_rejects_invalid_invocations_without_requests() {
    let mut server = Server::new();
    let forbidden = forbid_requests(&mut server);
    let config = create_monitor_config(&server.url(), "minioadmin", "minioadmin", DAILY_RULE);

    let cases: &[(&[&str], i32, &str)] = &[
        (
            &["monitor", "s3", "--format", "json"],
            2,
            "invalid value 'json'",
        ),
        (&["monitor", "s3", "-f", "csv"], 2, "invalid value 'csv'"),
        (&["monitor"], 2, "required arguments"),
        (
            &["monitor", "s3/bucket-a"],
            1,
            "monitor expects a host name only",
        ),
        (&["monitor", "nope"], 1, "Could not find host"),
    ];
    for (args, code, message) in cases {
        let output = run_s3m_with_config(config.path(), args);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(output.status.code(), Some(*code), "{args:?}: {stderr}");
        assert!(stderr.contains(message), "{args:?}: {stderr}");
        assert!(output.stdout.is_empty(), "{args:?}");
    }
    for mock in forbidden {
        mock.assert();
    }
}

#[test]
fn test_monitor_uses_custom_endpoint_when_region_is_also_set() {
    let mut server = Server::new();
    let list = listing(&mut server, "logs/", &[("logs/app.log", 2_048, FRESH)], 1);

    let config = format!(
        r"---
hosts:
  s3:
    endpoint: {}
    region: us-east-1
    access_key: minioadmin
    secret_key: minioadmin
    buckets:
      bucket-a:
        - prefix: logs/
          age: 1h
",
        server.url()
    );

    let mut file = NamedTempFile::new().unwrap();
    file.write_all(config.as_bytes()).unwrap();
    file.flush().unwrap();

    let output = run_s3m_with_config(file.path(), &["monitor", "s3"]);

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains(r#"s3m_object_exists{host="s3",bucket="bucket-a",prefix="logs/"} 1"#));
    list.assert();
}

#[derive(Default)]
struct InFlight {
    current: usize,
    peak: usize,
    released: bool,
}

/// A loopback S3 stand-in that records how many monitor listings are in flight
/// at once. Each request is held until `barrier` requests overlap (bounded by a
/// timeout), so a run that cannot reach that concurrency reports a lower peak.
fn spawn_concurrency_probe(barrier: usize) -> (String, Arc<(Mutex<InFlight>, Condvar)>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let state = Arc::new((Mutex::new(InFlight::default()), Condvar::new()));
    let shared = Arc::clone(&state);

    thread::spawn(move || {
        for stream in listener.incoming() {
            let mut stream = stream.unwrap();
            let state = Arc::clone(&shared);
            thread::spawn(move || {
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut request_line = String::new();
                reader.read_line(&mut request_line).unwrap();
                let mut header = String::new();
                while reader.read_line(&mut header).unwrap() > 2 {
                    header.clear();
                }
                let bucket = request_line
                    .split_whitespace()
                    .nth(1)
                    .and_then(|target| target.trim_start_matches('/').split('?').next())
                    .unwrap()
                    .to_string();

                let (lock, ready) = &*state;
                let mut in_flight = lock.lock().unwrap();
                in_flight.current += 1;
                in_flight.peak = in_flight.peak.max(in_flight.current);
                if in_flight.current >= barrier {
                    in_flight.released = true;
                    ready.notify_all();
                }
                let (mut in_flight, _) = ready
                    .wait_timeout_while(in_flight, Duration::from_secs(10), |state| !state.released)
                    .unwrap();
                // Leave before answering: the next sequential request can only
                // start after this response arrives.
                in_flight.current -= 1;
                drop(in_flight);

                let body = list_objects_xml(&bucket, "logs/", &[("logs/app.log", 2_048, FRESH)]);
                write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Type: application/xml\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .unwrap();
            });
        }
    });

    (url, state)
}

#[test]
fn test_monitor_number_preserves_results_and_two_allows_overlap() {
    let rules = r"      bucket-a:
        - prefix: logs/
          age: 1h
      bucket-b:
        - prefix: logs/
          age: 1h";
    let expected = "s3m,host=s3,bucket=bucket-a,prefix=logs/ error=0i,exist=1i,size_mismatch=0i\n\
                    s3m,host=s3,bucket=bucket-b,prefix=logs/ error=0i,exist=1i,size_mismatch=0i\n";

    for (number, barrier) in [("2", 2), ("1", 1)] {
        let (url, state) = spawn_concurrency_probe(barrier);
        let config = create_monitor_config(&url, "minioadmin", "minioadmin", rules);

        let output = run_s3m_with_config(
            config.path(),
            &["-n", number, "monitor", "s3", "-f", "influxdb"],
        );

        assert!(output.status.success(), "-n {number}: {output:?}");
        assert_eq!(String::from_utf8_lossy(&output.stdout), expected);
        // The binary peer can prove positive overlap. With one slot it
        // releases immediately, so it cannot prove an upper bound; the
        // monitor scheduler unit test owns that deterministic assertion.
        if number == "2" {
            assert_eq!(state.0.lock().unwrap().peak, 2, "-n {number}");
        }
    }
}
