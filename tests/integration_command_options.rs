//! Read and utility command options (`acl`, `cb`, `du`, `get`, `share`) through
//! Cargo's binary against local mock endpoints. Each test checks the exact
//! request, stdout, filesystem effect and exit code; `share` recomputes the
//! `SigV4` query signature independently from the AWS specification.
// allow: SIZE_OK - one binary-level owner per command-option row shares these
// fixtures; tests/common is owned elsewhere, so splitting would copy them.
use anyhow::{Context, Result, ensure};
use mockito::{Matcher, Mock, Server, ServerGuard};
use serde_json::{Value, json};
use std::{
    fmt::Write as _,
    fs,
    path::PathBuf,
    process::{Command, Output},
};
use tempfile::{TempDir, tempdir};

const ACCESS_KEY: &str = "AKIDCOMMANDOPTIONS";
const SECRET_KEY: &str = "cmd-0pt10ns-Secret-Never-Printed";
const REGION: &str = "eu-central-1";
const OBJECT: &str = "s3/bucket/dir/data.bin";
const OBJECT_PATH: &str = "/bucket/dir/data.bin";
const ACLS: [&str; 7] = [
    "private",
    "public-read",
    "public-read-write",
    "authenticated-read",
    "aws-exec-read",
    "bucket-owner-read",
    "bucket-owner-full-control",
];

/// Isolated HOME, config file and working directory for one CLI endpoint.
struct Cli {
    home: TempDir,
    config: PathBuf,
    work: PathBuf,
}

impl Cli {
    fn new(endpoint: &str) -> Result<Self> {
        let home = tempdir()?;
        let config = home.path().join("config.yml");
        let work = home.path().join("work");
        fs::create_dir(&work)?;
        fs::write(
            &config,
            format!(
                "---\nhosts:\n  s3:\n    endpoint: {endpoint}\n    region: {REGION}\n    access_key: {ACCESS_KEY}\n    secret_key: {SECRET_KEY}\n"
            ),
        )?;
        Ok(Self { home, config, work })
    }

    fn run(&self, args: &[&str]) -> Result<Output> {
        Ok(Command::new(env!("CARGO_BIN_EXE_s3m"))
            .env("HOME", self.home.path())
            .current_dir(&self.work)
            .arg("--config")
            .arg(&self.config)
            .args(args)
            .output()?)
    }

    /// Sorted names in the working directory, where `get` writes by default.
    fn work_entries(&self) -> Result<Vec<String>> {
        let mut names = fs::read_dir(&self.work)?
            .map(|entry| Ok(entry?.file_name().to_string_lossy().into_owned()))
            .collect::<Result<Vec<_>>>()?;
        names.sort();
        Ok(names)
    }
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

/// Checks the exact exit code and returns stdout.
fn expect_exit(output: &Output, code: i32, args: &[&str]) -> Result<String> {
    ensure!(
        output.status.code() == Some(code),
        "{args:?}: expected exit {code}, got {:?}\nstdout: {}\nstderr: {}",
        output.status.code(),
        text(&output.stdout),
        text(&output.stderr)
    );
    Ok(text(&output.stdout))
}

/// Runs a command that must fail with `code`, print nothing on stdout and
/// explain itself on stderr.
fn expect_failure(cli: &Cli, args: &[&str], code: i32, message: &str) -> Result<()> {
    let output = cli.run(args)?;
    let stdout = expect_exit(&output, code, args)?;
    ensure!(stdout.is_empty(), "{args:?} printed {stdout}");
    let stderr = text(&output.stderr);
    ensure!(stderr.contains(message), "{args:?}: {stderr}");
    Ok(())
}

/// Mocks that fail the test if the CLI sends any request at all.
fn forbid_requests(server: &mut ServerGuard) -> Vec<Mock> {
    ["GET", "HEAD", "PUT", "POST", "DELETE"]
        .into_iter()
        .map(|method| {
            server
                .mock(method, Matcher::Any)
                .match_query(Matcher::Any)
                .expect(0)
                .create()
        })
        .collect()
}

fn assert_all(mocks: &[Mock]) {
    for mock in mocks {
        mock.assert();
    }
}

/// Matches a request only when its decoded query pairs are exactly `expected`.
fn exact_query(
    expected: &[(&str, &str)],
) -> impl Fn(&mockito::Request) -> bool + Send + Sync + 'static {
    let mut expected: Vec<(String, String)> = expected
        .iter()
        .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
        .collect();
    expected.sort();
    move |request| {
        let query = request
            .path_and_query()
            .split_once('?')
            .map_or("", |(_, query)| query);
        let mut actual: Vec<(String, String)> = url::form_urlencoded::parse(query.as_bytes())
            .into_owned()
            .collect();
        actual.sort();
        actual == expected
    }
}

fn s3_error(code: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><Error><Code>{code}</Code><Message>{code} from mock</Message></Error>"#
    )
}

fn list_page(entries: &[(&str, u64, &str)], next_token: Option<&str>) -> String {
    let mut contents = String::new();
    for (key, size, last_modified) in entries {
        let _ = write!(
            contents,
            "<Contents><Key>{key}</Key><LastModified>{last_modified}</LastModified><ETag>\"e\"</ETag><Size>{size}</Size><StorageClass>STANDARD</StorageClass></Contents>"
        );
    }
    let next = next_token.map_or_else(String::new, |token| {
        format!("<NextContinuationToken>{token}</NextContinuationToken>")
    });
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Name>bucket</Name><Prefix></Prefix><MaxKeys>1000</MaxKeys><IsTruncated>{}</IsTruncated>{next}{contents}</ListBucketResult>"#,
        next_token.is_some()
    )
}

fn list_mock(server: &mut ServerGuard, query: &[(&str, &str)], body: String) -> Mock {
    server
        .mock("GET", "/bucket")
        .match_query(Matcher::Any)
        .match_request(exact_query(query))
        .with_body(body)
        .expect(1)
        .create()
}

// <https://github.com/s3m/s3m/issues/78>: Cloudflare R2 leaves out <Prefix> when the
// listing has no prefix, and `ls` on a bucket failed with "missing field `Prefix`".
#[test]
fn ls_accepts_a_listing_without_a_prefix_element() -> Result<()> {
    let mut server = Server::new();
    let body = r#"<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>bucket</Name><Contents><Key>probe78/a.txt</Key><Size>6</Size><LastModified>2026-11-26T23:37:12.345Z</LastModified><ETag>"b1946ac92492d2347c6235b8d2611184"</ETag><StorageClass>STANDARD</StorageClass></Contents><IsTruncated>false</IsTruncated><MaxKeys>1000</MaxKeys><KeyCount>1</KeyCount></ListBucketResult>"#;
    let list = list_mock(
        &mut server,
        &[("list-type", "2"), ("max-keys", "1000")],
        body.to_string(),
    );
    let cli = Cli::new(&server.url())?;

    let args = ["ls", "s3/bucket"];
    let stdout = expect_exit(&cli.run(&args)?, 0, &args)?;

    ensure!(stdout.trim_end().ends_with("6 B probe78/a.txt"), "{stdout}");
    list.assert();
    Ok(())
}

// <https://github.com/s3m/s3m/issues/79>: Cloudflare R2 doesn't implement the
// `max-buckets` parameter of ListBuckets and answers 501, so `ls <host>` failed there.
// s3m asks again without it; `--number` is enforced by s3m, so it still limits the list.
#[test]
fn ls_buckets_falls_back_when_max_buckets_is_not_implemented() -> Result<()> {
    let not_implemented = r#"<?xml version="1.0" encoding="UTF-8"?><Error><Code>NotImplemented</Code><Message>ListBuckets search parameter max-buckets not implemented</Message></Error>"#;
    let buckets = r#"<?xml version="1.0" encoding="UTF-8"?><ListAllMyBucketsResult><Buckets><Bucket><Name>b-1</Name><CreationDate>2026-03-14T08:00:00.000Z</CreationDate></Bucket><Bucket><Name>b-2</Name><CreationDate>2026-03-14T08:00:00.000Z</CreationDate></Bucket></Buckets></ListAllMyBucketsResult>"#;
    for (args, max_buckets, listed) in [
        (&["ls", "s3"][..], "10000", &["b-1", "b-2"][..]),
        (&["ls", "s3", "--number", "1"][..], "1", &["b-1"][..]),
    ] {
        let mut server = Server::new();
        let rejected = server
            .mock("GET", "/")
            .match_query(Matcher::Any)
            .match_request(exact_query(&[("max-buckets", max_buckets)]))
            .with_status(501)
            .with_body(not_implemented)
            .expect(1)
            .create();
        let plain = server
            .mock("GET", "/")
            .match_query(Matcher::Any)
            .match_request(exact_query(&[]))
            .with_body(buckets)
            .expect(1)
            .create();
        let cli = Cli::new(&server.url())?;

        let stdout = expect_exit(&cli.run(args)?, 0, args)?;

        let names: Vec<&str> = stdout
            .lines()
            .filter_map(|line| line.split_whitespace().last())
            .collect();
        ensure!(names == listed, "{args:?}: {stdout}");
        rejected.assert();
        plain.assert();
    }
    Ok(())
}

// #79: after the fallback the listing stays without `max-buckets` on every later
// page and follows the continuation token; `--json` output is unaffected.
#[test]
fn ls_buckets_fallback_keeps_paginating_without_max_buckets() -> Result<()> {
    let bucket = |name: &str| {
        format!(
            "<Bucket><Name>{name}</Name><CreationDate>2026-03-14T08:00:00.000Z</CreationDate></Bucket>"
        )
    };
    let mut server = Server::new();
    let rejected = server
        .mock("GET", "/")
        .match_query(Matcher::Any)
        .match_request(exact_query(&[("max-buckets", "10000")]))
        .with_status(501)
        .with_body("<Error><Code>NotImplemented</Code><Message>max-buckets</Message></Error>")
        .expect(1)
        .create();
    let page_1 = server
        .mock("GET", "/")
        .match_query(Matcher::Any)
        .match_request(exact_query(&[]))
        .with_body(format!(
            "<ListAllMyBucketsResult><Buckets>{}{}</Buckets><ContinuationToken>t2</ContinuationToken></ListAllMyBucketsResult>",
            bucket("b-1"),
            bucket("b-2")
        ))
        .expect(1)
        .create();
    let page_2 = server
        .mock("GET", "/")
        .match_query(Matcher::Any)
        .match_request(exact_query(&[("continuation-token", "t2")]))
        .with_body(format!(
            "<ListAllMyBucketsResult><Buckets>{}</Buckets></ListAllMyBucketsResult>",
            bucket("b-3")
        ))
        .expect(1)
        .create();
    let cli = Cli::new(&server.url())?;

    let args = ["ls", "s3", "--json"];
    let stdout = expect_exit(&cli.run(&args)?, 0, &args)?;

    let json: Value = serde_json::from_str(&stdout)?;
    let names: Vec<&str> = json
        .get("buckets")
        .and_then(Value::as_array)
        .context("buckets array")?
        .iter()
        .filter_map(|bucket| bucket.get("name").and_then(Value::as_str))
        .collect();
    assert_eq!(names, ["b-1", "b-2", "b-3"]);
    assert_all(&[rejected, page_1, page_2]);
    Ok(())
}

// #79: the fallback is tried once. When the request without `max-buckets` fails too
// (a key that can't list buckets, as on R2), that error is the one reported.
#[test]
fn ls_buckets_fallback_reports_the_error_of_the_second_request() -> Result<()> {
    let mut server = Server::new();
    let rejected = server
        .mock("GET", "/")
        .match_query(Matcher::Any)
        .match_request(exact_query(&[("max-buckets", "10000")]))
        .with_status(501)
        .with_body("<Error><Code>NotImplemented</Code><Message>max-buckets</Message></Error>")
        .expect(1)
        .create();
    let denied = server
        .mock("GET", "/")
        .match_query(Matcher::Any)
        .match_request(exact_query(&[]))
        .with_status(403)
        .with_body("<Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>")
        .expect(1)
        .create();
    let cli = Cli::new(&server.url())?;

    let args = ["ls", "s3"];
    let output = cli.run(&args)?;
    let stdout = expect_exit(&output, 1, &args)?;

    assert_eq!(stdout, "");
    let stderr = text(&output.stderr);
    ensure!(stderr.contains("Code: AccessDenied"), "{stderr}");
    ensure!(!stderr.contains("NotImplemented"), "{stderr}");
    assert_all(&[rejected, denied]);
    Ok(())
}

// `bucket` in a host's config was documented as a default bucket but is not used:
// the bucket always comes from the path. It and any other unknown key are ignored,
// so existing configs keep working.
#[test]
fn host_bucket_and_unknown_config_keys_are_ignored() -> Result<()> {
    let mut server = Server::new();
    let policy = "<AccessControlPolicy/>";
    let get = server
        .mock("GET", OBJECT_PATH)
        .match_query(Matcher::Any)
        .match_request(exact_query(&[("acl", "")]))
        .with_body(policy)
        .expect(1)
        .create();
    let elsewhere = server
        .mock("GET", Matcher::Regex("^/default-bucket".into()))
        .expect(0)
        .create();
    let cli = Cli::new(&server.url())?;
    let mut config = fs::read_to_string(&cli.config)?;
    config.push_str("\n    bucket: default-bucket\n    bucket_foo: x\nsettings:\n  anything: 1\n");
    fs::write(&cli.config, config)?;

    let args = ["acl", OBJECT];
    let output = cli.run(&args)?;
    let stdout = expect_exit(&output, 0, &args)?;

    assert_eq!(stdout, format!("{policy}\n"));
    assert_eq!(text(&output.stderr), "");
    get.assert();
    elsewhere.assert();
    Ok(())
}

// A missing or misspelled key used to be sent as an empty credential and fail as a
// signature error from the server. It is now reported before any request, unless
// the requests are unsigned (--no-sign-request, for public buckets).
#[test]
fn missing_host_keys_fail_before_any_request() -> Result<()> {
    let mut server = Server::new();
    let cli = Cli::new(&server.url())?;
    let url = server.url();
    fs::write(
        &cli.config,
        format!(
            "---\nhosts:\n  s3:\n    endpoint: {url}\n    region: {REGION}\n    acess_key: {ACCESS_KEY}\n    secret_key: {SECRET_KEY}\n"
        ),
    )?;
    let forbidden = forbid_requests(&mut server);
    let args = ["acl", OBJECT];
    let output = Command::new(env!("CARGO_BIN_EXE_s3m"))
        .env("HOME", cli.home.path())
        .env_remove("AWS_ACCESS_KEY_ID")
        .env_remove("AWS_SECRET_ACCESS_KEY")
        .current_dir(&cli.work)
        .arg("--config")
        .arg(&cli.config)
        .args(args)
        .output()?;
    expect_exit(&output, 1, &args)?;
    let stderr = text(&output.stderr);
    ensure!(
        stderr.contains(
            "hosts.s3 has no access_key in the config; set it, or use --no-sign-request for a public bucket"
        ),
        "{stderr}"
    );
    assert_all(&forbidden);

    // Unsigned requests need no keys.
    let policy = "<AccessControlPolicy/>";
    let get = server
        .mock("GET", OBJECT_PATH)
        .match_query(Matcher::Any)
        .match_header("authorization", Matcher::Missing)
        .with_body(policy)
        .expect(1)
        .create();
    let args = ["--no-sign-request", "acl", OBJECT];
    let output = Command::new(env!("CARGO_BIN_EXE_s3m"))
        .env("HOME", cli.home.path())
        .env_remove("AWS_ACCESS_KEY_ID")
        .env_remove("AWS_SECRET_ACCESS_KEY")
        .current_dir(&cli.work)
        .arg("--config")
        .arg(&cli.config)
        .args(args)
        .output()?;
    let stdout = expect_exit(&output, 0, &args)?;
    assert_eq!(stdout, format!("{policy}\n"));
    get.assert();

    // A presigned URL is always signed: share needs the keys, with or without
    // --no-sign-request, and doesn't suggest it.
    // The first stderr line: with RUST_BACKTRACE set a stack trace follows it.
    for (args, message) in [
        (
            &["share", OBJECT][..],
            "Error: hosts.s3 has no access_key in the config; set it",
        ),
        (
            &["--no-sign-request", "share", OBJECT][..],
            "Error: share signs the URL with the host's keys, so it can't be used with --no-sign-request",
        ),
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_s3m"))
            .env("HOME", cli.home.path())
            .env_remove("AWS_ACCESS_KEY_ID")
            .env_remove("AWS_SECRET_ACCESS_KEY")
            .current_dir(&cli.work)
            .arg("--config")
            .arg(&cli.config)
            .args(args)
            .output()?;
        expect_exit(&output, 1, args)?;
        let stderr = text(&output.stderr);
        ensure!(stderr.lines().next() == Some(message), "{args:?}: {stderr}");
    }
    Ok(())
}

#[test]
fn acl_without_flag_prints_the_current_policy_document() -> Result<()> {
    let mut server = Server::new();
    let policy = r#"<?xml version="1.0" encoding="UTF-8"?><AccessControlPolicy><Owner><ID>owner-1</ID></Owner><AccessControlList><Grant><Permission>READ</Permission></Grant></AccessControlList></AccessControlPolicy>"#;
    let get = server
        .mock("GET", OBJECT_PATH)
        .match_query(Matcher::Any)
        .match_request(exact_query(&[("acl", "")]))
        .with_body(policy)
        .expect(1)
        .create();
    let put = server.mock("PUT", Matcher::Any).expect(0).create();
    let cli = Cli::new(&server.url())?;

    let args = ["acl", OBJECT];
    let stdout = expect_exit(&cli.run(&args)?, 0, &args)?;

    assert_eq!(stdout, format!("{policy}\n"));
    get.assert();
    put.assert();
    Ok(())
}

#[test]
fn acl_flag_puts_every_canned_value_as_a_signed_header() -> Result<()> {
    let mut server = Server::new();
    let get = server.mock("GET", Matcher::Any).expect(0).create();
    let cli = Cli::new(&server.url())?;

    for acl in ACLS {
        let put = server
            .mock("PUT", OBJECT_PATH)
            .match_query(Matcher::Any)
            .match_request(exact_query(&[("acl", "")]))
            .match_header("x-amz-acl", acl)
            .match_header(
                "authorization",
                Matcher::Regex("SignedHeaders=[^,]*x-amz-acl".to_string()),
            )
            .with_status(200)
            .expect(2)
            .create();
        for flag in ["--acl", "-a"] {
            let args = ["acl", OBJECT, flag, acl];
            let stdout = expect_exit(&cli.run(&args)?, 0, &args)?;
            assert_eq!(stdout, "", "{args:?}");
        }
        put.assert();
    }
    get.assert();
    Ok(())
}

#[test]
fn acl_rejects_invalid_values_and_targets_without_requests() -> Result<()> {
    let mut server = Server::new();
    let forbidden = forbid_requests(&mut server);
    let cli = Cli::new(&server.url())?;

    for (args, code, message) in [
        (
            vec!["acl", OBJECT, "--acl", "public"],
            2,
            "invalid value 'public' for '--acl <acl>'",
        ),
        (
            vec!["acl", OBJECT, "-a", "PRIVATE"],
            2,
            "invalid value 'PRIVATE' for '--acl <acl>'",
        ),
        (
            vec!["acl", OBJECT, "--acl"],
            2,
            "a value is required for '--acl <acl>'",
        ),
        (vec!["acl", "s3/bucket"], 1, "file name missing"),
        (vec!["acl", "s3/bucket/"], 1, "file name missing"),
        (vec!["acl", "s3"], 1, "Bucket name missing"),
    ] {
        expect_failure(&cli, &args, code, message)?;
    }
    assert_all(&forbidden);
    Ok(())
}

#[test]
fn acl_service_errors_exit_nonzero_without_stdout() -> Result<()> {
    let mut server = Server::new();
    let get = server
        .mock("GET", OBJECT_PATH)
        .match_query(Matcher::Any)
        .with_status(404)
        .with_body(s3_error("NoSuchKey"))
        .expect(1)
        .create();
    let put = server
        .mock("PUT", OBJECT_PATH)
        .match_query(Matcher::Any)
        .with_status(403)
        .with_body(s3_error("AccessDenied"))
        .expect(1)
        .create();
    let cli = Cli::new(&server.url())?;

    expect_failure(&cli, &["acl", OBJECT], 1, "Code: NoSuchKey")?;
    expect_failure(
        &cli,
        &["acl", OBJECT, "--acl", "public-read"],
        1,
        "Code: AccessDenied",
    )?;
    get.assert();
    put.assert();
    Ok(())
}

#[test]
fn cb_sends_default_private_and_every_canned_acl_without_object_lock() -> Result<()> {
    let mut cases = vec![(Vec::new(), "private")];
    for acl in ACLS {
        for flag in ["--acl", "-a"] {
            cases.push((vec![flag, acl], acl));
        }
    }

    for (flags, acl) in cases {
        let mut server = Server::new();
        let create = server
            .mock("PUT", "/new-bucket")
            .match_query(Matcher::Any)
            .match_request(exact_query(&[]))
            .match_header("x-amz-acl", acl)
            .match_header("x-amz-bucket-object-lock-enabled", Matcher::Missing)
            .match_body(Matcher::Regex(format!(
                r"<CreateBucketConfiguration [^>]*>\s*<LocationConstraint>{REGION}</LocationConstraint>\s*</CreateBucketConfiguration>"
            )))
            .with_header("location", "/new-bucket")
            .expect(1)
            .create();
        let cli = Cli::new(&server.url())?;
        let args = [vec!["cb", "s3/new-bucket"], flags].concat();

        let stdout = expect_exit(&cli.run(&args)?, 0, &args)?;

        assert_eq!(stdout, "location: /new-bucket\n", "{args:?}");
        create.assert();
    }
    Ok(())
}

#[test]
fn cb_existing_bucket_and_denied_creation_exit_nonzero() -> Result<()> {
    for (status, code) in [(409, "BucketAlreadyOwnedByYou"), (403, "AccessDenied")] {
        let mut server = Server::new();
        let create = server
            .mock("PUT", "/new-bucket")
            .with_status(status)
            .with_body(s3_error(code))
            .expect(1)
            .create();
        let cli = Cli::new(&server.url())?;

        expect_failure(&cli, &["cb", "s3/new-bucket"], 1, &format!("Code: {code}"))?;
        create.assert();
    }
    Ok(())
}

#[test]
fn cb_rejects_missing_or_invalid_targets_without_requests() -> Result<()> {
    let mut server = Server::new();
    let forbidden = forbid_requests(&mut server);
    let cli = Cli::new(&server.url())?;

    for (args, code, message) in [
        (vec!["cb", "s3"], 1, "Bucket name missing"),
        (vec!["cb", "s3/"], 1, "Bucket name missing"),
        (
            vec!["cb", "s3/Invalid_Bucket"],
            1,
            "Invalid bucket name 'Invalid_Bucket'",
        ),
        (vec!["cb", "s3/ab"], 1, "Must be 3-63 characters long"),
        (vec!["cb", "s3/-leading-dash"], 1, "Must match pattern"),
        (
            vec!["cb", "s3/new-bucket", "--acl", "public"],
            2,
            "invalid value 'public' for '--acl <acl>'",
        ),
        (
            vec!["cb", "s3/new-bucket", "-a"],
            2,
            "a value is required for '--acl <acl>'",
        ),
    ] {
        expect_failure(&cli, &args, code, message)?;
    }
    assert_all(&forbidden);
    Ok(())
}

#[test]
fn du_text_totals_follow_every_page_for_bucket_and_prefix_targets() -> Result<()> {
    let mut server = Server::new();
    let token = "tok/1+=";
    let first = list_mock(
        &mut server,
        &[("list-type", "2"), ("prefix", "logs/")],
        list_page(
            &[
                ("logs/a", 100, "2026-03-13T00:00:00.000Z"),
                ("logs/b", 2048, "2026-03-14T00:00:00.000Z"),
            ],
            Some(token),
        ),
    );
    let second = list_mock(
        &mut server,
        &[
            ("list-type", "2"),
            ("prefix", "logs/"),
            ("continuation-token", token),
        ],
        list_page(&[("logs/c", 1_048_576, "2026-03-14T00:00:00.000Z")], None),
    );
    let bucket = list_mock(
        &mut server,
        &[("list-type", "2")],
        list_page(&[("only", 5, "2026-03-14T00:00:00.000Z")], None),
    );
    let cli = Cli::new(&server.url())?;

    let prefix = expect_exit(&cli.run(&["du", "s3/bucket/logs/"])?, 0, &["du"])?;
    let single = expect_exit(&cli.run(&["du", "s3/bucket"])?, 0, &["du"])?;

    assert_eq!(prefix, "s3/bucket/logs/: 3 objects, 1.0 MiB (1050724 B)\n");
    assert_eq!(single, "s3/bucket: 1 object, 5 B (5 B)\n");
    first.assert();
    second.assert();
    bucket.assert();

    let mut empty_server = Server::new();
    let empty = list_mock(
        &mut empty_server,
        &[("list-type", "2")],
        list_page(&[], None),
    );
    let empty_cli = Cli::new(&empty_server.url())?;
    let args = ["du", "s3/bucket", "--json"];
    let stdout = expect_exit(&empty_cli.run(&args)?, 0, &args)?;
    assert_eq!(
        serde_json::from_str::<Value>(&stdout)?,
        json!({"target": "s3/bucket", "group_by": null, "summary": {"object_count": 0, "bytes": 0, "human_size": "0 B"}})
    );
    empty.assert();
    Ok(())
}

fn grouped_listing(server: &mut ServerGuard) -> (Mock, Mock) {
    let first = list_mock(
        server,
        &[("list-type", "2")],
        list_page(
            &[
                ("a", 10, "2026-03-13T23:59:59.999Z"),
                ("b", 20, "2026-03-14T00:00:00.000Z"),
                ("c", 30, "2026-03-15T01:30:00+02:00"),
            ],
            Some("p2"),
        ),
    );
    let second = list_mock(
        server,
        &[("list-type", "2"), ("continuation-token", "p2")],
        list_page(&[("d", 40, "2026-03-16T12:00:00Z")], None),
    );
    (first, second)
}

#[test]
fn du_group_by_day_buckets_objects_by_utc_date_in_text_and_json() -> Result<()> {
    let mut server = Server::new();
    let (first, second) = grouped_listing(&mut server);
    let cli = Cli::new(&server.url())?;
    let args = ["du", "s3/bucket", "--group-by", "day"];

    let stdout = expect_exit(&cli.run(&args)?, 0, &args)?;

    assert_eq!(
        stdout,
        "s3/bucket grouped by UTC day:\n\
         DATE        OBJECTS       SIZE    RAW_BYTES\n\
         2026-03-13        1       10 B         10 B\n\
         2026-03-14        2       50 B         50 B\n\
         2026-03-16        1       40 B         40 B\n\
         TOTAL             4      100 B        100 B\n"
    );
    first.assert();
    second.assert();

    let mut json_server = Server::new();
    let (json_first, json_second) = grouped_listing(&mut json_server);
    let json_cli = Cli::new(&json_server.url())?;
    let args = ["du", "s3/bucket", "--group-by", "day", "--json"];
    let stdout = expect_exit(&json_cli.run(&args)?, 0, &args)?;
    assert_eq!(
        serde_json::from_str::<Value>(&stdout)?,
        json!({
            "target": "s3/bucket",
            "group_by": "day",
            "days": [
                {"date": "2026-03-13", "object_count": 1, "bytes": 10, "human_size": "10 B"},
                {"date": "2026-03-14", "object_count": 2, "bytes": 50, "human_size": "50 B"},
                {"date": "2026-03-16", "object_count": 1, "bytes": 40, "human_size": "40 B"}
            ],
            "total": {"object_count": 4, "bytes": 100, "human_size": "100 B"}
        })
    );
    json_first.assert();
    json_second.assert();
    Ok(())
}

#[test]
fn du_rejects_invalid_grouping_and_targets_without_requests() -> Result<()> {
    let mut server = Server::new();
    let forbidden = forbid_requests(&mut server);
    let cli = Cli::new(&server.url())?;

    for (args, code, message) in [
        (
            vec!["du", "s3/bucket", "--group-by", "week"],
            2,
            "invalid value 'week' for '--group-by <group-by>'",
        ),
        (
            vec!["du", "s3/bucket", "--group-by", "DAY"],
            2,
            "invalid value 'DAY' for '--group-by <group-by>'",
        ),
        (
            vec!["du", "s3/bucket", "--group-by"],
            2,
            "a value is required for '--group-by <group-by>'",
        ),
        (vec!["du", "s3"], 1, "Bucket name missing"),
    ] {
        expect_failure(&cli, &args, code, message)?;
    }
    assert_all(&forbidden);
    Ok(())
}

#[test]
fn du_listing_failures_exit_nonzero_without_a_report() -> Result<()> {
    let cases = [
        (
            403,
            s3_error("AccessDenied"),
            vec!["du", "s3/bucket", "--json"],
            "Code: AccessDenied",
        ),
        (
            200,
            list_page(&[("a", 1, "2026-03-14T00:00:00Z")], None)
                .replace("<IsTruncated>false", "<IsTruncated>true"),
            vec!["du", "s3/bucket"],
            "truncated response without a continuation token",
        ),
        (
            200,
            list_page(&[("a", 1, "yesterday")], None),
            vec!["du", "s3/bucket", "--group-by", "day", "--json"],
            "Failed to parse LastModified for object 'a': yesterday",
        ),
    ];

    for (status, body, args, message) in cases {
        let mut server = Server::new();
        let list = server
            .mock("GET", "/bucket")
            .match_query(Matcher::Any)
            .with_status(status)
            .with_body(body)
            .expect(1)
            .create();
        let cli = Cli::new(&server.url())?;

        expect_failure(&cli, &args, 1, message)?;
        list.assert();
    }
    Ok(())
}

fn payload() -> Vec<u8> {
    (0..=u8::MAX).cycle().take(70_001).collect()
}

fn object_mock(server: &mut ServerGuard, body: &[u8], expected: usize) -> Mock {
    server
        .mock("GET", OBJECT_PATH)
        .match_query(Matcher::Any)
        .match_request(exact_query(&[]))
        .with_body(body)
        .expect(expected)
        .create()
}

#[test]
fn get_writes_exact_bytes_to_default_file_and_directory_destinations() -> Result<()> {
    let mut server = Server::new();
    let bytes = payload();
    let get = object_mock(&mut server, &bytes, 4);
    let cli = Cli::new(&server.url())?;
    let named = cli.work.join("named.out");
    let directory = cli.home.path().join("into");
    fs::create_dir(&directory)?;
    let missing_parent = cli.home.path().join("absent").join("x.bin");

    for (destination, written) in [
        (None, cli.work.join("data.bin")),
        (Some(named.clone()), named),
        (Some(directory.clone()), directory.join("data.bin")),
    ] {
        let mut args = vec!["get".to_string(), OBJECT.to_string()];
        args.extend(destination.map(|path| path.display().to_string()));
        let args: Vec<&str> = args.iter().map(String::as_str).collect();

        let stdout = expect_exit(&cli.run(&args)?, 0, &args)?;

        assert_eq!(stdout, "", "{args:?}");
        let downloaded = fs::read(&written)?;
        assert_eq!(downloaded.len(), bytes.len(), "{args:?}");
        assert_eq!(blake3::hash(&downloaded), blake3::hash(&bytes), "{args:?}");
    }
    expect_failure(
        &cli,
        &["get", OBJECT, &missing_parent.display().to_string()],
        1,
        "does not exist",
    )?;
    ensure!(
        !missing_parent.exists(),
        "created {}",
        missing_parent.display()
    );
    get.assert();
    Ok(())
}

#[test]
fn get_refuses_an_existing_destination_unless_forced() -> Result<()> {
    let mut server = Server::new();
    let fresh = b"fresh object bytes".to_vec();
    let get = object_mock(&mut server, &fresh, 3);
    let cli = Cli::new(&server.url())?;
    let existing = cli.work.join("data.bin");
    let original = vec![b'x'; 4096];

    fs::write(&existing, &original)?;
    expect_failure(&cli, &["get", OBJECT], 1, "already exists")?;
    assert_eq!(fs::read(&existing)?, original, "refusal mutated the file");

    for flag in ["--force", "-f"] {
        fs::write(&existing, &original)?;
        let args = ["get", OBJECT, flag];
        let stdout = expect_exit(&cli.run(&args)?, 0, &args)?;
        assert_eq!(stdout, "");
        assert_eq!(
            fs::read(&existing)?,
            fresh,
            "{flag} did not replace the file"
        );
    }
    get.assert();
    Ok(())
}

#[test]
fn get_version_selects_the_requested_object_version() -> Result<()> {
    let mut server = Server::new();
    let old = server
        .mock("GET", OBJECT_PATH)
        .match_query(Matcher::Any)
        .match_request(exact_query(&[("versionId", "v-old")]))
        .with_body("old bytes")
        .expect(1)
        .create();
    let latest = object_mock(&mut server, b"latest bytes", 1);
    let head = server
        .mock("HEAD", OBJECT_PATH)
        .match_query(Matcher::Any)
        .match_request(exact_query(&[("versionId", "v-old")]))
        .with_header("content-length", "9")
        .with_header("x-amz-version-id", "v-old")
        .expect(1)
        .create();
    let cli = Cli::new(&server.url())?;

    expect_exit(
        &cli.run(&["get", OBJECT, "old.bin", "--version", "v-old"])?,
        0,
        &[],
    )?;
    expect_exit(&cli.run(&["get", OBJECT, "latest.bin"])?, 0, &[])?;
    let args = ["get", OBJECT, "-m", "--version", "v-old", "--json"];
    let stdout = expect_exit(&cli.run(&args)?, 0, &args)?;

    assert_eq!(fs::read(cli.work.join("old.bin"))?, b"old bytes");
    assert_eq!(fs::read(cli.work.join("latest.bin"))?, b"latest bytes");
    let meta: Value = serde_json::from_str(&stdout)?;
    assert_eq!(meta.get("version_id"), Some(&json!("v-old")));
    assert_eq!(meta.get("content_length"), Some(&json!(9)));
    old.assert();
    latest.assert();
    head.assert();
    Ok(())
}

fn head_mock(server: &mut ServerGuard, expected: usize) -> Mock {
    server
        .mock("HEAD", OBJECT_PATH)
        .match_query(Matcher::Any)
        .match_request(exact_query(&[]))
        .match_header("x-amz-checksum-mode", "ENABLED")
        .with_header("content-length", "70001")
        .with_header("content-type", "application/octet-stream")
        .with_header("etag", "\"meta-etag\"")
        .with_header("last-modified", "Sat, 14 Mar 2026 08:00:00 GMT")
        .with_header("x-amz-checksum-sha256", "c2hhMjU2")
        .with_header("x-amz-meta-owner", "alice")
        .expect(expected)
        .create()
}

#[test]
fn get_meta_reads_headers_without_downloading_the_body() -> Result<()> {
    let mut server = Server::new();
    let head = head_mock(&mut server, 3);
    let get = server.mock("GET", Matcher::Any).expect(0).create();
    let cli = Cli::new(&server.url())?;

    for flag in ["--meta", "-m"] {
        let args = ["get", OBJECT, flag];
        let stdout = expect_exit(&cli.run(&args)?, 0, &args)?;
        let fields: Vec<(&str, &str)> = stdout
            .lines()
            .filter_map(|line| line.split_once(':'))
            .map(|(name, value)| (name, value.trim()))
            .collect();
        for expected in [
            ("content-length", "70001"),
            ("etag", "\"meta-etag\""),
            ("x-amz-checksum-sha256", "c2hhMjU2"),
            ("x-amz-meta-owner", "alice"),
        ] {
            ensure!(
                fields.contains(&expected),
                "{flag}: {expected:?} in {stdout}"
            );
        }
    }

    let args = ["get", OBJECT, "--meta", "--json"];
    let stdout = expect_exit(&cli.run(&args)?, 0, &args)?;
    let meta: Value = serde_json::from_str(&stdout)?;
    let summary: Vec<Option<&Value>> = [
        "bucket",
        "key",
        "version_id",
        "content_length",
        "content_type",
        "etag",
        "last_modified",
        "checksum_sha256",
        "metadata",
    ]
    .iter()
    .map(|field| meta.get(field))
    .collect();
    assert_eq!(
        summary,
        [
            Some(&json!("bucket")),
            Some(&json!("dir/data.bin")),
            Some(&Value::Null),
            Some(&json!(70_001)),
            Some(&json!("application/octet-stream")),
            Some(&json!("\"meta-etag\"")),
            Some(&json!("2026-03-14T08:00:00+00:00")),
            Some(&json!("c2hhMjU2")),
            Some(&json!({"owner": "alice"})),
        ]
    );
    assert_eq!(cli.work_entries()?, Vec::<String>::new());
    head.assert();
    get.assert();
    Ok(())
}

fn versions_body(entries: &[(&str, bool, &str, u64)]) -> String {
    let mut versions = String::new();
    for (version_id, is_latest, last_modified, size) in entries {
        let _ = write!(
            versions,
            "<Version><Key>dir/data.bin</Key><VersionId>{version_id}</VersionId><IsLatest>{is_latest}</IsLatest><LastModified>{last_modified}</LastModified><ETag>\"e-{version_id}\"</ETag><Size>{size}</Size><Owner><ID>o</ID></Owner><StorageClass>STANDARD</StorageClass></Version>"
        );
    }
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><ListVersionsResult><Name>bucket</Name><Prefix>dir/data.bin</Prefix><KeyMarker></KeyMarker><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>{versions}</ListVersionsResult>"#
    )
}

#[test]
fn get_versions_lists_every_version_as_text_and_json() -> Result<()> {
    let mut server = Server::new();
    let list = server
        .mock("GET", "/bucket")
        .match_query(Matcher::Any)
        .match_request(exact_query(&[("versions", ""), ("prefix", "dir/data.bin")]))
        .with_body(versions_body(&[
            ("v2", true, "2026-03-14T08:00:00.000Z", 42),
            ("v1", false, "2026-03-13T07:00:00.000Z", 7),
        ]))
        .expect(2)
        .create();
    let get = server.mock("GET", OBJECT_PATH).expect(0).create();
    let cli = Cli::new(&server.url())?;

    let text_out = expect_exit(&cli.run(&["get", OBJECT, "--versions"])?, 0, &[])?;
    let args = ["get", OBJECT, "--versions", "--json"];
    let json_out = expect_exit(&cli.run(&args)?, 0, &args)?;

    assert_eq!(
        text_out,
        "[2026-03-14 08:00:00 UTC]       42 B dir/data.bin (latest) ID: v2\n\
         [2026-03-13 07:00:00 UTC]        7 B dir/data.bin ID: v1\n"
    );
    assert_eq!(
        serde_json::from_str::<Value>(&json_out)?,
        json!({
            "bucket": "bucket",
            "key_prefix": "dir/data.bin",
            "versions": [
                {"key": "dir/data.bin", "version_id": "v2", "is_latest": true, "last_modified": "2026-03-14T08:00:00.000Z", "size_bytes": 42, "etag": "\"e-v2\"", "storage_class": "STANDARD"},
                {"key": "dir/data.bin", "version_id": "v1", "is_latest": false, "last_modified": "2026-03-13T07:00:00.000Z", "size_bytes": 7, "etag": "\"e-v1\"", "storage_class": "STANDARD"}
            ]
        })
    );
    assert_eq!(cli.work_entries()?, Vec::<String>::new());
    list.assert();
    get.assert();

    let mut empty_server = Server::new();
    let empty = empty_server
        .mock("GET", "/bucket")
        .match_query(Matcher::Any)
        .with_body(versions_body(&[]))
        .expect(1)
        .create();
    let empty_cli = Cli::new(&empty_server.url())?;
    let stdout = expect_exit(&empty_cli.run(&["get", OBJECT, "--versions"])?, 0, &[])?;
    assert_eq!(stdout, "No versions found for key: dir/data.bin\n");
    empty.assert();
    Ok(())
}

#[test]
fn get_rejects_invalid_modes_and_targets_without_requests() -> Result<()> {
    let mut server = Server::new();
    let forbidden = forbid_requests(&mut server);
    let cli = Cli::new(&server.url())?;

    for (args, code, message) in [
        (
            vec!["get", OBJECT, "--json"],
            1,
            "--json is only supported with `s3m get --meta` or `s3m get --versions`",
        ),
        (
            vec!["get", OBJECT, "--version"],
            2,
            "a value is required for '--version <version>'",
        ),
        (vec!["get", OBJECT, "a", "b"], 2, "unexpected value 'b'"),
        (vec!["get", "s3/bucket"], 1, "file name missing"),
        (vec!["get", "s3/bucket/", "-m"], 1, "file name missing"),
        (vec!["get", "s3"], 1, "Bucket name missing"),
    ] {
        expect_failure(&cli, &args, code, message)?;
    }
    assert_eq!(cli.work_entries()?, Vec::<String>::new());
    assert_all(&forbidden);
    Ok(())
}

#[test]
fn get_service_failures_leave_destinations_untouched() -> Result<()> {
    let cases: [(&str, usize, String, Vec<&str>, &str); 6] = [
        (
            "GET",
            404,
            s3_error("NoSuchKey"),
            vec!["get", OBJECT, "-f"],
            "Code: NoSuchKey",
        ),
        (
            "GET",
            403,
            s3_error("AccessDenied"),
            vec!["get", OBJECT, "-f"],
            "Code: AccessDenied",
        ),
        (
            "HEAD",
            404,
            String::new(),
            vec!["get", OBJECT, "-m"],
            "404 Not Found",
        ),
        (
            "GET",
            200,
            "not xml".to_string(),
            vec!["get", OBJECT, "--versions"],
            "Error",
        ),
        (
            "GET",
            200,
            String::new(),
            vec!["get", OBJECT, "--force"],
            "could not get content_length",
        ),
        (
            "HEAD",
            200,
            String::new(),
            vec!["get", OBJECT, "-m", "--json"],
            "Failed to parse Last-Modified header",
        ),
    ];

    for (method, status, body, args, message) in cases {
        let mut server = Server::new();
        let mock = server
            .mock(method, Matcher::Any)
            .match_query(Matcher::Any)
            .with_status(status)
            .with_header("last-modified", "not a date");
        let mock = if body.is_empty() && method == "GET" {
            // A chunked body has no Content-Length.
            mock.with_chunked_body(|writer| writer.write_all(b"partial"))
        } else {
            mock.with_body(body)
        };
        let mock = mock.expect(1).create();
        let cli = Cli::new(&server.url())?;
        let existing = cli.work.join("data.bin");
        fs::write(&existing, b"keep me")?;

        expect_failure(&cli, &args, 1, message)?;

        assert_eq!(
            fs::read(&existing)?,
            b"keep me",
            "{args:?} touched the file"
        );
        assert_eq!(cli.work_entries()?, ["data.bin"], "{args:?}");
        mock.assert();
    }
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn get_progress_reaches_a_terminal_unless_quiet() -> Result<()> {
    let bytes = vec![7_u8; 3 * 1024];
    for flag in [None, Some("--quiet"), Some("-q")] {
        let mut server = Server::new();
        let get = object_mock(&mut server, &bytes, 1);
        let cli = Cli::new(&server.url())?;
        let destination = cli.work.join("out.bin");
        let command = format!(
            "{} --config {} get {} {OBJECT} {}",
            env!("CARGO_BIN_EXE_s3m"),
            cli.config.display(),
            flag.unwrap_or(""),
            destination.display()
        );

        let output = Command::new("script")
            .env("HOME", cli.home.path())
            .env("TERM", "xterm")
            .current_dir(&cli.work)
            .args(["-q", "-e", "-c", &command, "/dev/null"])
            .output()?;

        let terminal = expect_exit(&output, 0, &[&command])?;
        match flag {
            None => ensure!(
                terminal.contains("3.00 KiB/3.00 KiB"),
                "missing terminal progress: {terminal}"
            ),
            Some(_) => ensure!(
                output.stdout.is_empty() && output.stderr.is_empty(),
                "{flag:?} terminal emitted: {terminal}"
            ),
        }
        assert_eq!(fs::read(&destination)?, bytes, "{flag:?}");
        get.assert();
    }
    Ok(())
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().fold(String::new(), |mut out, byte| {
        let _ = write!(out, "{byte:02x}");
        out
    })
}

fn hmac_sha256(key: &[u8], data: &str) -> Vec<u8> {
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, key);
    ring::hmac::sign(&key, data.as_bytes()).as_ref().to_vec()
}

/// `SigV4` `UriEncode`: unreserved characters stay, every other byte is `%XX`.
fn uri_encode(value: &str, keep_slash: bool) -> String {
    value.bytes().fold(String::new(), |mut out, byte| {
        if byte.is_ascii_alphanumeric() || b"-._~".contains(&byte) || (keep_slash && byte == b'/') {
            out.push(char::from(byte));
        } else {
            let _ = write!(out, "%{byte:02X}");
        }
        out
    })
}

fn percent_decode(value: &str) -> Result<String> {
    let mut bytes = Vec::new();
    let mut rest = value.as_bytes();
    while let Some((&byte, tail)) = rest.split_first() {
        if byte == b'%' {
            let digits = tail.get(..2).context("truncated escape")?;
            bytes.push(u8::from_str_radix(std::str::from_utf8(digits)?, 16)?);
            rest = tail.get(2..).context("truncated escape")?;
        } else {
            bytes.push(byte);
            rest = tail;
        }
    }
    Ok(String::from_utf8(bytes)?)
}

/// Recomputes the presigned GET signature from the URL's own inputs following
/// <https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html>.
fn verify_presigned(printed: &str, endpoint: &str, key: &str, expires: &str) -> Result<()> {
    let url = url::Url::parse(printed.strip_suffix('\n').context("no newline")?)?;
    let base = url::Url::parse(endpoint)?;
    let host = format!(
        "{}:{}",
        base.host_str().context("host")?,
        base.port().context("port")?
    );
    ensure!(url.origin() == base.origin(), "{url} is not on {endpoint}");
    let path = percent_decode(url.path())?;
    ensure!(path == format!("/bucket/{key}"), "path {path}");

    let mut pairs: Vec<(String, String)> = url.query_pairs().into_owned().collect();
    let names: Vec<&str> = pairs.iter().map(|(name, _)| name.as_str()).collect();
    ensure!(
        names
            == [
                "X-Amz-Algorithm",
                "X-Amz-Credential",
                "X-Amz-Date",
                "X-Amz-Expires",
                "X-Amz-SignedHeaders",
                "X-Amz-Signature"
            ],
        "query {names:?}"
    );
    let (_, signature) = pairs.pop().context("signature")?;
    let value = |name: &str| {
        pairs
            .iter()
            .find(|(candidate, _)| candidate == name)
            .map(|(_, value)| value.clone())
            .context("missing query value")
    };
    let datetime = value("X-Amz-Date")?;
    jiff::civil::DateTime::strptime("%Y%m%dT%H%M%SZ", &datetime)?;
    let date = datetime.get(..8).context("date")?;
    let scope = format!("{date}/{REGION}/s3/aws4_request");
    ensure!(value("X-Amz-Algorithm")? == "AWS4-HMAC-SHA256");
    ensure!(value("X-Amz-Credential")? == format!("{ACCESS_KEY}/{scope}"));
    ensure!(value("X-Amz-Expires")? == expires, "expires {pairs:?}");
    ensure!(value("X-Amz-SignedHeaders")? == "host");

    let mut canonical_query: Vec<String> = pairs
        .iter()
        .map(|(name, value)| format!("{}={}", uri_encode(name, false), uri_encode(value, false)))
        .collect();
    canonical_query.sort();
    let canonical_request = format!(
        "GET\n{}\n{}\nhost:{host}\n\nhost\nUNSIGNED-PAYLOAD",
        uri_encode(&path, true),
        canonical_query.join("&")
    );
    let digest = ring::digest::digest(&ring::digest::SHA256, canonical_request.as_bytes());
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{datetime}\n{scope}\n{}",
        hex(digest.as_ref())
    );
    let signing_key = ["s3", "aws4_request"].iter().fold(
        hmac_sha256(
            &hmac_sha256(format!("AWS4{SECRET_KEY}").as_bytes(), date),
            REGION,
        ),
        |key, part| hmac_sha256(&key, part),
    );
    ensure!(
        signature == hex(&hmac_sha256(&signing_key, &string_to_sign)),
        "signature mismatch for {printed}"
    );
    Ok(())
}

#[test]
fn share_prints_a_presigned_get_signed_for_the_requested_expiry() -> Result<()> {
    let mut server = Server::new();
    let forbidden = forbid_requests(&mut server);
    let cli = Cli::new(&server.url())?;
    let key = "reports/Q1 2026/résumé (final)~v1.txt";
    let target = format!("s3/bucket/{key}");

    for (flags, expires) in [
        (vec![], "43200"),
        (vec!["--expire", "43200"], "43200"),
        (vec!["-e", "604800"], "604800"),
        (vec!["--expire", "604800"], "604800"),
        (vec!["-e", "1"], "1"),
    ] {
        let args = [vec!["share", target.as_str()], flags].concat();
        let stdout = expect_exit(&cli.run(&args)?, 0, &args)?;
        verify_presigned(&stdout, &server.url(), key, expires)
            .with_context(|| format!("{args:?}"))?;
        ensure!(!stdout.contains(SECRET_KEY), "{args:?} leaked the secret");
    }
    assert_all(&forbidden);
    Ok(())
}

#[test]
fn share_rejects_expiry_outside_the_documented_range() -> Result<()> {
    let mut server = Server::new();
    let forbidden = forbid_requests(&mut server);
    let cli = Cli::new(&server.url())?;

    for (flags, message) in [
        (
            vec!["--expire", "0"],
            "invalid value '0' for '--expire <expire>'",
        ),
        (
            vec!["--expire", "604801"],
            "invalid value '604801' for '--expire <expire>'",
        ),
        (
            vec!["-e", "4294967296"],
            "invalid value '4294967296' for '--expire <expire>'",
        ),
        (
            vec!["--expire", "18446744073709551616"],
            "invalid value '18446744073709551616' for '--expire <expire>'",
        ),
        (
            vec!["--expire=-1"],
            "invalid value '-1' for '--expire <expire>'",
        ),
        (vec!["-e", "-1"], "unexpected argument '-1'"),
        (
            vec!["--expire", "ten"],
            "invalid value 'ten' for '--expire <expire>'",
        ),
        (
            vec!["--expire", "1.5"],
            "invalid value '1.5' for '--expire <expire>'",
        ),
        (
            vec!["--expire", ""],
            "invalid value '' for '--expire <expire>'",
        ),
        (vec!["-e"], "a value is required for '--expire <expire>'"),
    ] {
        let args = [vec!["share", OBJECT], flags].concat();
        expect_failure(&cli, &args, 2, message)?;
    }
    for target in ["s3/bucket", "s3/bucket/"] {
        expect_failure(&cli, &["share", target], 1, "file name missing")?;
    }
    expect_failure(&cli, &["share", "s3"], 1, "Bucket name missing")?;
    assert_all(&forbidden);
    Ok(())
}
