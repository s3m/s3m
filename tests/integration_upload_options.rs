//! Upload options through Cargo's binary and independent wire/byte oracles.
use anyhow::{Context, Result, ensure};
use mockito::{Matcher, Server};
use std::fs;
#[path = "support/upload_fixture.rs"]
mod upload_fixture;
#[cfg(target_os = "linux")]
#[path = "support/upload_terminal.rs"]
mod upload_terminal;
use upload_fixture::{Fixture, complete, initiate, part_mock};
#[path = "support/upload_resume.rs"]
mod upload_resume;
#[path = "support/upload_wire.rs"]
mod upload_wire;

const INIT: &str = "<InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>key</Key><UploadId>UP1</UploadId></InitiateMultipartUploadResult>";
const DONE: &str = "<CompleteMultipartUploadResult><Location>loc</Location><Bucket>bucket</Bucket><Key>key</Key><ETag>done</ETag></CompleteMultipartUploadResult>";
const MIB: usize = 1024 * 1024;

#[test]
fn checksum_choices_send_known_digest_and_original_bytes() -> Result<()> {
    // Published "hello world" digest vectors; SHA vectors independently checked with openssl.
    for (algorithm, digest) in [
        ("crc32", "DUoRhQ=="),
        ("crc32c", "yZRlqg=="),
        ("sha1", "Kq5sNclPz7QV2+lfQIuc6R7oRu0="),
        ("sha256", "uU0nuZNNPgilLlLX2n2r+sSE7+N6U4DukIj3rOLvzek="),
    ] {
        let mut server = Server::new();
        let fixture = Fixture::new(&server.url(), b"hello world")?;
        let put = server
            .mock("PUT", "/bucket/key")
            .match_header(format!("x-amz-checksum-{algorithm}").as_str(), digest)
            .match_header("content-md5", Matcher::Missing)
            .match_body(b"hello world".to_vec())
            .with_header("etag", "digest-ok")
            .expect(1)
            .create();
        let output = fixture
            .command()
            .args(["--checksum", algorithm])
            .arg(&fixture.source)
            .arg("s3/bucket/key")
            .output()?;
        ensure!(
            output.status.success(),
            "{algorithm}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        put.assert();
        let report = format!("{algorithm} (base64): {digest}");
        ensure!(
            String::from_utf8_lossy(&output.stdout)
                .lines()
                .any(|line| line.trim() == report),
            "{algorithm}: {report} not in {}",
            String::from_utf8_lossy(&output.stdout)
        );
        println!(
            "checksum={algorithm} header={digest} transmitted=hello world exit={}",
            output.status
        );
    }
    Ok(())
}

// <https://github.com/s3m/s3m/issues/73>: Cloudflare R2 rejects Content-MD5 sent
// together with x-amz-checksum-*, so --checksum must replace it on every upload request.
#[test]
fn checksum_replaces_content_md5_on_single_and_multipart_uploads() -> Result<()> {
    let digest = "uU0nuZNNPgilLlLX2n2r+sSE7+N6U4DukIj3rOLvzek=";
    for (args, md5, checksum) in [
        (
            vec!["--checksum", "sha256"],
            Matcher::Missing,
            Matcher::Exact(digest.into()),
        ),
        (vec![], Matcher::Any, Matcher::Missing),
    ] {
        let mut server = Server::new();
        let fixture = Fixture::new(&server.url(), b"hello world")?;
        let put = server
            .mock("PUT", "/bucket/key")
            .match_header("content-md5", md5.clone())
            .match_header("x-amz-checksum-sha256", checksum)
            .match_body(b"hello world".to_vec())
            .with_header("etag", "single")
            .expect(1)
            .create();
        let output = fixture
            .command()
            .args(&args)
            .arg(&fixture.source)
            .arg("s3/bucket/key")
            .output()?;
        ensure!(
            output.status.success(),
            "single {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        put.assert();
        // On Windows each fixture holds the shared profile lock until dropped.
        drop(fixture);

        let mut server = Server::new();
        let payload = vec![7; 11 * MIB + 137];
        let fixture = Fixture::new(&server.url(), &payload)?;
        let init = initiate(&mut server)
            .match_header("content-md5", Matcher::Missing)
            .expect(1)
            .create();
        let checksum = if args.is_empty() {
            Matcher::Missing
        } else {
            Matcher::Any
        };
        let parts: Vec<_> = payload
            .chunks(10 * MIB)
            .enumerate()
            .map(|(i, bytes)| {
                part_mock(&mut server, i + 1, bytes)
                    .match_header("content-md5", md5.clone())
                    .match_header("x-amz-checksum-sha256", checksum.clone())
                    .expect(1)
                    .create()
            })
            .collect();
        // The parts' checksums travel in the body; R2 rejects the composite checksum
        // as an x-amz-checksum-* header on CompleteMultipartUpload (InvalidDigest).
        let part_checksums = if args.is_empty() {
            Matcher::Any
        } else {
            Matcher::Regex("<ChecksumSHA256>".into())
        };
        let done = complete(&mut server)
            .match_header("content-md5", Matcher::Missing)
            .match_header("x-amz-checksum-sha256", Matcher::Missing)
            .match_body(part_checksums)
            .expect(1)
            .create();
        let output = fixture
            .command()
            .args(["-n", "1"])
            .args(&args)
            .arg(&fixture.source)
            .arg("s3/bucket/key")
            .output()?;
        ensure!(
            output.status.success(),
            "multipart {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        init.assert();
        for part in parts {
            part.assert();
        }
        done.assert();
    }
    Ok(())
}

// --no-sign-request dropped every request header, not only the authorization: an
// upload went out chunked, without Content-Length, integrity header or metadata, and
// S3 rejected it. Unsigned uploads keep all of them.
#[test]
fn unsigned_uploads_keep_content_length_and_action_headers() -> Result<()> {
    let mut server = Server::new();
    let fixture = Fixture::new(&server.url(), b"hello world")?;
    let put = server
        .mock("PUT", "/bucket/key")
        .match_header("authorization", Matcher::Missing)
        .match_header("transfer-encoding", Matcher::Missing)
        .match_header("content-length", "11")
        .match_header("content-md5", "XrY7u+Ae7tCTyyK7j1rNww==")
        .match_header("x-amz-meta-owner", "me")
        .match_body(b"hello world".to_vec())
        .with_header("etag", "single")
        .expect(1)
        .create();
    let output = fixture
        .command()
        .args(["--no-sign-request", "--meta", "owner=me"])
        .arg(&fixture.source)
        .arg("s3/bucket/key")
        .output()?;
    ensure!(
        output.status.success(),
        "single: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    put.assert();
    // On Windows each fixture holds the shared profile lock until dropped.
    drop(fixture);

    let mut server = Server::new();
    let payload = vec![7; 11 * MIB + 137];
    let fixture = Fixture::new(&server.url(), &payload)?;
    let init = initiate(&mut server)
        .match_header("authorization", Matcher::Missing)
        .match_header("x-amz-checksum-algorithm", "SHA256")
        .expect(1)
        .create();
    let parts: Vec<_> = payload
        .chunks(10 * MIB)
        .enumerate()
        .map(|(i, bytes)| {
            part_mock(&mut server, i + 1, bytes)
                .match_header("authorization", Matcher::Missing)
                .match_header("transfer-encoding", Matcher::Missing)
                .match_header("content-length", bytes.len().to_string().as_str())
                .match_header("content-md5", Matcher::Missing)
                .match_header("x-amz-checksum-sha256", Matcher::Any)
                .expect(1)
                .create()
        })
        .collect();
    let done = complete(&mut server)
        .match_header("authorization", Matcher::Missing)
        .match_body(Matcher::Regex("<ChecksumSHA256>".into()))
        .expect(1)
        .create();
    let output = fixture
        .command()
        .args(["--no-sign-request", "-n", "1", "--checksum", "sha256"])
        .arg(&fixture.source)
        .arg("s3/bucket/key")
        .output()?;
    ensure!(
        output.status.success(),
        "multipart: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    init.assert();
    for part in parts {
        part.assert();
    }
    done.assert();
    Ok(())
}

// <https://github.com/s3m/s3m/issues/76>: a host that compresses or encrypts (set in
// the config) streams the upload, and --checksum was silently ignored. The --compress
// flag already conflicts with --checksum; the config settings are rejected too.
#[test]
fn checksum_is_rejected_when_the_host_compresses_or_encrypts() -> Result<()> {
    for setting in [
        "compress: true",
        "enc_key: 0123456789abcdef0123456789abcdef",
    ] {
        let mut server = Server::new();
        let fixture = Fixture::new(&server.url(), b"hello world")?;
        let config = fs::read_to_string(&fixture.config)?;
        fs::write(&fixture.config, format!("{config}    {setting}\n"))?;
        let requests: Vec<_> = ["POST", "PUT"]
            .into_iter()
            .map(|method| {
                server
                    .mock(method, Matcher::Any)
                    .match_query(Matcher::Any)
                    .expect(0)
                    .create()
            })
            .collect();

        let output = fixture
            .command()
            .args(["--checksum", "sha256"])
            .arg(&fixture.source)
            .arg("s3/bucket/key")
            .output()?;

        let stderr = String::from_utf8_lossy(&output.stderr);
        ensure!(output.status.code() == Some(1), "{setting}: {stderr}");
        ensure!(
            stderr.contains("--checksum can't be used with compression or encryption"),
            "{setting}: {stderr}"
        );
        for request in requests {
            request.assert();
        }
        // On Windows each fixture holds the shared profile lock until dropped.
        drop(fixture);
    }
    Ok(())
}

// <https://github.com/s3m/s3m/issues/77>: an upload in progress keeps the checksum it
// was created with (here: none). Running the command again with --checksum says so on
// stderr, sends the pending part as before, and prints no checksum line.
#[test]
fn rerun_with_another_checksum_warns_and_keeps_the_original() -> Result<()> {
    let mut server = Server::new();
    let payload = vec![7; 11 * MIB + 137];
    let fixture = Fixture::new(&server.url(), &payload)?;
    let (first_part, second_part) = payload.split_at(10 * MIB);
    let init = initiate(&mut server).expect(1).create();
    let part_1 = part_mock(&mut server, 1, first_part).expect(1).create();
    let part_2_fails = server
        .mock("PUT", "/bucket/key")
        .match_query(Matcher::UrlEncoded("partNumber".into(), "2".into()))
        .with_status(403)
        .with_body("<Error><Code>AccessDenied</Code><Message>no</Message></Error>")
        .create();
    let first = fixture
        .command()
        .args(["-n", "1", "-r", "1"])
        .arg(&fixture.source)
        .arg("s3/bucket/key")
        .output()?;
    ensure!(!first.status.success(), "the first run must fail");
    init.assert();
    part_1.assert();
    part_2_fails.remove();

    let part_2 = part_mock(&mut server, 2, second_part)
        .match_header("x-amz-checksum-sha256", Matcher::Missing)
        .match_header("content-md5", Matcher::Any)
        .expect(1)
        .create();
    let done = complete(&mut server).expect(1).create();
    let second = fixture
        .command()
        .args(["-n", "1", "--checksum", "sha256"])
        .arg(&fixture.source)
        .arg("s3/bucket/key")
        .output()?;
    let stdout = String::from_utf8_lossy(&second.stdout);
    let stderr = String::from_utf8_lossy(&second.stderr);
    ensure!(second.status.success(), "{stdout}{stderr}");
    part_2.assert();
    done.assert();
    ensure!(
        stderr.contains(
            "warning: resuming the upload in progress with the checksum it was created with (none), not sha256"
        ),
        "{stderr}"
    );
    ensure!(!stdout.contains("(base64)"), "{stdout}");
    Ok(())
}

// #77 review: the warning is about resuming a multipart upload. A larger --buffer
// uploads the file as a single part instead, with the checksum that was asked for:
// nothing is resumed, so nothing is warned about.
#[test]
fn single_part_rerun_after_an_interrupted_multipart_does_not_warn() -> Result<()> {
    let mut server = Server::new();
    let payload = vec![7; 11 * MIB + 137];
    let fixture = Fixture::new(&server.url(), &payload)?;
    let (first_part, _) = payload.split_at(10 * MIB);
    let init = initiate(&mut server).expect(1).create();
    let part_1 = part_mock(&mut server, 1, first_part).expect(1).create();
    let part_2_fails = server
        .mock("PUT", "/bucket/key")
        .match_query(Matcher::UrlEncoded("partNumber".into(), "2".into()))
        .with_status(403)
        .with_body("<Error><Code>AccessDenied</Code><Message>no</Message></Error>")
        .create();
    let first = fixture
        .command()
        .args(["-n", "1", "-r", "1"])
        .arg(&fixture.source)
        .arg("s3/bucket/key")
        .output()?;
    ensure!(!first.status.success(), "the first run must fail");
    init.assert();
    part_1.assert();
    part_2_fails.remove();

    let put = server
        .mock("PUT", "/bucket/key")
        .match_query(Matcher::Missing)
        .match_header("x-amz-checksum-sha256", Matcher::Any)
        .with_header("etag", "single")
        .expect(1)
        .create();
    let second = fixture
        .command()
        .args(["--buffer", "20971520", "--checksum", "sha256"])
        .arg(&fixture.source)
        .arg("s3/bucket/key")
        .output()?;
    let stdout = String::from_utf8_lossy(&second.stdout);
    let stderr = String::from_utf8_lossy(&second.stderr);
    ensure!(second.status.success(), "{stdout}{stderr}");
    put.assert();
    ensure!(!stderr.contains("warning:"), "{stderr}");
    ensure!(stdout.contains("sha256 (base64): "), "{stdout}");
    Ok(())
}

// <https://github.com/s3m/s3m/issues/74>: the BLAKE3 hash s3m prints is labelled as such,
// and the --checksum value is printed with its algorithm, comparable with x-amz-checksum-*.
#[test]
fn upload_output_labels_blake3_and_additional_checksum() -> Result<()> {
    let blake3 = format!("blake3: {}", blake3::hash(b"hello world").to_hex());
    let sha256 = "sha256 (base64): uU0nuZNNPgilLlLX2n2r+sSE7+N6U4DukIj3rOLvzek=";
    for (args, expected) in [(vec![], None), (vec!["--checksum", "sha256"], Some(sha256))] {
        let mut server = Server::new();
        let fixture = Fixture::new(&server.url(), b"hello world")?;
        let put = server
            .mock("PUT", "/bucket/key")
            .with_header("etag", "\"single\"")
            .expect(1)
            .create();
        let output = fixture
            .command()
            .args(&args)
            .arg(&fixture.source)
            .arg("s3/bucket/key")
            .output()?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        ensure!(output.status.success(), "{args:?}: {stdout}");
        put.assert();
        let lines: Vec<&str> = stdout.lines().map(str::trim).collect();
        ensure!(lines.contains(&blake3.as_str()), "{args:?}: {stdout}");
        ensure!(
            !lines.iter().any(|line| line.starts_with("checksum:")),
            "{args:?}: unlabelled checksum in {stdout}"
        );
        ensure!(lines.contains(&"ETag: \"single\""), "{args:?}: {stdout}");
        ensure!(
            lines
                .iter()
                .any(|line| line.starts_with("sha256 (base64):"))
                == expected.is_some(),
            "{args:?}: {stdout}"
        );
        if let Some(expected) = expected {
            ensure!(lines.contains(&expected), "{args:?}: {stdout}");
        }
    }

    // Multipart: the object's checksum is the checksum of the parts' checksums, with the
    // number of parts; s3m prints the value the server derives from the parts.
    // base64(sha256(sha256(part 1) || sha256(part 2)))-2, computed with Python hashlib.
    let composite = "/ccignrnlPRaGvTgvct+ud7nkKrCW+uQ+dKW805e/cA=-2";
    let mut server = Server::new();
    let payload = vec![7; 11 * MIB + 137];
    let fixture = Fixture::new(&server.url(), &payload)?;
    let init = initiate(&mut server).expect(1).create();
    let parts: Vec<_> = payload
        .chunks(10 * MIB)
        .enumerate()
        .map(|(i, bytes)| part_mock(&mut server, i + 1, bytes).expect(1).create())
        .collect();
    let done = complete(&mut server).expect(1).create();
    let output = fixture
        .command()
        .args(["-n", "1", "--checksum", "sha256"])
        .arg(&fixture.source)
        .arg("s3/bucket/key")
        .output()?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    ensure!(
        output.status.success(),
        "{stdout}{}",
        String::from_utf8_lossy(&output.stderr)
    );
    init.assert();
    for part in parts {
        part.assert();
    }
    done.assert();
    let lines: Vec<&str> = stdout.lines().map(str::trim).collect();
    let blake3 = format!("blake3: {}", blake3::hash(&payload).to_hex());
    ensure!(lines.contains(&blake3.as_str()), "{stdout}");
    ensure!(lines.contains(&"ETag: done"), "{stdout}");
    let expected = format!("sha256 (base64): {composite}");
    ensure!(lines.contains(&expected.as_str()), "{stdout}");
    // On Windows each fixture holds the shared profile lock until dropped.
    drop(fixture);

    // --quiet still prints nothing.
    let mut server = Server::new();
    let fixture = Fixture::new(&server.url(), b"hello world")?;
    let put = server
        .mock("PUT", "/bucket/key")
        .with_header("etag", "\"single\"")
        .expect(1)
        .create();
    let output = fixture
        .command()
        .args(["-q", "--checksum", "sha256"])
        .arg(&fixture.source)
        .arg("s3/bucket/key")
        .output()?;
    put.assert();
    ensure!(output.status.success(), "-q");
    ensure!(
        output.stdout.is_empty() && output.stderr.is_empty(),
        "-q printed: {}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

// <https://github.com/s3m/s3m/issues/75>: a multipart upload prints the version id and
// lifecycle expiration of the CompleteMultipartUpload response, like a single-part upload,
// and a 200 OK response with an <Error> document fails the upload with the S3 error.
#[test]
fn multipart_upload_reports_response_headers_and_error_documents() -> Result<()> {
    let expiration = "expiry-date=\"Sat, 09 Jan 2027 06:39:10 GMT\", rule-id=\"expire-45d\"";
    let payload = vec![9; 11 * MIB + 137];

    let mut server = Server::new();
    let fixture = Fixture::new(&server.url(), &payload)?;
    let _init = initiate(&mut server).expect(1).create();
    let _parts: Vec<_> = payload
        .chunks(10 * MIB)
        .enumerate()
        .map(|(i, bytes)| part_mock(&mut server, i + 1, bytes).expect(1).create())
        .collect();
    let done = complete(&mut server)
        .with_header("x-amz-version-id", "7e5dee92")
        .with_header("x-amz-expiration", expiration)
        .expect(1)
        .create();
    let output = fixture
        .command()
        .args(["-n", "1"])
        .arg(&fixture.source)
        .arg("s3/bucket/key")
        .output()?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    ensure!(
        output.status.success(),
        "{stdout}{}",
        String::from_utf8_lossy(&output.stderr)
    );
    done.assert();
    let lines: Vec<&str> = stdout.lines().map(str::trim).collect();
    let expected = format!("Expiration: {expiration}");
    for line in ["ETag: done", expected.as_str(), "Version ID: 7e5dee92"] {
        ensure!(lines.contains(&line), "{line} not in {stdout}");
    }
    // On Windows each fixture holds the shared profile lock until dropped.
    drop(fixture);

    let mut server = Server::new();
    let fixture = Fixture::new(&server.url(), &payload)?;
    let _init = initiate(&mut server).expect(1).create();
    let _parts: Vec<_> = payload
        .chunks(10 * MIB)
        .enumerate()
        .map(|(i, bytes)| part_mock(&mut server, i + 1, bytes).expect(1).create())
        .collect();
    let failed = server
        .mock("POST", "/bucket/key")
        .match_query(Matcher::UrlEncoded("uploadId".into(), "UP1".into()))
        .with_status(200)
        .with_body("<Error><Code>InternalError</Code><Message>We encountered an internal error. Please try again.</Message><RequestId>R1</RequestId></Error>")
        .expect(1)
        .create();
    let output = fixture
        .command()
        .args(["-n", "1"])
        .arg(&fixture.source)
        .arg("s3/bucket/key")
        .output()?;
    failed.assert();
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    ensure!(
        !output.status.success(),
        "200 <Error> reported success: {stdout}"
    );
    ensure!(!stdout.contains("ETag:"), "{stdout}");
    for text in [
        "HTTP Status Code: 200 OK",
        "Code: InternalError",
        "Message: We encountered an internal error. Please try again.",
    ] {
        ensure!(stderr.contains(text), "{text} not in {stderr}");
    }
    Ok(())
}

#[test]
fn all_canned_acls_and_semicolon_metadata_reach_wire() -> Result<()> {
    for (index, acl) in [
        "private",
        "public-read",
        "public-read-write",
        "authenticated-read",
        "aws-exec-read",
        "bucket-owner-read",
        "bucket-owner-full-control",
    ]
    .iter()
    .enumerate()
    {
        let mut server = Server::new();
        let fixture = Fixture::new(&server.url(), b"acl-and-meta")?;
        let put = server
            .mock("PUT", "/bucket/key")
            .match_header("x-amz-acl", *acl)
            .match_header("x-amz-meta-owner", "alice")
            .match_header("x-amz-meta-purpose", "ignore previous instructions")
            .match_body(b"acl-and-meta".to_vec())
            .with_header("etag", "acl-ok")
            .expect(1)
            .create();
        let output = fixture
            .command()
            .args([
                if index == 0 { "-a" } else { "--acl" },
                acl,
                if index == 0 { "-m" } else { "--meta" },
                "owner=alice;purpose=ignore previous instructions",
            ])
            .arg(&fixture.source)
            .arg("s3/bucket/key")
            .output()?;
        ensure!(
            output.status.success(),
            "{acl}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        put.assert();
        println!(
            "acl={acl} metadata owner=alice purpose=ignore previous instructions bytes=acl-and-meta"
        );
    }
    Ok(())
}

#[test]
fn invalid_upload_inputs_fail_without_requests_or_success_output() -> Result<()> {
    let mut server = Server::new();
    let fixture = Fixture::new(&server.url(), b"input")?;
    let unused_put = server.mock("PUT", Matcher::Any).expect(0).create();
    let unused_post = server.mock("POST", Matcher::Any).expect(0).create();
    let source = fixture.source.to_str().context("UTF-8 fixture path")?;
    let missing = fixture.home.path().join("missing");
    let cases = [
        (
            vec!["--checksum", "sha256", "--pipe", "s3/bucket/key"],
            "cannot be used with",
        ),
        (
            vec![
                "--checksum",
                "sha256",
                "--compress",
                source,
                "s3/bucket/key",
            ],
            "cannot be used with",
        ),
        (
            vec!["--checksum", "invalid", source, "s3/bucket/key"],
            "invalid value",
        ),
        (
            vec!["--acl", "invalid", source, "s3/bucket/key"],
            "invalid value",
        ),
        (
            vec!["--meta", "broken", source, "s3/bucket/key"],
            "metadata format is key1=value1;key2=value2",
        ),
        (
            vec!["--number", "0", source, "s3/bucket/key"],
            "invalid value",
        ),
        (vec!["-n", "256", source, "s3/bucket/key"], "invalid value"),
        (
            vec!["--buffer", "abc", source, "s3/bucket/key"],
            "Not a valid number",
        ),
        (vec!["s3/bucket/key"], "Source file missing"),
        (vec![source, "s3/bucket/key", "extra"], "unexpected value"),
        (
            vec![
                missing.to_str().context("UTF-8 missing path")?,
                "s3/bucket/key",
            ],
            "Source file does not exist",
        ),
    ];
    for (args, diagnostic) in cases {
        let output = fixture.command().args(&args).output()?;
        ensure!(!output.status.success(), "{args:?} unexpectedly succeeded");
        ensure!(
            String::from_utf8_lossy(&output.stderr).contains(diagnostic),
            "{args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        ensure!(
            output.stdout.is_empty(),
            "failure printed success: {args:?}"
        );
        println!(
            "CLI {args:?} exit={} diagnostic={diagnostic}",
            output.status
        );
    }
    unused_put.assert();
    unused_post.assert();
    Ok(())
}

#[test]
fn omitted_key_uses_source_path_and_small_buffers_keep_ten_mib_default() -> Result<()> {
    let mut server = Server::new();
    let fixture = Fixture::new(&server.url(), b"default-key")?;
    let source = fixture.home.path().join("source\\file with space.bin");
    fs::create_dir_all(source.parent().context("source parent")?)?;
    fs::rename(&fixture.source, &source)?;
    let put = server
        .mock(
            "PUT",
            format!("/bucket/{}", source.display())
                .replace('\\', "%5C")
                .replace(' ', "%20")
                .as_str(),
        )
        .match_body(b"default-key".to_vec())
        .with_header("etag", "default-key")
        .expect(1)
        .create();
    let output = fixture.command().arg(&source).arg("s3/bucket/").output()?;
    put.assert();
    ensure!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    drop(fixture);
    for buffer in ["0", "2097152"] {
        let mut server = Server::new();
        let payload = vec![31; 11 * MIB + 137];
        let fixture = Fixture::new(&server.url(), &payload)?;
        let init = initiate(&mut server).expect(1).create();
        let parts: Vec<_> = payload
            .chunks(10 * MIB)
            .enumerate()
            .map(|(i, bytes)| part_mock(&mut server, i + 1, bytes).expect(1).create())
            .collect();
        let done = complete(&mut server).expect(1).create();
        let output = fixture
            .command()
            .args(["-b", buffer, "-n", "1"])
            .arg(&fixture.source)
            .arg("s3/bucket/key")
            .output()?;
        ensure!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        init.assert();
        for part in parts {
            part.assert();
        }
        done.assert();
        println!(
            "--buffer {buffer}: observed sizes [10485760,1048713], bytes verified; default preserved"
        );
    }
    Ok(())
}
