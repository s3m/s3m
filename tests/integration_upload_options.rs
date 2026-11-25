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
        let done = complete(&mut server)
            .match_header("content-md5", Matcher::Missing)
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
