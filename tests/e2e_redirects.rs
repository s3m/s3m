//! Loopback S3 redirect regressions through the CLI.

#![allow(clippy::unwrap_used)]

use mockito::Server;
use std::{
    fs,
    io::Write,
    process::{Command, Stdio},
};
use tempfile::TempDir;

const DESTINATION: &str = "https://redirect-user:redirect-password@127.0.0.1:12345/target?X-Amz-Signature=fake-signature#private-fragment";

fn command(server: &Server, home: &TempDir) -> Command {
    let config = home.path().join("config.yml");
    fs::write(
        &config,
        format!("hosts:\n  mock:\n    endpoint: {}\n", server.url()),
    )
    .unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_s3m"));
    command
        .current_dir(home.path())
        .env("HOME", home.path())
        .env("S3M_HOME", home.path())
        .env("NO_PROXY", "*")
        .arg("-vv")
        .args(["--config"])
        .arg(config)
        .args(["--no-sign-request", "--retries", "2"]);
    command
}

fn assert_presigned_destination_is_redacted(header: &str, body: &str) {
    let mut server = Server::new();
    let request = server
        .mock("GET", "/bucket?list-type=2&max-keys=1000")
        .with_status(307)
        .with_header("location", header)
        .with_body(body)
        .create();
    let home = tempfile::tempdir().unwrap();
    let output = command(&server, &home)
        .args(["ls", "mock/bucket"])
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "{stderr}");
    assert_no_destination_credentials(&stderr);
    assert!(
        stderr.contains("https://127.0.0.1:12345/target"),
        "{stderr}"
    );
    request.assert();
}

fn assert_no_destination_credentials(stderr: &str) {
    for secret in [
        "redirect-user",
        "redirect-password",
        "X-Amz-Signature",
        "fake-signature",
        "private-fragment",
    ] {
        assert!(
            !stderr.contains(secret),
            "redirect leaked {secret}: {stderr}"
        );
    }
}

#[test]
fn redirects_keep_presigned_location_out_of_stderr_and_logs() {
    assert_presigned_destination_is_redacted(
        DESTINATION,
        &format!("Unexpected redirect to {DESTINATION}"),
    );
}

#[test]
fn redirects_keep_presigned_xml_endpoint_out_of_stderr_and_logs() {
    assert_presigned_destination_is_redacted(
        "",
        &format!(
            "<Error><Code>TemporaryRedirect</Code><Message>See {DESTINATION}</Message><Endpoint>{DESTINATION}</Endpoint></Error>"
        ),
    );
}

#[test]
fn redirects_report_the_region_on_a_region_only_307() {
    let mut server = Server::new();
    let request = server
        .mock("GET", "/bucket?list-type=2&max-keys=1000")
        .with_status(307)
        .with_header("x-amz-bucket-region", "us-west-2")
        .create();
    let home = tempfile::tempdir().unwrap();
    let output = command(&server, &home)
        .args(["ls", "mock/bucket"])
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "{stderr}");
    assert!(stderr.contains("bucket is in region us-west-2"), "{stderr}");
    assert!(stderr.contains("for this request"), "{stderr}");
    assert!(
        !stderr.to_ascii_lowercase().contains("update the host"),
        "{stderr}"
    );
    request.assert();
}

#[test]
fn redirects_precede_the_list_buckets_fallback() {
    let mut server = Server::new();
    let fallback = server
        .mock("GET", "/")
        .with_status(200)
        .with_body("<ListAllMyBucketsResult><Buckets/></ListAllMyBucketsResult>")
        .expect(0)
        .create();
    assert_command_rejects_redirect(
        &mut server,
        "/?max-buckets=10000",
        "NotImplemented",
        &["ls", "mock"],
    );
    fallback.assert();
}

#[test]
fn redirects_precede_the_missing_object_lock_fallback() {
    assert_command_rejects_redirect(
        &mut Server::new(),
        "/bucket?object-lock=",
        "NoSuchObjectLockConfiguration",
        &["object-lock", "get", "mock/bucket"],
    );
}

#[test]
fn redirects_preserve_the_cached_upload_when_verification_head_redirects() {
    let mut server = Server::new();
    let home = tempfile::tempdir().unwrap();
    let file = home.path().join("object");
    fs::write(&file, b"body").unwrap();
    let upload = |server: &Server| {
        command(server, &home)
            .arg(&file)
            .arg("mock/bucket/key")
            .output()
            .unwrap()
    };
    let stored = server
        .mock("PUT", "/bucket/key")
        .with_status(200)
        .with_header("etag", "\"stored\"")
        .create();
    let first = upload(&server);
    assert!(first.status.success(), "{first:?}");
    stored.assert();
    stored.remove();

    let redirect = server
        .mock("HEAD", "/bucket/key")
        .with_status(307)
        .with_header("location", DESTINATION)
        .create();
    let extra_put = server
        .mock("PUT", "/bucket/key")
        .with_status(200)
        .with_header("etag", "\"unexpected\"")
        .expect(0)
        .create();
    let redirected = upload(&server);
    let stderr = String::from_utf8_lossy(&redirected.stderr);
    assert!(
        !redirected.status.success() && stderr.contains("307") && stderr.contains("Redirect:"),
        "the cached upload's verification HEAD must report its 307 without re-uploading: {stderr}"
    );
    assert_no_destination_credentials(&stderr);
    redirect.assert();
    redirect.remove();

    let verify = server
        .mock("HEAD", "/bucket/key")
        .with_status(200)
        .with_header("etag", "\"stored\"")
        .create();
    let cached = upload(&server);
    assert!(cached.status.success(), "{cached:?}");
    verify.assert();
    extra_put.assert();
}

fn assert_command_rejects_redirect(server: &mut Server, path: &str, code: &str, args: &[&str]) {
    let request = server
        .mock("GET", path)
        .with_status(307)
        .with_header("location", DESTINATION)
        .with_body(format!("<Error><Code>{code}</Code></Error>"))
        .create();
    let home = tempfile::tempdir().unwrap();
    let output = command(server, &home).args(args).output().unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success() && stderr.contains("307") && stderr.contains("Redirect:"),
        "{args:?} must report the 307 redirect and exit nonzero: {stderr}"
    );
    assert_no_destination_credentials(&stderr);
    request.assert();
}

fn redirect_part(pipe: bool) {
    let mut server = Server::new();
    let start = server
        .mock("POST", "/bucket/key?uploads=")
        .with_status(200)
        .with_body("<InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>key</Key><UploadId>fixture</UploadId></InitiateMultipartUploadResult>")
        .create();
    let part = server
        .mock("PUT", "/bucket/key?partNumber=1&uploadId=fixture")
        .with_status(307)
        .with_header("location", DESTINATION)
        .with_body("<Error><Code>TemporaryRedirect</Code></Error>")
        .create();
    let abort = server
        .mock("DELETE", "/bucket/key?uploadId=fixture")
        .with_status(204)
        .expect(usize::from(pipe))
        .create();
    let home = tempfile::tempdir().unwrap();
    let mut command = command(&server, &home);
    let output = if pipe {
        let mut child = command
            .args(["--pipe", "mock/bucket/key"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child.stdin.take().unwrap().write_all(b"body").unwrap();
        child.wait_with_output().unwrap()
    } else {
        let path = home.path().join("object");
        fs::File::create(&path)
            .unwrap()
            .set_len(6 * 1024 * 1024)
            .unwrap();
        command
            .args(["-n", "1", "-b", "5242880"])
            .arg(path)
            .arg("mock/bucket/key")
            .output()
            .unwrap()
    };
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "{stderr}");
    assert!(
        stderr.contains("307") && stderr.contains("TemporaryRedirect"),
        "{stderr}"
    );
    part.assert();
    start.assert();
    abort.assert();
    assert_no_destination_credentials(&stderr);
}

#[test]
fn redirects_do_not_retry_file_parts() {
    redirect_part(false);
}

#[test]
fn redirects_do_not_retry_pipe_parts() {
    redirect_part(true);
}
