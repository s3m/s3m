//! Multipart failure and resume through the real CLI and persisted database.
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::indexing_slicing)]

mod common;
#[path = "support/stream_home.rs"]
mod stream_home;

use common::{create_config_file, get_s3m_binary};
use mockito::{Matcher, Mock, Server};
use s3m::stream::state::StreamMode;
use std::{
    fs,
    path::PathBuf,
    process::{Command, Stdio},
    sync::{Arc, Mutex, mpsc},
    time::Duration,
};
use stream_home::StreamHome;
use tempfile::NamedTempFile;

const PART_SIZE: usize = 5 * 1024 * 1024;
const INITIATE: &str = "<InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>key</Key><UploadId>resume-upload</UploadId></InitiateMultipartUploadResult>";
const COMPLETE: &str = "<CompleteMultipartUploadResult><Location>local</Location><Bucket>bucket</Bucket><Key>key</Key><ETag>completed</ETag></CompleteMultipartUploadResult>";

struct MultipartFixture {
    server: mockito::ServerGuard,
    initiate: Mock,
    first: Mock,
    home: StreamHome,
    config: NamedTempFile,
    source: PathBuf,
    payload: Vec<u8>,
}

impl MultipartFixture {
    fn new(payload: Vec<u8>) -> Self {
        let mut server = Server::new();
        let home = StreamHome::new().unwrap();
        let config = create_config_file(&server.url(), "dummy", "dummy-secret");
        let source = home.path().join("source.bin");
        fs::write(&source, &payload).unwrap();
        let initiate = server
            .mock("POST", "/bucket/key")
            .match_query(Matcher::UrlEncoded("uploads".into(), String::new()))
            .with_body(INITIATE)
            .expect(1)
            .create();
        let first = server
            .mock("PUT", "/bucket/key")
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("uploadId".into(), "resume-upload".into()),
                Matcher::UrlEncoded("partNumber".into(), "1".into()),
            ]))
            .match_body(payload[..PART_SIZE].to_vec())
            .with_header("etag", "first")
            .expect(1)
            .create();

        Self {
            server,
            initiate,
            first,
            home,
            config,
            source,
            payload,
        }
    }

    fn command(&self) -> Command {
        let mut command = Command::new(get_s3m_binary());
        command
            .env("HOME", self.home.path())
            .env("S3M_HOME", self.home.base())
            .arg("--config")
            .arg(self.config.path())
            .args(["-r", "1", "-n", "1"]);
        command
    }
}

#[test]
fn failed_multipart_preserves_confirmed_parts_and_resumes_only_pending_bytes() {
    let payload: Vec<u8> = (0..u32::try_from(PART_SIZE + 37).unwrap())
        .map(|offset| u8::try_from(offset % 251).unwrap())
        .collect();
    let mut fixture = MultipartFixture::new(payload);
    let failed = fixture
        .server
        .mock("PUT", "/bucket/key")
        .match_query(Matcher::UrlEncoded("partNumber".into(), "2".into()))
        .match_body(fixture.payload[PART_SIZE..].to_vec())
        .with_status(503)
        .with_body("<Error><Code>ServiceUnavailable</Code></Error>")
        .expect(2)
        .create();
    let complete = fixture
        .server
        .mock("POST", "/bucket/key")
        .match_query(Matcher::UrlEncoded(
            "uploadId".into(),
            "resume-upload".into(),
        ))
        .with_body(COMPLETE)
        .expect(0)
        .create();

    let result = fixture
        .command()
        .args([
            "--buffer",
            &PART_SIZE.to_string(),
            fixture.source.to_str().unwrap(),
            "s3/bucket/key",
        ])
        .output()
        .unwrap();
    assert!(!result.status.success(), "failed part reported success");
    assert!(String::from_utf8_lossy(&result.stderr).contains("multipart upload failed"));
    let entries = fixture.home.entries().unwrap();
    assert_eq!(entries.len(), 1);
    let entry = &entries[0];
    assert_eq!(entry.upload_id.as_deref(), Some("resume-upload"));
    assert_eq!((entry.parts_uploaded, entry.parts_pending), (1, 1));
    assert_eq!(entry.bytes_uploaded, u64::try_from(PART_SIZE).unwrap());
    assert_eq!(entry.etag, None);
    let metadata = entry.metadata.as_ref().unwrap();
    assert_eq!(
        metadata.checksum,
        blake3::hash(&fixture.payload).to_hex().to_string()
    );
    assert_eq!(metadata.source_path, fixture.source);
    assert_eq!(metadata.part_size, u64::try_from(PART_SIZE).unwrap());
    assert!(matches!(metadata.mode, StreamMode::FileMultipart));
    assert!(!metadata.pipe && !metadata.compress && !metadata.encrypt);
    let metadata_path = s3m::stream::state::state_file_path(fixture.home.base(), &entry.id);
    let id = entry.id.clone();

    // A second failed resume must retain the same pending part and upload ID.
    let again = fixture
        .command()
        .args(["streams", "resume", &id])
        .output()
        .unwrap();
    assert!(!again.status.success());
    let repeated = fixture.home.entries().unwrap();
    assert_eq!(
        (repeated[0].parts_uploaded, repeated[0].parts_pending),
        (1, 1)
    );
    assert_eq!(repeated[0].upload_id, entry.upload_id);
    failed.assert();
    complete.assert();
    failed.remove();
    complete.remove();
    let saved_metadata = fs::read(&metadata_path).unwrap();

    // Resume rejects changed and missing sources without destroying state.
    fs::write(&fixture.source, b"changed").unwrap();
    let changed = fixture
        .command()
        .args(["streams", "resume", &id])
        .output()
        .unwrap();
    assert!(!changed.status.success());
    assert!(String::from_utf8_lossy(&changed.stderr).contains("Source file changed"));
    fs::remove_file(&fixture.source).unwrap();
    let missing = fixture
        .command()
        .args(["streams", "resume", &id])
        .output()
        .unwrap();
    assert!(!missing.status.success());
    assert!(String::from_utf8_lossy(&missing.stderr).contains("Source file missing or unreadable"));
    assert_eq!(fs::read(&metadata_path).unwrap(), saved_metadata);
    assert_eq!(fixture.home.entries().unwrap()[0].parts_pending, 1);
    fixture.initiate.assert();
    fixture.first.assert();
}

#[test]
fn interrupted_multipart_resumes_pending_bytes_after_repeated_kills() {
    let payload = vec![23_u8; PART_SIZE + 19];
    let mut fixture = MultipartFixture::new(payload);
    for _ in 0..2 {
        let (seen_tx, seen_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = Arc::new(Mutex::new(release_rx));
        let stalled = fixture
            .server
            .mock("PUT", "/bucket/key")
            .match_query(Matcher::UrlEncoded("partNumber".into(), "2".into()))
            .match_body(fixture.payload[PART_SIZE..].to_vec())
            .with_body_from_request(move |_| {
                seen_tx.send(()).unwrap();
                release_rx
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(10))
                    .unwrap();
                Vec::new()
            })
            .expect(1)
            .create();
        let mut child = fixture
            .command()
            .arg("-b")
            .arg(PART_SIZE.to_string())
            .arg(&fixture.source)
            .arg("s3/bucket/key")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        seen_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("pending part reached server");
        child.kill().unwrap();
        assert!(!child.wait().unwrap().success());
        release_tx.send(()).unwrap();
        stalled.assert();
        stalled.remove();
        let interrupted = fixture.home.entries().unwrap();
        assert_eq!(
            (interrupted[0].parts_uploaded, interrupted[0].parts_pending),
            (1, 1)
        );
        assert_eq!(interrupted[0].upload_id.as_deref(), Some("resume-upload"));
        assert_eq!(interrupted[0].etag, None);
    }
    let entries = fixture.home.entries().unwrap();
    let second = fixture
        .server
        .mock("PUT", "/bucket/key")
        .match_query(Matcher::AllOf(vec![
            Matcher::UrlEncoded("uploadId".into(), "resume-upload".into()),
            Matcher::UrlEncoded("partNumber".into(), "2".into()),
        ]))
        .match_body(fixture.payload[PART_SIZE..].to_vec())
        .with_header("etag", "second")
        .expect(1)
        .create();
    let complete = fixture.server.mock("POST", "/bucket/key")
        .match_query(Matcher::UrlEncoded("uploadId".into(), "resume-upload".into()))
        .match_body(Matcher::Regex("(?s)<Part>.*<ETag>first</ETag>.*<PartNumber>1</PartNumber>.*<Part>.*<ETag>second</ETag>.*<PartNumber>2</PartNumber>".into()))
        .with_body(COMPLETE).expect(1).create();

    let resumed = fixture
        .command()
        .args(["streams", "resume", &entries[0].id])
        .output()
        .unwrap();
    assert!(
        resumed.status.success(),
        "{}",
        String::from_utf8_lossy(&resumed.stderr)
    );
    fixture.initiate.assert();
    fixture.first.assert();
    second.assert();
    complete.assert();
    let finished = fixture.home.entries().unwrap();
    assert_eq!(finished[0].etag.as_deref(), Some("completed"));
    assert_eq!(finished[0].parts_pending, 0);
    assert_eq!(fs::read(&fixture.source).unwrap(), fixture.payload);
}
