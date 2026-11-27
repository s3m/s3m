//! Two interrupted puts of the same bytes, with independent S3 upload IDs.
use super::*;
use mockito::{Matcher, Mock, Server, ServerGuard};
use s3m::s3::checksum::{Checksum, ChecksumAlgorithm};
use std::process::Output;
use tempfile::TempDir;

const PART: usize = 5 * 1024 * 1024;

struct Uploads {
    home: TempDir,
    server: ServerGuard,
    source: PathBuf,
    bytes: Vec<u8>,
}

impl Uploads {
    fn new() -> Self {
        let home = tempdir().unwrap();
        let server = Server::new();
        let config = write_config(home.path());
        fs::write(config, format!(
            "hosts:\n  s3:\n    endpoint: {}\n    access_key: mock\n    secret_key: mock-secret\n",
            server.url()
        )).unwrap();
        let source = home.path().join("source");
        let mut bytes = vec![7; PART];
        bytes.extend_from_slice(b"tail");
        fs::write(&source, &bytes).unwrap();
        Self {
            home,
            server,
            source,
            bytes,
        }
    }

    fn put(&self, key: &str, algorithm: &ChecksumAlgorithm) -> Output {
        run_streams(
            self.home.path(),
            &[
                "-n",
                "1",
                "-r",
                "1",
                "-b",
                &PART.to_string(),
                "--checksum",
                algorithm.name(),
                self.source.to_str().unwrap(),
                &format!("s3/bucket/{key}"),
            ],
        )
    }

    fn part(&mut self, key: &str, number: u16, algorithm: &ChecksumAlgorithm) -> Mock {
        let bytes = if number == 1 {
            &self.bytes[..PART]
        } else {
            &self.bytes[PART..]
        };
        let checksum = Checksum::new(algorithm.clone()).digest(bytes).unwrap();
        self.server
            .mock("PUT", format!("/bucket/{key}").as_str())
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("uploadId".into(), format!("uid-{key}")),
                Matcher::UrlEncoded("partNumber".into(), number.to_string()),
            ]))
            .match_header(algorithm.as_amz(), checksum.as_str())
            .match_body(bytes.to_vec())
            .with_header("etag", format!("part-{key}-{number}").as_str())
    }

    fn interrupt(&mut self, key: &str, algorithm: &ChecksumAlgorithm, first_pending: u16) {
        let init = self.server.mock("POST", format!("/bucket/{key}").as_str())
            .match_query(Matcher::UrlEncoded("uploads".into(), String::new()))
            .match_header("x-amz-checksum-algorithm", algorithm.as_algorithm())
            .with_body(format!("<InitiateMultipartUploadResult><UploadId>uid-{key}</UploadId></InitiateMultipartUploadResult>"))
            .expect(1).create();
        let accepted =
            (first_pending == 2).then(|| self.part(key, 1, algorithm).expect(1).create());
        let failed = self
            .part(key, first_pending, algorithm)
            .with_status(500)
            .with_body("<Error><Code>InternalError</Code></Error>")
            .expect(1)
            .create();
        let result = self.put(key, algorithm);
        assert!(!result.status.success());
        init.assert();
        failed.assert();
        failed.remove();
        if let Some(accepted) = accepted {
            accepted.assert();
            accepted.remove();
        }
    }

    fn list(&self) -> Vec<Value> {
        let output = run_streams(self.home.path(), &["streams", "ls", "--json"]);
        assert!(output.status.success(), "{output:?}");
        let value: Value = serde_json::from_slice(&output.stdout).unwrap();
        value["streams"].as_array().unwrap().clone()
    }

    fn finish(&mut self, id: &str, key: &str, algorithm: &ChecksumAlgorithm, first_pending: u16) {
        let parts: Vec<_> = (first_pending..=2)
            .map(|n| self.part(key, n, algorithm).expect(1).create())
            .collect();
        let complete = self
            .server
            .mock("POST", format!("/bucket/{key}").as_str())
            .match_query(Matcher::UrlEncoded("uploadId".into(), format!("uid-{key}")))
            .match_body(Matcher::AllOf(vec![
                Matcher::Regex(format!("part-{key}-1")),
                Matcher::Regex(format!("part-{key}-2")),
                Matcher::Regex(format!("Checksum{}", algorithm.as_algorithm())),
            ]))
            .with_body(
                "<CompleteMultipartUploadResult><ETag>done</ETag></CompleteMultipartUploadResult>",
            )
            .expect(1)
            .create();
        let result = run_streams(self.home.path(), &["streams", "resume", id]);
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        assert!(
            String::from_utf8_lossy(&result.stdout)
                .contains(&format!("{} (base64):", algorithm.name()))
        );
        for part in parts {
            part.assert();
        }
        complete.assert();
    }
}

#[test]
fn each_destination_resumes_its_own_parts_and_checksum() {
    let mut uploads = Uploads::new();
    uploads.interrupt("a", &ChecksumAlgorithm::Sha256, 1);
    uploads.interrupt("b", &ChecksumAlgorithm::Crc32, 2);
    let entries = uploads.list();
    assert_eq!(entries.len(), 2);
    let clean = run_streams(uploads.home.path(), &["streams", "clean", "--json"]);
    assert!(clean.status.success());
    let summary: Value = serde_json::from_slice(&clean.stdout).unwrap();
    assert_eq!(summary["kept"].as_array().unwrap().len(), 2);
    for (key, algorithm, pending) in [
        ("a", ChecksumAlgorithm::Sha256, 1),
        ("b", ChecksumAlgorithm::Crc32, 2),
    ] {
        let entry = entries
            .iter()
            .find(|e| e["upload_id"] == format!("uid-{key}"))
            .unwrap();
        assert_eq!(entry["destination"], format!("bucket/{key}"));
        assert_eq!(entry["parts_uploaded"], pending - 1);
        let id = entry["id"].as_str().unwrap();
        assert_eq!(
            show_json(uploads.home.path(), id)["upload_id"],
            entry["upload_id"]
        );
        uploads.finish(id, key, &algorithm, pending);
    }
}

#[test]
fn forgetting_one_destination_leaves_the_other_resumable_after_clean() {
    let mut uploads = Uploads::new();
    uploads.interrupt("a", &ChecksumAlgorithm::Sha256, 1);
    uploads.interrupt("b", &ChecksumAlgorithm::Crc32, 2);
    let gone = uploads
        .server
        .mock("PUT", "/bucket/a")
        .match_query(Matcher::UrlEncoded("uploadId".into(), "uid-a".into()))
        .with_status(404)
        .with_body("<Error><Code>NoSuchUpload</Code></Error>")
        .expect(1)
        .create();
    let result = uploads.put("a", &ChecksumAlgorithm::Sha256);
    assert!(!result.status.success());
    gone.assert();
    assert!(String::from_utf8_lossy(&result.stderr).contains("no longer exists"));
    let clean = run_streams(uploads.home.path(), &["streams", "clean"]);
    assert!(clean.status.success());
    let entries = uploads.list();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["upload_id"], "uid-b");
    assert_eq!(entries[0]["parts_uploaded"], 1);
    uploads.finish(
        entries[0]["id"].as_str().unwrap(),
        "b",
        &ChecksumAlgorithm::Crc32,
        2,
    );
}

#[test]
fn shared_legacy_parts_are_never_used_and_each_upload_id_is_visible() {
    let home = tempdir().unwrap();
    write_config(home.path());
    let (metadata, db) = create_state(home.path(), "legacy", Some("id-with-metadata"));
    // Raw records as written by a second destination in the old directory.
    // Its key can contain spaces. No source or host alias was recorded for it.
    let other = format!(
        "{} other key {}",
        &test_s3().hash()[..8],
        metadata.file_mtime
    );
    db.db().insert(&other, "id-without-metadata").unwrap();
    db.db()
        .insert(format!("checksum {other}"), "sha256")
        .unwrap();
    db.flush().unwrap();
    drop(db);
    wait_for_state_lock(home.path(), "legacy");

    let clean = run_streams(home.path(), &["streams", "clean", "--json"]);
    assert!(clean.status.success());
    let summary: Value = serde_json::from_slice(&clean.stdout).unwrap();
    let ids = summary["kept"].as_array().unwrap();
    assert_eq!(ids.len(), 2);
    assert!(ids.contains(&Value::from("legacy")));
    let list = run_streams(home.path(), &["streams", "ls"]);
    let list = String::from_utf8(list.stdout).unwrap();
    for id in ids {
        let id = id.as_str().unwrap();
        let entry = show_json(home.path(), id);
        let uid = if id == "legacy" {
            "id-with-metadata"
        } else {
            "id-without-metadata"
        };
        assert_eq!(entry["upload_id"], uid);
        assert_eq!(entry["status"], "broken");
        assert_eq!(entry["parts_total"], 0);
        assert!(
            list.contains(uid) && list.contains("abort any open uploads"),
            "{list}"
        );
        assert!(
            !run_streams(home.path(), &["streams", "resume", id])
                .status
                .success()
        );
    }
    for key in ["key", "other key"] {
        let error = Db::new(
            &test_s3(),
            key,
            "legacy",
            metadata.file_mtime,
            &s3m_dir(home.path()),
        )
        .unwrap_err();
        assert!(error.to_string().contains("parts cannot be attributed"));
    }
    let unrelated = Db::new(
        &test_s3(),
        "third",
        "legacy",
        metadata.file_mtime,
        &s3m_dir(home.path()),
    )
    .unwrap();
    assert!(unrelated.upload_id().unwrap().is_none());
    assert!(unrelated.db_parts().unwrap().is_empty());
}
