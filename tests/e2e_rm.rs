//! E2E tests for `rm` (delete object) command

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::uninlined_format_args,
    clippy::missing_panics_doc
)]

mod common;

use common::{MinioContext, run_s3m_with_minio};
use std::io::Write;
use tempfile::NamedTempFile;

#[tokio::test]
async fn test_e2e_delete_object() {
    let minio = MinioContext::get_or_start().await;

    let bucket_name = "e2e-delete-bucket";
    minio
        .create_bucket(bucket_name)
        .await
        .expect("Bucket creation");

    // Upload a test file first
    let mut test_file = NamedTempFile::new().expect("Failed to create temp file");
    test_file
        .write_all(b"File to be deleted")
        .expect("Write failed");
    test_file.flush().expect("Flush failed");

    let file_path = test_file.path().to_str().expect("Invalid path");
    let s3_uri = format!("s3/{}/to-delete.txt", bucket_name);

    let upload_output = run_s3m_with_minio(&minio, &[file_path, &s3_uri]);
    assert!(
        upload_output.status.success(),
        "Upload should succeed before delete"
    );

    // Delete the object
    let delete_output = run_s3m_with_minio(&minio, &["rm", &s3_uri]);

    let stdout = String::from_utf8_lossy(&delete_output.stdout);
    let stderr = String::from_utf8_lossy(&delete_output.stderr);

    println!("DELETE stdout: {}", stdout);
    println!("DELETE stderr: {}", stderr);

    // Delete should succeed (or object already doesn't exist)
    assert!(
        delete_output.status.success(),
        "Delete operation should succeed"
    );
}

fn listed_keys(minio: &MinioContext, bucket: &str) -> Vec<String> {
    let out = run_s3m_with_minio(minio, &["ls", &format!("s3/{bucket}")]);
    assert!(out.status.success(), "ls should succeed");
    let mut keys: Vec<String> = String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter_map(|line| line.split_once("] "))
        .filter_map(|(_, rest)| rest.split_whitespace().last().map(str::to_string))
        .collect();
    keys.sort();
    keys
}

#[tokio::test]
async fn test_e2e_delete_prefix_recursive_is_literal_and_keeps_bucket() {
    let minio = MinioContext::get_or_start().await;

    let bucket = "e2e-rm-prefix-bucket";
    minio.create_bucket(bucket).await.expect("Bucket creation");

    let mut file = NamedTempFile::new().expect("temp file");
    file.write_all(b"x").expect("write");
    file.flush().expect("flush");
    let path = file.path().to_str().expect("path");

    let all = [
        "integration/18/",
        "integration/18/file1.rpm",
        "integration/18/el9/file2.rpm",
        "integration/18/el10/file3.rpm",
        "integration/19/file4.rpm",
        "integration/180/file5.rpm",
        "integration/18-other/file6.rpm",
        "integration/18",
    ];
    for key in all {
        let out = run_s3m_with_minio(&minio, &[path, &format!("s3/{bucket}/{key}")]);
        assert!(out.status.success(), "upload {key}: {:?}", out);
    }

    let out = run_s3m_with_minio(
        &minio,
        &["rm", &format!("s3/{bucket}/integration/18/"), "--recursive"],
    );
    assert!(out.status.success(), "{:?}", out);

    let remaining = listed_keys(&minio, bucket);
    assert_eq!(
        remaining,
        vec![
            "integration/18",
            "integration/18-other/file6.rpm",
            "integration/180/file5.rpm",
            "integration/19/file4.rpm",
        ]
    );
    // The bucket must still exist: uploading into it (without recreating it) works.
    let out = run_s3m_with_minio(&minio, &[path, &format!("s3/{bucket}/after-delete.txt")]);
    assert!(out.status.success(), "bucket must still exist: {:?}", out);
    let out = run_s3m_with_minio(&minio, &["rm", &format!("s3/{bucket}/after-delete.txt")]);
    assert!(out.status.success(), "{:?}", out);

    // Nonexistent prefix is a successful no-op.
    let out = run_s3m_with_minio(
        &minio,
        &["rm", &format!("s3/{bucket}/nothing/here/"), "--recursive"],
    );
    assert!(out.status.success(), "{:?}", out);
    assert_eq!(listed_keys(&minio, bucket).len(), 4);

    // Empty prefix and prefix + --bucket are rejected without deleting anything.
    for args in [
        vec!["rm", &format!("s3/{bucket}"), "--recursive"],
        vec![
            "rm",
            "-b",
            "--recursive",
            &format!("s3/{bucket}/integration/19/"),
        ],
    ] {
        let out = run_s3m_with_minio(&minio, &args);
        assert!(!out.status.success());
    }
    assert_eq!(listed_keys(&minio, bucket).len(), 4);
}

#[tokio::test]
async fn test_e2e_bucket_delete_with_key_is_rejected_and_preserves_data() {
    let minio = MinioContext::get_or_start().await;

    let bucket = "e2e-rm-bucket-key-safety";
    minio.create_bucket(bucket).await.expect("Bucket creation");

    let mut file = NamedTempFile::new().expect("temp file");
    file.write_all(b"safety-check").expect("write");
    file.flush().expect("flush");
    let path = file.path().to_str().expect("path");

    let initial_keys = ["file.txt", "keep.txt", "nested/a.dat", "nested/b.dat"];
    for key in initial_keys {
        let out = run_s3m_with_minio(&minio, &[path, &format!("s3/{bucket}/{key}")]);
        assert!(out.status.success(), "upload {key}: {:?}", out);
    }
    assert_eq!(listed_keys(&minio, bucket).len(), 4);

    // Test rejection across various invocations combining -b with a key
    let target_file = format!("s3/{bucket}/file.txt");
    let target_nested_dir = format!("s3/{bucket}/nested/");
    let target_nested_file = format!("s3/{bucket}/nested/a.dat");
    let target_bucket = format!("s3/{bucket}");

    let attempts: [&[&str]; 5] = [
        &["rm", "-b", &target_file],
        &["rm", "--bucket", &target_file],
        &["rm", "-b", "--recursive", &target_nested_dir],
        &["rm", "-b", "-r", &target_nested_file],
        &["rm", &target_file, "-b"],
    ];
    for args in attempts {
        let out = run_s3m_with_minio(&minio, args);
        assert!(
            !out.status.success(),
            "{args:?}: command should fail when -b is combined with a key"
        );
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("cannot be combined with an object key or prefix"),
            "{args:?}: expected safety error on stderr, got:\n{stderr}"
        );
    }

    // Multi-target attempt: bucket root first, key second
    let multi_out = run_s3m_with_minio(&minio, &["rm", "-b", &target_bucket, &target_file]);
    assert!(!multi_out.status.success());
    let multi_stderr = String::from_utf8_lossy(&multi_out.stderr);
    assert!(
        multi_stderr.contains("Bucket deletion expects exactly one bucket target"),
        "multi_stderr: {multi_stderr}"
    );

    // Critical assertion: ALL objects must still exist (nothing was deleted)
    let remaining = listed_keys(&minio, bucket);
    assert_eq!(
        remaining,
        vec!["file.txt", "keep.txt", "nested/a.dat", "nested/b.dat"],
        "All objects must survive rejected -b commands"
    );

    // Critical assertion: bucket must still be intact and functional
    let check = run_s3m_with_minio(&minio, &["ls", &format!("s3/{bucket}")]);
    assert!(
        check.status.success(),
        "bucket must still exist and be readable"
    );
}
