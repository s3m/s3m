//! E2E tests for S3 Object Lock (WORM)
//!
//! Covers:
//! - Creating an Object-Lock-enabled bucket (`cb --object-lock`) and uploading
//!   an object with retention + legal hold (end-to-end, S3 must accept the
//!   `x-amz-object-lock-*` headers).
//! - The negative path: uploading lock settings into a plain (non-lock) bucket
//!   is rejected by S3.
//! - Enforcement: deleting a GOVERNANCE-locked *version* is refused without
//!   `--bypass-governance` and allowed with it.
//!
//! Note: a keyed `rm` (no version id) on a versioned bucket only writes a delete
//! marker and leaves the locked version intact — so enforcement is asserted by
//! deleting the specific version, not by a keyed `rm`.

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::uninlined_format_args,
    clippy::missing_panics_doc
)]

mod common;
#[path = "support/object_lock_preflight.rs"]
mod object_lock_preflight;

use common::owned_bucket::OwnedBucket;
use common::{
    MinioContext, create_config_file, create_config_file_with_options,
    create_test_file_with_content, get_s3m_binary, run_s3m_with_minio,
};
use mockito::{Matcher, Server};
use serde_json::{Value, json};
use std::io::Write;
use std::process::{Command, Output, Stdio};

/// Active retention for enforcement tests. COMPLIANCE data is owned by the
/// disposable service boundary; GOVERNANCE remains removable through bypass.
const FUTURE_DATE: &str = "2031-01-01T00:00:00Z";

/// Run `s3m` against the test S3 service, feeding `stdin` (for `--pipe` uploads).
fn run_with_stdin(minio: &MinioContext, args: &[&str], stdin: &[u8]) -> Output {
    let cfg = create_config_file(minio.endpoint(), minio.access_key(), minio.secret_key());
    let cfg_path = cfg.path().to_str().expect("config path");
    let mut child = Command::new(get_s3m_binary())
        .args(["--config", cfg_path])
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn s3m");
    child
        .stdin
        .as_mut()
        .expect("stdin pipe")
        .write_all(stdin)
        .expect("write stdin");
    let out = child.wait_with_output().expect("wait s3m");
    drop(cfg);
    out
}

/// Create a fresh Object-Lock-enabled bucket.
fn make_lock_bucket(minio: &MinioContext, bucket: &str) {
    let cb = run_s3m_with_minio(minio, &["cb", "--object-lock", &format!("s3/{}", bucket)]);
    let err = String::from_utf8_lossy(&cb.stderr);
    assert!(
        cb.status.success(),
        "object-lock bucket creation should succeed: {}",
        err
    );
}

/// Pull the `Version ID: <id>` line out of an upload's stdout.
fn version_id_from(stdout: &[u8]) -> String {
    String::from_utf8_lossy(stdout)
        .lines()
        .find_map(|l| l.strip_prefix("Version ID:").map(|v| v.trim().to_string()))
        .expect("upload should report a Version ID on a versioned bucket")
}

#[tokio::test]
async fn test_e2e_object_lock_upload_succeeds() {
    let minio = MinioContext::get_or_start().await;
    let owned = OwnedBucket::new(&minio, "e2e-objlock-bucket");
    let bucket = owned.name.as_str();

    // Create an Object-Lock-enabled bucket.
    let cb = run_s3m_with_minio(&minio, &["cb", "--object-lock", &format!("s3/{}", bucket)]);
    let cb_err = String::from_utf8_lossy(&cb.stderr);
    assert!(
        cb.status.success(),
        "object-lock bucket creation should succeed: {}",
        cb_err
    );

    // Upload with GOVERNANCE retention.
    let file = create_test_file_with_content(2048, "OBJECT_LOCK_");
    let file_path = file.path().to_str().expect("Invalid path");
    let s3_uri = format!("s3/{}/locked.dat", bucket);

    let out = run_s3m_with_minio(
        &minio,
        &[
            file_path,
            &s3_uri,
            "--object-lock-mode",
            "GOVERNANCE",
            "--retain-until",
            FUTURE_DATE,
        ],
    );
    assert!(
        out.status.success(),
        "locked upload should succeed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let lock = read_object_lock(&minio, &s3_uri);
    assert_retention(&lock, "GOVERNANCE");

    // The object should be retrievable.
    let head = run_s3m_with_minio(&minio, &["get", "-m", &s3_uri]);
    assert!(
        head.status.success(),
        "metadata get should succeed: {}",
        String::from_utf8_lossy(&head.stderr)
    );
}

#[tokio::test]
async fn test_e2e_object_lock_on_plain_bucket_fails() {
    let minio = MinioContext::get_or_start().await;
    let owned = OwnedBucket::new(&minio, "e2e-plain-bucket");
    let bucket = owned.name.as_str();

    // Plain bucket (no Object Lock).
    minio.create_bucket(bucket).await.expect("Bucket creation");

    let file = create_test_file_with_content(1024, "PLAIN_");
    let file_path = file.path().to_str().expect("Invalid path");
    let s3_uri = format!("s3/{}/should-fail.dat", bucket);

    let out = run_s3m_with_minio(
        &minio,
        &[
            file_path,
            &s3_uri,
            "--object-lock-mode",
            "COMPLIANCE",
            "--retain-until",
            FUTURE_DATE,
        ],
    );

    assert!(
        !out.status.success(),
        "object-lock upload to a plain bucket must fail"
    );

    // The error must carry the actionable hint to recreate the bucket with
    // Object Lock — exactly as documented.
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("cb --object-lock") && stderr.contains("not Object-Lock-enabled"),
        "error should surface the `cb --object-lock` hint, got: {}",
        stderr
    );
}

#[tokio::test]
async fn test_e2e_object_lock_bucket_default_retention() {
    let minio = MinioContext::get_or_start().await;
    let owned = OwnedBucket::new(&minio, "e2e-objlock-default");
    let bucket = owned.name.as_str();

    let cb = run_s3m_with_minio(&minio, &["cb", "--object-lock", &format!("s3/{}", bucket)]);
    let cb_err = String::from_utf8_lossy(&cb.stderr);
    assert!(
        cb.status.success(),
        "object-lock bucket creation should succeed: {}",
        cb_err
    );

    // Set a bucket default retention (GOVERNANCE so it stays bypassable).
    let set = run_s3m_with_minio(
        &minio,
        &[
            "object-lock",
            "set",
            &format!("s3/{}", bucket),
            "--mode",
            "GOVERNANCE",
            "--days",
            "1",
        ],
    );
    assert!(
        set.status.success(),
        "set bucket default retention should succeed: {}",
        String::from_utf8_lossy(&set.stderr)
    );

    assert_eq!(
        String::from_utf8_lossy(&set.stdout),
        "bucket default retention set: mode=GOVERNANCE\n"
    );

    // Read it back (Put/GetObjectLockConfiguration round-trip).
    let get = run_s3m_with_minio(&minio, &["object-lock", "get", &format!("s3/{}", bucket)]);
    assert!(get.status.success(), "get bucket config should succeed");
    assert_eq!(
        String::from_utf8_lossy(&get.stdout),
        "Object Lock: Enabled\nDefault retention:\n  mode: GOVERNANCE\n  days: 1\n"
    );
    let expected = json!({
        "kind": "object-lock",
        "enabled": true,
        "default_retention": {"mode": "GOVERNANCE", "days": 1, "years": null},
    });
    assert_eq!(bucket_lock_json(&minio, bucket), expected);

    // A zero period is not a retention period (the S3 contract requires a
    // positive integer): the service refuses it and the stored rule is unchanged.
    for unit in ["--days", "--years"] {
        let zero = run_s3m_with_minio(
            &minio,
            &[
                "object-lock",
                "set",
                &format!("s3/{}", bucket),
                "--mode",
                "GOVERNANCE",
                unit,
                "0",
            ],
        );
        assert_eq!(zero.status.code(), Some(1), "{unit} 0: {:?}", zero);
        assert!(
            String::from_utf8_lossy(&zero.stderr).contains("InvalidRetentionPeriod"),
            "{unit} 0: {:?}",
            zero
        );
        assert_eq!(bucket_lock_json(&minio, bucket), expected);
    }
}

fn bucket_lock_json(minio: &MinioContext, bucket: &str) -> Value {
    let out = run_s3m_with_minio(
        minio,
        &["object-lock", "get", &format!("s3/{bucket}"), "--json"],
    );
    assert!(out.status.success(), "{:?}", out);
    serde_json::from_slice(&out.stdout).unwrap()
}

fn object_lock_json(minio: &MinioContext, uri: &str, version_id: Option<&str>) -> Value {
    let mut args = vec!["object-lock", "get", uri, "--json"];
    if let Some(version_id) = version_id {
        args.extend(["--version-id", version_id]);
    }
    let out = run_s3m_with_minio(minio, &args);
    assert!(out.status.success(), "{:?}", out);
    serde_json::from_slice(&out.stdout).unwrap()
}

/// The JSON value at `pointer`; a missing field fails the test.
fn field<'v>(value: &'v Value, pointer: &str) -> &'v Value {
    let field = value.pointer(pointer);
    assert!(field.is_some(), "{pointer} missing in {value}");
    field.unwrap()
}

/// The retain-until instant of a JSON readback, compared as an instant.
fn retain_until(lock: &Value) -> chrono::DateTime<chrono::FixedOffset> {
    chrono::DateTime::parse_from_rfc3339(
        field(lock, "/retention/retain_until_date")
            .as_str()
            .unwrap(),
    )
    .unwrap()
}

fn instant(date: &str) -> chrono::DateTime<chrono::FixedOffset> {
    chrono::DateTime::parse_from_rfc3339(date).unwrap()
}

#[tokio::test]
async fn test_e2e_object_lock_per_object_retention() {
    let minio = MinioContext::get_or_start().await;
    let owned = OwnedBucket::new(&minio, "e2e-objlock-retention");
    let bucket = owned.name.as_str();

    let cb = run_s3m_with_minio(&minio, &["cb", "--object-lock", &format!("s3/{}", bucket)]);
    let cb_err = String::from_utf8_lossy(&cb.stderr);
    assert!(
        cb.status.success(),
        "object-lock bucket creation should succeed: {}",
        cb_err
    );

    // Upload without lock flags, then set per-object retention explicitly.
    let file = create_test_file_with_content(1024, "PER_OBJ_RET_");
    let file_path = file.path().to_str().expect("Invalid path");
    let s3_uri = format!("s3/{}/retained.dat", bucket);
    assert!(
        run_s3m_with_minio(&minio, &[file_path, &s3_uri])
            .status
            .success(),
        "upload should succeed"
    );

    // Before setting, get reports no retention (and the command must not error).
    let before = run_s3m_with_minio(&minio, &["object-lock", "get", &s3_uri]);
    assert!(
        before.status.success(),
        "get with no retention should succeed: {}",
        String::from_utf8_lossy(&before.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&before.stdout),
        "Retention: none\nLegal hold: OFF\n"
    );

    // An offset date is normalized to the same UTC instant before it is sent.
    let set = run_s3m_with_minio(
        &minio,
        &[
            "object-lock",
            "set",
            &s3_uri,
            "--mode",
            "GOVERNANCE",
            "--retain-until",
            "2031-01-01T02:00:00+02:00",
        ],
    );
    assert!(
        set.status.success(),
        "set per-object retention should succeed: {}",
        String::from_utf8_lossy(&set.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&set.stdout),
        format!("retention set: mode=GOVERNANCE until={FUTURE_DATE}\n")
    );

    let after = object_lock_json(&minio, &s3_uri, None);
    assert_eq!(*field(&after, "/retention/mode"), "GOVERNANCE");
    assert_eq!(retain_until(&after), instant(FUTURE_DATE));
    assert_eq!(*field(&after, "/legal_hold"), "OFF");
}

#[tokio::test]
async fn test_e2e_object_lock_version_delete_enforced() {
    let minio = MinioContext::get_or_start().await;
    let owned = OwnedBucket::new(&minio, "e2e-objlock-enforce");
    let bucket = owned.name.as_str();

    let cb = run_s3m_with_minio(&minio, &["cb", "--object-lock", &format!("s3/{}", bucket)]);
    let cb_err = String::from_utf8_lossy(&cb.stderr);
    assert!(
        cb.status.success(),
        "object-lock bucket creation should succeed: {}",
        cb_err
    );

    // Upload with GOVERNANCE retention; capture the locked version id.
    let file = create_test_file_with_content(1024, "ENFORCE_");
    let file_path = file.path().to_str().expect("Invalid path");
    let s3_uri = format!("s3/{}/locked.dat", bucket);
    let upload = run_s3m_with_minio(
        &minio,
        &[
            file_path,
            &s3_uri,
            "--object-lock-mode",
            "GOVERNANCE",
            "--retain-until",
            FUTURE_DATE,
        ],
    );
    assert!(upload.status.success(), "locked upload should succeed");
    let vid = version_id_from(&upload.stdout);

    // Deleting that version WITHOUT bypass must be refused by Object Lock.
    let denied = run_s3m_with_minio(&minio, &["rm", &s3_uri, "--version-id", &vid]);
    assert!(
        !denied.status.success(),
        "deleting a GOVERNANCE-locked version without bypass must fail"
    );
    let err = String::from_utf8_lossy(&denied.stderr).to_lowercase();
    assert!(
        err.contains("denied")
            || err.contains("worm")
            || err.contains("retention")
            || err.contains("lock"),
        "error should indicate Object Lock protection, got: {}",
        err
    );

    // WITH --bypass-governance it succeeds.
    let ok = run_s3m_with_minio(
        &minio,
        &["rm", &s3_uri, "--version-id", &vid, "--bypass-governance"],
    );
    assert!(
        ok.status.success(),
        "deleting the locked version with --bypass-governance should succeed: {}",
        String::from_utf8_lossy(&ok.stderr)
    );
}

#[tokio::test]
async fn test_e2e_delete_marker_reporting() {
    let minio = MinioContext::get_or_start().await;
    let owned = OwnedBucket::new(&minio, "e2e-objlock-marker");
    let bucket = owned.name.as_str();

    // cb --object-lock enables versioning, so a keyed delete inserts a marker.
    let cb = run_s3m_with_minio(&minio, &["cb", "--object-lock", &format!("s3/{}", bucket)]);
    let cb_err = String::from_utf8_lossy(&cb.stderr);
    assert!(
        cb.status.success(),
        "bucket creation should succeed: {}",
        cb_err
    );

    let file = create_test_file_with_content(512, "MARKER_");
    let file_path = file.path().to_str().expect("Invalid path");
    let s3_uri = format!("s3/{}/plain.dat", bucket);
    assert!(
        run_s3m_with_minio(&minio, &[file_path, &s3_uri])
            .status
            .success(),
        "upload should succeed"
    );

    // A keyed delete on a versioned bucket reports that a delete marker was made.
    let rm = run_s3m_with_minio(&minio, &["rm", &s3_uri]);
    assert!(rm.status.success(), "rm should succeed (delete marker)");
    assert!(
        String::from_utf8_lossy(&rm.stdout).contains("delete marker created"),
        "rm should report a delete marker, got: {}",
        String::from_utf8_lossy(&rm.stdout)
    );
}

#[tokio::test]
async fn test_e2e_object_lock_per_object_legal_hold() {
    let minio = MinioContext::get_or_start().await;
    let owned = OwnedBucket::new(&minio, "e2e-objlock-legalhold");
    let bucket = owned.name.as_str();

    let cb = run_s3m_with_minio(&minio, &["cb", "--object-lock", &format!("s3/{}", bucket)]);
    let cb_err = String::from_utf8_lossy(&cb.stderr);
    assert!(
        cb.status.success(),
        "object-lock bucket creation should succeed: {}",
        cb_err
    );

    let file = create_test_file_with_content(1024, "LEGAL_HOLD_");
    let file_path = file.path().to_str().expect("Invalid path");
    let s3_uri = format!("s3/{}/held.dat", bucket);
    assert!(
        run_s3m_with_minio(&minio, &[file_path, &s3_uri])
            .status
            .success(),
        "upload should succeed"
    );

    // Turn the legal hold ON, then read it back.
    let on = run_s3m_with_minio(
        &minio,
        &["object-lock", "set", &s3_uri, "--legal-hold", "on"],
    );
    assert!(
        on.status.success(),
        "legal-hold on should succeed: {}",
        String::from_utf8_lossy(&on.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&on.stdout), "legal hold: ON\n");
    let get_on = run_s3m_with_minio(&minio, &["object-lock", "get", &s3_uri]);
    assert!(
        get_on.status.success(),
        "get legal hold should succeed: {}",
        String::from_utf8_lossy(&get_on.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&get_on.stdout),
        "Retention: none\nLegal hold: ON\n"
    );

    // Turn it OFF again (otherwise the object can't be deleted).
    let off = run_s3m_with_minio(
        &minio,
        &["object-lock", "set", &s3_uri, "--legal-hold", "off"],
    );
    assert!(off.status.success(), "legal-hold off should succeed");
    assert_eq!(String::from_utf8_lossy(&off.stdout), "legal hold: OFF\n");
    assert_eq!(
        object_lock_json(&minio, &s3_uri, None),
        json!({"kind": "object-lock", "key": "held.dat", "retention": null, "legal_hold": "OFF"})
    );
}

#[tokio::test]
async fn test_e2e_object_lock_version_scoped_retention_and_legal_hold() {
    let minio = MinioContext::get_or_start().await;
    let owned = OwnedBucket::new(&minio, "versions");
    let bucket = owned.name.as_str();
    make_lock_bucket(&minio, bucket);

    // Distinct bytes per version, so each upload creates a new version.
    let first_file = create_test_file_with_content(512, &format!("V1_{}_", bucket));
    let second_file = create_test_file_with_content(512, &format!("V2_{}_", bucket));
    let uri = format!("s3/{}/doc.dat", bucket);
    let first = run_s3m_with_minio(&minio, &[first_file.path().to_str().unwrap(), &uri]);
    assert!(first.status.success(), "{:?}", first);
    let older = version_id_from(&first.stdout);
    let second = run_s3m_with_minio(&minio, &[second_file.path().to_str().unwrap(), &uri]);
    assert!(second.status.success(), "{:?}", second);
    let latest = version_id_from(&second.stdout);
    assert_ne!(older, latest);

    let set = run_s3m_with_minio(
        &minio,
        &[
            "object-lock",
            "set",
            &uri,
            "--version-id",
            &older,
            "--mode",
            "GOVERNANCE",
            "--retain-until",
            FUTURE_DATE,
            "--legal-hold",
            "on",
        ],
    );
    assert!(set.status.success(), "{:?}", set);

    // Only the addressed (older) version carries the retention and the hold.
    let older_lock = object_lock_json(&minio, &uri, Some(&older));
    assert_eq!(*field(&older_lock, "/retention/mode"), "GOVERNANCE");
    assert_eq!(retain_until(&older_lock), instant(FUTURE_DATE));
    assert_eq!(*field(&older_lock, "/legal_hold"), "ON");
    let unlocked =
        json!({"kind": "object-lock", "key": "doc.dat", "retention": null, "legal_hold": "OFF"});
    assert_eq!(object_lock_json(&minio, &uri, Some(&latest)), unlocked);
    assert_eq!(object_lock_json(&minio, &uri, None), unlocked);

    // The latest version is deletable; the held older version is not until
    // its hold is released for that version id.
    let held = run_s3m_with_minio(
        &minio,
        &["rm", &uri, "--version-id", &older, "--bypass-governance"],
    );
    assert_eq!(held.status.code(), Some(1), "{:?}", held);
    let release = run_s3m_with_minio(
        &minio,
        &[
            "object-lock",
            "set",
            &uri,
            "--version-id",
            &older,
            "--legal-hold",
            "off",
        ],
    );
    assert!(release.status.success(), "{:?}", release);
    assert_eq!(
        *field(&object_lock_json(&minio, &uri, Some(&older)), "/legal_hold"),
        "OFF"
    );
    let removed = run_s3m_with_minio(
        &minio,
        &["rm", &uri, "--version-id", &older, "--bypass-governance"],
    );
    assert!(removed.status.success(), "{:?}", removed);
    assert_eq!(
        String::from_utf8_lossy(&removed.stdout),
        format!("doc.dat: deleted version {older}\n")
    );
}

// ---------------------------------------------------------------------------
// Upload-time lock variants
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_e2e_upload_lock_modes() {
    let minio = MinioContext::get_or_start().await;
    let owned = OwnedBucket::retained(&minio, "e2e-objlock-modes");
    let bucket = owned.name.as_str();
    make_lock_bucket(&minio, bucket);

    let file = create_test_file_with_content(512, "MODES_");
    let path = file.path().to_str().expect("path");

    // GOVERNANCE retention.
    let gov = format!("s3/{}/gov.dat", bucket);
    assert!(
        run_s3m_with_minio(
            &minio,
            &[
                path,
                &gov,
                "--object-lock-mode",
                "GOVERNANCE",
                "--retain-until",
                FUTURE_DATE
            ]
        )
        .status
        .success(),
        "GOVERNANCE upload should succeed"
    );
    assert!(
        String::from_utf8_lossy(&run_s3m_with_minio(&minio, &["object-lock", "get", &gov]).stdout)
            .contains("GOVERNANCE")
    );

    // COMPLIANCE retention.
    let comp = format!("s3/{}/comp.dat", bucket);
    assert!(
        run_s3m_with_minio(
            &minio,
            &[
                path,
                &comp,
                "--object-lock-mode",
                "COMPLIANCE",
                "--retain-until",
                FUTURE_DATE
            ]
        )
        .status
        .success(),
        "COMPLIANCE upload should succeed"
    );
    assert!(
        String::from_utf8_lossy(&run_s3m_with_minio(&minio, &["object-lock", "get", &comp]).stdout)
            .contains("COMPLIANCE")
    );

    // Legal hold only (no retention).
    let hold = format!("s3/{}/hold.dat", bucket);
    assert!(
        run_s3m_with_minio(&minio, &[path, &hold, "--legal-hold"])
            .status
            .success(),
        "legal-hold-only upload should succeed"
    );
    let hold_out =
        String::from_utf8_lossy(&run_s3m_with_minio(&minio, &["object-lock", "get", &hold]).stdout)
            .to_string();
    assert!(hold_out.contains("Legal hold: ON"), "got: {hold_out}");

    // Retention + legal hold together.
    let both = format!("s3/{}/both.dat", bucket);
    assert!(
        run_s3m_with_minio(
            &minio,
            &[
                path,
                &both,
                "--object-lock-mode",
                "GOVERNANCE",
                "--retain-until",
                FUTURE_DATE,
                "--legal-hold"
            ]
        )
        .status
        .success(),
        "retention+legal-hold upload should succeed"
    );
    let both_out =
        String::from_utf8_lossy(&run_s3m_with_minio(&minio, &["object-lock", "get", &both]).stdout)
            .to_string();
    assert!(
        both_out.contains("GOVERNANCE") && both_out.contains("Legal hold: ON"),
        "got: {both_out}"
    );

    for (uri, mode) in [
        (&gov, "GOVERNANCE"),
        (&comp, "COMPLIANCE"),
        (&both, "GOVERNANCE"),
    ] {
        let lock = read_object_lock(&minio, uri);
        assert_retention(&lock, mode);
    }
    // Turn the legal holds off so the bucket can be torn down cleanly.
    for key in [&hold, &both] {
        assert!(
            run_s3m_with_minio(&minio, &["object-lock", "set", key, "--legal-hold", "off"])
                .status
                .success()
        );
    }
}

#[tokio::test]
async fn test_e2e_upload_lock_via_pipe() {
    let minio = MinioContext::get_or_start().await;
    let owned = OwnedBucket::new(&minio, "e2e-objlock-pipe");
    let bucket = owned.name.as_str();
    make_lock_bucket(&minio, bucket);

    // Streaming (--pipe) upload must carry retention through the stream engine.
    let uri = format!("s3/{}/piped.dat", bucket);
    let out = run_with_stdin(
        &minio,
        &[
            "--pipe",
            &uri,
            "--object-lock-mode",
            "GOVERNANCE",
            "--retain-until",
            FUTURE_DATE,
        ],
        b"streamed-worm-payload",
    );
    assert!(
        out.status.success(),
        "piped lock upload should succeed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let got = run_s3m_with_minio(&minio, &["object-lock", "get", &uri]);
    assert!(
        String::from_utf8_lossy(&got.stdout).contains("GOVERNANCE"),
        "piped upload should carry retention, got: {}",
        String::from_utf8_lossy(&got.stdout)
    );
    let lock = read_object_lock(&minio, &uri);
    assert_retention(&lock, "GOVERNANCE");
}

#[tokio::test]
async fn test_e2e_upload_lock_on_plain_bucket_via_pipe_fails() {
    let minio = MinioContext::get_or_start().await;
    let owned = OwnedBucket::new(&minio, "e2e-pipe-plain");
    let bucket = owned.name.as_str();
    minio.create_bucket(bucket).await.expect("plain bucket");

    let uri = format!("s3/{}/nope.dat", bucket);
    let out = run_with_stdin(
        &minio,
        &[
            "--pipe",
            &uri,
            "--object-lock-mode",
            "COMPLIANCE",
            "--retain-until",
            FUTURE_DATE,
        ],
        b"data",
    );
    assert!(
        !out.status.success(),
        "piped lock upload to a plain bucket must fail"
    );
}

// ---------------------------------------------------------------------------
// Enforcement
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_e2e_compliance_version_delete_cannot_be_bypassed() {
    let minio = MinioContext::get_or_start().await;
    let owned = OwnedBucket::retained(&minio, "e2e-objlock-compliance");
    let bucket = owned.name.as_str();
    make_lock_bucket(&minio, bucket);

    let file = create_test_file_with_content(1024, "COMPLIANCE_");
    let path = file.path().to_str().expect("path");
    let uri = format!("s3/{}/sealed.dat", bucket);
    let upload = run_s3m_with_minio(
        &minio,
        &[
            path,
            &uri,
            "--object-lock-mode",
            "COMPLIANCE",
            "--retain-until",
            FUTURE_DATE,
        ],
    );
    assert!(upload.status.success(), "COMPLIANCE upload should succeed");
    let vid = version_id_from(&upload.stdout);

    // Without bypass: refused.
    assert!(
        !run_s3m_with_minio(&minio, &["rm", &uri, "--version-id", &vid])
            .status
            .success(),
        "COMPLIANCE version delete must be refused"
    );

    // Even WITH --bypass-governance: still refused (COMPLIANCE cannot be bypassed).
    let bypass = run_s3m_with_minio(
        &minio,
        &["rm", &uri, "--version-id", &vid, "--bypass-governance"],
    );
    assert!(
        !bypass.status.success(),
        "COMPLIANCE retention must not be bypassable with --bypass-governance"
    );

    // Shortening or downgrading COMPLIANCE retention is refused even with
    // --bypass-governance, and the stored retention is unchanged.
    for (mode, date) in [
        ("COMPLIANCE", "2030-01-01T00:00:00Z"),
        ("GOVERNANCE", FUTURE_DATE),
    ] {
        let shorten = run_s3m_with_minio(
            &minio,
            &[
                "object-lock",
                "set",
                &uri,
                "--version-id",
                &vid,
                "--mode",
                mode,
                "--retain-until",
                date,
                "--bypass-governance",
            ],
        );
        assert_eq!(shorten.status.code(), Some(1), "{mode}: {:?}", shorten);
        let lock = object_lock_json(&minio, &uri, Some(&vid));
        assert_eq!(*field(&lock, "/retention/mode"), "COMPLIANCE");
        assert_eq!(retain_until(&lock), instant(FUTURE_DATE));
    }
}

#[tokio::test]
async fn test_e2e_legal_hold_blocks_version_delete() {
    let minio = MinioContext::get_or_start().await;
    let owned = OwnedBucket::new(&minio, "e2e-objlock-hold-delete");
    let bucket = owned.name.as_str();
    make_lock_bucket(&minio, bucket);

    // Upload with a legal hold, no retention.
    let file = create_test_file_with_content(1024, "HOLD_DEL_");
    let path = file.path().to_str().expect("path");
    let uri = format!("s3/{}/held.dat", bucket);
    let upload = run_s3m_with_minio(&minio, &[path, &uri, "--legal-hold"]);
    assert!(upload.status.success(), "legal-hold upload should succeed");
    let vid = version_id_from(&upload.stdout);

    // Legal hold blocks version delete; --bypass-governance does NOT help (it is
    // for GOVERNANCE retention, not legal holds).
    assert!(
        !run_s3m_with_minio(
            &minio,
            &["rm", &uri, "--version-id", &vid, "--bypass-governance"]
        )
        .status
        .success(),
        "legal hold must block version delete (even with --bypass-governance)"
    );

    // Release the hold, then the version can be deleted.
    assert!(
        run_s3m_with_minio(&minio, &["object-lock", "set", &uri, "--legal-hold", "off"])
            .status
            .success(),
        "releasing legal hold should succeed"
    );
    assert!(
        run_s3m_with_minio(&minio, &["rm", &uri, "--version-id", &vid])
            .status
            .success(),
        "version delete should succeed once the legal hold is off"
    );
}

#[tokio::test]
async fn test_e2e_governance_shorten_requires_bypass() {
    let minio = MinioContext::get_or_start().await;
    let owned = OwnedBucket::new(&minio, "e2e-objlock-shorten");
    let bucket = owned.name.as_str();
    make_lock_bucket(&minio, bucket);

    let file = create_test_file_with_content(1024, "SHORTEN_");
    let path = file.path().to_str().expect("path");
    let uri = format!("s3/{}/obj.dat", bucket);
    assert!(
        run_s3m_with_minio(
            &minio,
            &[
                path,
                &uri,
                "--object-lock-mode",
                "GOVERNANCE",
                "--retain-until",
                FUTURE_DATE
            ]
        )
        .status
        .success(),
        "upload should succeed"
    );

    // Shortening retention (earlier date) without bypass is refused.
    let earlier = "2030-01-01T00:00:00Z";
    assert!(
        !run_s3m_with_minio(
            &minio,
            &[
                "object-lock",
                "set",
                &uri,
                "--mode",
                "GOVERNANCE",
                "--retain-until",
                earlier
            ]
        )
        .status
        .success(),
        "shortening GOVERNANCE retention without bypass must be refused"
    );

    assert_eq!(
        retain_until(&object_lock_json(&minio, &uri, None)),
        instant(FUTURE_DATE)
    );

    // With --bypass-governance it is allowed.
    assert!(
        run_s3m_with_minio(
            &minio,
            &[
                "object-lock",
                "set",
                &uri,
                "--mode",
                "GOVERNANCE",
                "--retain-until",
                earlier,
                "--bypass-governance"
            ]
        )
        .status
        .success(),
        "shortening with --bypass-governance should succeed"
    );
    let lock = object_lock_json(&minio, &uri, None);
    assert_eq!(*field(&lock, "/retention/mode"), "GOVERNANCE");
    assert_eq!(retain_until(&lock), instant(earlier));
}

#[tokio::test]
async fn test_e2e_recursive_delete_blocked_by_lock() {
    let minio = MinioContext::get_or_start().await;
    let owned = OwnedBucket::new(&minio, "e2e-objlock-recursive");
    let bucket = owned.name.as_str();
    make_lock_bucket(&minio, bucket);

    let file = create_test_file_with_content(1024, "RECURSE_");
    let path = file.path().to_str().expect("path");
    let free_uri = format!("s3/{}/free.dat", bucket);
    assert!(
        run_s3m_with_minio(&minio, &[path, &free_uri])
            .status
            .success()
    );
    let uri = format!("s3/{}/locked.dat", bucket);
    assert!(
        run_s3m_with_minio(
            &minio,
            &[
                path,
                &uri,
                "--object-lock-mode",
                "GOVERNANCE",
                "--retain-until",
                FUTURE_DATE
            ]
        )
        .status
        .success(),
        "upload should succeed"
    );

    // The locked version keeps the bucket from being emptied/deleted.
    let rm = run_s3m_with_minio(
        &minio,
        &["rm", "-b", "--recursive", &format!("s3/{}", bucket)],
    );
    assert!(
        !rm.status.success(),
        "recursive delete of a bucket with a locked version must fail"
    );
    // Partial failure: the unlocked object is gone, exactly the locked object
    // survives, and the bucket itself is kept.
    let listing = run_s3m_with_minio(&minio, &["ls", &format!("s3/{}", bucket), "--json"]);
    assert!(listing.status.success(), "{:?}", listing);
    let listing: Value = serde_json::from_slice(&listing.stdout).unwrap();
    let keys: Vec<&str> = field(&listing, "/objects")
        .as_array()
        .unwrap()
        .iter()
        .map(|object| field(object, "/key").as_str().unwrap())
        .collect();
    assert_eq!(keys, ["locked.dat"]);

    // With --bypass-governance the recursive delete purges the locked versions
    // and removes the bucket.
    let bypass = run_s3m_with_minio(
        &minio,
        &[
            "rm",
            "-b",
            "--recursive",
            "--bypass-governance",
            &format!("s3/{}", bucket),
        ],
    );
    assert!(
        bypass.status.success(),
        "recursive delete with --bypass-governance should succeed: {}",
        String::from_utf8_lossy(&bypass.stderr)
    );
}

#[tokio::test]
async fn test_e2e_recursive_delete_versioned_unlocked() {
    let minio = MinioContext::get_or_start().await;
    let owned = OwnedBucket::new(&minio, "e2e-recursive-versioned");
    let bucket = owned.name.as_str();
    // Object Lock enables versioning, but we upload WITHOUT retention.
    make_lock_bucket(&minio, bucket);

    let file = create_test_file_with_content(512, "VERS_");
    let path = file.path().to_str().expect("path");
    let uri = format!("s3/{}/v.dat", bucket);

    // Create multiple versions + a delete marker, none of them locked.
    assert!(run_s3m_with_minio(&minio, &[path, &uri]).status.success());
    assert!(run_s3m_with_minio(&minio, &[path, &uri]).status.success());
    assert!(run_s3m_with_minio(&minio, &["rm", &uri]).status.success());

    // Recursive delete must purge all versions + the delete marker (no bypass
    // needed, since nothing is retained) and remove the bucket.
    let rm = run_s3m_with_minio(
        &minio,
        &["rm", "-b", "--recursive", &format!("s3/{}", bucket)],
    );
    assert!(
        rm.status.success(),
        "recursive delete of an unlocked versioned bucket should succeed: {}",
        String::from_utf8_lossy(&rm.stderr)
    );
}

// ---------------------------------------------------------------------------
// Bucket configuration states & JSON output
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_e2e_bucket_config_states() {
    let minio = MinioContext::get_or_start().await;

    // Lock-enabled bucket with no default rule yet.
    let owned_locked = OwnedBucket::new(&minio, "e2e-cfg-locked");
    let locked = owned_locked.name.as_str();
    make_lock_bucket(&minio, locked);
    let no_rule = run_s3m_with_minio(&minio, &["object-lock", "get", &format!("s3/{}", locked)]);
    assert!(no_rule.status.success());
    assert!(
        String::from_utf8_lossy(&no_rule.stdout).contains("Enabled"),
        "lock bucket without a default rule should report Enabled"
    );

    // Set a default retention in years and read it back.
    assert!(
        run_s3m_with_minio(
            &minio,
            &[
                "object-lock",
                "set",
                &format!("s3/{}", locked),
                "--mode",
                "COMPLIANCE",
                "--years",
                "2"
            ]
        )
        .status
        .success()
    );
    let years = run_s3m_with_minio(&minio, &["object-lock", "get", &format!("s3/{}", locked)]);
    assert_eq!(
        String::from_utf8_lossy(&years.stdout),
        "Object Lock: Enabled\nDefault retention:\n  mode: COMPLIANCE\n  years: 2\n"
    );
    assert_eq!(
        bucket_lock_json(&minio, locked),
        json!({
            "kind": "object-lock",
            "enabled": true,
            "default_retention": {"mode": "COMPLIANCE", "days": null, "years": 2},
        })
    );

    // Plain (non-lock) bucket reports "not enabled" and the command still succeeds.
    let owned_plain = OwnedBucket::new(&minio, "e2e-cfg-plain");
    let plain = owned_plain.name.as_str();
    minio.create_bucket(plain).await.expect("plain bucket");
    let plain_cfg = run_s3m_with_minio(&minio, &["object-lock", "get", &format!("s3/{}", plain)]);
    assert!(
        plain_cfg.status.success(),
        "get on a plain bucket should not error"
    );
    assert_eq!(
        String::from_utf8_lossy(&plain_cfg.stdout),
        "Object Lock: not enabled on this bucket\n"
    );
    assert_eq!(
        bucket_lock_json(&minio, plain),
        json!({"kind": "object-lock", "enabled": false})
    );

    // Object Lock settings on a plain bucket's object are a service error.
    let file = create_test_file_with_content(64, "PLAIN_OBJ_");
    let uri = format!("s3/{}/plain.dat", plain);
    assert!(
        run_s3m_with_minio(&minio, &[file.path().to_str().unwrap(), &uri])
            .status
            .success()
    );
    for args in [
        vec!["object-lock", "set", &uri, "--legal-hold", "on"],
        vec![
            "object-lock",
            "set",
            &uri,
            "--mode",
            "GOVERNANCE",
            "--retain-until",
            FUTURE_DATE,
        ],
        vec!["object-lock", "get", &uri],
    ] {
        let out = run_s3m_with_minio(&minio, &args);
        assert_eq!(out.status.code(), Some(1), "{args:?}: {:?}", out);
        assert!(out.stdout.is_empty(), "{args:?}: {:?}", out);
    }

    // A bucket that does not exist is an error, never "not enabled".
    let missing = format!("s3/{}-missing", plain);
    for args in [
        vec!["object-lock", "get", missing.as_str()],
        vec!["object-lock", "get", missing.as_str(), "--json"],
    ] {
        let out = run_s3m_with_minio(&minio, &args);
        assert_eq!(out.status.code(), Some(1), "{args:?}: {:?}", out);
        assert!(out.stdout.is_empty(), "{args:?}: {:?}", out);
        assert!(
            String::from_utf8_lossy(&out.stderr).contains("NoSuchBucket"),
            "{args:?}: {:?}",
            out
        );
    }
}

#[tokio::test]
async fn test_e2e_object_lock_json_output() {
    let minio = MinioContext::get_or_start().await;
    let owned = OwnedBucket::new(&minio, "e2e-objlock-json");
    let bucket = owned.name.as_str();
    make_lock_bucket(&minio, bucket);

    // Bucket default retention as JSON.
    run_s3m_with_minio(
        &minio,
        &[
            "object-lock",
            "set",
            &format!("s3/{}", bucket),
            "--mode",
            "GOVERNANCE",
            "--days",
            "7",
        ],
    );
    assert_eq!(
        bucket_lock_json(&minio, bucket),
        json!({
            "kind": "object-lock",
            "enabled": true,
            "default_retention": {"mode": "GOVERNANCE", "days": 7, "years": null},
        })
    );

    // Object retention as JSON.
    let file = create_test_file_with_content(256, "JSON_");
    let path = file.path().to_str().expect("path");
    let uri = format!("s3/{}/j.dat", bucket);
    run_s3m_with_minio(
        &minio,
        &[
            path,
            &uri,
            "--object-lock-mode",
            "GOVERNANCE",
            "--retain-until",
            FUTURE_DATE,
        ],
    );
    let object = object_lock_json(&minio, &uri, None);
    let mut keys: Vec<&str> = object
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    assert_eq!(keys, ["key", "kind", "legal_hold", "retention"]);
    assert_eq!(*field(&object, "/kind"), "object-lock");
    assert_eq!(*field(&object, "/key"), "j.dat");
    assert_eq!(*field(&object, "/retention/mode"), "GOVERNANCE");
    assert_eq!(retain_until(&object), instant(FUTURE_DATE));
    assert_eq!(*field(&object, "/legal_hold"), "OFF");
}

// ---------------------------------------------------------------------------
// Argument validation and error mapping against a local mock service
// ---------------------------------------------------------------------------

fn run_against(server: &Server, args: &[&str]) -> Output {
    let config = create_config_file(&server.url(), "minioadmin", "minioadmin");
    let output = Command::new(get_s3m_binary())
        .arg("--config")
        .arg(config.path())
        .args(args)
        .output()
        .expect("run s3m");
    drop(config);
    output
}

const TARGET_ERROR: &str = "to an object target (host/bucket/key), not a bucket";

const INVALID_OBJECT_LOCK_INVOCATIONS: &[(&[&str], i32, &str)] = &[
    (&["object-lock"], 2, "Usage:"),
    (&["object-lock", "get"], 2, "required arguments"),
    (
        &[
            "object-lock",
            "set",
            "s3/vault",
            "--mode",
            "WORM",
            "--days",
            "1",
        ],
        2,
        "invalid value 'WORM'",
    ),
    (
        &[
            "object-lock",
            "set",
            "s3/vault",
            "--mode",
            "GOVERNANCE",
            "--days",
            "1",
            "--years",
            "1",
        ],
        2,
        "cannot be used with",
    ),
    (
        &[
            "object-lock",
            "set",
            "s3/vault",
            "--mode",
            "GOVERNANCE",
            "--days=-1",
        ],
        2,
        "invalid value '-1'",
    ),
    (
        &["object-lock", "set", "s3/vault", "--mode", "GOVERNANCE"],
        1,
        "bucket default retention requires exactly one of --days or --years",
    ),
    (
        &["object-lock", "set", "s3/vault", "--days", "1"],
        1,
        "--mode is required to set a bucket default retention",
    ),
    (
        &[
            "object-lock",
            "set",
            "s3/vault/k",
            "--mode",
            "GOVERNANCE",
            "--days",
            "1",
        ],
        1,
        "--days/--years apply to a bucket target",
    ),
    (
        &[
            "object-lock",
            "set",
            "s3/vault",
            "--mode",
            "GOVERNANCE",
            "--retain-until",
            FUTURE_DATE,
        ],
        1,
        TARGET_ERROR,
    ),
    (
        &["object-lock", "set", "s3/vault", "--legal-hold", "on"],
        1,
        TARGET_ERROR,
    ),
    (
        &[
            "object-lock",
            "set",
            "s3/vault",
            "--mode",
            "GOVERNANCE",
            "--days",
            "1",
            "--version-id",
            "v1",
        ],
        1,
        TARGET_ERROR,
    ),
    (
        &[
            "object-lock",
            "set",
            "s3/vault",
            "--mode",
            "GOVERNANCE",
            "--days",
            "1",
            "--bypass-governance",
        ],
        1,
        TARGET_ERROR,
    ),
    (
        &["object-lock", "get", "s3/vault", "--version-id", "v1"],
        1,
        TARGET_ERROR,
    ),
    (
        &["object-lock", "set", "s3/vault/k", "--legal-hold", "maybe"],
        2,
        "invalid value 'maybe'",
    ),
    (
        &["object-lock", "set", "s3/vault/k", "--mode", "GOVERNANCE"],
        1,
        "--mode and --retain-until must be used together for an object",
    ),
    (
        &[
            "object-lock",
            "set",
            "s3/vault/k",
            "--retain-until",
            FUTURE_DATE,
        ],
        1,
        "--mode and --retain-until must be used together for an object",
    ),
    (
        &[
            "object-lock",
            "set",
            "s3/vault/k",
            "--mode",
            "GOVERNANCE",
            "--retain-until",
            "2031-13-01",
        ],
        1,
        "invalid date \"2031-13-01\"",
    ),
    (&["object-lock", "set", "s3/vault/k"], 1, "nothing to set"),
    (
        &[
            "object-lock",
            "set",
            "s3/vault/k",
            "--legal-hold",
            "on",
            "--bypass-governance",
        ],
        1,
        "--bypass-governance only applies when setting object retention",
    ),
];

#[test]
fn test_object_lock_rejects_invalid_flags_and_targets_without_requests() {
    let mut server = Server::new();
    let forbidden: Vec<mockito::Mock> = ["GET", "HEAD", "PUT", "POST", "DELETE"]
        .into_iter()
        .map(|method| {
            server
                .mock(method, Matcher::Any)
                .with_status(500)
                .expect(0)
                .create()
        })
        .collect();

    for (args, code, message) in INVALID_OBJECT_LOCK_INVOCATIONS {
        let out = run_against(&server, args);
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert_eq!(out.status.code(), Some(*code), "{args:?}: {stderr}");
        assert!(stderr.contains(message), "{args:?}: {stderr}");
        assert!(out.stdout.is_empty(), "{args:?}: {:?}", out.stdout);
    }
    for mock in forbidden {
        mock.assert();
    }
}

fn s3_error(status: usize, code: &str) -> (usize, String) {
    (
        status,
        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?><Error><Code>{code}</Code><Message>{code} message</Message><RequestId>r</RequestId></Error>"#
        ),
    )
}

#[test]
fn test_object_lock_get_separates_unset_settings_from_missing_resources() {
    let mut server = Server::new();

    // Missing lock configuration: the documented "not enabled" success.
    let (status, body) = s3_error(404, "ObjectLockConfigurationNotFoundError");
    let unset = server
        .mock("GET", "/plain")
        .match_query(Matcher::UrlEncoded("object-lock".into(), String::new()))
        .with_status(status)
        .with_body(body)
        .expect(1)
        .create();
    let out = run_against(&server, &["object-lock", "get", "s3/plain"]);
    assert!(out.status.success(), "{:?}", out);
    assert_eq!(
        String::from_utf8_lossy(&out.stdout),
        "Object Lock: not enabled on this bucket\n"
    );
    unset.assert();

    // Missing bucket, key or version: an error, not "none"/"not enabled".
    let (status, body) = s3_error(404, "NoSuchBucket");
    let bucket = server
        .mock("GET", "/gone")
        .match_query(Matcher::UrlEncoded("object-lock".into(), String::new()))
        .with_status(status)
        .with_body(body)
        .expect(1)
        .create();
    let out = run_against(&server, &["object-lock", "get", "s3/gone"]);
    assert_eq!(out.status.code(), Some(1), "{:?}", out);
    assert!(out.stdout.is_empty(), "{:?}", out);
    assert!(String::from_utf8_lossy(&out.stderr).contains("NoSuchBucket"));
    bucket.assert();

    for code in ["NoSuchKey", "NoSuchVersion"] {
        let (status, body) = s3_error(404, code);
        let retention = server
            .mock("GET", "/locked/missing.dat")
            .match_query(Matcher::UrlEncoded("retention".into(), String::new()))
            .with_status(status)
            .with_body(body)
            .expect(1)
            .create();
        let out = run_against(&server, &["object-lock", "get", "s3/locked/missing.dat"]);
        assert_eq!(out.status.code(), Some(1), "{code}: {:?}", out);
        assert!(out.stdout.is_empty(), "{code}: {:?}", out);
        assert!(
            String::from_utf8_lossy(&out.stderr).contains(code),
            "{:?}",
            out
        );
        retention.assert();
        retention.remove();
    }
}

#[derive(serde::Deserialize)]
struct ObjectLockReadback {
    retention: RetentionReadback,
    legal_hold: String,
}

#[derive(serde::Deserialize)]
struct RetentionReadback {
    mode: String,
    retain_until_date: String,
}

fn read_object_lock(minio: &MinioContext, uri: &str) -> ObjectLockReadback {
    let output = run_s3m_with_minio(minio, &["object-lock", "get", uri, "--json"]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

#[tokio::test]
async fn test_e2e_transformed_uploads_preserve_lock_and_bytes() {
    let minio = MinioContext::get_or_start().await;
    let bucket = OwnedBucket::new(&minio, "transform-lock");
    make_lock_bucket(&minio, &bucket.name);
    let data: Vec<u8> = (0..=255_u8).cycle().take(262_147).collect();
    let source = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(source.path(), &data).unwrap();
    for pipe in [false, true] {
        for (compress, encrypt, suffix) in [
            (true, false, ".zst"),
            (false, true, ".enc"),
            (true, true, ".zst.enc"),
        ] {
            let key = encrypt.then_some("01234567890123456789012345678901");
            let config = create_config_file_with_options(
                minio.endpoint(),
                minio.access_key(),
                minio.secret_key(),
                key,
                compress,
            );
            let uri = format!("s3/{}/transform-{pipe}-{compress}-{encrypt}", bucket.name);
            let mut command = Command::new(get_s3m_binary());
            command.args([
                "--config",
                config.path().to_str().unwrap(),
                "--object-lock-mode",
                "GOVERNANCE",
                "--retain-until",
                "2031-01-01T02:00:00+02:00",
                "--legal-hold",
            ]);
            if pipe {
                command.arg("--pipe");
            } else {
                command.arg(source.path());
            }
            let mut child = command
                .arg(&uri)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap();
            if pipe {
                child.stdin.take().unwrap().write_all(&data).unwrap();
            } else {
                drop(child.stdin.take());
            }
            let upload = child.wait_with_output().unwrap();
            assert!(
                upload.status.success(),
                "{}",
                String::from_utf8_lossy(&upload.stderr)
            );
            let stored = format!("{uri}{suffix}");
            let lock = read_object_lock(&minio, &stored);
            let released = run_s3m_with_minio(
                &minio,
                &["object-lock", "set", &stored, "--legal-hold", "off"],
            );
            assert!(released.status.success());
            assert_retention(&lock, "GOVERNANCE");
            assert_eq!(lock.legal_hold, "ON");

            let directory = tempfile::tempdir().unwrap();
            let decoded = directory.path().join("decoded");
            let download = Command::new(get_s3m_binary())
                .args([
                    "--config",
                    config.path().to_str().unwrap(),
                    "get",
                    &stored,
                    decoded.to_str().unwrap(),
                ])
                .output()
                .unwrap();
            assert!(
                download.status.success(),
                "{}",
                String::from_utf8_lossy(&download.stderr)
            );
            let downloaded = std::fs::read(decoded).unwrap();
            let restored = if compress {
                zstd::stream::decode_all(std::io::Cursor::new(downloaded)).unwrap()
            } else {
                downloaded
            };
            assert_eq!(restored, data);
            assert_eq!(blake3::hash(&restored), blake3::hash(&data));
        }
    }
}

fn assert_retention(lock: &ObjectLockReadback, mode: &str) {
    assert_eq!(lock.retention.mode, mode);
    let actual = &lock.retention.retain_until_date;
    assert_eq!(
        chrono::DateTime::parse_from_rfc3339(actual).unwrap(),
        chrono::DateTime::parse_from_rfc3339(FUTURE_DATE).unwrap()
    );
}
