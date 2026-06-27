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

use common::{
    MinioContext, create_config_file, create_test_file_with_content, get_s3m_binary,
    run_s3m_with_minio,
};
use std::io::Write;
use std::process::{Command, Output, Stdio};

/// Far-future retain-until date so the test is not time-sensitive.
const FUTURE_DATE: &str = "2099-01-01T00:00:00Z";

/// Run `s3m` against the test `MinIO`, feeding `stdin` (for `--pipe` uploads).
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

/// Create an Object-Lock-enabled bucket, tolerating "already exists".
fn make_lock_bucket(minio: &MinioContext, bucket: &str) {
    let cb = run_s3m_with_minio(minio, &["cb", "--object-lock", &format!("s3/{}", bucket)]);
    let err = String::from_utf8_lossy(&cb.stderr);
    assert!(
        cb.status.success()
            || err.contains("BucketAlreadyOwnedByYou")
            || err.contains("BucketAlreadyExists"),
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
    let bucket = "e2e-objlock-bucket";

    // Create an Object-Lock-enabled bucket.
    let cb = run_s3m_with_minio(&minio, &["cb", "--object-lock", &format!("s3/{}", bucket)]);
    let cb_err = String::from_utf8_lossy(&cb.stderr);
    assert!(
        cb.status.success()
            || cb_err.contains("BucketAlreadyOwnedByYou")
            || cb_err.contains("BucketAlreadyExists"),
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
    let bucket = "e2e-plain-bucket";

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
    let bucket = "e2e-objlock-default";

    let cb = run_s3m_with_minio(&minio, &["cb", "--object-lock", &format!("s3/{}", bucket)]);
    let cb_err = String::from_utf8_lossy(&cb.stderr);
    assert!(
        cb.status.success()
            || cb_err.contains("BucketAlreadyOwnedByYou")
            || cb_err.contains("BucketAlreadyExists"),
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

    // Read it back (Put/GetObjectLockConfiguration round-trip).
    let get = run_s3m_with_minio(&minio, &["object-lock", "get", &format!("s3/{}", bucket)]);
    assert!(get.status.success(), "get bucket config should succeed");
    let stdout = String::from_utf8_lossy(&get.stdout);
    assert!(
        stdout.contains("GOVERNANCE") && stdout.contains('1'),
        "bucket config should show the default retention, got: {}",
        stdout
    );
}

#[tokio::test]
async fn test_e2e_object_lock_per_object_retention() {
    let minio = MinioContext::get_or_start().await;
    let bucket = "e2e-objlock-retention";

    let cb = run_s3m_with_minio(&minio, &["cb", "--object-lock", &format!("s3/{}", bucket)]);
    let cb_err = String::from_utf8_lossy(&cb.stderr);
    assert!(
        cb.status.success()
            || cb_err.contains("BucketAlreadyOwnedByYou")
            || cb_err.contains("BucketAlreadyExists"),
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

    let set = run_s3m_with_minio(
        &minio,
        &[
            "object-lock",
            "set",
            &s3_uri,
            "--mode",
            "GOVERNANCE",
            "--retain-until",
            FUTURE_DATE,
        ],
    );
    assert!(
        set.status.success(),
        "set per-object retention should succeed: {}",
        String::from_utf8_lossy(&set.stderr)
    );

    let after = run_s3m_with_minio(&minio, &["object-lock", "get", &s3_uri]);
    assert!(
        after.status.success(),
        "get object retention should succeed: {}",
        String::from_utf8_lossy(&after.stderr)
    );
    assert!(
        String::from_utf8_lossy(&after.stdout).contains("GOVERNANCE"),
        "object retention should read GOVERNANCE, got: {}",
        String::from_utf8_lossy(&after.stdout)
    );
}

#[tokio::test]
async fn test_e2e_object_lock_version_delete_enforced() {
    let minio = MinioContext::get_or_start().await;
    let bucket = "e2e-objlock-enforce";

    let cb = run_s3m_with_minio(&minio, &["cb", "--object-lock", &format!("s3/{}", bucket)]);
    let cb_err = String::from_utf8_lossy(&cb.stderr);
    assert!(
        cb.status.success()
            || cb_err.contains("BucketAlreadyOwnedByYou")
            || cb_err.contains("BucketAlreadyExists"),
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
    let bucket = "e2e-objlock-marker";

    // cb --object-lock enables versioning, so a keyed delete inserts a marker.
    let cb = run_s3m_with_minio(&minio, &["cb", "--object-lock", &format!("s3/{}", bucket)]);
    let cb_err = String::from_utf8_lossy(&cb.stderr);
    assert!(
        cb.status.success()
            || cb_err.contains("BucketAlreadyOwnedByYou")
            || cb_err.contains("BucketAlreadyExists"),
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
    let bucket = "e2e-objlock-legalhold";

    let cb = run_s3m_with_minio(&minio, &["cb", "--object-lock", &format!("s3/{}", bucket)]);
    let cb_err = String::from_utf8_lossy(&cb.stderr);
    assert!(
        cb.status.success()
            || cb_err.contains("BucketAlreadyOwnedByYou")
            || cb_err.contains("BucketAlreadyExists"),
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
    let get_on = run_s3m_with_minio(&minio, &["object-lock", "get", &s3_uri]);
    assert!(
        get_on.status.success(),
        "get legal hold should succeed: {}",
        String::from_utf8_lossy(&get_on.stderr)
    );
    assert!(
        String::from_utf8_lossy(&get_on.stdout).contains("ON"),
        "legal hold should read ON, got: {}",
        String::from_utf8_lossy(&get_on.stdout)
    );

    // Turn it OFF again (otherwise the object can't be deleted).
    let off = run_s3m_with_minio(
        &minio,
        &["object-lock", "set", &s3_uri, "--legal-hold", "off"],
    );
    assert!(off.status.success(), "legal-hold off should succeed");
    let get_off = run_s3m_with_minio(&minio, &["object-lock", "get", &s3_uri]);
    assert!(
        String::from_utf8_lossy(&get_off.stdout).contains("OFF"),
        "legal hold should read OFF"
    );
}

// ---------------------------------------------------------------------------
// Upload-time lock variants
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_e2e_upload_lock_modes() {
    let minio = MinioContext::get_or_start().await;
    let bucket = "e2e-objlock-modes";
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

    // Turn the legal holds off so the bucket can be torn down cleanly.
    for key in [&hold, &both] {
        run_s3m_with_minio(&minio, &["object-lock", "set", key, "--legal-hold", "off"]);
    }
}

#[tokio::test]
async fn test_e2e_upload_lock_via_pipe() {
    let minio = MinioContext::get_or_start().await;
    let bucket = "e2e-objlock-pipe";
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
}

#[tokio::test]
async fn test_e2e_upload_lock_on_plain_bucket_via_pipe_fails() {
    let minio = MinioContext::get_or_start().await;
    let bucket = "e2e-pipe-plain";
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
    let bucket = "e2e-objlock-compliance";
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
}

#[tokio::test]
async fn test_e2e_legal_hold_blocks_version_delete() {
    let minio = MinioContext::get_or_start().await;
    let bucket = "e2e-objlock-hold-delete";
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
    let bucket = "e2e-objlock-shorten";
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
}

#[tokio::test]
async fn test_e2e_recursive_delete_blocked_by_lock() {
    let minio = MinioContext::get_or_start().await;
    let bucket = "e2e-objlock-recursive";
    make_lock_bucket(&minio, bucket);

    let file = create_test_file_with_content(1024, "RECURSE_");
    let path = file.path().to_str().expect("path");
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
    let bucket = "e2e-recursive-versioned";
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
    let locked = "e2e-cfg-locked";
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
    let years_out = String::from_utf8_lossy(&years.stdout);
    assert!(
        years_out.contains("COMPLIANCE") && years_out.contains('2'),
        "default retention in years should read back, got: {years_out}"
    );

    // Plain (non-lock) bucket reports "not enabled" and the command still succeeds.
    let plain = "e2e-cfg-plain";
    minio.create_bucket(plain).await.expect("plain bucket");
    let plain_cfg = run_s3m_with_minio(&minio, &["object-lock", "get", &format!("s3/{}", plain)]);
    assert!(
        plain_cfg.status.success(),
        "get on a plain bucket should not error"
    );
    assert!(
        String::from_utf8_lossy(&plain_cfg.stdout).contains("not enabled"),
        "plain bucket should report Object Lock not enabled"
    );
}

#[tokio::test]
async fn test_e2e_object_lock_json_output() {
    let minio = MinioContext::get_or_start().await;
    let bucket = "e2e-objlock-json";
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
    let bucket_json = run_s3m_with_minio(
        &minio,
        &["object-lock", "get", &format!("s3/{}", bucket), "--json"],
    );
    let bj = String::from_utf8_lossy(&bucket_json.stdout);
    assert!(
        bj.contains("\"kind\": \"object-lock\"")
            && bj.contains("\"default_retention\"")
            && bj.contains("GOVERNANCE"),
        "bucket JSON should describe the default retention, got: {bj}"
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
    let obj_json = run_s3m_with_minio(&minio, &["object-lock", "get", &uri, "--json"]);
    let oj = String::from_utf8_lossy(&obj_json.stdout);
    assert!(
        oj.contains("\"kind\": \"object-lock\"")
            && oj.contains("\"retention\"")
            && oj.contains("\"legal_hold\""),
        "object JSON should include retention and legal_hold, got: {oj}"
    );
}
