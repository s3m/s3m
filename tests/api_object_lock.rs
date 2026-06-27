//! Library-API test: proves a third-party consumer (e.g. the `backup` tool) can
//! drive S3 Object Lock end-to-end through the **public `s3m_core` API alone** —
//! no CLI types. Every call below goes through `s3m_core::*`.
//!
//! The `MinIO` container harness only provides an endpoint + credentials; the S3
//! client and all Object Lock operations are built purely from `s3m_core`.

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::uninlined_format_args,
    clippy::missing_panics_doc
)]

mod common;

use common::{MinioContext, create_test_file_with_content};
use s3m_core::{
    Credentials, ObjectLock, ObjectLockMode, Region, RequestOptions, S3,
    s3::actions::{
        CreateBucket, GetObjectLegalHold, GetObjectLockConfiguration, GetObjectRetention,
        PutObject, PutObjectLegalHold, PutObjectLockConfiguration, PutObjectRetention,
    },
};
use secrecy::SecretString;

fn client(minio: &MinioContext, bucket: &str) -> S3 {
    let credentials = Credentials::new(
        minio.access_key(),
        &SecretString::new(minio.secret_key().to_string().into()),
    );
    let region = Region::custom("us-east-1", minio.endpoint().to_string());
    S3::new(&credentials, &region, Some(bucket.to_string()), false)
}

#[tokio::test]
async fn test_object_lock_via_public_api() {
    let minio = MinioContext::get_or_start().await;
    let bucket = "api-objlock";
    let s3 = client(&minio, bucket);

    // 1. Enable Object Lock at bucket creation (also enables versioning).
    CreateBucket::new("private", true)
        .request(&s3)
        .await
        .expect("create object-lock bucket");

    // 2. Upload an object carrying retention via the PutObject action. The
    //    same ObjectLock can also ride on RequestOptions::object_lock when going
    //    through the streaming engine.
    let file = create_test_file_with_content(1024, "API_OL_");
    let object_lock = ObjectLock {
        retention: Some((
            ObjectLockMode::Governance,
            "2099-01-01T00:00:00Z".to_string(),
        )),
        legal_hold: None,
    };
    PutObject::new(
        "locked.dat",
        file.path(),
        None,
        None,
        None,
        None,
        Some(object_lock),
    )
    .request(&s3, RequestOptions::new())
    .await
    .expect("put object with retention");

    // 3. Read the per-object retention back.
    let retention = GetObjectRetention::new("locked.dat", None)
        .request(&s3)
        .await
        .expect("get object retention");
    assert_eq!(retention.mode.as_deref(), Some("GOVERNANCE"));

    // 3b. Set retention on a second object explicitly via PutObjectRetention.
    let file2 = create_test_file_with_content(512, "API_OL2_");
    PutObject::new("explicit.dat", file2.path(), None, None, None, None, None)
        .request(&s3, RequestOptions::new())
        .await
        .expect("put plain object");
    PutObjectRetention::new(
        "explicit.dat",
        ObjectLockMode::Governance,
        "2099-01-01T00:00:00Z".to_string(),
        None,
        false,
    )
    .request(&s3)
    .await
    .expect("put object retention");
    let explicit = GetObjectRetention::new("explicit.dat", None)
        .request(&s3)
        .await
        .expect("get explicit retention");
    assert_eq!(explicit.mode.as_deref(), Some("GOVERNANCE"));

    // 4. Bucket-level default retention round-trip.
    PutObjectLockConfiguration::new(ObjectLockMode::Governance, Some(1), None)
        .request(&s3)
        .await
        .expect("put bucket object-lock configuration");
    let config = GetObjectLockConfiguration::new()
        .request(&s3)
        .await
        .expect("get bucket object-lock configuration");
    let default_retention = config
        .rule
        .expect("rule")
        .default_retention
        .expect("default retention");
    assert_eq!(default_retention.mode.as_deref(), Some("GOVERNANCE"));
    assert_eq!(default_retention.days, Some(1));

    // 5. Per-object legal hold round-trip.
    PutObjectLegalHold::new("locked.dat", true, None)
        .request(&s3)
        .await
        .expect("legal hold on");
    let hold = GetObjectLegalHold::new("locked.dat", None)
        .request(&s3)
        .await
        .expect("get legal hold");
    assert_eq!(hold.status.as_deref(), Some("ON"));

    PutObjectLegalHold::new("locked.dat", false, None)
        .request(&s3)
        .await
        .expect("legal hold off");
}
