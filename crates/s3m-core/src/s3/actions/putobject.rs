use crate::s3::error::{Error, Result};
use crate::s3::{
    S3,
    actions::{Action, response_error, upload_response_headers},
    checksum::{
        Checksum, ChecksumAlgorithm, hasher::ChecksumHasher, sha256_md5_digest,
        sha256_md5_digest_with,
    },
    object_lock::ObjectLock,
    options::RequestOptions,
    request::{self, source_changed},
};
use base64ct::{Base64, Encoding};
use bytes::Bytes;
use reqwest::Method;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;

#[derive(Debug)]
pub struct PutObject<'a> {
    key: &'a str,
    file: &'a Path,
    acl: Option<String>,
    meta: Option<BTreeMap<String, String>>,
    sender: Option<UnboundedSender<usize>>,
    additional_checksum: Option<Checksum>,
    object_lock: Option<ObjectLock>,
}

impl<'a> PutObject<'a> {
    #[must_use]
    pub const fn new(
        key: &'a str,
        file: &'a Path,
        acl: Option<String>,
        meta: Option<BTreeMap<String, String>>,
        sender: Option<UnboundedSender<usize>>,
        additional_checksum: Option<Checksum>,
        object_lock: Option<ObjectLock>,
    ) -> Self {
        Self {
            key,
            file,
            acl,
            meta,
            sender,
            additional_checksum,
            object_lock,
        }
    }

    /// # Errors
    ///
    /// Will return `Err` if can not make the request
    pub async fn request(self, s3: &S3, globals: RequestOptions) -> Result<BTreeMap<&str, String>> {
        self.send(s3, globals, None)
            .await
            .map(|(headers, _)| headers)
    }

    pub(crate) async fn request_checked(
        self,
        s3: &S3,
        globals: RequestOptions,
        file_size: u64,
        file_blake3: &str,
    ) -> Result<(BTreeMap<&'static str, String>, Option<Checksum>)> {
        self.send(s3, globals, Some((file_size, file_blake3))).await
    }

    async fn digests(
        &mut self,
        size: u64,
        expected_blake3: &str,
    ) -> Result<(Bytes, Bytes, usize, blake3::Hash)> {
        let mut blake3 = blake3::Hasher::new();
        let checksum = self.additional_checksum.as_mut();
        let mut extra = checksum
            .as_deref()
            .filter(|checksum| checksum.algorithm != ChecksumAlgorithm::Sha256)
            .map(Checksum::hasher);
        let (sha, md5, length) = sha256_md5_digest_with(self.file, |bytes| {
            blake3.update(bytes);
            if let Some(hasher) = &mut extra {
                hasher.update(bytes);
            }
        })
        .await?;
        if u64::try_from(length).ok() != Some(size) {
            return Err(source_changed(&format!(
                "size changed from {size} to {length} bytes"
            )));
        }
        let digest = blake3.finalize();
        if digest.to_hex().as_str() != expected_blake3 {
            return Err(source_changed("BLAKE3 checksum changed since it was read"));
        }
        // PutObject's x-amz-checksum-* describes the same payload as the signing digests.
        // <https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html>
        if let Some(checksum) = checksum {
            let bytes = extra.map_or_else(|| sha.clone(), ChecksumHasher::finalize);
            checksum.checksum = Base64::encode_string(&bytes);
        }
        Ok((sha, md5, length, digest))
    }

    async fn send(
        mut self,
        s3: &S3,
        globals: RequestOptions,
        source: Option<(u64, &str)>,
    ) -> Result<(BTreeMap<&'static str, String>, Option<Checksum>)> {
        let (sha, md5, length, expected_blake3) = if let Some((size, digest)) = source {
            let (sha, md5, length, digest) = self.digests(size, digest).await?;
            (sha, md5, length, Some(digest))
        } else {
            let (sha, md5, length) = sha256_md5_digest(self.file).await?;
            (sha, md5, length, None)
        };

        let (url, headers) = &self.sign(s3, sha.as_ref(), Some(md5.as_ref()), Some(length))?;
        #[cfg(test)]
        tests::before_body_open();
        let progress = self.sender.take().map(|sender| {
            let callback: request::ProgressCallback = Arc::new(move |bytes_count| {
                if sender.send(bytes_count).is_err() {
                    log::trace!("Progress receiver dropped");
                }
            });
            callback
        });

        let response = request::request_with_file(
            s3.client(),
            url.clone(),
            self.http_method()?,
            headers,
            Some((self.file, expected_blake3)),
            progress,
            globals.throttle,
        )
        .await?;

        if response.status().is_success() {
            let mut h: BTreeMap<&str, String> = upload_response_headers(response.headers());
            if let Some(etag) = response.headers().get("ETag") {
                h.insert("ETag", etag.to_str()?.to_string());
            }
            Ok((h, self.additional_checksum))
        } else {
            let error = response_error(response).await;
            if let Some((size, digest)) = source
                && matches!(
                    error.code(),
                    Some("BadDigest" | "XAmzContentSHA256Mismatch")
                )
            {
                // The body is read after signing. Check if a rewrite caused the rejection.
                match self.digests(size, digest).await {
                    Err(Error::Io(error)) => {
                        return Err(source_changed(&format!(
                            "could not verify the file: {error}"
                        )));
                    }
                    result => {
                        result?;
                    }
                }
            }
            Err(error)
        }
    }
}

// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html>
impl Action for PutObject<'_> {
    fn http_method(&self) -> Result<Method> {
        Ok(Method::from_bytes(b"PUT")?)
    }

    fn headers(&self) -> Option<BTreeMap<&str, &str>> {
        let mut map: BTreeMap<&str, &str> = BTreeMap::new();

        // https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectAcl.html
        if let Some(acl) = &self.acl {
            map.insert("x-amz-acl", acl);
        }

        if let Some(meta) = &self.meta {
            for (k, v) in meta {
                map.insert(k, v);
            }
        }

        // <https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html>
        if let Some(additional_checksum) = &self.additional_checksum {
            map.insert(
                additional_checksum.algorithm.as_amz(),
                &additional_checksum.checksum,
            );
        }

        // <https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html>
        if let Some(object_lock) = &self.object_lock {
            object_lock.apply(&mut map);
            // The PutObject reference requires Content-MD5 or
            // x-amz-sdk-checksum-algorithm for an upload with Object Lock
            // parameters. The additional checksum replaces Content-MD5 (#73), so
            // name its algorithm the way the AWS SDKs do.
            if let Some(additional_checksum) = &self.additional_checksum {
                map.insert(
                    "x-amz-sdk-checksum-algorithm",
                    additional_checksum.algorithm.as_algorithm(),
                );
            }
        }

        Some(map)
    }

    fn query_pairs(&self) -> Option<BTreeMap<&str, &str>> {
        None
    }

    fn path(&self) -> Option<Vec<&str>> {
        Some(self.key.split('/').collect())
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::unnecessary_wraps
)]
mod tests {
    use super::*;
    use crate::{
        s3::{Credentials, Region, tools},
        stream::{
            db::Db,
            upload_default::{UploadRequest, upload},
        },
    };
    use base64ct::{Base64, Encoding};
    use mockito::Matcher;
    use secrecy::SecretString;
    use std::{
        cell::Cell,
        fs, io,
        path::PathBuf,
        time::{Duration, UNIX_EPOCH},
    };

    thread_local! {
        static ON_BODY_OPEN: Cell<Option<Box<dyn FnOnce()>>> = const { Cell::new(None) };
    }

    pub(super) fn before_body_open() {
        if let Some(hook) = ON_BODY_OPEN.take() {
            hook();
        }
    }

    fn on_next_body_open(hook: impl FnOnce() + 'static) {
        ON_BODY_OPEN.set(Some(Box::new(hook)));
    }

    #[tokio::test]
    async fn put_object_checks_the_source_after_a_server_digest_rejection() {
        for (code, checked, rewrite) in [
            ("BadDigest", false, true),
            ("BadDigest", true, false),
            ("XAmzContentSHA256Mismatch", true, false),
        ] {
            assert_digest_rejection(code, checked, rewrite).await;
        }
    }

    async fn assert_digest_rejection(code: &str, checked: bool, rewrite: bool) {
        let mut server = mockito::Server::new_async().await;
        let s3 = loopback_s3(server.url());
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("body");
        fs::write(&file, "original").unwrap();
        let reject = server
            .mock("PUT", "/bucket/key")
            .match_header(
                "content-md5",
                Base64::encode_string(&md5::compute(b"original").0).as_str(),
            )
            .match_body(if rewrite { "changed!" } else { "original" })
            .with_status(400)
            .with_body(format!("<Error><Code>{code}</Code></Error>"))
            .create_async()
            .await;

        if rewrite {
            let path = file.clone();
            on_next_body_open(move || fs::write(path, "changed!").unwrap());
        }
        let action = PutObject::new("key", &file, None, None, None, None, None);
        let options = RequestOptions::default();
        let result = if checked {
            let digest = blake3::hash(b"original").to_hex();
            action
                .request_checked(&s3, options, 8, digest.as_str())
                .await
                .map(|(headers, _)| headers)
        } else {
            action.request(&s3, options).await
        };

        reject.assert_async().await;
        let error = result.unwrap_err();
        assert_eq!(error.code(), Some(code));
    }

    fn loopback_s3(endpoint: String) -> S3 {
        loopback_s3_with_signing(endpoint, false)
    }

    fn loopback_s3_with_signing(endpoint: String, no_sign_request: bool) -> S3 {
        S3::new(
            &Credentials::new("test", &SecretString::new("test".into())),
            &Region::custom("test", endpoint),
            Some("bucket".into()),
            no_sign_request,
        )
    }

    fn sha256_of(bytes: &[u8]) -> Checksum {
        let mut checksum = Checksum::new(ChecksumAlgorithm::Sha256);
        checksum.digest(bytes).unwrap();
        checksum
    }

    fn unsigned_upload<'a>(
        s3: &'a S3,
        file: &'a Path,
        db: &'a Db,
        blake3: &'a str,
        checksum: Checksum,
    ) -> UploadRequest<'a> {
        UploadRequest {
            s3,
            key: "key",
            file,
            file_size: 8,
            file_mtime: None,
            file_blake3: blake3,
            sdb: db,
            acl: None,
            meta: None,
            quiet: true,
            additional_checksum: Some(checksum),
            globals: RequestOptions {
                retries: 2,
                ..RequestOptions::default()
            },
        }
    }

    #[tokio::test]
    async fn unsigned_put_derives_its_checksum_from_the_verified_read_on_every_attempt() {
        let mut server = mockito::Server::new_async().await;
        let s3 = loopback_s3_with_signing(server.url(), true);
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("body");
        fs::write(&file, "original").unwrap();
        let blake3 = blake3::hash(b"original").to_hex();
        let db = Db::new(&s3, "key", blake3.as_str(), 0, dir.path()).unwrap();
        let expected = sha256_of(b"original");
        let response = |server: &mut mockito::Server, status| {
            server
                .mock("PUT", "/bucket/key")
                .match_header("authorization", Matcher::Missing)
                .match_header("content-md5", Matcher::Missing)
                .match_header("x-amz-content-sha256", Matcher::Missing)
                .match_header("x-amz-checksum-sha256", expected.checksum.as_str())
                .match_body("original")
                .with_status(status)
                .with_header("ETag", "\"original\"")
                .expect(1)
        };
        let throttled = response(&mut server, 503).create_async().await;
        let stored = response(&mut server, 200).create_async().await;

        let request = unsigned_upload(&s3, &file, &db, blake3.as_str(), sha256_of(b"changed!"));
        let report = upload(request)
            .await
            .expect("both attempts must send SHA-256 from the BLAKE3-verified read");
        assert!(report.contains(&expected.report()), "{report}");
        assert_eq!(db.cached_etag().unwrap().as_deref(), Some("\"original\""));
        throttled.assert_async().await;
        stored.assert_async().await;
    }

    #[tokio::test]
    async fn unsigned_put_with_checksum_rejects_a_flip_after_signing_without_caching() {
        assert_unsigned_flip_is_rejected(true).await;
    }

    #[tokio::test]
    async fn unsigned_put_rejects_a_flip_even_when_the_peer_ignores_checksum_headers() {
        assert_unsigned_flip_is_rejected(false).await;
    }

    async fn assert_unsigned_flip_is_rejected(validates_checksum: bool) {
        let mut server = mockito::Server::new_async().await;
        let s3 = loopback_s3_with_signing(server.url(), true);
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("body");
        let blake3 = blake3::hash(b"original").to_hex();
        let db = Db::new(&s3, "key", blake3.as_str(), 0, dir.path()).unwrap();
        let mtime = UNIX_EPOCH + Duration::from_secs(10);

        // Reproduce the old extra-checksum read seeing different bytes from BLAKE3.
        fs::write(&file, "changed!").unwrap();
        let mut checksum = Checksum::new(ChecksumAlgorithm::Sha256);
        checksum.calculate(&file).await.unwrap();
        let match_checksum = if validates_checksum {
            Matcher::Exact(checksum.checksum.clone())
        } else {
            Matcher::Any
        };
        let otherwise_accepted = server
            .mock("PUT", "/bucket/key")
            .match_header("authorization", Matcher::Missing)
            .match_header("x-amz-checksum-sha256", match_checksum)
            .match_body("changed!")
            .with_status(200)
            .with_header("ETag", "\"changed\"")
            .expect(0)
            .create_async()
            .await;
        let bad_digest = if validates_checksum {
            Some(
                server
                    .mock("PUT", "/bucket/key")
                    .match_header(
                        "x-amz-checksum-sha256",
                        sha256_of(b"original").checksum.as_str(),
                    )
                    .match_body("changed!")
                    .with_status(400)
                    .with_body("<Error><Code>BadDigest</Code></Error>")
                    .expect(0)
                    .create_async()
                    .await,
            )
        } else {
            None
        };

        fs::write(&file, "original").unwrap();
        fs::OpenOptions::new()
            .write(true)
            .open(&file)
            .unwrap()
            .set_modified(mtime)
            .unwrap();
        let path = file.clone();
        on_next_body_open(move || {
            fs::write(&path, "changed!").unwrap();
            fs::OpenOptions::new()
                .write(true)
                .open(path)
                .unwrap()
                .set_modified(mtime)
                .unwrap();
        });

        let request = unsigned_upload(&s3, &file, &db, blake3.as_str(), checksum);
        let error = upload(request)
            .await
            .expect_err("an unsigned PUT must fail when the body flips after signing");
        assert!(
            error.to_string().contains(
                "the file changed during the upload: BLAKE3 checksum changed since it was read"
            ),
            "{error}"
        );
        assert!(db.cached_etag().unwrap().is_none());
        otherwise_accepted.assert_async().await;
        if let Some(bad_digest) = bad_digest {
            bad_digest.assert_async().await;
        }
    }

    #[tokio::test]
    async fn a_short_body_is_a_local_read_error_and_is_never_retried() {
        assert_local_read_failure(io::ErrorKind::UnexpectedEof, |path| {
            fs::write(path, "part").unwrap();
        })
        .await;
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn a_local_body_read_error_is_never_retried() {
        assert_local_read_failure(io::ErrorKind::IsADirectory, |path| {
            fs::remove_file(&path).unwrap();
            fs::create_dir(path).unwrap();
        })
        .await;
    }

    async fn assert_local_read_failure(
        expected: io::ErrorKind,
        change_after_signing: impl FnOnce(PathBuf) + 'static,
    ) {
        let mut server = mockito::Server::new_async().await;
        let s3 = loopback_s3(server.url());
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("body");
        fs::write(&file, "original").unwrap();
        let file_blake3 = blake3::hash(b"original").to_hex();
        let db = Db::new(&s3, "key", file_blake3.as_str(), 0, dir.path()).unwrap();
        let unexpected = server
            .mock("PUT", "/bucket/key")
            .with_header("etag", "\"wrong\"")
            .expect(0)
            .create_async()
            .await;

        let path = file.clone();
        on_next_body_open(move || change_after_signing(path));
        let request = UploadRequest {
            s3: &s3,
            key: "key",
            file: &file,
            file_size: 8,
            file_mtime: None,
            file_blake3: file_blake3.as_str(),
            sdb: &db,
            acl: None,
            meta: None,
            quiet: true,
            additional_checksum: None,
            globals: RequestOptions {
                retries: 2,
                ..RequestOptions::default()
            },
        };
        let error = tokio::time::timeout(Duration::from_secs(10), upload(request))
            .await
            .unwrap()
            .unwrap_err();
        assert!(
            matches!(error.downcast_ref::<Error>(), Some(Error::Io(error))
                if error.kind() == expected),
            "{error:?}"
        );
        assert!(db.cached_etag().unwrap().is_none());
        unexpected.assert_async().await;
    }

    #[test]
    fn test_method() {
        let action = PutObject::new("key", Path::new("/"), None, None, None, None, None);
        assert_eq!(Method::PUT, action.http_method().unwrap());
    }

    #[test]
    fn test_headers_acl() {
        let test = vec![
            ("private", "private"),
            ("public-read", "public-read"),
            ("public-read-write", "public-read-write"),
            ("authenticated-read", "authenticated-read"),
            ("aws-exec-read", "aws-exec-read"),
            ("bucket-owner-read", "bucket-owner-read"),
            ("bucket-owner-full-control", "bucket-owner-full-control"),
        ];

        let s3 = S3::new(
            &Credentials::new(
                "AKIAIOSFODNN7EXAMPLE",
                &SecretString::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into()),
            ),
            &"us-west-1".parse::<Region>().unwrap(),
            Some("awsexamplebucket1".to_string()),
            false,
        );

        for (acl, expected) in test {
            let action = PutObject::new(
                "key",
                Path::new("/"),
                Some(acl.to_string()),
                None,
                None,
                None,
                None,
            );
            let (_, headers) = action
                .sign(&s3, tools::sha256_digest("").as_ref(), None, None)
                .unwrap();
            assert_eq!(expected, headers.get("x-amz-acl").unwrap());
        }
    }

    #[test]
    fn test_headers_object_lock() {
        use crate::s3::object_lock::{ObjectLock, ObjectLockMode};

        let object_lock = ObjectLock {
            retention: Some((
                ObjectLockMode::Governance,
                "2027-01-01T00:00:00Z".to_string(),
            )),
            legal_hold: Some(true),
        };
        let action = PutObject::new(
            "key",
            Path::new("/"),
            None,
            None,
            None,
            None,
            Some(object_lock),
        );
        let headers = action.headers().unwrap();
        assert_eq!(headers.get("x-amz-object-lock-mode"), Some(&"GOVERNANCE"));
        assert_eq!(
            headers.get("x-amz-object-lock-retain-until-date"),
            Some(&"2027-01-01T00:00:00Z")
        );
        assert_eq!(headers.get("x-amz-object-lock-legal-hold"), Some(&"ON"));
    }

    // #73: with --checksum there is no Content-MD5, which the PutObject reference asks
    // for when the upload carries Object Lock parameters, unless the request names the
    // checksum algorithm. Without Object Lock the header is not sent.
    #[test]
    fn test_headers_object_lock_with_checksum_names_the_algorithm() {
        use crate::s3::{
            checksum::ChecksumAlgorithm,
            object_lock::{ObjectLock, ObjectLockMode},
        };

        let checksum = || {
            Some(Checksum {
                algorithm: ChecksumAlgorithm::Sha256,
                checksum: "checksum-value".to_string(),
            })
        };
        let object_lock = ObjectLock {
            retention: Some((
                ObjectLockMode::Governance,
                "2027-01-01T00:00:00Z".to_string(),
            )),
            legal_hold: None,
        };

        let action = PutObject::new(
            "key",
            Path::new("/"),
            None,
            None,
            None,
            checksum(),
            Some(object_lock.clone()),
        );
        let headers = action.headers().unwrap();
        assert_eq!(
            headers.get("x-amz-checksum-sha256"),
            Some(&"checksum-value")
        );
        assert_eq!(headers.get("x-amz-sdk-checksum-algorithm"), Some(&"SHA256"));

        let action = PutObject::new("key", Path::new("/"), None, None, None, checksum(), None);
        let headers = action.headers().unwrap();
        assert_eq!(headers.get("x-amz-sdk-checksum-algorithm"), None);

        let action = PutObject::new(
            "key",
            Path::new("/"),
            None,
            None,
            None,
            None,
            Some(object_lock),
        );
        let headers = action.headers().unwrap();
        assert_eq!(headers.get("x-amz-sdk-checksum-algorithm"), None);
    }

    fn test_s3() -> S3 {
        S3::new(
            &Credentials::new(
                "AKIAIOSFODNN7EXAMPLE",
                &SecretString::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into()),
            ),
            &"us-west-1".parse::<Region>().unwrap(),
            Some("awsexamplebucket1".to_string()),
            false,
        )
    }

    #[test]
    fn test_sign_sends_content_md5_without_additional_checksum() {
        let action = PutObject::new("key", Path::new("/"), None, None, None, None, None);
        let (_, headers) = action
            .sign(
                &test_s3(),
                tools::sha256_digest("").as_ref(),
                Some(b"0123456789abcdef"),
                None,
            )
            .unwrap();
        assert_eq!(
            Some(&"MDEyMzQ1Njc4OWFiY2RlZg==".to_string()),
            headers.get("content-md5")
        );
        assert!(
            headers
                .get("authorization")
                .unwrap()
                .contains("content-md5;")
        );
    }

    // --no-sign-request sends no authorization, but the request still needs its
    // Content-Length (S3 rejects a chunked upload), its integrity header and the
    // action's own headers.
    #[test]
    fn test_unsigned_request_keeps_body_and_action_headers() {
        let s3 = S3::new(
            &Credentials::new("", &SecretString::new("".into())),
            &"us-west-1".parse::<Region>().unwrap(),
            Some("awsexamplebucket1".to_string()),
            true,
        );
        // Names that differ only in case are one header, as when signed; the map's
        // order decides which value is kept.
        let meta = BTreeMap::from([
            ("x-amz-meta-Owner".to_string(), "other".to_string()),
            ("x-amz-meta-owner".to_string(), " me ".to_string()),
        ]);
        let action = PutObject::new(
            "key",
            Path::new("/"),
            Some("public-read".to_string()),
            Some(meta),
            None,
            None,
            None,
        );
        let (_, headers) = action
            .sign(
                &s3,
                tools::sha256_digest("").as_ref(),
                Some(b"0123456789abcdef"),
                Some(11),
            )
            .unwrap();
        assert_eq!(
            headers,
            BTreeMap::from([
                ("content-length".to_string(), "11".to_string()),
                (
                    "content-md5".to_string(),
                    "MDEyMzQ1Njc4OWFiY2RlZg==".to_string()
                ),
                ("x-amz-acl".to_string(), "public-read".to_string()),
                ("x-amz-meta-owner".to_string(), "me".to_string()),
            ])
        );
    }

    // <https://github.com/s3m/s3m/issues/73>: Cloudflare R2 rejects Content-MD5
    // together with an x-amz-checksum-* header.
    #[test]
    fn test_sign_omits_content_md5_with_additional_checksum() {
        use crate::s3::checksum::ChecksumAlgorithm;

        let checksum = Checksum {
            algorithm: ChecksumAlgorithm::Sha256,
            checksum: "uU0nuZNNPgilLlLX2n2r+sSE7+N6U4DukIj3rOLvzek=".to_string(),
        };
        let action = PutObject::new(
            "key",
            Path::new("/"),
            None,
            None,
            None,
            Some(checksum),
            None,
        );
        let (_, headers) = action
            .sign(
                &test_s3(),
                tools::sha256_digest("").as_ref(),
                Some(b"0123456789abcdef"),
                None,
            )
            .unwrap();
        assert_eq!(None, headers.get("content-md5"));
        assert_eq!(
            Some(&"uU0nuZNNPgilLlLX2n2r+sSE7+N6U4DukIj3rOLvzek=".to_string()),
            headers.get("x-amz-checksum-sha256")
        );
        let authorization = headers.get("authorization").unwrap();
        assert!(!authorization.contains("content-md5"));
        assert!(authorization.contains("x-amz-checksum-sha256"));
    }

    #[test]
    fn test_sign() {
        let s3 = S3::new(
            &Credentials::new(
                "AKIAIOSFODNN7EXAMPLE",
                &SecretString::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into()),
            ),
            &"us-west-1".parse::<Region>().unwrap(),
            Some("awsexamplebucket1".to_string()),
            false,
        );

        let action = PutObject::new("key", Path::new("/"), None, None, None, None, None);
        let (url, headers) = action
            .sign(&s3, tools::sha256_digest("").as_ref(), None, None)
            .unwrap();
        assert_eq!(
            "https://s3.us-west-1.amazonaws.com/awsexamplebucket1/key",
            url.as_str()
        );
        assert!(
            headers
                .get("authorization")
                .unwrap()
                .starts_with("AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE")
        );
    }
}
