# s3m-core

[![crates.io](https://img.shields.io/crates/v/s3m-core.svg)](https://crates.io/crates/s3m-core)

The reusable S3 client and streaming engine behind the
[`s3m`](https://crates.io/crates/s3m) CLI ([s3m.stream](https://s3m.stream)).

`s3m-core` is free of any command-line concerns, so other applications can embed
it to talk to S3-compatible object storage directly. It provides:

- **`s3`** — the S3 client (`S3`), `Credentials`, `Region`, request signing
  (AWS SigV4), the S3 actions (put/get/list/delete/multipart/…), and a typed
  request [`Error`] you can match on programmatically.
- **`stream`** — resumable multipart uploads plus the compression (Zstandard)
  and encryption (ChaCha20-Poly1305) transfer pipelines.

## Installation

```sh
cargo add s3m-core
```

The example below also uses `tokio`, `secrecy`, and `anyhow` directly, so add
those to your own crate as well:

```sh
cargo add tokio --features macros,rt-multi-thread
cargo add secrecy
cargo add anyhow
```

(`anyhow` is only used here for the example's `main` return type — `s3m-core`
itself exposes the typed [`Error`] and does not require it.)

## Usage

```rust
use s3m_core::{Credentials, Region, RequestOptions, S3};
use s3m_core::s3::actions::GetObject;
use secrecy::SecretString;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let credentials = Credentials::new("ACCESS_KEY", &SecretString::new("SECRET_KEY".into()));
    let region: Region = "us-west-2".parse()?;
    let s3 = S3::new(&credentials, &region, Some("my-bucket".to_string()), false);

    // Download an object; `request` returns a typed `s3m_core::Error` on failure.
    let response = GetObject::new("path/file.dat", None)
        .request(&s3, &RequestOptions::new())
        .await?;
    // ... stream `response.bytes_stream()` to disk ...
    Ok(())
}
```

Errors are matchable — e.g. distinguish a missing object from an auth failure:

```rust
use s3m_core::Error;

fn handle(err: Error) {
    if err.is_not_found() {
        // object/key does not exist (NoSuchKey / 404)
    }
    match err.code() {
        Some("AccessDenied") => { /* credentials / permissions */ }
        _ => {}
    }
}
```

### Object Lock (WORM)

[`ObjectLock`] carries per-upload retention and legal-hold settings; pass it via
[`RequestOptions::object_lock`] and it is emitted as the `x-amz-object-lock-*`
headers on `PutObject` / `CreateMultipartUpload`. Enable Object Lock on the
bucket at creation with `CreateBucket::new(acl, /* object_lock_enabled */ true)`.

```rust
use s3m_core::{ObjectLock, ObjectLockMode, RequestOptions};

let mut opts = RequestOptions::new();
opts.object_lock = Some(ObjectLock {
    retention: Some((ObjectLockMode::Compliance, "2027-01-01T00:00:00Z".to_string())),
    legal_hold: None,
});
```

To manage Object Lock after upload, the crate exposes `Get`/`PutObjectLockConfiguration`
(bucket default retention), `Get`/`PutObjectRetention`, and `Get`/`PutObjectLegalHold`
under `s3m_core::s3::actions`, returning `ObjectLockConfiguration` / `ObjectRetention`
/ `ObjectLegalHold` from `s3m_core::s3::responses`.

## License

BSD-3-Clause. See the [main repository](https://github.com/s3m/s3m) for the full
project, the `s3m` CLI, and documentation.
