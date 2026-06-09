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

## Usage

```rust
use s3m_core::{S3, Credentials, Region, RequestOptions};
use s3m_core::s3::actions::GetObject;
use secrecy::SecretString;

# async fn run() -> anyhow::Result<()> {
let credentials = Credentials::new("ACCESS_KEY", &SecretString::new("SECRET_KEY".into()));
let region: Region = "us-west-2".parse()?;
let s3 = S3::new(&credentials, &region, Some("my-bucket".to_string()), false);

// Download an object; `request` returns a typed `s3_core::Error` on failure.
let response = GetObject::new("path/file.dat", None)
    .request(&s3, &RequestOptions::new())
    .await?;
# Ok(())
# }
```

Errors are matchable — e.g. distinguish a missing object from an auth failure:

```rust
# use s3m_core::Error;
# fn handle(err: Error) {
if err.is_not_found() {
    // object/key does not exist (NoSuchKey / 404)
}
match err.code() {
    Some("AccessDenied") => { /* credentials/permissions */ }
    _ => {}
}
# }
```

## License

BSD-3-Clause. See the [main repository](https://github.com/s3m/s3m) for the full
project, the `s3m` CLI, and documentation.
