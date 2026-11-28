# s3m

[![Release](https://github.com/s3m/s3m/actions/workflows/release.yml/badge.svg?branch=develop)](https://github.com/s3m/s3m/actions/workflows/release.yml?query=branch%3Adevelop)
[![Deploy](https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fapi.github.com%2Frepos%2Fs3m%2Fs3m%2Freleases%2Flatest&query=%24.tag_name&label=ghcr.io%2Fs3m%2Fs3m&logo=docker&logoColor=white&color=blue)](https://github.com/s3m/s3m/pkgs/container/s3m)
[![Test & Build](https://github.com/s3m/s3m/actions/workflows/build.yml/badge.svg)](https://github.com/s3m/s3m/actions/workflows/build.yml)
[![codecov](https://codecov.io/gh/s3m/s3m/graph/badge.svg?token=Y2BQJUGPJ5)](https://codecov.io/gh/s3m/s3m)
[![crates.io](https://img.shields.io/crates/v/s3m.svg)](https://crates.io/crates/s3m)
[![s3m-core](https://img.shields.io/crates/v/s3m-core.svg?label=s3m-core)](https://crates.io/crates/s3m-core)

**s3m** a command-line tool for storing streams of data in s3 buckets.

It is built on the reusable [`s3m-core`](https://crates.io/crates/s3m-core)
library — the same S3 client and streaming engine, free of CLI dependencies, so
you can embed it in your own application (see [Use as a library](#use-as-a-library)).

## Problem trying to solve

There are streams of data that can not be lost besides that when created,
they degrade the performance of running systems, for example, if the stream
is a backup of a database, every time the stream is produced it may lock the
entire cluster (depends on the options and tools used mysqldump/xtrabackup for
example), however, the effort to create the stream is proportional to the size
of the database, in most cases the bigger the database is the more time
and CPU and Memory are required.

In the case of backups, normally the streams are piped to a compression tool
and later put in the disk, in some cases writing to the existing disk where
the database is or to a remote mount endpoint, is not possible due to size
constraints and the compressed backup should be streamed to am s3 bucket (X
provider), therefore if for some reason the connection gets lost while streaming
almost before finishing, the whole backup procedure could be corrupted and in
worst scenario everything should start all over again.

The aim of **s3m** apart from trying to consume fewer resources is to make as
much as possible "fault-tolerant" the storage procedure of the incoming stream
so that even if the server lost network connectivity, the stream can be resumed
and continue the upload process where it was left without the need to start all
over again (no when input is from STDIN/pipe).

https://s3m.stream

## System Requirements

**s3m** is designed to run in very constrained environments with minimal resource usage:

- **Disk Space**: 512MB when streaming from STDIN (for buffering multipart uploads)
- **Memory**: Minimal - uses streaming to avoid loading data into RAM
- **Network**: Internet connection for S3 uploads (supports resumable uploads on disconnection)

## Configuration

Create `~/.config/s3m/config.yml`, or pass another file with `--config`:

```yaml
hosts:
  s3:
    endpoint: s3.us-west-2.amazonaws.com
    access_key: YOUR_ACCESS_KEY
    secret_key: YOUR_SECRET_KEY
```

The bucket is always part of the path (`s3/my-bucket/file`); a host has no default bucket.

### Multiple Providers

```yaml
hosts:
  aws:
    region: us-west-1
    access_key: AWS_KEY
    secret_key: AWS_SECRET

  backblaze:
    endpoint: s3.us-west-000.backblazeb2.com
    access_key: B2_KEY
    secret_key: B2_SECRET
```

### Where s3m keeps its files

s3m reads `config.yml` and keeps the local state of uploads (`streams/`, used to
resume multipart uploads) in `~/.config/s3m`. Set `S3M_HOME` to use another
directory, for example a writable volume in a container:

```bash
S3M_HOME=/var/lib/s3m s3m --config /etc/s3m/config.yml backup.tar s3/backups/backup.tar
```

The directory is created only when an upload writes to it. Commands that keep no
local state (`ls`, `get`, `rm`, `show`, ...) also work with a read-only home.

## Usage

s3m uses the format: `/host/bucket/file`

### Upload a file

```bash
# Upload file
s3m file.dat /s3/my-bucket/

# Upload with different name
s3m local-file.dat /s3/my-bucket/remote-name.dat

# Stream from STDIN
mariadb-dump database | s3m --pipe /s3/backups/db-backup.sql
```

### Download a file

```bash
s3m get /s3/my-bucket/file.dat
```

### List buckets and objects

```bash
# List all buckets
s3m ls s3

# List objects in bucket
s3m ls s3/my-bucket

# List with prefix
s3m ls s3/my-bucket/path/

# List only the first 100 objects
s3m ls s3/my-bucket --number 100

# List in-progress multipart uploads under a key prefix
s3m ls s3/my-bucket --multipart --prefix backups/
```

Listing follows every page of results for objects, buckets and multipart
uploads, so a single `ls` returns the complete set unless you limit it.

* `--number/-n N` is a **total** limit enforced by s3m, not a page size. It
  counts what is printed, and with `--older-than` only the objects that pass the
  age filter count toward `N`. `--number 0` lists nothing and sends no list
  request.
* `--multipart` honours `--prefix`. `--start-after` maps to the key marker only,
  so it skips keys up to that key but never selects an upload id.
* A host without a bucket (for example `s3m ls s3`) lists buckets and rejects
  `--multipart`, `--prefix`, `--start-after` and `--older-than` before sending
  any request.
* The `--json` output schema is unchanged: no keys were added or removed.
* `rm --older-than` is not subject to this limit and still considers every
  matching object.

### Usage summary

```bash
# Bucket summary
s3m du s3/my-bucket

# Prefix summary grouped by UTC day
s3m du s3/my-bucket/backups/ --group-by day
```

### Stream state

```bash
# List resumable multipart state
s3m streams

# Show one entry
s3m streams show <id>

# Clean broken/completed entries
s3m streams clean
```

### Delete multiple objects

```bash
# One object uses DeleteObject
s3m rm s3/my-bucket/file.dat

# Multiple objects use DeleteObjects grouped by bucket
s3m rm s3/my-bucket/a.txt s3/my-bucket/b.txt
s3m rm s3/bucket-a/a.txt s3/bucket-b/b.txt
```

### Delete all objects under a prefix

```bash
# Delete one exact object (a key ending in "/" is deleted as that exact key)
s3m rm s3/my-bucket/path/file.txt

# Delete every object whose key starts with the literal prefix
s3m rm s3/my-bucket/integration/18/ --recursive
```

* The key is a **literal S3 key prefix**, not a directory: `integration/18/` matches `integration/18/a`, `integration/18/x/b` and a key equal to `integration/18/`, but not `integration/18`, `integration/180/a` or `integration/18-other/a`. Keep the trailing slash; without it `integration/18` would also match `integration/180/...`, and s3m prints a warning on stderr (the delete still runs). No globbing or path normalization is applied.
* Without `--recursive`, the target is deleted as one exact key (unchanged). With `--recursive` and no `--bucket`, the bucket is never deleted.
* The prefix must not be empty (`s3/my-bucket --recursive` is rejected; use `-b --recursive` to delete a bucket). `--bucket` cannot be combined with an object key or prefix. `--recursive` takes exactly one target and cannot be combined with `--older-than`, `--version-id` or `--abort`.
* A prefix with no matching objects is a successful no-op.
* Objects are listed and deleted page by page (`DeleteObjects`, at most 1,000 keys per request). Any listing error, request error or per-object error (for example Object Lock) stops the run with a non-zero exit status and reports how many objects were already removed; those stay deleted.
* **Versioned buckets**: only current objects are listed and deleted without a version id, so S3 adds delete markers and keeps every earlier version (the command reports the number of delete markers created). Permanently purging versions is not done in prefix mode. Object Lock retention is enforced by S3; `--bypass-governance` applies to GOVERNANCE retention as for other deletes.

### Recursive bucket delete

```bash
# Delete all objects in the bucket, then delete the bucket
s3m rm -b --recursive s3/my-bucket
```

### JSON output for automation

```bash
# List objects as JSON
s3m ls s3/my-bucket --json

# Get object metadata as JSON
s3m get -m s3/my-bucket/file.dat --json

# Usage summary as JSON
s3m du s3/my-bucket --json

# Stream state as JSON
s3m streams ls --json
```

### Create bucket

```bash
s3m cb s3/new-bucket
```

### Delete

```bash
# Delete object
s3m rm s3/my-bucket/file.dat

# Delete multiple objects
s3m rm s3/my-bucket/a.txt s3/my-bucket/b.txt

# Delete all objects under a literal key prefix (bucket is kept)
s3m rm s3/my-bucket/integration/18/ --recursive

# Delete bucket
s3m rm -b s3/empty-bucket

# Recursively delete bucket contents, then the bucket
s3m rm -b --recursive s3/my-bucket
```

## Compression & Encryption

### Compression

```bash
# Compress before upload (uses Zstandard)
s3m --compress mysqldump.sql s3/backups/db.sql.zst
```

> **Compatibility note:** the dependency update moved the Zstandard encoder
> (`zstd-safe` 8.2 to 8.3), so newly compressed objects are no longer
> byte-for-byte identical to output from older releases. Frames written by
> either build still decompress with the other, so nothing stored is lost, but
> don't rely on identical bytes (or identical checksums) for the same input
> across versions.

### Encryption

```bash
# Generate secure encryption key (32 characters)
openssl rand -hex 16 > encryption.key
```

```yaml
# ~/.config/s3m/config.yml
hosts:
  secure:
    endpoint: s3.us-west-2.amazonaws.com
    access_key: YOUR_ACCESS_KEY
    secret_key: YOUR_SECRET_KEY
    enc_key: 0123456789abcdef0123456789abcdef
```

```bash
# Encrypt during upload using the host config key
s3m file.dat /secure/my-bucket/file.dat.enc

# Download and decrypt using the same host config key
s3m get /secure/my-bucket/file.dat.enc

# Decrypt a local encrypted file manually
s3m --decrypt file.dat.enc 0123456789abcdef0123456789abcdef
```

Uploads and downloads do not accept `--encrypt` or `--enc-key` flags. Encryption is enabled by setting `enc_key` on the selected host in `config.yml`.

> **Integrity note:** encryption uses ChaCha20-Poly1305 in a streaming (chunked) mode — every chunk is authenticated, so tampering, reordering, and forged content are detected and decryption fails. It does **not** detect *truncation* of an already-stored object (trailing chunks removed), and incomplete downloads are caught at the transport layer. If you need end-to-end "exactly what I uploaded" verification, validate restores out-of-band (e.g. compare a checksum).

## Object Lock (WORM)

[S3 Object Lock](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html)
stores objects using a write-once-read-many (WORM) model so they cannot be
deleted or overwritten until a retention date passes or a legal hold is removed.

Object Lock can **only be enabled when the bucket is created** — this also
enables versioning on the bucket:

```bash
s3m cb --object-lock s3/vault
```

Then apply retention and/or a legal hold at upload time:

```bash
# Retain in COMPLIANCE mode until a fixed date (RFC 3339)
s3m --object-lock-mode COMPLIANCE --retain-until 2027-01-01T00:00:00Z backup.tar s3/vault/backup.tar

# GOVERNANCE mode (privileged users can bypass) + a legal hold
s3m --object-lock-mode GOVERNANCE --retain-until 2027-01-01T00:00:00Z --legal-hold db.sql s3/vault/db.sql

# Legal hold only (no fixed retention date)
s3m --legal-hold report.pdf s3/vault/report.pdf
```

`--object-lock-mode` and `--retain-until` must be used together. The retention
and legal-hold settings apply to every upload path, including `--pipe`,
`--compress`, and encrypted streams.

> **Note:** the target bucket must already have Object Lock enabled. Uploading
> lock settings into a plain bucket is rejected by S3 (`InvalidRequest`); s3m
> surfaces a hint to create the bucket with `s3m cb --object-lock`.

### Bucket default retention

Instead of passing lock flags on every upload, set a **bucket default
retention** once — S3 then applies it to every new object automatically. The
bucket default uses a duration (`--days` or `--years`), not a fixed date:

```bash
# Every new object gets 30 days of COMPLIANCE retention
s3m object-lock set s3/vault --mode COMPLIANCE --days 30

# View the bucket's Object Lock configuration
s3m object-lock get s3/vault
s3m object-lock get s3/vault --json
```

### Managing retention & legal hold on existing objects

```bash
# Show an object's retention + legal hold
s3m object-lock get s3/vault/file.dat

# Set/extend per-object retention (uses a date, like uploads)
s3m object-lock set s3/vault/file.dat --mode GOVERNANCE --retain-until 2027-01-01T00:00:00Z

# Shorten GOVERNANCE retention needs an explicit bypass
s3m object-lock set s3/vault/file.dat --mode GOVERNANCE --retain-until 2026-07-01T00:00:00Z --bypass-governance

# Toggle a legal hold
s3m object-lock set s3/vault/file.dat --legal-hold on
s3m object-lock set s3/vault/file.dat --legal-hold off
```

A specific version can be targeted with `--version-id <id>`, for objects only.
`object-lock get --version-id` on a bucket target is rejected.

`--bypass-governance` only applies when setting object retention (`--mode` with
`--retain-until`). It is rejected on a bucket target and on a legal-hold-only
change. A bucket that doesn't exist is an error (exit 1), not "not enabled".

## Advanced Options

### Buffer size

```bash
# Adjust part size for multipart uploads (in bytes): 50 MiB
s3m --buffer 52428800 big-file.dat s3/large/huge-file.dat
```

`--buffer` takes bytes, not megabytes. Values below 5 MiB (5242880) are raised
to the 10 MiB default.

### Bandwidth throttling

```bash
# Limit upload speed (in KB/s, 0 means no limit)
s3m -k 10240 file.dat s3/backups/file.dat  # about 10 MB/s
```

The flag is `--kilobytes/-k`. There is no `--throttle` option.

### Retries

```bash
# Make up to 5 attempts for each multipart part
s3m --retries 5 big-file.dat s3/bucket/big-file.dat
```

`--retries N` sets the total number of **attempts** for single-part uploads,
multipart parts, and streaming requests. `--retries 1` and `--retries 0` both
make a single attempt. Single-part uploads retry temporary connection or service
failures with exponential backoff and jitter. Each attempt checks the source
size and modification time before sending, computes all payload checksums in
the same read that verifies the original BLAKE3, and verifies the streamed
body against it. If the source changes during a successful PUT, s3m reports
the change and leaves the ETag out of local upload state.

On a versioned bucket, a successful single-part attempt whose response is lost
can leave an earlier identical version when the retry succeeds. The earlier
version keeps its retention and legal hold. s3m reports the version ID returned
by the successful retry.

### Connection and response timeouts

s3m uses HTTP/1.1 and allows 60 seconds to connect, including DNS, proxy
negotiation, and TLS, matching the
[AWS CLI socket connect default](https://docs.aws.amazon.com/cli/latest/reference/s3/ls.html#global-options).
Once connected, s3m normally times out after 60 seconds of silence. Any bytes
received from the peer keep the request alive, including partial TLS records
and TLS control traffic. Successful socket writes and acknowledgements of
queued upload bytes also count as progress when s3m can observe them.

Once the server has acknowledged the entire upload, a slow server and a stuck
one look the same from the client. The response wait then has the same
60-second idle limit. If the send queue cannot be sampled, the kernel can
deliver buffered upload bytes while the next socket write is blocked. During
that wait and after body handoff, s3m allows five minutes plus an estimate of
the time to drain that request's buffered bytes, capped at 15 minutes of
silence. The estimate assumes at least 1 KiB/s. Slower receivers or larger
buffered tails can time out when the queue is unavailable.

Transfers have no overall deadline. Waiting for local upload input or deliberate
pacing does not by itself cause a timeout. Connection and transfer timeouts
count as transient failures for single-part upload retries.

### Behavior notes

* S3 redirects report the HTTP status and any S3 error code, region, or
  sanitized destination supplied by the server. For a permanent redirect (301),
  check the region or endpoint in the host config. A temporary redirect (307)
  describes the destination for that request.
* `s3m share --expire` accepts 1 to 604800 seconds. Values outside that range
  exit 2 without printing a URL.
* `s3m --clean` exits nonzero when it can't remove stale state.
* Without `--config`, a missing `~/.config/s3m/config.yml` (or
  `$S3M_HOME/config.yml`) exits 1 with an error that names the file.
* Uploading a file that s3m already uploaded to the same key, unchanged (same
  content and modification time), is skipped when the object on the server still
  has the `ETag` of that upload. s3m checks with a `HEAD` request and reports a
  redirect without re-uploading. When the object is gone or changed, or the `HEAD`
  request fails for another reason (such as write-only access), the file is
  uploaded again. The skip compares only
  the `ETag`: `--meta`, `--acl` and Object Lock flags given on that run are not
  applied to the existing object (use `s3m acl` or `s3m object-lock` for that).

### Concurrency

```bash
# Set the max number of concurrent part requests
s3m -n 8 big-file.dat s3/large/huge-file.dat
```

When `-n/--number` is not given, the default is derived from
[`std::thread::available_parallelism()`], which **respects cgroup CPU quotas and
CPU affinity** — so inside a container limited to e.g. 2 CPUs the default stays
low even on a many-core host, keeping resource usage predictable. Two units are
reserved for the runtime, and the value is always at least `1`.

[`std::thread::available_parallelism()`]: https://doc.rust-lang.org/std/thread/fn.available_parallelism.html

## Notes for streaming and transformed uploads

- Regular file multipart uploads can be resumed.
- `STDIN` / `--pipe` uploads are not resumable after interruption because the original input stream cannot be replayed safely.
- When the input size is unknown, `s3m` uses a fixed multipart buffer of `512 MiB` per part.
- Streaming and transformed upload paths (for example `STDIN`, compression, and encryption) use a two-line progress display: the top line shows local buffering progress for the current `512 MiB` part, and the bottom line shows either `confirmed ...` bytes or `sending part N ... | confirmed ...` during the active part upload.
- For interrupted or failed multipart uploads from streaming paths, configure bucket lifecycle rules to clean up incomplete multipart uploads automatically.

## Container image

Each release is published as a multi-arch image (`linux/amd64`, `linux/arm64`) at
`ghcr.io/s3m/s3m`, tagged `X.Y.Z`, `X.Y`, `X` and `latest`. The image holds the
static binary at `/s3m` (the entrypoint), CA certificates and a non-root user, on
[distroless static](https://github.com/GoogleContainerTools/distroless). `S3M_HOME`
is `/tmp/s3m`.

```bash
docker run --rm \
  -v ~/.config/s3m/config.yml:/config.yml:ro \
  -v "$PWD":/data:ro \
  ghcr.io/s3m/s3m:latest --config /config.yml /data/backup.tar s3/backups/backup.tar
```

The `/s3m` binary is byte for byte the one in that release's
`s3m-X.Y.Z-<arch>-unknown-linux-musl.tar.gz`, so it can be checked against the
release's `SHA256SUMS`. The image has a build provenance attestation:

```bash
gh attestation verify oci://ghcr.io/s3m/s3m:latest --repo s3m/s3m \
  --signer-workflow s3m/s3m/.github/workflows/release.yml
```

### Kubernetes image volume

To run s3m from another container (for example a backup `CronJob` that first
writes a snapshot), mount the image as an
[image volume](https://kubernetes.io/docs/concepts/storage/volumes/#image) pinned
by digest. Image volumes may be mounted `noexec`, so copy the binary to an
`emptyDir` first:

```yaml
spec:
  volumes:
    - name: s3m
      image:
        reference: ghcr.io/s3m/s3m@sha256:<digest>
    - name: s3m-config
      secret:
        secretName: s3m-config # holds config.yml
    - name: work
      emptyDir: {}
  containers:
    - name: backup
      image: <your job image>
      env:
        - name: S3M_HOME
          value: /work/s3m
      command: ["/bin/sh", "-ec"]
      args:
        - |
          cp /opt/s3m/s3m /work/s3m-bin && chmod 0755 /work/s3m-bin
          # ... write /work/snapshot ...
          /work/s3m-bin --config /etc/s3m/config.yml --retries 5 \
            /work/snapshot "r2/backups/snapshot-$(date +%F).snap"
      volumeMounts:
        - { name: s3m, mountPath: /opt/s3m, readOnly: true }
        - { name: s3m-config, mountPath: /etc/s3m, readOnly: true }
        - { name: work, mountPath: /work }
```

## Use as a library

The S3 client and streaming engine live in the
[`s3m-core`](https://crates.io/crates/s3m-core) crate, which has **no CLI
dependencies** (no clap/colored/env_logger/…). Add it to your own project:

```bash
cargo add s3m-core secrecy anyhow tokio --features tokio/full
```

```rust
use s3m_core::{S3, Credentials, Region, RequestOptions};
use s3m_core::s3::actions::GetObject;
use secrecy::SecretString;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let credentials = Credentials::new("ACCESS_KEY", &SecretString::new("SECRET_KEY".into()));
    let region: Region = "us-west-2".parse()?;
    let s3 = S3::new(&credentials, &region, Some("my-bucket".to_string()), false);

    // Every action returns a typed `s3m_core::Error` you can match on.
    match GetObject::new("path/file.dat", None).request(&s3, &RequestOptions::new()).await {
        Ok(response) => { /* stream the body */ }
        Err(e) if e.is_not_found() => eprintln!("object missing (NoSuchKey/404)"),
        Err(e) => return Err(e.into()),
    }
    Ok(())
}
```

Curated re-exports (`s3m_core::{S3, Credentials, Region, RequestOptions, Error}`)
keep the common entry points one path deep; the full S3 actions live under
`s3m_core::s3::actions` and the resumable/transform pipelines under
`s3m_core::stream`.

## Development

### Running Tests

```bash
# Unit + integration tests (workspace: s3m + s3m-core)
cargo test --workspace

# Integration tests with MinIO (Podman)
just container            # start MinIO (idempotent)
just test-integration     # run MinIO-backed e2e tests against that MinIO
# or in one go
just container test-integration
# full suite (fmt + clippy + docs + unit + integration)
just test
# hunt a flaky test: run a target many times, failed runs are kept in target/stress/
just test-stress                        # the streams tests, 100 times
just test-stress 300 --workspace --lib  # the unit tests, 300 times
# stop/clean the container
podman rm -f s3m-test-minio
```

Example `config.yml` for the default MinIO container:

```yaml
---
hosts:
  minio:
    endpoint: http://localhost:9000
    access_key: minioadmin
    secret_key: minioadmin
```

### Test Coverage

```bash
cargo install cargo-llvm-cov
cargo llvm-cov --all-features --workspace
```

Coverage: **80%+**

### Contributing

1. Write tests for new features
2. Run: `just test` (to run the containerized integration tests)
3. Run: `cargo clippy --workspace --all-targets --all-features`
4. Run: `cargo fmt`
