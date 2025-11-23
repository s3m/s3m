# s3m

[![Deploy](https://github.com/s3m/s3m/actions/workflows/deploy.yml/badge.svg)](https://github.com/s3m/s3m/actions/workflows/deploy.yml)
[![Test & Build](https://github.com/s3m/s3m/actions/workflows/build.yml/badge.svg)](https://github.com/s3m/s3m/actions/workflows/build.yml)
[![codecov](https://codecov.io/gh/s3m/s3m/graph/badge.svg?token=Y2BQJUGPJ5)](https://codecov.io/gh/s3m/s3m)
[![crates.io](https://img.shields.io/crates/v/s3m.svg)](https://crates.io/crates/s3m)

**s3m** a command-line tool for storing streams of data in s3 buckets.

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

Create `~/.config/s3m/config.yml`:

```yaml
hosts:
  s3:
    endpoint: s3.us-west-2.amazonaws.com
    access_key: YOUR_ACCESS_KEY
    secret_key: YOUR_SECRET_KEY
    bucket: my-bucket  # optional default bucket
```

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
```

### Create bucket

```bash
s3m cb s3/new-bucket
```

### Delete

```bash
# Delete object
s3m rm s3/my-bucket/file.dat

# Delete bucket
s3m rm -b s3/empty-bucket
```

## Compression & Encryption

### Compression

```bash
# Compress before upload (uses Zstandard)
s3m --compress mysqldump.sql s3/backups/db.sql.zst
```

### Encryption

```bash
# Generate secure encryption key (32 characters)
openssl rand -hex 16 > encryption.key

# Encrypt during upload
s3m --encrypt --enc-key "$(cat encryption.key)" file.dat s3/secure/file.dat.enc

# Decrypt during download
s3m get s3/secure/file.dat.enc --enc-key "$(cat encryption.key)"
```

## Advanced Options

### Buffer size

```bash
# Adjust part size for multipart uploads (in MB)
s3m --buffer 50 big-file.dat s3/large/huge-file.dat
```

### Bandwidth throttling

```bash
# Limit upload speed (in KB/s)
s3m --throttle 10240 file.dat s3/backups/file.dat  # 10MB/s
```

### Retries

```bash
# Configure retry attempts for failed parts
s3m --retries 5 file.dat s3/bucket/file.dat
```

## Development

### Running Tests

```bash
# Unit tests (255 tests)
cargo test

# Integration tests with MinIO (Podman)
just container            # start MinIO (idempotent)
just test-integration     # run ignored e2e tests against that MinIO
# or in one go
just container test-integration
# full suite (fmt + clippy + unit + integration)
just test
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

Coverage: **80%+** with 255+ tests

### Contributing

1. Write tests for new features
2. Run: `just test` (to run the containerized integration tests)
3. Run: `cargo clippy --all-targets --all-features`
4. Run: `cargo fmt`
