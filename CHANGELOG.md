## 0.14.4
* **Test Infrastructure Overhaul**: Refactored e2e tests from single 1200+ line file into organized, maintainable structure:
  - `tests/common/mod.rs` - Shared helpers (MinioContext, config, binary, hash utilities)
  - `tests/e2e_put.rs` - Upload tests (23 tests)
  - `tests/e2e_get.rs` - Download tests (3 tests)
  - `tests/e2e_cb.rs` - Create bucket tests (1 test)
  - `tests/e2e_ls.rs` - List objects tests (1 test)
  - `tests/e2e_rm.rs` - Delete object tests (1 test)
  - `tests/e2e_misc.rs` - Binary tests (3 tests)
* **New File Size Tests**: Added 6 comprehensive tests to verify single-shot vs multipart upload behavior:
  - Tiny file (1MB) - single PUT
  - Small file (9MB < 10MB buffer) - single PUT
  - Edge case (exactly 10MB) - boundary testing
  - Large file (11MB > 10MB buffer) - multipart upload
  - Very large file (25MB, ~3 parts) - multipart upload
  - Custom buffer test (3MB file with 2MB buffer) - demonstrates testing multipart with smaller files
* **Progress Bar Tests**: Added 4 tests to verify progress bar functionality:
  - Default progress bar behavior (enabled)
  - Progress suppression with --quiet flag
  - Progress bar with multipart uploads
  - --quiet flag with get command
* **Documentation**: Added comprehensive buffer size documentation explaining default 10MB buffer (10485760 bytes) and upload behavior (single-shot vs multipart)
* **Test Fixes**: Fixed encryption tests to use config files (`enc_key` and `compress` fields) instead of non-existent `--encrypt` and `--enc-key` CLI flags
* **CI/CD Updates**: Updated GitHub Actions workflow to run all refactored test files with correct test counts
* **Dependencies**: Upgraded testcontainers from 0.23 to 0.25
* **Test Coverage**: 32 e2e tests + 17 API tests = 49 total integration tests, 255 unit tests
* All tests include blake3 hash verification for file integrity validation
* Tests use production code (`s3m::s3::tools::blake3`) for hash calculation - eating our own dog food
* Benefits: Easy to find/add tests, no code duplication, each file focused on specific command

## 0.14.3
* Fixed issue #65: Missing source file validation. Now returns a clear error message when source file is not provided instead of silently doing nothing
* Improved error handling in multipart upload completion: replaced panic with proper error when part count exceeds S3's 10,000 limit. Error now includes actionable cleanup instructions and bug reporting guidance
* Improved error visibility: Additional checksum failures (--checksum flag) are now logged with appropriate severity (WARN for calculation errors, ERROR for task panics) instead of silently returning None
* Added defensive error handling in `object_put` handler as a safeguard against future validation logic changes
* Code quality improvements: Fixed all clippy warnings using best practices (inlined format args, proper Result handling instead of expect/unwrap)
* Architecture improvement: Centralized AWS S3 limits in `src/s3/limits.rs` module for easy updates when AWS changes quotas (currently supports 5TB max object size, 5GB max part size, 10,000 max parts)
* Added `panic = "abort"` and explicit `opt-level = 3` to release profile for better binary optimization
* Test coverage improvements: Added 12 comprehensive tests (9 for multipart upload + 3 for S3 limits validation) including boundary cases and edge case validation
* Added test case `test_dispatch_missing_source_file` to prevent regression

## 0.14.2
* Cargo lint fixes
* Removed openssl vendored feature

## 0.14.1
* **Critical Fix**: Improved error handling when uploading to non-existent buckets or when server closes connection prematurely. Previously, larger files (>~300KB) would show cryptic "error decoding response body" messages. Now always displays HTTP status code (e.g., "404 Not Found") even if response body can't be read, giving users clear feedback about what went wrong.
* **Clarification**: Progress bar shows file read/send progress, not confirmed upload status. The bar reaches 100% when data is sent to the server, but errors may still occur during server processing. Use `-q` flag to disable progress bar if desired.

## 0.14.0 ✈️
* Migrated to Rust Edition 2024
* Updated all GitHub Actions to latest versions (actions/checkout@v5, etc.)
* Added DEB package generation alongside RPM
* Fixed metadata parsing crash bug with proper error handling
* Replaced deprecated serde_yaml with serde_yaml_ng
* Enhanced deploy workflow with test tag filtering (tags starting with 't')
* Removed unused dependencies and cargo-machete metadata
* Zero deprecation warnings
* All 242 tests passing

## 0.13.2
* Only verify the bucket name when creating a bucket, option `cb`

## 0.13.1
* Fixed get_key to not lower_case the key when using compression or encryption
* Refactored `s3::Region`
* Fixed issues #61, #62

## 0.13.0 🚢
* Enabled encryption using `enc_key` (32 len) in the config file
* replaced serde_xml_rs with quick-xml for XML parsing
* refactored code to use `async` in the try_fold (compression, encryption, stdin pipe)

## 0.12.0 🛥️
* Cargo update, upgrade to bincode 2

## 0.11.1
* Added option `--version` to command `get` to download a specific object version
* Fix missing bandwithd throttle when using `--pipe` or `--compress` option

## 0.11.0 🚆
* Added option `--versions` to command `get` to list object versions
* Added option `--number` to command `ls` to limit list to a number of objects, upload or buckets

## 0.10.0 🛶
* Compression support `-x/--compress` (using zstd)

## 0.9.5
* Fixed a regression that prevented listing buckets when only a host is defined. Added a test case to cover this scenario.

## 0.9.4
* Accept new regions if any `s3.new-region.amazonaws.com`
* fixes #58 ensure min(1)/max(255) when using num_cpus

## 0.9.3
* Allow path(key) names ending with `/` fixes #57

## 0.9.0 🚅
* Added option `-r/--retries` defaults to 3 (exponential backoff 1 second)

## 0.8.3
* Added user-agent on the request
* Implemented MAX_RETRIES when reading from STDIN
* Fixed bug when chunk > 512MB (next 8192 bytes were not stored)

## 0.8.2
* Using `openssl = { version = "0.10", optional = true, features = ["vendored"] }` to make FreeBSD Port and Linux package (musl)

## 0.8.1
* Added option `-k/--kilobytes` to throttle the bandwidth in kilobytes per second

## 0.8.0 🚋
* Added option `--no-sign-request` to support public buckets

## 0.7.6
* Added option `-n/--number` [1..3500] to specify the max number of concurrent requests

## 0.7.4
* Added --verbose flag
* Fixed bug potential to loop indefinitely while creating multi-parts

## 0.7.1
* Removed after_help.
* Added `-f/--force` when getting a file.
* Replaced `-H/--head` with `-m/--meta` to retrieve metadata.

## 0.7.0 🚜
* Added support for Additional Checksum Algorithms `x-amz-checksum-mode` option `--checksum`

## 0.6.1
* Added subcommand `show` instead of option `-s/--show` to list current available hosts

## 0.6.0 🛺
* Added sub-comand `cb` (Create bucket)
* Added option `-b/--bucket` to remove bucket when using `rm`
* Added option `-t/--tmp-dir` directory for temporarily storing the stdin buffer.
* Added option `-s/--show` to list current available hosts

## 0.5.0 🛵
* removed ending `/` when listing buckets
* fixed sub-command `get` to download file in the current path
* Added option `--clean` instead of `-r/--remove`
* Added option `-a/--acl` to set `x-amz-acl` for the object
* Added option `-m/--meta` to add custom metadata for the object
* Added option `-q/--quiet` to silent output
* Added option `-p/--pipe` to read from STDIN
* STDIN pipe/stream will be uploaded in chunks to prevent keeping the chunk in RAM it uses a temp file (512MB) and from there streams
* `ls` now supports options  `-p/--prefix` and `-a/--start-after` to list only files starting with a prefix or start listing from a specified key
* new sub-command `acl` to get and set ACL's


## 0.4.0 ⛸
* New sub-command `get` with option `-h` to return HeadObject.
* New sub-command `share` to create presigned URL's.
* Defaults to 512MB buffer size when no `-b` defined in case input is 5TB when uploading from STDIN.


## 0.3.1 🛴
* STDIN pipe/stream will upload the chunks in oneshot not using `Transfer-Encoding: chunked`
* `{bytes}/{total_bytes}` in progress bar.
* `UTC` timezone when listing objects/buckets.
* Print uploaded bytes when streaming (#19).


## 0.3.0  🚲
* Using [blake3](https://crates.io/crates/blake3), for creating the file checksum, thanks @oconnor663
* Using [sled](http://sled.rs/) with [serde_cbor](https://crates.io/crates/serde_cbor) to keep track of the uploaded files. thanks @spacejam, @D1plo1d
* Multipart uploads can be resumed and is the default behavior.
* The file checksum, mtime and the s3 credentials are used to keep track of the uploads this also prevent uploading the same file multiple times to the same location.
* Added option `-r` to cleanup `~/.s3m/streams` directory.
* Added option `ls -m` to list in-progress multipart uploads.
* New sub-command `rm` to remove objects and abort a multipart upload.
* `ls` list format (green, yellow, key) "date, size, file name"
* STDIN pipe/stream uploads (WIP #17)


## 0.2.0
* Implemented lifetimes  🌱
* `sha256` and `md5` returning digest in the same loop using `async` so that we could use `Content-MD5` to better check integrity of an object.
* [blake2](https://crates.io/crates/blake2s_simd) for keeping track of upload progress using `sled` (WIP)
