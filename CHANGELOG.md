## 0.19.0 ⚽ (2026-06-27)
* **S3 Object Lock (WORM)**: new support for write-once-read-many retention and legal holds (issue #67).
  * `s3m cb --object-lock <host>/<bucket>` creates a bucket with Object Lock enabled (which also enables versioning).
  * Uploads accept `--object-lock-mode GOVERNANCE|COMPLIANCE` with `--retain-until <RFC3339>` (the two are required together) and/or `--legal-hold`. Settings apply across every upload path — regular, multipart, `--pipe`, compressed, and encrypted streams. When a lock-carrying upload hits a bucket that is not Object-Lock-enabled, S3 returns `InvalidRequest` and `s3m` appends a hint to create the bucket with `--object-lock`.
  * New `s3m object-lock get/set` command. On a **bucket** target it manages the default retention rule (`--mode` + `--days`/`--years`) so every new object inherits retention without per-upload flags; on an **object** target it gets/sets per-object retention (`--mode` + `--retain-until`, with `--bypass-governance` to shorten GOVERNANCE) and legal hold (`--legal-hold on|off`). `--version-id` targets a specific version; `--json` for machine-readable output.
  * `s3m rm` gains `--version-id` (delete a specific object version instead of inserting a delete marker) and `--bypass-governance` (delete GOVERNANCE-locked versions, requires `s3:BypassGovernanceRetention`). On a versioned bucket a keyed delete now reports that a **delete marker** was created (existing versions retained), so a successful `rm` is no longer mistaken for data removal.
  * `s3m rm -b --recursive` is now **version-aware**: it enumerates object versions and delete markers (via `ListObjectVersions`) and removes them before deleting the bucket, so it can empty versioned / Object-Lock buckets (previously it only deleted current keys, leaving versions behind and failing `DeleteBucket`). Retained versions are still refused unless `--bypass-governance` is given, and COMPLIANCE retention can never be bypassed.
* **`s3m-core` — reusable Object Lock API**: new `ObjectLock` / `ObjectLockMode` types (re-exported at the crate root) carried via `RequestOptions::object_lock`; `CreateBucket::new` takes an `object_lock_enabled` flag and `PutObject::new` / `CreateMultipartUpload::new` take an `Option<ObjectLock>`, emitting the `x-amz-bucket-object-lock-enabled` and `x-amz-object-lock-*` headers. Object Lock can only be enabled at bucket creation. Added six new actions — `Get`/`PutObjectLockConfiguration`, `Get`/`PutObjectRetention`, `Get`/`PutObjectLegalHold` — with response types `ObjectLockConfiguration`/`ObjectRetention`/`ObjectLegalHold`. Added unit tests for header/body emission plus MinIO-backed e2e tests (lock-enabled upload succeeds; plain-bucket upload rejected; bucket default retention round-trips; per-object retention and legal hold round-trip).
* **BREAKING (`s3m-core` public API)** — call sites must be updated when moving from 0.18.x to 0.19.0:
  * `CreateBucket::new(acl)` → `CreateBucket::new(acl, object_lock_enabled: bool)`.
  * `PutObject::new(..)` gains a trailing `object_lock: Option<ObjectLock>` argument.
  * `CreateMultipartUpload::new(..)` gains a trailing `object_lock: Option<ObjectLock>` argument.
  * `RequestOptions` gains a public `object_lock: Option<ObjectLock>` field (only affects exhaustive struct-literal construction; `RequestOptions::new()` / `..Default::default()` are unaffected).
  * `DeleteObject::request` now returns `DeleteObjectOutput { delete_marker, version_id }` instead of `String` (the old value was the — usually empty — response body). `DeleteObject` also gains `version_id(..)` / `bypass_governance(..)` builders and `DeleteObjects` gains `bypass_governance(..)` (additive).
  * All other changes are additive. The `0.x` minor bump (0.18 → 0.19) signals this incompatibility, so Cargo will not auto-upgrade dependents.

## 0.18.2
* **Release packaging — aarch64 (ARM64) Linux**: the deploy workflow now builds `.deb` and `.rpm` packages (plus the musl binary tarball) for `aarch64-unknown-linux-musl` on a native ARM runner, alongside the existing x86_64 artifacts. The RPM architecture is set explicitly via `--arch`, and `cargo deb` is invoked with `--target` so the Debian architecture (`amd64`/`arm64`) and binary path are correct per build.
* **Dependencies**: cargo update to the latest compatible set. `time` is pinned to `0.3.47` — `time 0.3.48` fails to compile (`E0119` conflicting `From` impls in its `format_description` parser) when `bollard` (via the `testcontainers` dev-dependency) is built; the pin can be dropped once a fixed `time` is released.

## 0.18.1
* **`s3m-core` — fully typed public `s3` API**: `S3::endpoint`, `s3::request::{request, multipart_upload, upload}`, `Signature::{new, sign, presigned_url}`, `tools::calculate_part_size`, the `checksum::` digest helpers, and the internal `DeleteObjects`/`CompleteMultipartUpload` body helpers now return typed errors (`s3::error::Result` / `Result<_, s3::Error>` / `std::io::Result`) instead of `anyhow::Result`, so the reusable `s3` surface no longer leaks `anyhow` (only the deliberate `From<anyhow::Error>` bridge remains, to convert internal helpers inward). Added `Error` variants `Url`, `HeaderName`, and `HeaderValue`, plus a `From<tokio::task::JoinError>` conversion. Behavior is unchanged (the CLI already consumed these via `?`).
* **`s3m-core` — re-exports**: `ApiError` and the `Result` alias are now re-exported from the crate root (alongside `S3`/`Credentials`/`Region`/`RequestOptions`/`Error`).
* **`s3m-core` — README**: fixed the crates.io README (removed rustdoc doctest-hidden `#` lines that rendered literally; corrected an `s3_core::Error` → `s3m_core::Error` typo; added an Installation section covering `s3m-core` plus the `tokio`/`secrecy`/`anyhow` deps the example uses directly).
* **CI publish hardening**: removed `continue-on-error: true` from the crates.io publish job and switched the script to `set -euo pipefail`, so a *genuine* publish failure now marks the workflow red instead of being silently swallowed. Routine `s3m`-only releases still stay green on their own — the unchanged `s3m-core` is detected as already-published and skipped in-script (returning `0`), and a genuine `s3m-core` failure now aborts before attempting the dependent `s3m` publish.

## 0.18.0 🛸
* **`s3m-core` — symmetric decode API (restore side)**: Added the counterparts of the encode primitives so the library can *restore* data, not just upload it: `decompress_chunk` (zstd decode, handles concatenated frames, **bounded by a `max_output` cap** to guard against decompression bombs on untrusted data), `init_decryption` + `decrypt_chunk` (ChaCha20-Poly1305 streaming decrypt), and `parse_nonce_header`. This makes `s3m-core` fully reusable for download/restore workflows (e.g. backup tooling) without depending on the CLI. Added round-trip unit tests (compress↔decompress incl. multi-frame and the size-limit guard, encrypt↔decrypt with framing, nonce-header parse).
* **CLI delegates to `s3m-core`**: `s3m --decrypt` and the streaming download-decrypt path (`get` on encrypted objects) now call the shared `s3m-core` decode primitives instead of duplicating the logic. Behavior is unchanged (verified by the existing decrypt unit test and the `get`+decrypt e2e round-trip).
* **`monitor`: optional `prefix`**: Monitor bucket rules no longer require `prefix` — when omitted (or empty) the rule scans the whole bucket, with `suffix`/`age`/`size` still applied. An empty `prefix` is omitted from the metric labels/tags (an empty InfluxDB tag value is invalid line protocol; the Prometheus label is dropped too), matching how an empty `suffix` is handled.
* **Hardening**: the streaming download-decrypt path now rejects an absurd per-chunk length frame (corrupt/hostile data) before buffering, instead of accumulating up to ~4 GiB. Note (unchanged, documented limitation): encryption authenticates every chunk (tampering, reordering, and forgery are detected) but does not detect truncation of a stored object.

## 0.17.2
* **CI publish fix**: The crates.io publish step pre-checked version existence via the crates.io API with `curl`, but crates.io rejects the default `curl` User-Agent with `403` — so the check failed, the step tried to republish the already-published `s3m-core 0.17.0`, collided, and never published the binary crate. The step now publishes each crate (`s3m-core` then `s3m`) and treats an "already uploaded" error as a skip, independent of the crates.io API. (Note: `0.17.1` reached GitHub Releases and PackageCloud but not crates.io due to this bug; `0.17.2` restores crates.io publishing.)

## 0.17.1
* **Docs**: Documented the reusable `s3m-core` library in the README — a new "Use as a library" section with a typed-`Error`/`RequestOptions` example, an `s3m-core` crates.io badge, and a `### Concurrency` note explaining the cgroup-aware `available_parallelism()` default for `-n/--number`.
* **CI**: Bumped `codecov/codecov-action` from `v5` to `v7` (Node 24 runtime + template-injection security fix from v6.0.1).
* **Packaging**: Trimmed the PackageCloud distribution lists to currently-supported releases (e.g. Debian `bookworm`/`trixie`, Ubuntu `jammy`/`noble`, Fedora `43`/`44`, EL `9`/`10`, openSUSE `15.6`/`16.0`).
* **Dev tooling**: Fixed the `.justfile` recipes that regressed after the workspace split — `test-unit`, `clippy`, and `coverage` now run with `--workspace` so the `s3m-core` crate is included.

## 0.17.0 🪐
* **Security (transitive)**: Refreshed `Cargo.lock` to clear `cargo audit` advisories in transitive dependencies: `rustls-webpki` (RUSTSEC-2026-0104, reachable panic in CRL parsing, reached via `reqwest`), `rkyv` (RUSTSEC-2026-0122, unsound `clear` use-after-free), and `astral-tokio-tar` (RUSTSEC-2026-0112/0113/0145, dev-only via `testcontainers`). No production source changes were required.
* **cgroup-aware concurrency**: Replaced `num_cpus::get_physical()` with `std::thread::available_parallelism()` for the default concurrent-request count via a new `tools::default_concurrency()` helper. Unlike physical-core counting, this respects cgroup CPU quotas and CPU affinity, so the default no longer over-subscribes when running under a container CPU limit (the primary deployment target). Behavior note: on hyper-threaded bare metal the default may rise (logical vs. physical cores); set `-n/--number` to pin an explicit value.
* **Dependency hygiene**: Dropped the `num_cpus` dependency, trimmed `tokio` from the `full` feature set to only the features actually used (`rt-multi-thread`, `macros`, `fs`, `io-std`, `io-util`, `sync`, `time`), and moved the test-only `temp-env` crate to `[dev-dependencies]`.
* **Library reusability — decoupled options**: Moved the per-transfer options struct out of the CLI module into `s3::options::RequestOptions`, so the `s3` module no longer depends on any CLI types. `cli::globals::GlobalArgs` is now a thin alias of it, keeping the binary unchanged. Added curated top-level re-exports (`s3m::{S3, Credentials, Region, RequestOptions, Error}`) so external consumers avoid deep module paths.
* **Library reusability — workspace split**: Extracted the S3 client and streaming engine into a new `s3m-core` library crate (containing the `s3`, `stream`, and `progressbar` modules); the `s3m` binary crate now depends on it and re-exports it, so `s3m::s3::…` / `s3m::stream::…` paths and the CLI are unchanged. `s3m-core` pulls in none of the CLI dependencies (clap, colored, dirs, env_logger, regex, serde_json), giving downstream consumers a lean dependency tree. The strict lint set is now shared via `[workspace.lints]`, and CI (`test`/`coverage` workflows) runs with `--workspace`.
* **Library reusability — typed errors**: Introduced `s3::Error` (a `thiserror` enum) and `s3::error::Result<T>`; every public S3 action now returns it instead of `anyhow::Error`. The `Error::Api` variant carries the structured S3 `code`/`message`/HTTP `status` (with `Error::code()`/`status()`/`is_not_found()` helpers) so callers can match on failures programmatically. `anyhow` is still used internally and in the binary — `s3::Error` converts into `anyhow::Error` via `?`, and internal `anyhow` errors convert into `Error::Other` — so the CLI is unchanged.
* **Known issue**: `sled 0.34` (resumable-upload state DB) still pulls in the unmaintained `instant` and `fxhash` crates (RUSTSEC-2024-0384, RUSTSEC-2025-0057). These are local-only, low-risk `unmaintained` warnings; replacing the embedded DB is tracked separately.

## 0.16.2
* **Time-dependent test fix**: Updated S3 action integration tests to use dynamic timestamps for "new" objects in mocks, preventing unintended 30-day filter matches as time passes.
* **Rust 1.95 compatibility**: Updated the codebase to be fully compliant with Rust 1.95 standards, including updated `cargo clippy` and `cargo fmt` rules.

## 0.16.1
* **Streaming progress semantics**: Streaming and transformed uploads now use a two-line progress display with per-part buffering progress (`0 .. 512 MiB`) plus a lower status line that switches between `confirmed ...` and `sending part N ... | confirmed ...` during the active part transfer.
* **Retry progress inflation fix**: Multipart `StreamPart` retries no longer inflate spinner totals when a part is retried after a transient network or S3 error.
* **Encrypted stream accounting**: Initial encryption header bytes are now counted consistently in per-part buffering progress for encrypted streaming paths.
* **Documentation and tests**: Updated the README notes for streaming/transformed uploads and added direct unit coverage for buffering, sending, and confirmed progress accounting, alongside end-to-end verification for raw stdin, compressed stdin, transformed file uploads, and normal file uploads.

## 0.16.0 🛰️
* **`monitor` subcommand**: Added `s3m monitor <host>` to run host-scoped bucket monitoring checks from the existing `config.yml` without changing current upload, download, list, or stream commands.
* **Host-scoped monitor rules**: Extended `hosts.<host>.buckets` in `config.yml` to support per-bucket rule lists with `prefix`, optional `suffix`, `age`, and `size`, including multiple prefixes per bucket.
* **Human-friendly age syntax**: Monitor `age` now accepts either plain seconds or suffixed durations such as `30s`, `15m`, `12h`, and `7d`.
* **Metrics-first output**: `monitor` now emits Prometheus text exposition format by default, with `--format influxdb` available for line-protocol output suitable for `vmagent` ingestion.
* **Failure exit flag**: Added `--exit-on-check-failure` so metrics are still printed first, then the process exits non-zero when a check is missing, errors, or has a size mismatch.
* **Monitor metrics**: Added `s3m_object_exists`, `s3m_check_error`, and `s3m_size_mismatch` metrics with `host`, `bucket`, `prefix`, and optional `suffix` labels.
* **Endpoint and region compatibility**: Fixed config resolution so a host can use a custom `endpoint` together with a `region`, preserving the custom endpoint while using the region for signing. This benefits `monitor` and the existing S3 subcommands.
* **Monitor safety and efficiency**: Empty monitor rule lists now fail fast instead of silently succeeding, and checks stop scanning once a fresh object satisfies the rule.
* **Documentation**: Updated the docs site to add a dedicated `monitor` page, replace the homepage JSON entry with `Monitor`, and document Prometheus/InfluxDB output, `vmagent` push examples, and user cron wrappers with alerting.
* **Regression tests and cleanup**: Added broad unit, integration, and end-to-end coverage for monitor parsing/evaluation/output, endpoint+region handling, and resumable stream uploads. Also removed the remaining production `#[allow(clippy::...)]` suppressions through refactors instead of silencing lints.

## 0.15.0 🚀
* **Multi-object delete**: Added low-level S3 `DeleteObjects` support, including XML request generation, response parsing for deleted objects and per-object errors, and request batching up to `1000` objects.
* **`rm` improvements**: `s3m rm` now accepts multiple object paths. A single object still uses `DeleteObject`; `2+` object targets use `DeleteObjects`, grouped by host/bucket and split into `1000`-key batches as needed.
* **Bucket delete orchestration**: Added CLI-side recursive bucket deletion support that removes bucket contents in batches before issuing `DeleteBucket`, while keeping the low-level bucket delete action thin.
* **Multipart stream state commands**: Added `s3m streams`, `streams ls`, `streams show <id>`, `streams resume <id>`, and `streams clean` for inspecting and managing local resumable multipart upload state. Legacy `--clean` now uses the same conservative cleanup path.
* **Stream state output improvements**: `streams ls` now prints copyable stream IDs, a header row, truncated upload IDs for list view, and colorized statuses to make broken/resumable/active state easier to scan quickly.
* **`du` command**: Added `s3m du` to summarize object count and total bytes for a bucket or prefix using paginated `ListObjectsV2` aggregation without storing every object in memory.
* **`du --group-by day`**: Added per-day usage summaries grouped by each object's `LastModified` UTC calendar day, with deterministic date ordering and a final total row.
* **Retention filters**: Added `--older-than` filtering for `ls` and object-mode `rm` using each object's `LastModified`, with positive-only duration parsing for `Nd`, `Nh`, and `Nm`.
* **JSON output**: Added `--json` output for `ls`, `get --meta`, `get --versions`, `du`, `streams`, `streams ls`, `streams show`, and `streams clean`, keeping text output unchanged by default and emitting valid machine-readable JSON only when requested.
* **Documentation**: Updated the README and the `s3m.stream` docs site to cover `du`, `streams`, retention-oriented delete/list filtering, and the new JSON output mode.
* **Regression tests**: Added unit, action-layer, binary integration, and mocked S3 coverage for `DeleteObjects`, multi-object `rm` grouping/batching/error handling, recursive bucket deletion orchestration, stream state commands, and `du` including grouped day summaries.

## 0.14.8
* **Regression tests**: Added direct action-layer `request()` coverage against mocked S3 responses to reduce the coverage drop caused by the shared `reqwest::Client` refactor.
* **E2E test helper fixes**: Updated the MinIO test helpers to create buckets through signed S3 actions instead of unsigned raw HTTP, restoring the ignored MinIO/Podman-backed end-to-end suite.
* **CI maintenance**: Updated `actions/cache` to `v5` in the container integration workflow to stay compatible with GitHub Actions Node.js 24.

## 0.14.7
* **HTTP client reuse**: Reuse a shared `reqwest::Client` across requests instead of creating a new client per operation. This improves connection pooling and TLS session reuse during multipart uploads and downloads.
* **Progress handling cleanup**: Replaced the blocking progress channel path with Tokio-native async channels so progress reporting no longer blocks Tokio worker threads.
* **CLI help improvements**: Kept `-h` short while expanding `--help` with clearer S3 path syntax, beginner-friendly wording, and practical examples for the main command and common subcommands.
* **STDIN documentation**: Documented that regular file multipart uploads are resumable, but `STDIN` / `--pipe` uploads are not resumable after interruption and use fixed `512 MiB` multipart parts when the input size is unknown.
* **Regression tests**: Added coverage for progress task shutdown/byte accumulation and for reusing the same HTTP client across multiple requests.
* **Benchmarks**: Added benchmarks for the progress channel path and a more realistic read/hash/progress hot path. Results confirm progress signaling overhead is negligible compared with hashing and I/O.
* **Dependencies**: Removed unused `crossbeam`, upgraded `quick-xml` to `0.39`, `rand` to `0.10.0`, and `testcontainers` to `0.27`.

## 0.14.5
* **Replaced bincode with rkyv**: Migrated serialization from bincode to rkyv for zero-copy deserialization
  - Faster reads from sled database (no allocations on deserialize)
  - Lower memory pressure for constrained environments
  - rkyv is actively maintained (bincode becoming unmaintained)
* **Breaking change**: Existing sled databases with pending uploads won't be readable. Run `s3m --clean` to clear old data before upgrading
* **Test coverage**: Added 13 new tests for rkyv serialization (268 total unit tests)

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
