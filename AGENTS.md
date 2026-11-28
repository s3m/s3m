# AGENTS.md

Instructions for coding agents (Codex, Claude, Gemini and others) working on s3m. Read
[AI_POLICY.md](AI_POLICY.md) too: a human owns every change.

s3m is a command-line tool (crate `s3m`, in `src/`) built on a library (crate `s3m-core`,
in `crates/s3m-core/`) that speaks the Amazon S3 API to AWS S3 and to S3-compatible
servers such as Cloudflare R2, RustFS/MinIO and Backblaze B2.

## 1. AWS defines the protocol: check the official docs first

**Amazon S3 is the specification.** Every request s3m sends and every answer it parses
must follow the AWS documentation. Other providers implement the same API, so code that
follows AWS works everywhere it is meant to; code written around one provider's
behaviour breaks the others and makes every flow harder to reason about.

Before changing how s3m builds a request, signs it, reads an answer, classifies an error
or retries:

1. **Find the AWS documentation for it and cite it** (the page URL) in the issue and the
   commit message. Start from:
   - API reference: <https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html>
   - Error responses and codes: <https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html>
   - Signature Version 4: <https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-authenticating-requests.html>
   - Checksums and integrity: <https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html>
   - Retry behaviour of the AWS SDKs: <https://docs.aws.amazon.com/sdkref/latest/guide/feature-retry-behavior.html>
2. **Follow the documented behaviour, not what one server happens to accept.** A mock
   server or RustFS accepting a request does not make it correct.
3. **Provider differences are exceptions, handled narrowly.** When a provider deviates
   (for example R2 answering `501 NotImplemented` to per-part SHA checksums, #80, or
   leaving out `<Prefix>`, #78), confirm it in that provider's own documentation (R2:
   <https://developers.cloudflare.com/r2/api/s3/api/>) and with a real request, then:
   - keep the AWS behaviour as the default;
   - detect the deviation from what the server actually answered, not from a host name;
   - make the fallback as small as possible, comment it with the issue number and the
     provider's doc link, and cover both paths with tests.
   Never change the AWS behaviour to make one provider work.
4. **If the docs and a real server disagree**, or the docs don't cover the case, stop and
   say so in the issue: don't guess.

## 2. Working on the repository

- Work happens on `sandbox`. `develop` is fast-forwarded from `sandbox` and `main` is
  moved by the release; never push to `develop` or `main`, create tags or releases, or
  run `just release` unless the maintainer explicitly asks for it in that session.
  Pull requests from contributors target `develop` (see [CONTRIBUTING.md](CONTRIBUTING.md)).
- One commit per issue, with the code, its tests and its CHANGELOG entry. Don't mix
  unrelated changes. Never rewrite published commits.
- Commit subjects are plain imperative sentences that say what changed, ending with the
  issue (`Retry single-part uploads on transient network errors (#93)`). No
  Conventional Commits prefixes (`fix:`, `feat:` ...) and no AI co-author trailers.
- `CHANGELOG.md`: add the entry under `## Unreleased` in the style of the existing ones:
  what was wrong, what changed, and any `s3m-core` API change marked **BREAKING**
  (library) or additive. Breaking library changes also go in the compatibility notes of
  `crates/s3m-core/README.md`.
- Keep user-facing docs (README, `--help`) in step with behaviour changes.

## 3. Code rules

The workspace denies warnings and Clippy's `pedantic`, `all`, `unwrap_used`,
`expect_used`, `panic` and `indexing_slicing` (see `Cargo.toml`). Test modules opt out
explicitly with an `#[allow(...)]` list; follow the module's existing list. Functions over
100 lines fail Clippy: split them. Rustdoc warnings fail CI (`just doc`).

## 4. Tests

- **A regression test must fail without the fix.** Show it: revert or disable the
  production change, run the test, see it fail with a message that names the problem,
  restore, see it pass.
- Unit and CLI tests talk to **loopback only** (mockito or in-process listeners). They
  must never reach a real endpoint or read the user's `~/.config/s3m/config.yml`.
- **No child processes in unit tests.** A fork briefly inherits every open file of the
  test process, including other tests' sled lock files, and makes unrelated tests flaky.
  Use threads, tasks and direct system calls instead.
- CLI tests set `HOME` and `S3M_HOME` to the fixture's private directories; never inherit
  them, and don't leave temporary directories behind (`TempDir::keep` is not allowed).
- Timing-sensitive behaviour: run the whole suite many times, not once, before trusting it.
- `just test` runs Clippy, fmt, docs, unit tests and the integration tests against a
  disposable RustFS container in podman (`just test-integration`).

## 5. Real providers

`scripts/r2-check.sh` and `scripts/aws-check.sh` (with the shared
`scripts/provider-scenarios.sh`) run against a dedicated Cloudflare R2 bucket and a
dedicated AWS bucket with Object Lock, in CI (`.github/workflows/r2.yml`, `aws.yml`) and
locally. They are the final check for anything that touches the wire protocol.

- They need the maintainer's credentials: agents without them must not try to obtain them.
  Never print, log or commit a credential, and never use a real user bucket.
- Every run writes under a unique `ci/<timestamp>-...` prefix and must delete everything
  it created; a run that leaves anything behind fails.
- On AWS, only GOVERNANCE retention of minutes, never COMPLIANCE (it can't be removed).
  Legal holds never expire: release them in the same run.
- Mocks and RustFS are written to behave like AWS; they are not AWS. When a change depends
  on what a real server returns, check it on the real providers before calling it done.

## 6. Releases

See [.github/RELEASING.md](.github/RELEASING.md). The released commit is exactly the
commit CI tested; agents don't release without the maintainer's explicit go-ahead.
