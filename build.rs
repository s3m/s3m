//! Builds what `s3m --version` prints, in rustc's format: the version, then the
//! short commit hash and the commit date, `0.23.0 (7464e3d15 2026-11-21)`.
//! `s3m -V` prints the version alone.
//!
//! The commit comes from, in order:
//! 1. `.cargo_vcs_info.json`, which `cargo package`/`cargo publish` writes into
//!    every published crate, so `cargo install s3m` reports the released commit.
//!    It records no date: `0.23.0 (7464e3d15)`;
//! 2. git, only when this crate is the root of the repository, so a build inside
//!    an unrelated repository never reports that repository's commit;
//! 3. nothing (a source tarball without `.git`): `--version` then matches `-V`.

use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
};

/// rustc prints 9 characters of the commit hash.
const SHORT_HASH_LEN: usize = 9;

fn main() {
    // Without any directive Cargo reruns this script whenever any file in the
    // package changes; with one, only when a watched path changes.
    println!("cargo:rerun-if-changed=build.rs");

    let manifest_dir = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap_or_default());
    let version = env::var("CARGO_PKG_VERSION").unwrap_or_default();

    let long_version = match packaged_commit(&manifest_dir)
        .map(|sha| (sha, None))
        .or_else(|| git_commit(&manifest_dir))
    {
        Some((sha, Some(date))) => format!("{version} ({} {date})", short(&sha)),
        Some((sha, None)) => format!("{version} ({})", short(&sha)),
        None => version,
    };

    println!("cargo:rustc-env=S3M_LONG_VERSION={long_version}");
}

/// The commit recorded by `cargo package` in a published crate.
fn packaged_commit(manifest_dir: &Path) -> Option<String> {
    let path = manifest_dir.join(".cargo_vcs_info.json");
    let info = fs::read_to_string(&path).ok()?;
    rerun_if_changed(&path);
    // {"git": {"sha1": "<40 hex>"}, "path_in_vcs": ""}
    let (_, rest) = info.split_once("\"sha1\"")?;
    let (_, rest) = rest.split_once('"')?;
    let (sha, _) = rest.split_once('"')?;
    is_commit_id(sha).then(|| sha.to_string())
}

/// The checked-out commit and its date (in the commit's own time zone, like
/// rustc), when this crate is the root of its git repository.
fn git_commit(manifest_dir: &Path) -> Option<(String, Option<String>)> {
    let top_level = git(manifest_dir, &["rev-parse", "--show-toplevel"])?;
    if fs::canonicalize(top_level).ok()? != fs::canonicalize(manifest_dir).ok()? {
        return None;
    }

    watch_head(manifest_dir);

    // `--no-show-signature`: with `log.showSignature` set, git would print the
    // signature check before the formatted line.
    let line = git(
        manifest_dir,
        &[
            "log",
            "-1",
            "--no-show-signature",
            "--date=short",
            "--format=%H %cd",
            "HEAD",
        ],
    )?;
    let (sha, date) = line.split_once(' ')?;
    is_commit_id(sha).then(|| (sha.to_string(), is_date(date).then(|| date.to_string())))
}

/// Rebuild when HEAD moves. A checkout rewrites HEAD; a commit rewrites the
/// branch ref, which is a loose file, a line in `packed-refs` (after `git gc`)
/// or a reftable table. Only existing paths are watched: Cargo reruns a build
/// script on every build while a watched path is missing.
fn watch_head(manifest_dir: &Path) {
    let git_path = |name: &str| {
        git(manifest_dir, &["rev-parse", "--git-path", name]).map(|path| manifest_dir.join(path))
    };
    for name in ["HEAD", "packed-refs", "reftable/tables.list"] {
        if let Some(path) = git_path(name).filter(|path| path.exists()) {
            rerun_if_changed(&path);
        }
    }

    // A packed branch has no loose file until the next commit creates one, so
    // watch the deepest existing directory under refs/ that will hold it. The
    // walk stops at refs/: watching .git itself would rebuild after every
    // `git status` (it rewrites the index).
    let Some(branch) = git(manifest_dir, &["symbolic-ref", "-q", "HEAD"]) else {
        return; // detached HEAD: HEAD itself holds the commit
    };
    let (Some(ref_path), Some(refs_root)) = (git_path(&branch), git_path("refs")) else {
        return;
    };
    let mut candidate = Some(ref_path.as_path());
    while let Some(path) = candidate {
        if path.exists() {
            rerun_if_changed(path);
            return;
        }
        if path == refs_root {
            return;
        }
        candidate = path.parent();
    }
}

fn git(dir: &Path, args: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8(output.stdout).ok()?;
    let text = text.trim();
    (!text.is_empty()).then(|| text.to_string())
}

fn short(sha: &str) -> &str {
    sha.get(..SHORT_HASH_LEN).unwrap_or(sha)
}

/// A full SHA-1 or SHA-256 object name.
fn is_commit_id(value: &str) -> bool {
    matches!(value.len(), 40 | 64) && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// `YYYY-MM-DD`.
fn is_date(value: &str) -> bool {
    matches!(
        value.as_bytes(),
        [y1, y2, y3, y4, b'-', m1, m2, b'-', d1, d2]
            if [y1, y2, y3, y4, m1, m2, d1, d2].iter().all(|byte| byte.is_ascii_digit())
    )
}

fn rerun_if_changed(path: &Path) {
    println!("cargo:rerun-if-changed={}", path.display());
}
