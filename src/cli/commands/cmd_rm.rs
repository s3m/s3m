use crate::cli::commands::validator_age_filter;
use clap::{Arg, ArgAction, Command};

pub fn command() -> Command {
    Command::new("rm")
        .about("Delete objects, an object prefix, a bucket, or abort a multipart upload")
        .after_long_help(
            "Examples:\n  s3m rm s3/my-bucket/file.dat\n  s3m rm s3/my-bucket/integration/18/ --recursive\n  s3m rm -b s3/empty-bucket\n  s3m rm -b --recursive s3/my-bucket\n  s3m rm s3/my-bucket/file.dat --abort <upload-id>\n\nPrefix deletion (--recursive without --bucket):\n  The key is a literal S3 key prefix, not a directory: 'integration/18/' matches\n  'integration/18/a' and 'integration/18/x/b' (and a key equal to the prefix) but\n  not 'integration/180/a' or 'integration/18'. The trailing slash matters.\n  The prefix must not be empty and the bucket is never deleted.\n  Without --recursive the target is deleted as one exact key.\n  In versioned buckets a delete marker is added and earlier versions are\n  retained; Object Lock protections apply (see --bypass-governance).\n  If any listing or deletion fails the command exits non-zero, and objects\n  already deleted stay deleted.",
        )
        .arg(
            Arg::new("arguments")
                .help("host/bucket/object [host/bucket/object ...] or host/bucket")
                .long_help("Object or bucket target.\n\nExamples:\n  s3/my-bucket/file.dat\n  s3/my-bucket/file-a.dat s3/my-bucket/file-b.dat\n  s3/my-bucket")
                .required(true)
                .num_args(1..),
        )
        .arg(
            Arg::new("UploadId")
                .help("aborts a multipart upload")
                .long_help("Abort an in-progress multipart upload by upload ID.")
                .long("abort")
                .short('a')
                .num_args(1),
        )
        .arg(
            Arg::new("bucket")
                .help("Delete bucket (All objects in the bucket must be deleted before it can be deleted)")
                .long_help("Delete the bucket itself. The bucket must already be empty.")
                .long("bucket")
                .short('b')
                .num_args(0),
        )
        .arg(
            Arg::new("recursive")
                .help("Delete every object under the key prefix, or with --bucket all objects then the bucket")
                .long_help("Without --bucket: delete every object whose key starts with the literal prefix in the target (for example s3/my-bucket/integration/18/), including a key equal to the prefix. The bucket and all other objects are kept. The prefix must not be empty. In versioned buckets this adds delete markers and retains earlier versions.\n\nWith --bucket: delete all objects and versions in the bucket in batches, then the bucket itself.")
                .long("recursive")
                .short('r')
                .conflicts_with("UploadId")
                .conflicts_with("version-id")
                .conflicts_with("older-than")
                .num_args(0),
        )
        .arg(
            Arg::new("version-id")
                .help("Delete a specific object version (Object Lock / versioned buckets)")
                .long_help("Permanently delete a specific object version by id, instead of inserting a delete marker. Single-object deletes only.")
                .long("version-id")
                .conflicts_with("bucket")
                .conflicts_with("UploadId")
                .conflicts_with("older-than")
                .num_args(1),
        )
        .arg(
            Arg::new("bypass-governance")
                .help("Send x-amz-bypass-governance-retention to delete GOVERNANCE-locked versions")
                .long_help("Bypass GOVERNANCE-mode Object Lock retention when deleting (requires the s3:BypassGovernanceRetention permission). Has no effect on COMPLIANCE-locked objects.")
                .long("bypass-governance")
                .conflicts_with("UploadId")
                .action(ArgAction::SetTrue),
        )
        .arg(
            Arg::new("older-than")
                .help("Only delete objects whose LastModified is strictly older than the given duration")
                .long_help("Filter object deletions by age using LastModified.\n\nSupported forms:\n  30d\n  12h\n  45m")
                .long("older-than")
                .value_name("DURATION")
                .value_parser(validator_age_filter())
                .conflicts_with("bucket")
                .conflicts_with("UploadId")
                .num_args(1),
        )
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
    use anyhow::Result;

    #[test]
    fn test_check_arguments() -> Result<()> {
        let cmd = command();
        let m = cmd.try_get_matches_from(vec!["s3m", "test"]);
        assert!(m.is_ok());

        // get matches
        let m = m.unwrap();
        assert_eq!(
            m.get_one::<String>("arguments").map(String::as_str),
            Some("test")
        );
        Ok(())
    }

    #[test]
    fn test_check_abort() -> Result<()> {
        let cmd = command();
        let m = cmd.try_get_matches_from(vec!["s3m", "test", "--abort", "test"]);
        assert!(m.is_ok());

        // get matches
        let m = m.unwrap();
        assert_eq!(
            m.get_one::<String>("UploadId").map(String::as_str),
            Some("test")
        );
        Ok(())
    }

    #[test]
    fn test_check_bucket() -> Result<()> {
        let cmd = command();
        let m = cmd.try_get_matches_from(vec!["s3m", "test", "--bucket"]);
        assert!(m.is_ok());

        // get matches
        let m = m.unwrap();
        assert_eq!(m.get_one::<bool>("bucket").copied(), Some(true));
        Ok(())
    }

    #[test]
    fn test_check_recursive_bucket() -> Result<()> {
        let cmd = command();
        let m = cmd.try_get_matches_from(vec!["s3m", "test", "--bucket", "--recursive"]);
        assert!(m.is_ok());

        let m = m.unwrap();
        assert_eq!(m.get_one::<bool>("recursive").copied(), Some(true));
        Ok(())
    }

    #[test]
    fn test_check_recursive_prefix() {
        let m = command()
            .try_get_matches_from(vec!["s3m", "s3/bucket/integration/18/", "--recursive"])
            .unwrap();
        assert_eq!(m.get_one::<bool>("recursive").copied(), Some(true));
        assert_eq!(m.get_one::<bool>("bucket").copied(), Some(false));
    }

    #[test]
    fn test_check_bucket_flag_combinations() {
        let m = command()
            .try_get_matches_from(vec!["s3m", "-b", "s3/bucket/key"])
            .unwrap();
        assert_eq!(m.get_one::<bool>("bucket").copied(), Some(true));

        let m = command()
            .try_get_matches_from(vec!["s3m", "s3/bucket/key", "-b"])
            .unwrap();
        assert_eq!(m.get_one::<bool>("bucket").copied(), Some(true));

        let m = command()
            .try_get_matches_from(vec!["s3m", "-br", "s3/bucket/key"])
            .unwrap();
        assert_eq!(m.get_one::<bool>("bucket").copied(), Some(true));
        assert_eq!(m.get_one::<bool>("recursive").copied(), Some(true));
    }

    #[test]
    fn test_reject_recursive_with_conflicting_modes() {
        for extra in [
            vec!["--abort", "id"],
            vec!["--version-id", "v1"],
            vec!["--older-than", "30d"],
        ] {
            let mut args = vec!["s3m", "s3/bucket/p/", "--recursive"];
            args.extend(extra);
            assert!(command().try_get_matches_from(args).is_err());
        }
    }

    #[test]
    fn test_check_multiple_arguments() -> Result<()> {
        let cmd = command();
        let m = cmd.try_get_matches_from(vec!["s3m", "a", "b"]);
        assert!(m.is_ok());

        let m = m.unwrap();
        let args: Vec<_> = m
            .get_many::<String>("arguments")
            .unwrap()
            .map(String::as_str)
            .collect();
        assert_eq!(args, vec!["a", "b"]);
        Ok(())
    }

    #[test]
    fn test_check_older_than() -> Result<()> {
        let cmd = command();
        let m = cmd.try_get_matches_from(vec!["s3m", "test", "--older-than", "12h"]);
        assert!(m.is_ok());

        let m = m.unwrap();
        assert_eq!(
            m.get_one::<crate::cli::age_filter::AgeFilter>("older-than")
                .copied()
                .map(crate::cli::age_filter::AgeFilter::duration),
            Some(chrono::Duration::hours(12))
        );
        Ok(())
    }

    #[test]
    fn test_reject_invalid_older_than() {
        let cmd = command();
        let m = cmd.try_get_matches_from(vec!["s3m", "test", "--older-than", "12x"]);
        assert!(m.is_err());
    }

    #[test]
    fn test_reject_negative_older_than() {
        let cmd = command();
        let m = cmd.try_get_matches_from(vec!["s3m", "test", "--older-than=-1d"]);
        assert!(m.is_err());
    }

    #[test]
    fn test_reject_bucket_with_older_than() {
        let cmd = command();
        let m = cmd.try_get_matches_from(vec!["s3m", "test", "--bucket", "--older-than", "30d"]);
        assert!(m.is_err());
    }
}
