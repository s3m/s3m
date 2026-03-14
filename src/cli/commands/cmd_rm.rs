use crate::cli::commands::validator_age_filter;
use clap::{Arg, Command};

pub fn command() -> Command {
    Command::new("rm")
        .about("Delete an object, a bucket, or abort a multipart upload")
        .after_long_help(
            "Examples:\n  s3m rm s3/my-bucket/file.dat\n  s3m rm -b s3/empty-bucket\n  s3m rm -b --recursive s3/my-bucket\n  s3m rm s3/my-bucket/file.dat --abort <upload-id>",
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
                .help("Delete all objects in the bucket before deleting it")
                .long_help("Recursively delete objects from the bucket in batches before deleting the bucket itself.")
                .long("recursive")
                .short('r')
                .requires("bucket")
                .conflicts_with("UploadId")
                .num_args(0),
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
