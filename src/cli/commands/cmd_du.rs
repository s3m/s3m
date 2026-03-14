use clap::{Arg, Command};

pub fn command() -> Command {
    Command::new("du")
        .about("Summarize object count and total size for a bucket or prefix")
        .after_long_help(
            "Examples:\n  s3m du s3/my-bucket\n  s3m du s3/my-bucket/backups/\n  s3m du s3/my-bucket/backups/2026/03/",
        )
        .arg(
            Arg::new("arguments")
                .help("host/bucket or host/bucket/prefix")
                .long_help(
                    "Summarize usage for a whole bucket or one key prefix.\n\nExamples:\n  s3/my-bucket\n  s3/my-bucket/backups/\n  s3/my-bucket/backups/2026/03/",
                )
                .required(true)
                .num_args(1),
        )
        .arg(
            Arg::new("group-by")
                .long("group-by")
                .help("Group usage output by time period")
                .long_help("Group usage output by a supported time period.\n\nSupported values:\n  day")
                .value_parser(["day"])
                .num_args(1),
        )
        .arg(
            Arg::new("json")
                .help("Emit machine-readable JSON output")
                .long("json")
                .num_args(0),
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
    fn test_check_bucket_argument() -> Result<()> {
        let cmd = command();
        let m = cmd.try_get_matches_from(vec!["s3m", "s3/my-bucket"])?;
        assert_eq!(
            m.get_one::<String>("arguments").map(String::as_str),
            Some("s3/my-bucket")
        );
        Ok(())
    }

    #[test]
    fn test_check_prefix_argument() -> Result<()> {
        let cmd = command();
        let m = cmd.try_get_matches_from(vec!["s3m", "s3/my-bucket/prefix"])?;
        assert_eq!(
            m.get_one::<String>("arguments").map(String::as_str),
            Some("s3/my-bucket/prefix")
        );
        Ok(())
    }

    #[test]
    fn test_check_group_by_day() -> Result<()> {
        let cmd = command();
        let m = cmd.try_get_matches_from(vec!["s3m", "s3/my-bucket", "--group-by", "day"])?;
        assert_eq!(
            m.get_one::<String>("group-by").map(String::as_str),
            Some("day")
        );
        Ok(())
    }

    #[test]
    fn test_check_json() -> Result<()> {
        let cmd = command();
        let m = cmd.try_get_matches_from(vec!["s3m", "s3/my-bucket", "--json"])?;
        assert_eq!(m.get_one::<bool>("json").copied(), Some(true));
        Ok(())
    }
}
