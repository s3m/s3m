use crate::cli::commands::validator_is_num;
use clap::{Arg, Command};

pub fn command() -> Command {
    Command::new("share")
        .about("Share object using a presigned URL")
        .after_long_help(
            "Examples:\n  s3m share s3/my-bucket/file.dat\n  s3m share s3/my-bucket/file.dat --expire 3600",
        )
        .arg(
            Arg::new("arguments")
                .help("host/bucket/object")
                .long_help("Object to share.\n\nExample:\n  s3/my-bucket/file.dat")
                .required(true)
                .num_args(1),
        )
        .arg(
            Arg::new("expire")
                .help("Time period in seconds, max value 604800 (seven days)")
                .long_help("Presigned URL expiration in seconds.\n\nMaximum: 604800 (7 days). Default: 43200 (12 hours).")
                .long("expire")
                .short('e')
                .default_value("43200")
                .num_args(1)
                .value_parser(validator_is_num()),
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
    fn test_check_expire() -> Result<()> {
        let cmd = command();
        let m = cmd.try_get_matches_from(vec!["s3m", "test", "--expire", "604800"]);
        assert!(m.is_ok());

        // get matches
        let m = m.unwrap();
        assert_eq!(
            m.get_one::<usize>("expire").map_or_else(|| 0, |v| *v),
            604_800_usize
        );
        Ok(())
    }

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
}
