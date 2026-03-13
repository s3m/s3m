use clap::{Arg, Command};

pub fn command() -> Command {
    Command::new("ls")
        .about("List objects and in-progress multipart uploads")
        .after_long_help(
            "Examples:\n  s3m ls s3\n  s3m ls s3/my-bucket\n  s3m ls s3/my-bucket --prefix backups/\n  s3m ls s3/my-bucket --multipart",
        )
        .arg(
            Arg::new("arguments")
                .help("host or host/bucket")
                .long_help("List all buckets for a host, or list the contents of one bucket.\n\nExamples:\n  s3\n  s3/my-bucket")
                .required(true)
                .num_args(1),
        )
        .arg(
            Arg::new("ListMultipartUploads")
                .help("Lists in-progress multipart uploads")
                .long_help("List in-progress multipart uploads instead of normal objects.")
                .long("multipart")
                .short('m')
                .num_args(0),
        )
        .arg(
            Arg::new("prefix")
                .help("Limits the response to keys that begin with the specified prefix")
                .long_help("Only list keys that start with the given prefix.")
                .long("prefix")
                .short('p')
                .num_args(1),
        )
        .arg(
            Arg::new("start-after")
                .help("Starts listing after this specified key")
                .long_help("Start listing after the specified key.")
                .long("start-after")
                .short('a')
                .num_args(1),
        )
        .arg(
            Arg::new("max-kub")
                .help("Limits the number of keys, uploads or buckets returned in the response")
                .long_help("Maximum number of buckets, objects or uploads to return.")
                .long("number")
                .short('n')
                .value_name("NUMBER")
                .value_parser(clap::value_parser!(usize))
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
    fn test_check_multipart() -> Result<()> {
        let cmd = command();
        let m = cmd.try_get_matches_from(vec!["s3m", "test", "--multipart"]);
        assert!(m.is_ok());

        // get matches
        let m = m.unwrap();

        assert!(
            m.get_one::<bool>("ListMultipartUploads")
                .map_or_else(|| false, |v| *v)
        );

        Ok(())
    }

    #[test]
    fn test_check_prefix() -> Result<()> {
        let cmd = command();
        let m = cmd.try_get_matches_from(vec!["s3m", "test", "--prefix", "test"]);
        assert!(m.is_ok());

        // get matches
        let m = m.unwrap();
        assert_eq!(
            m.get_one::<String>("prefix").map(String::as_str),
            Some("test")
        );
        Ok(())
    }

    #[test]
    fn test_check_start_after() -> Result<()> {
        let cmd = command();
        let m = cmd.try_get_matches_from(vec!["s3m", "test", "--start-after", "test"]);
        assert!(m.is_ok());

        // get matches
        let m = m.unwrap();
        assert_eq!(
            m.get_one::<String>("start-after").map(String::as_str),
            Some("test")
        );
        Ok(())
    }

    #[test]
    fn test_check_max_keys() -> Result<()> {
        let cmd = command();
        let m = cmd.try_get_matches_from(vec!["s3m", "test", "--number", "10"]);
        assert!(m.is_ok());

        // get matches
        let m = m.unwrap();
        assert_eq!(m.get_one::<usize>("max-kub").map_or_else(|| 0, |v| *v), 10);
        Ok(())
    }
}
