use clap::{Arg, Command};

pub fn command() -> Command {
    Command::new("ls")
        .about("List objects and in-progress multipart uploads")
        .arg(
            Arg::new("arguments")
                .help("\"host\" to list buckets or \"host/bucket\" to list bucket contents")
                .required(true)
                .num_args(1),
        )
        .arg(
            Arg::new("ListMultipartUploads")
                .help("Lists in-progress multipart uploads")
                .long("multipart")
                .short('m')
                .num_args(0),
        )
        .arg(
            Arg::new("prefix")
                .help("Limits the response to keys that begin with the specified prefix")
                .long("prefix")
                .short('p')
                .num_args(1),
        )
        .arg(
            Arg::new("start-after")
                .help("Starts listing after this specified key")
                .long("start-after")
                .short('a')
                .num_args(1),
        )
}

#[cfg(test)]
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
        assert_eq!(
            m.get_one::<bool>("ListMultipartUploads")
                .map_or_else(|| false, |v| *v),
            true
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
}
