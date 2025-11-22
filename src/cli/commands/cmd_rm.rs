use clap::{Arg, Command};

pub fn command() -> Command {
    Command::new("rm")
        .about("Delete objects, bucket (-b) and aborts a multipart upload")
        .arg(
            Arg::new("arguments")
                .help("<s3 provider>/<bucket>/<file>")
                .required(true)
                .num_args(1),
        )
        .arg(
            Arg::new("UploadId")
                .help("aborts a multipart upload")
                .long("abort")
                .short('a')
                .num_args(1),
        )
        .arg(
            Arg::new("bucket")
                .help("Delete bucket (All objects in the bucket must be deleted before it can be deleted)")
                .long("bucket")
                .short('b')
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
}
