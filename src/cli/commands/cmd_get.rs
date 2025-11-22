use clap::{Arg, Command};

pub fn command() -> Command {
    Command::new("get")
        .about("Retrieves objects")
        .arg(
            Arg::new("arguments")
                .help("<s3 provider>/<bucket>/<file> <optional path/file>")
                .required(true)
                .num_args(1..=2),
        )
        .arg(
            Arg::new("metadata")
                .help("Retrieves metadata from an object without returning the object itself")
                .long("meta")
                .short('m')
                .num_args(0),
        )
        .arg(
            Arg::new("quiet")
                .long("quiet")
                .short('q')
                .help("Don't show progress bar")
                .num_args(0),
        )
        .arg(
            Arg::new("force")
                .long("force")
                .short('f')
                .help("Force overwrite")
                .num_args(0),
        )
        .arg(
            Arg::new("versions")
                .long("versions")
                .help("List all versions of an object (path/file will be used as prefix)")
                .num_args(0),
        )
        .arg(
            Arg::new("version")
                .long("version")
                .help("Get a specific version of an object")
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
    fn test_check_arguments_dest() -> Result<()> {
        let cmd = command();
        let m = cmd.try_get_matches_from(vec!["s3m", "s3/bucket/file", "/dest/path/to/file"]);

        assert!(m.is_ok());

        // get matches
        let m = m.unwrap();
        assert_eq!(
            m.get_one::<String>("arguments").map(String::as_str),
            Some("s3/bucket/file")
        );
        let args: Vec<&str> = m
            .get_many::<String>("arguments")
            .unwrap_or_default()
            .map(String::as_str)
            .collect();
        assert_eq!(args, vec!["s3/bucket/file", "/dest/path/to/file"]);

        Ok(())
    }

    #[test]
    fn test_check_metadata() -> Result<()> {
        let cmd = command();
        let m = cmd.try_get_matches_from(vec!["s3m", "test", "--meta"]);
        assert!(m.is_ok());

        // get matches
        let m = m.unwrap();
        assert_eq!(m.get_one::<bool>("metadata").copied(), Some(true));
        Ok(())
    }

    #[test]
    fn test_check_quiet() -> Result<()> {
        let cmd = command();
        let m = cmd.try_get_matches_from(vec!["s3m", "test", "--quiet"]);
        assert!(m.is_ok());

        // get matches
        let m = m.unwrap();
        assert_eq!(m.get_one::<bool>("quiet").copied(), Some(true));
        Ok(())
    }

    #[test]
    fn test_check_force() -> Result<()> {
        let cmd = command();
        let m = cmd.try_get_matches_from(vec!["s3m", "test", "--force"]);
        assert!(m.is_ok());

        // get matches
        let m = m.unwrap();
        assert_eq!(m.get_one::<bool>("force").copied(), Some(true));
        Ok(())
    }

    #[test]
    fn test_check_versions() -> Result<()> {
        let cmd = command();
        let m = cmd.try_get_matches_from(vec!["s3m", "test", "--versions"]);
        assert!(m.is_ok());

        // get matches
        let m = m.unwrap();
        assert_eq!(m.get_one::<bool>("versions").copied(), Some(true));
        Ok(())
    }

    #[test]
    fn test_check_version() -> Result<()> {
        let cmd = command();
        let m = cmd.try_get_matches_from(vec!["s3m", "test", "--version", "1"]);
        assert!(m.is_ok());

        // get matches
        let m = m.unwrap();
        assert_eq!(
            m.get_one::<String>("version").map(String::as_str),
            Some("1")
        );
        Ok(())
    }
}
