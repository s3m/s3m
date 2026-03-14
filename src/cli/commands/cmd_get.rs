use clap::{Arg, Command};

pub fn command() -> Command {
    Command::new("get")
        .about("Download an object or show its metadata")
        .after_long_help(
            "Examples:\n  s3m get s3/my-bucket/file.dat\n  s3m get s3/my-bucket/file.dat /tmp/file.dat\n  s3m get s3/my-bucket/file.dat --meta",
        )
        .arg(
            Arg::new("arguments")
                .help("host/bucket/object <optional local path>")
                .long_help("Object to download.\n\nSyntax:\n  host/bucket/object [local path]\n\nExamples:\n  s3/my-bucket/file.dat\n  s3/my-bucket/file.dat /tmp/file.dat")
                .required(true)
                .num_args(1..=2),
        )
        .arg(
            Arg::new("metadata")
                .help("Retrieves metadata from an object without returning the object itself")
                .long_help("Return object metadata only, without downloading the object body.")
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
                .long_help("Overwrite the local destination if it already exists.")
                .num_args(0),
        )
        .arg(
            Arg::new("versions")
                .long("versions")
                .help("List all versions of an object (path/file will be used as prefix)")
                .long_help("List all known versions for the object key prefix instead of downloading the object.")
                .num_args(0),
        )
        .arg(
            Arg::new("version")
                .long("version")
                .help("Get a specific version of an object")
                .long_help("Download a specific object version by version ID.")
                .num_args(1),
        )
        .arg(
            Arg::new("json")
                .help("Emit machine-readable JSON output for metadata or version listing")
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

    #[test]
    fn test_check_json() -> Result<()> {
        let cmd = command();
        let m = cmd.try_get_matches_from(vec!["s3m", "test", "--meta", "--json"])?;
        assert_eq!(m.get_one::<bool>("json").copied(), Some(true));
        Ok(())
    }
}
