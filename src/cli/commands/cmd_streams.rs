use clap::{Arg, Command};

pub fn command() -> Command {
    Command::new("streams")
        .about("Inspect and manage local multipart stream state")
        .after_long_help(
            "Examples:\n  s3m streams\n  s3m streams ls\n  s3m streams show <id>\n  s3m streams resume <id>\n  s3m streams clean",
        )
        .arg(
            Arg::new("json")
                .help("Emit machine-readable JSON output for listing, show, or clean")
                .long("json")
                .global(true)
                .num_args(0),
        )
        .subcommand(
            Command::new("ls")
                .about("List local multipart stream state")
                .after_long_help("Example:\n  s3m streams ls"),
        )
        .subcommand(
            Command::new("show")
                .about("Show one local multipart stream state entry")
                .arg(Arg::new("id").required(true).num_args(1)),
        )
        .subcommand(
            Command::new("resume")
                .about("Resume a local multipart upload from saved state")
                .arg(Arg::new("id").required(true).num_args(1)),
        )
        .subcommand(
            Command::new("clean")
                .about("Remove broken or completed local multipart stream state")
                .after_long_help("Example:\n  s3m streams clean"),
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
    fn test_streams_show_id() -> Result<()> {
        let cmd = command();
        let matches = cmd.try_get_matches_from(vec!["streams", "show", "abc"]);
        assert!(matches.is_ok());

        let matches = matches.unwrap();
        let sub = matches.subcommand_matches("show").unwrap();
        assert_eq!(sub.get_one::<String>("id").map(String::as_str), Some("abc"));
        Ok(())
    }

    #[test]
    fn test_streams_resume_id() -> Result<()> {
        let cmd = command();
        let matches = cmd.try_get_matches_from(vec!["streams", "resume", "abc"]);
        assert!(matches.is_ok());

        let matches = matches.unwrap();
        let sub = matches.subcommand_matches("resume").unwrap();
        assert_eq!(sub.get_one::<String>("id").map(String::as_str), Some("abc"));
        Ok(())
    }

    #[test]
    fn test_streams_ls_default_subcommand_not_required() -> Result<()> {
        let cmd = command();
        let matches = cmd.try_get_matches_from(vec!["streams"]);
        assert!(matches.is_ok());
        Ok(())
    }

    #[test]
    fn test_streams_json_flag() -> Result<()> {
        let cmd = command();
        let matches = cmd.try_get_matches_from(vec!["streams", "ls", "--json"])?;
        assert_eq!(matches.get_one::<bool>("json").copied(), Some(true));
        Ok(())
    }
}
