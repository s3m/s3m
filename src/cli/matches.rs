use anyhow::{anyhow, Context, Result};
use clap::ArgMatches;

/// returns the host, bucket and path from the arguments
pub fn host_bucket_path(matches: &ArgMatches) -> Result<Vec<&str>> {
    // Host, Bucket, Path
    let hbp: Vec<&str>;

    let parse_args = |subcommand| -> Result<Vec<&str>> {
        log::debug!("subcommand: {}", subcommand);

        let args: Vec<&str> = matches
            .subcommand_matches(subcommand)
            .context("arguments missing")?
            .get_many::<String>("arguments")
            .unwrap_or_default()
            .map(String::as_str)
            .collect();

        let v: Vec<&str> = args[0].splitn(3, '/').collect();

        if subcommand == "ls" {
            return Ok(v);
        }

        // check bucket
        if v.get(1).is_none() {
            return Err(anyhow!(
                "No \"bucket\" found, try: <s3 provider>/<bucket name>/path"
            ));
        }

        // build hbp
        let mut hbp = vec![v[0], v[1]];

        if v.len() > 2 {
            // check path
            if v[2].starts_with('/') {
                return Err(anyhow!("Please remove leading slashes from path."));
            }

            // Split v[2] by '/' and collect into a vector of &str
            let split_v2: Vec<&str> = v[2].split('/').collect();

            // Extend hbp with the split_v2 vector
            hbp.extend(split_v2);
        }

        Ok(hbp)
    };

    match matches.subcommand_name() {
        // ACL
        Some("acl") => {
            hbp = parse_args("acl")?;
        }

        // GetObject
        Some("get") => {
            hbp = parse_args("get")?;
        }

        // ListObjects
        Some("ls") => {
            hbp = parse_args("ls")?;
        }

        // CreateBucket
        Some("cb") => {
            hbp = parse_args("cb")?;
        }

        // DeleteObject
        Some("rm") => {
            hbp = parse_args("rm")?;
        }

        // ShareObject
        Some("share") => {
            hbp = parse_args("share")?;
        }

        // PutObject
        _ => {
            let args: Vec<&str> = matches
                .get_many::<String>("arguments")
                .unwrap_or_default()
                .map(String::as_str)
                .collect();

            if args.len() == 2 {
                // When not using pipe the format is:
                // s3m /path/to/file host/bucket/file
                hbp = args[1].split('/').collect();
            } else if matches.contains_id("pipe") {
                hbp = args[0].split('/').collect();
            } else {
                return Err(anyhow!(
                "missing argument or use --pipe for standard input. For more information try: --help"
            ));
            }
        }
    }

    Ok(hbp)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::commands::new;
    use anyhow::Result;
    use std::{fs::File, io::Write, path::PathBuf};

    use tempfile::Builder;

    const CONF: &str = r#"---
hosts:
  s3:
    region: xx-region-y
    access_key: XXX
    secret_key: YYY
    bucket: my-bucket"#;

    fn get_config() -> Result<PathBuf> {
        let tmp_dir = Builder::new().prefix("test-s3m-").tempdir()?;
        let config_path = tmp_dir.path().join("config.yml");
        let mut config = File::create(&config_path)?;
        config.write_all(CONF.as_bytes())?;
        Ok(tmp_dir.into_path())
    }

    struct Test {
        args: Vec<&'static str>,
        hbp: Vec<&'static str>,
    }

    #[test]
    fn test_matches_3_arguments() {
        let config = get_config().unwrap();
        let cmd = new(&config);
        let m = cmd.try_get_matches_from(vec!["s3m", "foo", "bar", "baz"]);
        assert!(m.is_err());
    }

    #[test]
    fn test_matches_xxx() {
        let tests = [
            Test {
                args: vec!["s3m", "host"],
                hbp: vec!["host"],
            },
            Test {
                args: vec!["s3m", "host/bucket"],
                hbp: vec!["host", "bucket"],
            },
            Test {
                args: vec!["s3m", "foo", "bar"],
                hbp: vec!["bar"],
            },
            Test {
                args: vec!["s3m", "/path/to/file", "host/bucket/file"],
                hbp: vec!["host", "bucket", "file"],
            },
            Test {
                args: vec!["s3m", "foo", "host/bucket"],
                hbp: vec!["host", "bucket"],
            },
            Test {
                args: vec!["s3m", "foo", "host/bucket/file"],
                hbp: vec!["host", "bucket", "file"],
            },
            Test {
                args: vec!["s3m", "~/path/to/file", "host/bucket/file"],
                hbp: vec!["host", "bucket", "file"],
            },
            Test {
                args: vec!["s3m", "~/path/to/file", "host/bucket/file/a/b/c.txt"],
                hbp: vec!["host", "bucket", "file", "a", "b", "c.txt"],
            },
            Test {
                args: vec!["s3m", "pipe", "host/bucket"],
                hbp: vec!["host", "bucket"],
            },
            Test {
                args: vec!["s3m", "pipe", "host/bucket/file/a/b/c.txt"],
                hbp: vec!["host", "bucket", "file", "a", "b", "c.txt"],
            },
            Test {
                args: vec!["s3m", "pipe", "host/bucket/file/"],
                hbp: vec!["host", "bucket", "file", ""],
            },
            Test {
                args: vec!["s3m", "pipe", "host/bucket/file//"],
                hbp: vec!["host", "bucket", "file", "", ""],
            },
            Test {
                args: vec!["s3m", "--pipe", "host/bucket/file/a/b/c.txt"],
                hbp: vec!["host", "bucket", "file", "a", "b", "c.txt"],
            },
            Test {
                args: vec!["s3m", "--pipe", "host/bucket/file//"],
                hbp: vec!["host", "bucket", "file", "", ""],
            },
            Test {
                args: vec!["s3m", "acl", "host/bucket/file"],
                hbp: vec!["host", "bucket", "file"],
            },
            Test {
                args: vec!["s3m", "get", "host/bucket/file"],
                hbp: vec!["host", "bucket", "file"],
            },
            Test {
                args: vec!["s3m", "ls", "host"],
                hbp: vec!["host"],
            },
            Test {
                args: vec!["s3m", "ls", "host/bucket"],
                hbp: vec!["host", "bucket"],
            },
            Test {
                args: vec!["s3m", "ls", "host/bucket/file"],
                hbp: vec!["host", "bucket", "file"],
            },
            Test {
                args: vec!["s3m", "cb", "host/bucket/file"],
                hbp: vec!["host", "bucket", "file"],
            },
            Test {
                args: vec!["s3m", "rm", "host/bucket/file"],
                hbp: vec!["host", "bucket", "file"],
            },
            Test {
                args: vec!["s3m", "rm", "host/bucket"],
                hbp: vec!["host", "bucket"],
            },
            Test {
                args: vec!["s3m", "share", "host/bucket/file"],
                hbp: vec!["host", "bucket", "file"],
            },
            Test {
                args: vec!["s3m", "rm", "host/bucket/file/"],
                hbp: vec!["host", "bucket", "file", ""],
            },
            Test {
                args: vec!["s3m", "rm", "host/bucket/file//"],
                hbp: vec!["host", "bucket", "file", "", ""],
            },
        ];
        for test in tests.iter() {
            let config = get_config().unwrap();
            let cmd = new(&config);
            let m = cmd.try_get_matches_from(test.args.clone());
            assert!(m.is_ok());

            let m = m.unwrap();
            let hbp = host_bucket_path(&m).unwrap();
            assert_eq!(hbp, test.hbp);
        }
    }

    #[test]
    fn test_matches_pipe() {
        let config = get_config().unwrap();
        let cmd = new(&config);
        let m = cmd.try_get_matches_from(vec!["s3m", "--pipe", "host/bucket/file"]);
        assert!(m.is_ok());
        let m = m.unwrap();
        let hbp = host_bucket_path(&m).unwrap();
        assert_eq!(hbp, vec!["host", "bucket", "file"]);
    }

    #[test]
    fn test_matches_args_missing() {
        let config = get_config().unwrap();
        let cmd = new(&config);
        let m = cmd.try_get_matches_from(vec!["s3m", "--pipe"]);
        assert!(m.is_err());
    }

    #[test]
    fn test_matches_args_missing_2() {
        let config = get_config().unwrap();
        let cmd = new(&config);
        let m = cmd.try_get_matches_from(vec!["s3m", "host/bucket/file"]);
        assert!(m.is_ok());
        let m = m.unwrap();
        let hbp = host_bucket_path(&m).unwrap();
        assert_eq!(hbp, vec!["host", "bucket", "file"]);
    }
}
