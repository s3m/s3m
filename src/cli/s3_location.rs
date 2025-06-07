use anyhow::{anyhow, Context, Result};
use clap::ArgMatches;
use regex::Regex;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct S3Location {
    pub host: String,
    pub bucket: Option<String>,
    pub key: Option<String>,
}

impl S3Location {
    /// Parse an S3 location string in the format "host/bucket/key"
    fn parse(location: &str, allow_missing_bucket: bool) -> Result<Self> {
        let parts: Vec<&str> = location.splitn(3, '/').collect();

        log::info!("Parts: {:?}", parts);

        let host = parts
            .first()
            .filter(|h| !h.is_empty())
            .ok_or_else(|| anyhow!("Host cannot be empty"))?
            .to_string();

        let bucket = match parts.get(1) {
            Some(b) if !b.is_empty() => {
                Self::validate_bucket_name(b)?;
                Some(b.to_string())
            }

            Some(_) if allow_missing_bucket => None,

            _ => {
                if !allow_missing_bucket {
                    return Err(anyhow!(
                        "Bucket name missing, expected format: <s3 provider>/<bucket name>/key"
                    ));
                } else {
                    None
                }
            }
        };

        let key = parts
            .get(2)
            .map(|k| {
                if k.starts_with('/') {
                    return Err(anyhow!("Please remove leading slashes from key"));
                }

                Self::validate_object_key(k)?;

                if k.is_empty() {
                    return Ok(None);
                }

                Ok(Some(k.to_string()))
            })
            .transpose()?
            .unwrap_or_default();

        Ok(S3Location { host, bucket, key })
    }

    /// Validate S3 bucket name according to AWS specifications
    /// - Length: 3-63 characters
    /// - Pattern: [a-z0-9][\.\-a-z0-9]{1,61}[a-z0-9]
    fn validate_bucket_name(bucket: &str) -> Result<()> {
        if bucket.len() < 3 || bucket.len() > 63 {
            return Err(anyhow!(
                "Invalid bucket name '{}'. Must be 3-63 characters long",
                bucket
            ));
        }

        let bucket_regex =
            Regex::new(r"^[a-z0-9][\.\-a-z0-9]{1,61}[a-z0-9]$").expect("Invalid regex pattern");

        if !bucket_regex.is_match(bucket) {
            return Err(anyhow!(
                "Invalid bucket name '{}'. Must match pattern: [a-z0-9][\\.-a-z0-9]{{1,61}}[a-z0-9]",
                bucket
            ));
        }

        Ok(())
    }

    /// Validate S3 object key according to AWS specifications
    /// - Length: 0-1024 characters
    /// - Pattern: [\P{M}\p{M}]* (Unicode characters, including combining marks)
    fn validate_object_key(key: &str) -> Result<()> {
        if key.len() > 1024 {
            return Err(anyhow!(
                "Object key '{}' is too long. Maximum length is 1024 characters",
                key
            ));
        }

        // AWS allows most Unicode characters in object keys
        // The pattern [\P{M}\p{M}]* means any Unicode character including combining marks
        // We'll be more permissive here and only check for problematic characters

        // Check for null bytes and other control characters that could cause issues
        if key.contains('\0') {
            return Err(anyhow!("Object key cannot contain null bytes"));
        }

        if key.chars().any(|c| c.is_control()) {
            log::warn!("Object key '{key}' contains control characters (including newline, tab, etc.) which may cause issues");
        }

        Ok(())
    }
}

/// Returns the host, bucket and key from the command line arguments
pub fn host_bucket_key(matches: &ArgMatches) -> Result<S3Location> {
    let subcommand = matches.subcommand_name();

    log::debug!("Subcommand: {:?}", subcommand);

    match subcommand {
        Some(cmd @ ("acl" | "get" | "ls" | "cb" | "rm" | "share")) => {
            parse_subcommand_args(matches, cmd)
        }
        _ => parse_put_object_args(matches),
    }
}

fn parse_subcommand_args(matches: &ArgMatches, subcommand: &str) -> Result<S3Location> {
    log::info!("Processing subcommand: {}", subcommand);

    let args = get_subcommand_arguments(matches, subcommand)?;

    log::info!("Arguments for subcommand '{}': {:?}", subcommand, args);

    let s3_location = args
        .first()
        .ok_or_else(|| anyhow!("Missing S3 location argument"))?;

    log::info!("Parsed S3 location: {}", s3_location);

    // For 'ls' command, allow missing bucket (for listing buckets)
    let allow_missing_bucket = subcommand == "ls";

    log::info!("Allow missing bucket: {}", allow_missing_bucket);

    S3Location::parse(s3_location, allow_missing_bucket)
}

fn parse_put_object_args(matches: &ArgMatches) -> Result<S3Location> {
    let args = get_main_arguments(matches)?;

    let s3_location = match args.len() {
        2 => {
            // Format: s3m /path/to/file host/bucket/key
            args[1]
        }
        1 if matches.contains_id("pipe") => {
            // Format: s3m host/bucket/key (with --pipe)
            args[0]
        }
        _ => return Err(anyhow!("Invalid arguments. Expected format: '/path/to/file <s3 provider>/<bucket>/<file>' or use --pipe for standard input"))

    };

    log::info!("Parsed S3 location for put object: {}", s3_location);

    S3Location::parse(s3_location, false)
}

fn get_subcommand_arguments<'a>(
    matches: &'a ArgMatches,
    subcommand: &'a str,
) -> Result<Vec<&'a str>> {
    let subcommand_matches = matches
        .subcommand_matches(subcommand)
        .context("Subcommand arguments missing")?;

    Ok(subcommand_matches
        .get_many::<String>("arguments")
        .unwrap_or_default()
        .map(String::as_str)
        .collect())
}

fn get_main_arguments(matches: &ArgMatches) -> Result<Vec<&str>> {
    Ok(matches
        .get_many::<String>("arguments")
        .unwrap_or_default()
        .map(String::as_str)
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_s3_location_parse_valid() {
        let s3_location =
            S3Location::parse("s3.amazonaws.com/my-bucket/path/to/file.txt", false).unwrap();
        assert_eq!(s3_location.host, "s3.amazonaws.com");
        assert_eq!(s3_location.bucket, Some("my-bucket".to_string()));
        assert_eq!(s3_location.key, Some("path/to/file.txt".to_string()));
    }

    #[test]
    fn test_s3_location_parse_no_key() {
        let s3_location = S3Location::parse("s3.amazonaws.com/my-bucket", false).unwrap();
        assert_eq!(s3_location.host, "s3.amazonaws.com");
        assert_eq!(s3_location.bucket, Some("my-bucket".to_string()));
        assert_eq!(s3_location.key, None);
    }

    #[test]
    fn test_s3_location_parse_ls_no_bucket() {
        let s3_location = S3Location::parse("s3.amazonaws.com", true).unwrap();
        assert_eq!(s3_location.host, "s3.amazonaws.com");
        assert_eq!(s3_location.bucket, None);
        assert_eq!(s3_location.key, None);
    }

    #[test]
    fn test_s3_location_parse_invalid_leading_slash() {
        let result = S3Location::parse("s3.amazonaws.com/my-bucket//file.txt", false);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("leading slashes"));
    }

    #[test]
    fn test_s3_location_parse_missing_bucket() {
        let result = S3Location::parse("s3.amazonaws.com", false);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("bucket"));
    }

    #[test]
    fn test_validate_bucket_name_valid() {
        assert!(S3Location::validate_bucket_name("my-bucket").is_ok());
        assert!(S3Location::validate_bucket_name("bucket123").is_ok());
        assert!(S3Location::validate_bucket_name("my.bucket.name").is_ok());
        assert!(S3Location::validate_bucket_name("a1b").is_ok()); // minimum length
        assert!(S3Location::validate_bucket_name("my..bucket").is_ok()); // consecutive dots allowed per AWS spec
    }

    #[test]
    fn test_validate_bucket_name_invalid() {
        // Too short
        assert!(S3Location::validate_bucket_name("ab").is_err());

        // Too long
        let long_name = "a".repeat(64);
        assert!(S3Location::validate_bucket_name(&long_name).is_err());

        // Invalid characters
        assert!(S3Location::validate_bucket_name("My-Bucket").is_err()); // uppercase
        assert!(S3Location::validate_bucket_name("bucket_name").is_err()); // underscore

        // Invalid start/end (must be alphanumeric)
        assert!(S3Location::validate_bucket_name("-bucket").is_err()); // starts with hyphen
        assert!(S3Location::validate_bucket_name("bucket-").is_err()); // ends with hyphen
        assert!(S3Location::validate_bucket_name(".bucket").is_err()); // starts with dot
        assert!(S3Location::validate_bucket_name("bucket.").is_err()); // ends with dot
    }

    #[test]
    fn test_validate_object_key_valid() {
        assert!(S3Location::validate_object_key("").is_ok()); // empty key
        assert!(S3Location::validate_object_key("simple-file.txt").is_ok());
        assert!(S3Location::validate_object_key("path/to/file.txt").is_ok());
        assert!(S3Location::validate_object_key("файл.txt").is_ok()); // Unicode
        assert!(S3Location::validate_object_key("file with spaces.txt").is_ok());
    }

    #[test]
    fn test_validate_object_key_invalid() {
        // Too long
        let long_key = "a".repeat(1025);
        assert!(S3Location::validate_object_key(&long_key).is_err());

        // Null byte
        assert!(S3Location::validate_object_key("file\0name").is_err());
    }

    #[test]
    fn test_bucket_validation_in_parse() {
        // Valid bucket should work
        assert!(S3Location::parse("s3.com/valid-bucket/key", false).is_ok());

        // Invalid bucket should fail
        let result = S3Location::parse("s3.com/INVALID-BUCKET/key", false);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid bucket name"));
    }
}
