use crate::s3::Region;
use anyhow::{Context, Result};
use secrecy::SecretString;
use serde::{Deserialize, Deserializer, de::Error};
use std::{collections::BTreeMap, fs::File, path::PathBuf};

const fn default_monitor_age() -> u64 {
    86_400
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum MonitorAgeValue {
    Integer(u64),
    Text(String),
}

fn parse_monitor_age_text(value: &str) -> std::result::Result<u64, String> {
    if let Ok(seconds) = value.parse::<u64>() {
        return Ok(seconds);
    }

    let unit = value
        .chars()
        .last()
        .ok_or_else(|| "monitor age can not be empty".to_string())?;
    let amount = value
        .get(..value.len().saturating_sub(1))
        .ok_or_else(|| format!("Invalid monitor age '{value}'"))?;

    let multiplier = match unit {
        's' => 1,
        'm' => 60,
        'h' => 3_600,
        'd' => 86_400,
        _ => {
            return Err(format!(
                "Invalid monitor age '{value}'. Expected seconds or a suffix: s, m, h, d"
            ));
        }
    };

    let amount = amount.parse::<u64>().map_err(|_| {
        format!("Invalid monitor age '{value}'. Expected an integer followed by s, m, h, or d")
    })?;

    amount
        .checked_mul(multiplier)
        .ok_or_else(|| format!("Invalid monitor age '{value}'. Value is too large"))
}

// An empty value (`prefix:`), `~` or `null` is a YAML null; treat it like an
// omitted field. Quote the value (`"null"`) to keep it as a literal string.
fn deserialize_null_as_default<'de, D, T>(deserializer: D) -> std::result::Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Default + Deserialize<'de>,
{
    Ok(Option::<T>::deserialize(deserializer)?.unwrap_or_default())
}

fn deserialize_monitor_age<'de, D>(deserializer: D) -> std::result::Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    match MonitorAgeValue::deserialize(deserializer)? {
        MonitorAgeValue::Integer(seconds) => Ok(seconds),
        MonitorAgeValue::Text(value) => parse_monitor_age_text(&value).map_err(D::Error::custom),
    }
}

// Name the failing field (`hosts.s3.compress: ...`) and replace serde-saphyr's
// duplicate-key hint, which points at an option the CLI does not expose.
fn config_parse_error(err: &serde_saphyr::Error, path: &str) -> anyhow::Error {
    match err {
        serde_saphyr::Error::DuplicateMappingKey { key, location } => {
            let key = key
                .as_deref()
                .map(|k| format!(" `{k}`"))
                .unwrap_or_default();
            anyhow::anyhow!(
                "duplicate key{key} at line {}, column {}",
                location.line(),
                location.column()
            )
        }
        _ if path.is_empty() || path == "." => anyhow::anyhow!("{err}"),
        _ => anyhow::anyhow!("{path}: {err}"),
    }
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub hosts: BTreeMap<String, Host>,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
pub struct MonitorRule {
    // Optional: when omitted (or empty) the rule scans the whole bucket.
    #[serde(default, deserialize_with = "deserialize_null_as_default")]
    pub prefix: String,

    #[serde(default, deserialize_with = "deserialize_null_as_default")]
    pub suffix: String,

    #[serde(
        default = "default_monitor_age",
        deserialize_with = "deserialize_monitor_age"
    )]
    pub age: u64,

    #[serde(default)]
    pub size: u64,
}

#[derive(Debug, Deserialize)]
pub struct Host {
    pub endpoint: Option<String>,
    pub region: Option<String>,

    #[serde(default, deserialize_with = "deserialize_null_as_default")]
    pub access_key: String,

    #[serde(default, deserialize_with = "deserialize_null_as_default")]
    pub secret_key: SecretString,

    pub bucket: Option<String>,

    #[serde(default)]
    pub buckets: BTreeMap<String, Vec<MonitorRule>>,

    pub enc_key: Option<String>,
    pub compress: Option<bool>,
}

impl Config {
    /// Create a new config from a config.yml file
    /// # Errors
    /// Will return an error if the config file is not found or if the config
    /// cannot be parsed
    pub fn new(config_path: PathBuf) -> Result<Self> {
        let file = File::open(config_path)?;

        // No source snippets: they would echo config lines, secret_key included.
        let options = serde_saphyr::options! { with_snippet: false };
        let mut path = String::new();
        let config: Self =
            serde_saphyr::with_deserializer_from_reader_with_options(file, options, |de| {
                serde_path_to_error::deserialize(de).map_err(|err| {
                    path = err.path().to_string();
                    err.into_inner()
                })
            })
            .map_err(|err| config_parse_error(&err, &path))
            .context("unable to parse config file")?;

        Ok(config)
    }

    /// Get the host from the config.yml
    /// # Errors
    /// Will return an error if the host is not found
    pub fn get_host(&self, name: &str) -> Result<&Host> {
        self.hosts.get(name).with_context(|| name.to_string())
    }
}

impl Host {
    /// Get the region for the host
    /// # Errors
    /// Will return an error if the region is not found
    pub fn get_region(&self) -> Result<Region> {
        match (&self.endpoint, &self.region) {
            (Some(endpoint), Some(region)) => Ok(Region::custom(region.clone(), endpoint.clone())),
            (Some(endpoint), None) => Ok(Region::Custom {
                name: String::new(),
                endpoint: endpoint.clone(),
            }),
            (None, Some(region)) => Ok(region.parse::<Region>()?),
            (None, None) => Err(anyhow::anyhow!(
                "could not parse host need an endpoint or region"
            )),
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::unnecessary_wraps,
    clippy::too_many_lines,
    clippy::format_push_string
)]
mod tests {
    use super::*;
    use crate::s3::Region;
    use secrecy::ExposeSecret;
    use std::io::Write;
    use std::str::FromStr;
    use tempfile::NamedTempFile;

    const CONF: &str = r"---
hosts:
  s3:
    region: xx-region-y.foo
    access_key: XXX
    secret_key: YYY
    bucket: my-bucket";

    const CONF_AWS: &str = r"---
hosts:
  s3:
    region: eu-central-2
    access_key: XXX
    secret_key: YYY
    bucket: my-bucket";

    const CONF_OTHER: &str = r"---
hosts:
  s3:
    endpoint: s3.us-west-000.backblazeb2.com
    access_key: XXX
    secret_key: YYY
    bucket: my-bucket";

    const CONF_NO_REGION: &str = r"---
hosts:
  s3:
    access_key: XXX
    secret_key: YYY
    bucket: my-bucket";

    const CONF_X_REGION: &str = r"---
hosts:
  s3:
    region: x
    access_key: XXX
    secret_key: YYY
    bucket: my-bucket";

    const CONF_ENDPOINT_AND_REGION: &str = r"---
hosts:
  s3:
    endpoint: https://minio.example.com
    region: us-east-1
    access_key: XXX
    secret_key: YYY
    bucket: my-bucket";

    const CONF_COMPRESS: &str = r"---
hosts:
  s3:
    region: us-east-2
    access_key: XXX
    secret_ke: YYY
    compress: true";

    const CONF_ENCRYPT: &str = r"---
hosts:
  s3:
    region: us-east-2
    access_key: XXX
    secret_ke: YYY
    compress: true
    enc_key: secret";

    const CONF_MONITOR: &str = r"---
hosts:
  s3:
    endpoint: https://s3.example.test
    access_key: XXX
    secret_key: YYY
    buckets:
      bucket_A:
        - prefix: backups/daily
          suffix: .log
          age: 1d
          size: 30720
      bucket_B:
        - prefix: foo
          age: 12h
          size: 1024
        - prefix: path/to/logs/
          age: 43200";

    const CONF_MONITOR_DEFAULTS: &str = r"---
hosts:
  s3:
    endpoint: https://s3.example.test
    access_key: XXX
    secret_key: YYY
    buckets:
      bucket_A:
        - prefix: backups/daily";

    const CONF_MONITOR_NO_PREFIX: &str = r"---
hosts:
  s3:
    endpoint: https://s3.example.test
    access_key: XXX
    secret_key: YYY
    buckets:
      bucket_A:
        - suffix: .sql
          age: 12h";

    const CONF_MONITOR_EMPTY_VALUES: &str = r#"---
hosts:
  s3:
    endpoint: https://s3.example.test
    access_key:
    secret_key: ~
    buckets:
      bucket_A:
        - prefix:
          suffix: null
          age: 1h
        - prefix: "~"
          suffix: "null""#;

    #[test]
    fn test_config_get_host() {
        let mut tmp_file = NamedTempFile::new().unwrap();
        tmp_file.write_all(CONF.as_bytes()).unwrap();
        let c = Config::new(tmp_file.into_temp_path().to_path_buf());
        assert!(c.is_ok());
        let c = c.unwrap();
        assert_eq!(c.hosts.len(), 1);
        assert_eq!(c.hosts.get("s3").unwrap().access_key, "XXX");
        assert_eq!(c.hosts.get("s3").unwrap().secret_key.expose_secret(), "YYY");
        assert_eq!(
            c.hosts.get("s3").unwrap().bucket,
            Some("my-bucket".to_string())
        );
        assert_eq!(
            c.hosts.get("s3").unwrap().region,
            Some("xx-region-y.foo".to_string())
        );
    }

    #[test]
    fn test_config_get_host_missing() {
        let mut tmp_file = NamedTempFile::new().unwrap();
        tmp_file.write_all(CONF.as_bytes()).unwrap();
        let c = Config::new(tmp_file.into_temp_path().to_path_buf());
        assert!(c.is_ok());
        let c = c.unwrap();
        assert!(c.get_host("foo").is_err());
    }

    #[test]
    fn test_config_get_region_missing() {
        let mut tmp_file = NamedTempFile::new().unwrap();
        tmp_file.write_all(CONF_NO_REGION.as_bytes()).unwrap();
        let c = Config::new(tmp_file.into_temp_path().to_path_buf());
        assert!(c.is_ok());
        let c = c.unwrap();
        let h = c.get_host("s3");
        assert!(h.is_ok());
        let h = h.unwrap();
        let r = h.get_region();
        println!("{r:?}");
    }

    #[test]
    fn test_config_get_aws_region() {
        let mut tmp_file = NamedTempFile::new().unwrap();
        tmp_file.write_all(CONF_AWS.as_bytes()).unwrap();
        let c = Config::new(tmp_file.into_temp_path().to_path_buf());
        assert!(c.is_ok());
        let c = c.unwrap();
        let h = c.get_host("s3");
        assert!(h.is_ok());
        let h = h.unwrap();
        let r = h.get_region();
        assert!(r.is_ok());
        let r = r.unwrap();
        assert_eq!(r, Region::aws("eu-central-2"));
    }

    #[test]
    fn test_config_get_custom_aws_region() {
        let mut tmp_file = NamedTempFile::new().unwrap();
        tmp_file.write_all(CONF.as_bytes()).unwrap();
        let c = Config::new(tmp_file.into_temp_path().to_path_buf());
        assert!(c.is_ok());
        let c = c.unwrap();
        let h = c.get_host("s3");
        assert!(h.is_ok());
        let h = h.unwrap();
        let r = h.get_region().unwrap();
        assert_eq!(
            r,
            Region::Custom {
                name: "xx-region-y".to_string(),
                endpoint: "xx-region-y.foo".to_string(),
            },
        );
    }

    #[test]
    fn test_config_get_custom_region() {
        let mut tmp_file = NamedTempFile::new().unwrap();
        tmp_file.write_all(CONF_OTHER.as_bytes()).unwrap();
        let c = Config::new(tmp_file.into_temp_path().to_path_buf());
        assert!(c.is_ok());
        let c = c.unwrap();
        let h = c.get_host("s3");
        assert!(h.is_ok());
        let h = h.unwrap();
        let r = h.get_region();
        assert!(r.is_ok());
        let r = r.unwrap();
        assert_eq!(
            r,
            Region::Custom {
                name: String::new(),
                endpoint: "s3.us-west-000.backblazeb2.com".to_string()
            }
        );
    }

    #[test]
    fn test_config_get_bad_region() {
        let mut tmp_file = NamedTempFile::new().unwrap();
        tmp_file.write_all(CONF_X_REGION.as_bytes()).unwrap();
        let c = Config::new(tmp_file.into_temp_path().to_path_buf());
        assert!(c.is_ok());
        let c = c.unwrap();
        let h = c.get_host("s3");
        assert!(h.is_ok());
        let h = h.unwrap();
        let r = h.get_region();
        assert!(r.is_err());
    }

    #[test]
    fn test_config_get_endpoint_and_region_prefers_custom_endpoint() {
        let mut tmp_file = NamedTempFile::new().unwrap();
        tmp_file
            .write_all(CONF_ENDPOINT_AND_REGION.as_bytes())
            .unwrap();
        let c = Config::new(tmp_file.into_temp_path().to_path_buf()).unwrap();
        let h = c.get_host("s3").unwrap();
        let r = h.get_region().unwrap();
        assert_eq!(
            r,
            Region::Custom {
                name: "us-east-1".to_string(),
                endpoint: "https://minio.example.com".to_string(),
            }
        );
    }

    #[test]
    fn test_config_monitor_rules() {
        let mut tmp_file = NamedTempFile::new().unwrap();
        tmp_file.write_all(CONF_MONITOR.as_bytes()).unwrap();
        let c = Config::new(tmp_file.into_temp_path().to_path_buf()).unwrap();
        let host = c.get_host("s3").unwrap();

        assert_eq!(host.buckets.len(), 2);
        assert_eq!(host.buckets["bucket_A"].len(), 1);
        assert_eq!(
            host.buckets["bucket_A"][0],
            MonitorRule {
                prefix: "backups/daily".to_string(),
                suffix: ".log".to_string(),
                age: 86_400,
                size: 30_720,
            }
        );
        assert_eq!(host.buckets["bucket_B"].len(), 2);
        assert_eq!(
            host.buckets["bucket_B"][0],
            MonitorRule {
                prefix: "foo".to_string(),
                suffix: String::new(),
                age: 43_200,
                size: 1_024,
            }
        );
    }

    #[test]
    fn test_config_monitor_rule_defaults() {
        let mut tmp_file = NamedTempFile::new().unwrap();
        tmp_file
            .write_all(CONF_MONITOR_DEFAULTS.as_bytes())
            .unwrap();
        let c = Config::new(tmp_file.into_temp_path().to_path_buf()).unwrap();
        let host = c.get_host("s3").unwrap();
        let rule = &host.buckets["bucket_A"][0];

        assert_eq!(rule.prefix, "backups/daily");
        assert_eq!(rule.suffix, "");
        assert_eq!(rule.age, 86_400);
        assert_eq!(rule.size, 0);
    }

    #[test]
    fn test_config_monitor_rule_optional_prefix() {
        let mut tmp_file = NamedTempFile::new().unwrap();
        tmp_file
            .write_all(CONF_MONITOR_NO_PREFIX.as_bytes())
            .unwrap();
        let c = Config::new(tmp_file.into_temp_path().to_path_buf()).unwrap();
        let host = c.get_host("s3").unwrap();
        let rule = &host.buckets["bucket_A"][0];

        // prefix omitted -> empty string -> whole-bucket scan
        assert_eq!(rule.prefix, "");
        assert_eq!(rule.suffix, ".sql");
        assert_eq!(rule.age, 43_200);
    }

    #[test]
    fn test_config_empty_values_as_default() {
        let mut tmp_file = NamedTempFile::new().unwrap();
        tmp_file
            .write_all(CONF_MONITOR_EMPTY_VALUES.as_bytes())
            .unwrap();
        let c = Config::new(tmp_file.into_temp_path().to_path_buf()).unwrap();
        let host = c.get_host("s3").unwrap();
        let rule = &host.buckets["bucket_A"][0];
        let quoted = &host.buckets["bucket_A"][1];

        // empty, `~` and `null` parse as null and fall back to the field defaults
        assert_eq!(host.access_key, "");
        assert_eq!(host.secret_key.expose_secret(), "");
        assert_eq!(rule.prefix, "");
        assert_eq!(rule.suffix, "");
        assert_eq!(rule.age, 3_600);

        // quoted values stay literal strings
        assert_eq!(quoted.prefix, "~");
        assert_eq!(quoted.suffix, "null");
    }

    #[test]
    fn test_config_parse_error_hides_secret_key() {
        let base =
            "---\nhosts:\n  s3:\n    endpoint: https://s3.example.test\n    access_key: XXX\n";
        let broken = [
            // unclosed quote
            "    secret_key: \"TOPSECRET\n",
            // unclosed flow sequence
            "    secret_key: [TOPSECRET\n",
            // mapping where a string is expected
            "    secret_key: {nested: TOPSECRET}\n",
            // bad indentation right after the secret
            "    secret_key: TOPSECRET\n      bucket: b\n",
            // invalid value on the line after the secret
            "    secret_key: TOPSECRET\n    compress: maybe\n",
        ];

        for tail in broken {
            let mut tmp_file = NamedTempFile::new().unwrap();
            tmp_file
                .write_all(format!("{base}{tail}").as_bytes())
                .unwrap();
            let err = Config::new(tmp_file.into_temp_path().to_path_buf()).unwrap_err();

            for rendered in [format!("{err:#}"), format!("{err:?}")] {
                assert!(rendered.contains("unable to parse config file"));
                assert!(
                    !rendered.contains("TOPSECRET"),
                    "secret leaked for {tail:?}: {rendered}"
                );
            }
        }
    }

    #[test]
    fn test_config_duplicate_keys_are_rejected() {
        let duplicated = [
            (
                "---\nhosts:\n  s3:\n    endpoint: https://one.example.test\n  s3:\n    endpoint: https://two.example.test\n",
                "duplicate key `s3` at line 5, column 3",
            ),
            (
                "---\nhosts:\n  s3:\n    endpoint: https://s3.example.test\n    buckets:\n      bucket_A:\n        - prefix: a\n      bucket_A:\n        - prefix: b\n",
                "duplicate key `bucket_A` at line 8, column 7",
            ),
            (
                "---\nhosts:\n  s3:\n    access_key: a\n    secret_key: s\n    access_key: b\n",
                "duplicate key `access_key` at line 6, column 5",
            ),
        ];

        for (yaml, expected) in duplicated {
            let mut tmp_file = NamedTempFile::new().unwrap();
            tmp_file.write_all(yaml.as_bytes()).unwrap();
            let err = Config::new(tmp_file.into_temp_path().to_path_buf()).unwrap_err();
            let rendered = format!("{err:#}");

            assert!(rendered.contains(expected), "{rendered}");
            // serde-saphyr's hint names an option the CLI does not expose
            assert!(!rendered.contains("DuplicateKeyPolicy"), "{rendered}");
        }
    }

    #[test]
    fn test_config_parse_error_names_field() {
        let base = "---\nhosts:\n  s3:\n    endpoint: https://s3.example.test\n";
        let invalid = [
            (
                "    compress: maybe\n",
                "hosts.s3.compress: invalid boolean at line 5, column 15",
            ),
            (
                "    buckets:\n      bucket_A:\n        - prefix: logs/\n          age: 30x\n",
                "hosts.s3.buckets.bucket_A[0].age:",
            ),
        ];

        for (tail, expected) in invalid {
            let mut tmp_file = NamedTempFile::new().unwrap();
            tmp_file
                .write_all(format!("{base}{tail}").as_bytes())
                .unwrap();
            let err = Config::new(tmp_file.into_temp_path().to_path_buf()).unwrap_err();

            assert!(format!("{err:#}").contains(expected), "{err:#}");
        }
    }

    #[test]
    fn test_parse_monitor_age_text_units() {
        assert_eq!(parse_monitor_age_text("30s").unwrap(), 30);
        assert_eq!(parse_monitor_age_text("15m").unwrap(), 900);
        assert_eq!(parse_monitor_age_text("12h").unwrap(), 43_200);
        assert_eq!(parse_monitor_age_text("7d").unwrap(), 604_800);
        assert_eq!(parse_monitor_age_text("86400").unwrap(), 86_400);
    }

    #[test]
    fn test_parse_monitor_age_text_invalid_unit() {
        let err = parse_monitor_age_text("1w").unwrap_err();
        assert!(err.contains("Expected seconds or a suffix"));
    }

    #[test]
    fn test_config_get_compress() {
        let mut tmp_file = NamedTempFile::new().unwrap();
        tmp_file.write_all(CONF_COMPRESS.as_bytes()).unwrap();
        let c = Config::new(tmp_file.into_temp_path().to_path_buf());
        assert!(c.is_ok());
        let c = c.unwrap();
        let h = c.get_host("s3");
        assert!(h.is_ok());
        let h = h.unwrap();
        let r = h.get_region();
        assert!(r.is_ok());
        let r = r.unwrap();
        assert_eq!(r, Region::aws("us-east-2"));
        assert_eq!(h.compress, Some(true));
    }

    #[test]
    fn test_config_get_compress_and_encrypt() {
        let mut tmp_file = NamedTempFile::new().unwrap();
        tmp_file.write_all(CONF_ENCRYPT.as_bytes()).unwrap();
        let c = Config::new(tmp_file.into_temp_path().to_path_buf());
        assert!(c.is_ok());
        let c = c.unwrap();
        let h = c.get_host("s3");
        assert!(h.is_ok());
        let h = h.unwrap();
        let r = h.get_region();
        assert!(r.is_ok());
        let r = r.unwrap();
        assert_eq!(r, Region::aws("us-east-2"));
        assert_eq!(h.compress, Some(true));
        assert_eq!(h.enc_key, Some(String::from("secret")));
    }

    #[test]
    fn test_config_get_aws_regions_endpoints() {
        // https://docs.aws.amazon.com/general/latest/gr/rande.html
        let aws_s3_regions = vec![
            ("US East (Ohio)", "us-east-2", "s3.us-east-2.amazonaws.com"),
            (
                "US East (N. Virginia)",
                "us-east-1",
                "s3.us-east-1.amazonaws.com",
            ),
            (
                "US West (N. California)",
                "us-west-1",
                "s3.us-west-1.amazonaws.com",
            ),
            (
                "US West (Oregon)",
                "us-west-2",
                "s3.us-west-2.amazonaws.com",
            ),
            (
                "Africa (Cape Town)",
                "af-south-1",
                "s3.af-south-1.amazonaws.com",
            ),
            (
                "Asia Pacific (Hong Kong)",
                "ap-east-1",
                "s3.ap-east-1.amazonaws.com",
            ),
            (
                "Asia Pacific (Hyderabad)",
                "ap-south-2",
                "s3.ap-south-2.amazonaws.com",
            ),
            (
                "Asia Pacific (Jakarta)",
                "ap-southeast-3",
                "s3.ap-southeast-3.amazonaws.com",
            ),
            (
                "Asia Pacific (Melbourne)",
                "ap-southeast-4",
                "s3.ap-southeast-4.amazonaws.com",
            ),
            (
                "Asia Pacific (Mumbai)",
                "ap-south-1",
                "s3.ap-south-1.amazonaws.com",
            ),
            (
                "Asia Pacific (Osaka)",
                "ap-northeast-3",
                "s3.ap-northeast-3.amazonaws.com",
            ),
            (
                "Asia Pacific (Seoul)",
                "ap-northeast-2",
                "s3.ap-northeast-2.amazonaws.com",
            ),
            (
                "Asia Pacific (Singapore)",
                "ap-southeast-1",
                "s3.ap-southeast-1.amazonaws.com",
            ),
            (
                "Asia Pacific (Sydney)",
                "ap-southeast-2",
                "s3.ap-southeast-2.amazonaws.com",
            ),
            (
                "Asia Pacific (Tokyo)",
                "ap-northeast-1",
                "s3.ap-northeast-1.amazonaws.com",
            ),
            (
                "Canada (Central)",
                "ca-central-1",
                "s3.ca-central-1.amazonaws.com",
            ),
            (
                "Europe (Frankfurt)",
                "eu-central-1",
                "s3.eu-central-1.amazonaws.com",
            ),
            (
                "Europe (Ireland)",
                "eu-west-1",
                "s3.eu-west-1.amazonaws.com",
            ),
            ("Europe (London)", "eu-west-2", "s3.eu-west-2.amazonaws.com"),
            (
                "Europe (Milan)",
                "eu-south-1",
                "s3.eu-south-1.amazonaws.com",
            ),
            ("Europe (Paris)", "eu-west-3", "s3.eu-west-3.amazonaws.com"),
            (
                "Europe (Spain)",
                "eu-south-2",
                "s3.eu-south-2.amazonaws.com",
            ),
            (
                "Europe (Stockholm)",
                "eu-north-1",
                "s3.eu-north-1.amazonaws.com",
            ),
            (
                "Europe (Zurich)",
                "eu-central-2",
                "s3.eu-central-2.amazonaws.com",
            ),
            (
                "Israel (Tel Aviv)",
                "il-central-1",
                "s3.il-central-1.amazonaws.com",
            ),
            (
                "Middle East (Bahrain)",
                "me-south-1",
                "s3.me-south-1.amazonaws.com",
            ),
            (
                "Middle East (UAE)",
                "me-central-1",
                "s3.me-central-1.amazonaws.com",
            ),
            (
                "South America (São Paulo)",
                "sa-east-1",
                "s3.sa-east-1.amazonaws.com",
            ),
            (
                "AWS GovCloud (US-East)",
                "us-gov-east-1",
                "s3.us-gov-east-1.amazonaws.com",
            ),
            (
                "AWS GovCloud (US-West)",
                "us-gov-west-1",
                "s3.us-gov-west-1.amazonaws.com",
            ),
            (
                "new region",
                "new-region-1",
                "s3.new-region-1.amazonaws.com",
            ),
        ];

        let mut yaml_content = "---\nhosts:\n".to_string();
        for (name, region, _) in &aws_s3_regions {
            let formatted_name = name.replace(' ', "_");
            yaml_content.push_str(&format!("  {formatted_name}:\n    region: {region}\n\n"));
        }
        let mut tmp_file = NamedTempFile::new().unwrap();
        tmp_file.write_all(yaml_content.as_bytes()).unwrap();
        let c = Config::new(tmp_file.into_temp_path().to_path_buf());
        assert!(c.is_ok());
        let c = c.unwrap();

        for (name, region, endpoint) in &aws_s3_regions {
            let formatted_name = name.replace(' ', "_");
            let h = c.get_host(&formatted_name);
            assert!(h.is_ok());
            let h = h.unwrap();
            let r = h.get_region();
            assert!(r.is_ok(), "Expected region to parse for '{formatted_name}'");
            let r = r.unwrap();
            assert_eq!(r, Region::from_str(region).unwrap());
            assert_eq!(r.endpoint(), (*endpoint).to_string());
        }
    }
}
