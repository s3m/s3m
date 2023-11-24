use crate::s3::Region;
use anyhow::{Context, Result};
use serde::Deserialize;
use std::{collections::BTreeMap, fs::File, path::PathBuf};

#[derive(Debug, Deserialize, Eq, PartialEq)]
pub struct Config {
    pub hosts: BTreeMap<String, Host>,
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
pub struct Host {
    pub endpoint: Option<String>,
    pub region: Option<String>,
    #[serde(default)]
    pub access_key: String,
    #[serde(default)]
    pub secret_key: String,
    pub bucket: Option<String>,
}

impl Config {
    pub fn new(config_path: PathBuf) -> Result<Self> {
        let file = File::open(config_path)?;

        let config: Self = serde_yaml::from_reader(file).context("unable to parse config file")?;

        Ok(config)
    }

    /// Get the host from the config.yml
    pub fn get_host(&self, name: &str) -> Result<&Host> {
        self.hosts
            .get(name)
            .with_context(|| format!("could not find host {}", name))
    }
}

impl Host {
    /// Get the region for the host
    pub fn get_region(&self) -> Result<Region> {
        Ok(match &self.region {
            Some(r) => r.parse::<Region>()?,
            None => {
                let r = self
                    .endpoint
                    .as_ref()
                    .context("could not parse host need an endpoint or region")?;
                Region::Custom {
                    name: String::new(),
                    endpoint: r.to_string(),
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::str::FromStr;
    use tempfile::NamedTempFile;

    const CONF: &str = r#"---
hosts:
  s3:
    region: xx-region-y
    access_key: XXX
    secret_key: YYY
    bucket: my-bucket"#;

    const CONF_AWS: &str = r#"---
hosts:
  s3:
    region: eu-central-2
    access_key: XXX
    secret_key: YYY
    bucket: my-bucket"#;

    const CONF_OTHER: &str = r#"---
hosts:
  s3:
    endpoint: s3.us-west-000.backblazeb2.com
    access_key: XXX
    secret_key: YYY
    bucket: my-bucket"#;

    const CONF_NO_REGION: &str = r#"---
hosts:
  s3:
    access_key: XXX
    secret_key: YYY
    bucket: my-bucket"#;

    #[test]
    fn test_config_get_host() {
        let mut tmp_file = NamedTempFile::new().unwrap();
        tmp_file.write_all(CONF.as_bytes()).unwrap();
        let c = Config::new(tmp_file.into_temp_path().to_path_buf());
        println!("{:?}", c);
        assert!(c.is_ok());
        let c = c.unwrap();
        assert_eq!(c.hosts.len(), 1);
        assert_eq!(c.hosts.get("s3").unwrap().access_key, "XXX");
        assert_eq!(c.hosts.get("s3").unwrap().secret_key, "YYY");
        assert_eq!(
            c.hosts.get("s3").unwrap().bucket,
            Some("my-bucket".to_string())
        );
        assert_eq!(
            c.hosts.get("s3").unwrap().region,
            Some("xx-region-y".to_string())
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
        println!("{:?}", r);
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
        assert_eq!(r, Region::EuCentral2);
    }

    #[test]
    fn test_config_get_invalid_aws_region() {
        let mut tmp_file = NamedTempFile::new().unwrap();
        tmp_file.write_all(CONF.as_bytes()).unwrap();
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
        ];

        let mut yaml_content = String::from(format!("---\nhosts:\n"));
        for (name, region, _) in &aws_s3_regions {
            let formatted_name = name.replace(" ", "_");
            yaml_content.push_str(&format!(
                "  {}:\n    region: {}\n\n",
                formatted_name, region
            ));
        }
        let mut tmp_file = NamedTempFile::new().unwrap();
        tmp_file.write_all(yaml_content.as_bytes()).unwrap();
        let c = Config::new(tmp_file.into_temp_path().to_path_buf());
        assert!(c.is_ok());
        let c = c.unwrap();

        for (name, region, endpoint) in &aws_s3_regions {
            let formatted_name = name.replace(" ", "_");
            let h = c.get_host(&formatted_name);
            assert!(h.is_ok());
            let h = h.unwrap();
            let r = h.get_region();
            assert!(r.is_ok());
            let r = r.unwrap();
            assert_eq!(r, Region::from_str(region).unwrap());
            assert_eq!(r.endpoint(), endpoint.to_string())
        }
    }
}
