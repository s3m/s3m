use std::{
    error::Error,
    fmt::{Display, Formatter},
    str::FromStr,
};

/// https://docs.aws.amazon.com/general/latest/gr/rande.html#regional-endpoints
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Region {
    /// AWS region (follows s3.{region}.amazonaws.com pattern)
    Aws(String),

    /// Custom region with explicit endpoint
    Custom { name: String, endpoint: String },
}

impl Region {
    /// Create a new AWS region
    pub fn aws(name: impl Into<String>) -> Self {
        Self::Aws(name.into())
    }

    /// Create a custom region with explicit endpoint
    pub fn custom(name: impl Into<String>, endpoint: impl Into<String>) -> Self {
        Self::Custom {
            name: name.into(),
            endpoint: endpoint.into(),
        }
    }

    /// Get the region name
    #[must_use]
    pub fn name(&self) -> &str {
        match self {
            Self::Aws(name) => name,
            Self::Custom { name, .. } => name,
        }
    }

    /// Get the S3 endpoint for this region
    #[must_use]
    pub fn endpoint(&self) -> String {
        match self {
            Self::Aws(region) => format!("s3.{}.amazonaws.com", region),
            Self::Custom { endpoint, .. } => endpoint.clone(),
        }
    }

    /// Check if this is a valid AWS region
    #[must_use]
    pub fn is_aws_region(&self) -> bool {
        match self {
            Self::Aws(region) => {
                // Basic validation - AWS regions follow pattern: 2-3 letter prefix, dash, direction, dash, number
                let parts: Vec<&str> = region.split('-').collect();
                if parts.len() < 3 {
                    return false;
                }

                // Check if last part is a number
                parts
                    .last()
                    .is_some_and(|&last| last.chars().all(|c| c.is_ascii_digit()))
            }
            Self::Custom { .. } => false,
        }
    }
}

impl FromStr for Region {
    type Err = ParseRegionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let input = s.trim();

        if input.is_empty() || input.contains(' ') {
            return Err(ParseRegionError::new(s));
        }

        // Check if the input contains a period and handle it as custom region
        if input.contains("://") || input.contains('.') {
            // Extract name from URL if possible, otherwise use the input as name
            let name = if let Some(host) = input.split("://").nth(1) {
                host.split('.').next().unwrap_or(input).to_string()
            } else {
                input.split('.').next().unwrap_or(input).to_string()
            };

            return Ok(Self::custom(name, input));
        }

        // Match against known AWS regions
        let region = input.to_lowercase();
        let aws_region = Self::aws(region);

        if !aws_region.is_aws_region() {
            return Err(ParseRegionError::new(s));
        }

        Ok(aws_region)
    }
}

/// An error produced when attempting to convert a `str` into a `Region` fails.
#[derive(Debug, Eq, PartialEq)]
pub struct ParseRegionError {
    message: String,
}

impl ParseRegionError {
    /// Parses a region given as a string literal into a type `Region`
    #[must_use]
    pub fn new(input: &str) -> Self {
        Self {
            message: format!("Not a valid AWS region: {input}"),
        }
    }
}

impl Error for ParseRegionError {}

impl Display for ParseRegionError {
    fn fmt(&self, f: &mut Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "{}", self.message)
    }
}

impl Default for Region {
    fn default() -> Self {
        std::env::var("AWS_DEFAULT_REGION")
            .or_else(|_| std::env::var("AWS_REGION"))
            .as_ref()
            .map_or_else(
                |_| Self::aws("us-east-1"),
                |v| Self::from_str(v).unwrap_or_else(|_| Self::aws("us-east-1")),
            )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_region_from_str() {
        assert_eq!(
            "us-east-1".parse::<Region>().unwrap(),
            Region::aws("us-east-1")
        );
        assert_eq!(
            "eu-west-1".parse::<Region>().unwrap(),
            Region::aws("eu-west-1")
        );
        assert_eq!(
            "ap-southeast-1".parse::<Region>().unwrap(),
            Region::aws("ap-southeast-1")
        );
    }

    #[test]
    fn test_endpoints() {
        let us_east = Region::aws("us-east-1");
        assert_eq!(us_east.endpoint(), "s3.us-east-1.amazonaws.com");

        let custom = Region::custom("custom", "localhost:9000");
        assert_eq!(custom.endpoint(), "localhost:9000");

        let custom = "foo.bar".parse::<Region>().unwrap();
        assert_eq!(custom.name(), "foo");
        assert_eq!(custom.endpoint(), "foo.bar");
    }

    #[test]
    fn test_validation() {
        assert!(Region::aws("us-east-1").is_aws_region());
        assert!(Region::aws("eu-west-2").is_aws_region());
        assert!(!Region::aws("invalid").is_aws_region());
        assert!(!Region::custom("custom", "localhost:9000").is_aws_region());

        let name = String::from("foo");
        assert_eq!(name.parse::<Region>(), Err(ParseRegionError::new("foo")));
    }
}
