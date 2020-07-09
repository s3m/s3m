use std::error::Error;
use std::fmt::{Display, Formatter};
use std::str::FromStr;

// https://docs.aws.amazon.com/general/latest/gr/rande.html#regional-endpoints
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Region {
    // Africa (Cape Town)           af-south-1
    AfSouth1,

    // Asia Pacific (Hong Kong)	    ap-east-1
    ApEast1,

    // Asia Pacific (Tokyo)	    ap-northeast-1
    ApNortheast1,

    // Asia Pacific (Seoul)	    ap-northeast-2
    ApNortheast2,

    // Asia Pacific (Osaka-Local)   ap-northeast-3
    ApNortheast3,

    // Asia Pacific (Mumbai)	    ap-south-1
    ApSouth1,

    // Asia Pacific (Singapore)	    ap-southeast-1
    ApSoutheast1,

    // Asia Pacific (Sydney)	    ap-southeast-2
    ApSoutheast2,

    // Canada (Central)	            ca-central-1
    CaCentral1,

    // China (Beijing)	            cn-north-1
    CnNorth1,

    // China (Ningxia)	            cn-northwest-1
    CnNorthwest1,

    // Europe (Frankfurt)	    eu-central-1
    EuCentral1,

    // Europe (Stockholm)	    eu-north-1
    EuNorth1,

    // Europe (Milan)	            eu-south-1
    EuSouth1,

    // Europe (Ireland)	            eu-west-1
    EuWest1,

    // Europe (London)	            eu-west-2
    EuWest2,

    // Europe (Paris)	            eu-west-3
    EuWest3,

    // Middle East (Bahrain)	    me-south-1
    MeSouth1,

    // South America (São Paulo)    sa-east-1
    SaEast1,

    // US East (N. Virginia)	    us-east-1
    UsEast1,

    // US East (Ohio)	            us-east-2
    UsEast2,

    // US West (N. California)	    us-west-1
    UsWest1,

    // US West (Oregon)	            us-west-2
    UsWest2,

    // Custom region, endpoint
    Custom { name: String, endpoint: String },
}

impl Region {
    #[must_use]
    pub fn name(&self) -> &str {
        match *self {
            Self::AfSouth1 => "af-south-1",
            Self::ApEast1 => "ap-east-1",
            Self::ApNortheast1 => "ap-northeast-1",
            Self::ApNortheast2 => "ap-northeast-2",
            Self::ApNortheast3 => "ap-northeast-3",
            Self::ApSouth1 => "ap-south-1",
            Self::ApSoutheast1 => "ap-southeast-1",
            Self::ApSoutheast2 => "ap-southeast-2",
            Self::CaCentral1 => "ca-central-1",
            Self::EuCentral1 => "eu-central-1",
            Self::EuSouth1 => "eu-south-1",
            Self::EuWest1 => "eu-west-1",
            Self::EuWest2 => "eu-west-2",
            Self::EuWest3 => "eu-west-3",
            Self::EuNorth1 => "eu-north-1",
            Self::MeSouth1 => "me-south-1",
            Self::SaEast1 => "sa-east-1",
            Self::UsEast1 => "us-east-1",
            Self::UsEast2 => "us-east-2",
            Self::UsWest1 => "us-west-1",
            Self::UsWest2 => "us-west-2",
            Self::CnNorth1 => "cn-north-1",
            Self::CnNorthwest1 => "cn-northwest-1",
            Self::Custom { ref name, .. } => name,
        }
    }
}

impl FromStr for Region {
    type Err = ParseRegionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let v: &str = &s.to_lowercase();
        match v {
            "af-south-1" => Ok(Self::AfSouth1),
            "ap-east-1" => Ok(Self::ApEast1),
            "ap-northeast-1" => Ok(Self::ApNortheast1),
            "ap-northeast-2" => Ok(Self::ApNortheast2),
            "ap-northeast-3" => Ok(Self::ApNortheast3),
            "ap-south-1" => Ok(Self::ApSouth1),
            "ap-southeast-1" => Ok(Self::ApSoutheast1),
            "ap-southeast-2" => Ok(Self::ApSoutheast2),
            "ca-central-1" => Ok(Self::CaCentral1),
            "eu-central-1" => Ok(Self::EuCentral1),
            "eu-west-1" => Ok(Self::EuWest1),
            "eu-west-2" => Ok(Self::EuWest2),
            "eu-west-3" => Ok(Self::EuWest3),
            "eu-north-1" => Ok(Self::EuNorth1),
            "me-south-1" => Ok(Self::MeSouth1),
            "sa-east-1" => Ok(Self::SaEast1),
            "us-east-1" => Ok(Self::UsEast1),
            "us-east-2" => Ok(Self::UsEast2),
            "us-west-1" => Ok(Self::UsWest1),
            "us-west-2" => Ok(Self::UsWest2),
            "cn-north-1" => Ok(Self::CnNorth1),
            "cn-northwest-1" => Ok(Self::CnNorthwest1),
            _ => Err(ParseRegionError::new(s)),
        }
    }
}

/// An error produced when attempting to convert a `str` into a `Region` fails.
#[derive(Debug, PartialEq)]
pub struct ParseRegionError {
    message: String,
}

impl ParseRegionError {
    /// Parses a region given as a string literal into a type `Region'
    #[must_use]
    pub fn new(input: &str) -> Self {
        Self {
            message: format!("Not a valid AWS region: {}", input),
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
        match std::env::var("AWS_DEFAULT_REGION").or_else(|_| std::env::var("AWS_REGION")) {
            Ok(ref v) => Self::from_str(v).unwrap_or(Self::UsEast1),
            Err(_) => Self::UsEast1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Region;

    #[test]
    fn from_str() {
        assert_eq!(
            "foo"
                .parse::<Region>()
                .err()
                .expect("Parsing foo as a Region was not an error")
                .to_string(),
            "Not a valid AWS region: foo".to_owned()
        );
        assert_eq!("af-south-1".parse(), Ok(Region::AfSouth1));
        assert_eq!("ap-east-1".parse(), Ok(Region::ApEast1));
        assert_eq!("ap-northeast-1".parse(), Ok(Region::ApNortheast1));
        assert_eq!("ap-northeast-2".parse(), Ok(Region::ApNortheast2));
        assert_eq!("ap-northeast-3".parse(), Ok(Region::ApNortheast3));
        assert_eq!("ap-south-1".parse(), Ok(Region::ApSouth1));
        assert_eq!("ap-southeast-1".parse(), Ok(Region::ApSoutheast1));
        assert_eq!("ap-southeast-2".parse(), Ok(Region::ApSoutheast2));
        assert_eq!("ca-central-1".parse(), Ok(Region::CaCentral1));
        assert_eq!("eu-central-1".parse(), Ok(Region::EuCentral1));
        assert_eq!("eu-west-1".parse(), Ok(Region::EuWest1));
        assert_eq!("eu-west-2".parse(), Ok(Region::EuWest2));
        assert_eq!("eu-west-3".parse(), Ok(Region::EuWest3));
        assert_eq!("eu-north-1".parse(), Ok(Region::EuNorth1));
        assert_eq!("me-south-1".parse(), Ok(Region::MeSouth1));
        assert_eq!("sa-east-1".parse(), Ok(Region::SaEast1));
        assert_eq!("us-east-1".parse(), Ok(Region::UsEast1));
        assert_eq!("us-east-2".parse(), Ok(Region::UsEast2));
        assert_eq!("us-west-1".parse(), Ok(Region::UsWest1));
        assert_eq!("us-west-2".parse(), Ok(Region::UsWest2));
        assert_eq!("cn-north-1".parse(), Ok(Region::CnNorth1));
        assert_eq!("cn-northwest-1".parse(), Ok(Region::CnNorthwest1));
    }
}
