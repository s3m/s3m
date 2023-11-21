use std::{
    error::Error,
    fmt::{Display, Formatter},
    str::FromStr,
};

// https://docs.aws.amazon.com/general/latest/gr/rande.html#regional-endpoints
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Region {
    // Africa (Cape Town)           af-south-1
    AfSouth1,

    // Asia Pacific (Hong Kong)	    ap-east-1
    ApEast1,

    // Asia Pacific (Tokyo)	        ap-northeast-1
    ApNortheast1,

    // Asia Pacific (Seoul)	        ap-northeast-2
    ApNortheast2,

    // Asia Pacific (Osaka-Local)   ap-northeast-3
    ApNortheast3,

    // Asia Pacific (Mumbai)	    ap-south-1
    ApSouth1,

    // Asia Pacific (Hyderabad)     ap-south-2
    ApSouth2,

    // Asia Pacific (Singapore)	    ap-southeast-1
    ApSoutheast1,

    // Asia Pacific (Sydney)	    ap-southeast-2
    ApSoutheast2,

    // Asia Pacific (Jakarta)       ap-southeast-3
    ApSoutheast3,

    // Asia Pacific (Melbourne)     ap-southeast-4
    ApSoutheast4,

    // Canada (Central)	            ca-central-1
    CaCentral1,

    // China (Beijing)	            cn-north-1
    CnNorth1,

    // China (Ningxia)	            cn-northwest-1
    CnNorthwest1,

    // Europe (Frankfurt)	        eu-central-1
    EuCentral1,

    // Europe (Zurich)	            eu-central-2
    EuCentral2,

    // Europe (Stockholm)	        eu-north-1
    EuNorth1,

    // Europe (Milan)	            eu-south-1
    EuSouth1,

    // Europe (Spain)               eu-south-2
    EuSouth2,

    // Europe (Ireland)	            eu-west-1
    EuWest1,

    // Europe (London)	            eu-west-2
    EuWest2,

    // Europe (Paris)	            eu-west-3
    EuWest3,

    // Israel (Tel Aviv)            il-central-1
    IlCentral1,

    // Middle East (Bahrain)	    me-south-1
    MeSouth1,

    // Middle East (UAE)            me-central-1
    MeCentral1,

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

    // US GovCloud (US-East)       us-gov-east-1
    UsGovEast1,

    // US GovCloud (US-West)       us-gov-west-1
    UsGovWest1,

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
            Self::ApSouth2 => "ap-south-2",
            Self::ApSoutheast1 => "ap-southeast-1",
            Self::ApSoutheast2 => "ap-southeast-2",
            Self::ApSoutheast3 => "ap-southeast-3",
            Self::ApSoutheast4 => "ap-southeast-4",
            Self::CaCentral1 => "ca-central-1",
            Self::EuCentral1 => "eu-central-1",
            Self::EuCentral2 => "eu-central-2",
            Self::EuSouth1 => "eu-south-1",
            Self::EuSouth2 => "eu-south-2",
            Self::EuWest1 => "eu-west-1",
            Self::EuWest2 => "eu-west-2",
            Self::EuWest3 => "eu-west-3",
            Self::EuNorth1 => "eu-north-1",
            Self::IlCentral1 => "il-central-1",
            Self::MeSouth1 => "me-south-1",
            Self::MeCentral1 => "me-central-1",
            Self::SaEast1 => "sa-east-1",
            Self::UsEast1 => "us-east-1",
            Self::UsEast2 => "us-east-2",
            Self::UsWest1 => "us-west-1",
            Self::UsWest2 => "us-west-2",
            Self::CnNorth1 => "cn-north-1",
            Self::CnNorthwest1 => "cn-northwest-1",
            Self::UsGovEast1 => "us-gov-east-1",
            Self::UsGovWest1 => "us-gov-west-1",
            Self::Custom { ref name, .. } => name,
        }
    }

    #[must_use]
    pub fn endpoint(&self) -> &str {
        match *self {
            Self::AfSouth1 => "s3.af-south-1.amazonaws.com",
            Self::ApEast1 => "s3.ap-east-1.amazonaws.com",
            Self::ApNortheast1 => "s3.ap-northeast-1.amazonaws.com",
            Self::ApNortheast2 => "s3.ap-northeast-2.amazonaws.com",
            Self::ApNortheast3 => "s3.ap-northeast-3.amazonaws.com",
            Self::ApSouth1 => "s3.ap-south-1.amazonaws.com",
            Self::ApSouth2 => "s3.ap-south-2.amazonaws.com",
            Self::ApSoutheast1 => "s3.ap-southeast-1.amazonaws.com",
            Self::ApSoutheast2 => "s3.ap-southeast-2.amazonaws.com",
            Self::ApSoutheast3 => "s3.ap-southeast-3.amazonaws.com",
            Self::ApSoutheast4 => "s3.ap-southeast-4.amazonaws.com",
            Self::CaCentral1 => "s3.ca-central-1.amazonaws.com",
            Self::EuCentral1 => "s3.eu-central-1.amazonaws.com",
            Self::EuCentral2 => "s3.eu-central-2.amazonaws.com",
            Self::EuSouth1 => "s3.eu-south-1.amazonaws.com",
            Self::EuSouth2 => "s3.eu-south-2.amazonaws.com",
            Self::EuWest1 => "s3.eu-west-1.amazonaws.com",
            Self::EuWest2 => "s3.eu-west-2.amazonaws.com",
            Self::EuWest3 => "s3.eu-west-3.amazonaws.com",
            Self::EuNorth1 => "s3.eu-north-1.amazonaws.com",
            Self::IlCentral1 => "s3.il-central-1.amazonaws.com",
            Self::MeSouth1 => "s3.me-south-1.amazonaws.com",
            Self::MeCentral1 => "s3.me-central-1.amazonaws.com",
            Self::SaEast1 => "s3.sa-east-1.amazonaws.com",
            Self::UsEast1 => "s3.us-east-1.amazonaws.com",
            Self::UsEast2 => "s3.us-east-2.amazonaws.com",
            Self::UsWest1 => "s3.us-west-1.amazonaws.com",
            Self::UsWest2 => "s3.us-west-2.amazonaws.com",
            Self::CnNorth1 => "s3.cn-north-1.amazonaws.com.cn",
            Self::CnNorthwest1 => "s3.cn-northwest-1.amazonaws.com.cn",
            Self::UsGovEast1 => "s3.us-gov-east-1.amazonaws.com",
            Self::UsGovWest1 => "s3.us-gov-west-1.amazonaws.com",
            Self::Custom { ref endpoint, .. } => endpoint,
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
            "ap-south-2" => Ok(Self::ApSouth2),
            "ap-southeast-1" => Ok(Self::ApSoutheast1),
            "ap-southeast-2" => Ok(Self::ApSoutheast2),
            "ap-southeast-3" => Ok(Self::ApSoutheast3),
            "ap-southeast-4" => Ok(Self::ApSoutheast4),
            "ca-central-1" => Ok(Self::CaCentral1),
            "eu-central-1" => Ok(Self::EuCentral1),
            "eu-central-2" => Ok(Self::EuCentral2),
            "eu-south-1" => Ok(Self::EuSouth1),
            "eu-south-2" => Ok(Self::EuSouth2),
            "eu-west-1" => Ok(Self::EuWest1),
            "eu-west-2" => Ok(Self::EuWest2),
            "eu-west-3" => Ok(Self::EuWest3),
            "eu-north-1" => Ok(Self::EuNorth1),
            "il-central-1" => Ok(Self::IlCentral1),
            "me-south-1" => Ok(Self::MeSouth1),
            "me-central-1" => Ok(Self::MeCentral1),
            "sa-east-1" => Ok(Self::SaEast1),
            "us-east-1" => Ok(Self::UsEast1),
            "us-east-2" => Ok(Self::UsEast2),
            "us-west-1" => Ok(Self::UsWest1),
            "us-west-2" => Ok(Self::UsWest2),
            "cn-north-1" => Ok(Self::CnNorth1),
            "cn-northwest-1" => Ok(Self::CnNorthwest1),
            "us-gov-east-1" => Ok(Self::UsGovEast1),
            "us-gov-west-1" => Ok(Self::UsGovWest1),
            _ => Err(ParseRegionError::new(s)),
        }
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
            .map_or(Self::UsEast1, |v| {
                Self::from_str(v).unwrap_or(Self::UsEast1)
            })
    }
}

#[cfg(test)]
mod tests {
    use super::Region;

    #[test]
    fn test_region_from_str() {
        assert_eq!(
            "foo"
                .parse::<Region>()
                .expect_err("Parsing foo as a Region was not an error")
                .to_string(),
            "Not a valid AWS region: foo".to_owned()
        );
        assert_eq!("af-south-1".parse(), Ok(Region::AfSouth1));
        assert_eq!("ap-east-1".parse(), Ok(Region::ApEast1));
        assert_eq!("ap-northeast-1".parse(), Ok(Region::ApNortheast1));
        assert_eq!("ap-northeast-2".parse(), Ok(Region::ApNortheast2));
        assert_eq!("ap-northeast-3".parse(), Ok(Region::ApNortheast3));
        assert_eq!("ap-south-1".parse(), Ok(Region::ApSouth1));
        assert_eq!("ap-south-2".parse(), Ok(Region::ApSouth2));
        assert_eq!("ap-southeast-1".parse(), Ok(Region::ApSoutheast1));
        assert_eq!("ap-southeast-2".parse(), Ok(Region::ApSoutheast2));
        assert_eq!("ap-southeast-3".parse(), Ok(Region::ApSoutheast3));
        assert_eq!("ap-southeast-4".parse(), Ok(Region::ApSoutheast4));
        assert_eq!("ca-central-1".parse(), Ok(Region::CaCentral1));
        assert_eq!("eu-central-1".parse(), Ok(Region::EuCentral1));
        assert_eq!("eu-central-2".parse(), Ok(Region::EuCentral2));
        assert_eq!("eu-south-1".parse(), Ok(Region::EuSouth1));
        assert_eq!("eu-south-2".parse(), Ok(Region::EuSouth2));
        assert_eq!("eu-west-1".parse(), Ok(Region::EuWest1));
        assert_eq!("eu-west-2".parse(), Ok(Region::EuWest2));
        assert_eq!("eu-west-3".parse(), Ok(Region::EuWest3));
        assert_eq!("eu-north-1".parse(), Ok(Region::EuNorth1));
        assert_eq!("il-central-1".parse(), Ok(Region::IlCentral1));
        assert_eq!("me-south-1".parse(), Ok(Region::MeSouth1));
        assert_eq!("me-central-1".parse(), Ok(Region::MeCentral1));
        assert_eq!("sa-east-1".parse(), Ok(Region::SaEast1));
        assert_eq!("us-east-1".parse(), Ok(Region::UsEast1));
        assert_eq!("us-east-2".parse(), Ok(Region::UsEast2));
        assert_eq!("us-west-1".parse(), Ok(Region::UsWest1));
        assert_eq!("us-west-2".parse(), Ok(Region::UsWest2));
        assert_eq!("cn-north-1".parse(), Ok(Region::CnNorth1));
        assert_eq!("cn-northwest-1".parse(), Ok(Region::CnNorthwest1));
        assert_eq!("us-gov-east-1".parse(), Ok(Region::UsGovEast1));
        assert_eq!("us-gov-west-1".parse(), Ok(Region::UsGovWest1));
    }
}
