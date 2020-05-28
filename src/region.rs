// https://docs.aws.amazon.com/general/latest/gr/rande.html#regional-endpoints
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
    pub fn name(&self) -> &str {
        match *self {
            Region::AfSouth1 => "af-south-1",
            Region::ApEast1 => "ap-east-1",
            Region::ApNortheast1 => "ap-northeast-1",
            Region::ApNortheast2 => "ap-northeast-2",
            Region::ApNortheast3 => "ap-northeast-3",
            Region::ApSouth1 => "ap-south-1",
            Region::ApSoutheast1 => "ap-southeast-1",
            Region::ApSoutheast2 => "ap-southeast-2",
            Region::CaCentral1 => "ca-central-1",
            Region::EuCentral1 => "eu-central-1",
            Region::EuSouth1 => "eu-south-1",
            Region::EuWest1 => "eu-west-1",
            Region::EuWest2 => "eu-west-2",
            Region::EuWest3 => "eu-west-3",
            Region::EuNorth1 => "eu-north-1",
            Region::MeSouth1 => "me-south-1",
            Region::SaEast1 => "sa-east-1",
            Region::UsEast1 => "us-east-1",
            Region::UsEast2 => "us-east-2",
            Region::UsWest1 => "us-west-1",
            Region::UsWest2 => "us-west-2",
            Region::CnNorth1 => "cn-north-1",
            Region::CnNorthwest1 => "cn-northwest-1",
            Region::Custom { ref name, .. } => name,
        }
    }
}
