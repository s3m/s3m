use std::convert::TryFrom;

// Define the global arguments
#[derive(Debug, Clone, Default)]
pub struct GlobalArgs {
    pub throttle: Option<usize>,
    pub retries: u32,
    pub compress: bool,
    pub encrypt: bool,
}

impl GlobalArgs {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            throttle: None,
            retries: 3,
            compress: false,
            encrypt: false,
        }
    }

    pub fn set_retries(&mut self, retries: usize) {
        self.retries = u32::try_from(retries).unwrap_or(3);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_global_args() {
        let mut global_args = GlobalArgs::new();
        assert_eq!(global_args.throttle, None);
        assert_eq!(global_args.retries, 3);
        assert!(!global_args.compress);
        assert!(!global_args.encrypt);

        global_args.throttle = Some(10);
        assert_eq!(global_args.throttle, Some(10));

        global_args.set_retries(5);
        assert_eq!(global_args.retries, 5);

        global_args.compress = true;
        assert!(global_args.compress);

        global_args.encrypt = true;
        assert!(global_args.encrypt);
    }
}
