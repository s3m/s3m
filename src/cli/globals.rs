// Define the global arguments
#[derive(Debug, Clone, Default)]
pub struct GlobalArgs {
    pub throttle: Option<usize>,
}

impl GlobalArgs {
    #[must_use]
    pub const fn new() -> Self {
        Self { throttle: None }
    }

    pub fn set_throttle(&mut self, throttle: usize) {
        self.throttle = Some(throttle);
    }
}
