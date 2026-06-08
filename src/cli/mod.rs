mod config;
pub use self::config::{Config, Host};

pub mod actions;
pub mod age_filter;
pub mod globals;
// `progressbar` lives in `s3m-core`; re-export it here so existing
// `crate::cli::progressbar::…` paths keep resolving.
pub use crate::progressbar;

mod start;
pub use self::start::start;

mod commands;
mod decrypt;
mod dispatch;
mod s3_location;
