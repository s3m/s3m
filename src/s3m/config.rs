use serde::Deserialize;
use std::collections::BTreeMap;

#[derive(Debug, Deserialize, PartialEq)]
pub struct Config {
    pub hosts: BTreeMap<String, Host>,
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct Host {
    pub endpoint: Option<String>,
    pub region: Option<String>,
    #[serde(default)]
    pub access_key: String,
    #[serde(default)]
    pub secret_key: String,
    pub bucket: Option<String>,
}
