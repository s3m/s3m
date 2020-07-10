use reqwest::Method;

#[derive(Clone, Debug)]
pub enum Actions {
    ListObjectsV2 {
        continuation_token: Option<String>,
        delimiter: Option<String>,
        fetch_owner: Option<bool>,
        prefix: Option<String>,
        start_after: Option<String>,
    },
}

impl Actions {
    #[must_use]
    pub fn http_verb(&self) -> Method {
        match *self {
            Self::ListObjectsV2 { .. } => Method::GET,
        }
    }
}
