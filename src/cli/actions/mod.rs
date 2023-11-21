use std::collections::BTreeMap;
use std::path::PathBuf;

#[derive(Debug)]
pub enum Action {
    ACL {
        key: String,
        acl: Option<String>,
    },
    ListObjects {
        bucket: Option<String>,
        list_multipart_uploads: bool,
        prefix: Option<String>,
        start_after: Option<String>,
    },
    MakeBucket {
        acl: String,
    },
    PutObject {
        acl: Option<String>,
        meta: Option<BTreeMap<String, String>>,
        buf_size: usize,
        file: Option<String>,
        key: String,
        pipe: bool,
        s3m_dir: PathBuf,
        quiet: bool,
    },
    DeleteObject {
        key: String,
        upload_id: String,
    },
    GetObject {
        key: String,
        get_head: bool,
        dest: Option<String>,
        quiet: bool,
    },
    ShareObject {
        key: String,
        expire: usize,
    },
}
