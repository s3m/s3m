pub mod acl;
pub mod bucket;
pub mod object_delete;
pub mod object_get;
pub mod object_list;
pub mod object_put;
pub mod object_share;

use std::{collections::BTreeMap, path::PathBuf};

#[derive(Debug)]
pub enum Action {
    ACL {
        key: String,
        acl: Option<String>,
    },
    CreateBucket {
        acl: String,
    },
    DeleteObject {
        key: String,
        upload_id: String,
        bucket: bool,
    },
    ListObjects {
        bucket: Option<String>,
        list_multipart_uploads: bool,
        prefix: Option<String>,
        start_after: Option<String>,
    },
    GetObject {
        key: String,
        get_head: bool,
        dest: Option<String>,
        quiet: bool,
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
        tmp_dir: PathBuf,
    },
    ShareObject {
        key: String,
        expire: usize,
    },
}
