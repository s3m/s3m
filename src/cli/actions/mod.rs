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
        acl: Option<String>,
        key: String,
    },
    CreateBucket {
        acl: String,
    },
    DeleteObject {
        bucket: bool,
        key: String,
        upload_id: String,
    },
    ListObjects {
        bucket: Option<String>,
        list_multipart_uploads: bool,
        prefix: Option<String>,
        start_after: Option<String>,
    },
    GetObject {
        dest: Option<String>,
        metadata: bool,
        key: String,
        quiet: bool,
        force: bool,
    },
    PutObject {
        acl: Option<String>,
        buf_size: usize,
        checksum_algorithm: Option<String>,
        file: Option<String>,
        key: String,
        meta: Option<BTreeMap<String, String>>,
        pipe: bool,
        quiet: bool,
        s3m_dir: PathBuf,
        tmp_dir: PathBuf,
        number: u8,
    },
    ShareObject {
        expire: usize,
        key: String,
    },
}
