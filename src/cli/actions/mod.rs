pub mod acl;
pub mod bucket;
pub mod monitor;
pub mod object_delete;
pub mod object_du;
pub mod object_get;
pub mod object_list;
pub mod object_put;
pub mod object_share;
pub mod streams;

use crate::cli::age_filter::AgeFilter;
use crate::s3::{S3, actions::ObjectIdentifier};
use std::{collections::BTreeMap, path::PathBuf};

#[derive(Debug, Clone)]
pub struct DeleteGroup {
    pub objects: Vec<ObjectIdentifier>,
    pub s3: S3,
}

#[derive(Debug)]
pub enum StreamCommand {
    List,
    Show { id: String },
    Resume { id: String },
    Clean,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DuGroupBy {
    Day,
}

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
        older_than: Option<AgeFilter>,
        recursive: bool,
        targets: Vec<DeleteGroup>,
        upload_id: String,
    },
    DiskUsage {
        group_by: Option<DuGroupBy>,
        json: bool,
        prefix: Option<String>,
        target: String,
    },
    ListObjects {
        bucket: Option<String>,
        json: bool,
        list_multipart_uploads: bool,
        // max keys,uploads,buckets
        max_kub: Option<String>,
        older_than: Option<AgeFilter>,
        prefix: Option<String>,
        start_after: Option<String>,
    },
    GetObject {
        dest: Option<String>,
        metadata: bool,
        key: String,
        quiet: bool,
        force: bool,
        json: bool,
        versions: bool,
        version: Option<String>,
    },
    PutObject {
        acl: Option<String>,
        buf_size: usize,
        checksum_algorithm: Option<String>,
        file: Option<String>,
        host: String,
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
    Monitor {
        host: String,
        checks: Vec<monitor::MonitorCheck>,
        format: monitor::MonitorOutputFormat,
        exit_on_check_failure: bool,
        number: u8,
    },
    Streams {
        command: StreamCommand,
        config_file: PathBuf,
        json: bool,
        s3m_dir: PathBuf,
        number: u8,
    },
}
