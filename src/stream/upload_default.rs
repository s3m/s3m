use crate::{
    cli::globals::GlobalArgs,
    s3::{S3, actions, checksum::Checksum},
    stream::{db::Db, setup_progress},
};
use anyhow::{Result, anyhow};
use std::{collections::BTreeMap, fmt::Write, path::Path};

pub struct UploadRequest<'a> {
    pub s3: &'a S3,
    pub key: &'a str,
    pub file: &'a Path,
    pub file_size: u64,
    pub sdb: &'a Db,
    pub acl: Option<String>,
    pub meta: Option<BTreeMap<String, String>>,
    pub quiet: bool,
    pub additional_checksum: Option<Checksum>,
    pub globals: GlobalArgs,
}

/// # Errors
/// Will return an error if the upload fails
pub async fn upload(request: UploadRequest<'_>) -> Result<String> {
    let progress_sender = setup_progress(request.quiet, Some(request.file_size)).await;

    let action = actions::PutObject::new(
        request.key,
        request.file,
        request.acl,
        request.meta,
        progress_sender,
        request.additional_checksum,
    );

    let response = action.request(request.s3, request.globals).await?;
    let etag = &response.get("ETag").ok_or_else(|| anyhow!("no etag"))?;
    request.sdb.save_etag(etag)?;

    Ok(response.iter().fold(String::new(), |mut output, (k, v)| {
        let _ = writeln!(output, "{k}: {v}");
        output
    }))
}
