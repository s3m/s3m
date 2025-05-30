use crate::{
    cli::globals::GlobalArgs,
    s3::{actions, checksum::Checksum, S3},
    stream::{db::Db, setup_progress},
};
use anyhow::{anyhow, Result};
use std::{collections::BTreeMap, fmt::Write, path::Path};

/// # Errors
/// Will return an error if the upload fails
#[allow(clippy::too_many_arguments)]
pub async fn upload(
    s3: &S3,
    key: &str,
    file: &Path,
    file_size: u64,
    sdb: &Db,
    acl: Option<String>,
    meta: Option<BTreeMap<String, String>>,
    quiet: bool,
    additional_checksum: Option<Checksum>,
    globals: GlobalArgs,
) -> Result<String> {
    let progress_sender = setup_progress(quiet, Some(file_size)).await;

    let action = actions::PutObject::new(
        key,
        Path::new(file),
        acl,
        meta,
        progress_sender,
        additional_checksum,
    );

    let response = action.request(s3, globals).await?;
    let etag = &response.get("ETag").ok_or_else(|| anyhow!("no etag"))?;
    sdb.save_etag(etag)?;

    Ok(response.iter().fold(String::new(), |mut output, (k, v)| {
        let _ = writeln!(output, "{k}: {v}");
        output
    }))
}
