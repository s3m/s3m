use crate::cli::{progressbar::Bar, Db};
use crate::s3::{actions, S3};
use anyhow::{anyhow, Result};
use crossbeam::channel::unbounded;
use std::{cmp::min, collections::BTreeMap, fmt::Write, path::Path};

#[allow(clippy::too_many_arguments)]
pub async fn upload(
    s3: &S3,
    key: &str,
    file: &str,
    file_size: u64,
    sdb: &Db,
    acl: Option<String>,
    meta: Option<BTreeMap<String, String>>,
    quiet: bool,
) -> Result<String> {
    let (sender, receiver) = unbounded::<usize>();
    let channel = if quiet { None } else { Some(sender) };
    let action = actions::PutObject::new(key, Path::new(file), acl, meta, channel);
    // TODO
    //    action.x_amz_acl = Some(String::from("public-read"));

    if !quiet {
        if let Some(pb) = Bar::new(file_size).progress {
            tokio::spawn(async move {
                let mut uploaded = 0;
                while let Ok(i) = receiver.recv() {
                    let new = min(uploaded + i as u64, file_size);
                    uploaded = new;
                    pb.set_position(new);
                }
                pb.finish();
            });
        }
    };

    let response = action.request(s3).await?;
    let etag = &response.get("ETag").ok_or_else(|| anyhow!("no etag"))?;
    sdb.save_etag(etag)?;

    Ok(response.iter().fold(String::new(), |mut output, (k, v)| {
        let _ = writeln!(output, "{}: {}", k, v);
        output
    }))
}
