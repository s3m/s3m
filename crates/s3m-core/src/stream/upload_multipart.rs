use crate::{
    progressbar::Bar,
    s3::RequestOptions,
    s3::{
        S3, actions,
        checksum::{Checksum, ChecksumAlgorithm},
        responses::CompleteMultipartUploadResult,
    },
    stream::{db::Db, iterator::PartIterator, part::Part, retry::backoff_secs, upload_report},
};
use anyhow::{Context, Result, anyhow};
use futures::stream::{FuturesUnordered, StreamExt};
use rkyv::{from_bytes, rancor::Error as RkyvError, to_bytes};
use sled::transaction::{TransactionError, Transactional};
use std::{collections::BTreeMap, path::Path, time::SystemTime};
use tokio::time::{Duration, sleep};

pub struct MultipartUploadRequest<'a> {
    pub s3: &'a S3,
    pub key: &'a str,
    pub file: &'a Path,
    pub file_size: u64,
    /// The file's modification time, in milliseconds since the Unix epoch, when its
    /// size was read and its hash computed. The upload stops if the file is not that
    /// one any more (<https://github.com/s3m/s3m/issues/89>). `None` checks only the
    /// size.
    pub file_mtime: Option<u128>,
    pub chunk_size: u64,
    pub sdb: &'a Db,
    pub acl: Option<String>,
    pub meta: Option<BTreeMap<String, String>>,
    pub quiet: bool,
    pub additional_checksum: Option<Checksum>,
    pub max_requests: u8,
    pub globals: RequestOptions,
}

struct UploadPartRequest<'a> {
    s3: &'a S3,
    key: &'a str,
    file: &'a Path,
    part_number: u16,
    uid: &'a str,
    seek: u64,
    chunk: u64,
    additional_checksum: &'a mut Option<Checksum>,
    globals: RequestOptions,
}

// https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingRESTAPImpUpload.html
// * Initiate Multipart Upload
// * Upload Part
// * Complete Multipart Upload
/// # Errors
/// Will return an error if the upload fails
pub async fn upload_multipart(request: MultipartUploadRequest<'_>) -> Result<String> {
    let source = check_source_size(&request).await?;

    log::debug!(
        "Starting multi part upload:
        key: {}
        file: {}
        file_size: {}
        part size: {}
        acl: {:#?}
        meta: {:#?}
        additional checksum: {:#?}",
        request.key,
        request.file.display(),
        request.file_size,
        request.chunk_size,
        request.acl.as_ref(),
        request.meta.as_ref(),
        request.additional_checksum.as_ref()
    );

    // trees for keeping track of parts to upload
    let db_parts = request.sdb.db_parts()?;
    let db_uploaded = request.sdb.db_uploaded()?;

    let (upload_id, additional_checksum) = start_or_resume_upload(
        request.s3,
        request.sdb,
        actions::CreateMultipartUpload::new(
            request.key,
            request.acl,
            request.meta,
            request.additional_checksum.clone(),
            request.globals.object_lock.clone(),
        ),
        request.additional_checksum,
    )
    .await?;

    log::debug!("upload_id: {upload_id}");

    // Parts saved by a previous run are not created again: the pending ones are sent,
    // and if every part was uploaded but CompleteMultipartUpload failed (S3 can answer
    // 200 OK with an <Error>), only the completion is retried. The parts the saved plan
    // lacks are added, so that it always covers the whole file.
    let plan = complete_part_plan(
        request.sdb,
        request.file_size,
        request.chunk_size,
        additional_checksum.as_ref(),
    );
    check_part_plan(plan, request.s3, request.key, &upload_id, request.sdb).await?;

    // Upload parts progress bar
    let pb = if request.quiet {
        Bar::default()
    } else {
        Bar::new(request.file_size)
    };

    increment_progress_bar(&pb, db_uploaded.len() as u64 * request.chunk_size, None);

    log::info!("Max concurrent requests: {}", request.max_requests);

    let parts_uploaded = upload_pending_parts(
        PendingParts {
            s3: request.s3,
            key: request.key,
            file: request.file,
            upload_id: &upload_id,
            db: request.sdb,
            globals: &request.globals,
            chunk_size: request.chunk_size,
            max_requests: request.max_requests.into(),
        },
        &pb,
    )
    .await;

    if let Err(error) = parts_uploaded {
        return Err(parts_error(request.s3, request.key, &upload_id, request.sdb, error).await);
    }

    // finish progress bar
    increment_progress_bar(&pb, 0, Some(true));

    if !db_parts.is_empty() {
        return Err(anyhow!("could not upload all parts"));
    }

    if let Some(change) = source.change(request.file).await {
        return Err(changed_file_error(
            request.s3,
            request.key,
            Some(&upload_id),
            request.sdb,
            &change,
        )
        .await);
    }

    complete_upload(
        request.s3,
        request.key,
        &upload_id,
        request.sdb,
        request.file_size,
        additional_checksum,
    )
    .await
}

struct SourceFile {
    size: u64,
    modified: SystemTime,
}

impl SourceFile {
    async fn read(file: &Path) -> std::io::Result<Self> {
        let metadata = tokio::fs::metadata(file).await?;
        Ok(Self {
            size: metadata.len(),
            modified: metadata.modified()?,
        })
    }

    fn modified_millis(&self) -> Option<u128> {
        self.modified
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .map(|since| since.as_millis())
    }

    async fn change(&self, file: &Path) -> Option<String> {
        match Self::read(file).await {
            Ok(now) if now.size != self.size => Some(size_change(self.size, now.size)),
            Ok(now) if now.modified != self.modified => {
                Some("modification time changed".to_string())
            }
            Ok(_) => None,
            Err(error) => Some(format!(
                "could not verify the file's size and modification time: {error}"
            )),
        }
    }
}

fn size_change(before: u64, after: u64) -> String {
    format!("size changed from {before} to {after} bytes")
}

async fn check_source_size(request: &MultipartUploadRequest<'_>) -> Result<SourceFile> {
    let source = SourceFile::read(request.file).await?;
    let change = if source.size == request.file_size {
        // The file can also have been rewritten with the same size while its hash was
        // computed, before the upload started.
        request
            .file_mtime
            .filter(|expected| source.modified_millis() != Some(*expected))
            .map(|_| "modification time changed since it was read".to_string())
    } else {
        Some(size_change(request.file_size, source.size))
    };
    if let Some(change) = change {
        let upload_id = request.sdb.upload_id()?;
        return Err(changed_file_error(
            request.s3,
            request.key,
            upload_id.as_deref(),
            request.sdb,
            &change,
        )
        .await);
    }
    Ok(source)
}

async fn changed_file_error(
    s3: &S3,
    key: &str,
    upload_id: Option<&str>,
    db: &Db,
    change: &str,
) -> anyhow::Error {
    let reason = format!("the file changed during the upload: {change}");
    let Some(upload_id) = upload_id else {
        return anyhow!("{reason}. Run the upload again.");
    };
    match abort_and_forget(s3, key, upload_id, db).await {
        Ok(()) => anyhow!("{reason}. The multipart upload was aborted; run the upload again."),
        Err(abort) => anyhow!(
            "{reason}. The multipart upload could not be aborted ({abort}); abort it with \
             `s3m rm <host>/<bucket>/{key} --abort {upload_id}` and run the upload again."
        ),
    }
}

async fn check_part_plan(
    plan: Result<()>,
    s3: &S3,
    key: &str,
    upload_id: &str,
    db: &Db,
) -> Result<()> {
    // Only parts that can't belong to this file end the upload. Anything else (the
    // state could not be read or written) leaves it as it is, to be resumed.
    match plan {
        Err(error) => Err(match error.downcast::<PlanMismatch>() {
            Ok(mismatch) => abort_unusable_state(s3, key, upload_id, db, &mismatch).await,
            Err(error) => error,
        }),
        Ok(()) => Ok(()),
    }
}

/// Complete the multipart upload once every part is uploaded, and return its report.
/// An upload whose parts don't hold the whole file is never completed.
async fn complete_upload(
    s3: &S3,
    key: &str,
    upload_id: &str,
    db: &Db,
    file_size: u64,
    additional_checksum: Option<Checksum>,
) -> Result<String> {
    let uploaded = db.uploaded_parts()?;
    let ranges = db.uploaded_part_ranges()?;
    let cover = if uploaded.keys().eq(ranges.keys()) {
        parts_cover(&ranges, file_size)
    } else {
        Err(PlanMismatch(
            "the uploaded parts changed while they were read".to_string(),
        ))
    };
    if let Err(mismatch) = cover {
        return Err(anyhow!(
            "the uploaded parts don't match the file ({mismatch}); the multipart upload was \
             not completed"
        ));
    }

    let action =
        actions::CompleteMultipartUpload::new(key, upload_id, uploaded, additional_checksum);
    let checksum = action.checksum()?;
    let rs = match action.request(s3).await {
        Ok(rs) => rs,
        Err(error) if upload_is_gone(&error, true) => {
            return Err(forget_gone_upload(s3, db, &error.to_string()));
        }
        Err(error) => return Err(error.into()),
    };

    verify_completion_checksum(&rs, checksum.as_ref(), db)?;

    // Save the returned ETag first: killed between the two steps, the upload is
    // recorded as complete, not as an upload to resume that the server no longer has.
    db.save_etag(&rs.e_tag)?;

    // cleanup uploads tree
    db.db_uploaded()?.clear()?;

    // upload finished
    log::info!("Upload finished, ETag: {}", rs.e_tag);

    Ok(upload_report(rs, checksum.as_ref()))
}

fn verify_completion_checksum(
    result: &CompleteMultipartUploadResult,
    checksum: Option<&Checksum>,
    db: &Db,
) -> Result<()> {
    let Some(local) = checksum else {
        return Ok(());
    };
    let remote = match local.algorithm {
        ChecksumAlgorithm::Crc32 => result.checksum_crc32.as_deref(),
        ChecksumAlgorithm::Crc32c => result.checksum_crc32c.as_deref(),
        ChecksumAlgorithm::Sha1 => result.checksum_sha1.as_deref(),
        ChecksumAlgorithm::Sha256 => result.checksum_sha256.as_deref(),
        ChecksumAlgorithm::Md5 => None,
    };
    // XML may put white space around the value; it is not part of it.
    let Some(remote) = remote.map(str::trim).filter(|remote| {
        !remote.is_empty()
            && *remote != local.checksum
            && is_composite_checksum(remote) == is_composite_checksum(&local.checksum)
    }) else {
        return Ok(());
    };

    let reason = format!(
        "the object was stored but does not have the {} checksum s3m computed (s3m: {}, server: {remote})",
        local.algorithm.name(),
        local.checksum
    );
    forget_aborted_upload(db)
        .with_context(|| format!("{reason}. Could not clear the saved upload state"))?;
    Err(anyhow!(reason))
}

fn is_composite_checksum(value: &str) -> bool {
    value.rsplit_once('-').is_some_and(|(_, parts)| {
        !parts.is_empty() && parts.bytes().all(|byte| byte.is_ascii_digit())
    })
}

/// The saved parts are not the parts of this file.
#[derive(Debug)]
struct PlanMismatch(String);

impl std::fmt::Display for PlanMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for PlanMismatch {}

/// S3 allows 10,000 parts in a multipart upload.
const MAX_PARTS: u64 = 10_000;

/// Make the saved part plan cover the file: plan every part of a new upload, and add
/// the parts a saved plan lacks. A plan can be cut short when s3m is killed while it
/// writes it (older versions wrote it part by part); resuming it used to complete the
/// upload without the missing parts (<https://github.com/s3m/s3m/issues/83>).
///
/// The saved parts keep their size, whatever `chunk_size` this run was given: the
/// parts already uploaded were cut with it.
///
/// # Errors
///
/// Will return a [`PlanMismatch`], with what doesn't match, if the saved parts are not
/// parts of this file cut at one part size; any other error if the state can't be
/// read or written
fn complete_part_plan(
    db: &Db,
    file_size: u64,
    chunk_size: u64,
    checksum: Option<&Checksum>,
) -> Result<()> {
    let mismatch = |what: String| anyhow::Error::new(PlanMismatch(what));

    let mut saved = db.uploaded_part_ranges()?;
    for (number, range) in db.pending_part_ranges()? {
        if let Some(uploaded) = saved.insert(number, range)
            && uploaded != range
        {
            return Err(mismatch(format!(
                "part {number} is saved with two different ranges"
            )));
        }
    }

    let part_size = match saved.get(&1) {
        Some(&(_, chunk)) => chunk,
        None if saved.is_empty() => chunk_size,
        None => {
            return Err(mismatch(
                "part 1 is missing from the saved parts".to_string(),
            ));
        }
    };
    // A saved size that can't be a part size of this file must not be planned with.
    if !saved.is_empty() && (part_size == 0 || file_size.div_ceil(part_size) > MAX_PARTS) {
        return Err(mismatch(format!(
            "part 1 is saved as {part_size} bytes, which can't be the part size of a file of \
             {file_size} bytes"
        )));
    }

    let mut missing = Vec::new();
    let mut planned = 0;
    for (number, seek, chunk) in PartIterator::new(file_size, part_size) {
        planned += 1;
        match saved.get(&number) {
            Some(&range) if range == (seek, chunk) => {}
            Some(&(saved_seek, saved_chunk)) => {
                return Err(mismatch(format!(
                    "part {number} is saved as {saved_chunk} bytes at {saved_seek}, the file \
                     has {chunk} bytes at {seek} for it"
                )));
            }
            None => missing.push((number, seek, chunk)),
        }
    }
    if saved.len() + missing.len() != planned {
        return Err(mismatch(format!(
            "{} parts are saved, the file of {file_size} bytes has {planned}",
            saved.len()
        )));
    }

    if !missing.is_empty() {
        if !saved.is_empty() {
            log::warn!(
                "The saved state had {} of {planned} parts; added the missing ones",
                saved.len()
            );
        }
        db.create_parts(missing, checksum)?;
    }
    Ok(())
}

/// Whether the parts, `(seek, chunk)` by number, are the whole file: numbered from 1
/// without gaps, each starting where the previous one ends, ending at `file_size`.
///
/// # Errors
///
/// Will return `Err` with what doesn't match
fn parts_cover(
    parts: &BTreeMap<u16, (u64, u64)>,
    file_size: u64,
) -> std::result::Result<(), PlanMismatch> {
    let mut end = 0;
    for (expected, (&number, &(seek, chunk))) in (1_u16..).zip(parts) {
        if number != expected {
            return Err(PlanMismatch(format!("part {expected} is missing")));
        }
        if seek != end {
            return Err(PlanMismatch(format!(
                "part {number} starts at byte {seek}, not at {end}"
            )));
        }
        end += chunk;
    }
    if end != file_size {
        return Err(PlanMismatch(format!(
            "they hold {end} bytes, the file has {file_size}"
        )));
    }
    Ok(())
}

/// The saved parts are not parts of this file, so the upload they belong to can't be
/// finished: it is aborted and forgotten, and the next run starts a new one. Returns
/// the error to report. When the abort fails the upload is still open on the server:
/// the state is kept, and the error says how to abort it (a run after that finds the
/// upload gone and forgets it).
async fn abort_unusable_state(
    s3: &S3,
    key: &str,
    upload_id: &str,
    db: &Db,
    mismatch: &PlanMismatch,
) -> anyhow::Error {
    match abort_and_forget(s3, key, upload_id, db).await {
        Ok(()) => anyhow!(
            "the saved state of this upload doesn't match the file ({mismatch}). The \
             multipart upload was aborted; run the upload again to start a new one."
        ),
        Err(abort) => anyhow!(
            "the saved state of this upload doesn't match the file ({mismatch}). The \
             multipart upload could not be aborted ({abort}); abort it with \
             `s3m rm <host>/<bucket>/{key} --abort {upload_id}` and run the upload again."
        ),
    }
}

/// Abort the multipart upload and forget it, with the parts saved for it. An upload
/// the server no longer has counts as aborted.
///
/// # Errors
///
/// Will return the server's error if the abort fails; the state is kept then
async fn abort_and_forget(
    s3: &S3,
    key: &str,
    upload_id: &str,
    db: &Db,
) -> std::result::Result<(), crate::s3::Error> {
    // Only where the upload was started: another server never had it, and whatever it
    // answers (it may well accept the abort) says nothing about the real upload.
    if !started_here(s3, db) {
        return Err(crate::s3::Error::Other(
            "the host's endpoint, bucket or keys are not the ones this upload was started with"
                .to_string(),
        ));
    }
    match actions::AbortMultipartUpload::new(key, upload_id)
        .request(s3)
        .await
    {
        Ok(_) => {}
        // Already gone on the server.
        Err(abort) if upload_is_gone(&abort, false) => {}
        Err(abort) => return Err(abort),
    }
    if let Err(forget) = forget_aborted_upload(db) {
        log::warn!("Could not forget the aborted multipart upload: {forget}");
    }
    Ok(())
}

struct PendingParts<'a> {
    s3: &'a S3,
    key: &'a str,
    file: &'a Path,
    upload_id: &'a str,
    db: &'a Db,
    globals: &'a RequestOptions,
    chunk_size: u64,
    max_requests: usize,
}

/// Upload the parts still pending, `max_requests` at a time. The parts in flight
/// live in this function: when it returns an error they are dropped, so this side
/// starts no more parts, retries nothing and reads no more answers. A request already
/// on the wire may still reach the server.
async fn upload_pending_parts(parts: PendingParts<'_>, pb: &Bar) -> Result<()> {
    let mut tasks = FuturesUnordered::new();

    for part in parts
        .db
        .db_parts()?
        .iter()
        .values()
        .filter_map(Result::ok)
        .map(|p| from_bytes::<Part, RkyvError>(&p).map_err(anyhow::Error::from))
    {
        let part: Part = part?;

        log::info!("Task push part: {}", part.get_number());

        // spawn task (upload part)
        tasks.push(upload_part(
            parts.s3,
            parts.key,
            parts.file,
            parts.upload_id,
            parts.db,
            part,
            parts.globals,
        ));

        await_tasks(&mut tasks, pb, parts.chunk_size, parts.max_requests).await?;
    }

    // wait for the remaining tasks
    await_remaining_tasks(&mut tasks, pb, parts.chunk_size).await
}

/// The upload id to send parts to, and the additional checksum to send them with.
///
/// An upload in progress keeps the algorithm it was created with (or none), whatever
/// this run was given: its parts and its completion must use the same one. Otherwise
/// the multipart upload is initiated, and its id and algorithm are recorded for a
/// resume.
async fn start_or_resume_upload(
    s3: &S3,
    db: &Db,
    create: actions::CreateMultipartUpload<'_>,
    additional_checksum: Option<Checksum>,
) -> Result<(String, Option<Checksum>)> {
    if let Some(upload_id) = db.upload_id()? {
        return Ok((upload_id, resumed_checksum(db, additional_checksum)?));
    }

    let response = create.request(s3).await?;

    // A new upload id starts from nothing. The part trees are shared by every upload of
    // the same content, so parts left by an interrupted upload (another key, or this key
    // before the file was touched) would otherwise be planned or completed again; a stale
    // part in CompleteMultipartUpload fails it with InvalidPart.
    db.db_parts()?.clear()?;
    db.db_uploaded()?.clear()?;
    // Recorded before the upload id, so a resume always finds the algorithm of the
    // upload id it finds.
    db.save_upload_checksum(
        additional_checksum
            .as_ref()
            .map(|checksum| &checksum.algorithm),
    )?;
    // save the upload_id to resume if required
    db.save_upload_id(&response.upload_id)?;
    Ok((response.upload_id, additional_checksum))
}

/// The additional checksum of a multipart upload that is being resumed: the one the
/// upload was created with (see [`Db::resume_checksum`]), whatever this run asked for.
fn resumed_checksum(db: &Db, requested: Option<Checksum>) -> Result<Option<Checksum>> {
    let requested = requested.map(|checksum| checksum.algorithm);
    let used = db.resume_checksum(requested.as_ref())?;
    if requested.is_some() && requested != used {
        log::warn!(
            "Resuming with the checksum the upload was created with ({}), not {}",
            used.as_ref().map_or("none", ChecksumAlgorithm::name),
            requested.as_ref().map_or("none", ChecksumAlgorithm::name)
        );
    }
    Ok(used.map(Checksum::new))
}

#[cfg(test)]
#[path = "multipart_scheduling_tests.rs"]
mod scheduling_tests;

#[cfg(test)]
#[path = "multipart_attempts_tests.rs"]
mod attempts_tests;

async fn await_tasks<T>(
    tasks: &mut FuturesUnordered<T>,
    pb: &Bar,
    chunk_size: u64,
    max_requests: usize,
) -> Result<()>
where
    T: std::future::Future<Output = Result<usize>> + Send,
{
    log::debug!("Running tasks: {}", tasks.len());

    // limit to available parallelism - 2 or 1 (see tools::default_concurrency)
    while tasks.len() >= max_requests {
        if let Some(r) = tasks.next().await {
            r.map_err(flatten_error)?;
            increment_progress_bar(pb, chunk_size, None);
        }
    }

    Ok(())
}

// consume remaining tasks
async fn await_remaining_tasks<T>(
    tasks: &mut FuturesUnordered<T>,
    pb: &Bar,
    chunk_size: u64,
) -> Result<()>
where
    T: std::future::Future<Output = Result<usize>> + Send,
{
    log::debug!("Remaining tasks: {}", tasks.len());

    while let Some(r) = tasks.next().await {
        r.map_err(flatten_error)?;
        increment_progress_bar(pb, chunk_size, None);
    }
    Ok(())
}

/// What to report when the parts could not all be uploaded. Two errors end the upload
/// for good, and its saved state with it: a part checksum the server doesn't
/// implement, and a multipart upload the server no longer has.
async fn parts_error(
    s3: &S3,
    key: &str,
    upload_id: &str,
    db: &Db,
    error: anyhow::Error,
) -> anyhow::Error {
    let error = match error.downcast::<UnsupportedChecksum>() {
        Ok(unsupported) => {
            return abort_unsupported_checksum(s3, key, upload_id, db, unsupported).await;
        }
        Err(error) => error,
    };
    match error.downcast::<UploadGone>() {
        Ok(gone) => forget_gone_upload(s3, db, &gone.answer),
        Err(error) => error,
    }
}

/// A part's error as one message, as it has always been reported. The markers that
/// end the upload keep their type: they are acted on once the parts stop.
fn flatten_error(error: anyhow::Error) -> anyhow::Error {
    if error.is::<UnsupportedChecksum>() || error.is::<UploadGone>() {
        error
    } else {
        anyhow!("{error}")
    }
}

fn increment_progress_bar(pb: &Bar, chunk_size: u64, finish: Option<bool>) {
    if let Some(pb) = pb.progress.as_ref() {
        pb.inc(chunk_size);

        if finish == Some(true) {
            pb.finish();
        }
    }
}

async fn upload_part(
    s3: &S3,
    key: &str,
    file: &Path,
    uid: &str,
    db: &Db,
    part: Part,
    globals: &RequestOptions,
) -> Result<usize> {
    let unprocessed = db.db_parts()?;
    let processed = db.db_uploaded()?;

    let mut additional_checksum = part.get_checksum();

    // do request to get the ETag and update the checksum if required
    let part_number = part.get_number();

    let mut etag: String = String::new();

    // Retry with exponential backoff, capped like streamed parts
    let attempts = globals.retries.max(1);
    for attempt in 1..=attempts {
        let backoff_time = backoff_secs(attempt);
        if attempt > 1 {
            log::warn!("Error uploading part: {part_number}, retrying in {backoff_time} seconds");

            sleep(Duration::from_secs(backoff_time)).await;
        }

        match try_upload_part(UploadPartRequest {
            s3,
            key,
            file,
            part_number,
            uid,
            seek: part.get_seek(),
            chunk: part.get_chunk(),
            additional_checksum: &mut additional_checksum,
            globals: globals.clone(),
        })
        .await
        {
            Ok(e) => {
                etag = e;

                log::info!(
                    "Uploaded part: {}, etag: {}{}",
                    part_number,
                    etag,
                    additional_checksum
                        .as_ref()
                        .map_or_default(|c| format!(" additional_checksum: {}", c.checksum))
                );

                break;
            }

            Err(e) => {
                // The upload this part belongs to is gone: no retry can succeed.
                if is_no_such_upload(&e) {
                    return Err(UploadGone {
                        answer: e.to_string(),
                    }
                    .into());
                }

                // Only before any part was accepted: after that, the server does
                // support the checksum. No retry can succeed; the upload is aborted
                // once, after every part in flight has stopped.
                if let Some(checksum) = additional_checksum
                    .as_ref()
                    .filter(|_| is_not_implemented(&e) && processed.is_empty())
                {
                    return Err(UnsupportedChecksum {
                        algorithm: checksum.algorithm.name(),
                        answer: e.to_string(),
                    }
                    .into());
                }

                log::error!(
                    "Error uploading part: {}, attempt {}/{} failed: {}",
                    part.get_number(),
                    attempt,
                    attempts,
                    e
                );

                if attempt == attempts {
                    return Err(e);
                }
            }
        }
    }

    // update part with the etag and checksum if any
    let part = part.set_etag(etag).set_checksum(additional_checksum);

    let rkyv_part = to_bytes::<RkyvError>(&part)?;

    // move part to uploaded
    (&unprocessed, &processed)
        .transaction(|(unprocessed, processed)| {
            unprocessed.remove(&part_number.to_be_bytes())?;
            processed.insert(&part_number.to_be_bytes(), rkyv_part.as_slice())?;
            Ok(())
        })
        .map_err(|err| match err {
            TransactionError::Abort(err) | TransactionError::Storage(err) => err,
        })?;

    db.flush()
}

/// A 501 answer with the S3 error code `NotImplemented` that names the checksum: the
/// server doesn't support it, so sending the part again can't succeed. Cloudflare R2
/// answers `Header 'x-amz-checksum-sha256' with value '...' not implemented`.
///
/// A bare 501 from something in between (a proxy) is not that, and neither is a
/// `NotImplemented` about something else (a storage class, for example): the upload
/// must not be aborted for it.
fn is_not_implemented(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<crate::s3::error::Error>()
        .is_some_and(|error| {
            error.status() == Some(501)
                && error.code() == Some("NotImplemented")
                && error.to_string().to_ascii_lowercase().contains("checksum")
        })
}

/// The server says it has no multipart upload with this id: the S3 error code
/// `NoSuchUpload` with status 404, or in the body of a 200 answer to
/// `CompleteMultipartUpload` (`on_completion`). The same code with another status (a
/// gateway's 503, for example) doesn't prove the upload is gone.
fn upload_is_gone(error: &crate::s3::error::Error, on_completion: bool) -> bool {
    error.code() == Some("NoSuchUpload")
        && (error.status() == Some(404) || (on_completion && error.status() == Some(200)))
}

fn is_no_such_upload(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<crate::s3::error::Error>()
        .is_some_and(|error| upload_is_gone(error, false))
}

/// The server no longer has the multipart upload: it was aborted (by hand, or by a
/// lifecycle rule for incomplete uploads), or an earlier run completed it and lost the
/// answer.
#[derive(Debug)]
struct UploadGone {
    /// The server's answer.
    answer: String,
}

impl std::fmt::Display for UploadGone {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.answer)
    }
}

impl std::error::Error for UploadGone {}

/// New states identify the endpoint and bucket. Older state keys also depend on
/// the credentials that were in use when the upload started.
fn started_here(s3: &S3, db: &Db) -> bool {
    let Some((host, _)) = db.state_key().split_once(' ') else {
        return false;
    };
    if host.starts_with("v2-") {
        s3.matches_upload_host(host)
    } else {
        s3.hash().get(..8) == Some(host)
    }
}

/// Nothing can be sent to an upload the server no longer has, so every later run would
/// fail the same way (<https://github.com/s3m/s3m/issues/84>). Its saved state is
/// removed, and the next run starts a new upload. Returns the error to report.
///
/// The state is kept when the answer comes from somewhere else than the upload was
/// started on: `streams resume` opens a saved state by its key, whatever the host's
/// configuration is now, and the server that has the upload may still have it.
fn forget_gone_upload(s3: &S3, db: &Db, answer: &str) -> anyhow::Error {
    if !started_here(s3, db) {
        return anyhow!(
            "the server has no such multipart upload, but the host's endpoint, bucket or \
             keys are not the ones this upload was started with, so its saved state was \
             kept. To resume or abort it, give the host the settings it was started \
             with.\n{answer}"
        );
    }
    if let Err(forget) = forget_aborted_upload(db) {
        return anyhow!(
            "the multipart upload saved for this file no longer exists on the server, and \
             its saved state could not be removed ({forget}).\n{answer}"
        );
    }
    anyhow!(
        "the multipart upload saved for this file no longer exists on the server: it was \
         aborted or it expired, or an earlier run completed it and lost the answer. Its \
         saved state was removed; run the upload again to start a new one.\n{answer}"
    )
}

/// A part's additional checksum was rejected as not implemented, before any part was
/// accepted (Cloudflare R2 does this for SHA-1 and SHA-256).
#[derive(Debug)]
struct UnsupportedChecksum {
    algorithm: &'static str,
    /// The server's answer.
    answer: String,
}

impl std::fmt::Display for UnsupportedChecksum {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.answer)
    }
}

impl std::error::Error for UnsupportedChecksum {}

/// The server doesn't implement the additional checksum on parts. Neither a retry nor
/// a resume can succeed, and the upload in progress would keep its algorithm, so it is
/// aborted and forgotten: the next run starts a new upload. Called once, after the
/// other parts were dropped (see [`upload_pending_parts`]). Returns the error to
/// report.
///
/// Nothing is aborted when a part is recorded as uploaded: a safeguard, since the
/// rejected part already saw none. It can't see a part the server accepted but whose
/// answer was dropped with its task; a server that rejects one part's checksum this
/// way rejects them all. When the abort fails the upload is still open on the server:
/// the state is kept, and the error says how to abort it.
async fn abort_unsupported_checksum(
    s3: &S3,
    key: &str,
    upload_id: &str,
    db: &Db,
    unsupported: UnsupportedChecksum,
) -> anyhow::Error {
    let UnsupportedChecksum { algorithm, answer } = unsupported;
    if !db.db_uploaded().is_ok_and(|uploaded| uploaded.is_empty()) {
        return anyhow!("{answer}");
    }
    let advice = "Use another --checksum algorithm (Cloudflare R2 accepts crc32 and crc32c on \
                  multipart uploads), or a --buffer larger than the file to upload it as a \
                  single part.";

    match abort_and_forget(s3, key, upload_id, db).await {
        Ok(()) => anyhow!(
            "the server doesn't support {algorithm} checksums on the parts of a multipart \
             upload, and the upload was aborted. {advice}\n{answer}"
        ),
        Err(abort) => anyhow!(
            "the server doesn't support {algorithm} checksums on the parts of a multipart \
             upload. The multipart upload could not be aborted ({abort}); abort it with \
             `s3m rm <host>/<bucket>/{key} --abort {upload_id}`. {advice}\n{answer}"
        ),
    }
}

/// Forget an upload the server no longer has, with the parts planned for it.
fn forget_aborted_upload(db: &Db) -> Result<()> {
    db.forget_upload()?;
    db.db_parts()?.clear()?;
    db.db_uploaded()?.clear()?;
    db.flush()?;
    Ok(())
}

async fn try_upload_part(request: UploadPartRequest<'_>) -> Result<String> {
    let action = actions::UploadPart::new(
        request.key,
        request.file,
        request.part_number,
        request.uid,
        request.seek,
        request.chunk,
        request.additional_checksum.as_mut(),
    );

    log::debug!("Uploading part: {}", request.part_number);

    Ok(action.request(request.s3, &request.globals).await?)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::{MultipartUploadRequest, complete_upload, parts_cover, upload_multipart};
    use crate::{
        s3::{
            Credentials, Region, RequestOptions, S3,
            checksum::{Checksum, ChecksumAlgorithm},
        },
        stream::{db::Db, part::Part},
    };
    use mockito::{Matcher, Server};
    use rkyv::{rancor::Error as RkyvError, to_bytes};
    use secrecy::SecretString;
    use std::{fs, io::Write, time::UNIX_EPOCH};

    // Regression: a fresh upload id kept the parts another upload of the same content had
    // already sent, and CompleteMultipartUpload listed them (S3 answers InvalidPart).
    #[tokio::test]
    async fn new_upload_does_not_complete_with_stale_uploaded_parts() {
        let mut server = Server::new_async().await;
        let s3 = S3::new(
            &Credentials::new("AKIA", &SecretString::new("secret".into())),
            &Region::custom("us-east-1", server.url()),
            Some("bucket".to_string()),
            false,
        );
        let dir = tempfile::tempdir().unwrap();
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"0123456789").unwrap();

        let db = Db::new(&s3, "key", "checksum", 1, dir.path()).unwrap();
        let stale = Part::new(3, 10, 5, None).set_etag("\"STALE\"".to_string());
        db.db_uploaded()
            .unwrap()
            .insert(
                3_u16.to_be_bytes(),
                to_bytes::<RkyvError>(&stale).unwrap().as_slice(),
            )
            .unwrap();

        let _initiate = server
            .mock("POST", "/bucket/key")
            .match_query(Matcher::UrlEncoded("uploads".into(), String::new()))
            .with_body("<InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>key</Key><UploadId>new-id</UploadId></InitiateMultipartUploadResult>")
            .create_async()
            .await;
        let parts = server
            .mock("PUT", "/bucket/key")
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("uploadId".into(), "new-id".into()),
                Matcher::Regex("partNumber=[12]".into()),
            ]))
            .with_header("ETag", "\"fresh\"")
            .expect(2)
            .create_async()
            .await;
        let complete = server
            .mock("POST", "/bucket/key")
            .match_query(Matcher::UrlEncoded("uploadId".into(), "new-id".into()))
            .with_body_from_request(|request| {
                let body = String::from_utf8_lossy(request.body().unwrap()).to_string();
                if body.contains("STALE") {
                    b"stale part sent".to_vec()
                } else {
                    b"<CompleteMultipartUploadResult><Location>l</Location><Bucket>bucket</Bucket><Key>key</Key><ETag>\"done-2\"</ETag></CompleteMultipartUploadResult>".to_vec()
                }
            })
            .create_async()
            .await;

        let result = upload_multipart(MultipartUploadRequest {
            s3: &s3,
            key: "key",
            file: file.path(),
            file_size: 10,
            file_mtime: None,
            chunk_size: 5,
            sdb: &db,
            acl: None,
            meta: None,
            quiet: true,
            additional_checksum: None,
            max_requests: 1,
            globals: RequestOptions::default(),
        })
        .await;

        parts.assert_async().await;
        complete.assert_async().await;
        assert_eq!(result.unwrap(), "ETag: \"done-2\"");
    }

    // When every part was uploaded but CompleteMultipartUpload failed (for example a
    // 200 OK with an <Error> document), the next run only retries the completion.
    #[tokio::test]
    async fn completion_retry_does_not_upload_parts_again() {
        let mut server = Server::new_async().await;
        let s3 = S3::new(
            &Credentials::new("AKIA", &SecretString::new("secret".into())),
            &Region::custom("us-east-1", server.url()),
            Some("bucket".to_string()),
            false,
        );
        let dir = tempfile::tempdir().unwrap();
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"0123456789").unwrap();

        let db = Db::new(&s3, "key", "checksum", 1, dir.path()).unwrap();
        db.save_upload_id("uid").unwrap();
        for (number, seek) in [(1_u16, 0), (2, 5)] {
            let part = Part::new(number, seek, 5, None).set_etag(format!("\"p{number}\""));
            db.db_uploaded()
                .unwrap()
                .insert(
                    number.to_be_bytes(),
                    to_bytes::<RkyvError>(&part).unwrap().as_slice(),
                )
                .unwrap();
        }

        let parts = server
            .mock("PUT", "/bucket/key")
            .match_query(Matcher::Any)
            .expect(0)
            .create_async()
            .await;
        let complete = server
            .mock("POST", "/bucket/key")
            .match_query(Matcher::UrlEncoded("uploadId".into(), "uid".into()))
            .match_body(Matcher::Regex(
                "<ETag>\"p1\"</ETag><PartNumber>1</PartNumber>.*<ETag>\"p2\"</ETag><PartNumber>2</PartNumber>".into(),
            ))
            .with_body("<CompleteMultipartUploadResult><Location>l</Location><Bucket>bucket</Bucket><Key>key</Key><ETag>\"done-2\"</ETag></CompleteMultipartUploadResult>")
            .expect(1)
            .create_async()
            .await;

        let result = upload_multipart(MultipartUploadRequest {
            s3: &s3,
            key: "key",
            file: file.path(),
            file_size: 10,
            file_mtime: None,
            chunk_size: 5,
            sdb: &db,
            acl: None,
            meta: None,
            quiet: true,
            additional_checksum: None,
            max_requests: 1,
            globals: RequestOptions::default(),
        })
        .await;

        parts.assert_async().await;
        complete.assert_async().await;
        assert_eq!(result.unwrap(), "ETag: \"done-2\"");
    }

    const SHA256_PART_1: &str = "xWX+A8qbYkLgHf3e/pu6PZiycOGc0C/YXOr3XislvxI=";
    const SHA256_PART_2: &str = "92BDp07DO2rvuyiQUPr3qo1IIJVHc5fj5jNFEl1J9Sc=";
    // base64(sha256(sha256("01234") || sha256("56789")))-2
    const SHA256_COMPOSITE: &str = "N7HU4sCX924sGwPEjjVrehEoVZGWFs9+jec8UebzU+k=-2";

    fn sha256(checksum: &str) -> Checksum {
        Checksum {
            algorithm: ChecksumAlgorithm::Sha256,
            checksum: checksum.to_string(),
        }
    }

    fn checksum_s3(server: &Server) -> S3 {
        S3::new(
            &Credentials::new("AKIA", &SecretString::new("secret".into())),
            &Region::custom("us-east-1", server.url()),
            Some("bucket".to_string()),
            false,
        )
    }

    async fn resume_without_checksum(s3: &S3, db: &Db, file: &std::path::Path) -> String {
        upload_multipart(MultipartUploadRequest {
            s3,
            key: "key",
            file,
            file_size: 10,
            file_mtime: None,
            chunk_size: 5,
            sdb: db,
            acl: None,
            meta: None,
            quiet: true,
            additional_checksum: None,
            max_requests: 1,
            globals: RequestOptions::default(),
        })
        .await
        .unwrap()
    }

    // <https://github.com/s3m/s3m/issues/77>: `streams resume` doesn't know the
    // --checksum of the original upload. A state written before the algorithm was
    // saved still has it on its parts: the pending part is sent with its checksum, and
    // the report has the checksum line like the original upload.
    #[tokio::test]
    async fn resume_recovers_the_checksum_algorithm_from_the_parts() {
        let mut server = Server::new_async().await;
        let s3 = checksum_s3(&server);
        let dir = tempfile::tempdir().unwrap();
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"0123456789").unwrap();

        let db = Db::new(&s3, "key", "checksum", 1, dir.path()).unwrap();
        db.save_upload_id("uid").unwrap();
        let uploaded = Part::new(1, 0, 5, Some(sha256(SHA256_PART_1))).set_etag("\"p1\"".into());
        db.db_uploaded()
            .unwrap()
            .insert(
                1_u16.to_be_bytes(),
                to_bytes::<RkyvError>(&uploaded).unwrap().as_slice(),
            )
            .unwrap();
        db.create_part(2, 5, 5, Some(Checksum::new(ChecksumAlgorithm::Sha256)))
            .unwrap();

        let part = server
            .mock("PUT", "/bucket/key")
            .match_query(Matcher::UrlEncoded("partNumber".into(), "2".into()))
            .match_header("x-amz-checksum-sha256", SHA256_PART_2)
            .match_header("content-md5", Matcher::Missing)
            .with_header("etag", "\"p2\"")
            .expect(1)
            .create_async()
            .await;
        let complete = server
            .mock("POST", "/bucket/key")
            .match_query(Matcher::UrlEncoded("uploadId".into(), "uid".into()))
            .match_body(Matcher::Regex(format!(
                "<ChecksumSHA256>{}</ChecksumSHA256>.*<ChecksumSHA256>{}</ChecksumSHA256>",
                regex_escape(SHA256_PART_1),
                regex_escape(SHA256_PART_2)
            )))
            .with_body("<CompleteMultipartUploadResult><Location>l</Location><Bucket>bucket</Bucket><Key>key</Key><ETag>\"done-2\"</ETag></CompleteMultipartUploadResult>")
            .expect(1)
            .create_async()
            .await;

        let report = resume_without_checksum(&s3, &db, file.path()).await;

        part.assert_async().await;
        complete.assert_async().await;
        assert_eq!(
            report,
            format!("ETag: \"done-2\"\nsha256 (base64): {SHA256_COMPOSITE}")
        );
    }

    fn regex_escape(text: &str) -> String {
        text.replace('+', "\\+")
    }

    const COMPLETED: &str = "<CompleteMultipartUploadResult><Location>l</Location><Bucket>bucket</Bucket><Key>key</Key><ETag>\"done-2\"</ETag></CompleteMultipartUploadResult>";

    const CRC32_COMPOSITE: &str = "3O3KWw==-2";

    async fn complete_with_crc32(response_value: Option<&str>, mismatch: bool) {
        let mut server = Server::new_async().await;
        let s3 = checksum_s3(&server);
        let dir = tempfile::tempdir().unwrap();
        let db = Db::new(&s3, "key", "checksum", 1, dir.path()).unwrap();
        db.save_upload_id("uid").unwrap();
        db.save_upload_checksum(Some(&ChecksumAlgorithm::Crc32))
            .unwrap();
        for (number, offset, crc32) in [(1_u16, 0, "3aRwJA=="), (2, 5, "Ex2gcA==")] {
            let checksum = Checksum {
                algorithm: ChecksumAlgorithm::Crc32,
                checksum: crc32.into(),
            };
            let part = Part::new(number, offset, 5, Some(checksum)).set_etag(format!("p{number}"));
            db.db_uploaded()
                .unwrap()
                .insert(
                    number.to_be_bytes(),
                    to_bytes::<RkyvError>(&part).unwrap().as_slice(),
                )
                .unwrap();
        }

        let body = response_value.map_or_else(
            || COMPLETED.to_string(),
            |crc32| {
                const END: &str = "</CompleteMultipartUploadResult>";
                COMPLETED.replace(END, &format!("<ChecksumCRC32>{crc32}</ChecksumCRC32>{END}"))
            },
        );
        let complete = server
            .mock("POST", "/bucket/key")
            .match_query(Matcher::UrlEncoded("uploadId".into(), "uid".into()))
            .with_body(body)
            .expect(1)
            .create_async()
            .await;
        let abort = server
            .mock("DELETE", "/bucket/key")
            .match_query(Matcher::Any)
            .expect(0)
            .create_async()
            .await;

        let result = complete_upload(
            &s3,
            "key",
            "uid",
            &db,
            10,
            Some(Checksum::new(ChecksumAlgorithm::Crc32)),
        )
        .await;

        complete.assert_async().await;
        abort.assert_async().await;
        if mismatch {
            let error = result.unwrap_err().to_string();
            assert!(
                error.contains(
                    "the object was stored but does not have the crc32 checksum s3m computed"
                ),
                "{error}"
            );
            assert!(error.contains(CRC32_COMPOSITE), "{error}");
            assert!(error.contains(response_value.unwrap().trim()), "{error}");
            assert_eq!(db.cached_etag().unwrap(), None);
            assert_eq!(db.upload_id().unwrap(), None);
            assert_eq!(db.upload_checksum().unwrap(), None);
            assert!(db.db_parts().unwrap().is_empty());
        } else {
            assert_eq!(
                result.unwrap(),
                format!("ETag: \"done-2\"\ncrc32 (base64): {CRC32_COMPOSITE}")
            );
            assert_eq!(db.cached_etag().unwrap().as_deref(), Some("\"done-2\""));
        }
        assert!(db.db_uploaded().unwrap().is_empty());
    }

    #[tokio::test]
    async fn rejects_a_different_composite_completion_checksum_and_forgets_upload() {
        complete_with_crc32(Some("AAAAAA==-2"), true).await;
        // #88 review: white space around the value didn't hide a mismatch: the line
        // break made it look like a full-object value, which is not compared.
        complete_with_crc32(Some("\n  AAAAAA==-2\n"), true).await;
    }

    #[tokio::test]
    async fn accepts_matching_missing_and_full_object_completion_checksums() {
        let padded = format!("  {CRC32_COMPOSITE}\n");
        for value in [
            Some(CRC32_COMPOSITE),
            None,
            Some("poTHxg=="),
            // #88 review: white space around an equal value is not a mismatch.
            Some(padded.as_str()),
            Some(""),
        ] {
            complete_with_crc32(value, false).await;
        }
    }

    // #77: the upload id is saved but s3m stopped before recording the parts. The
    // resume creates them with the algorithm the upload was created with; without it
    // the parts would carry Content-MD5 and S3 would reject the completion.
    #[tokio::test]
    async fn resume_creates_parts_with_the_saved_checksum_algorithm() {
        let mut server = Server::new_async().await;
        let s3 = checksum_s3(&server);
        let dir = tempfile::tempdir().unwrap();
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"0123456789").unwrap();

        let db = Db::new(&s3, "key", "checksum", 1, dir.path()).unwrap();
        db.save_upload_checksum(Some(&ChecksumAlgorithm::Sha256))
            .unwrap();
        db.save_upload_id("uid").unwrap();

        let mut parts = Vec::new();
        for (number, checksum) in [("1", SHA256_PART_1), ("2", SHA256_PART_2)] {
            parts.push(
                server
                    .mock("PUT", "/bucket/key")
                    .match_query(Matcher::UrlEncoded("partNumber".into(), number.into()))
                    .match_header("x-amz-checksum-sha256", checksum)
                    .match_header("content-md5", Matcher::Missing)
                    .with_header("etag", format!("\"p{number}\"").as_str())
                    .expect(1)
                    .create_async()
                    .await,
            );
        }
        let complete = server
            .mock("POST", "/bucket/key")
            .match_query(Matcher::UrlEncoded("uploadId".into(), "uid".into()))
            .match_body(Matcher::Regex("<ChecksumSHA256>".into()))
            .with_body(COMPLETED)
            .expect(1)
            .create_async()
            .await;

        let report = resume_without_checksum(&s3, &db, file.path()).await;

        for part in parts {
            part.assert_async().await;
        }
        complete.assert_async().await;
        assert_eq!(
            report,
            format!("ETag: \"done-2\"\nsha256 (base64): {SHA256_COMPOSITE}")
        );
    }

    // <https://github.com/s3m/s3m/issues/80>: Cloudflare R2 answers 501 NotImplemented
    // to a part that carries a SHA checksum. No retry can succeed, and neither can a
    // resume: the upload is aborted and forgotten, and the error says what to do.
    #[tokio::test]
    async fn part_checksum_not_implemented_aborts_the_upload_without_retrying() {
        let mut server = Server::new_async().await;
        let s3 = checksum_s3(&server);
        let dir = tempfile::tempdir().unwrap();
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"0123456789").unwrap();
        let db = Db::new(&s3, "key", "checksum", 1, dir.path()).unwrap();

        let create = server
            .mock("POST", "/bucket/key")
            .match_query(Matcher::Regex("uploads".into()))
            .match_header("x-amz-checksum-algorithm", "SHA256")
            .with_body("<InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>key</Key><UploadId>uid</UploadId></InitiateMultipartUploadResult>")
            .expect(1)
            .create_async()
            .await;
        let part = server
            .mock("PUT", "/bucket/key")
            .match_query(Matcher::Any)
            .with_status(501)
            .with_body(r#"<?xml version="1.0" encoding="UTF-8"?><Error><Code>NotImplemented</Code><Message>Header 'x-amz-checksum-sha256' with value 'xWX+A8qbYkLgHf3e/pu6PZiycOGc0C/YXOr3XislvxI=' not implemented</Message></Error>"#)
            .expect(1)
            .create_async()
            .await;
        let abort = server
            .mock("DELETE", "/bucket/key")
            .match_query(Matcher::UrlEncoded("uploadId".into(), "uid".into()))
            .with_status(204)
            .expect(1)
            .create_async()
            .await;

        let error = upload_multipart(MultipartUploadRequest {
            s3: &s3,
            key: "key",
            file: file.path(),
            file_size: 10,
            file_mtime: None,
            chunk_size: 5,
            sdb: &db,
            acl: None,
            meta: None,
            quiet: true,
            additional_checksum: Some(Checksum::new(ChecksumAlgorithm::Sha256)),
            max_requests: 1,
            globals: RequestOptions {
                retries: 3,
                ..RequestOptions::default()
            },
        })
        .await
        .unwrap_err()
        .to_string();

        create.assert_async().await;
        part.assert_async().await;
        abort.assert_async().await;
        assert!(
            error.contains("doesn't support sha256 checksums on the parts of a multipart upload"),
            "{error}"
        );
        assert!(error.contains("and the upload was aborted"), "{error}");
        assert!(
            error.contains("Use another --checksum algorithm"),
            "{error}"
        );
        assert_eq!(db.upload_id().unwrap(), None);
        assert_eq!(db.upload_checksum().unwrap(), None);
    }

    // #77 review: a state from an older s3m has no record, and its parts were sent
    // without a checksum. Running the command again with --checksum must not change
    // what the pending part carries, nor print a checksum the server doesn't have.
    #[tokio::test]
    async fn resume_of_an_old_state_keeps_parts_without_a_checksum() {
        let mut server = Server::new_async().await;
        let s3 = checksum_s3(&server);
        let dir = tempfile::tempdir().unwrap();
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"0123456789").unwrap();

        let db = Db::new(&s3, "key", "checksum", 1, dir.path()).unwrap();
        db.save_upload_id("uid").unwrap();
        let uploaded = Part::new(1, 0, 5, None).set_etag("\"p1\"".into());
        db.db_uploaded()
            .unwrap()
            .insert(
                1_u16.to_be_bytes(),
                to_bytes::<RkyvError>(&uploaded).unwrap().as_slice(),
            )
            .unwrap();
        db.create_part(2, 5, 5, None).unwrap();

        let part = server
            .mock("PUT", "/bucket/key")
            .match_query(Matcher::UrlEncoded("partNumber".into(), "2".into()))
            .match_header("x-amz-checksum-sha256", Matcher::Missing)
            .match_header("content-md5", Matcher::Any)
            .with_header("etag", "\"p2\"")
            .expect(1)
            .create_async()
            .await;
        let complete = server
            .mock("POST", "/bucket/key")
            .match_query(Matcher::UrlEncoded("uploadId".into(), "uid".into()))
            .with_body(COMPLETED)
            .expect(1)
            .create_async()
            .await;

        let report = upload_multipart(MultipartUploadRequest {
            s3: &s3,
            key: "key",
            file: file.path(),
            file_size: 10,
            file_mtime: None,
            chunk_size: 5,
            sdb: &db,
            acl: None,
            meta: None,
            quiet: true,
            additional_checksum: Some(Checksum::new(ChecksumAlgorithm::Sha256)),
            max_requests: 1,
            globals: RequestOptions::default(),
        })
        .await
        .unwrap();

        part.assert_async().await;
        complete.assert_async().await;
        assert_eq!(report, "ETag: \"done-2\"");
    }

    const SHA256_NOT_IMPLEMENTED: &str = r#"<?xml version="1.0" encoding="UTF-8"?><Error><Code>NotImplemented</Code><Message>Header 'x-amz-checksum-sha256' not implemented</Message></Error>"#;
    const INITIATED: &str = "<InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>key</Key><UploadId>uid</UploadId></InitiateMultipartUploadResult>";

    fn sha256_upload<'a>(
        s3: &'a S3,
        db: &'a Db,
        file: &'a std::path::Path,
    ) -> MultipartUploadRequest<'a> {
        MultipartUploadRequest {
            s3,
            key: "key",
            file,
            file_size: 10,
            file_mtime: None,
            chunk_size: 5,
            sdb: db,
            acl: None,
            meta: None,
            quiet: true,
            additional_checksum: Some(Checksum::new(ChecksumAlgorithm::Sha256)),
            max_requests: 1,
            globals: RequestOptions {
                retries: 1,
                ..RequestOptions::default()
            },
        }
    }

    // #80 review: when the abort itself fails, the upload is still open on the
    // server. The error must not claim it was aborted, and the state keeps the upload
    // id so it can still be found and aborted.
    #[tokio::test]
    async fn part_checksum_not_implemented_reports_a_failed_abort() {
        let mut server = Server::new_async().await;
        let s3 = checksum_s3(&server);
        let dir = tempfile::tempdir().unwrap();
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"0123456789").unwrap();
        let db = Db::new(&s3, "key", "checksum", 1, dir.path()).unwrap();
        let _create = server
            .mock("POST", "/bucket/key")
            .match_query(Matcher::Regex("uploads".into()))
            .with_body(INITIATED)
            .create_async()
            .await;
        let _part = server
            .mock("PUT", "/bucket/key")
            .match_query(Matcher::Any)
            .with_status(501)
            .with_body(SHA256_NOT_IMPLEMENTED)
            .create_async()
            .await;
        let abort = server
            .mock("DELETE", "/bucket/key")
            .match_query(Matcher::Any)
            .with_status(403)
            .with_body("<Error><Code>AccessDenied</Code><Message>no</Message></Error>")
            .expect(1)
            .create_async()
            .await;

        let error = upload_multipart(sha256_upload(&s3, &db, file.path()))
            .await
            .unwrap_err()
            .to_string();

        abort.assert_async().await;
        assert!(!error.contains("the upload was aborted"), "{error}");
        assert!(error.contains("could not be aborted"), "{error}");
        assert!(error.contains("--abort uid"), "{error}");
        assert_eq!(db.upload_id().unwrap().as_deref(), Some("uid"));
    }

    // #80 review: with several parts in flight, each rejected part must not send its
    // own abort. The parts stop, then the upload is aborted once.
    #[tokio::test]
    async fn part_checksum_not_implemented_aborts_once_with_parallel_parts() {
        let mut server = Server::new_async().await;
        let s3 = checksum_s3(&server);
        let dir = tempfile::tempdir().unwrap();
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"0123456789abcdefghij").unwrap();
        let db = Db::new(&s3, "key", "checksum", 1, dir.path()).unwrap();
        let _create = server
            .mock("POST", "/bucket/key")
            .match_query(Matcher::Regex("uploads".into()))
            .with_body(INITIATED)
            .create_async()
            .await;
        let _parts = server
            .mock("PUT", "/bucket/key")
            .match_query(Matcher::Any)
            .with_status(501)
            .with_body(SHA256_NOT_IMPLEMENTED)
            .create_async()
            .await;
        let abort = server
            .mock("DELETE", "/bucket/key")
            .match_query(Matcher::UrlEncoded("uploadId".into(), "uid".into()))
            .with_status(204)
            .expect(1)
            .create_async()
            .await;

        let mut request = sha256_upload(&s3, &db, file.path());
        request.file_size = 20;
        request.max_requests = 4;
        let error = upload_multipart(request).await.unwrap_err().to_string();

        // Let a request that was already on its way arrive before counting.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        abort.assert_async().await;
        assert!(error.contains("and the upload was aborted"), "{error}");
        assert_eq!(db.upload_id().unwrap(), None);
        assert!(db.db_parts().unwrap().is_empty());
    }

    // #80 review: a 501 after a part was accepted with its checksum is not "the
    // server doesn't support this checksum". The parts already uploaded are kept,
    // and nothing is aborted.
    #[tokio::test]
    async fn not_implemented_after_an_uploaded_part_keeps_the_upload() {
        let mut server = Server::new_async().await;
        let s3 = checksum_s3(&server);
        let dir = tempfile::tempdir().unwrap();
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"0123456789").unwrap();
        let db = Db::new(&s3, "key", "checksum", 1, dir.path()).unwrap();
        let _create = server
            .mock("POST", "/bucket/key")
            .match_query(Matcher::Regex("uploads".into()))
            .with_body(INITIATED)
            .create_async()
            .await;
        let part_1 = server
            .mock("PUT", "/bucket/key")
            .match_query(Matcher::UrlEncoded("partNumber".into(), "1".into()))
            .match_header("x-amz-checksum-sha256", SHA256_PART_1)
            .with_header("etag", "\"p1\"")
            .expect(1)
            .create_async()
            .await;
        let _part_2 = server
            .mock("PUT", "/bucket/key")
            .match_query(Matcher::UrlEncoded("partNumber".into(), "2".into()))
            .with_status(501)
            .with_body(SHA256_NOT_IMPLEMENTED)
            .create_async()
            .await;
        let abort = server
            .mock("DELETE", "/bucket/key")
            .match_query(Matcher::Any)
            .expect(0)
            .create_async()
            .await;

        let error = upload_multipart(sha256_upload(&s3, &db, file.path()))
            .await
            .unwrap_err()
            .to_string();

        part_1.assert_async().await;
        abort.assert_async().await;
        assert!(!error.contains("aborted"), "{error}");
        assert_eq!(db.upload_id().unwrap().as_deref(), Some("uid"));
        assert_eq!(db.uploaded_parts().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn checksum_501_after_an_accepted_part_honors_the_retry_budget() {
        let mut server = Server::new_async().await;
        let s3 = checksum_s3(&server);
        let dir = tempfile::tempdir().unwrap();
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"0123456789").unwrap();
        let db = Db::new(&s3, "key", "checksum", 1, dir.path()).unwrap();
        db.save_upload_id("uid").unwrap();
        let first = part_mock(&mut server, 1, "01234")
            .expect(1)
            .create_async()
            .await;
        let retry = server
            .mock("PUT", "/bucket/key")
            .match_query(Matcher::UrlEncoded("partNumber".into(), "2".into()))
            .with_status(501)
            .with_body(SHA256_NOT_IMPLEMENTED)
            .expect(2)
            .create_async()
            .await;
        let abort = server
            .mock("DELETE", "/bucket/key")
            .match_query(Matcher::Any)
            .expect(0)
            .create_async()
            .await;

        let mut request = sha256_upload(&s3, &db, file.path());
        request.globals.retries = 2;
        let error = upload_multipart(request).await.unwrap_err();

        first.assert_async().await;
        assert!(
            retry.matched_async().await,
            "checksum 501 after an accepted part must use both attempts: {error}"
        );
        abort.assert_async().await;
        assert_eq!(db.upload_id().unwrap().as_deref(), Some("uid"));
        assert_eq!(db.uploaded_parts().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn unsupported_part_checksum_requires_both_501_and_not_implemented() {
        for (status, code) in [(501, "InternalError"), (503, "NotImplemented")] {
            let mut server = Server::new_async().await;
            let s3 = checksum_s3(&server);
            let dir = tempfile::tempdir().unwrap();
            let mut file = tempfile::NamedTempFile::new().unwrap();
            file.write_all(b"0123456789").unwrap();
            let db = Db::new(&s3, "key", "checksum", 1, dir.path()).unwrap();
            db.save_upload_id("uid").unwrap();
            let part = server
                .mock("PUT", "/bucket/key")
                .match_query(Matcher::Any)
                .with_status(status)
                .with_body(format!(
                    "<Error><Code>{code}</Code><Message>x-amz-checksum-sha256 failed</Message></Error>"
                ))
                .expect(1)
                .create_async()
                .await;
            let abort = server
                .mock("DELETE", "/bucket/key")
                .match_query(Matcher::Any)
                .with_status(204)
                .expect(0)
                .create_async()
                .await;

            let error = upload_multipart(sha256_upload(&s3, &db, file.path()))
                .await
                .unwrap_err()
                .to_string();

            part.assert_async().await;
            assert!(
                abort.matched_async().await,
                "HTTP {status} / {code} must not abort the upload for an unsupported checksum"
            );
            assert!(error.contains(code), "{error}");
            assert_eq!(db.upload_id().unwrap().as_deref(), Some("uid"));
        }
    }

    // A new upload records its algorithm (or that it has none), and a resume keeps
    // that choice even when it is run with another --checksum: the server's upload
    // was created without one, so its parts can't carry one.
    #[tokio::test]
    async fn upload_records_its_checksum_algorithm_and_resume_keeps_it() {
        let mut server = Server::new_async().await;
        let s3 = checksum_s3(&server);
        let dir = tempfile::tempdir().unwrap();
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"0123456789").unwrap();
        let db = Db::new(&s3, "key", "checksum", 1, dir.path()).unwrap();
        assert_eq!(db.upload_checksum().unwrap(), None);

        // First run: created without a checksum, part 2 fails.
        let create = server
            .mock("POST", "/bucket/key")
            .match_query(Matcher::Regex("uploads".into()))
            .match_header("x-amz-checksum-algorithm", Matcher::Missing)
            .with_body("<InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>key</Key><UploadId>uid</UploadId></InitiateMultipartUploadResult>")
            .expect(1)
            .create_async()
            .await;
        let part_1 = server
            .mock("PUT", "/bucket/key")
            .match_query(Matcher::UrlEncoded("partNumber".into(), "1".into()))
            .match_header("x-amz-checksum-sha256", Matcher::Missing)
            .with_header("etag", "\"p1\"")
            .expect(1)
            .create_async()
            .await;
        let part_2_fails = server
            .mock("PUT", "/bucket/key")
            .match_query(Matcher::UrlEncoded("partNumber".into(), "2".into()))
            .with_status(403)
            .with_body("<Error><Code>AccessDenied</Code><Message>no</Message></Error>")
            .create_async()
            .await;
        let request = |checksum: Option<Checksum>| MultipartUploadRequest {
            s3: &s3,
            key: "key",
            file: file.path(),
            file_size: 10,
            file_mtime: None,
            chunk_size: 5,
            sdb: &db,
            acl: None,
            meta: None,
            quiet: true,
            additional_checksum: checksum,
            max_requests: 1,
            globals: RequestOptions {
                retries: 1,
                ..RequestOptions::default()
            },
        };
        assert!(upload_multipart(request(None)).await.is_err());
        create.assert_async().await;
        part_1.assert_async().await;
        assert_eq!(db.upload_checksum().unwrap(), Some(None));
        part_2_fails.remove_async().await;

        // Second run asks for sha256: the pending part still goes without it.
        let part_2 = server
            .mock("PUT", "/bucket/key")
            .match_query(Matcher::UrlEncoded("partNumber".into(), "2".into()))
            .match_header("x-amz-checksum-sha256", Matcher::Missing)
            .match_header("content-md5", Matcher::Any)
            .with_header("etag", "\"p2\"")
            .expect(1)
            .create_async()
            .await;
        let complete = server
            .mock("POST", "/bucket/key")
            .match_query(Matcher::UrlEncoded("uploadId".into(), "uid".into()))
            .with_body(COMPLETED)
            .expect(1)
            .create_async()
            .await;

        let report = upload_multipart(request(Some(Checksum::new(ChecksumAlgorithm::Sha256))))
            .await
            .unwrap();

        part_2.assert_async().await;
        complete.assert_async().await;
        assert_eq!(report, "ETag: \"done-2\"");
    }

    const COMPLETED_3: &str = "<CompleteMultipartUploadResult><Location>l</Location><Bucket>bucket</Bucket><Key>key</Key><ETag>\"done-3\"</ETag></CompleteMultipartUploadResult>";

    fn ten_bytes_in_parts_of_four<'a>(
        s3: &'a S3,
        db: &'a Db,
        file: &'a std::path::Path,
    ) -> MultipartUploadRequest<'a> {
        MultipartUploadRequest {
            s3,
            key: "key",
            file,
            file_size: 10,
            file_mtime: None,
            chunk_size: 4,
            sdb: db,
            acl: None,
            meta: None,
            quiet: true,
            additional_checksum: None,
            max_requests: 1,
            globals: RequestOptions::default(),
        }
    }

    /// Mocks that fail the test if any request for the object arrives.
    async fn no_requests(server: &mut Server) -> Vec<mockito::Mock> {
        let mut mocks = Vec::new();
        for method in ["PUT", "POST", "DELETE", "GET", "HEAD"] {
            mocks.push(
                server
                    .mock(method, "/bucket/key")
                    .match_query(Matcher::Any)
                    .expect(0)
                    .create_async()
                    .await,
            );
        }
        mocks
    }

    fn part_mock(server: &mut Server, number: u16, body: &str) -> mockito::Mock {
        server
            .mock("PUT", "/bucket/key")
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("uploadId".into(), "uid".into()),
                Matcher::UrlEncoded("partNumber".into(), number.to_string()),
            ]))
            .match_body(body)
            .with_header("ETag", &format!("\"p{number}\""))
    }

    fn complete_three_parts(server: &mut Server) -> mockito::Mock {
        server
            .mock("POST", "/bucket/key")
            .match_query(Matcher::UrlEncoded("uploadId".into(), "uid".into()))
            .match_body(Matcher::Regex(
                "^<CompleteMultipartUpload><Part><ETag>\"p1\"</ETag><PartNumber>1</PartNumber></Part><Part><ETag>\"p2\"</ETag><PartNumber>2</PartNumber></Part><Part><ETag>\"p3\"</ETag><PartNumber>3</PartNumber></Part></CompleteMultipartUpload>$".into(),
            ))
            .with_body(COMPLETED_3)
    }

    #[tokio::test]
    async fn verifies_the_source_again_before_completing() {
        for (replacement, expected_change) in [
            (Some("1123456789"), Some("modification time changed")),
            (
                Some("0123456789+"),
                Some("size changed from 10 to 11 bytes"),
            ),
            (None, None),
        ] {
            let mut server = Server::new_async().await;
            let s3 = checksum_s3(&server);
            let dir = tempfile::tempdir().unwrap();
            let file = tempfile::NamedTempFile::new().unwrap();
            fs::write(file.path(), "0123456789").unwrap();
            file.as_file().set_modified(UNIX_EPOCH).unwrap();
            let db = Db::new(&s3, "key", "checksum", 1, dir.path()).unwrap();
            db.save_upload_id("uid").unwrap();

            let path = file.path().to_owned();
            let first = part_mock(&mut server, 1, "0123")
                .with_body_from_request(move |_| {
                    if let Some(replacement) = replacement {
                        fs::write(&path, replacement).unwrap();
                    }
                    Vec::new()
                })
                .create_async()
                .await;
            let second = part_mock(&mut server, 2, "4567").create_async().await;
            let third = part_mock(&mut server, 3, "89").create_async().await;
            let complete = complete_three_parts(&mut server)
                .expect(usize::from(expected_change.is_none()))
                .create_async()
                .await;
            let abort = server
                .mock("DELETE", "/bucket/key")
                .match_query(Matcher::UrlEncoded("uploadId".into(), "uid".into()))
                .with_status(204)
                .expect(usize::from(expected_change.is_some()))
                .create_async()
                .await;

            let result = upload_multipart(ten_bytes_in_parts_of_four(&s3, &db, file.path())).await;

            for mock in [first, second, third, complete, abort] {
                mock.assert_async().await;
            }
            if let Some(expected) = expected_change {
                let error = result.unwrap_err().to_string();
                assert!(
                    error.contains("the file changed during the upload"),
                    "{error}"
                );
                assert!(error.contains(expected), "{error}");
                assert!(error.contains("run the upload again"), "{error}");
                assert_eq!(db.upload_id().unwrap(), None);
                assert_eq!(db.cached_etag().unwrap(), None);
                assert!(db.db_parts().unwrap().is_empty());
                assert!(db.db_uploaded().unwrap().is_empty());
            } else {
                assert_eq!(result.unwrap(), "ETag: \"done-3\"");
            }
        }
    }

    #[tokio::test]
    async fn aborts_saved_upload_when_source_size_is_already_wrong() {
        let mut server = Server::new_async().await;
        let s3 = checksum_s3(&server);
        let dir = tempfile::tempdir().unwrap();
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"0123456789+").unwrap();
        let db = Db::new(&s3, "key", "checksum", 1, dir.path()).unwrap();
        db.save_upload_id("uid").unwrap();

        let parts = server
            .mock("PUT", "/bucket/key")
            .match_query(Matcher::Any)
            .with_header("ETag", "part")
            .expect(0)
            .create_async()
            .await;
        let complete = server
            .mock("POST", "/bucket/key")
            .match_query(Matcher::Any)
            .with_body(COMPLETED_3)
            .expect(0)
            .create_async()
            .await;
        let abort = server
            .mock("DELETE", "/bucket/key")
            .match_query(Matcher::UrlEncoded("uploadId".into(), "uid".into()))
            .with_status(204)
            .create_async()
            .await;

        let result = upload_multipart(ten_bytes_in_parts_of_four(&s3, &db, file.path())).await;

        for mock in [parts, complete, abort] {
            mock.assert_async().await;
        }
        let error = result.unwrap_err().to_string();
        assert!(
            error.contains("size changed from 10 to 11 bytes"),
            "{error}"
        );
        assert_eq!(db.upload_id().unwrap(), None);
    }

    // <https://github.com/s3m/s3m/issues/83>: s3m was killed while it wrote the part
    // plan, and the state kept the upload id with only the first of three parts. The
    // resume uploaded that part and completed the upload: a 4-byte object for a
    // 10-byte file, with exit 0. The missing parts are now planned and uploaded.
    #[tokio::test]
    async fn resume_adds_the_parts_an_interrupted_plan_lacks() {
        let mut server = Server::new_async().await;
        let s3 = checksum_s3(&server);
        let dir = tempfile::tempdir().unwrap();
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"0123456789").unwrap();
        let db = Db::new(&s3, "key", "checksum", 1, dir.path()).unwrap();
        db.save_upload_id("uid").unwrap();
        db.create_part(1, 0, 4, None).unwrap();

        let parts = [
            part_mock(&mut server, 1, "0123")
                .expect(1)
                .create_async()
                .await,
            part_mock(&mut server, 2, "4567")
                .expect(1)
                .create_async()
                .await,
            part_mock(&mut server, 3, "89")
                .expect(1)
                .create_async()
                .await,
        ];
        let complete = complete_three_parts(&mut server)
            .expect(1)
            .create_async()
            .await;

        let result = upload_multipart(ten_bytes_in_parts_of_four(&s3, &db, file.path())).await;

        for part in parts {
            part.assert_async().await;
        }
        complete.assert_async().await;
        assert_eq!(result.unwrap(), "ETag: \"done-3\"");
    }

    // #83: the same when the part that survived was already uploaded. The saved parts
    // keep their size: this run asks for parts of 5 bytes, the upload goes on with 4.
    #[tokio::test]
    async fn resume_adds_the_missing_parts_after_an_uploaded_one() {
        let mut server = Server::new_async().await;
        let s3 = checksum_s3(&server);
        let dir = tempfile::tempdir().unwrap();
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"0123456789").unwrap();
        let db = Db::new(&s3, "key", "checksum", 1, dir.path()).unwrap();
        db.save_upload_id("uid").unwrap();
        let uploaded = Part::new(1, 0, 4, None).set_etag("\"p1\"".to_string());
        db.db_uploaded()
            .unwrap()
            .insert(
                1_u16.to_be_bytes(),
                to_bytes::<RkyvError>(&uploaded).unwrap().as_slice(),
            )
            .unwrap();

        let first = part_mock(&mut server, 1, "0123")
            .expect(0)
            .create_async()
            .await;
        let parts = [
            part_mock(&mut server, 2, "4567")
                .expect(1)
                .create_async()
                .await,
            part_mock(&mut server, 3, "89")
                .expect(1)
                .create_async()
                .await,
        ];
        let complete = complete_three_parts(&mut server)
            .expect(1)
            .create_async()
            .await;

        let mut request = ten_bytes_in_parts_of_four(&s3, &db, file.path());
        request.chunk_size = 5;
        let result = upload_multipart(request).await;

        first.assert_async().await;
        for part in parts {
            part.assert_async().await;
        }
        complete.assert_async().await;
        assert_eq!(result.unwrap(), "ETag: \"done-3\"");
    }

    // #83: saved parts that are not parts of this file can't be completed into it. The
    // upload is aborted and forgotten without sending a part, and the next run starts
    // a new one.
    #[tokio::test]
    async fn saved_parts_that_do_not_match_the_file_abort_the_upload() {
        let mut server = Server::new_async().await;
        let s3 = checksum_s3(&server);
        let dir = tempfile::tempdir().unwrap();
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"0123456789").unwrap();
        let db = Db::new(&s3, "key", "checksum", 1, dir.path()).unwrap();
        db.save_upload_id("uid").unwrap();
        db.create_part(1, 0, 4, None).unwrap();
        // Part 2 of a 10-byte file in parts of 4 is 4 bytes at 4.
        db.create_part(2, 4, 6, None).unwrap();

        let sent = server
            .mock("PUT", "/bucket/key")
            .match_query(Matcher::Any)
            .expect(0)
            .create_async()
            .await;
        let completed = server
            .mock("POST", "/bucket/key")
            .match_query(Matcher::Any)
            .expect(0)
            .create_async()
            .await;
        let abort = server
            .mock("DELETE", "/bucket/key")
            .match_query(Matcher::UrlEncoded("uploadId".into(), "uid".into()))
            .with_status(204)
            .expect(1)
            .create_async()
            .await;

        let error = upload_multipart(ten_bytes_in_parts_of_four(&s3, &db, file.path()))
            .await
            .unwrap_err()
            .to_string();

        sent.assert_async().await;
        completed.assert_async().await;
        abort.assert_async().await;
        assert!(
            error.contains("part 2 is saved as 6 bytes at 4, the file has 4 bytes at 4 for it"),
            "{error}"
        );
        assert!(
            error.contains("The multipart upload was aborted"),
            "{error}"
        );
        assert!(
            error.contains("run the upload again to start a new one"),
            "{error}"
        );
        assert_eq!(db.upload_id().unwrap(), None);
        assert!(db.db_parts().unwrap().is_empty());
        assert!(db.db_uploaded().unwrap().is_empty());
    }

    // #83 review: only saved parts that can't belong to the file end the upload. When
    // the state can't be written (here its metadata file is unreadable), the upload on
    // the server is left alone: nothing is aborted, and the upload id is kept.
    #[tokio::test]
    async fn a_state_that_cannot_be_written_does_not_abort_the_upload() {
        let mut server = Server::new_async().await;
        let s3 = checksum_s3(&server);
        let dir = tempfile::tempdir().unwrap();
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"0123456789").unwrap();
        let db = Db::new(&s3, "key", "checksum", 1, dir.path()).unwrap();
        db.save_upload_id("uid").unwrap();
        let uploaded = Part::new(1, 0, 4, None).set_etag("\"p1\"".to_string());
        db.db_uploaded()
            .unwrap()
            .insert(
                1_u16.to_be_bytes(),
                to_bytes::<RkyvError>(&uploaded).unwrap().as_slice(),
            )
            .unwrap();
        // Adding the missing parts refreshes the metadata file, which fails on this one.
        std::fs::write(
            db.path().join(crate::stream::state::STATE_FILE),
            "not: [the metadata",
        )
        .unwrap();

        let no_requests = no_requests(&mut server).await;

        let error = upload_multipart(ten_bytes_in_parts_of_four(&s3, &db, file.path()))
            .await
            .unwrap_err()
            .to_string();

        for mock in no_requests {
            mock.assert_async().await;
        }
        assert!(!error.contains("doesn't match the file"), "{error}");
        assert_eq!(db.upload_id().unwrap().as_deref(), Some("uid"));
        assert_eq!(db.db_uploaded().unwrap().len(), 1);
    }

    // #83 review: a saved part size that can't be this file's is a mismatch, not a plan
    // of more parts than S3 allows (one byte per part here).
    #[tokio::test]
    async fn a_saved_part_size_that_needs_too_many_parts_aborts_the_upload() {
        let mut server = Server::new_async().await;
        let s3 = checksum_s3(&server);
        let dir = tempfile::tempdir().unwrap();
        let file = tempfile::NamedTempFile::new().unwrap();
        file.as_file().set_len(70_000).unwrap();
        let db = Db::new(&s3, "key", "checksum", 1, dir.path()).unwrap();
        db.save_upload_id("uid").unwrap();
        db.create_part(1, 0, 1, None).unwrap();

        let abort = server
            .mock("DELETE", "/bucket/key")
            .match_query(Matcher::UrlEncoded("uploadId".into(), "uid".into()))
            .with_status(204)
            .expect(1)
            .create_async()
            .await;

        let mut request = ten_bytes_in_parts_of_four(&s3, &db, file.path());
        request.file_size = 70_000;
        let error = upload_multipart(request).await.unwrap_err().to_string();

        abort.assert_async().await;
        assert!(
            error.contains(
                "part 1 is saved as 1 bytes, which can't be the part size of a file of 70000 bytes"
            ),
            "{error}"
        );
        assert_eq!(db.db_parts().unwrap().len(), 0);
    }

    // #83 review: the last line of defence, on its own: uploaded parts that don't hold
    // the whole file send no CompleteMultipartUpload.
    #[tokio::test]
    async fn uploaded_parts_that_do_not_cover_the_file_are_never_completed() {
        let mut server = Server::new_async().await;
        let s3 = checksum_s3(&server);
        let dir = tempfile::tempdir().unwrap();
        let db = Db::new(&s3, "key", "checksum", 1, dir.path()).unwrap();
        db.save_upload_id("uid").unwrap();
        let uploaded = Part::new(1, 0, 4, None).set_etag("\"p1\"".to_string());
        db.db_uploaded()
            .unwrap()
            .insert(
                1_u16.to_be_bytes(),
                to_bytes::<RkyvError>(&uploaded).unwrap().as_slice(),
            )
            .unwrap();
        let no_requests = no_requests(&mut server).await;

        let error = complete_upload(&s3, "key", "uid", &db, 10, None)
            .await
            .unwrap_err()
            .to_string();

        for mock in no_requests {
            mock.assert_async().await;
        }
        assert_eq!(
            error,
            "the uploaded parts don't match the file (they hold 4 bytes, the file has 10); \
             the multipart upload was not completed"
        );
        assert_eq!(db.upload_id().unwrap().as_deref(), Some("uid"));
        assert_eq!(db.check().unwrap(), None);
    }

    // #83: when that abort fails the upload is still open, so the state is kept and
    // the error gives the command. A run after the manual abort finds the upload gone
    // (NoSuchUpload), forgets it and says so.
    #[tokio::test]
    async fn unusable_state_reports_a_failed_abort_and_recovers_after_a_manual_one() {
        let mut server = Server::new_async().await;
        let s3 = checksum_s3(&server);
        let dir = tempfile::tempdir().unwrap();
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"0123456789").unwrap();
        let db = Db::new(&s3, "key", "checksum", 1, dir.path()).unwrap();
        db.save_upload_id("uid").unwrap();
        db.create_part(2, 4, 4, None).unwrap();

        let denied = server
            .mock("DELETE", "/bucket/key")
            .match_query(Matcher::Any)
            .with_status(403)
            .with_body("<Error><Code>AccessDenied</Code><Message>no</Message></Error>")
            .expect(1)
            .create_async()
            .await;
        let error = upload_multipart(ten_bytes_in_parts_of_four(&s3, &db, file.path()))
            .await
            .unwrap_err()
            .to_string();
        denied.assert_async().await;
        assert!(
            error.contains("part 1 is missing from the saved parts"),
            "{error}"
        );
        assert!(error.contains("could not be aborted"), "{error}");
        assert!(error.contains("--abort uid"), "{error}");
        assert_eq!(db.upload_id().unwrap().as_deref(), Some("uid"));
        denied.remove_async().await;

        let gone = server
            .mock("DELETE", "/bucket/key")
            .match_query(Matcher::Any)
            .with_status(404)
            .with_body("<Error><Code>NoSuchUpload</Code><Message>gone</Message></Error>")
            .expect(1)
            .create_async()
            .await;
        let error = upload_multipart(ten_bytes_in_parts_of_four(&s3, &db, file.path()))
            .await
            .unwrap_err()
            .to_string();
        gone.assert_async().await;
        assert!(
            error.contains("The multipart upload was aborted"),
            "{error}"
        );
        assert_eq!(db.upload_id().unwrap(), None);
        assert!(db.db_parts().unwrap().is_empty());
    }

    // #83: the check before CompleteMultipartUpload.
    #[test]
    fn uploaded_parts_must_cover_the_file() {
        let s3 = S3::new(
            &Credentials::new("AKIA", &SecretString::new("secret".into())),
            &Region::custom("us-east-1", "http://127.0.0.1:9"),
            Some("bucket".to_string()),
            false,
        );
        let dir = tempfile::tempdir().unwrap();
        let db = Db::new(&s3, "key", "checksum", 1, dir.path()).unwrap();
        let upload = |number: u16, seek: u64, chunk: u64| {
            let part = Part::new(number, seek, chunk, None).set_etag("\"e\"".to_string());
            db.db_uploaded()
                .unwrap()
                .insert(
                    number.to_be_bytes(),
                    to_bytes::<RkyvError>(&part).unwrap().as_slice(),
                )
                .unwrap();
        };
        let cover = |file_size: u64| parts_cover(&db.uploaded_part_ranges().unwrap(), file_size);
        let mismatch = |file_size: u64| cover(file_size).unwrap_err().to_string();

        assert_eq!(mismatch(10), "they hold 0 bytes, the file has 10");
        upload(1, 0, 4);
        assert_eq!(mismatch(10), "they hold 4 bytes, the file has 10");
        upload(3, 8, 2);
        assert_eq!(mismatch(10), "part 2 is missing");
        upload(2, 5, 3);
        assert_eq!(mismatch(10), "part 2 starts at byte 5, not at 4");
        upload(2, 4, 4);
        cover(10).unwrap();
        assert_eq!(mismatch(11), "they hold 10 bytes, the file has 11");
    }

    const NO_SUCH_UPLOAD: &str = "<Error><Code>NoSuchUpload</Code><Message>The specified multipart upload does not exist.</Message></Error>";

    // <https://github.com/s3m/s3m/issues/84>: the saved multipart upload was aborted
    // on the server (by hand, or by a lifecycle rule). Every run sent its parts to the
    // dead upload id, three times each, and failed; no command removed the state. The
    // first NoSuchUpload now ends the run and removes the state, and the next run
    // starts a new upload.
    #[tokio::test]
    async fn a_part_for_an_upload_the_server_no_longer_has_forgets_the_upload() {
        let mut server = Server::new_async().await;
        let s3 = checksum_s3(&server);
        let dir = tempfile::tempdir().unwrap();
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"0123456789").unwrap();
        let db = Db::new(&s3, "key", "checksum", 1, dir.path()).unwrap();
        db.save_upload_id("uid").unwrap();
        db.create_parts([(1, 0, 4), (2, 4, 4), (3, 8, 2)], None)
            .unwrap();

        let gone = server
            .mock("PUT", "/bucket/key")
            .match_query(Matcher::UrlEncoded("uploadId".into(), "uid".into()))
            .with_status(404)
            .with_body(NO_SUCH_UPLOAD)
            .expect(1)
            .create_async()
            .await;
        let mut request = ten_bytes_in_parts_of_four(&s3, &db, file.path());
        request.globals.retries = 3;
        let error = upload_multipart(request).await.unwrap_err().to_string();

        gone.assert_async().await;
        assert!(
            error.contains("no longer exists on the server") && error.contains("NoSuchUpload"),
            "{error}"
        );
        assert!(
            error.contains("run the upload again to start a new one"),
            "{error}"
        );
        assert_eq!(db.upload_id().unwrap(), None);
        assert!(db.db_parts().unwrap().is_empty());
        gone.remove_async().await;

        // The next run starts a new upload and sends every part to it.
        let create = server
            .mock("POST", "/bucket/key")
            .match_query(Matcher::Regex("uploads".into()))
            .with_body("<InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>key</Key><UploadId>new-id</UploadId></InitiateMultipartUploadResult>")
            .expect(1)
            .create_async()
            .await;
        let parts = server
            .mock("PUT", "/bucket/key")
            .match_query(Matcher::UrlEncoded("uploadId".into(), "new-id".into()))
            .with_header("ETag", "\"fresh\"")
            .expect(3)
            .create_async()
            .await;
        let complete = server
            .mock("POST", "/bucket/key")
            .match_query(Matcher::UrlEncoded("uploadId".into(), "new-id".into()))
            .with_body(COMPLETED_3)
            .expect(1)
            .create_async()
            .await;

        let report = upload_multipart(ten_bytes_in_parts_of_four(&s3, &db, file.path()))
            .await
            .unwrap();

        create.assert_async().await;
        parts.assert_async().await;
        complete.assert_async().await;
        assert_eq!(report, "ETag: \"done-3\"");
    }

    // #84: every part is uploaded, and the completion answers NoSuchUpload (the upload
    // was aborted, or an earlier completion went through and its answer was lost).
    async fn assert_completion_no_such_upload_forgets_state(status: usize) {
        let mut server = Server::new_async().await;
        let s3 = checksum_s3(&server);
        let dir = tempfile::tempdir().unwrap();
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"0123456789").unwrap();
        let db = Db::new(&s3, "key", "checksum", 1, dir.path()).unwrap();
        db.save_upload_id("uid").unwrap();
        for (number, seek, chunk) in [(1_u16, 0, 4), (2, 4, 4), (3, 8, 2)] {
            let part = Part::new(number, seek, chunk, None).set_etag(format!("\"p{number}\""));
            db.db_uploaded()
                .unwrap()
                .insert(
                    number.to_be_bytes(),
                    to_bytes::<RkyvError>(&part).unwrap().as_slice(),
                )
                .unwrap();
        }

        let sent = server
            .mock("PUT", "/bucket/key")
            .match_query(Matcher::Any)
            .expect(0)
            .create_async()
            .await;
        let complete = server
            .mock("POST", "/bucket/key")
            .match_query(Matcher::UrlEncoded("uploadId".into(), "uid".into()))
            .with_status(status)
            .with_body(NO_SUCH_UPLOAD)
            .expect(1)
            .create_async()
            .await;

        let error = upload_multipart(ten_bytes_in_parts_of_four(&s3, &db, file.path()))
            .await
            .unwrap_err()
            .to_string();

        sent.assert_async().await;
        complete.assert_async().await;
        assert!(
            error.contains("no longer exists on the server"),
            "HTTP {status} NoSuchUpload at completion must clear the upload: {error}"
        );
        assert_eq!(db.upload_id().unwrap(), None);
        assert!(db.db_uploaded().unwrap().is_empty());
        assert_eq!(db.check().unwrap(), None);
    }

    #[tokio::test]
    async fn a_completion_for_an_upload_the_server_no_longer_has_forgets_the_upload() {
        assert_completion_no_such_upload_forgets_state(404).await;
    }

    #[tokio::test]
    async fn an_embedded_no_such_upload_at_completion_forgets_the_upload() {
        assert_completion_no_such_upload_forgets_state(200).await;
    }

    // #84 review: only a 404 proves the upload is gone. The same error code from
    // something in between (a 503 here) is retried like any other error, and the
    // upload id is kept.
    #[tokio::test]
    async fn no_such_upload_with_another_status_keeps_the_upload() {
        let mut server = Server::new_async().await;
        let s3 = checksum_s3(&server);
        let dir = tempfile::tempdir().unwrap();
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"0123456789").unwrap();
        let db = Db::new(&s3, "key", "checksum", 1, dir.path()).unwrap();
        db.save_upload_id("uid").unwrap();
        db.create_parts([(1, 0, 4), (2, 4, 4), (3, 8, 2)], None)
            .unwrap();

        let unavailable = server
            .mock("PUT", "/bucket/key")
            .match_query(Matcher::Any)
            .with_status(503)
            .with_body(NO_SUCH_UPLOAD)
            .expect(1)
            .create_async()
            .await;

        let error = upload_multipart(ten_bytes_in_parts_of_four(&s3, &db, file.path()))
            .await
            .unwrap_err()
            .to_string();

        unavailable.assert_async().await;
        assert!(!error.contains("no longer exists"), "{error}");
        assert_eq!(db.upload_id().unwrap().as_deref(), Some("uid"));
        assert_eq!(db.db_parts().unwrap().len(), 3);
    }

    #[test]
    fn equivalent_endpoints_find_the_same_upload_and_pass_the_host_check() {
        let s3_at = |endpoint: &str| {
            S3::new(
                &Credentials::new("id", &SecretString::new("secret".into())),
                &Region::custom("region", endpoint),
                Some("bucket".into()),
                false,
            )
        };
        let dir = tempfile::tempdir().unwrap();
        let original = s3_at("storage.example/base");
        let db = Db::new(&original, "key", "hash", 1, dir.path()).unwrap();
        let path = db.path().to_owned();
        db.save_upload_id("original").unwrap();
        drop(db);
        for endpoint in [
            "https://storage.example/base",
            "https://STORAGE.EXAMPLE:443/base/",
        ] {
            let s3 = s3_at(endpoint);
            let reopened = Db::new(&s3, "key", "hash", 1, dir.path()).unwrap();
            assert_eq!(reopened.path(), path);
            assert_eq!(reopened.upload_id().unwrap().as_deref(), Some("original"));
            assert!(super::started_here(&original, &reopened));
            assert!(super::started_here(&s3, &reopened));
            assert_eq!(s3.endpoint().unwrap(), original.endpoint().unwrap());
            assert!(!super::started_here(
                &s3_at("http://storage.example/base"),
                &reopened
            ));
            assert!(!super::started_here(
                &s3_at("https://storage.example/other"),
                &reopened
            ));
        }
        for endpoint in [
            "https://token@storage.example",
            "storage.example?token",
            "storage.example#token",
        ] {
            let key = s3_at(endpoint).upload_host();
            assert!(key.contains("blake3") && !key.contains("token"));
        }
    }

    #[test]
    fn previous_v2_endpoint_spelling_keeps_its_saved_upload() {
        let dir = tempfile::tempdir().unwrap();
        let credentials = Credentials::new("id", &SecretString::new("secret".into()));
        let at = |endpoint| {
            S3::new(
                &credentials,
                &Region::custom("region", endpoint),
                Some("bucket".into()),
                false,
            )
        };
        let key = "v2-endpoint=STORAGE.EXAMPLE%3A443%2F&bucket=bucket key 1";
        let id = format!("v2-{}", blake3::hash(format!("hash\0{key}").as_bytes()));
        let path = Db::state_dir(dir.path(), &id);
        std::fs::create_dir_all(&path).unwrap();
        let db = Db::open_existing(&path, key.into()).unwrap();
        db.save_upload_id("previous-v2").unwrap();
        assert!(super::started_here(&at("https://storage.example"), &db));
        drop(db);

        let db = Db::new(&at("STORAGE.EXAMPLE:443/"), "key", "hash", 1, dir.path()).unwrap();
        assert_eq!(db.path(), path);
        assert_eq!(db.upload_id().unwrap().as_deref(), Some("previous-v2"));
    }

    #[tokio::test]
    async fn rotated_keys_can_forget_a_dead_upload_on_its_original_server() {
        let mut server = Server::new_async().await;
        let s3 = checksum_s3(&server);
        let rotated = S3::new(
            &Credentials::new("replacement", &SecretString::new("new-secret".into())),
            &Region::custom("us-east-1", server.url()),
            Some("bucket".to_owned()),
            false,
        );
        let dir = tempfile::tempdir().unwrap();
        let db = Db::new(&s3, "key", "checksum", 1, dir.path()).unwrap();
        db.save_upload_id("uid").unwrap();
        db.create_parts([(1, 0, 4), (2, 4, 4), (3, 8, 2)], None)
            .unwrap();
        drop(db);
        let db = Db::new(&rotated, "key", "checksum", 1, dir.path()).unwrap();
        assert_eq!(db.upload_id().unwrap().as_deref(), Some("uid"));
        assert!(!super::started_here(
            &rotated.with_bucket(Some("another".into())),
            &db
        ));
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"0123456789").unwrap();
        let gone = server
            .mock("PUT", "/bucket/key")
            .match_query(Matcher::UrlEncoded("uploadId".into(), "uid".into()))
            .with_status(404)
            .with_body(NO_SUCH_UPLOAD)
            .expect(1)
            .create_async()
            .await;
        let error = upload_multipart(ten_bytes_in_parts_of_four(&rotated, &db, file.path()))
            .await
            .unwrap_err()
            .to_string();
        gone.assert_async().await;
        assert!(error.contains("no longer exists"), "{error}");
        assert_eq!(db.upload_id().unwrap(), None);

        let path = dir.path().join("old");
        std::fs::create_dir(&path).unwrap();
        let legacy = Db::open_existing(&path, format!("{} key 1", &s3.hash()[..8])).unwrap();
        assert!(super::started_here(&s3, &legacy));
        assert!(!super::started_here(&rotated, &legacy));
    }

    // #84 review: `streams resume` opens a saved state by its key. If the host now
    // points to another server (or bucket, or account), that one never had the upload:
    // its NoSuchUpload must not erase the state of an upload that may still be open
    // where it was started.
    #[tokio::test]
    async fn no_such_upload_from_another_host_keeps_the_upload() {
        let mut server = Server::new_async().await;
        let started_on = S3::new(
            &Credentials::new("AKIA", &SecretString::new("secret".into())),
            &Region::custom("us-east-1", "http://127.0.0.1:9"),
            Some("bucket".to_string()),
            false,
        );
        let s3 = checksum_s3(&server);
        let dir = tempfile::tempdir().unwrap();
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"0123456789").unwrap();
        let db = Db::new(&started_on, "key", "checksum", 1, dir.path()).unwrap();
        db.save_upload_id("uid").unwrap();
        db.create_parts([(1, 0, 4), (2, 4, 4), (3, 8, 2)], None)
            .unwrap();

        let elsewhere = server
            .mock("PUT", "/bucket/key")
            .match_query(Matcher::Any)
            .with_status(404)
            .with_body(NO_SUCH_UPLOAD)
            .expect(1)
            .create_async()
            .await;

        let error = upload_multipart(ten_bytes_in_parts_of_four(&s3, &db, file.path()))
            .await
            .unwrap_err()
            .to_string();

        elsewhere.assert_async().await;
        assert!(error.contains("its saved state was kept"), "{error}");
        assert_eq!(db.upload_id().unwrap().as_deref(), Some("uid"));
        assert_eq!(db.db_parts().unwrap().len(), 3);
    }

    // <https://github.com/s3m/s3m/issues/89>: the file was cut to 6 bytes after its
    // 10 bytes were planned as parts of 4. The second part read the 2 bytes that were
    // left and sent them; the server accepted the shorter part and the upload was
    // completed, with exit 0 and an object that was not the file. A part with fewer
    // bytes than planned is now an error, before anything is sent for it.
    #[tokio::test]
    async fn a_part_shorter_than_planned_is_not_sent() {
        let mut server = Server::new_async().await;
        let s3 = checksum_s3(&server);
        let dir = tempfile::tempdir().unwrap();
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"0123456789").unwrap();
        let db = Db::new(&s3, "key", "checksum", 1, dir.path()).unwrap();
        db.save_upload_id("uid").unwrap();

        let path = file.path().to_owned();
        let first = part_mock(&mut server, 1, "0123")
            .with_body_from_request(move |_| {
                fs::write(&path, b"012345").unwrap();
                Vec::new()
            })
            .expect(1)
            .create_async()
            .await;
        let short = server
            .mock("PUT", "/bucket/key")
            .match_query(Matcher::Regex("partNumber=[23]".into()))
            .expect(0)
            .create_async()
            .await;
        let complete = server
            .mock("POST", "/bucket/key")
            .match_query(Matcher::Any)
            .expect(0)
            .create_async()
            .await;

        let error = upload_multipart(ten_bytes_in_parts_of_four(&s3, &db, file.path()))
            .await
            .unwrap_err()
            .to_string();

        first.assert_async().await;
        short.assert_async().await;
        complete.assert_async().await;
        assert!(
            error.contains(
                "the file changed during the upload: part 2 has 2 bytes at offset 4, not the 4 \
                 it was planned with"
            ),
            "{error}"
        );
        assert_eq!(db.upload_id().unwrap().as_deref(), Some("uid"));
        assert_eq!(db.db_uploaded().unwrap().len(), 1);
        assert_eq!(db.check().unwrap(), None);
    }

    // #84 review: the abort paths (#80, #83) must not act on another server than the
    // one the upload was started on. It never had the upload: its 404 NoSuchUpload
    // was taken for "already aborted", and its 204 for an abort, and both erased the
    // state of an upload that was still open elsewhere. No abort is sent there.
    #[tokio::test]
    async fn an_upload_started_on_another_host_is_not_aborted() {
        let mut server = Server::new_async().await;
        let started_on = S3::new(
            &Credentials::new("AKIA", &SecretString::new("secret".into())),
            &Region::custom("us-east-1", "http://127.0.0.1:9"),
            Some("bucket".to_string()),
            false,
        );
        let s3 = checksum_s3(&server);
        let dir = tempfile::tempdir().unwrap();
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"0123456789").unwrap();
        let db = Db::new(&started_on, "key", "checksum", 1, dir.path()).unwrap();
        db.save_upload_id("uid").unwrap();
        // Part 2 saved with a range that isn't this file's: on the right host the
        // upload would be aborted.
        db.create_parts([(1, 0, 4), (2, 4, 6)], None).unwrap();

        let abort = server
            .mock("DELETE", "/bucket/key")
            .match_query(Matcher::Any)
            .with_status(204)
            .expect(0)
            .create_async()
            .await;

        let error = upload_multipart(ten_bytes_in_parts_of_four(&s3, &db, file.path()))
            .await
            .unwrap_err()
            .to_string();

        abort.assert_async().await;
        assert!(
            error.contains(
                "could not be aborted (the host's endpoint, bucket or keys are not the ones \
                 this upload was started with)"
            ),
            "{error}"
        );
        assert_eq!(db.upload_id().unwrap().as_deref(), Some("uid"));
        assert_eq!(db.db_parts().unwrap().len(), 2);
    }

    // #80 review: a 501 NotImplemented that isn't about the checksum (a storage class
    // here) says nothing about the checksum. The upload was aborted for it, with a
    // message blaming sha256. It is an error like any other: nothing is aborted, and
    // the upload id is kept.
    #[tokio::test]
    async fn not_implemented_about_something_else_keeps_the_upload() {
        let mut server = Server::new_async().await;
        let s3 = checksum_s3(&server);
        let dir = tempfile::tempdir().unwrap();
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"0123456789").unwrap();
        let db = Db::new(&s3, "key", "checksum", 1, dir.path()).unwrap();
        let _create = server
            .mock("POST", "/bucket/key")
            .match_query(Matcher::Regex("uploads".into()))
            .with_body(INITIATED)
            .create_async()
            .await;
        let part = server
            .mock("PUT", "/bucket/key")
            .match_query(Matcher::Any)
            .with_status(501)
            .with_body("<Error><Code>NotImplemented</Code><Message>The storage class you specified is not implemented</Message></Error>")
            .expect(1)
            .create_async()
            .await;
        let abort = server
            .mock("DELETE", "/bucket/key")
            .match_query(Matcher::Any)
            .with_status(204)
            .expect(0)
            .create_async()
            .await;

        let error = upload_multipart(sha256_upload(&s3, &db, file.path()))
            .await
            .unwrap_err()
            .to_string();

        part.assert_async().await;
        abort.assert_async().await;
        assert!(!error.contains("doesn't support sha256"), "{error}");
        assert!(error.contains("storage class"), "{error}");
        assert_eq!(db.upload_id().unwrap().as_deref(), Some("uid"));
    }

    // #83 review, round 2: the same part number saved as pending and as uploaded with
    // different ranges can't be trusted either way.
    #[tokio::test]
    async fn a_part_saved_with_two_ranges_aborts_the_upload() {
        let mut server = Server::new_async().await;
        let s3 = checksum_s3(&server);
        let dir = tempfile::tempdir().unwrap();
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"0123456789").unwrap();
        let db = Db::new(&s3, "key", "checksum", 1, dir.path()).unwrap();
        db.save_upload_id("uid").unwrap();
        db.create_parts([(1, 0, 4), (2, 4, 4), (3, 8, 2)], None)
            .unwrap();
        let uploaded = Part::new(2, 4, 6, None).set_etag("\"p2\"".to_string());
        db.db_uploaded()
            .unwrap()
            .insert(
                2_u16.to_be_bytes(),
                to_bytes::<RkyvError>(&uploaded).unwrap().as_slice(),
            )
            .unwrap();

        let abort = server
            .mock("DELETE", "/bucket/key")
            .match_query(Matcher::UrlEncoded("uploadId".into(), "uid".into()))
            .with_status(204)
            .expect(1)
            .create_async()
            .await;
        let sent = server
            .mock("PUT", "/bucket/key")
            .match_query(Matcher::Any)
            .expect(0)
            .create_async()
            .await;

        let error = upload_multipart(ten_bytes_in_parts_of_four(&s3, &db, file.path()))
            .await
            .unwrap_err()
            .to_string();

        abort.assert_async().await;
        sent.assert_async().await;
        assert!(
            error.contains("part 2 is saved with two different ranges"),
            "{error}"
        );
        assert_eq!(db.upload_id().unwrap(), None);
    }

    // #84 review: a 404 whose body is not an S3 <Error> document doesn't say the
    // upload is gone, even if it has a <Code>NoSuchUpload</Code> in it. The upload was
    // forgotten for it; it is now an error like any other, and the upload id is kept.
    #[tokio::test]
    async fn no_such_upload_in_another_document_keeps_the_upload() {
        let mut server = Server::new_async().await;
        let s3 = checksum_s3(&server);
        let dir = tempfile::tempdir().unwrap();
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"0123456789").unwrap();
        let db = Db::new(&s3, "key", "checksum", 1, dir.path()).unwrap();
        db.save_upload_id("uid").unwrap();
        db.create_parts([(1, 0, 4), (2, 4, 4), (3, 8, 2)], None)
            .unwrap();

        let other = server
            .mock("PUT", "/bucket/key")
            .match_query(Matcher::Any)
            .with_status(404)
            .with_body("<ReplicaStatus><Code>NoSuchUpload</Code></ReplicaStatus>")
            .expect(1)
            .create_async()
            .await;

        let error = upload_multipart(ten_bytes_in_parts_of_four(&s3, &db, file.path()))
            .await
            .unwrap_err()
            .to_string();

        other.assert_async().await;
        assert!(!error.contains("no longer exists"), "{error}");
        assert_eq!(db.upload_id().unwrap().as_deref(), Some("uid"));
        assert_eq!(db.db_parts().unwrap().len(), 3);
    }

    // #89 review: the file was rewritten with the same size while its hash was computed,
    // before the upload started. The modification time read with the size is passed
    // in; a different one now stops the upload before any part is sent, and a saved
    // upload is aborted. The same time lets the upload go on.
    #[tokio::test]
    async fn a_file_rewritten_before_the_upload_started_is_not_uploaded() {
        for (rewritten, expect_abort) in [(true, true), (false, false)] {
            let mut server = Server::new_async().await;
            let s3 = checksum_s3(&server);
            let dir = tempfile::tempdir().unwrap();
            let file = tempfile::NamedTempFile::new().unwrap();
            fs::write(file.path(), "0123456789").unwrap();
            file.as_file().set_modified(UNIX_EPOCH).unwrap();
            let read_at = 0_u128;
            if rewritten {
                fs::write(file.path(), "9876543210").unwrap();
            }
            let db = Db::new(&s3, "key", "checksum", 1, dir.path()).unwrap();
            db.save_upload_id("uid").unwrap();

            let parts = server
                .mock("PUT", "/bucket/key")
                .match_query(Matcher::Any)
                .with_header("ETag", "\"p\"")
                .expect(if rewritten { 0 } else { 3 })
                .create_async()
                .await;
            let abort = server
                .mock("DELETE", "/bucket/key")
                .match_query(Matcher::UrlEncoded("uploadId".into(), "uid".into()))
                .with_status(204)
                .expect(usize::from(expect_abort))
                .create_async()
                .await;
            let complete = server
                .mock("POST", "/bucket/key")
                .match_query(Matcher::UrlEncoded("uploadId".into(), "uid".into()))
                .with_body(COMPLETED_3)
                .expect(usize::from(!rewritten))
                .create_async()
                .await;

            let mut request = ten_bytes_in_parts_of_four(&s3, &db, file.path());
            request.file_mtime = Some(read_at);
            let result = upload_multipart(request).await;

            for mock in [parts, abort, complete] {
                mock.assert_async().await;
            }
            if rewritten {
                let error = result.unwrap_err().to_string();
                assert!(
                    error.contains("modification time changed since it was read"),
                    "{error}"
                );
                assert_eq!(db.upload_id().unwrap(), None);
            } else {
                result.unwrap();
            }
        }
    }
}
