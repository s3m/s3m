//! Exercise each CLI pagination loop with empty pages and non-adjacent cycles.
#[path = "support/audit_cli.rs"]
mod audit_cli;

use anyhow::{Result, ensure};
use audit_cli::{Cli, query, signed};
use mockito::{Matcher, Mock, Server, ServerGuard};
use serde_json::Value;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Clone, Copy, Debug)]
enum Mode {
    Objects,
    Uploads,
    Buckets,
}

impl Mode {
    fn args(self) -> Vec<&'static str> {
        match self {
            Self::Objects => vec!["ls", "s3/bucket", "--number", "3", "--json"],
            Self::Uploads => vec!["ls", "s3/bucket", "--multipart", "--number", "3", "--json"],
            Self::Buckets => vec!["ls", "s3", "--number", "3", "--json"],
        }
    }

    fn path(self) -> &'static str {
        match self {
            Self::Objects | Self::Uploads => "/bucket",
            Self::Buckets => "/",
        }
    }

    fn query(
        self,
        cursor: Option<&'static str>,
        remaining: &'static str,
    ) -> Vec<(&'static str, &'static str)> {
        let mut pairs = match self {
            Self::Objects => vec![("list-type", "2"), ("max-keys", remaining)],
            Self::Uploads => vec![("uploads", ""), ("max-uploads", remaining)],
            Self::Buckets => vec![("max-buckets", remaining)],
        };
        if let Some(cursor) = cursor {
            match self {
                Self::Objects | Self::Buckets => pairs.push(("continuation-token", cursor)),
                Self::Uploads => {
                    pairs.push(("key-marker", cursor));
                    pairs.push(("upload-id-marker", cursor));
                }
            }
        }
        pairs
    }

    fn page(self, name: Option<&str>, cursor: Option<&str>) -> String {
        let entry = name.map_or_else(String::new, |name| match self {
            Self::Objects => format!(
                "<Contents><Key>{name}</Key><LastModified>2020-01-01T00:00:00Z</LastModified>\
                 <ETag>etag</ETag><Size>1</Size><StorageClass>STANDARD</StorageClass></Contents>"
            ),
            Self::Uploads => format!(
                "<Upload><Key>{name}</Key><UploadId>id-{name}</UploadId>\
                 <Initiated>2020-01-01T00:00:00Z</Initiated><StorageClass>STANDARD</StorageClass></Upload>"
            ),
            Self::Buckets => format!(
                "<Bucket><Name>{name}</Name><CreationDate>2020-01-01T00:00:00Z</CreationDate></Bucket>"
            ),
        });
        let next = cursor.map_or_else(String::new, |cursor| match self {
            Self::Objects => format!("<NextContinuationToken>{cursor}</NextContinuationToken>"),
            Self::Uploads => format!(
                "<NextKeyMarker>{cursor}</NextKeyMarker><NextUploadIdMarker>{cursor}</NextUploadIdMarker>"
            ),
            Self::Buckets => format!("<ContinuationToken>{cursor}</ContinuationToken>"),
        });
        let truncated = cursor.is_some();
        match self {
            Self::Objects => format!(
                "<ListBucketResult><Name>bucket</Name><Prefix></Prefix><MaxKeys>3</MaxKeys>\
                 <IsTruncated>{truncated}</IsTruncated>{next}{entry}</ListBucketResult>"
            ),
            Self::Uploads => format!(
                "<ListMultipartUploadsResult><Bucket>bucket</Bucket><MaxUploads>3</MaxUploads>\
                 <IsTruncated>{truncated}</IsTruncated>{next}{entry}</ListMultipartUploadsResult>"
            ),
            Self::Buckets => format!(
                "<ListAllMyBucketsResult><Buckets>{entry}</Buckets>{next}</ListAllMyBucketsResult>"
            ),
        }
    }
}

struct Page {
    mode: Mode,
    cursor: Option<&'static str>,
    remaining: &'static str,
    body: String,
}

fn mock_page(server: &mut ServerGuard, page: &Page) -> Mock {
    let matches_query = query(&page.mode.query(page.cursor, page.remaining));
    let available = AtomicBool::new(true);
    server
        .mock("GET", page.mode.path())
        .match_query(Matcher::Any)
        // Serve each cursor once. A broken cycle guard receives an unmatched-request
        // error on its fourth request instead of leaving the child process looping.
        .match_request(move |request| {
            matches_query(request) && available.swap(false, Ordering::SeqCst)
        })
        .match_header("authorization", signed())
        .with_body(&page.body)
        .expect(1)
        .create()
}

#[test]
fn short_then_empty_nonterminal_pages_reach_the_final_record_in_each_mode() -> Result<()> {
    for mode in [Mode::Objects, Mode::Uploads, Mode::Buckets] {
        let mut server = Server::new();
        let mut mocks = Vec::new();
        for (cursor, remaining, name, next) in [
            (None, "3", Some("first"), Some("A")),
            (Some("A"), "2", None, Some("B")),
            (Some("B"), "2", Some("last"), None),
        ] {
            mocks.push(mock_page(
                &mut server,
                &Page {
                    mode,
                    cursor,
                    remaining,
                    body: mode.page(name, next),
                },
            ));
        }
        let cli = Cli::new(&server.url())?;
        let value: Value = serde_json::from_str(&cli.run(&mode.args(), 0)?)?;
        let collection = match mode {
            Mode::Objects => "objects",
            Mode::Uploads => "uploads",
            Mode::Buckets => "buckets",
        };
        let records = value
            .get(collection)
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow::anyhow!("{mode:?}: {value}"))?;
        let field = match mode {
            Mode::Objects | Mode::Uploads => "key",
            Mode::Buckets => "name",
        };
        let names: Vec<Option<&str>> = records
            .iter()
            .map(|record| record.get(field).and_then(Value::as_str))
            .collect();
        assert_eq!(names, [Some("first"), Some("last")], "{mode:?}");
        match mode {
            Mode::Uploads => {
                let ids: Vec<Option<&str>> = records
                    .iter()
                    .map(|record| record.get("upload_id").and_then(Value::as_str))
                    .collect();
                assert_eq!(ids, [Some("id-first"), Some("id-last")]);
            }
            Mode::Objects | Mode::Buckets => {}
        }
        ensure!(cli.files()?.is_empty(), "{mode:?} wrote files");
        for mock in mocks {
            mock.assert();
        }
    }
    Ok(())
}

#[test]
fn nonadjacent_a_b_a_cursor_cycles_fail_after_exactly_three_requests_in_each_mode() -> Result<()> {
    for mode in [Mode::Objects, Mode::Uploads, Mode::Buckets] {
        let mut server = Server::new();
        let mut mocks = Vec::new();
        for (cursor, next) in [(None, "A"), (Some("A"), "B"), (Some("B"), "A")] {
            mocks.push(mock_page(
                &mut server,
                &Page {
                    mode,
                    cursor,
                    remaining: "3",
                    body: mode.page(None, Some(next)),
                },
            ));
        }
        let cli = Cli::new(&server.url())?;
        let stderr = cli.run(&mode.args(), 1)?;
        ensure!(stderr.contains("repeated"), "{mode:?}: {stderr}");
        for mock in mocks {
            mock.assert();
        }
    }
    Ok(())
}
