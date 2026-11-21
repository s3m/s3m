//! Filtered totals bound the signed `ListObjectsV2` protocol as well as CLI output.
#[path = "support/audit_cli.rs"]
mod audit_cli;

use anyhow::Result;
use audit_cli::{Cli, query, signed};
use mockito::{Matcher, Server};
use serde_json::{Value, json};
use std::fmt::Write as _;

const OLD: &str = "2000-01-01T00:00:00Z";
const NEW: &str = "2999-01-01T00:00:00Z";
const TOKEN: &str = "A+/=\u{e9}";

fn page(entries: &[(&str, &str)], next: Option<&str>, max_keys: &str) -> Result<String> {
    let mut contents = String::new();
    for (key, timestamp) in entries {
        write!(
            contents,
            "<Contents><Key>{key}</Key><LastModified>{timestamp}</LastModified>\
             <ETag>e</ETag><Size>1</Size><StorageClass>STANDARD</StorageClass></Contents>"
        )?;
    }
    let next = next.map_or_else(String::new, |token| {
        format!("<NextContinuationToken>{token}</NextContinuationToken>")
    });
    Ok(format!(
        "<ListBucketResult><Name>bucket</Name><Prefix>logs/</Prefix><MaxKeys>{max_keys}</MaxKeys>\
         <IsTruncated>true</IsTruncated>{next}{contents}</ListBucketResult>"
    ))
}

#[test]
fn filtered_requests_use_full_pages_and_stop_at_the_eligible_total() -> Result<()> {
    for json_output in [false, true] {
        let mut server = Server::new();
        let mut mocks = Vec::new();
        for (cursor, entries, next) in [
            (
                None,
                vec![("logs/new-a", NEW), ("logs/new-b", NEW)],
                Some(TOKEN),
            ),
            (
                Some(TOKEN),
                vec![("logs/old-a", OLD), ("logs/new-c", NEW)],
                Some("B"),
            ),
            (Some("B"), vec![("logs/new-d", NEW)], Some("C")),
            // Exhaustion must precede parsing the unused bad timestamp or
            // checking the missing trailing cursor.
            (
                Some("C"),
                vec![("logs/old-b", OLD), ("logs/bad", "invalid")],
                None,
            ),
        ] {
            let mut pairs = vec![
                ("list-type", "2"),
                ("max-keys", "1000"),
                ("prefix", "logs/"),
            ];
            if let Some(cursor) = cursor {
                pairs.push(("continuation-token", cursor));
            } else {
                pairs.push(("start-after", "logs/before"));
            }
            mocks.push(
                server
                    .mock("GET", "/bucket")
                    .match_query(Matcher::Any)
                    .match_request(query(&pairs))
                    .match_header("authorization", signed())
                    .with_body(page(&entries, next, "1000")?)
                    .expect(1)
                    .create(),
            );
        }
        let cli = Cli::new(&server.url())?;
        let mut args = vec![
            "ls",
            "s3/bucket/logs/",
            "--older-than",
            "1d",
            "--number",
            "2",
            "--start-after",
            "logs/before",
        ];
        if json_output {
            args.push("--json");
        }
        let stdout = cli.run(&args, 0);
        for mock in mocks {
            mock.assert();
        }
        let stdout = stdout?;
        if json_output {
            let value: Value = serde_json::from_str(&stdout)?;
            let objects: Vec<Value> = ["logs/old-a", "logs/old-b"]
                .into_iter()
                .map(|key| {
                    json!({
                        "bucket": "bucket", "key": key, "size_bytes": 1,
                        "last_modified": OLD, "etag": "e", "storage_class": "STANDARD"
                    })
                })
                .collect();
            assert_eq!(
                value,
                json!({
                    "kind": "objects", "bucket": "bucket", "prefix": "logs/",
                    "start_after": "logs/before", "objects": objects
                })
            );
        } else {
            let keys: Vec<Option<&str>> = stdout
                .lines()
                .map(|line| line.split_whitespace().last())
                .collect();
            assert_eq!(keys, [Some("logs/old-a"), Some("logs/old-b")]);
        }
        assert_eq!(cli.files()?, Vec::<std::path::PathBuf>::new());
    }
    Ok(())
}

#[test]
fn zero_filtered_total_makes_no_request_in_text_or_json() -> Result<()> {
    for json_output in [false, true] {
        let mut server = Server::new();
        let no_request = server.mock("GET", Matcher::Any).expect(0).create();
        let cli = Cli::new(&server.url())?;
        let mut args = vec![
            "ls",
            "s3/bucket/logs/",
            "--older-than",
            "1d",
            "--number",
            "0",
        ];
        if json_output {
            args.push("--json");
        }
        let stdout = cli.run(&args, 0)?;
        if json_output {
            assert_eq!(
                serde_json::from_str::<Value>(&stdout)?,
                json!({
                    "kind": "objects", "bucket": "bucket", "prefix": "logs/",
                    "start_after": null, "objects": []
                })
            );
        } else {
            assert_eq!(stdout, "");
        }
        assert_eq!(cli.files()?, Vec::<std::path::PathBuf>::new());
        no_request.assert();
    }
    Ok(())
}
