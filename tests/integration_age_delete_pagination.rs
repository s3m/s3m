//! The 1000/1000/1 fixture goes through the real CLI and every `DeleteObjects` batch.
#[path = "support/audit_cli.rs"]
mod audit_cli;

use anyhow::Result;
use audit_cli::{Cli, query, signed};
use mockito::{Matcher, Server};
use std::{fmt::Write as _, ops::Range};

fn objects(range: Range<usize>) -> Result<String> {
    let mut body = String::new();
    for number in range {
        write!(
            body,
            "<Contents><Key>old/key-{number:04}</Key>\
             <LastModified>2000-01-01T00:00:00Z</LastModified><ETag>e</ETag>\
             <Size>1</Size><StorageClass>STANDARD</StorageClass></Contents>"
        )?;
    }
    Ok(body)
}

fn delete_body(range: Range<usize>) -> Result<String> {
    let mut body = String::from("<Delete xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");
    for number in range {
        write!(body, "<Object><Key>old/key-{number:04}</Key></Object>")?;
    }
    body.push_str("<Quiet>true</Quiet></Delete>");
    Ok(body)
}

#[test]
fn age_delete_lists_and_deletes_all_2001_exact_keys_over_three_pages_and_batches() -> Result<()> {
    let mut server = Server::new();
    let mut mocks = Vec::new();
    for (range, cursor, next) in [
        (0..1000, None, Some("page-A")),
        (1000..2000, Some("page-A"), Some("page-B")),
        (2000..2001, Some("page-B"), None),
    ] {
        let mut pairs = vec![("list-type", "2"), ("max-keys", "1000"), ("prefix", "old/")];
        if let Some(cursor) = cursor {
            pairs.push(("continuation-token", cursor));
        }
        let next_xml = next.map_or_else(String::new, |next| {
            format!("<NextContinuationToken>{next}</NextContinuationToken>")
        });
        let body = format!(
            "<ListBucketResult><Name>bucket</Name><Prefix>old/</Prefix><MaxKeys>1000</MaxKeys>\
             <IsTruncated>{}</IsTruncated>{next_xml}{}</ListBucketResult>",
            next.is_some(),
            objects(range.clone())?
        );
        mocks.push(
            server
                .mock("GET", "/bucket")
                .match_query(Matcher::Any)
                .match_request(query(&pairs))
                .match_header("authorization", signed())
                .with_body(body)
                .expect(1)
                .create(),
        );
        mocks.push(
            server
                .mock("POST", "/bucket")
                .match_query(Matcher::Any)
                .match_request(query(&[("delete", "")]))
                .match_header("authorization", signed())
                .match_body(Matcher::Exact(delete_body(range)?))
                .with_body("<DeleteResult/>")
                .expect(1)
                .create(),
        );
    }
    let single_or_bucket_delete = server.mock("DELETE", Matcher::Any).expect(0).create();
    let cli = Cli::new(&server.url())?;
    let stdout = cli.run(&["rm", "s3/bucket/old/", "--older-than", "1d"], 0)?;
    assert_eq!(stdout, "Matched 2001 objects; deleted 2001 objects.\n");
    assert_eq!(cli.files()?, Vec::<std::path::PathBuf>::new());
    for mock in mocks {
        mock.assert();
    }
    single_or_bucket_delete.assert();
    Ok(())
}
