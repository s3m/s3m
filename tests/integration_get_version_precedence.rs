//! Preserve the existing versions-listing branch when a version ID is also supplied.
#[path = "support/audit_cli.rs"]
mod audit_cli;

use anyhow::Result;
use audit_cli::{Cli, query, signed};
use mockito::{Matcher, Server};
use serde_json::{Value, json};

#[test]
fn versions_with_version_id_keeps_listing_precedence_and_creates_no_file() -> Result<()> {
    let mut server = Server::new();
    let listing = server
        .mock("GET", "/bucket")
        .match_query(Matcher::Any)
        .match_request(query(&[("versions", ""), ("prefix", "dir/data.bin")]))
        .match_header("authorization", signed())
        .with_body(
            "<ListVersionsResult><Name>bucket</Name><Prefix>dir/data.bin</Prefix>\
             <KeyMarker></KeyMarker><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>\
             <Version><Key>dir/data.bin</Key><VersionId>v-new</VersionId>\
             <IsLatest>true</IsLatest><LastModified>2026-03-14T08:00:00Z</LastModified>\
             <ETag>new</ETag><Size>42</Size><StorageClass>STANDARD</StorageClass></Version>\
             <Version><Key>dir/data.bin</Key><VersionId>v-old</VersionId>\
             <IsLatest>false</IsLatest><LastModified>2026-03-13T08:00:00Z</LastModified>\
             <ETag>old</ETag><Size>7</Size><StorageClass>STANDARD</StorageClass></Version>\
             </ListVersionsResult>",
        )
        .expect(3)
        .create();
    let download = server
        .mock("GET", "/bucket/dir/data.bin")
        .expect(0)
        .create();
    let metadata = server.mock("HEAD", Matcher::Any).expect(0).create();
    let cli = Cli::new(&server.url())?;
    let expected = json!({
        "bucket": "bucket",
        "key_prefix": "dir/data.bin",
        "versions": [
            {"key": "dir/data.bin", "version_id": "v-new", "is_latest": true,
             "last_modified": "2026-03-14T08:00:00Z", "size_bytes": 42,
             "etag": "new", "storage_class": "STANDARD"},
            {"key": "dir/data.bin", "version_id": "v-old", "is_latest": false,
             "last_modified": "2026-03-13T08:00:00Z", "size_bytes": 7,
             "etag": "old", "storage_class": "STANDARD"}
        ]
    });
    for flags in [
        vec!["--version", "v-old", "--versions", "--json"],
        vec!["--versions", "--version", "v-old", "--json"],
    ] {
        let mut args = vec!["get", "s3/bucket/dir/data.bin"];
        args.extend(flags);
        let actual: Value = serde_json::from_str(&cli.run(&args, 0)?)?;
        assert_eq!(actual, expected);
    }
    let text = cli.run(
        &[
            "get",
            "s3/bucket/dir/data.bin",
            "--version",
            "v-old",
            "--versions",
        ],
        0,
    )?;
    assert!(text.contains("v-new"), "{text}");
    assert!(text.contains("v-old"), "{text}");
    assert_eq!(cli.files()?, Vec::<std::path::PathBuf>::new());
    listing.assert();
    download.assert();
    metadata.assert();
    Ok(())
}
