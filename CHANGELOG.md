## 0.11.1
* Added option `--version` to command `get` to download a specific object version
* Fix missing bandwithd throttle when using `--pipe` or `--compress` option

## 0.11.0 🚆
* Added option `--versions` to command `get` to list object versions
* Added option `--number` to command `ls` to limit list to a number of objects, upload or buckets

## 0.10.0 🛶
* Compression support `-x/--compress` (using zstd)

## 0.9.5
* Fixed a regression that prevented listing buckets when only a host is defined. Added a test case to cover this scenario.

## 0.9.4
* Accept new regions if any `s3.new-region.amazonaws.com`
* fixes #58 ensure min(1)/max(255) when using num_cpus

## 0.9.3
* Allow path(key) names ending with `/` fixes #57

## 0.9.0 🚅
* Added option `-r/--retries` defaults to 3 (exponential backoff 1 second)

## 0.8.3
* Added user-agent on the request
* Implemented MAX_RETRIES when reading from STDIN
* Fixed bug when chunk > 512MB (next 8192 bytes were not stored)

## 0.8.2
* Using `openssl = { version = "0.10", optional = true, features = ["vendored"] }` to make FreeBSD Port and Linux package (musl)

## 0.8.1
* Added option `-k/--kilobytes` to throttle the bandwidth in kilobytes per second

## 0.8.0 🚋
* Added option `--no-sign-request` to support public buckets

## 0.7.6
* Added option `-n/--number` [1..3500] to specify the max number of concurrent requests

## 0.7.4
* Added --verbose flag
* Fixed bug potential to loop indefinitely while creating multi-parts

## 0.7.1
* Removed after_help.
* Added `-f/--force` when getting a file.
* Replaced `-H/--head` with `-m/--meta` to retrieve metadata.

## 0.7.0 🚜
* Added support for Additional Checksum Algorithms `x-amz-checksum-mode` option `--checksum`

## 0.6.1
* Added subcommand `show` instead of option `-s/--show` to list current available hosts

## 0.6.0 🛺
* Added sub-comand `cb` (Create bucket)
* Added option `-b/--bucket` to remove bucket when using `rm`
* Added option `-t/--tmp-dir` directory for temporarily storing the stdin buffer.
* Added option `-s/--show` to list current available hosts

## 0.5.0 🛵
* removed ending `/` when listing buckets
* fixed sub-command `get` to download file in the current path
* Added option `--clean` instead of `-r/--remove`
* Added option `-a/--acl` to set `x-amz-acl` for the object
* Added option `-m/--meta` to add custom metadata for the object
* Added option `-q/--quiet` to silent output
* Added option `-p/--pipe` to read from STDIN
* STDIN pipe/stream will be uploaded in chunks to prevent keeping the chunk in RAM it uses a temp file (512MB) and from there streams
* `ls` now supports options  `-p/--prefix` and `-a/--start-after` to list only files starting with a prefix or start listing from a specified key
* new sub-command `acl` to get and set ACL's


## 0.4.0 ⛸
* New sub-command `get` with option `-h` to return HeadObject.
* New sub-command `share` to create presigned URL's.
* Defaults to 512MB buffer size when no `-b` defined in case input is 5TB when uploading from STDIN.


## 0.3.1 🛴
* STDIN pipe/stream will upload the chunks in oneshot not using `Transfer-Encoding: chunked`
* `{bytes}/{total_bytes}` in progress bar.
* `UTC` timezone when listing objects/buckets.
* Print uploaded bytes when streaming (#19).


## 0.3.0  🚲
* Using [blake3](https://crates.io/crates/blake3), for creating the file checksum, thanks @oconnor663
* Using [sled](http://sled.rs/) with [serde_cbor](https://crates.io/crates/serde_cbor) to keep track of the uploaded files. thanks @spacejam, @D1plo1d
* Multipart uploads can be resumed and is the default behavior.
* The file checksum, mtime and the s3 credentials are used to keep track of the uploads this also prevent uploading the same file multiple times to the same location.
* Added option `-r` to cleanup `~/.s3m/streams` directory.
* Added option `ls -m` to list in-progress multipart uploads.
* New sub-command `rm` to remove objects and abort a multipart upload.
* `ls` list format (green, yellow, key) "date, size, file name"
* STDIN pipe/stream uploads (WIP #17)


## 0.2.0
* Implemented lifetimes  🌱
* `sha256` and `md5` returning digest in the same loop using `async` so that we could use `Content-MD5` to better check integrity of an object.
* [blake2](https://crates.io/crates/blake2s_simd) for keeping track of upload progress using `sled` (WIP)
