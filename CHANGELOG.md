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
