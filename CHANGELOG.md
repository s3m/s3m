## [Unreleased]
* New sub-command `get` with option `-h` to return HeadObject.
* New sub-command `share` to create presigned URL's.



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
