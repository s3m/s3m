## 0.3.0
* Using [blake3](https://crates.io/crates/blake3), for creating the file checksum, thanks @oconnor663
* Using [sled](http://sled.rs/) to keep track of the uploaded  files.

## 0.2.0
* Implemented lifetimes  🌱
* `sha256` and `md5` returning digest in the same loop using `async` so that we could use `Content-MD5` to better check integrity of an object.
* [blake2](https://crates.io/crates/blake2s_simd) for keeping track of upload progress using `sled` (WIP)
