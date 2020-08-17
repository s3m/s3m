## 0.2.0
* Implemented lifetimes  🌱
* `sha256` and `md5` returning digest in the same loop using `async` so that we could use `Content-MD5` to better check integrity of an object.
* [blake2](https://crates.io/crates/blake2s_simd) for keeping track of upload progress using `sled` (WIP)
