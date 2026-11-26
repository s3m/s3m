#!/usr/bin/env bash
# Upload checks against a real Cloudflare R2 bucket.
#
#   scripts/r2-check.sh <s3m binary> <config.yml> <host> <bucket>
#
# Uploads a few MiB under ci/<unique prefix>/ and deletes every object it created.
# Use a dedicated test bucket and an API token limited to it.
#
# Known R2 limitation, not checked here: multipart uploads with --checksum sha1 or
# sha256 (R2 answers 501 for per-part SHA checksums).
set -euo pipefail

if [[ $# -ne 4 ]]; then
  echo "usage: $0 <s3m binary> <config.yml> <host> <bucket>" >&2
  exit 2
fi

bin=$(realpath "$1")
config=$(realpath "$2")
target="$3/$4"
prefix="ci/$(date -u +%Y%m%dT%H%M%SZ)-${GITHUB_RUN_ID:-local}-$RANDOM"

work=$(mktemp -d)
export S3M_HOME="$work/home"
created=()

s3m() { "$bin" --config "$config" "$@"; }

cleanup() {
  status=$?
  for key in "${created[@]}"; do
    s3m rm "$target/$key" >/dev/null 2>&1 || {
      echo "warning: could not delete $target/$key" >&2
    }
  done
  rm -rf "$work"
  exit "$status"
}
trap cleanup EXIT

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

# upload <name> <file> [s3m options...]: prints s3m's output. It runs in a
# subshell ($(upload ...)), so callers record the key with `track` first.
upload() {
  local name=$1 file=$2
  shift 2
  s3m --retries 1 "$@" "$file" "$target/$prefix/$name"
}

track() {
  created+=("$prefix/$1")
}

# stored_checksum <name> <algorithm>: the x-amz-checksum-<algorithm> R2 stored
stored_checksum() {
  s3m get -m "$target/$prefix/$1" | awk -v h="x-amz-checksum-$2:" 'tolower($1) == h { print $2 }'
}

# check_checksum <name> <file> <algorithm> [s3m options...]
check_checksum() {
  local name=$1 file=$2 alg=$3
  shift 3
  local output printed stored
  track "$name"
  output=$(upload "$name" "$file" --checksum "$alg" "$@") || fail "$name: upload failed: $output"
  printed=$(awk -v l="$alg" '$1 == l && $2 == "(base64):" { print $3 }' <<<"$output")
  stored=$(stored_checksum "$name" "$alg")
  [[ -n "$printed" ]] || fail "$name: no '$alg (base64):' line in: $output"
  [[ "$printed" == "$stored" ]] || fail "$name: printed $printed, R2 stored ${stored:-nothing}"
  echo "ok  $name  $alg $printed"
}

# check_download <name> <file>
check_download() {
  s3m get -q "$target/$prefix/$1" "$work/$1.download" >/dev/null
  cmp -s "$2" "$work/$1.download" || fail "$1: downloaded bytes differ"
  echo "ok  $1  download matches"
}

head -c 1048576 /dev/urandom >"$work/small.bin"
head -c 12582912 /dev/urandom >"$work/big.bin"
part=(--buffer 5242880)

echo "R2 checks under $target/$prefix/"

# Without --checksum, uploads send Content-MD5.
track small-plain.bin
output=$(upload small-plain.bin "$work/small.bin") || fail "small-plain.bin: $output"
grep -q '^ETag: ' <<<"$output" || fail "small-plain.bin: no ETag in: $output"
echo "ok  small-plain.bin"
track big-plain.bin
output=$(upload big-plain.bin "$work/big.bin" "${part[@]}") || fail "big-plain.bin: $output"
grep -q '^ETag: ".*-3"$' <<<"$output" || fail "big-plain.bin: no multipart ETag in: $output"
check_download big-plain.bin "$work/big.bin"

# Single-part with --checksum: R2 rejects Content-MD5 next to x-amz-checksum-* (#73).
for alg in crc32 crc32c sha1 sha256; do
  check_checksum "small-$alg.bin" "$work/small.bin" "$alg"
done

# Multipart with --checksum: R2 rejects a composite checksum header on
# CompleteMultipartUpload (#73); it derives the same value from the parts.
for alg in crc32 crc32c; do
  check_checksum "big-$alg.bin" "$work/big.bin" "$alg" "${part[@]}"
done
check_download big-crc32c.bin "$work/big.bin"

echo "all R2 checks passed"
