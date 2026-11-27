#!/usr/bin/env bash
# Upload checks against a real Cloudflare R2 bucket.
#
#   scripts/r2-check.sh <s3m binary> <config.yml> <host> <bucket>
#
# Uploads a few MiB under ci/<unique prefix>/ and deletes every object it created,
# and aborts any multipart upload it left unfinished. Use a dedicated test bucket
# and an API token limited to it. Needs jq.
#
# Known R2 limitation, checked at the end: multipart uploads with --checksum sha1 or
# sha256 (R2 answers 501 for per-part SHA checksums, #80).
set -euo pipefail

if [[ $# -ne 4 ]]; then
  echo "usage: $0 <s3m binary> <config.yml> <host> <bucket>" >&2
  exit 2
fi
command -v jq >/dev/null || {
  echo "jq is required" >&2
  exit 2
}

# The checks parse s3m's output; keep it free of color codes.
export NO_COLOR=1
unset CLICOLOR_FORCE

bin=$(realpath "$1")
config=$(realpath "$2")
host=$3
target="$3/$4"
prefix="ci/$(date -u +%Y%m%dT%H%M%SZ)-${GITHUB_RUN_ID:-local}-$RANDOM"

work=$(mktemp -d)
# Upload state of this run only, so cleanup finds the uploads it left unfinished.
export S3M_HOME="$work/home"
created=()

s3m() { "$bin" --config "$config" "$@"; }

# abort_listed <label>: reads "<host path>\t<upload id>" lines and aborts each upload.
abort_listed() {
  local label=$1 path upload_id status=0
  while IFS=$'\t' read -r path upload_id; do
    [[ -n "$upload_id" ]] || continue
    s3m rm "$path" --abort "$upload_id" >/dev/null 2>&1 || {
      echo "cleanup: could not abort the upload of $path ($label)" >&2
      status=1
    }
  done
  return "$status"
}

cleanup() {
  local status=$? failed=0 key streams uploads listed
  trap - EXIT
  for key in ${created[@]+"${created[@]}"}; do
    s3m rm "$target/$key" >/dev/null 2>&1 || {
      echo "cleanup: could not delete $target/$key" >&2
      failed=1
    }
  done
  # A failed or interrupted multipart upload keeps its parts until aborted. First the
  # uploads this run recorded in its own state.
  if [[ -d "$S3M_HOME" ]]; then
    if streams=$(s3m streams --json ls 2>/dev/null) &&
      listed=$(jq -r --arg host "$host" '.streams[] | select(.etag == null and .upload_id != null) | [$host + "/" + .destination, .upload_id] | @tsv' <<<"$streams"); then
      abort_listed "recorded state" <<<"$listed" || failed=1
    else
      echo "cleanup: could not read the upload state in $S3M_HOME" >&2
      failed=1
    fi
  fi
  # Then whatever the server still has open under this run's prefix. The state above
  # can't show an upload whose id was never recorded, or one whose state is locked by
  # an s3m that is still running. If this script alone is killed while such a child
  # runs, the bucket's lifecycle rule for incomplete multipart uploads is the backstop.
  if uploads=$(s3m ls "$target" --multipart --prefix "$prefix/" --json 2>/dev/null) &&
    listed=$(jq -r --arg target "$target" '.uploads[] | [$target + "/" + .key, .upload_id] | @tsv' <<<"$uploads"); then
    abort_listed "still open on the server" <<<"$listed" || failed=1
  else
    echo "cleanup: could not list the unfinished uploads under $target/$prefix/" >&2
    failed=1
  fi
  rm -rf "$work"
  # Leftovers in the bucket fail an otherwise green run.
  if [[ $status -eq 0 && $failed -ne 0 ]]; then
    status=1
  fi
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

# Listing without a prefix: R2 leaves <Prefix> out of the response (#78).
listing=$(s3m ls "$target" 2>&1) || fail "ls $target: $listing"
awk -v key="$prefix/big-plain.bin" '$NF == key { found = 1 } END { exit !found }' <<<"$listing" ||
  fail "ls $target doesn't list $prefix/big-plain.bin"
echo "ok  ls without a prefix"

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

# Multipart with a SHA checksum: R2 answers 501 NotImplemented to the parts (#80).
# s3m must stop at once, abort the upload and say so. If R2 starts accepting them,
# this fails: move sha1/sha256 to the multipart checks above.
track big-sha256.bin
if output=$(upload big-sha256.bin "$work/big.bin" --checksum sha256 "${part[@]}" 2>&1); then
  fail "big-sha256.bin: a multipart upload with --checksum sha256 succeeded: $output"
fi
grep -q "doesn't support sha256 checksums on the parts of a multipart upload, and the upload was aborted" <<<"$output" ||
  fail "big-sha256.bin: unexpected error: $output"
echo "ok  big-sha256.bin  rejected at once and aborted"

echo "all R2 checks passed"
