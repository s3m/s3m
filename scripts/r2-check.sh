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
# sha256 (R2 answers 501 for per-part SHA checksums, #80). Then the interrupted and
# changing uploads of scripts/provider-scenarios.sh.
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
bucket=$4
target="$host/$bucket"
# shellcheck source=scripts/provider-scenarios.sh
source "$(dirname "$(realpath "$0")")/provider-scenarios.sh"
prefix=$(run_prefix)

work=$(mktemp -d)
export S3M_HOME="$work/home"
created="$work/created"
: >"$created"

track() {
  printf '%s\n' "$prefix/$1" >>"$created"
}

object_rows() {
  s3m ls "$target" --prefix "$1" --json | json_array objects |
    jq -r --arg p "$1" "$safe_string"'
      if all(.[]; (.key | safe_string and startswith($p)))
      then .[].key else error("invalid object row") end'
}

delete_rows() {
  local scope=$1 key failed=0
  while IFS= read -r key; do
    [[ -n $key ]] || continue
    if [[ $key != "$scope/"* || $key == *[$'\t\r']* ]]; then
      echo "cleanup: invalid object row under $target/$scope/" >&2
      failed=1
    elif ! retry s3m rm "$target/$key" >/dev/null 2>&1; then
      echo "cleanup: could not delete $target/$key" >&2
      failed=1
    fi
  done
  return "$failed"
}

cleanup() {
  local status=$? failed=0
  trap - EXIT
  stop_upload
  abort_recorded_uploads || failed=1
  if ! clean_scope "$prefix"; then
    failed=1
    # An unavailable listing must not prevent deleting known keys.
    sort -u "$created" | delete_rows "$prefix" || true
  fi
  rm -rf "$work" || failed=1
  if ((status == 0)); then
    ((failed == 0)) || exit 1
    echo "all R2 checks passed"
  fi
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

head -c 1048576 /dev/urandom >"$work/small.bin"
head -c 12582912 /dev/urandom >"$work/big.bin"
part=(--buffer 5242880)

echo "R2 checks under $target/$prefix/"

# Without --checksum, uploads send Content-MD5.
output=$(upload small-plain.bin "$work/small.bin") || fail "small-plain.bin: $output"
grep -q '^ETag: ' <<<"$output" || fail "small-plain.bin: no ETag in: $output"
echo "ok  small-plain.bin"
output=$(upload big-plain.bin "$work/big.bin" "${part[@]}") || fail "big-plain.bin: $output"
grep -q '^ETag: ".*-3"$' <<<"$output" || fail "big-plain.bin: no multipart ETag in: $output"
check_download big-plain.bin "$work/big.bin"

# Listing without a prefix: R2 leaves <Prefix> out of the response (#78).
listing=$(s3m ls "$target" --json 2>/dev/null) || fail "ls $target failed"
json_array objects <<<"$listing" |
  jq -e --arg k "$prefix/big-plain.bin" 'any(.[]; .key == $k)' >/dev/null ||
  fail "ls $target doesn't list $prefix/big-plain.bin"
echo "ok  ls without a prefix"

# Listing buckets: R2 answers 501 to max-buckets, and s3m asks again without it (#79).
# The output is not printed: the token may see other buckets. A token limited to the
# test bucket may not list buckets at all (403 AccessDenied): then this is skipped.
if buckets=$(s3m ls "$host" --json 2>"$work/buckets.err"); then
  json_array buckets <<<"$buckets" | jq -e --arg b "$bucket" 'any(.[]; .name == $b)' >/dev/null ||
    fail "ls $host doesn't list the test bucket (output not shown)"
  echo "ok  ls $host"
elif grep -qx 'Error: Code: AccessDenied' "$work/buckets.err" &&
     grep -qx 'HTTP Status Code: 403 Forbidden' "$work/buckets.err"; then
  echo "skip  ls $host: the token may not list buckets"
else
  fail "ls $host failed (output not shown)"
fi

# Single-part with --checksum: R2 rejects Content-MD5 next to x-amz-checksum-* (#73).
for alg in crc32 crc32c sha1 sha256; do
  check_checksum "small-$alg.bin" "$work/small.bin" "$alg" single
done

# Multipart with --checksum: R2 rejects a composite checksum header on
# CompleteMultipartUpload (#73); it derives the same value from the parts.
for alg in crc32 crc32c; do
  check_checksum "big-$alg.bin" "$work/big.bin" "$alg" multipart "${part[@]}"
done
check_download big-crc32c.bin "$work/big.bin"

# Multipart with a SHA checksum: R2 answers 501 NotImplemented to the parts (#80).
# s3m must stop at once, abort the upload and say so. If R2 starts accepting them,
# this fails: move sha1/sha256 to the multipart checks above.
if output=$(upload big-sha256.bin "$work/big.bin" --checksum sha256 "${part[@]}" 2>&1); then
  fail "big-sha256.bin: a multipart upload with --checksum sha256 succeeded: $output"
fi
grep -q "doesn't support sha256 checksums on the parts of a multipart upload, and the upload was aborted" <<<"$output" ||
  fail "big-sha256.bin: unexpected error: $output"
count=$(open_uploads big-sha256.bin) || fail "big-sha256.bin: could not list open uploads"
[[ $count == 0 ]] || fail "big-sha256.bin: $count multipart uploads still open"
check_absent big-sha256.bin
echo "ok  big-sha256.bin  rejected and aborted"

run_provider_scenarios
