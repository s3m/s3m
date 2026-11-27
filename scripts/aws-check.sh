#!/usr/bin/env bash
# Checks against a real AWS S3 bucket with Object Lock and versioning enabled.
#
#   scripts/aws-check.sh <s3m binary> <config.yml> <host> <bucket>
#
# Covers what neither R2 nor the RustFS integration tests can show: Object Lock
# uploads with --checksum (#73), the server's composite checksum after a multipart
# upload (#88), version listings longer than one page (#90), listings in which
# versions and delete markers alternate (#91), and the Version ID and Expiration that
# multipart and streamed uploads report (#75, #76; the bucket needs a lifecycle rule
# that expires current versions). Then the interrupted and changing uploads of
# scripts/provider-scenarios.sh.
#
# Everything is written under ci/<unique prefix>/ and removed again: legal holds are
# released, every version and delete marker is deleted (with
# x-amz-bypass-governance-retention), unfinished multipart uploads are aborted, and
# the run fails if anything is left under the prefix. Retention is GOVERNANCE only and
# lasts minutes. Use a dedicated bucket and a key limited to it; the IAM policy should
# deny COMPLIANCE mode and long retention, so a mistake here can't lock anything for
# long. Needs jq and xargs.
#
# The version paging check uploads ~1,000 objects of one byte (about $0.005 per run).
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
prefix="ci/$(date -u +%Y%m%dT%H%M%SZ)-${GITHUB_RUN_ID:-local}-$RANDOM"
jobs=8

work=$(mktemp -d)
# Upload state of this run only, so cleanup finds the uploads it left unfinished.
export S3M_HOME="$work/home"
# Keys uploaded with a legal hold, and "<key>\t<version id>" of every delete marker
# this run created (s3m lists versions, not delete markers).
held=()
markers="$work/markers"
: >"$markers"

s3m() { "$bin" --config "$config" "$@"; }
# Cleanup deletes every version under the prefix; nothing to record per key.
track() { :; }
# shellcheck source=scripts/provider-scenarios.sh
source "$(dirname "$(realpath "$0")")/provider-scenarios.sh"

# Only keys of this run, without characters that would break the tab-separated rows.
# shellcheck disable=SC2016 # $prefix is a jq variable
safe_key='type == "string" and startswith($prefix + "/") and (test("[\t\n\r]") | not)'
safe_id='type == "string" and length > 0 and (test("[\t\n\r]") | not)'

# version_rows <key or prefix>: "<key>\t<version id>" for every version under it.
version_rows() {
  local listing
  listing=$(s3m get --versions --json "$target/$1") || return 1
  jq -e --arg prefix "$prefix" "(.versions | type) == \"array\" and all(.versions[]; (.key | $safe_key) and (.version_id | $safe_id))" <<<"$listing" >/dev/null || {
    echo "unexpected version listing for $1" >&2
    return 1
  }
  jq -r '.versions[] | [.key, .version_id] | join("\t")' <<<"$listing"
}

# delete_rows: reads "<key>\t<version id>" lines and deletes each version, $jobs at once.
# Each delete is tried 3 times: under this load AWS fails an occasional request, and
# s3m retries uploads but not deletes.
delete_rows() {
  # shellcheck disable=SC2016 # expanded by the inner bash, one version per call
  awk -F'\t' -v t="$target/" 'NF == 2 { print t $1; print $2 }' |
    xargs -r -d '\n' -n 2 -P "$jobs" bash -c \
      'for try in 1 2 3; do
         err=$("$0" --config "$1" rm "$2" --version-id "$3" --bypass-governance 2>&1 >/dev/null) && exit 0
         sleep "$try"
       done
       echo "cleanup: could not delete $2 version $3: $(tr "\n" " " <<<"$err")" >&2
       exit 1' \
      "$bin" "$config"
}

# abort_listed <label>: reads "<key>\t<upload id>" lines and aborts each upload under
# this run's prefix.
abort_listed() {
  local label=$1 key upload_id status=0
  while IFS=$'\t' read -r key upload_id; do
    [[ -n "$key$upload_id" ]] || continue
    if [[ "$key" != "$prefix/"* || -z "$upload_id" ]]; then
      echo "cleanup: unexpected upload '$key' ($label), not aborted" >&2
      status=1
      continue
    fi
    s3m rm "$target/$key" --abort "$upload_id" >/dev/null 2>&1 || {
      echo "cleanup: could not abort the upload of $target/$key ($label)" >&2
      status=1
    }
  done
  return "$status"
}

valid_uploads='length == 1 and (.[0].uploads | type) == "array" and all(.[0].uploads[]; ([.key, .upload_id] | all(type == "string" and length > 0 and (test("[\t\n\r]") | not))))'
valid_streams='length == 1 and (.[0].streams | type) == "array" and all(.[0].streams[] | select(.etag == null and .upload_id != null); ([.destination, .upload_id] | all(type == "string" and length > 0 and (test("[\t\n\r]") | not))))'

cleanup() {
  local status=$? failed=0 key row rows streams uploads listed
  trap - EXIT
  # A version under a legal hold can't be deleted, and no lifecycle rule removes the
  # hold: release it first, on every version of the keys uploaded with one.
  for key in ${held[@]+"${held[@]}"}; do
    if rows=$(version_rows "$key"); then
      while IFS=$'\t' read -r row_key row; do
        [[ "$row_key" == "$key" ]] || continue
        s3m object-lock set "$target/$key" --legal-hold off --version-id "$row" >/dev/null 2>&1 || {
          echo "cleanup: could not release the legal hold on $target/$key version $row" >&2
          failed=1
        }
      done <<<"$rows"
    else
      echo "cleanup: could not list the versions of $target/$key" >&2
      failed=1
    fi
  done
  # Every version under the prefix, then the delete markers this run created.
  if rows=$(version_rows "$prefix/"); then
    delete_rows <<<"$rows" || failed=1
  else
    echo "cleanup: could not list the versions under $target/$prefix/" >&2
    failed=1
  fi
  delete_rows <"$markers" || failed=1
  # Unfinished multipart uploads: those in this run's state, then whatever the server
  # still has open under the prefix.
  if [[ -d "$S3M_HOME" ]]; then
    if streams=$(s3m streams --json ls 2>/dev/null) &&
      jq -es "$valid_streams" <<<"$streams" >/dev/null &&
      listed=$(jq -r --arg bucket "$bucket/" '.streams[] | select(.etag == null and .upload_id != null) | [(.destination | ltrimstr($bucket)), .upload_id] | join("\t")' <<<"$streams"); then
      abort_listed "recorded state" <<<"$listed" || failed=1
    else
      echo "cleanup: could not read the upload state in $S3M_HOME" >&2
      failed=1
    fi
  fi
  if uploads=$(s3m ls "$target" --multipart --prefix "$prefix/" --json 2>/dev/null) &&
    jq -es "$valid_uploads" <<<"$uploads" >/dev/null &&
    listed=$(jq -r '.uploads[] | [.key, .upload_id] | join("\t")' <<<"$uploads"); then
    abort_listed "still open on the server" <<<"$listed" || failed=1
  else
    echo "cleanup: could not list the unfinished uploads under $target/$prefix/" >&2
    failed=1
  fi
  # Nothing may be left. Delete markers whose id was never recorded don't show here;
  # the bucket's lifecycle rule for expired delete markers removes those.
  if rows=$(version_rows "$prefix/"); then
    if [[ -n "$rows" ]]; then
      echo "cleanup: $(wc -l <<<"$rows") versions left under $target/$prefix/" >&2
      failed=1
    fi
  else
    failed=1
  fi
  rm -rf "$work"
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

# upload <name> <file> [s3m options...]: prints s3m's output.
upload() {
  local name=$1 file=$2
  shift 2
  s3m --retries 1 "$@" "$file" "$target/$prefix/$name"
}

# stored_checksum <name> <algorithm>: the x-amz-checksum-<algorithm> AWS stored
stored_checksum() {
  s3m get -m "$target/$prefix/$1" | awk -v h="x-amz-checksum-$2:" 'tolower($1) == h { print $2 }'
}

# check_checksum <name> <file> <algorithm> [s3m options...]
check_checksum() {
  local name=$1 file=$2 alg=$3
  shift 3
  local output printed stored
  output=$(upload "$name" "$file" --checksum "$alg" "$@" 2>&1) || fail "$name: upload failed: $output"
  printed=$(awk -v l="$alg" '$1 == l && $2 == "(base64):" { print $3 }' <<<"$output")
  stored=$(stored_checksum "$name" "$alg")
  [[ -n "$printed" ]] || fail "$name: no '$alg (base64):' line in: $output"
  [[ "$printed" == "$stored" ]] || fail "$name: printed $printed, AWS stored ${stored:-nothing}"
  echo "ok  $name  $alg $printed"
}

# check_lock <name> <pattern>: `object-lock get` on the object prints a matching line
check_lock() {
  local output
  output=$(s3m object-lock get "$target/$prefix/$1" 2>&1) || fail "$1: object-lock get: $output"
  grep -Eqi "$2" <<<"$output" || fail "$1: no '$2' in: $output"
}

# check_download <name> <file>
check_download() {
  s3m get -q "$target/$prefix/$1" "$work/$1.download" >/dev/null
  cmp -s "$2" "$work/$1.download" || fail "$1: downloaded bytes differ"
  echo "ok  $1  download matches"
}

# mark_deleted <name>: deletes the current version (adds a delete marker) and records
# the marker for cleanup.
mark_deleted() {
  local output marker
  output=$(s3m rm "$target/$prefix/$1" 2>&1) || fail "$1: rm: $output"
  marker=$(sed -n 's/.*delete marker created (version \([^)]*\)).*/\1/p' <<<"$output")
  [[ -n "$marker" ]] || fail "$1: rm didn't create a delete marker: $output"
  printf '%s\t%s\n' "$prefix/$1" "$marker" >>"$markers"
}

head -c 1048576 /dev/urandom >"$work/small.bin"
head -c 12582912 /dev/urandom >"$work/big.bin"
printf x >"$work/tiny.bin"
part=(--buffer 5242880)
# GOVERNANCE retention long enough to outlast the run; cleanup bypasses it.
until=$(date -u -d '+15 minutes' +%Y-%m-%dT%H:%M:%SZ)
lock=(--object-lock-mode GOVERNANCE --retain-until "$until")

echo "AWS checks under $target/$prefix/"

# Plain uploads send Content-MD5.
output=$(upload small-plain.bin "$work/small.bin" 2>&1) || fail "small-plain.bin: $output"
grep -q '^ETag: ' <<<"$output" || fail "small-plain.bin: no ETag in: $output"
echo "ok  small-plain.bin"
output=$(upload big-plain.bin "$work/big.bin" "${part[@]}" 2>&1) || fail "big-plain.bin: $output"
grep -q '^ETag: ".*-3"$' <<<"$output" || fail "big-plain.bin: no multipart ETag in: $output"
check_download big-plain.bin "$work/big.bin"

# Multipart and streamed uploads report the headers of the final answer (#75, #76).
# check_report <name> <output>
check_report() {
  grep -q '^Version ID: .' <<<"$2" || fail "$1: no Version ID in: $2"
  grep -q '^Expiration: .*rule-id=' <<<"$2" || fail "$1: no Expiration in: $2"
  echo "ok  $1  Version ID and Expiration reported"
}
check_report big-plain.bin "$output"
output=$(s3m --retries 1 --pipe "$target/$prefix/pipe.bin" <"$work/small.bin" 2>&1) || fail "pipe.bin: $output"
check_report pipe.bin "$output"
output=$(upload compress.zst "$work/small.bin" --compress 2>&1) || fail "compress.zst: $output"
check_report compress.zst "$output"

# --checksum, single-part and multipart. AWS keeps a composite checksum of the parts
# and returns it when the upload completes; s3m compares it with its own (#88).
for alg in crc32 crc32c sha1 sha256; do
  check_checksum "small-$alg.bin" "$work/small.bin" "$alg"
  check_checksum "big-$alg.bin" "$work/big.bin" "$alg" "${part[@]}"
done
check_download big-sha256.bin "$work/big.bin"

# Object Lock. AWS requires an integrity checksum on uploads with retention or a legal
# hold: Content-MD5 without --checksum, x-amz-sdk-checksum-algorithm with it (#73).
output=$(upload lock-plain.bin "$work/small.bin" "${lock[@]}" 2>&1) || fail "lock-plain.bin: $output"
check_lock lock-plain.bin GOVERNANCE
echo "ok  lock-plain.bin  GOVERNANCE retention"
check_checksum lock-sha256.bin "$work/small.bin" sha256 "${lock[@]}"
check_lock lock-sha256.bin GOVERNANCE
check_checksum lock-big-crc32c.bin "$work/big.bin" crc32c "${lock[@]}" "${part[@]}"
check_lock lock-big-crc32c.bin GOVERNANCE
held+=("$prefix/hold-crc32.bin")
check_checksum hold-crc32.bin "$work/small.bin" crc32 --legal-hold
check_lock hold-crc32.bin 'legal hold.*on'

# Versions and delete markers alternating in key order (#91):
# Version(a), DeleteMarker(b), Version(b), DeleteMarker(c), Version(c).
for name in a b c; do
  output=$(upload "v/$name" "$work/tiny.bin" 2>&1) || fail "v/$name: $output"
done
mark_deleted v/b
mark_deleted v/c
rows=$(version_rows "$prefix/v/") || fail "get --versions $prefix/v/ failed"
[[ $(wc -l <<<"$rows") -eq 3 ]] || fail "get --versions $prefix/v/: expected 3 versions, got: $rows"
echo "ok  get --versions with interleaved delete markers"

# More versions than one page of ListObjectVersions (1,000) (#90).
seq -w 0 1000 | xargs -d '\n' -P "$jobs" -I{} \
  "$bin" --config "$config" --retries 1 -q "$work/tiny.bin" "$target/$prefix/page/{}" >/dev/null ||
  fail "uploading the objects for the paging check failed"
rows=$(version_rows "$prefix/page/") || fail "get --versions $prefix/page/ failed"
count=$(wc -l <<<"$rows")
[[ $count -eq 1001 ]] || fail "get --versions $prefix/page/: expected 1001 versions, got $count"
echo "ok  get --versions over two pages ($count versions)"

run_provider_scenarios

echo "all AWS checks passed"
