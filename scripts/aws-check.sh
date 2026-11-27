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
# Cleanup releases legal holds, deletes versions and recorded delete markers, and
# aborts unfinished multipart uploads. Startup also cleans run prefixes older than
# one hour. The CLI does not list delete markers: if a marker creation response was
# lost, its ID is unknown. An optional expired-delete-marker lifecycle rule removes
# eligible orphan markers. Use a dedicated bucket and a key that denies COMPLIANCE
# mode and long retention. Needs GNU date, jq and xargs.
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
# shellcheck source=scripts/provider-scenarios.sh
source "$(dirname "$(realpath "$0")")/provider-scenarios.sh"
prefix=$(run_prefix)
jobs=8

work=$(mktemp -d)
export S3M_HOME="$work/home"
markers="$work/markers"
: >"$markers"

version_listing() {
  s3m get --versions --json "$target/$1" | json_array versions |
    jq -ce --arg p "$1" "$safe_string"'
      if all(.[]; (.key | safe_string and startswith($p)) and (.version_id | safe_string))
      then . else error("invalid version row") end'
}

object_rows() {
  version_listing "$1" | jq -r '.[] | [.key, .version_id] | join("\t")'
}

# One tab-separated row per worker. Check the exact scope before sending either
# request, including when the rows came from the local delete marker record.
delete_rows() (
  export -f retry
  # shellcheck disable=SC2016 # expanded by the worker Bash
  xargs -r -d '\n' -n 1 -P "$jobs" bash -c '
    bin=$1 config=$2 target=$3 scope=$4
    tab=$5 cr=$6
    key=${7%%"$tab"*}
    id=${7#*"$tab"}
    if [[ $7 != *"$tab"* || $key != "$scope/"* ||
          $key == *"$cr"* || -z $id || $id == *["$tab$cr"]* ]]; then
      echo "cleanup: invalid version row under $target/$scope/" >&2
      exit 1
    fi
    # All keys uploaded with a legal hold start with hold- at the run root.
    if [[ ${key#"$scope/"} == hold-* ]] &&
       ! retry "$bin" --config "$config" object-lock set "$target/$key" --legal-hold off --version-id="$id" >/dev/null 2>&1; then
      echo "cleanup: could not release the legal hold on $target/$key version $id" >&2
      exit 1
    fi
    if ! retry "$bin" --config "$config" rm "$target/$key" --version-id="$id" --bypass-governance >/dev/null 2>&1; then
      echo "cleanup: could not delete $target/$key version $id" >&2
      exit 1
    fi
  ' bash "$bin" "$config" "$target" "$1" $'\t' $'\r'
)

sweep_old_runs() {
  local now versions uploads scopes scope stamp failed=0
  now=$(date -u +%s) || return 1
  if ! versions=$(retry object_rows ci/ 2>/dev/null); then
    echo "cleanup: could not discover old versions under $target/ci/" >&2
    versions=
    failed=1
  fi
  if ! uploads=$(retry upload_rows ci 2>/dev/null); then
    echo "cleanup: could not discover old multipart uploads under $target/ci/" >&2
    uploads=
    failed=1
  fi
  scopes=$(printf '%s\n' "$versions" "$uploads" | awk -F '[\t/]' 'NF >= 4 { print $1 "/" $2 }' | sort -u) ||
    return 1
  while IFS= read -r scope; do
    [[ $scope =~ ^ci/([0-9]{4})([0-9]{2})([0-9]{2})T([0-9]{2})([0-9]{2})([0-9]{2})Z-[A-Za-z0-9_-]+$ ]] ||
      continue
    stamp=$(date -u -d "${BASH_REMATCH[1]}-${BASH_REMATCH[2]}-${BASH_REMATCH[3]} ${BASH_REMATCH[4]}:${BASH_REMATCH[5]}:${BASH_REMATCH[6]}Z" +%s 2>/dev/null) ||
      continue
    ((stamp < now - 3600)) || continue
    echo "cleanup: removing leftovers under $target/$scope/"
    clean_scope "$scope" || failed=1
  done <<<"$scopes"
  return "$failed"
}

cleanup() {
  local status=$? failed=0
  trap - EXIT
  stop_upload
  abort_recorded_uploads || failed=1
  clean_scope "$prefix" || failed=1
  if [[ -s $markers ]]; then
    delete_rows "$prefix" <"$markers" || failed=1
  fi
  rm -rf "$work" || failed=1
  if ((status == 0)); then
    ((failed == 0)) || exit 1
    echo "all AWS checks passed"
  fi
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

sweep_old_runs || fail "could not sweep old AWS runs"

# Object Lock reports JSON on stdout. Compare retention as a timestamp because
# the server may format the same instant with a different UTC offset or precision.
check_lock() {
  local name=$1 expected=$2 output until actual want
  output=$(s3m object-lock get "$target/$prefix/$name" --json) || fail "$name: object-lock get failed"
  if [[ $expected == ON ]]; then
    jq -es 'length == 1 and .[0].legal_hold == "ON"' <<<"$output" >/dev/null ||
      fail "$name: expected legal hold ON"
  else
    until=$(jq -ers 'if length == 1 and .[0].retention.mode == "GOVERNANCE" then .[0].retention.retain_until_date | strings else empty end' <<<"$output") &&
      actual=$(date -u -d "$until" +%s) && want=$(date -u -d "$expected" +%s) &&
      [[ -n $until && $actual == "$want" ]] || fail "$name: expected GOVERNANCE retention until $expected"
  fi
}

release_hold() {
  local key="$prefix/$1" rows version_key id found=1
  rows=$(retry object_rows "$key") || return 1
  while IFS=$'\t' read -r version_key id; do
    [[ $version_key == "$key" ]] || continue
    retry s3m object-lock set "$target/$key" --legal-hold off --version-id="$id" >/dev/null || return 1
    found=0
  done <<<"$rows"
  return "$found"
}

# mark_deleted <name>: deletes the current version (adds a delete marker) and records
# the marker for cleanup.
mark_deleted() {
  local output marker
  output=$(s3m rm "$target/$prefix/$1") || fail "$1: rm failed; marker ID is unknown"
  marker=$(sed -n 's/.*delete marker created (version \([^)]*\)).*/\1/p' <<<"$output")
  [[ -n $marker && $marker != *[$'\t\n\r']* ]] || fail "$1: rm did not report exactly one marker ID"
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
output=$(upload big-plain.bin "$work/big.bin" "${part[@]}") || fail "big-plain.bin: $output"
grep -q '^ETag: ".*-3"$' <<<"$output" || fail "big-plain.bin: no multipart ETag in: $output"
check_download big-plain.bin "$work/big.bin"

# Multipart and streamed uploads report the headers of the final answer (#75, #76).
# check_report <name> <output>
check_report() {
  local name=$1 output=$2 version expiration metadata
  local expiration_pattern='^expiry-date="[^"]+",[[:space:]]*rule-id="[^"]+"$'
  version=$(sed -n 's/^Version ID: //p' <<<"$output")
  expiration=$(sed -n 's/^Expiration: //p' <<<"$output")
  [[ -n $version && $version != *$'\n'* &&
     $expiration =~ $expiration_pattern ]] ||
    fail "$name: expected Version ID and lifecycle Expiration on stdout"
  metadata=$(s3m get -m "$target/$prefix/$name" --json) || fail "$name: could not read report metadata"
  jq -es --arg v "$version" --arg e "$expiration" '
    length == 1 and .[0].headers["x-amz-version-id"] == $v and .[0].headers["x-amz-expiration"] == $e
  ' <<<"$metadata" >/dev/null || fail "$name: Version ID or Expiration differs from stored metadata"
  echo "ok  $name  Version ID and Expiration reported"
}
check_report big-plain.bin "$output"
output=$(s3m --retries 1 --pipe "$target/$prefix/pipe.bin" <"$work/small.bin") || fail "pipe.bin: upload failed"
check_report pipe.bin "$output"
output=$(upload compress.zst "$work/small.bin" --compress) || fail "compress.zst: upload failed"
check_report compress.zst "$output"

# --checksum, single-part and multipart. AWS keeps a composite checksum of the parts
# and returns it when the upload completes; s3m compares it with its own (#88).
for alg in crc32 crc32c sha1 sha256; do
  check_checksum "small-$alg.bin" "$work/small.bin" "$alg" single
  check_checksum "big-$alg.bin" "$work/big.bin" "$alg" multipart "${part[@]}"
done
check_download big-sha256.bin "$work/big.bin"

# Object Lock. AWS requires an integrity checksum on uploads with retention or a legal
# hold: Content-MD5 without --checksum, x-amz-sdk-checksum-algorithm with it (#73).
output=$(upload lock-plain.bin "$work/small.bin" "${lock[@]}" 2>&1) || fail "lock-plain.bin: $output"
check_lock lock-plain.bin "$until"
echo "ok  lock-plain.bin  GOVERNANCE retention"
check_checksum lock-sha256.bin "$work/small.bin" sha256 single "${lock[@]}"
check_lock lock-sha256.bin "$until"
check_checksum lock-big-crc32c.bin "$work/big.bin" crc32c multipart "${lock[@]}" "${part[@]}"
check_lock lock-big-crc32c.bin "$until"
check_checksum hold-crc32.bin "$work/small.bin" crc32 single --legal-hold
check_lock hold-crc32.bin ON
release_hold hold-crc32.bin || fail "hold-crc32.bin: immediate legal hold release failed"

# Versions and delete markers alternating in key order (#91):
# Version(a), DeleteMarker(b), Version(b), DeleteMarker(c), Version(c).
for name in a b c; do
  output=$(retry upload "v/$name" "$work/tiny.bin" 2>&1) ||
    fail "v/$name: setup upload failed after 3 attempts: $output"
done
mark_deleted v/b
mark_deleted v/c
listing=$(version_listing "$prefix/v/") || fail "get --versions $prefix/v/ failed"
jq -e --arg p "$prefix/v/" '
  map({key,is_latest}) | sort_by(.key) ==
    [{key:($p+"a"),is_latest:true},{key:($p+"b"),is_latest:false},{key:($p+"c"),is_latest:false}]
' <<<"$listing" >/dev/null || fail "get --versions $prefix/v/: expected one version of each key, with only a current"
echo "ok  get --versions with interleaved delete markers"

# More versions than one page of ListObjectVersions (1,000) (#90).
# s3m's --retries does not cover the single-part PUTs used for this fixture.
(
  export -f retry
  # shellcheck disable=SC2016 # expanded by the worker Bash
  seq -w 0 1000 | xargs -r -d '\n' -P "$jobs" -n 1 bash -c '
    key=$4/$5
    if ! errors=$(retry "$1" --config "$2" --retries 1 -q "$3" "$key" 2>&1 >/dev/null); then
      printf "FAIL: %s: setup upload failed after 3 attempts\n%s\n" "$key" "$errors" >&2
      exit 1
    fi
  ' bash "$bin" "$config" "$work/tiny.bin" "$target/$prefix/page"
) || fail "uploading the objects for the paging check failed"
listing=$(version_listing "$prefix/page/") || fail "get --versions $prefix/page/ failed"
actual=$(jq -r '.[].key' <<<"$listing" | LC_ALL=C sort)
expected=$(seq -w 0 1000 | sed "s|^|$prefix/page/|")
[[ $actual == "$expected" ]] || fail "get --versions $prefix/page/: expected exactly the 1,001 paging keys"
echo "ok  get --versions over two pages (1001 versions)"

run_provider_scenarios
