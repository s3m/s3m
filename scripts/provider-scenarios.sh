# shellcheck shell=bash
# shellcheck disable=SC2154 # caller variables listed below
# Shared helpers and upload scenarios for aws-check.sh and r2-check.sh.
# The caller sets bin, config, host, bucket, target, prefix and work, and defines
# object_rows <key prefix> and delete_rows <run prefix> for its type of bucket.
# object_rows returns the key as the first tab-separated field.
# Each interruption attempt waits 15 seconds after observing the multipart upload.

s3m() { "$bin" --config "$config" "$@"; }

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

run_prefix() {
  local run_id=${GITHUB_RUN_ID:-local} stamp suffix
  local LC_ALL=C
  [[ $host =~ ^[A-Za-z0-9_][A-Za-z0-9_.-]*$ &&
     $bucket =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ &&
     $run_id =~ ^[A-Za-z0-9_-]+$ ]] || {
    echo "invalid host, bucket, or run ID" >&2
    return 2
  }
  stamp=$(date -u +%Y%m%dT%H%M%SZ) || return 1
  suffix=$(od -An -N4 -tx1 /dev/urandom | tr -d ' \n') || return 1
  [[ $suffix =~ ^[0-9a-f]{8}$ ]] || return 1
  printf 'ci/%s-%s-%s\n' "$stamp" "$run_id" "$suffix"
}

retry() {
  local delay
  for delay in 1 2; do
    "$@" && return 0
    sleep "$delay"
  done
  "$@"
}

# The entire response must be one JSON document. A jq stream can contain
# several documents even when jq -e succeeds.
json_array() {
  jq -ces --arg field "$1" '
    if length == 1 and (.[0][$field] | type) == "array" then .[0][$field]
    else error("expected one JSON object with a \($field) array") end'
}

safe_string='def safe_string: type == "string" and length > 0 and (test("[\u0000\t\r\n]") | not);'

upload_rows() {
  local scope=$1
  s3m ls "$target" --multipart --prefix "$scope/" --json |
    json_array uploads |
    jq -r --arg p "$scope/" "$safe_string"'
      if all(.[]; (.key | safe_string and startswith($p)) and (.upload_id | safe_string))
      then .[] | [.key, .upload_id] | join("\t")
      else error("invalid upload row") end'
}

# A completed or previously aborted upload can still appear in local state.
abort_upload() {
  local output
  output=$(s3m rm "$target/$1" --abort="$2" 2>&1 >/dev/null) && return 0
  grep -Eq '^(Error: )?Code: NoSuchUpload$' <<<"$output" &&
    grep -qx 'HTTP Status Code: 404 Not Found' <<<"$output"
}

abort_rows() {
  local scope=$1 row key id failed=0
  while IFS= read -r row; do
    [[ -n $row ]] || continue
    key=${row%%$'\t'*}
    id=${row#*$'\t'}
    if [[ $row != *$'\t'* || $key != "$scope/"* ||
          $key == *$'\r'* || -z $id || $id == *[$'\t\r']* ]]; then
      echo "cleanup: invalid upload row under $target/$scope/" >&2
      failed=1
    elif ! retry abort_upload "$key" "$id"; then
      echo "cleanup: could not abort the upload of $target/$key" >&2
      failed=1
    fi
  done
  return "$failed"
}

recorded_upload_rows() {
  s3m streams --json ls | json_array streams |
    jq -r --arg b "$bucket/" --arg p "$prefix/" "$safe_string"'
      [.[] | select(.etag == null and .upload_id != null)] |
      if all(.[]; (.destination | safe_string and startswith($b + $p)) and (.upload_id | safe_string))
      then .[] | [(.destination | ltrimstr($b)), .upload_id] | join("\t")
      else error("invalid recorded upload row") end'
}

# Local state can find an upload that a server listing missed. The server can
# also have uploads the client never recorded, or have closed a recorded one.
abort_recorded_uploads() {
  local uploads
  [[ -d $S3M_HOME ]] || return 0
  if uploads=$(retry recorded_upload_rows 2>/dev/null); then
    abort_rows "$prefix" <<<"$uploads"
  else
    echo "cleanup: could not read recorded uploads under $target/$prefix/" >&2
    return 1
  fi
}

clean_scope() {
  local scope=$1 pass objects uploads listing_failed
  for pass in 1 2 3 4; do
    listing_failed=0
    if ! objects=$(retry object_rows "$scope/" 2>/dev/null); then
      objects=
      listing_failed=1
      [[ $pass != 4 ]] || echo "cleanup: could not verify objects under $target/$scope/" >&2
    fi
    if ! uploads=$(retry upload_rows "$scope" 2>/dev/null); then
      uploads=
      listing_failed=1
      [[ $pass != 4 ]] || echo "cleanup: could not verify multipart uploads under $target/$scope/" >&2
    fi
    [[ -n $objects || -n $uploads || $listing_failed != 0 ]] || return 0
    if [[ $pass == 4 ]]; then
      [[ -z $objects ]] || echo "cleanup: objects remain under $target/$scope/" >&2
      [[ -z $uploads ]] || echo "cleanup: multipart uploads remain under $target/$scope/" >&2
      return 1
    fi
    # Clean what each successful listing found, then verify on the next pass.
    abort_rows "$scope" <<<"$uploads" || true
    if [[ -n $objects ]]; then
      delete_rows "$scope" <<<"$objects" || true
    fi
  done
}

# AWS inventories versions independently. R2 overrides this to retain exact keys
# it can still delete if listing the prefix fails.
track() { :; }

upload() {
  local name=$1 file=$2
  shift 2
  track "$name" || return 1
  s3m --retries 1 "$@" "$file" "$target/$prefix/$name"
}

stored_checksum() {
  s3m get -m "$target/$prefix/$1" | awk -v h="x-amz-checksum-$2:" 'tolower($1) == h { print $2 }'
}

# check_checksum <name> <file> <algorithm> <single|multipart> [s3m options...]
check_checksum() {
  local name=$1 file=$2 alg=$3 kind=$4 output printed stored
  shift 4
  output=$(upload "$name" "$file" --checksum "$alg" "$@") || fail "$name: upload failed"
  printed=$(awk -v l="$alg" '$1 == l && $2 == "(base64):" { print $3 }' <<<"$output")
  stored=$(stored_checksum "$name" "$alg") || {
    local status=$?
    echo "FAIL: $name: could not read stored $alg" >&2
    exit "$status"
  }
  [[ $printed =~ ^[A-Za-z0-9+/]+=*(-3)?$ ]] || fail "$name: missing or invalid $alg checksum"
  [[ $printed == "$stored" ]] || fail "$name: printed $printed, stored ${stored:-nothing}"
  if [[ $kind == multipart ]]; then
    { [[ $printed == *-3 ]] && grep -Eq '^ETag: "[^"]+-3"$' <<<"$output"; } ||
      fail "$name: expected a three-part ETag and composite $alg"
  else
    [[ $printed != *-3 ]] || fail "$name: expected a single-part $alg"
  fi
  echo "ok  $name  $alg $printed"
}

check_download() {
  s3m get -q "$target/$prefix/$1" "$work/$1.download" >/dev/null
  cmp -s "$2" "$work/$1.download" || fail "$1: downloaded bytes differ"
  echo "ok  $1  download matches"
}

check_absent() {
  local key="$prefix/$1" rows
  rows=$(object_rows "$key") || fail "$1: could not verify object absence"
  awk -F '\t' -v k="$key" '$1 == k { found = 1 } END { exit found }' <<<"$rows" ||
    fail "$1: an object was stored"
}

# Only these scenarios start background uploads. Stop and reap the child before
# either provider's EXIT trap inventories the bucket.
upload_pid=
stop_upload() {
  if [[ -n $upload_pid ]]; then
    kill -KILL "$upload_pid" 2>/dev/null || true
    wait "$upload_pid" 2>/dev/null || true
    upload_pid=
  fi
}

# The upload holds its local DB lock, so use the server to tell when it has
# started, and inspect the saved part counts after killing it.
wait_for_upload() {
  local name=$1 rows
  local deadline=$((SECONDS + 20))
  while ((SECONDS < deadline)) && kill -0 "$upload_pid" 2>/dev/null; do
    if rows=$(upload_rows "$prefix"); then
      if awk -F '\t' -v k="$prefix/$name" '$1 == k { n++ } END { exit (n != 1) }' <<<"$rows"; then
        kill -0 "$upload_pid" 2>/dev/null && return 0
      fi
    fi
    sleep 0.25
  done
  return 1
}

# interrupt <name variable> [s3m options...]
# At 512 KiB/s with one request at a time, the 5 MiB parts land around 10
# and 20 seconds. Killing 15 seconds after observation leaves room for a few
# seconds of listing lag. A timing miss gets one fresh key for the scenario.
interrupt() {
  local name_var=$1 attempt_name=${!1} attempt status elapsed killed entries result observed_state
  local observed_at timing
  shift
  for attempt in 1 2; do
    track "$attempt_name" || fail "$attempt_name: could not record upload key"
    "$bin" --config "$config" --buffer 5242880 -n 1 -k 512 "$@" "$work/big.bin" "$target/$prefix/$attempt_name" >/dev/null 2>&1 &
    upload_pid=$!
    elapsed=n/a
    if wait_for_upload "$attempt_name" 2>/dev/null; then
      observed_at=$SECONDS
      sleep 15
      elapsed="$((SECONDS - observed_at))s"
    fi
    killed=false
    kill -KILL "$upload_pid" 2>/dev/null && killed=true
    status=0
    wait "$upload_pid" 2>/dev/null || status=$?
    upload_pid=

    timing="exit_status=$status observation_to_kill_attempt=$elapsed"
    entries=$(stream_entries "$attempt_name" 2>/dev/null) ||
      fail "$attempt_name: could not read saved state ($timing state=unavailable)"
    # Normal completion clears the uploaded-parts tree after saving the ETag.
    result=$(jq -r --argjson exit_status "$status" --argjson killed "$killed" --arg elapsed "$elapsed" "$safe_string"'
      if length == 1 and (.[0].id | safe_string) and (.[0].upload_id | safe_string) then
        .[0] |
        if $exit_status == 0 and .status == "complete" and .etag != null and
           ((.parts_total == 0 and .parts_uploaded == 0) or (.parts_total == 3 and .parts_uploaded == 3)) then
          "upload ended before the kill"
        elif $killed and $exit_status == 137 and $elapsed != "n/a" and
             (.status == "active" or .status == "resumable" or .status == "stale") and
             .etag == null and .parts_total == 3 then
          if .parts_uploaded == 1 or .parts_uploaded == 2 then "partial"
          elif .parts_uploaded == 0 then "no parts uploaded"
          else "unexpected saved state" end
        else "unexpected upload exit or saved state" end
      else "expected one saved upload with IDs" end' <<<"$entries" 2>/dev/null) ||
      fail "$attempt_name: could not inspect saved state ($timing state=unavailable)"
    if [[ $result == partial ]]; then
      printf -v "$name_var" '%s' "$attempt_name"
      return 0
    fi

    observed_state=$(jq -c 'map({
      status: (if .status | IN("active", "resumable", "stale", "broken", "complete") then .status else null end),
      parts_total: (.parts_total | if type == "number" then . else null end),
      parts_uploaded: (.parts_uploaded | if type == "number" then . else null end),
      etag_present: (.etag != null)
    })' <<<"$entries" 2>/dev/null) || observed_state=unavailable
    if [[ $attempt == 1 && ( $result == 'no parts uploaded' || $result == 'upload ended before the kill' ) ]]; then
      printf '%s: %s, retrying as %s.retry (%s state=%s)\n' "$attempt_name" "$result" "$attempt_name" "$timing" "$observed_state" >&2
      attempt_name+=.retry
    else
      fail "$attempt_name: $result ($timing state=$observed_state)"
    fi
  done
}

stream_entries() {
  s3m streams --json ls | json_array streams |
    jq -c --arg d "$bucket/$prefix/$1" '[.[] | select(.destination == $d)]'
}

stream() {
  stream_entries "$1" | jq -er "$safe_string"'
    if length == 1 and (.[0].id | safe_string) and (.[0].upload_id | safe_string)
    then .[0] | '"$2"' else error("expected one saved upload with IDs") end'
}

open_uploads() {
  upload_rows "$prefix" |
    awk -F '\t' -v k="$prefix/$1" '$1 == k { n++ } END { print n+0 }'
}

scenario_resume_checksum() {
  local name=resume-crc32c.bin id output printed stored
  interrupt name --checksum crc32c
  id=$(stream "$name" .id)
  output=$(s3m streams resume "$id") || fail "$name: streams resume failed"
  printed=$(awk '$1 == "crc32c" && $2 == "(base64):" { print $3 }' <<<"$output")
  stored=$(stored_checksum "$name" crc32c) || fail "$name: could not read stored checksum"
  { [[ $printed =~ ^[A-Za-z0-9+/]+=*-3$ ]] && grep -Eq '^ETag: "[^"]+-3"$' <<<"$output"; } ||
    fail "$name: no three-part ETag and composite crc32c"
  [[ $printed == "$stored" ]] || fail "$name: printed $printed, stored ${stored:-nothing}"
  check_download "$name" "$work/big.bin"
  echo "ok  $name  streams resume kept crc32c ($printed)"
}

scenario_gone_upload() {
  local name=gone.bin upload_id output entries count
  interrupt name
  upload_id=$(stream "$name" .upload_id)
  printf '%s\t%s\n' "$prefix/$name" "$upload_id" | abort_rows "$prefix" ||
    fail "$name: could not abort the interrupted upload"
  count=$(open_uploads "$name") || fail "$name: could not verify the abort"
  [[ $count == 0 ]] || fail "$name: multipart upload is still open"
  if output=$(upload "$name" "$work/big.bin" --buffer 5242880 2>&1); then
    fail "$name: the upload to an aborted multipart upload succeeded"
  fi
  grep -q 'no longer exists on the server' <<<"$output" || fail "$name: unexpected error: $output"
  entries=$(stream_entries "$name") || fail "$name: could not verify that the state was forgotten"
  jq -e 'all(.[]; .upload_id == null and .parts_total == 0)' <<<"$entries" >/dev/null ||
    fail "$name: aborted upload ID or saved parts are still present"
  upload "$name" "$work/big.bin" --buffer 5242880 >/dev/null || fail "$name: the next run failed"
  check_download "$name" "$work/big.bin"
  echo "ok  $name  aborted upload reported, next run uploaded it"
}

scenario_single_part_rerun() {
  local name=rerun.bin output count
  interrupt name
  output=$(upload "$name" "$work/big.bin" --buffer 16777216) || fail "$name: single-part rerun failed"
  grep -Eq '^ETag: "[^"-]+"$' <<<"$output" || fail "$name: expected a single-part ETag"
  count=$(open_uploads "$name") || fail "$name: could not list open uploads"
  [[ $count == 0 ]] || fail "$name: $count multipart uploads left open after the single-part rerun"
  check_download "$name" "$work/big.bin"
  echo "ok  $name  single-part rerun aborted the multipart upload"
}

scenario_two_destinations() {
  local name_a=two-a.bin name_b=two-b.bin id_a id_b
  interrupt name_a
  interrupt name_b
  id_a=$(stream "$name_a" .id)
  id_b=$(stream "$name_b" .id)
  [[ $id_a != "$id_b" ]] || fail "$name_a and $name_b share the same saved state"
  s3m streams resume "$id_a" >/dev/null || fail "$name_a: streams resume failed"
  check_download "$name_a" "$work/big.bin"
  check_absent "$name_b"
  s3m streams resume "$id_b" >/dev/null || fail "$name_b: streams resume failed"
  check_download "$name_b" "$work/big.bin"
  echo "ok  $name_a $name_b  one file, two destinations, resumed separately"
}

scenario_file_changes() {
  local output status=0 count
  track changing.bin || fail "changing.bin: could not record upload key"
  cp "$work/big.bin" "$work/changing.bin"
  "$bin" --config "$config" --retries 1 --buffer 5242880 -n 1 -k 1024 "$work/changing.bin" "$target/$prefix/changing.bin" >"$work/changing.out" 2>&1 &
  upload_pid=$!
  wait_for_upload changing.bin || fail "changing.bin: could not observe one active multipart upload"
  printf x >>"$work/changing.bin"
  wait "$upload_pid" || status=$?
  upload_pid=
  output=$(<"$work/changing.out")
  [[ $status -ne 0 ]] || fail "changing.bin: the upload of a file that grew succeeded"
  grep -q 'the file changed during the upload' <<<"$output" || fail "changing.bin: unexpected error: $output"
  count=$(open_uploads changing.bin) || fail "changing.bin: could not list open uploads"
  [[ $count == 0 ]] || fail "changing.bin: $count multipart uploads left open"
  check_absent changing.bin
  echo "ok  changing.bin  a file that grew failed the upload, which was aborted"
}

# Keep the entire verbose log local. A redaction regression must not print the
# signing values while reporting the failure.
scenario_verbose_log() {
  local log="$work/verbose.log" redacted leaked
  track verbose.bin || fail "verbose.bin: could not record upload key"
  s3m -vv --retries 1 "$work/small.bin" "$target/$prefix/verbose.bin" >"$log" 2>&1 ||
    fail "verbose.bin: upload with -vv failed (log not shown, it may hold credentials)"
  redacted=$(grep -ci 'authorization.*<redacted>' "$log" || true)
  leaked=$(grep -Eci 'Signature=|Credential=|x-amz-security-token[^,]*[A-Za-z0-9/+]{20}' "$log" || true)
  [[ $redacted -gt 0 ]] || fail "verbose.bin: no redacted Authorization header in the -vv log"
  [[ $leaked -eq 0 ]] || fail "verbose.bin: $leaked lines of the -vv log show signing values"
  echo "ok  verbose.bin  -vv log hides the signature ($redacted redacted headers)"
}

run_provider_scenarios() {
  scenario_resume_checksum
  scenario_gone_upload
  scenario_single_part_rerun
  scenario_two_destinations
  scenario_file_changes
  scenario_verbose_log
}
