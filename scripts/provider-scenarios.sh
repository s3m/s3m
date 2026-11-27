# shellcheck shell=bash
# shellcheck disable=SC2154 # bin, config, target, bucket, prefix and work come from the caller
# Interrupted and changing uploads, run against a real provider by scripts/r2-check.sh
# and scripts/aws-check.sh (sourced, not run). The caller defines: bin, config, target
# (<host>/<bucket>), bucket, prefix, work (with big.bin of 12 MiB and small.bin), and the
# functions s3m, upload, track, fail, stored_checksum and check_download. Its cleanup
# deletes what these create and aborts the multipart uploads they leave open.
#
# An upload is interrupted by starting it with 5 MiB parts, one at a time, at 1 MiB/s,
# and killing it (SIGKILL, as a crash would) 7.5 s later: after its first part and
# before its second.

# interrupt <name> [s3m options...]: uploads big.bin to <name> and kills s3m after its
# first part; the saved state must show 1 or 2 of 3 parts uploaded.
interrupt() {
  local name=$1 pid
  shift
  track "$name"
  "$bin" --config "$config" --buffer 5242880 -n 1 -k 1024 "$@" "$work/big.bin" "$target/$prefix/$name" >/dev/null 2>&1 &
  pid=$!
  sleep 7.5
  kill -9 "$pid" 2>/dev/null || fail "$name: the upload ended before it could be interrupted"
  wait "$pid" 2>/dev/null || true
  stream "$name" '.parts_uploaded >= 1 and .parts_uploaded < .parts_total' >/dev/null ||
    fail "$name: the interrupted upload has no partial state: $(stream "$name" '{status, parts_uploaded, parts_total}' 2>&1)"
}

# stream <name> <jq filter>: applies the filter to the one saved upload state of <name>.
stream() {
  local listing
  listing=$(s3m streams --json ls) || return 1
  jq -er --arg d "$bucket/$prefix/$1" "[.streams[] | select(.destination == \$d)] | if length == 1 then .[0] | $2 else error(\"\(length) states for \(\$d)\") end" <<<"$listing"
}

# open_uploads <name>: the number of multipart uploads the server has open for <name>.
open_uploads() {
  s3m ls "$target" --multipart --prefix "$prefix/$1" --json |
    jq -e --arg k "$prefix/$1" '[.uploads[] | select(.key == $k)] | length'
}

# A resumed upload keeps the --checksum it was started with (#77).
scenario_resume_checksum() {
  local id output printed stored
  interrupt resume-crc32c.bin --checksum crc32c
  id=$(stream resume-crc32c.bin .id)
  output=$(s3m streams resume "$id" 2>&1) || fail "resume-crc32c.bin: streams resume: $output"
  printed=$(awk '$1 == "crc32c" && $2 == "(base64):" { print $3 }' <<<"$output")
  stored=$(stored_checksum resume-crc32c.bin crc32c)
  [[ "$printed" == *-3 ]] || fail "resume-crc32c.bin: no composite crc32c line in: $output"
  [[ "$printed" == "$stored" ]] || fail "resume-crc32c.bin: printed $printed, stored ${stored:-nothing}"
  check_download resume-crc32c.bin "$work/big.bin"
  echo "ok  resume-crc32c.bin  streams resume kept crc32c ($printed)"
}

# A multipart upload aborted on the server is reported and forgotten, and the next run
# starts a new one (#84).
scenario_gone_upload() {
  local upload_id output
  interrupt gone.bin
  upload_id=$(stream gone.bin .upload_id)
  s3m rm "$target/$prefix/gone.bin" --abort "$upload_id" >/dev/null 2>&1 ||
    fail "gone.bin: could not abort the interrupted upload"
  if output=$(upload gone.bin "$work/big.bin" --buffer 5242880 2>&1); then
    fail "gone.bin: the upload to an aborted multipart upload succeeded: $output"
  fi
  grep -q 'no longer exists on the server' <<<"$output" || fail "gone.bin: unexpected error: $output"
  output=$(upload gone.bin "$work/big.bin" --buffer 5242880 2>&1) || fail "gone.bin: the next run failed: $output"
  check_download gone.bin "$work/big.bin"
  echo "ok  gone.bin  aborted upload reported, next run uploaded it"
}

# A single-part rerun of an interrupted multipart upload aborts the multipart upload (#86).
scenario_single_part_rerun() {
  local output count
  interrupt rerun.bin
  output=$(upload rerun.bin "$work/big.bin" --buffer 16777216 2>&1) || fail "rerun.bin: single-part rerun: $output"
  count=$(open_uploads rerun.bin) || fail "rerun.bin: could not list open uploads"
  [[ $count -eq 0 ]] || fail "rerun.bin: $count multipart uploads left open after the single-part rerun"
  check_download rerun.bin "$work/big.bin"
  echo "ok  rerun.bin  single-part rerun aborted the multipart upload"
}

# The same file interrupted on its way to two keys keeps two states, and each resumes
# to the right object (#85).
scenario_two_destinations() {
  local id_a id_b output
  interrupt two-a.bin
  interrupt two-b.bin
  id_a=$(stream two-a.bin .id)
  id_b=$(stream two-b.bin .id)
  [[ "$id_a" != "$id_b" ]] || fail "two-a.bin and two-b.bin share the state $id_a"
  output=$(s3m streams resume "$id_a" 2>&1) || fail "two-a.bin: streams resume: $output"
  output=$(s3m streams resume "$id_b" 2>&1) || fail "two-b.bin: streams resume: $output"
  check_download two-a.bin "$work/big.bin"
  check_download two-b.bin "$work/big.bin"
  echo "ok  two-a.bin two-b.bin  one file, two destinations, resumed separately"
}

# A file that grows during the upload fails it, and the upload is aborted (#89).
scenario_file_changes() {
  local pid output status=0 count
  track changing.bin
  cp "$work/big.bin" "$work/changing.bin"
  "$bin" --config "$config" --retries 1 --buffer 5242880 -n 1 -k 1024 "$work/changing.bin" "$target/$prefix/changing.bin" >"$work/changing.out" 2>&1 &
  pid=$!
  sleep 2.5
  printf x >>"$work/changing.bin"
  wait "$pid" || status=$?
  output=$(<"$work/changing.out")
  [[ $status -ne 0 ]] || fail "changing.bin: the upload of a file that grew succeeded: $output"
  grep -q 'the file changed during the upload' <<<"$output" || fail "changing.bin: unexpected error: $output"
  count=$(open_uploads changing.bin) || fail "changing.bin: could not list open uploads"
  [[ $count -eq 0 ]] || fail "changing.bin: $count multipart uploads left open"
  if s3m get -m "$target/$prefix/changing.bin" >/dev/null 2>&1; then
    fail "changing.bin: an object was stored"
  fi
  echo "ok  changing.bin  a file that grew failed the upload, which was aborted"
}

# -vv logs hide the signature and credentials (#87). The log stays in a file: only
# counts are printed, so a regression can't show a credential in CI output.
scenario_verbose_log() {
  local log="$work/verbose.log" redacted leaked
  track verbose.bin
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
