#!/usr/bin/env bash
# Run an s3m image the way it is used: as a non-root user, on a read-only root file system,
# with the config mounted read-only. Usage: smoke-test.sh <image> <expected version>
set -euo pipefail

image="$1"
version="$2"
engine="${CONTAINER_ENGINE:-docker}"

out="$("${engine}" run --rm "${image}" -V)"
echo "${out}"
if [[ "${out}" != "s3m ${version}" ]]; then
  echo "::error::${image} reports '${out}', expected 's3m ${version}'."
  exit 1
fi

config="$(mktemp -d)"
trap 'rm -rf "${config}"' EXIT
cat > "${config}/config.yml" <<'YAML'
---
hosts:
  r2:
    endpoint: https://example.r2.cloudflarestorage.com
    region: auto
    access_key: AKIAEXAMPLE
    secret_key: SECRETEXAMPLE
YAML
chmod -R a+rX "${config}"

# No writable path at all: commands that keep no local state must still run.
out="$("${engine}" run --rm --read-only -v "${config}:/config:ro" "${image}" \
  --config /config/config.yml show)"
echo "${out}"
if [[ "${out}" != *"- r2"* ]]; then
  echo "::error::${image} could not read its config on a read-only root file system."
  exit 1
fi

echo "${image}: ok"
