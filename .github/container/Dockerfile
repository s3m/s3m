# The s3m container image (ghcr.io/s3m/s3m). It packages a binary that was already built and
# released; nothing is compiled here. The build context holds `linux/amd64/s3m` and
# `linux/arm64/s3m`, the static musl binaries from the release tarballs, so the binary in the
# image has the same SHA-256 as the one listed in the release's SHA256SUMS.
#
# distroless/static brings the CA certificates s3m needs for TLS, a non-root user (65532) and
# a writable /tmp, and nothing else: no shell, no package manager. Refresh the digest with:
#   curl -sI -H "Accept: application/vnd.oci.image.index.v1+json" \
#     https://gcr.io/v2/distroless/static-debian13/manifests/nonroot | grep -i docker-content-digest
FROM gcr.io/distroless/static-debian13:nonroot@sha256:8ce6dd87e9823c5b48dcfb2c8604714ccb351ce59607327bcc4957448da279b5

ARG TARGETOS
ARG TARGETARCH

COPY --chmod=0755 ${TARGETOS}/${TARGETARCH}/s3m /s3m

# Config and upload state live under /tmp/s3m, not in a home directory. Pass the config with
# `--config` (for example a mounted Secret) or mount it at /tmp/s3m/config.yml. With a
# read-only root file system, mount an emptyDir at /tmp to keep upload state.
ENV S3M_HOME=/tmp/s3m

ENTRYPOINT ["/s3m"]
