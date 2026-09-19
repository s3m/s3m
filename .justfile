# Run all tests (unit + e2e integration tests with RustFS)
test: clippy fmt test-unit test-integration

# Run only unit tests (both workspace crates: s3m + s3m-core)
test-unit:
  cargo test --workspace --lib

# Start RustFS container with Podman for integration tests
container:
  #!/usr/bin/env bash
  set -euo pipefail

  echo "🐳 Starting RustFS container with Podman..."
  RUSTFS_CONTAINER_NAME="s3m-test-rustfs"

  if podman ps --filter "name=${RUSTFS_CONTAINER_NAME}" --quiet | grep -q .; then
    echo "✅ RustFS container is already running"
    exit 0
  fi

  podman rm -f ${RUSTFS_CONTAINER_NAME} 2>/dev/null || true

  podman run -d \
    --name ${RUSTFS_CONTAINER_NAME} \
    -p 9000:9000 \
    -p 9001:9001 \
    -e RUSTFS_ACCESS_KEY=minioadmin \
    -e RUSTFS_SECRET_KEY=minioadmin \
    ghcr.io/rustfs/rustfs:1.0.0

  echo "Waiting for RustFS to be ready..."
  timeout 30 bash -c 'until curl -sf http://localhost:9000/minio/health/live &>/dev/null; do sleep 1; done' || {
    echo "❌ RustFS failed to become healthy"
    podman logs ${RUSTFS_CONTAINER_NAME}
    exit 1
  }

  echo "✅ RustFS is ready at http://localhost:9000"

# Debug: check environment and RustFS connectivity
test-debug:
  #!/usr/bin/env bash
  set -euo pipefail

  echo "🔍 Checking RustFS setup..."

  # Check if RustFS is accessible
  if curl -sf http://localhost:9000/minio/health/live &>/dev/null; then
    echo "✅ RustFS is accessible at http://localhost:9000"
  else
    echo "❌ RustFS is NOT accessible at http://localhost:9000"
    exit 1
  fi

  # Set and verify environment variables
  export MINIO_ENDPOINT=http://localhost:9000
  export MINIO_ACCESS_KEY=minioadmin
  export MINIO_SECRET_KEY=minioadmin

  echo "✅ Environment variables:"
  echo "   MINIO_ENDPOINT=${MINIO_ENDPOINT}"
  echo "   MINIO_ACCESS_KEY=${MINIO_ACCESS_KEY}"
  echo "   MINIO_SECRET_KEY=${MINIO_SECRET_KEY}"

  echo ""
  echo "🧪 Running single test with debug output..."
  cargo test --test e2e_binary test_binary_version -- --nocapture

# Run e2e integration tests with RustFS (Podman)
test-integration: container
  #!/usr/bin/env bash
  set -euo pipefail

  # Retain the existing MINIO_* test-harness contract for the S3 endpoint.
  export MINIO_ENDPOINT=${MINIO_ENDPOINT:-http://localhost:9000}
  export MINIO_ACCESS_KEY=${MINIO_ACCESS_KEY:-minioadmin}
  export MINIO_SECRET_KEY=${MINIO_SECRET_KEY:-minioadmin}

  echo "🧪 Running e2e tests with RustFS..."
  cargo test --tests -- --test-threads=1

clippy:
  cargo clippy --workspace --all-targets --all-features

fmt:
  cargo fmt --all -- --check

coverage:
  CARGO_INCREMENTAL=0 RUSTFLAGS='-Cinstrument-coverage' LLVM_PROFILE_FILE='coverage-%p-%m.profraw' cargo test --workspace
  grcov . --binary-path ./target/debug/deps/ -s . -t html --branch --ignore-not-existing --ignore '../*' --ignore "/*" -o target/coverage/html
  firefox target/coverage/html/index.html
  rm -rf *.profraw
