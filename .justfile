# Run all tests (unit + e2e integration tests with MinIO)
test: clippy fmt test-unit test-integration

# Run only unit tests
test-unit:
  cargo test --lib

# Start MinIO container with Podman for integration tests
container:
  #!/usr/bin/env bash
  set -euo pipefail

  echo "🐳 Starting MinIO container with Podman..."
  MINIO_CONTAINER_NAME="s3m-test-minio"

  if podman ps --filter "name=${MINIO_CONTAINER_NAME}" --quiet | grep -q .; then
    echo "✅ MinIO container is already running"
    exit 0
  fi

  podman rm -f ${MINIO_CONTAINER_NAME} 2>/dev/null || true

  podman run -d \
    --name ${MINIO_CONTAINER_NAME} \
    -p 9000:9000 \
    -p 9001:9001 \
    -e MINIO_ROOT_USER=minioadmin \
    -e MINIO_ROOT_PASSWORD=minioadmin \
    minio/minio server /data --console-address ":9001"

  echo "Waiting for MinIO to be ready..."
  timeout 30 bash -c 'until curl -sf http://localhost:9000/minio/health/live &>/dev/null; do sleep 1; done' || {
    echo "❌ MinIO failed to become healthy"
    podman logs ${MINIO_CONTAINER_NAME}
    exit 1
  }

  echo "✅ MinIO is ready at http://localhost:9000"

# Debug: check environment and MinIO connectivity
test-debug:
  #!/usr/bin/env bash
  set -euo pipefail

  echo "🔍 Checking MinIO setup..."

  # Check if MinIO is accessible
  if curl -sf http://localhost:9000/minio/health/live &>/dev/null; then
    echo "✅ MinIO is accessible at http://localhost:9000"
  else
    echo "❌ MinIO is NOT accessible at http://localhost:9000"
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

# Run e2e integration tests with MinIO (Podman)
test-integration: container
  #!/usr/bin/env bash
  set -euo pipefail

  # Export MinIO credentials for e2e tests (using external MinIO)
  export MINIO_ENDPOINT=${MINIO_ENDPOINT:-http://localhost:9000}
  export MINIO_ACCESS_KEY=${MINIO_ACCESS_KEY:-minioadmin}
  export MINIO_SECRET_KEY=${MINIO_SECRET_KEY:-minioadmin}

  echo "🧪 Running e2e tests with MinIO..."
  cargo test --tests -- --test-threads=1

clippy:
  cargo clippy --all-targets --all-features

fmt:
  cargo fmt --all -- --check

coverage:
  CARGO_INCREMENTAL=0 RUSTFLAGS='-Cinstrument-coverage' LLVM_PROFILE_FILE='coverage-%p-%m.profraw' cargo test
  grcov . --binary-path ./target/debug/deps/ -s . -t html --branch --ignore-not-existing --ignore '../*' --ignore "/*" -o target/coverage/html
  firefox target/coverage/html/index.html
  rm -rf *.profraw
