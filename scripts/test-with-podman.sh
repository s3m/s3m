#!/usr/bin/env bash
#
# Run MinIO integration tests with Podman
#
# This script sets up the necessary environment variables for Podman
# and runs the MinIO integration tests.
#
# Usage:
#   ./scripts/test-with-podman.sh [cargo test args]
#
# Examples:
#   ./scripts/test-with-podman.sh                    # Run all MinIO tests
#   ./scripts/test-with-podman.sh test_minio_container_lifecycle  # Run specific test
#   ./scripts/test-with-podman.sh -- --nocapture     # Run with output

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Print colored message
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if podman is installed
if ! command -v podman &> /dev/null; then
    print_error "Podman is not installed or not in PATH"
    echo ""
    echo "Install podman:"
    echo "  Fedora/RHEL: sudo dnf install podman"
    echo "  Ubuntu/Debian: sudo apt-get install podman"
    echo "  macOS: brew install podman"
    exit 1
fi

print_info "Podman found: $(podman --version)"

# Check if podman is running (or can be started)
if ! podman info &> /dev/null; then
    print_warn "Podman daemon is not running, attempting to start..."

    # Try to start podman machine on macOS
    if [[ "$OSTYPE" == "darwin"* ]]; then
        podman machine start || print_warn "Could not start podman machine automatically"
    fi

    # Verify again
    if ! podman info &> /dev/null; then
        print_error "Could not connect to Podman daemon"
        echo ""
        echo "On macOS, run: podman machine init && podman machine start"
        echo "On Linux, ensure podman service is running or use rootless mode"
        exit 1
    fi
fi

# Get user ID for rootless podman socket path
USER_ID=$(id -u)

# Detect OS-specific socket path
if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS uses podman machine
    PODMAN_SOCKET="$HOME/.local/share/containers/podman/machine/podman.sock"
    if [ ! -S "$PODMAN_SOCKET" ]; then
        # Try alternate location
        PODMAN_SOCKET="/var/run/docker.sock"
        if [ ! -S "$PODMAN_SOCKET" ]; then
            print_error "Could not find Podman socket"
            print_info "Is podman machine running? Try: podman machine start"
            exit 1
        fi
    fi
else
    # Linux rootless podman
    PODMAN_SOCKET="/run/user/${USER_ID}/podman/podman.sock"

    if [ ! -S "$PODMAN_SOCKET" ]; then
        print_warn "Podman socket not found at $PODMAN_SOCKET"
        print_info "Trying to start podman service..."

        # Try to start user podman service
        systemctl --user start podman.socket 2>/dev/null || true

        # Wait a bit for socket to appear
        sleep 1

        if [ ! -S "$PODMAN_SOCKET" ]; then
            print_error "Could not find or create Podman socket"
            echo ""
            echo "Try running: systemctl --user start podman.socket"
            echo "Or use: podman system service --time=0 unix://$PODMAN_SOCKET &"
            exit 1
        fi
    fi
fi

print_info "Using Podman socket: $PODMAN_SOCKET"

# Set environment variables for testcontainers to use Podman
export DOCKER_HOST="unix://${PODMAN_SOCKET}"
export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE="${PODMAN_SOCKET}"
export TESTCONTAINERS_RYUK_DISABLED=true  # Disable Ryuk container (optional)

print_info "Environment configured for Podman:"
print_info "  DOCKER_HOST=${DOCKER_HOST}"
print_info "  TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=${TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE}"

# Check if MinIO image is available, if not pull it
print_info "Checking for MinIO image..."
if ! podman image exists minio/minio:latest; then
    print_info "Pulling MinIO image (this may take a few minutes)..."
    podman pull minio/minio:latest
else
    print_info "MinIO image already available"
fi

# Check if MinIO container is already running
MINIO_CONTAINER_NAME="s3m-test-minio"
print_info "Checking for MinIO container..."

if ! podman ps --filter "name=${MINIO_CONTAINER_NAME}" --format "{{.Names}}" | grep -q "${MINIO_CONTAINER_NAME}"; then
    print_info "MinIO container not running, starting it..."

    # Remove old container if it exists
    podman rm -f ${MINIO_CONTAINER_NAME} 2>/dev/null || true

    # Start MinIO container
    podman run -d \
        --name ${MINIO_CONTAINER_NAME} \
        -p 9000:9000 \
        -p 9001:9001 \
        -e MINIO_ROOT_USER=minioadmin \
        -e MINIO_ROOT_PASSWORD=minioadmin \
        minio/minio server /data --console-address ":9001"

    if [ $? -ne 0 ]; then
        print_error "Failed to start MinIO container"
        exit 1
    fi

    print_info "Waiting for MinIO to be ready..."
    sleep 3

    # Wait for MinIO health endpoint (max 30 seconds)
    timeout 30 bash -c 'until curl -sf http://localhost:9000/minio/health/live &>/dev/null; do sleep 1; done' || {
        print_error "MinIO failed to become healthy"
        podman logs ${MINIO_CONTAINER_NAME}
        exit 1
    }

    print_info "✅ MinIO is ready"
else
    print_info "✅ MinIO container is already running"
fi

# Verify MinIO is accessible
if ! curl -sf http://localhost:9000/minio/health/live &>/dev/null; then
    print_error "MinIO is not accessible at http://localhost:9000"
    exit 1
fi

# Export MinIO credentials for e2e tests (using external MinIO)
export MINIO_ENDPOINT=http://localhost:9000
export MINIO_ACCESS_KEY=minioadmin
export MINIO_SECRET_KEY=minioadmin

print_info "Credentials configured for external MinIO:"
print_info "  MINIO_ENDPOINT=${MINIO_ENDPOINT}"
print_info "  MINIO_ACCESS_KEY=${MINIO_ACCESS_KEY}"
print_info "  MINIO_SECRET_KEY=${MINIO_SECRET_KEY}"
echo ""

# Run the tests
print_info "Running end-to-end binary tests..."
echo ""

# Build the test command - run e2e tests only
if [ $# -gt 0 ]; then
    # User provided specific test filter
    TEST_CMD="cargo test --test e2e_binary -- --ignored $*"
else
    # Run all ignored tests
    TEST_CMD="cargo test --test e2e_binary -- --ignored"
fi

print_info "Executing: $TEST_CMD"
echo ""

# Run the tests
if eval "$TEST_CMD"; then
    echo ""
    print_info "✓ All tests passed!"
    echo ""
    print_info "Test Summary:"
    print_info "  - End-to-end binary tests: 16 tests"
    echo ""
    print_info "MinIO container '${MINIO_CONTAINER_NAME}' is still running for faster subsequent test runs"
    print_info "To stop it: podman stop ${MINIO_CONTAINER_NAME}"
    print_info "To remove it: podman rm -f ${MINIO_CONTAINER_NAME}"
    exit 0
else
    echo ""
    print_error "✗ Tests failed"
    echo ""
    print_warn "MinIO logs:"
    podman logs --tail 20 ${MINIO_CONTAINER_NAME}
    echo ""
    print_info "To stop MinIO: podman stop ${MINIO_CONTAINER_NAME}"
    exit 1
fi
