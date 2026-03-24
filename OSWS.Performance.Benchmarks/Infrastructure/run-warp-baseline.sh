#!/bin/bash

# Run OSWS Warp Baseline Benchmarks
# 
# This script orchestrates the full Warp benchmark suite:
# 1. Validates Warp is installed
# 2. Starts OSWS instances (encrypted and non-encrypted modes)
# 3. Runs Warp benchmarks against each instance configuration
# 4. Collects and stores results
# 5. Stops instances
#
# Usage: ./run-warp-baseline.sh [instance-count] [concurrency] [duration-seconds]
#
# Configuration: Copy .env.example to .env and configure your S3 backend
#
# Examples:
#   ./run-warp-baseline.sh         # Run with default settings (1, 2, 4, 8 instances)
#   ./run-warp-baseline.sh 1       # Run only with 1 instance
#   ./run-warp-baseline.sh 4 32 90 # Run with 4 instances, 32 concurrent clients, 90 second duration

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCHMARK_DIR="$(dirname "$SCRIPT_DIR")"
REPO_ROOT="$(dirname "$BENCHMARK_DIR")"

# Load environment from .env file
ENV_FILE="$BENCHMARK_DIR/.env"
if [[ -f "$ENV_FILE" ]]; then
    export $(cat "$ENV_FILE" | grep -v '^#' | xargs)
    ENV_LOADED=true
else
    echo "⚠️  Warning: .env file not found at $ENV_FILE"
    echo "   To set up configuration, run:"
    echo "   cp .env.example .env"
    echo "   Then edit .env with your S3 backend credentials"
    echo ""
    ENV_LOADED=false
fi

# Configuration (can be overridden from command line, .env, or appsettings.json)
INSTANCE_COUNT="${1:-}"
WARP_CONCURRENCY="${2:-${WARP_CONCURRENCY:-16}}"
WARP_DURATION="${3:-${WARP_DURATION_SECONDS:-60}}"
WORKLOAD_PROFILE="${4:-${WARP_WORKLOAD_PROFILE:-mixed}}"

# Constants
OSWS_BASE_PORT="${OSWS_BASE_PORT:-8000}"
RESULTS_DIR="./warp-results"
BUCKET_NAME="warp-benchmark-test"

echo "╔════════════════════════════════════════════════════════╗"
echo "║   OSWS Warp Baseline Benchmark Suite                  ║"
echo "╚════════════════════════════════════════════════════════╝"
echo ""

# Check if Warp is installed
echo "Checking if Warp is installed..."
if ! command -v warp &> /dev/null; then
    echo "ERROR: Warp not found in PATH"
    echo "Please install Warp from: https://github.com/minio/warp"
    echo ""
    echo "macOS:  brew install minio/stable/warp"
    echo "Linux:  See https://github.com/minio/warp"
    exit 1
fi

WARP_VERSION=$(warp --version 2>/dev/null || echo "unknown")
echo "✓ Warp found: $WARP_VERSION"
echo ""

# Validate .env configuration
echo "Validating configuration..."
if [[ "$ENV_LOADED" != "true" ]]; then
    echo "ERROR: .env file not found or could not be loaded"
    echo "Please ensure .env file exists in: $BENCHMARK_DIR"
    exit 1
fi
S3_ACCESS_KEY="${S3Settings__AccessKeyId:-}"
S3_SECRET_KEY="${S3Settings__SecretAccessKey:-}"
S3_ENDPOINT="${S3Settings__EndpointHostname:-}"
if [[ -z "$S3_ACCESS_KEY" || -z "$S3_SECRET_KEY" || -z "$S3_ENDPOINT" ]]; then
    echo "ERROR: S3/R2 credentials not configured in .env file"
    echo "Required environment variables:"
    echo "  - S3Settings__AccessKeyId"
    echo "  - S3Settings__SecretAccessKey"
    echo "  - S3Settings__EndpointHostname"
    echo ""
    echo "Please update your .env file with valid S3/R2 credentials"
    exit 1
fi
echo "✓ S3/R2 credentials configured"
echo ""

echo "✓ Configuration validated"
echo ""

# Create results directory
mkdir -p "$RESULTS_DIR"
echo "Results will be saved to: $RESULTS_DIR"
echo ""

echo "Configuration:"
echo "  OSWS Base Port: $OSWS_BASE_PORT"
echo "  Warp Concurrency: $WARP_CONCURRENCY clients"
echo "  Warp Duration: $WARP_DURATION seconds"
echo "  Workload Profile: $WORKLOAD_PROFILE"
echo "  Results Directory: $RESULTS_DIR"
echo "  S3 Bucket: $BUCKET_NAME"
echo "  S3/R2 Endpoint: $S3_ENDPOINT"
echo ""

if [[ -z "$INSTANCE_COUNT" ]]; then
    echo "Instance Counts: All (1, 2, 4, 8)"
    INSTANCE_COUNTS=(1 2 4 8)
else
    echo "Instance Count: $INSTANCE_COUNT"
    INSTANCE_COUNTS=($INSTANCE_COUNT)
fi
echo ""

echo "NOTE: Warp connects to OSWS instances on localhost ports."
echo "      OSWS instances use S3/R2 credentials from .env for backend operations."
echo "      For multiple instances, connect to different ports (8000, 8002, 8004, etc.)"
echo ""

# Cleanup function to stop instances on exit
cleanup() {
    echo ""
    echo "════════════════════════════════════════════════════════"
    echo "Cleaning up..."
    bash "$SCRIPT_DIR/osws-stop.sh" all 2>/dev/null || true
}

trap cleanup EXIT

# Run benchmark for each instance count
for num_instances in "${INSTANCE_COUNTS[@]}"; do
    echo "════════════════════════════════════════════════════════"
    echo "Running benchmark with $num_instances instance(s)"
    echo "════════════════════════════════════════════════════════"
    echo ""

    echo "Step 1: Starting OSWS instances..."
    
    # Start encrypted instances
    for i in $(seq 1 $num_instances); do
        PORT=$((OSWS_BASE_PORT + (i-1)*2))
        echo "  Instance $i (encrypted) on port $PORT..."
        bash "$SCRIPT_DIR/osws-start.sh" "$i" "true" "$PORT" &
        PIDS[$i]=$!
    done
    
    echo "  Waiting for instances to start..."
    sleep 10
    
    # Verify instances are running
    echo "  Verifying instances..."
    all_healthy=true
    for i in $(seq 1 $num_instances); do
        PORT=$((OSWS_BASE_PORT + (i-1)*2))
        if curl -s "http://localhost:$PORT/health" > /dev/null 2>&1; then
            echo "    ✓ Instance $i (port $PORT) is healthy"
        else
            echo "    ✗ Instance $i (port $PORT) is NOT responding"
            all_healthy=false
        fi
    done
    
    if [[ "$all_healthy" != "true" ]]; then
        echo "ERROR: Some instances failed to start"
        echo "STOPPING: Will not continue to next instance count due to failure"
        exit 1
    fi
    
    echo ""
    echo "Step 2: Running Warp benchmarks..."
    
    # Run Warp benchmarks against first encrypted instance
    # For multiple instances, users should use load balancer
    TARGET_PORT=$OSWS_BASE_PORT
    TARGET_URL="http://localhost:$TARGET_PORT"
    
    echo "  Running Warp benchmark..."
    echo "  Target: $TARGET_URL"
    echo "  Instances: $num_instances, Concurrency: $WARP_CONCURRENCY, Duration: $WARP_DURATION seconds"
    echo "  S3 Bucket: $BUCKET_NAME"
    
    RESULT_FILE="$RESULTS_DIR/warp-${num_instances}instances-encrypted.json"
    
    # Run Warp benchmark against OSWS instance
    # Note: Warp uses generic credentials to connect to OSWS.
    # OSWS internally uses the real S3/R2 credentials from environment variables.
    if warp $WORKLOAD_PROFILE \
        --duration "${WARP_DURATION}s" \
        --concurrent $WARP_CONCURRENCY \
        --objects 1000 \
        --obj.size 1M \
        --host "localhost:$TARGET_PORT" \
        --access-key "minioadmin" \
        --secret-key "minioadmin" \
        --json > "$RESULT_FILE" 2>&1; then
        echo "  ✓ Warp benchmark completed"
        echo "    Results saved to: $RESULT_FILE"
    else
        echo "  ✗ Warp benchmark failed"
        echo "    Check $RESULT_FILE for details"
        echo "    ERROR: Stopping execution due to benchmark failure"
        exit 1
    fi
    
    echo ""
    echo "Step 3: Stopping OSWS instances..."
    bash "$SCRIPT_DIR/osws-stop.sh" all 2>/dev/null || true
    sleep 2
    echo "  Instances stopped"
    echo ""
done

echo ""
echo "════════════════════════════════════════════════════════"
echo "Benchmark Suite Complete"
echo "════════════════════════════════════════════════════════"
echo ""
echo "Results location: $RESULTS_DIR"
echo ""

# List results
if [[ -d "$RESULTS_DIR" && -n "$(ls -A "$RESULTS_DIR" 2>/dev/null)" ]]; then
    echo "Results files:"
    ls -lh "$RESULTS_DIR"/*.json 2>/dev/null || echo "  No JSON files found"
else
    echo "No results found in $RESULTS_DIR"
fi

echo ""
echo "Next steps:"
echo "  1. Review JSON results in $RESULTS_DIR"
echo "  2. Parse results and compare metrics"
echo "  3. For multiple instances, set up nginx load balancer"
echo "  4. Compare encrypted vs non-encrypted performance"
echo ""

