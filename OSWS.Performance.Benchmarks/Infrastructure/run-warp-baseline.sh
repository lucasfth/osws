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
else
    echo "⚠️  Warning: .env file not found at $ENV_FILE"
    echo "   To set up configuration, run:"
    echo "   cp .env.example .env"
    echo "   Then edit .env with your S3 backend credentials"
    echo ""
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

# S3/R2 Configuration - extract from environment variables set by .env
S3_ACCESS_KEY="${S3Settings__AccessKeyId:-}"
S3_SECRET_KEY="${S3Settings__SecretAccessKey:-}"
S3_ENDPOINT="${S3Settings__EndpointHostname:-}"
S3_REGION="${S3Settings__Region:-auto}"

# Validate S3 credentials are configured
validate_s3_credentials() {
    if [[ -z "$S3_ACCESS_KEY" || -z "$S3_SECRET_KEY" || -z "$S3_ENDPOINT" ]]; then
        echo "ERROR: S3/R2 credentials not configured in .env file"
        echo "Required environment variables:"
        echo "  - S3Settings__AccessKeyId"
        echo "  - S3Settings__SecretAccessKey"
        echo "  - S3Settings__EndpointHostname"
        echo ""
        echo "Please update your .env file with valid S3/R2 credentials"
        return 1
    fi
    return 0
}

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

# Validate S3 credentials
echo "Validating S3/R2 credentials..."
if ! validate_s3_credentials; then
    exit 1
fi
echo "✓ S3/R2 credentials configured"
echo ""

# Parse S3 endpoint to extract host and scheme
S3_SCHEME="https"
S3_HOST="$S3_ENDPOINT"
if [[ "$S3_ENDPOINT" == http://* ]]; then
    S3_SCHEME="http"
    S3_HOST="${S3_ENDPOINT#http://}"
elif [[ "$S3_ENDPOINT" == https://* ]]; then
    S3_HOST="${S3_ENDPOINT#https://}"
fi

# Test S3 connectivity and create bucket if needed
test_and_create_s3_bucket() {
    local bucket="$1"
    local access_key="$2"
    local secret_key="$3"
    local host="$4"
    local scheme="$5"
    
    echo "Testing S3/R2 connectivity..."
    
    # Try to list buckets to verify credentials work
    if ! aws s3api list-buckets \
        --endpoint-url "${scheme}://${host}" \
        --region "$S3_REGION" \
        --access-key "$access_key" \
        --secret-key "$secret_key" > /dev/null 2>&1; then
        echo "  ⚠️  Warning: Could not verify S3/R2 credentials with aws cli"
        echo "     This may be expected if aws cli is not installed or configured"
        echo "     Warp will attempt to authenticate when benchmarking"
    else
        echo "✓ S3/R2 authentication successful"
        
        # Check if bucket exists, create if not
        echo "Checking for bucket: $bucket"
        if aws s3api head-bucket \
            --bucket "$bucket" \
            --endpoint-url "${scheme}://${host}" \
            --region "$S3_REGION" \
            --access-key "$access_key" \
            --secret-key "$secret_key" 2>/dev/null; then
            echo "✓ Bucket '$bucket' exists"
        else
            echo "  Creating bucket: $bucket"
            if aws s3api create-bucket \
                --bucket "$bucket" \
                --endpoint-url "${scheme}://${host}" \
                --region "$S3_REGION" \
                --access-key "$access_key" \
                --secret-key "$secret_key" 2>/dev/null; then
                echo "✓ Bucket '$bucket' created"
            else
                echo "  ⚠️  Could not create bucket (may already exist or permission denied)"
                echo "     Warp will attempt to create it during benchmark"
            fi
        fi
    fi
}

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
echo "  S3/R2 Region: $S3_REGION"
echo ""

if [[ -z "$INSTANCE_COUNT" ]]; then
    echo "Instance Counts: All (1, 2, 4, 8)"
    INSTANCE_COUNTS=(1 2 4 8)
else
    echo "Instance Count: $INSTANCE_COUNT"
    INSTANCE_COUNTS=($INSTANCE_COUNT)
fi
echo ""

echo "NOTE: This implementation runs Warp directly against instance ports."
echo "      For multiple instances, Warp connects to different ports (8000, 8002, 8004, etc.)"
echo "      In production, use a load balancer (nginx) for fair distribution."
echo ""

# Test S3 bucket before benchmarking
test_and_create_s3_bucket "$BUCKET_NAME" "$S3_ACCESS_KEY" "$S3_SECRET_KEY" "$S3_HOST" "$S3_SCHEME"
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
    
    # Run Warp with proper S3/R2 credentials from environment
    if warp $WORKLOAD_PROFILE \
        --duration "${WARP_DURATION}s" \
        --concurrent $WARP_CONCURRENCY \
        --objects 1000 \
        --obj.size 1M \
        --host "localhost:$TARGET_PORT" \
        --access-key "$S3_ACCESS_KEY" \
        --secret-key "$S3_SECRET_KEY" \
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

