#!/bin/bash

# Run OSWS Warp baseline benchmarks.
#
# Executes these baseline categories for each selected instance count:
# 1) S3/R2 direct
# 2) OSWS without encryption
# 3) OSWS with encryption (cache disabled)
# 4) OSWS with encryption (cache enabled)
#
# Usage: ./run-warp-baseline.sh [instance-count] [concurrency] [duration-seconds] [workload-profile]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCHMARK_DIR="$(dirname "$SCRIPT_DIR")"
REPO_ROOT="$(dirname "$BENCHMARK_DIR")"
WEBAPP_DIR="$REPO_ROOT/OSWS.WebApi"

# Load environment from .env file
ENV_FILE="$BENCHMARK_DIR/.env"
if [[ -f "$ENV_FILE" ]]; then
    set -a
    while IFS= read -r line || [[ -n "$line" ]]; do
        [[ "$line" =~ ^[[:space:]]*# ]] && continue
        [[ -z "${line// }" ]] && continue
        eval "export $line"
    done < "$ENV_FILE"
    set +a
    ENV_LOADED=true
else
    ENV_LOADED=false
fi

# Configuration (can be overridden from command line, .env, or appsettings.json)
INSTANCE_COUNT="${1:-}"
WARP_CONCURRENCY="${2:-${WARP_CONCURRENCY:-8}}"
WARP_DURATION="${3:-${WARP_DURATION_SECONDS:-60}}"
WORKLOAD_PROFILE="${4:-${WARP_WORKLOAD_PROFILE:-mixed}}"

# Constants
OSWS_BASE_PORT="${OSWS_BASE_PORT:-8000}"
RESULTS_DIR="$BENCHMARK_DIR/warp-results"
BUCKET_NAME="${WARP_BUCKET_NAME:-warp-benchmark-test}"
ENABLE_PARQUET_GET="${WARP_ENABLE_PARQUET_GET:-true}"
PARQUET_BUCKET_NAME="${WARP_PARQUET_BUCKET_NAME:-$BUCKET_NAME}"
PARQUET_PREFIX="${WARP_PARQUET_PREFIX:-parquet/}"
PARQUET_OBJECT_LIMIT="${WARP_PARQUET_OBJECT_LIMIT:-0}"
PARQUET_SEED_IF_EMPTY="${WARP_PARQUET_SEED_IF_EMPTY:-true}"
PARQUET_SAMPLE_DIR="${WARP_PARQUET_SAMPLE_DIR:-$REPO_ROOT/samples}"

echo "╔════════════════════════════════════════════════════════╗"
echo "║   OSWS Warp Baseline Benchmark Suite                   ║"
echo "╚════════════════════════════════════════════════════════╝"
echo ""

if ! command -v warp >/dev/null 2>&1; then
    echo "ERROR: Warp not found in PATH"
    echo "Install: brew install minio/stable/warp"
    exit 1
fi

WARP_VERSION=$(warp --version 2>/dev/null || echo "unknown")
echo "✓ Warp found: $WARP_VERSION"

if [[ "$ENV_LOADED" != "true" ]]; then
    echo "ERROR: .env file not found at $ENV_FILE"
    echo "Run: cp .env.example .env"
    exit 1
fi

S3_ACCESS_KEY="${S3Settings__AccessKeyId:-}"
S3_SECRET_KEY="${S3Settings__SecretAccessKey:-}"
S3_ENDPOINT="${S3Settings__EndpointHostname:-}"

if [[ -z "$S3_ACCESS_KEY" || -z "$S3_SECRET_KEY" || -z "$S3_ENDPOINT" ]]; then
    echo "ERROR: Missing required S3 settings in .env"
    echo "  - S3Settings__AccessKeyId"
    echo "  - S3Settings__SecretAccessKey"
    echo "  - S3Settings__EndpointHostname"
    exit 1
fi

if [[ -z "$INSTANCE_COUNT" ]]; then
    INSTANCE_COUNTS=(1 2 4 8)
else
    INSTANCE_COUNTS=($INSTANCE_COUNT)
fi

S3_HOST="$S3_ENDPOINT"
S3_HOST="${S3_HOST#http://}"
S3_HOST="${S3_HOST#https://}"
S3_HOST="${S3_HOST%/}"

S3_WARP_TLS_ARGS=()
if [[ "$S3_ENDPOINT" == https://* ]]; then
    S3_WARP_TLS_ARGS+=("--tls")
fi

if [[ "${WARP_INSECURE_TLS:-false}" == "true" ]]; then
    S3_WARP_TLS_ARGS+=("--insecure")
fi

mkdir -p "$RESULTS_DIR"

echo ""
echo "Configuration:"
echo "  Instance counts: ${INSTANCE_COUNTS[*]}"
echo "  Concurrency: $WARP_CONCURRENCY"
echo "  Duration: ${WARP_DURATION}s"
echo "  Workload profile: $WORKLOAD_PROFILE"
echo "  Bucket: $BUCKET_NAME"
echo "  Parquet GET enabled: $ENABLE_PARQUET_GET"
echo "  Parquet bucket: $PARQUET_BUCKET_NAME"
echo "  Parquet prefix: $PARQUET_PREFIX"
echo "  Parquet seed-if-empty: $PARQUET_SEED_IF_EMPTY"
echo "  Parquet sample dir: $PARQUET_SAMPLE_DIR"
echo "  S3 endpoint: $S3_ENDPOINT"
echo "  Results dir: $RESULTS_DIR"
echo ""

if [[ "$ENABLE_PARQUET_GET" == "true" ]]; then
    echo "Parquet mode note:"
    echo "  Parquet GET benchmarks use existing objects only (--list-existing)."
    echo "  Seed parquet objects beforehand under bucket '$PARQUET_BUCKET_NAME' and prefix '$PARQUET_PREFIX'."
    echo ""
fi

echo "Pre-building OSWS.WebApi (Release, isolated project build)..."
if dotnet build "$WEBAPP_DIR/OSWS.WebApi.csproj" -c Release -p:BuildProjectReferences=false -v minimal >/dev/null; then
    echo "  ✓ OSWS.WebApi build succeeded"
else
    echo "  ✗ OSWS.WebApi build failed"
    echo "    Fix build errors before running Warp against OSWS"
    exit 1
fi
echo ""

echo "Pre-building OSWS.Performance.Benchmarks (Release, isolated project build)..."
if dotnet build "$BENCHMARK_DIR/OSWS.Performance.Benchmarks.csproj" -c Release -p:BuildProjectReferences=false -v minimal >/dev/null; then
    echo "  ✓ OSWS.Performance.Benchmarks build succeeded"
else
    echo "  ✗ OSWS.Performance.Benchmarks build failed"
    exit 1
fi
echo ""

cleanup() {
    bash "$SCRIPT_DIR/osws-stop.sh" all >/dev/null 2>&1 || true
}

trap cleanup EXIT

run_warp() {
    local category=$1
    local instance_count=$2
    local host=$3
    local access_key=$4
    local secret_key=$5
    local -a tls_args=()
    local -a payload_args=()

    if [[ "$category" == "s3-direct" ]]; then
        tls_args=("${S3_WARP_TLS_ARGS[@]}")
    else
        # OSWS benchmark routes currently forward request bodies and do not decode
        # aws-chunked signed payload framing. Disable client-side sha256 payload
        # signing so uploaded object size remains identical for PUT/GET/STAT checks.
        payload_args=("--disable-sha256-payload")
    fi

    # Warp appends its own .json.zst suffix to --benchdata output.
    local result_base="$RESULTS_DIR/warp-${instance_count}instances-${category}"
    local result_file="${result_base}.json.zst"
    local result_json="${result_base}.json"

    echo "  Running: $category (instances=$instance_count, host=$host)"
    if warp "$WORKLOAD_PROFILE" \
        --duration "${WARP_DURATION}s" \
        --concurrent "$WARP_CONCURRENCY" \
        --objects 1000 \
        --obj.size 1M \
        --bucket "$BUCKET_NAME" \
        --host "$host" \
        --access-key "$access_key" \
        --secret-key "$secret_key" \
        --benchdata "$result_base" \
        "${tls_args[@]}" \
        "${payload_args[@]}" \
        --json; then
        # Primary expected output.
        if [[ -f "$result_file" ]]; then
            echo "    ✓ Saved: $result_file"
            write_plain_json_if_possible "$result_file" "$result_json"
        # Backward-compat: if caller passed extension before this fix, Warp may emit double suffix.
        elif [[ -f "${result_file}.json.zst" ]]; then
            echo "    ✓ Saved: ${result_file}.json.zst"
            mv "${result_file}.json.zst" "$result_file"
            echo "    ✓ Renamed to: $result_file"
            write_plain_json_if_possible "$result_file" "$result_json"
        else
            echo "    ✗ Warp completed but no benchdata file was created"
            echo "      Expected one of: $result_file or ${result_file}.json.zst"
            exit 1
        fi
    else
        echo "    ✗ Warp failed for $category ($instance_count instances)"
        echo "      Check: $result_file"
        exit 1
    fi
}

run_warp_parquet_get() {
    local category=$1
    local instance_count=$2
    local host=$3
    local access_key=$4
    local secret_key=$5
    local -a tls_args=()

    if [[ "$category" == "s3-direct" ]]; then
        tls_args=("${S3_WARP_TLS_ARGS[@]}")
    fi

    # Warp appends its own .json.zst suffix to --benchdata output.
    local result_base="$RESULTS_DIR/warp-${instance_count}instances-${category}-parquet-get"
    local result_file="${result_base}.json.zst"
    local result_json="${result_base}.json"

    echo "  Running: ${category} parquet-get (instances=$instance_count, host=$host, prefix=$PARQUET_PREFIX)"
    if warp get \
        --duration "${WARP_DURATION}s" \
        --concurrent "$WARP_CONCURRENCY" \
        --objects "$PARQUET_OBJECT_LIMIT" \
        --bucket "$PARQUET_BUCKET_NAME" \
        --host "$host" \
        --access-key "$access_key" \
        --secret-key "$secret_key" \
        --prefix "$PARQUET_PREFIX" \
        --list-existing \
        --noclear \
        --benchdata "$result_base" \
        "${tls_args[@]}" \
        --json; then
        if [[ -f "$result_file" ]]; then
            echo "    ✓ Saved: $result_file"
            write_plain_json_if_possible "$result_file" "$result_json"
        elif [[ -f "${result_file}.json.zst" ]]; then
            echo "    ✓ Saved: ${result_file}.json.zst"
            mv "${result_file}.json.zst" "$result_file"
            echo "    ✓ Renamed to: $result_file"
            write_plain_json_if_possible "$result_file" "$result_json"
        else
            echo "    ✗ Parquet GET completed but no benchdata file was created"
            echo "      Expected one of: $result_file or ${result_file}.json.zst"
            exit 1
        fi
    else
        echo "    ✗ Parquet GET failed for $category ($instance_count instances)"
        echo "      Ensure parquet objects exist under bucket '$PARQUET_BUCKET_NAME' and prefix '$PARQUET_PREFIX'"
        exit 1
    fi
}

seed_parquet_if_needed() {
    local category=$1
    local host=$2
    local access_key=$3
    local secret_key=$4

    if [[ "$PARQUET_SEED_IF_EMPTY" != "true" ]]; then
        return 0
    fi

    if [[ ! -d "$PARQUET_SAMPLE_DIR" ]]; then
        echo "    ✗ Parquet sample directory not found: $PARQUET_SAMPLE_DIR"
        echo "      Set WARP_PARQUET_SAMPLE_DIR to a directory containing .parquet files"
        return 1
    fi

    local endpoint_url
    if [[ "$category" == "s3-direct" ]]; then
        endpoint_url="$S3_ENDPOINT"
    else
        endpoint_url="http://$host"
    fi

    echo "  Ensuring parquet seed objects exist (category=$category, endpoint=$endpoint_url)..."
    if dotnet run --project "$BENCHMARK_DIR/OSWS.Performance.Benchmarks.csproj" -c Release --no-build -- \
        seed-parquet \
        --endpoint "$endpoint_url" \
        --access-key "$access_key" \
        --secret-key "$secret_key" \
        --bucket "$PARQUET_BUCKET_NAME" \
        --prefix "$PARQUET_PREFIX" \
        --sample-dir "$PARQUET_SAMPLE_DIR"; then
        echo "    ✓ Parquet seed check complete"
    else
        echo "    ✗ Failed to seed parquet objects for $category"
        return 1
    fi
}

start_osws_instances() {
    local instance_count=$1
    local disable_encryption=$2
    local enable_file_cache=$3

    local encryption_enabled=true
    if [[ "$disable_encryption" == "true" ]]; then
        encryption_enabled=false
    fi

    bash "$SCRIPT_DIR/osws-stop.sh" all >/dev/null 2>&1 || true

    echo "  Starting OSWS instances (disable-encryption=$disable_encryption, file-cache=$enable_file_cache)"
    for i in $(seq 1 "$instance_count"); do
        local port=$((OSWS_BASE_PORT + (i-1)*2))
        echo "    Instance $i on port $port"
        bash "$SCRIPT_DIR/osws-start.sh" "$i" "$encryption_enabled" "$port" "$enable_file_cache"
    done

    echo "  Verifying health"
    for i in $(seq 1 "$instance_count"); do
        local port=$((OSWS_BASE_PORT + (i-1)*2))
        if curl -fsS "http://localhost:$port/health" >/dev/null 2>&1; then
            echo "    ✓ http://localhost:$port/health"
        else
            echo "    ✗ Instance on port $port failed health check"
            exit 1
        fi
    done
}

write_plain_json_if_possible() {
    local source_zst=$1
    local output_json=$2

    if [[ ! -f "$source_zst" ]]; then
        return 0
    fi

    if command -v zstd >/dev/null 2>&1; then
        if zstd -d -f -q "$source_zst" -o "$output_json"; then
            echo "    ✓ Saved: $output_json"
        else
            echo "    ! Could not extract plain JSON from $source_zst"
        fi
    else
        echo "    ! zstd not found; skipping plain JSON extraction"
        echo "      Install with: brew install zstd"
    fi
}

for num_instances in "${INSTANCE_COUNTS[@]}"; do
    echo "════════════════════════════════════════════════════════"
    echo "Instance count: $num_instances"
    echo "════════════════════════════════════════════════════════"

    # Category: Baseline / S3 (direct)
    run_warp "s3-direct" "$num_instances" "$S3_HOST" "$S3_ACCESS_KEY" "$S3_SECRET_KEY"
    if [[ "$ENABLE_PARQUET_GET" == "true" ]]; then
        seed_parquet_if_needed "s3-direct" "$S3_HOST" "$S3_ACCESS_KEY" "$S3_SECRET_KEY"
        run_warp_parquet_get "s3-direct" "$num_instances" "$S3_HOST" "$S3_ACCESS_KEY" "$S3_SECRET_KEY"
    fi

    # Category: Baseline / OSWS (encryption disabled)
    start_osws_instances "$num_instances" "true" "false"
    run_warp "osws-no-encryption" "$num_instances" "localhost:$OSWS_BASE_PORT" "minioadmin" "minioadmin"
    if [[ "$ENABLE_PARQUET_GET" == "true" ]]; then
        seed_parquet_if_needed "osws-no-encryption" "localhost:$OSWS_BASE_PORT" "minioadmin" "minioadmin"
        run_warp_parquet_get "osws-no-encryption" "$num_instances" "localhost:$OSWS_BASE_PORT" "minioadmin" "minioadmin"
    fi
    bash "$SCRIPT_DIR/osws-stop.sh" all >/dev/null 2>&1 || true

    # Category: Baseline / OSWS + encryption (cache disabled)
    start_osws_instances "$num_instances" "false" "false"
    run_warp "osws-encryption-no-cache" "$num_instances" "localhost:$OSWS_BASE_PORT" "minioadmin" "minioadmin"
    if [[ "$ENABLE_PARQUET_GET" == "true" ]]; then
        seed_parquet_if_needed "osws-encryption-no-cache" "localhost:$OSWS_BASE_PORT" "minioadmin" "minioadmin"
        run_warp_parquet_get "osws-encryption-no-cache" "$num_instances" "localhost:$OSWS_BASE_PORT" "minioadmin" "minioadmin"
    fi
    bash "$SCRIPT_DIR/osws-stop.sh" all >/dev/null 2>&1 || true

    # Category: Baseline / OSWS + encryption (cache enabled)
    start_osws_instances "$num_instances" "false" "true"
    run_warp "osws-encryption-cache" "$num_instances" "localhost:$OSWS_BASE_PORT" "minioadmin" "minioadmin"
    if [[ "$ENABLE_PARQUET_GET" == "true" ]]; then
        seed_parquet_if_needed "osws-encryption-cache" "localhost:$OSWS_BASE_PORT" "minioadmin" "minioadmin"
        run_warp_parquet_get "osws-encryption-cache" "$num_instances" "localhost:$OSWS_BASE_PORT" "minioadmin" "minioadmin"
    fi
    bash "$SCRIPT_DIR/osws-stop.sh" all >/dev/null 2>&1 || true

    echo ""
done

echo "════════════════════════════════════════════════════════"
echo "Benchmark suite complete"
echo "Results location: $RESULTS_DIR"
echo "════════════════════════════════════════════════════════"
ls -lh "$RESULTS_DIR"/*.json.zst 2>/dev/null || echo "No Warp result files generated"
ls -lh "$RESULTS_DIR"/*.json 2>/dev/null || true

