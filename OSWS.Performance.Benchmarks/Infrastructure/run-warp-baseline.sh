#!/bin/bash

# Run OSWS Warp baseline benchmarks against a remote VM.
#
# OSWS runs on a remote VM controlled via a management API.
# Warp runs locally and benchmarks against the OSWS instances.
#
# Scaling model:
#   1. Scale once to the maximum instance count at the start of each category.
#   2. For each iteration, Warp targets only a subset of those instances.
#      The VM keeps all instances running; Warp just uses fewer hosts.
#   3. Stop once at the end of the category.
#
# This avoids tearing down and rebuilding instances between iterations.
#
# Executes these baseline categories for each selected instance count:
# 1) S3/R2 direct
# 2) OSWS without encryption
# 3) OSWS with encryption (cache disabled)
# 4) OSWS with encryption (cache enabled)
#
# Usage: ./run-warp-baseline.sh [instance-count] [concurrency] [duration-seconds] [workload-profile]
#
# Concurrency behavior:
# - If [concurrency] arg is provided, it is treated as a fixed value for all instance counts.
# - If omitted, concurrency scales with instance count using:
#     effective_concurrency = WARP_CONCURRENCY_PER_INSTANCE * instance_count
#   (defaults to 8 * instance_count)
#
# Required .env variables:
#   VM_MANAGEMENT_URL  - URL of the VM management API (e.g. http://192.168.1.100:9000)
#   VM_OSWS_HOST       - Hostname/IP of the VM where OSWS runs
#   S3Settings__*      - S3 backend credentials

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCHMARK_DIR="$(dirname "$SCRIPT_DIR")"
REPO_ROOT="$(dirname "$BENCHMARK_DIR")"

# Load environment from .env file
ENV_FILE="$BENCHMARK_DIR/.env"
if [[ -f "$ENV_FILE" ]]; then
    set -a
    while IFS= read -r line || [[ -n "$line" ]]; do
        [[ "$line" =~ ^[[:space:]]*# ]] && continue
        [[ -z "${line// }" ]] && continue
        if [[ "$line" =~ ^([^=]+)=(.*)$ ]]; then
            key="${BASH_REMATCH[1]}"
            value="${BASH_REMATCH[2]}"

            key="${key##+([[:space:]])}"
            key="${key%%+([[:space:]])}"
            value="${value##+([[:space:]])}"
            value="${value%%+([[:space:]])}"

            if [[ ("$value" == \"*\" && "$value" == *\") || ("$value" == \'*\' && "$value" == *\') ]]; then
                value="${value:1:${#value}-2}"
            fi

            export "$key=$value"
        fi
    done < "$ENV_FILE"
    set +a
    ENV_LOADED=true
else
    ENV_LOADED=false
fi

# CLI arguments
INSTANCE_COUNT="${1:-}"
WARP_CONCURRENCY_ARG="${2:-}"
WARP_DURATION="${3:-${WARP_DURATION_SECONDS:-60}}"
WORKLOAD_PROFILE="${4:-${WARP_WORKLOAD_PROFILE:-mixed}}"

WARP_CONCURRENCY_MODE="${WARP_CONCURRENCY_MODE:-per-instance}"
WARP_CONCURRENCY_PER_INSTANCE="${WARP_CONCURRENCY_PER_INSTANCE:-${WARP_CONCURRENCY:-8}}"
WARP_CONCURRENCY_FIXED="${WARP_CONCURRENCY_FIXED:-${WARP_CONCURRENCY:-8}}"

if [[ -n "$WARP_CONCURRENCY_ARG" ]]; then
    WARP_CONCURRENCY_MODE="fixed"
    WARP_CONCURRENCY_FIXED="$WARP_CONCURRENCY_ARG"
fi

if [[ ! "$WARP_CONCURRENCY_PER_INSTANCE" =~ ^[0-9]+$ || "$WARP_CONCURRENCY_PER_INSTANCE" -le 0 ]]; then
    echo "ERROR: WARP_CONCURRENCY_PER_INSTANCE must be a positive integer"
    exit 1
fi

if [[ ! "$WARP_CONCURRENCY_FIXED" =~ ^[0-9]+$ || "$WARP_CONCURRENCY_FIXED" -le 0 ]]; then
    echo "ERROR: WARP_CONCURRENCY_FIXED must be a positive integer"
    exit 1
fi

if [[ "$WARP_CONCURRENCY_MODE" != "fixed" && "$WARP_CONCURRENCY_MODE" != "per-instance" ]]; then
    echo "ERROR: WARP_CONCURRENCY_MODE must be 'fixed' or 'per-instance'"
    exit 1
fi

# VM configuration (required)
VM_MANAGEMENT_URL="${VM_MANAGEMENT_URL:?VM_MANAGEMENT_URL is required in .env}"
VM_MANAGEMENT_URL="${VM_MANAGEMENT_URL%/}"
VM_OSWS_HOST="${VM_OSWS_HOST:?VM_OSWS_HOST is required in .env}"
VM_HEALTH_TIMEOUT_SECONDS="${VM_HEALTH_TIMEOUT_SECONDS:-120}"

RESULTS_DIR="$BENCHMARK_DIR/warp-results"
BUCKET_NAME="${WARP_BUCKET_NAME:-warp-benchmark-test}"
ENABLE_PARQUET_GET="${WARP_ENABLE_PARQUET_GET:-true}"
PARQUET_BUCKET_NAME="${WARP_PARQUET_BUCKET_NAME:-$BUCKET_NAME}"
PARQUET_PREFIX="${WARP_PARQUET_PREFIX:-parquet/}"
PARQUET_OBJECT_LIMIT="${WARP_PARQUET_OBJECT_LIMIT:-0}"
WARP_OSWS_USER_NAME="${WARP_OSWS_USER_NAME:-warp-benchmark}"
WARP_OSWS_ROLE_NAME="${WARP_OSWS_ROLE_NAME:-warp-benchmark-role}"

echo "╔════════════════════════════════════════════════════════╗"
echo "║   OSWS Warp Baseline Benchmark Suite (VM Mode)         ║"
echo "╚════════════════════════════════════════════════════════╝"
echo ""

if ! command -v warp >/dev/null 2>&1; then
    echo "ERROR: Warp not found in PATH"
    echo "Install: brew install minio/stable/warp"
    exit 1
fi

if ! command -v python3 >/dev/null 2>&1; then
    echo "ERROR: python3 not found in PATH (required to parse VM API responses)"
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

# Find the maximum instance count — we scale to this once per category.
MAX_INSTANCE_COUNT=0
for n in "${INSTANCE_COUNTS[@]}"; do
    if (( n > MAX_INSTANCE_COUNT )); then
        MAX_INSTANCE_COUNT=$n
    fi
done

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
echo "  VM management API: $VM_MANAGEMENT_URL"
echo "  VM OSWS host: $VM_OSWS_HOST"
echo "  Instance counts: ${INSTANCE_COUNTS[*]}"
echo "  Max instances (scale target): $MAX_INSTANCE_COUNT"
if [[ "$WARP_CONCURRENCY_MODE" == "fixed" ]]; then
    echo "  Concurrency mode: fixed"
    echo "  Concurrency: $WARP_CONCURRENCY_FIXED"
else
    echo "  Concurrency mode: per-instance"
    echo "  Concurrency per instance: $WARP_CONCURRENCY_PER_INSTANCE"
fi
echo "  Duration: ${WARP_DURATION}s"
echo "  Workload profile: $WORKLOAD_PROFILE"
echo "  Bucket: $BUCKET_NAME"
echo "  Parquet GET enabled: $ENABLE_PARQUET_GET"
echo "  Parquet bucket: $PARQUET_BUCKET_NAME"
echo "  Parquet prefix: $PARQUET_PREFIX"
echo "  OSWS benchmark user: $WARP_OSWS_USER_NAME"
echo "  OSWS benchmark role: $WARP_OSWS_ROLE_NAME"
echo "  S3 endpoint: $S3_ENDPOINT"
echo "  Results dir: $RESULTS_DIR"
echo ""

if [[ "$ENABLE_PARQUET_GET" == "true" ]]; then
    echo "Parquet mode note:"
    echo "  Parquet GET benchmarks use existing objects only (--list-existing)."
    echo "  Seed parquet objects beforehand under bucket '$PARQUET_BUCKET_NAME' and prefix '$PARQUET_PREFIX'."
    echo ""
fi

# VM management functions

# All instance endpoints are stored in this array after /scale.
# Format: "host:port"
declare -a ALL_INSTANCE_ENDPOINTS=()

vm_scale() {
    local instance_count=$1
    local disable_encryption=$2
    local enable_file_cache=$3

    local body="{\"instances\":${instance_count},\"disableEncryption\":${disable_encryption},\"enableFileCache\":${enable_file_cache}}"

    echo "  Scaling to $instance_count instance(s) on VM..."
    local response
    response=$(curl -sf -X POST "$VM_MANAGEMENT_URL/scale" \
        -H "Content-Type: application/json" \
        -d "$body" 2>&1) || {
        echo "    ✗ Failed to scale OSWS instances on VM"
        echo "    Response: $response"
        return 1
    }

    # Parse instance endpoints from response.
    # Expected format: {"instances": [{"host":"192.168.1.100","port":8000}, ...]}
    ALL_INSTANCE_ENDPOINTS=()
    local num_endpoints
    num_endpoints=$(echo "$response" | python3 -c "
import sys, json
data = json.load(sys.stdin)
instances = data.get('instances', [])
for i in instances:
    print(f\"{i['host']}:{i['port']}\")
" 2>/dev/null) || {
        echo "    ✗ Failed to parse instance endpoints from scale response"
        echo "    Response: $response"
        return 1
    }

    while IFS= read -r ep; do
        [[ -n "$ep" ]] && ALL_INSTANCE_ENDPOINTS+=("$ep")
    done <<< "$num_endpoints"

    if (( ${#ALL_INSTANCE_ENDPOINTS[@]} != instance_count )); then
        echo "    ✗ Expected $instance_count endpoints but got ${#ALL_INSTANCE_ENDPOINTS[@]}"
        echo "    Endpoints: ${ALL_INSTANCE_ENDPOINTS[*]}"
        return 1
    fi

    echo "  ✓ Scale request accepted: ${#ALL_INSTANCE_ENDPOINTS[@]} instances available"
    echo "    Endpoints: ${ALL_INSTANCE_ENDPOINTS[*]}"
}

vm_health() {
    local max_wait=$VM_HEALTH_TIMEOUT_SECONDS
    local waited=0

    echo "  Waiting for VM instances to be ready (timeout: ${max_wait}s)..."
    while [[ $waited -lt $max_wait ]]; do
        local response
        response=$(curl -sf -X GET "$VM_MANAGEMENT_URL/health" 2>&1) || response=""

        if echo "$response" | grep -q '"healthy"[[:space:]]*:[[:space:]]*true'; then
            echo "  ✓ VM healthy: $response"
            return 0
        fi

        sleep 5
        waited=$((waited + 5))
        echo "    Waiting... (${waited}s/${max_wait}s)"
    done

    echo "    ✗ VM did not become healthy within ${max_wait}s"
    return 1
}

vm_stop() {
    echo "  Stopping OSWS instances on VM..."
    curl -sf -X POST "$VM_MANAGEMENT_URL/stop" >/dev/null 2>&1 || true
    echo "  ✓ Stop request sent"
}

# Build a comma-separated host list from a subset of instances.
# Takes the first N entries from ALL_INSTANCE_ENDPOINTS.
build_osws_hosts() {
    local count=$1
    local hosts=""
    local limit=$count
    if (( limit > ${#ALL_INSTANCE_ENDPOINTS[@]} )); then
        limit=${#ALL_INSTANCE_ENDPOINTS[@]}
    fi
    for (( i=0; i<limit; i++ )); do
        if [[ -n "$hosts" ]]; then
            hosts="$hosts,${ALL_INSTANCE_ENDPOINTS[$i]}"
        else
            hosts="${ALL_INSTANCE_ENDPOINTS[$i]}"
        fi
    done
    echo "$hosts"
}

# Cleanup on exit

cleanup() {
    vm_stop >/dev/null 2>&1 || true
    cleanup_osws_credential >/dev/null 2>&1 || true
}

trap cleanup EXIT

# Warp execution

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
        payload_args=("--disable-sha256-payload")
    fi

    local result_base="$RESULTS_DIR/warp-${instance_count}instances-${category}"
    local result_file="${result_base}.json.zst"
    local result_json="${result_base}.json"

    echo "  Running: $category (instances=$instance_count, host=$host, concurrent=$CURRENT_WARP_CONCURRENCY)"
    if warp "$WORKLOAD_PROFILE" \
        --duration "${WARP_DURATION}s" \
        --concurrent "$CURRENT_WARP_CONCURRENCY" \
        --objects 1000 \
        --obj.size 1M \
        --bucket "$BUCKET_NAME" \
        --host "$host" \
        --host-select roundrobin \
        --access-key "$access_key" \
        --secret-key "$secret_key" \
        --benchdata "$result_base" \
        "${tls_args[@]}" \
        "${payload_args[@]}" \
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

    local result_base="$RESULTS_DIR/warp-${instance_count}instances-${category}-parquet-get"
    local result_file="${result_base}.json.zst"
    local result_json="${result_base}.json"

    echo "  Running: ${category} parquet-get (instances=$instance_count, host=$host, concurrent=$CURRENT_WARP_CONCURRENCY, prefix=$PARQUET_PREFIX)"
    local warp_output
    warp_output=$(warp get \
        --duration "${WARP_DURATION}s" \
        --concurrent "$CURRENT_WARP_CONCURRENCY" \
        --objects "$PARQUET_OBJECT_LIMIT" \
        --bucket "$PARQUET_BUCKET_NAME" \
        --host "$host" \
        --host-select roundrobin \
        --access-key "$access_key" \
        --secret-key "$secret_key" \
        --prefix "$PARQUET_PREFIX" \
        --list-existing \
        --noclear \
        --benchdata "$result_base" \
        "${tls_args[@]}" \
        --json 2>&1) || warp_output="$warp_output"

    if echo "$warp_output" | grep -qi "no objects found\|no such key\|does not exist"; then
        echo "  No parquet objects found for $category (skipping parquet-get)"
        return 0
    fi

    if echo "$warp_output" | grep -qi "error\|failed"; then
        echo "$warp_output" | head -20
        echo "    ✗ Parquet GET error for $category"
        return 0
    fi

    if [[ -f "$result_file" ]]; then
        echo "    ✓ Saved: $result_file"
        write_plain_json_if_possible "$result_file" "$result_json"
    elif [[ -f "${result_file}.json.zst" ]]; then
        echo "    ✓ Saved: ${result_file}.json.zst"
        mv "${result_file}.json.zst" "$result_file"
        echo "    ✓ Renamed to: $result_file"
        write_plain_json_if_possible "$result_file" "$result_json"
    else
        echo "    ! No parquet-get results for $category"
    fi
}

seed_osws_credential() {
    local seed_output
    seed_output=$(dotnet run --project "$BENCHMARK_DIR/OSWS.Performance.Benchmarks.csproj" -c Release --no-build -- \
        seed-s3-credential \
        --user-name "$WARP_OSWS_USER_NAME" \
        --role-name "$WARP_OSWS_ROLE_NAME") || return 1

    WARP_OSWS_ACCESS_KEY=$(echo "$seed_output" | grep '^WARP_OSWS_ACCESS_KEY=' | cut -d= -f2-)
    WARP_OSWS_SECRET_KEY=$(echo "$seed_output" | grep '^WARP_OSWS_SECRET_KEY=' | cut -d= -f2-)

    if [[ -z "$WARP_OSWS_ACCESS_KEY" || -z "$WARP_OSWS_SECRET_KEY" ]]; then
        echo "    ✗ Failed to parse OSWS benchmark credentials"
        echo "$seed_output"
        return 1
    fi

    echo "  ✓ Seeded OSWS benchmark credential"
}

ensure_osws_bucket() {
    if [[ ${#ALL_INSTANCE_ENDPOINTS[@]} -eq 0 ]]; then
        echo "    ✗ No instance endpoints available for bucket creation"
        return 1
    fi
    local endpoint_url="http://${ALL_INSTANCE_ENDPOINTS[0]}"
    if dotnet run --project "$BENCHMARK_DIR/OSWS.Performance.Benchmarks.csproj" -c Release --no-build -- \
        ensure-bucket \
        --endpoint "$endpoint_url" \
        --access-key "$WARP_OSWS_ACCESS_KEY" \
        --secret-key "$WARP_OSWS_SECRET_KEY" \
        --bucket "$BUCKET_NAME"; then
        echo "  ✓ Ensured OSWS bucket exists ($BUCKET_NAME)"
    else
        echo "    ✗ Failed to ensure OSWS bucket exists ($BUCKET_NAME)"
        return 1
    fi
}

cleanup_osws_credential() {
    if [[ -z "$WARP_OSWS_ACCESS_KEY" ]]; then
        return 0
    fi

    dotnet run --project "$BENCHMARK_DIR/OSWS.Performance.Benchmarks.csproj" -c Release --no-build -- \
        cleanup-s3-credential \
        --access-key "$WARP_OSWS_ACCESS_KEY" \
        --user-name "$WARP_OSWS_USER_NAME" \
        --role-name "$WARP_OSWS_ROLE_NAME" >/dev/null || true

    WARP_OSWS_ACCESS_KEY=""
    WARP_OSWS_SECRET_KEY=""
}

start_osws_category() {
    local disable_encryption=$1
    local enable_file_cache=$2

    # Scale once to max instances for this category.
    vm_scale "$MAX_INSTANCE_COUNT" "$disable_encryption" "$enable_file_cache" || exit 1
    vm_health || exit 1
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

# Main benchmark loop

# Phase 1: S3 direct — runs for each instance count
echo "════════════════════════════════════════════════════════"
echo "Phase 1: S3 Direct (baseline)"
echo "════════════════════════════════════════════════════════"

for num_instances in "${INSTANCE_COUNTS[@]}"; do
    echo ""
    echo "--- $num_instances instance(s) ---"
    if [[ "$WARP_CONCURRENCY_MODE" == "fixed" ]]; then
        CURRENT_WARP_CONCURRENCY="$WARP_CONCURRENCY_FIXED"
    else
        CURRENT_WARP_CONCURRENCY=$((WARP_CONCURRENCY_PER_INSTANCE * num_instances))
    fi
    echo "Effective concurrency: $CURRENT_WARP_CONCURRENCY"

    run_warp "s3-direct" "$num_instances" "$S3_HOST" "$S3_ACCESS_KEY" "$S3_SECRET_KEY"
    if [[ "$ENABLE_PARQUET_GET" == "true" ]]; then
        run_warp_parquet_get "s3-direct" "$num_instances" "$S3_HOST" "$S3_ACCESS_KEY" "$S3_SECRET_KEY"
    fi
done

# Phase 2: OSWS categories
# For each category: scale once to MAX, run all instance counts, then stop.
echo ""
echo "════════════════════════════════════════════════════════"
echo "Phase 2: OSWS Categories"
echo "════════════════════════════════════════════════════════"

run_osws_category() {
    local category_name=$1
    local disable_encryption=$2
    local enable_file_cache=$3

    echo ""
    echo "────────────────────────────────────────────────────"
    echo "Category: $category_name"
    echo "  disableEncryption=$disable_encryption  enableFileCache=$enable_file_cache"
    echo "────────────────────────────────────────────────────"

    # Scale once to max instances
    start_osws_category "$disable_encryption" "$enable_file_cache"
    seed_osws_credential
    ensure_osws_bucket

    for num_instances in "${INSTANCE_COUNTS[@]}"; do
        echo ""
        echo "--- $num_instances instance(s) ---"
        if [[ "$WARP_CONCURRENCY_MODE" == "fixed" ]]; then
            CURRENT_WARP_CONCURRENCY="$WARP_CONCURRENCY_FIXED"
        else
            CURRENT_WARP_CONCURRENCY=$((WARP_CONCURRENCY_PER_INSTANCE * num_instances))
        fi
        echo "Effective concurrency: $CURRENT_WARP_CONCURRENCY"

        local osws_hosts
        osws_hosts=$(build_osws_hosts "$num_instances")
        echo "  Targeting: $osws_hosts"

        run_warp "$category_name" "$num_instances" "$osws_hosts" "$WARP_OSWS_ACCESS_KEY" "$WARP_OSWS_SECRET_KEY"
        if [[ "$ENABLE_PARQUET_GET" == "true" ]]; then
            run_warp_parquet_get "$category_name" "$num_instances" "$osws_hosts" "$WARP_OSWS_ACCESS_KEY" "$WARP_OSWS_SECRET_KEY"
        fi
    done

    cleanup_osws_credential
    vm_stop >/dev/null 2>&1 || true
}

run_osws_category "osws-no-encryption" "true" "false"
run_osws_category "osws-encryption-no-cache" "false" "false"
run_osws_category "osws-encryption-cache" "false" "true"

echo "════════════════════════════════════════════════════════"
echo "Benchmark suite complete"
echo "Results location: $RESULTS_DIR"
echo "════════════════════════════════════════════════════════"
ls -lh "$RESULTS_DIR"/*.json.zst 2>/dev/null || echo "No Warp result files generated"
ls -lh "$RESULTS_DIR"/*.json 2>/dev/null || true
