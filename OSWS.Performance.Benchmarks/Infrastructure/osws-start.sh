#!/bin/bash

# Start OSWS Instance(s)
# Usage: osws-start.sh <instance-number> <encryption-enabled> <port> [file-cache-enabled]
#
# Parameters:
#   instance-number: Instance identifier (1, 2, 4, 8, etc.)
#   encryption-enabled: "true" for encrypted mode, "false" for non-encrypted mode
#   port: Port number to listen on
#   file-cache-enabled: Optional. "true" or "false" (default: true)
#
# Examples:
#   osws-start.sh 1 true 5000        # Start instance 1 in encrypted mode on port 5000
#   osws-start.sh 1 true 5000 false  # Start encrypted mode with file cache disabled
#   osws-start.sh 1 false 5001       # Start instance 1 in non-encrypted mode on port 5001

set -e

if [[ $# -lt 3 || $# -gt 4 ]]; then
    echo "Usage: osws-start.sh <instance-number> <encryption-enabled> <port> [file-cache-enabled]"
    exit 1
fi

INSTANCE_NUM=$1
ENCRYPTION_ENABLED=$2
PORT=$3
FILE_CACHE_ENABLED=${4:-true}

# Determine encryption mode string
if [[ "$ENCRYPTION_ENABLED" == "true" ]]; then
    MODE="encrypted"
    DISABLE_ENCRYPTION=false
else
    MODE="non-encrypted"
    DISABLE_ENCRYPTION=true
fi

# Get script directory and navigate to repo root
# Infrastructure is 2 levels down from repo root:
# repo/OSWS.Performance.Benchmarks/Infrastructure
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
WEBAPP_DIR="$REPO_ROOT/OSWS.WebApi"
BENCHMARK_DIR="$(dirname "$SCRIPT_DIR")"

# Load .env file if it exists
ENV_FILE="$BENCHMARK_DIR/.env"
if [[ -f "$ENV_FILE" ]]; then
    # Export variables to current shell so they're inherited by dotnet process
    # Use set -a to automatically export all variables
    set -a
    # Source the .env file, filtering out comments and empty lines
    while IFS= read -r line || [[ -n "$line" ]]; do
        # Skip comments and empty lines
        [[ "$line" =~ ^[[:space:]]*# ]] && continue
        [[ -z "${line// }" ]] && continue
        # Export the variable
        eval "export $line"
    done < "$ENV_FILE"
    set +a
fi

echo "Starting OSWS instance $INSTANCE_NUM ($MODE) on port $PORT..."

if [[ ! -d "$WEBAPP_DIR" ]]; then
    echo "ERROR: OSWS.WebApi directory not found at: $WEBAPP_DIR"
    exit 1
fi

# Set up environment and start OSWS in the background
export ASPNETCORE_URLS="http://0.0.0.0:$PORT"
export Encryption__DisableEncryption=$DISABLE_ENCRYPTION
export Cache__EnableFileCache=$FILE_CACHE_ENABLED

cd "$WEBAPP_DIR"

# Run the Release binary directly (avoids launchSettings.json override)
DLL_PATH="bin/Release/net10.0/OSWS.WebApi.dll"
if [[ ! -f "$DLL_PATH" ]]; then
    echo "ERROR: Release DLL not found at: $DLL_PATH"
    echo "Run build first: dotnet build OSWS.WebApi/OSWS.WebApi.csproj -c Release -p:BuildProjectReferences=false"
    exit 1
fi

wait_for_port_release() {
    local port=$1
    local max_attempts=${2:-20}
    local attempt=1

    while [[ $attempt -le $max_attempts ]]; do
        local listeners
        listeners=$(lsof -nP -tiTCP:"$port" -sTCP:LISTEN 2>/dev/null || true)
        if [[ -z "$listeners" ]]; then
            return 0
        fi

        sleep 0.5
        attempt=$((attempt + 1))
    done

    return 1
}

# Avoid hitting stale servers by force-clearing any existing listener on the target port.
EXISTING_PIDS=$(lsof -nP -tiTCP:"$PORT" -sTCP:LISTEN 2>/dev/null || true)
if [[ -n "$EXISTING_PIDS" ]]; then
    echo "Port $PORT already in use; stopping existing listener(s): $EXISTING_PIDS"
    kill $EXISTING_PIDS 2>/dev/null || true
    sleep 2
    STILL_LISTENING=$(lsof -nP -tiTCP:"$PORT" -sTCP:LISTEN 2>/dev/null || true)
    if [[ -n "$STILL_LISTENING" ]]; then
        kill -9 $STILL_LISTENING 2>/dev/null || true
        sleep 3
    fi
fi

if ! wait_for_port_release "$PORT"; then
    echo "ERROR: Port $PORT is still in use after cleanup"
    lsof -nP -iTCP:"$PORT" || true
    exit 1
fi

# Start dotnet - environment variables are inherited from exported shell variables
LOG_DIR="$SCRIPT_DIR/../logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/osws-instance-${INSTANCE_NUM}-${MODE}.log"
nohup dotnet "$DLL_PATH" > "$LOG_FILE" 2>&1 &
ACTUAL_PID=$!

# Wait a moment for the dotnet process to start
sleep 2

if [[ -z "$ACTUAL_PID" ]]; then
    echo "ERROR: Could not find dotnet process for instance $INSTANCE_NUM"
    exit 1
fi

echo $ACTUAL_PID > "/tmp/osws-instance-${INSTANCE_NUM}-${MODE}.pid"

# Wait for instance to be ready
echo "Waiting for instance $INSTANCE_NUM to be ready..."
for i in {1..30}; do
    if curl -s "http://localhost:$PORT/health" > /dev/null 2>&1; then
        echo "✓ Instance $INSTANCE_NUM is ready on port $PORT"
        exit 0
    fi
    if ! kill -0 $ACTUAL_PID 2>/dev/null; then
        echo "ERROR: Instance process died"
        cat "$LOG_FILE"
        exit 1
    fi
    echo "  Attempt $i/30..."
    sleep 1
done

echo "ERROR: Instance $INSTANCE_NUM failed to start within 30 seconds"
kill $ACTUAL_PID 2>/dev/null || true
cat "$LOG_FILE"
exit 1
