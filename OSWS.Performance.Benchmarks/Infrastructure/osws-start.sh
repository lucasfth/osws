#!/bin/bash

# Start OSWS Instance(s)
# Usage: osws-start.sh <instance-number> <encryption-enabled> <port>
#
# Parameters:
#   instance-number: Instance identifier (1, 2, 4, 8, etc.)
#   encryption-enabled: "true" for encrypted mode, "false" for non-encrypted mode
#   port: Port number to listen on
#
# Examples:
#   osws-start.sh 1 true 5000        # Start instance 1 in encrypted mode on port 5000
#   osws-start.sh 1 false 5001       # Start instance 1 in non-encrypted mode on port 5001

if [[ $# -ne 3 ]]; then
    echo "Usage: osws-start.sh <instance-number> <encryption-enabled> <port>"
    exit 1
fi

INSTANCE_NUM=$1
ENCRYPTION_ENABLED=$2
PORT=$3

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
ENV_VARS=""
if [[ -f "$ENV_FILE" ]]; then
    # Export variables to current shell
    export $(cat "$ENV_FILE" | grep -v '^#' | xargs)
    
    # Also collect them for passing to dotnet process
    while IFS= read -r line; do
        [[ "$line" =~ ^[^#]*= ]] && ENV_VARS="$ENV_VARS $line"
    done < <(cat "$ENV_FILE" | grep -v '^#' | grep '=')
fi

echo "Starting OSWS instance $INSTANCE_NUM ($MODE) on port $PORT..."

if [[ ! -d "$WEBAPP_DIR" ]]; then
    echo "ERROR: OSWS.WebApi directory not found at: $WEBAPP_DIR"
    exit 1
fi

# Set up environment and start OSWS in the background
export ASPNETCORE_URLS="http://0.0.0.0:$PORT"
export Encryption__DisableEncryption=$DISABLE_ENCRYPTION

cd "$WEBAPP_DIR"

# Build in Release mode if not already built
if [[ ! -f "bin/Release/net10.0/OSWS.WebApi" ]]; then
    echo "Building Release binary..."
    dotnet build -c Release -q > /dev/null 2>&1
fi

# Run the Release binary directly (avoids launchSettings.json override)
DLL_PATH="bin/Release/net10.0/OSWS.WebApi.dll"
if [[ ! -f "$DLL_PATH" ]]; then
    echo "ERROR: Release DLL not found at: $DLL_PATH"
    exit 1
fi

# Start dotnet with environment variables from .env explicitly passed
nohup env $ENV_VARS ASPNETCORE_URLS="http://0.0.0.0:$PORT" Encryption__DisableEncryption=$DISABLE_ENCRYPTION dotnet "$DLL_PATH" > "/tmp/osws-instance-${INSTANCE_NUM}-${MODE}.log" 2>&1 &
SHELL_PID=$!

# Wait a moment for the dotnet process to start
sleep 2

# Find the actual dotnet process that was just started
# Look for dotnet processes that match our DLL and were started recently
ACTUAL_PID=$(ps aux | grep "dotnet.*OSWS.WebApi.dll" | grep -v grep | awk '{print $2}' | head -1)

if [[ -z "$ACTUAL_PID" ]]; then
    echo "ERROR: Could not find dotnet process for instance $INSTANCE_NUM"
    wait $SHELL_PID 2>/dev/null || true
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
        cat "/tmp/osws-instance-${INSTANCE_NUM}-${MODE}.log"
        exit 1
    fi
    echo "  Attempt $i/30..."
    sleep 1
done

echo "ERROR: Instance $INSTANCE_NUM failed to start within 30 seconds"
kill $ACTUAL_PID 2>/dev/null || true
cat "/tmp/osws-instance-${INSTANCE_NUM}-${MODE}.log"
exit 1
