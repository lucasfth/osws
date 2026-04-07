#!/bin/bash

# Stop OSWS Instance(s)
# Usage: osws-stop.sh <instance-number> [encryption-enabled]
#
# Parameters:
#   instance-number: Instance identifier (1, 2, 4, 8, etc.)
#   encryption-enabled: Optional. If "true" or "false", stops specific mode.
#                       If omitted, stops both encrypted and non-encrypted modes.
#
# Examples:
#   osws-stop.sh 1 true         # Stop instance 1 in encrypted mode
#   osws-stop.sh 1              # Stop instance 1 (both modes)
#   osws-stop.sh all            # Stop all instances

if [[ $# -lt 1 ]]; then
    echo "Usage: osws-stop.sh <instance-number> [encryption-enabled]"
    exit 1
fi

INSTANCE_NUM=$1
ENCRYPTION_ENABLED=$2

stop_instance() {
    local instance=$1
    local mode=$2
    local pid_file="/tmp/osws-instance-${instance}-${mode}.pid"

    if [[ -f "$pid_file" ]]; then
        local pid=$(cat "$pid_file")
        echo "Stopping OSWS instance $instance ($mode) with PID $pid..."
        
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
            sleep 2
            if kill -0 "$pid" 2>/dev/null; then
                echo "  Force killing..."
                kill -9 "$pid" 2>/dev/null || true
            fi
        fi
        
        rm -f "$pid_file"
    else
        echo "No PID file found for instance $instance ($mode)"
    fi
}

if [[ "$INSTANCE_NUM" == "all" ]]; then
    echo "Stopping all OSWS instances..."
    for pid_file in /tmp/osws-instance-*.pid; do
        if [[ -f "$pid_file" ]]; then
            pid=$(cat "$pid_file")
            echo "Killing PID $pid..."
            kill "$pid" 2>/dev/null || true
            sleep 0.5
            kill -9 "$pid" 2>/dev/null || true
        fi
    done
    rm -f /tmp/osws-instance-*.pid
elif [[ -z "$ENCRYPTION_ENABLED" ]]; then
    stop_instance "$INSTANCE_NUM" "encrypted"
    stop_instance "$INSTANCE_NUM" "non-encrypted"
else
    local mode="encrypted"
    if [[ "$ENCRYPTION_ENABLED" == "false" ]]; then
        mode="non-encrypted"
    fi
    stop_instance "$INSTANCE_NUM" "$mode"
fi

echo "Done"
