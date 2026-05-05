#!/bin/bash
set -euo pipefail

# Run from repo root: ./e2e/run.sh
cd "$(dirname "$0")/.."

COMPOSE_FILE="e2e/docker-compose.e2e.yml"
PROJECT_NAME="osws-e2e"

cleanup() {
  # Azure KV cleanup (only runs if Azure mode was used)
  if [[ "${E2E_KV_PROVIDER:-Internal}" == "Azure" ]]; then
    echo "Cleaning up Azure Key Vault keys..."
    python3 e2e/kv_cleanup.py || echo "⚠ KV cleanup failed (keys may need manual cleanup)"
  fi

  docker compose -f "$COMPOSE_FILE" -p "$PROJECT_NAME" down -v --remove-orphans
}
trap cleanup EXIT

# 1. Start services (build OSWS image, wait for health checks)
echo "Starting services..."
docker compose -f "$COMPOSE_FILE" -p "$PROJECT_NAME" up -d --build --wait

# 2. Install Python dependencies
pip3 install -q -r e2e/requirements.txt

# 3. Run the E2E test (migrations applied automatically by OSWS on startup)
echo "Running E2E tests..."
python3 -m e2e.test_permissions

# 4. Run DuckDB integration test (reuses seeded environment)
echo "Running DuckDB integration test..."
python3 -m e2e.test_duckdb

# 5. Run Spark integration test (reuses seeded environment; requires Java)
echo "Running Spark integration test..."
python3 -m e2e.test_spark

echo "All E2E tests passed"
