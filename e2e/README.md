# OSWS End-to-End Tests

End-to-end test suite that exercises OSWS as a complete system: from S3 API calls through encryption, column-level permission filtering, and real analytics engine queries.

## Test files

| File | Description |
| ---- | ----------- |
| `test_permissions.py` | Column-level RBAC: creates 4 roles (admin, analyst, junior, intern) with hierarchical permissions, uploads a Parquet file, verifies each role sees only permitted columns |
| `test_duckdb.py` | DuckDB queries OSWS over its S3 interface — verifies standard query engines work without modification |
| `test_spark.py` | Same scenario using PySpark via the Hadoop S3A connector |
| `kv_cleanup.py` | Cleans up Azure Key Vault keys created during the test run |
| `seed.py` | Seeds test data (users, roles, permissions) into the database |

## Services

| Service | Image | Port(s) | Purpose |
| ------- | ----- | ------- | ------- |
| `osws-api` | Built from repo | `5000` | The OSWS proxy under test |
| `postgres` | `postgres:17` | `5433` | OSWS RBAC metadata store |
| `minio` | `minio/minio` | `9000`, `9001` | Backend S3 store; MinIO console on `9001` |

All three are managed by `docker-compose.e2e.yml`. OSWS applies EF Core migrations automatically on startup and connects to MinIO as its backend S3.

## Requirements

- **Docker**
- **Python 3.10+** and `pip3`
- **Java 17+** (required by PySpark/Spark)

Python packages are installed automatically by `run.sh` from `requirements.txt`.

## Key Vault

By default the suite runs with the **internal (in-memory) key vault**. To test against **Azure Key Vault**, copy `.env` and set:

```env
E2E_KV_PROVIDER=Azure
E2E_KV_VAULT_URI=https://<your-vault>.vault.azure.net/
AZURE_TENANT_ID=...
AZURE_CLIENT_ID=...
AZURE_CLIENT_SECRET=...
```

The test automatically cleans up all keys created in the key vault.

## How to run

Run from the **repository root**:

```bash
./e2e/run.sh
```

This will:

1. Build the OSWS Docker image and start all three services (waiting for health checks)
2. Install Python dependencies
3. Run `test_permissions.py`, `test_duckdb.py`, and `test_spark.py` in order
4. Tear down all containers and volumes (and clean up Azure KV keys if applicable)

### Running individual tests

```bash
# Start services
docker compose -f e2e/docker-compose.e2e.yml -p osws-e2e up -d --build --wait

# Install deps
pip3 install -q -r e2e/requirements.txt

# Run a single test
python3 -m e2e.test_duckdb

# Tear down
docker compose -f e2e/docker-compose.e2e.yml -p osws-e2e down -v
```

## E2E mode

OSWS supports an `App__E2EMode=true` flag (set in the compose file) that bypasses OIDC authentication. In this mode, only SigV4 authentication is required, which lets the test suite work without an OIDC provider. The E2E compose file sets a dummy OIDC provider that is never used.
