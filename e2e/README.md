# OSWS End-to-End Tests

End-to-end test suite that exercises OSWS as a complete system: from S3 API calls down through encryption, column-level permission filtering, and real analytics engine queries.

## Purpose

The suite verifies three things:

1. **Column-level permission filtering** (`test_permissions.py`). The suite creates four users: admin, analyst, junior and intern. The "intern" role has access to a subset of the columns. "Junior" inherits permissions from "intern" and adds a single permission more. "Analyst" inherits from "junior" and has access to the rest of the columns - thus it has full has access. The admin user is only used to configure permissions through the Admin API.
   The test uploads a Parquet file with the "analyst" role, and asserts that all users querying the file only have access to the columns their role permits.

2. **DuckDB compatibility** (`test_duckdb.py`). A real DuckDB instance queries OSWS over its S3 interface and receives correctly filtered Parquet output, demonstrating that standard query engines work without modification.

3. **Apache Spark compatibility** (`test_spark.py`). The same scenario using PySpark via the Hadoop S3A connector.

## Services

| Service    | Image           | Port(s)        | Purpose                                          |
| ---------- | --------------- | -------------- | ------------------------------------------------ |
| `osws-api` | Built from repo | `5000`         | The OSWS proxy under test                        |
| `postgres` | `postgres:17`   | `5433`         | OSWS RBAC metadata store (users, roles, columns) |
| `minio`    | `minio/minio`   | `9000`, `9001` | Backend S3 store; MinIO console on `9001`        |

All three are managed by `docker-compose.e2e.yml`. OSWS applies EF Core migrations automatically on startup and connects to MinIO as its backend S3.

## Requirements

- **Docker**
- **Python 3.10+** and `pip3`
- **Java 17+** (required by PySpark/Spark)

Python packages are installed automatically by `run.sh` from `requirements.txt`:

```
psycopg2-binary, pyarrow, boto3, requests, duckdb, pyspark>=4.0, azure-identity, azure-keyvault-keys, python-dotenv
```

## Key Vault

By default the suite runs with the **internal (in-memory) key vault**. However, the suite can also be configured to run against **Azure Key Vault** for testing with a real external vault provider. The test will automatically clean up all keys created in the key vault.

To test against **Azure Key Vault**, copy `.env` and set:

```env
E2E_KV_PROVIDER=Azure
E2E_KV_VAULT_URI=https://<your-vault>.vault.azure.net/
AZURE_TENANT_ID=...
AZURE_CLIENT_ID=...
AZURE_CLIENT_SECRET=...
```

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
