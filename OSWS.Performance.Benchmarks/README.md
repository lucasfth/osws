# OSWS Performance Benchmarks

Measures OSWS performance using MinIO's Warp (baseline) and BenchmarkDotNet (micro-benchmarks).

## Quick Start

```bash
cd OSWS.Performance.Benchmarks
cp .env.example .env
# Edit .env with S3 credentials + VM config

./Infrastructure/run-warp-baseline.sh              # Full suite (1, 2, 4, 8 instances)
./Infrastructure/run-warp-baseline.sh 1 4 10       # Quick: 1 instance, 4 clients, 10s

dotnet run -c Release -- unwrap   # Micro: key unwrap
dotnet run -c Release -- decrypt  # Micro: decryption
```

## VM Management API

OSWS runs on a remote VM. Warp runs locally and targets individual OSWS instances.

### Scaling Model

**Scale once, subset per iteration:**
1. `POST /scale` to max instances (e.g., 8) at start of each category
2. Warp runs against subsets: 1, 2, 4, 8 — no teardown between
3. `POST /stop` at end of category

### API Contract

**`POST /scale`** — Start instances, return endpoints.

Request:
```json
{"instances": 8, "disableEncryption": false, "enableFileCache": true}
```

Response (required):
```json
{
  "instances": [
    {"host": "192.168.1.100", "port": 8000},
    {"host": "192.168.1.100", "port": 8002}
  ]
}
```

Each instance gets a unique port (e.g., 8000, 8002, 8004...). The VM must configure:
- `disableEncryption: true` → `Encryption__DisableEncryption=true`
- `enableFileCache: false` → `Cache__EnableFileCache=false`

**`GET /health`** — Return when ready.

Response:
```json
{"healthy": true, "instances": 8}
```

Script polls every 5s until `healthy: true` or timeout (120s default).

**`POST /stop`** — Stop all instances.

Response:
```json
{"status": "ok"}
```

### VM Implementation Notes

- Each instance needs its own port, reachable from the local machine
- `/health` should verify all instances respond before returning `healthy: true`
- Use Docker with explicit port mappings or a wrapper script that generates `docker-compose.yml` per scale request

## Benchmark Categories

| Category | Encryption | File Cache | Purpose |
|----------|-----------|------------|---------|
| `s3-direct` | None | N/A | Raw S3 baseline |
| `osws-no-encryption` | Disabled | N/A | OSWS proxy overhead |
| `osws-encryption-no-cache` | Enabled | Disabled | Encryption overhead |
| `osws-encryption-cache` | Enabled | Enabled | Best-case with caching |

Each category runs against 1, 2, 4, 8 instances. Warp workload: 50% GET, 20% PUT, 20% LIST, 10% DELETE.

## Micro-benchmarks

| Benchmark | Measures | Setup |
|-----------|----------|-------|
| Key Unwrap | DEK unwrap latency | Cold cache, 128/192/256-bit keys |
| Decryption | Column decrypt latency | Warm cache, 5K/10K/100K rows × 2000 cols |
| Permission Service | RBAC (flat roles) | Local Postgres, 4/64/256 roles |
| Permission Hierarchy | RBAC (inheritance) | Local Postgres, depth 0/4/16/64 |

Requires local Postgres with migrations:
```bash
psql -U <user> -c "CREATE DATABASE osws_dev;"
dotnet ef database update --project OSWS.KeyManager --startup-project OSWS.WebApi
```

## Configuration

### .env

```env
# S3 backend (required)
S3Settings__AccessKeyId=your-key
S3Settings__SecretAccessKey=your-secret
S3Settings__EndpointHostname=https://your-s3-endpoint.com
S3Settings__Region=auto

# VM (required)
VM_MANAGEMENT_URL=http://192.168.1.100:9000
VM_OSWS_HOST=192.168.1.100
VM_HEALTH_TIMEOUT_SECONDS=120

# Optional
WARP_CONCURRENCY=8
WARP_DURATION_SECONDS=60
INSTANCE_COUNTS="1 2 4 8"
```

### S3 Backend Options

- **AWS S3**: `https://s3.amazonaws.com`
- **Cloudflare R2**: `https://<account-id>.r2.cloudflarestorage.com`
- **MinIO**: `http://localhost:9000`

## Output

- `Infrastructure/warp-results/warp-<N>instances-<category>.json.zst` — Warp results
- `BenchmarkDotNet.Artifacts/results/` — Micro-benchmark reports

## Troubleshooting

| Error | Fix |
|-------|-----|
| `VM_MANAGEMENT_URL is required` | Set in `.env` |
| Warp 500 errors | Check S3 backend is accessible |
| No results files | Check `warp-results/` dir exists |
| "too many clients" | PostgreSQL `max_connections` ≥ 200 |
| Parquet GET fails | Ensure parquet files exist in S3 under correct prefix |
