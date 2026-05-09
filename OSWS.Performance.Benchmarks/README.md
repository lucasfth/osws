# OSWS Performance Benchmarks

Measures OSWS latency using direct AWS SDK calls (throughput suite) and BenchmarkDotNet (micro-benchmarks).

## Quick Start

```bash
cd OSWS.Performance.Benchmarks
cp .env.example .env
# Fill in S3 credentials (R2) and OSWS credentials (see below)
```

### 1. Seed benchmark user and bucket

An instance of OSWS **with encryption** must be running for steps 1 and 2. Ensure the correct endpoint and database connection string is set.

```bash
# Create benchmark user + role, prints credentials — copy to .env
dotnet run -c Release -- seed-s3-credential

# Ensure benchmark bucket exists (via OSWS). Or create via storage backend dashboard.
dotnet run -c Release -- ensure-bucket \
  --endpoint http://localhost:5000 \
  --access-key <BENCH_OSWS_ACCESS_KEY> \
  --secret-key <BENCH_OSWS_SECRET_KEY> \
  --bucket osws-benchmark
```

### 2. Generate and upload corpus

OSWS must be running with **encryption enabled** for corpus upload (so column permissions are registered).

First, generate the datasets:

```bash
dotnet run -c Release -- generate-datasets
```

```bash
dotnet run -c Release -- generate-corpus
```

This uploads:

- `bench/s3-direct/{size}.parquet` — plaintext, directly to R2
- `bench/osws/warm/{size}.parquet` — through OSWS (encrypted)
- `bench/osws/cold/{size}/{001..n}.parquet` — N cold copies through OSWS (distinct DEKs), N being Repetitions from .env

### 3. Run benchmarks

Each configuration requires OSWS started with the correct env vars. Run automated using:

```bash
# 1. No OSWS — direct R2 baseline
python Infrastructure/run-suite.py
```

Each run outputs `benchmark-results/run-{timestamp}/<results>`.

### 4. Analyse results

```bash
python Infrastructure/analyse-results.py benchmark-results/run-{timestamp}
```

Prints mean / stddev / p50 / p95 per (config, operation, cache state, file size).

---

## Benchmark Design

### File corpus (100 columns of random doubles)

| Label  | Row count | Approx size |
| ------ | --------- | ----------- |
| tiny   | 1,000     | ~0.5 MB     |
| small  | 10,000    | ~5 MB       |
| medium | 250,000   | ~120 MB     |
| large  | 100,000   | ~480 MB     |
| xlarge | 2,000,000 | ~950 MB     |

### Configurations

| Config                       | Encryption | File cache | DEK Cache | Description                              |
| ---------------------------- | ---------- | ---------- | --------- | ---------------------------------------- |
| `s3-direct`                  | N/A        | N/A        | N/A       | Plaintext PUT/GET directly to S3-backend |
| `osws-encrypt-cache`         | Enabled    | Enabled    | Enabled   | Full service, with cache                 |
| `osws-encrypt-no-file-cache` | Enabled    | Disabled   | Enabled   | Full service, always cold                |
| `osws-encrypt-no-dek-cache`  | Enabled    | Enabled    | Disabled  | Full service, always cold                |
| `osws-no-encrypt`            | Disabled   | N/A        | N/A       | Forwarding overhead only                 |

### Cold vs warm GET

- **Cold:** each of the N corpus copies has distinct DEKs → each GET requires a fresh KV unwrap
- **Warm:** same file GETted N+1 times; first is discarded (fills DEK + file cache); N recorded

### Repetitions

N=10 per configuration (set `BENCH_REPETITIONS` in `.env` to override).

---

## Micro-benchmarks

Run independently from the R2 PUT/GET suite. Require local Postgres with migrations applied.

```bash
dotnet run -c Release -- unwrap      # Key unwrap latency
dotnet run -c Release -- decrypt     # Decryption latency
dotnet run -c Release -- auth        # RBAC permission lookup
dotnet run -c Release -- hierarchy   # Role hierarchy traversal
```

Results saved to `BenchmarkDotNet.Artifacts/results/`.

---

## Configuration

### .env

```env
# S3 backend (R2 or MinIO)
S3Settings__AccessKeyId=your-r2-key
S3Settings__SecretAccessKey=your-r2-secret
S3Settings__EndpointHostname=https://<account-id>.r2.cloudflarestorage.com
S3Settings__Region=auto

KeyVault__Provider=Azure
KeyVault__VaultUri=https://<your-vault>.vault.azure.net/
KeyVault__TenantId=
KeyVault__ClientId=
KeyVault__ClientSecret=

ConnectionStrings__OswsContext=Host=localhost;Port=5432;Database=osws_bench;Username=postgres;Password=postgres

# Local OSWS instance
OSWS_ENDPOINT=http://localhost:5000

# Benchmark user credentials (from seed-s3-credential output)
BENCH_OSWS_ACCESS_KEY=
BENCH_OSWS_SECRET_KEY=

# Settings
BENCH_BUCKET=osws-benchmark
BENCH_REPETITIONS=10
```

---

## Troubleshooting

| Error                                      | Fix                                                                     |
| ------------------------------------------ | ----------------------------------------------------------------------- |
| `BENCH_OSWS_ACCESS_KEY not found`          | Run `seed-s3-credential` and copy output to `.env`                      |
| OSWS health check failed                   | Start OSWS with correct config before running the script                |
| `No objects found` on GET                  | Run `generate-corpus` first                                             |
| Corpus upload fails on column registration | Ensure OSWS is running with encryption enabled during `generate-corpus` |
