# OSWS Performance Benchmarks

Measures OSWS latency using direct AWS SDK calls (throughput suite) and BenchmarkDotNet (micro-benchmarks).

## Quick Start

```bash
cd OSWS.Performance.Benchmarks
cp .env.example .env
# Fill in S3 credentials (R2) and OSWS credentials (see below)
```

### 1. Seed benchmark user and bucket

```bash
# Create benchmark user + role, prints credentials — copy to .env
dotnet run -c Release -- seed-s3-credential --user-name bench-user --role-name bench-role

# Ensure benchmark bucket exists (via OSWS)
dotnet run -c Release -- ensure-bucket \
  --endpoint http://localhost:5000 \
  --access-key <BENCH_OSWS_ACCESS_KEY> \
  --secret-key <BENCH_OSWS_SECRET_KEY> \
  --bucket osws-benchmark
```

### 2. Generate and upload corpus

OSWS must be running with **encryption enabled** for corpus upload (so column permissions are registered).

```bash
dotnet run -c Release -- generate-corpus
```

This uploads:
- `bench/s3-direct/{size}.parquet` — plaintext, directly to R2
- `bench/osws/warm/{size}.parquet` — through OSWS (encrypted)
- `bench/osws/cold/{size}/{001..010}.parquet` — 10 cold copies through OSWS (distinct DEKs)

### 3. Run benchmarks

Each configuration requires OSWS started with the correct env vars. Run one invocation per config:

```bash
# 1. No OSWS — direct R2 baseline
python Infrastructure/run-benchmark.py --config s3-direct

# 2. Start OSWS: Encryption__DisableEncryption=false, Cache__EnableFileCache=true
python Infrastructure/run-benchmark.py --config osws-encrypt-cache

# 3. Restart OSWS: Encryption__DisableEncryption=false, Cache__EnableFileCache=false
python Infrastructure/run-benchmark.py --config osws-encrypt-no-cache

# 4. Restart OSWS: Encryption__DisableEncryption=true, Cache__EnableFileCache=false
python Infrastructure/run-benchmark.py --config osws-no-encrypt
```

Each run outputs `benchmark-results/results_<config>_<timestamp>.csv`.

### 4. Analyse results

```bash
python Infrastructure/analyse-results.py benchmark-results/
```

Prints mean / stddev / p50 / p95 per (config, operation, cache state, file size).

---

## Benchmark Design

### File corpus (100 columns of random doubles)

| Label  | Row count | Approx size |
|--------|-----------|-------------|
| small  | 10,000    | ~9 MB       |
| medium | 250,000   | ~217 MB     |
| large  | 600,000   | ~488 MB     |
| xlarge | 1,250,000 | ~1,000 MB   |

### Configurations

| Config                  | Encryption | File cache | Description                        |
|-------------------------|------------|------------|------------------------------------|
| `s3-direct`             | N/A        | N/A        | Plaintext PUT/GET directly to R2   |
| `osws-encrypt-cache`    | Enabled    | Enabled    | Full service, warm cache           |
| `osws-encrypt-no-cache` | Enabled    | Disabled   | Full service, always cold          |
| `osws-no-encrypt`       | Disabled   | Disabled   | Proxy routing overhead only        |

### Cold vs warm GET

- **Cold:** each of the 10 corpus copies has a distinct DEK → each GET requires a fresh AKV unwrap
- **Warm:** same file GETted N+1 times; first is discarded (fills DEK + file cache); N recorded

### Repetitions

N=10 per configuration (set `BENCH_REPETITIONS` in `.env` to override).

---

## Micro-benchmarks

Run independently from the throughput suite. Require local Postgres with migrations applied.

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

| Error | Fix |
|-------|-----|
| `BENCH_OSWS_ACCESS_KEY not found` | Run `seed-s3-credential` and copy output to `.env` |
| OSWS health check failed | Start OSWS with correct config before running the script |
| `No objects found` on GET | Run `generate-corpus` first |
| Corpus upload fails on column registration | Ensure OSWS is running with encryption enabled during `generate-corpus` |
