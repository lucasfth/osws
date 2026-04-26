# OSWS Performance Benchmarks

This project measures OSWS performance across baseline and micro-benchmarks using MinIO's Warp and custom OSWS-integrated benchmarks.

## Quick Start

### For Warp Baseline Benchmarks (S3 Scaling)

```bash
# 1. Setup configuration
cd OSWS.Performance.Benchmarks
cp .env.example .env
# Edit .env with your S3 credentials

# 2. Run benchmarks
cd Infrastructure
./run-warp-baseline.sh              # Full suite (1, 2, 4, 8 instances)
./run-warp-baseline.sh 1 4 10       # 1 instance, 4 clients, 10 seconds
./run-warp-baseline.sh 4 32 90      # 4 instances, 32 clients, 90 seconds

# 3. View results
ls warp-results/
```

### For Micro-benchmarks (Component Latency)

```bash
cd OSWS.Performance.Benchmarks

# Run all benchmarks
dotnet run -c Release

# Run specific benchmark
dotnet run -c Release -- unwrap     # Key unwrap latency
dotnet run -c Release -- decrypt    # Decryption latency
dotnet run -c Release -- auth       # Authorization latency
dotnet run -c Release -- hierarchy  # Authorization latency (role hierarchy)
```

Auth benchmark setup (one-time, from repo root):

```bash
# Create a local database (use your local Postgres user)
psql -U <your-user> -c "CREATE DATABASE osws_dev;"

# Apply migrations
dotnet ef database update --project OSWS.KeyManager --startup-project OSWS.WebApi
```

Set `ConnectionStrings:OswsContext` for this project (appsettings.json or environment).
Example for OSWS.Performance.Benchmarks/appsettings.json:

```json
{
  "ConnectionStrings": {
    "OswsContext": "Host=localhost;Port=5432;Database=osws_dev;Username=<your-user>;Password=<your-password>"
  }
}
```

## Benchmark Categories

### 1. Warp Baseline Benchmarks (S3-Compatible)

Compare OSWS against S3 systems using MinIO's [Warp](https://github.com/minio/warp).

**What it measures:**

- IOPS and throughput performance
- Scaling from 1 to 8 instances
- Encryption and cache impact
- System architecture overhead

**Configurations:**

- S3 direct (baseline reference)
- OSWS without encryption
- OSWS with encryption (file cache disabled)
- OSWS with encryption + file cache enabled

**Scale:**

- 1, 2, 4, 8 OSWS instances
- Configurable concurrency (default: 16 clients)
- Configurable duration (default: 60 seconds)
- Mixed workload (gets, puts, deletes)

**Results:**

- Saved to `warp-results/` as compressed benchmark data files (`.json.zst`)
- Contains throughput, latency (p50, p90, p99), error rates

### 2. Micro-benchmarks (Real OSWS Operations)

Deep investigation of specific system components using **real OSWS operations**, not simulations.

| Benchmark         | Measures                   | Implementation             | Notes                               |
| ----------------- | -------------------------- | -------------------------- | ----------------------------------- |
| **Key Unwrap**    | DEK unwrap latency         | Cold cache + ParquetReader | Isolates key unwrap from decryption |
| **Decryption**    | Column decryption latency  | Warm cache + ParquetReader | Isolates decryption from key unwrap |
| **Permission Service** | RBAC authorization latency (flat roles) | PermissionServiceBenchmark | Uses seeded Postgres fixtures |
| **Permission Hierarchy** | RBAC authorization latency (role inheritance) | PermissionHierarchyBenchmark | Uses seeded Postgres fixtures |

**Key Unwrap Benchmark:**

- Generates 10-column × 100-row encrypted parquet file (~10 cold KV calls)
- Sweeps DEK sizes: 128, 192, 256 bits (valid AES key sizes)
- Uses **cold cache** to force key unwrapping on every iteration
- Measures time to read encrypted parquet (includes DEK unwrapping)
- Real operations via OSWS.ParquetReader and IKeyVaultProvider
- **Note:** Key unwrap time is dominated by cold cache KV calls, so results reflect overall system latency

**Decryption Benchmark:**

- Generates 2000-column × N-row encrypted parquet files (N = 5K, 10K, or 100K)
- Uses **warm cache** (pre-populated with DEKs) to isolate decryption
- Measures pure decryption time without key unwrap overhead
- Real operations via OSWS.ParquetReader

**Authorization Benchmark:**

- Measures authorization checks with 4, 64, 256 direct roles (flat hierarchy)
- Requires a local Postgres database with migrations applied
- Uses per-iteration fixture data seeded into the OSWS schema

**Authorization Hierarchy Benchmark:**

- Measures authorization checks across role inheritance depth (0, 4, 16, 64)
- Requires a local Postgres database with migrations applied
- Uses per-iteration fixture data seeded into the OSWS schema

## Supported Key Sizes

OSWS supports the following DEK (Data Encryption Key) sizes for AES encryption:

| Key Size (bits) | Key Size (bytes) | AES Variant |
| --------------- | ---------------- | ----------- |
| 128             | 16               | AES-128     |
| 192             | 24               | AES-192     |
| 256             | 32               | AES-256     |

These correspond to valid AES key sizes. The Key Unwrap benchmark sweeps all three sizes to measure performance impact of key size selection.

## Statistical Reliability Guidelines

To ensure statistically reliable benchmark results, the following iteration and warmup settings are used:

| Benchmark          | Iterations | Warmup Runs | Target Error Margin |
| ------------------ | ---------- | ----------- | ------------------- |
| KeyUnwrap          | 30         | 8           | < 5%                |
| Decryption (5K)    | 10         | 3           | < 3%                |
| Decryption (10K)   | 10         | 3           | < 3%                |
| Decryption (100K)  | 15         | 5           | < 5%                |
| Authorization      | 15         | 3           | < 2%                |

**Notes:**
- **Iterations**: Number of times the benchmark executes to collect timing samples
- **Warmup Runs**: Number of initial runs discarded to allow JIT compilation and cache warming
- **Target Error Margin**: Expected coefficient of variation (CV) for reliable results
- Authorization benchmarks use seeded database fixtures; point to a dedicated local DB

## Configuration

### .env File Setup (Recommended)

The easiest way to configure OSWS benchmarks is using the `.env` file for secure credential management:

```bash
# Copy the template
cp .env.example .env

# Edit .env with your S3 backend credentials
S3Settings__AccessKeyId=your-access-key
S3Settings__SecretAccessKey=your-secret-key
S3Settings__EndpointHostname=https://your-s3-endpoint.com
S3Settings__Region=auto

# Optional: Benchmark settings
OSWS_BASE_PORT=8000
WARP_CONCURRENCY=16
WARP_DURATION_SECONDS=60
Encryption__DisableEncryption=false
WARP_INSECURE_TLS=false
```

Notes:

- Direct `S3` baseline uses your configured `S3Settings__EndpointHostname`.
- If the endpoint starts with `https://`, the benchmark script automatically enables Warp TLS mode.
- Set `WARP_INSECURE_TLS=true` only for self-signed certificates in local/test setups.
- For `OSWS` baseline categories, the script uses Warp `--disable-sha256-payload` to avoid aws-chunked payload framing mismatches when proxying uploads through OSWS.

The `.env` file is **git-ignored** and will never be committed. Benefits:

- ✅ Keeps credentials local and secure
- ✅ Easy switching between S3 backends
- ✅ No environment variable pollution
- ✅ Works across local/CI/CD environments

### S3 Backend Options

**AWS S3:**

```env
S3Settings__AccessKeyId=AKIA...
S3Settings__SecretAccessKey=...
S3Settings__EndpointHostname=https://s3.amazonaws.com
S3Settings__Region=us-east-1
```

**Cloudflare R2 (S3-compatible):**

```env
S3Settings__AccessKeyId=your-r2-access-key
S3Settings__SecretAccessKey=your-r2-secret-key
S3Settings__EndpointHostname=https://<account-id>.r2.cloudflarestorage.com
S3Settings__Region=auto
```

**MinIO (Local S3 Mock):**

```bash
# Start MinIO first
minio server ./data

# Then in .env:
S3Settings__AccessKeyId=minioadmin
S3Settings__SecretAccessKey=minioadmin
S3Settings__EndpointHostname=http://localhost:9000
S3Settings__Region=us-east-1
```

### Alternative: Environment Variables

You can set environment variables directly instead of using `.env`:

```bash
export S3Settings__AccessKeyId=your-key
export S3Settings__SecretAccessKey=your-secret
export S3Settings__EndpointHostname=https://endpoint.com
export S3Settings__Region=auto
./Infrastructure/run-warp-baseline.sh
```

The `.env` file takes precedence if both are set.

### appsettings.json

Additional settings in `OSWS.WebApi/appsettings.json`:

```json
{
  "Encryption": {
    "DisableEncryption": false // Set to true for baseline (no encryption overhead)
  },
  "WarpSettings": {
    "InstanceCounts": [1, 2, 4, 8],
    "Concurrency": 16,
    "DurationSeconds": 60,
    "WorkloadProfile": "mixed"
  }
}
```

### Baseline Categories Run By Script

`./Infrastructure/run-warp-baseline.sh` now runs all four baseline categories for each selected instance count (1, 2, 4, 8 by default):

1. `S3/R2 direct` (direct to configured backend)
2. `OSWS without encryption`
3. `OSWS with encryption` (file cache disabled)
4. `OSWS with encryption + caching` (file cache enabled)

This aligns outputs with category-level comparisons for IOPS and throughput.

## Warp Infrastructure

### Architecture

- **run-warp-baseline.sh** - Main orchestration script
  - Loads configuration from `.env`
  - Supports 1, 2, 4, 8 instances
  - Configurable concurrency, duration, workload
  - Automatic instance startup/shutdown

- **osws-start.sh** - Instance lifecycle management
  - Starts OSWS on configurable ports (8000, 8002, 8004, etc.)
  - Toggles encryption mode via environment
  - Loads `.env` configuration
  - Health checks before returning

- **osws-stop.sh** - Graceful shutdown
  - Stops all instances
  - Cleans up resources
  - PID-based process management

### Instance Ports

- Instance 1: Port 8000
- Instance 2: Port 8002 (+2)
- Instance 3: Port 8004 (+4)
- Instance 4: Port 8006 (+6)
- ... and so on

For multiple instances, a load balancer (nginx) can be configured to distribute traffic. Currently, Warp connects directly to individual instance ports.

### Prerequisites

- **Warp installed**: `brew install minio/stable/warp` (macOS)
  - Or download from: https://github.com/minio/warp
- **OSWS.WebApi buildable**: `dotnet build -c Release`
- **S3 Backend configured**: See Configuration section above

## Output Files

### Warp Baseline

- `warp-results/` - Warp benchmark output files
- Named as: `warp-<instances>instances-<category>.json.zst`
- Examples:
  - `warp-4instances-s3-direct.json.zst`
  - `warp-4instances-osws-no-encryption.json.zst`
  - `warp-4instances-osws-encryption-no-cache.json.zst`
  - `warp-4instances-osws-encryption-cache.json.zst`

Each result file contains:

- Throughput metrics (requests/sec, data/sec)
- Latency metrics (p50, p90, p99, p999 milliseconds)
- Error rates and failure analysis
- System info (CPU, memory, Go version)

### Micro-benchmarks

- `BenchmarkDotNet.Artifacts/results/` - BenchmarkDotNet HTML reports
- `benchmark-results.csv` - Raw metrics for charting

## Troubleshooting

### Setup & Configuration

- **".env file not found" warning**: Run `cp .env.example .env` and configure your S3 backend credentials
- **"No RegionEndpoint or ServiceURL configured"**: Verify S3Settings in .env or environment variables are set correctly
- **Warp not found**: Install from https://github.com/minio/warp

### Instance Management

- **Port conflicts**: Change OSWS_BASE_PORT in .env or appsettings.json
- **Instances won't start**: Check that OSWS.WebApi builds: `dotnet build -c Release`
- **Instances start but fail health check**: Review logs in `/tmp/osws-instance-*.log`
- **Address already in use**: Kill existing processes: `kill -9 <PID>` (find with: `ps aux | grep OSWS`)

### Benchmark Execution

- **Warp benchmark fails with 500 errors**:
  - Ensure S3 backend is configured and accessible
  - Test connectivity: `aws s3 ls` or `curl -I <endpoint>`
- **No results files**: Check `./warp-results/` directory and verify write permissions
- **Instances healthy but Warp fails**: Review S3 configuration and backend accessibility

### Micro-benchmarks

- **Build fails**: Ensure Release mode: `dotnet build -c Release`
- **KeyVault errors**: Set `Encryption__DisableEncryption=true` to skip key vault
- **Slow runs**: Reduce iterations: `BENCH_ITERATIONS=5 dotnet run -c Release`
- **Auth benchmarks fail on startup**: Ensure `ConnectionStrings:OswsContext` is set and migrations are applied

## Examples

### Run Quick Test

```bash
cd Infrastructure
./run-warp-baseline.sh 1 4 10
# 1 instance, 4 concurrent clients, 10 second duration
```

### Run Full Benchmark Suite

```bash
cd Infrastructure
./run-warp-baseline.sh
# Runs all instance counts: 1, 2, 4, 8
```

### Run Without Encryption (Baseline)

```bash
# Edit .env or appsettings.json
Encryption__DisableEncryption=true
cd Infrastructure
./run-warp-baseline.sh 1 16 60
# Results saved to warp-results/warp-1instances-osws-no-encryption.json.zst
```

### Run Micro-benchmark

```bash
cd OSWS.Performance.Benchmarks
dotnet run -c Release -- unwrap
```

## Architecture Overview

```
OSWS.Performance.Benchmarks/
├── Infrastructure/
│   ├── run-warp-baseline.sh       # Main orchestration script
│   ├── osws-start.sh              # Start instances
│   ├── osws-stop.sh               # Stop instances
│   └── warp-results/              # Benchmark results
├── Measurements/
│   ├── AuthorizationBenchmark.cs  # RBAC latency (PermissionServiceBenchmark)
│   ├── PermissionHierarchyBenchmark.cs  # RBAC hierarchy latency
│   ├── KeyUnwrapBenchmark.cs      # Key unwrap latency
│   └── DecryptionBenchmark.cs     # Decryption latency
├── Warp/
│   ├── WarpSettings.cs            # Configuration model
│   └── WarpOrchestrator.cs        # Orchestration logic
├── .env.example                   # Configuration template
├── .gitignore                     # Ignore .env with secrets
└── README.md                      # This file
```

## Next Steps

1. **Setup configuration**: `cp .env.example .env` and edit with S3 credentials
2. **Run Warp benchmarks**: `cd Infrastructure && ./run-warp-baseline.sh`
3. **Analyze results**: Review JSON files in `warp-results/`
4. **Compare modes**: Run with and without encryption to measure overhead
5. **Test scalability**: Compare results across 1, 2, 4, 8 instances

## Key Features

- ✅ Automatic multi-instance orchestration (1, 2, 4, 8 instances)
- ✅ Health checks before benchmarking
- ✅ Configurable ports, concurrency, duration
- ✅ Encryption toggle for baseline comparison
- ✅ Secure credential management (.env file)
- ✅ Works with any S3-compatible backend (AWS, R2, MinIO)
- ✅ Real micro-benchmarks (not synthetic)
- ✅ Full solution builds without errors
- ✅ Production-ready documentation

## Future Enhancements

- [ ] Add nginx load balancer configuration for proper request distribution
- [ ] Implement results parser and automated comparison reports
- [ ] Add RBAC implementation for Authorization benchmark
- [ ] Create visualization dashboard for benchmark results
- [ ] Add CI/CD integration for automated performance tracking
