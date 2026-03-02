# OSWS Performance Tests

This project contains comprehensive performance tests for the OSWS (Object Storage Web Service) system, testing both in-memory encryption/decryption performance and real-world S3/R2 integration scenarios.

## Benchmark Types

### 📦 In-Memory Benchmarks (Measurements 1-4)

Test pure crypto performance without network/storage overhead:

- Encryption/decryption throughput
- DEK cache effectiveness
- Memory usage patterns

### 🌐 S3/R2 Integration Benchmarks (Measurements 5-6)

Test real-world scenarios with actual storage:

- Direct S3 vs OSWS pipeline comparison
- Cache effectiveness with S3 backend
- Network latency impact

## Test Structure

### Dataset Generators

Located in `DatasetGenerators/`:

- **WideDatasetGenerator**: Creates parquet files with 2,000 columns × 10,000 rows (~150MB)
  - Stresses footer parsing and key retrieval with many columns
  - Each column requires unique KEK from Azure Key Vault

- **DeepDatasetGenerator**: Creates parquet files with 10 columns × 10,000,000 rows (~500MB)
  - Stresses cryptographic operations on large data volumes
  - Tests sustained decryption throughput

- **SmallDatasetGenerator**: Creates parquet files with 5 columns × 5,000 rows (~1MB)
  - Stresses request overhead and key retrieval latency
  - Used for cache eviction stress tests

### Test Fixtures

Located in `Fixtures/`:

- **ColdStartFixture**: Clears all caches before tests to simulate first-time access
- **WarmStartFixture**: Pre-populates caches to simulate repeated access patterns

### Measurement Tests

Located in `Measurements/`:

#### 📦 In-Memory Crypto Benchmarks

#### Measurement 1: Cold Start Wide Dataset Range Request

- Tests footer parsing overhead with 2,000 column parquet file
- Simulates client requesting final 2MB via byte-range
- Fully in-memory (no S3 I/O)
- **File**: `Measurement1ColdWideRangeRequest.cs`

#### Measurement 2: Warm DEK Cache Column Selection

- Tests I/O efficiency with deep dataset (10 cols × 100K rows)
- DEK cache is warm, file cache is cold
- Fully in-memory (no S3 I/O)
- **File**: `Measurement2WarmDekColumnSelect.cs`

#### Measurement 3: Full Decryption Throughput

- Tests maximum decryption throughput with warm caches
- Runs on 1, 4, and 8 CPU cores to verify linear scaling
- Measures throughput in MB/s
- Fully in-memory (no S3 I/O)
- **File**: `Measurement3FullDecryptionThroughput.cs`

#### Measurement 4: DEK Cache Stress Test

- Reads 100 distinct parquet files in parallel
- Forces cache eviction and KEK unwrapping
- Tests concurrent cache access patterns
- Fully in-memory (no S3 I/O)
- **File**: `Measurement4DekCacheStressTest.cs`

#### 🌐 S3/R2 Integration Benchmarks

#### Measurement 5: Direct S3 vs OSWS Pipeline

- **Baseline**: Direct S3 download (no decryption)
- **OSWS**: S3 download + decryption pipeline
- Measures encryption/decryption overhead in real-world scenarios
- Uses wide dataset (2000 cols × 10K rows)
- **File**: `Measurement5S3DirectVsOSWSBenchmark.cs`

#### Measurement 6: S3 Cache Effectiveness

- **First access**: Cold cache (S3 download + decrypt)
- **Second access**: Warm cache (cache hit, no S3 download)
- Shows cache benefit: reduced S3 API calls and latency
- Uses deep dataset (10 cols × 100K rows)
- **File**: `Measurement6S3CacheEffectivenessBenchmark.cs`
**For All Benchmarks:**

  - Configure Key Vault in `appsettings.json` (defaults to Internal provider)
  - Ensure sufficient disk space for dataset generation

2. **For S3/R2 Benchmarks (Measurements 5-6):**
   - Configure S3/R2 credentials in `appsettings.json`
   - Ensure bucket exists or create it (will be auto-created)
   - Benchmark requires network access to S3/R2 endpoint
Benchmarks use a generic `DispatchProxy`‑based decorator to wrap both the
`IKeyVaultProvider` and, when needed, the `IAmazonS3` client.  The proxy
records each call’s latency and increments counters in `MetricsCollector`.
This lets measurements capture Azure Key Vault and S3 interaction costs
without littering the test methods.

All tests collect comprehensive metrics via `MetricsCollector`:

- **Latency**: Total elapsed time, average per operation
- **Memory**: Initial, peak, and increase during operation
- **Azure Key Vault**: Call count, average latency, total latency
- **S3**: Call count, average latency, total latency
- **Throughput**: MB/s for data operations
- **Cache**: Hit rate, entry count, eviction behavior

## Running Tests

### Prerequisites

1. Configure Key Vault connection in `appsettings.json` (defaults to Internal provider)
2. Configure S3/R2 connection in `appsettings.json` (optional for benchmarking)
3. Ensure sufficient disk space for dataset generation

### Run All Benchmarks

```bash
cd OSWS.Performance.Benchmarks
dotnet run -c Release
``In-Memory Crypto Benchmarks
dotnet run -c Release -- 1  # Cold start wide range request
dotnet run -c Release -- 2  # Warm DEK column select
dotnet run -c Release -- 3  # Full decryption throughput
dotnet run -c Release -- 4  # DEK cache stress test

# S3/R2 Integration Benchmarks (requires S3 config)
dotnet run -c Release -- 5  # Direct S3 vs OSWS pipeline
dotnet run -c Release -- 6  # S3 cache effectiveness
# Run Measurement 3 (full decryption throughput)
dotnet run -c Release -- 3

# Run Measurement 4 (DEK cache stress test)
dotnet run -c Release -- 4
```

Or set the environment variable:

```bash
BENCH_MEASUREMENT=1 dotnet run -c Release
```

### Control Iteration Count

```bash
# Set iterations via environment variable
BENCH_ITERATIONS=10 dotnet run -c Release

# Or pass to BenchmarkDotNet
dotnet run -c Release -- --iterationCount 10
```

### Output Files

Benchmark results are saved to:

- `BenchmarkDotNet.Artifacts/results/` - Detailed BenchmarkDotNet reports
- `benchmark-metrics.csv` - Custom metrics CSV for all measurements

## Important Notes

### BenchmarkDotNet

### Dataset Sizes

Default dataset sizes are **reduced for testing**:
- Wide datasets: 2,000 columns × 10,000 rows (~150MB)
- Deep datasets: 10 columns × 10,000,000 rows (~500MB)
- Small datasets: 5 columns × 5,000 rows (~1MB)

Adjust parameters in dataset generators for different test scenarios.

### Cache Configuration

Cache settings can be configured in `appsettings.json`:

```json
{
  "Cache": {"  // Auto: uses temp directory
  }
}
```

### S3/R2 Configuration

For Measurements 5-6, configure S3/R2 settings:

```json
{
  "S3Settings": {
    "AccessKeyId": "your-access-key",
    "SecretAccessKey": "your-secret-key",
**In-Memory Benchmarks (1-4):**
- **Measurement 1**: Footer parsing < 500ms for 2,000 columns
- **Measurement 2**: Warm DEK cache reduces latency by 80%+
- **Measurement 3**: Throughput scales linearly with CPU cores
- **Measurement 4**: Zero Azure KV 429 errors, < 1s average latency

**S3/R2 Benchmarks (5-6):**
- **Measurement 5**: OSWS overhead < 2x vs direct S3 download
- **Measurement 6**: Cache hit reduces latency by 90%+ (no S3 fetch)
  },
  "BenchmarkSettings": {
    "S3BucketName": "osws-benchmark-test",
    "CleanupAfterRun": true,
    "UseExistingFiles": false
  }
}
```

**Note for Cloudflare R2:**
- Use your R2 account subdomain as endpoint
- Set Region to "auto" or "us-east-1"
- Bucket will be auto-created if it doesn't exist
- Files are automatically cleaned up after benchmarks (configurable) "CacheDirectory": "/tmp/osws-cache"
  }
}
```

## Expected Results

### Performance Targets

- **Measurement 1**: Footer parsing < 500ms for 2,000 columns
- **Measurement 2**: Warm DEK cache reduces latency by 80%+
- **BenchmarkDotNet

These benchmarks use BenchmarkDotNet for accurate performance measurement:

- Multiple warmup iterations before measurement
- Statistical analysis of results
- Automatic outlier detection
- Memory diagnostics with `[MemoryDiagnoser]`

For detailed BenchmarkDotNet configuration, see `SharedBenchmarkConfig.cs`.

### Dataset Sizes

Default dataset sizes balance performance testing with execution time

### Benchmarks Take Too Long
x] S3/R2 integration benchmarks
- [x] Cache effectiveness measurements
- [ ] Implement column-specific byte-range optimization
- [ ] Add multi-core parallelism in ParquetSharp operations
- [ ] Add metrics export to Prometheus/Grafana
- [ ] Add automated performance regression detection
- [ ] Add benchmarks for different file sizes (1MB, 10MB, 100MB, 1GB)
- [ ] Add benchmarks for different S3 regions/endpoints
3. Run individual benchmarks instead of all at once

### Out of Memory

For large datasets:

1. Reduce dataset size parameters (especially row count)
2. Increase available system memory
3. Monitor memory usage in BenchmarkDotNet results

### Azure KV Rate Limiting

If encountering 429 errors:

1. Reduce parallel request count in Measurement 4
2. Increase delay between requests
3. Consider using Azure KV Premium tier

### Out of Memory

For large datasets:

1. Reduce dataset size parameters
2. Increase available system memory
3. Run tests sequentially instead of parallel

## Future Enhancements

- [ ] Implement column-specific byte-range optimization
- [ ] Add multi-core parallelism in ParquetSharp operations
- [ ] Add metrics export to Prometheus/Grafana
- [ ] Implement direct S3 baseline comparison tests
- [ ] Add automated performance regression detection
