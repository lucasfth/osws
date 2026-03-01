# OSWS Performance Tests

This project contains comprehensive performance tests for the OSWS (Object Storage Web Service) system, specifically testing encryption/decryption performance, cache behavior, and Azure Key Vault integration.

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

#### Measurement 1: Cold Start Wide Dataset Range Request
- Tests footer parsing overhead with 2,000 column parquet file
- Simulates client requesting final 2MB via byte-range
- Compares OSWS latency vs direct S3 access baseline
- **File**: `Measurement1ColdWideRangeRequest.cs`

#### Measurement 2: Warm DEK Cache Column Selection
- Tests I/O efficiency with deep dataset (10 cols × 10M rows)
- DEK cache is warm, file cache is cold
- Requests specific columns (1 and 10) to test selective decryption
- **File**: `Measurement2WarmDekColumnSelect.cs`

#### Measurement 3: Full Decryption Throughput
- Tests maximum decryption throughput with warm caches
- Runs on 1, 4, and 8 CPU cores to verify linear scaling
- Measures throughput in MB/s
- **File**: `Measurement3FullDecryptionThroughput.cs`

#### Measurement 4: DEK Cache Stress Test
- Reads 100 distinct parquet files in parallel
- Forces cache eviction and KEK unwrapping
- Success criteria: No Azure KV 429 errors, latency < 1s per file
- **File**: `Measurement4DekCacheStressTest.cs`

## Collected Metrics

All tests collect comprehensive metrics via `MetricsCollector`:

- **Latency**: Total elapsed time, average per operation
- **Memory**: Initial, peak, and increase during operation
- **Azure Key Vault**: Call count, average latency, total latency
- **S3**: Call count, average latency, total latency
- **Throughput**: MB/s for data operations
- **Cache**: Hit rate, entry count, eviction behavior

## Running Tests

### Prerequisites

1. Configure Azure Key Vault connection in `appsettings.json`
2. Configure S3/R2 connection in `appsettings.json`
3. Ensure sufficient disk space for dataset generation

### Run All Tests

```bash
dotnet test OSWS.Performance.Tests/OSWS.Performance.Tests.csproj
```

### Run Specific Measurement

```bash
# Run Measurement 1
dotnet test --filter "FullyQualifiedName~Measurement1"

# Run Measurement 2
dotnet test --filter "FullyQualifiedName~Measurement2"

# Run Measurement 3
dotnet test --filter "FullyQualifiedName~Measurement3"

# Run Measurement 4
dotnet test --filter "FullyQualifiedName~Measurement4"
```

### Run with Detailed Output

```bash
dotnet test OSWS.Performance.Tests/OSWS.Performance.Tests.csproj -v detailed
```

## Important Notes

### Test Status

Most tests are currently **skipped by default** (marked with `Skip` attribute) because they require:
- Azure Key Vault setup and credentials
- S3/Cloudflare R2 bucket configuration
- Potentially long execution times

To run these tests:
1. Remove the `Skip` attribute from test methods
2. Ensure proper configuration in `appsettings.json`
3. Run tests with appropriate timeouts

### Dataset Sizes

Default dataset sizes are **reduced for testing**:
- Deep datasets use 100,000 rows instead of 10,000,000 for faster iteration
- Adjust parameters in test methods for full-scale performance testing

### Cache Configuration

Cache settings can be configured in `appsettings.json`:

```json
{
  "Cache": {
    "EnableFileCache": true,
    "MaxCacheSizeBytes": 10737418240,  // 10GB
    "CacheDirectory": "/tmp/osws-cache"
  }
}
```

## Expected Results

### Performance Targets

- **Measurement 1**: Footer parsing < 500ms for 2,000 columns
- **Measurement 2**: Warm DEK cache reduces latency by 80%+
- **Measurement 3**: Throughput scales linearly with CPU cores
- **Measurement 4**: Zero Azure KV 429 errors, < 1s average latency

### Comparison to Direct S3

OSWS adds overhead for:
- Parquet decryption
- DEK unwrapping (first access)
- Footer parsing

OSWS reduces overhead on repeat access:
- Cached DEKs eliminate Azure KV calls
- Cached encrypted files eliminate S3 calls

## Troubleshooting

### Tests Time Out

Increase test timeout or reduce dataset sizes:

```csharp
[Fact(Timeout = 300000)] // 5 minutes
public async Task MyTest() { ... }
```

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
