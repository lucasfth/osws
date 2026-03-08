using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using OSWS.Models.Interfaces;
using OSWS.ParquetSolver;
using OSWS.Performance.Benchmarks.DatasetGenerators;
using OSWS.Performance.Benchmarks.Fixtures;
using OSWS.Performance.Benchmarks.Helpers;

namespace OSWS.Performance.Benchmarks.Measurements;

/// <summary>
/// Measurement 3: Per-Column Decryption Latency Analysis
/// Measures how decryption latency is distributed across columns in the wide dataset.
/// Shows if early columns have different latency than late columns due to cache effects.
/// Uses cold start (no cache) to isolate per-column Azure KV decrypt time.
/// </summary>
[Config(typeof(SharedBenchmarkConfig))]
public class Measurement3FullDecryptionThroughputBenchmark
{
    private ServiceProvider? _services;
    private IKeyVaultProvider? _keyVaultProvider;
    private ColdStartFixture? _fixture;
    private Stream? _wideEncrypted;
    private readonly MetricsCollector _metrics = new();
    private ParquetWriter? _parquetWriter;

    // Accumulate results across all iterations to write once in GlobalCleanup
    private readonly Dictionary<string, PerformanceMetrics> _accumulatedResults = new();

    [GlobalSetup]
    public async Task SetupAsync()
    {
        _services = BenchmarkServiceFactory.BuildServiceProvider();
        _keyVaultProvider = _services.GetRequiredService<IKeyVaultProvider>();
        _fixture = new ColdStartFixture();
        _parquetWriter = new ParquetWriter(_keyVaultProvider, "Internal");

        // Generate the wide dataset once - 2000 columns provides sufficient granularity
        var unencrypted = await WideDatasetGenerator.GenerateAsync(
            2000,
            10000,
            CancellationToken.None
        );
        _wideEncrypted = await _parquetWriter.WriteParquetAsync(unencrypted, "default");
    }

    [IterationSetup]
    public void IterationSetup()
    {
        // Cold start: clear caches before each iteration
        _fixture?.ClearCachesAsync().GetAwaiter().GetResult();
    }

    [Benchmark(Description = "Per-Column Latency - Wide Dataset (2000 cols × 10K rows)")]
    public async Task PerColumnLatency_WideDataset()
    {
        if (_wideEncrypted == null || _keyVaultProvider == null || _fixture == null)
            throw new InvalidOperationException("Benchmark not properly initialized");

        _metrics.StartMeasurement();

        _wideEncrypted.Position = 0;

        // Create reader with latency tracking - will record latency for each column
        var reader = new ParquetReader(
            _keyVaultProvider,
            _fixture.DekCache,
            (latency) =>
                _metrics.RecordOperationLatency(
                    $"Measurement3_PerColumnLatency_WideDataset_Decrypt",
                    latency
                )
        );

        var decrypted = await reader.ReadParquetAsync(_wideEncrypted);

        _metrics.StopMeasurement();
        var metrics = _metrics.GetMetrics();

        // Store for later writing in GlobalCleanup
        var resultKey = $"Measurement3_PerColumnLatency_WideDataset_{DateTime.UtcNow.Ticks}";
        _accumulatedResults[resultKey] = metrics;

        _metrics.Reset();
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        // Write all accumulated results to CSV once at the end
        foreach (var (resultKey, metrics) in _accumulatedResults)
        {
            ResultsRecorder.Record(resultKey, metrics);
        }

        _wideEncrypted?.Dispose();
        _fixture?.Dispose();
        _services?.Dispose();
    }
}
