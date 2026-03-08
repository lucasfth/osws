using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using OSWS.Models.Interfaces;
using OSWS.ParquetSolver;
using OSWS.Performance.Benchmarks.DatasetGenerators;
using OSWS.Performance.Benchmarks.Fixtures;
using OSWS.Performance.Benchmarks.Helpers;

namespace OSWS.Performance.Benchmarks.Measurements;

/// <summary>
/// Measurement 1: Cold Start Latency Breakdown
/// Measures decryption latency with empty caches for Small, Wide, and Deep datasets.
/// Captures per-operation latencies (footer, columns) to understand which operations are bottlenecks.
/// </summary>
[Config(typeof(SharedBenchmarkConfig))]
public class Measurement1ColdWideRangeRequestBenchmark
{
    private ServiceProvider? _services;
    private IKeyVaultProvider? _keyVaultProvider;
    private ColdStartFixture? _fixture;

    private Stream? _smallEncrypted;
    private Stream? _wideEncrypted;
    private Stream? _deepEncrypted;

    private readonly MetricsCollector _metrics = new();
    private ParquetWriter? _parquetWriter;

    // Accumulate results across all iterations to write once in GlobalCleanup
    private readonly Dictionary<string, PerformanceMetrics> _accumulatedResults = new();

    [GlobalSetup]
    public async Task GlobalSetupAsync()
    {
        _services = BenchmarkServiceFactory.BuildServiceProvider();
        _keyVaultProvider = _services.GetRequiredService<IKeyVaultProvider>();
        _fixture = new ColdStartFixture();
        _parquetWriter = new ParquetWriter(_keyVaultProvider, "Internal");

        // Generate and encrypt three dataset types once, reused for all iterations
        var smallUnencrypted = await SmallDatasetGenerator.GenerateAsync(
            5,
            5000,
            CancellationToken.None
        );
        _smallEncrypted = await _parquetWriter.WriteParquetAsync(smallUnencrypted, "default");

        var wideUnencrypted = await WideDatasetGenerator.GenerateAsync(
            2000,
            10000,
            CancellationToken.None
        );
        _wideEncrypted = await _parquetWriter.WriteParquetAsync(wideUnencrypted, "default");

        var deepUnencrypted = await DeepDatasetGenerator.GenerateAsync(
            10,
            10_000_000,
            CancellationToken.None
        );
        _deepEncrypted = await _parquetWriter.WriteParquetAsync(deepUnencrypted, "default");
    }

    [IterationSetup]
    public void IterationSetup()
    {
        // Cold start: clear all caches before each iteration
        _fixture?.ClearCachesAsync().GetAwaiter().GetResult();
    }

    [Benchmark(Description = "Cold Start - Small Dataset (5 cols × 5K rows)")]
    public async Task ColdStart_SmallDataset()
    {
        await RunColdStartBenchmark(_smallEncrypted, "SmallDataset");
    }

    [Benchmark(Description = "Cold Start - Wide Dataset (2000 cols × 10K rows)")]
    public async Task ColdStart_WideDataset()
    {
        await RunColdStartBenchmark(_wideEncrypted, "WideDataset");
    }

    [Benchmark(Description = "Cold Start - Deep Dataset (10 cols × 10M rows)")]
    public async Task ColdStart_DeepDataset()
    {
        await RunColdStartBenchmark(_deepEncrypted, "DeepDataset");
    }

    private async Task RunColdStartBenchmark(Stream? encryptedStream, string datasetType)
    {
        if (encryptedStream == null || _keyVaultProvider == null || _fixture == null)
            throw new InvalidOperationException("Benchmark not properly initialized");

        _metrics.StartMeasurement();

        encryptedStream.Position = 0;

        // Create reader with latency tracking callback
        var reader = new ParquetReader(
            _keyVaultProvider,
            _fixture.DekCache,
            (latency) =>
                _metrics.RecordOperationLatency(
                    $"Measurement1_ColdStart_Decrypt_{datasetType}",
                    latency
                )
        );
        _ = await reader.ReadParquetAsync(encryptedStream);

        _metrics.StopMeasurement();
        var metrics = _metrics.GetMetrics();

        // Store for later writing in GlobalCleanup
        var resultKey = $"Measurement1_ColdStart_{datasetType}_{DateTime.UtcNow.Ticks}";
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

        _smallEncrypted?.Dispose();
        _wideEncrypted?.Dispose();
        _deepEncrypted?.Dispose();
        _fixture?.Dispose();
        _services?.Dispose();
    }
}
