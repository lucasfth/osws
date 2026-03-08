using System.ComponentModel;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using OSWS.Models.Interfaces;
using OSWS.ParquetSolver;
using OSWS.Performance.Benchmarks.DatasetGenerators;
using OSWS.Performance.Benchmarks.Fixtures;
using OSWS.Performance.Benchmarks.Helpers;

namespace OSWS.Performance.Benchmarks.Measurements;

/// <summary>
/// Measurement 2: Warm Start Latency Breakdown
/// Measures decryption latency with pre-populated caches for Small, Wide, and Deep datasets.
/// Compares against cold start (Measurement 1) to quantify cache mitigation impact.
/// Shows that warm caches eliminate Azure KV latency and accelerate footer + column decryption.
/// </summary>
[Config(typeof(SharedBenchmarkConfig))]
public class Measurement2WarmDekColumnSelectBenchmark
{
    private ServiceProvider? _services;
    private IKeyVaultProvider? _keyVaultProvider;
    private WarmStartFixture? _fixture;

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
        _fixture = new WarmStartFixture();
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

        // Warm up the DEK cache by doing one full read of each dataset
        await WarmupCache(_smallEncrypted, "small");
        await WarmupCache(_wideEncrypted, "wide");
        await WarmupCache(_deepEncrypted, "deep");
    }

    private async Task WarmupCache(Stream? encryptedStream, string name)
    {
        if (encryptedStream == null || _keyVaultProvider == null || _fixture == null)
            return;

        encryptedStream.Position = 0;
        var reader = new ParquetReader(_keyVaultProvider, _fixture.DekCache);
        var decrypted = await reader.ReadParquetAsync(encryptedStream);
        // Consume the stream to trigger all key retrievals
        var buffer = new byte[8192];
        while (await decrypted.ReadAsync(buffer) > 0) { }
    }

    [Benchmark(Description = "Warm Start - Small Dataset (5 cols × 5K rows)")]
    public async Task WarmStart_SmallDataset()
    {
        await RunWarmStartBenchmark(_smallEncrypted, "SmallDataset");
    }

    [Benchmark(Description = "Warm Start - Wide Dataset (2000 cols × 10K rows)")]
    public async Task WarmStart_WideDataset()
    {
        await RunWarmStartBenchmark(_wideEncrypted, "WideDataset");
    }

    [Benchmark(Description = "Warm Start - Deep Dataset (10 cols × 10M rows)")]
    public async Task WarmStart_DeepDataset()
    {
        await RunWarmStartBenchmark(_deepEncrypted, "DeepDataset");
    }

    private async Task RunWarmStartBenchmark(Stream? encryptedStream, string datasetType)
    {
        if (encryptedStream == null || _keyVaultProvider == null || _fixture == null)
            throw new InvalidOperationException("Benchmark not properly initialized");

        _metrics.StartMeasurement();

        encryptedStream.Position = 0;

        // Create reader with latency tracking callback - should see cache hits
        var reader = new ParquetReader(
            _keyVaultProvider,
            _fixture.DekCache,
            (latency) =>
                _metrics.RecordOperationLatency(
                    $"Measurement2_WarmStart_Decrypt_{datasetType}",
                    latency
                )
        );

        var decrypted = await reader.ReadParquetAsync(encryptedStream);

        _metrics.StopMeasurement();
        var metrics = _metrics.GetMetrics();

        // Store for later writing in GlobalCleanup
        var resultKey = $"Measurement2_WarmStart_{datasetType}_{DateTime.UtcNow.Ticks}";
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
