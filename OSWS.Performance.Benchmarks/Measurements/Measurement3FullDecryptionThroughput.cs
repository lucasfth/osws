using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Configuration;
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
public class Measurement3FullDecryptionThroughputBenchmark : ScenarioMeasurementBenchmarkBase
{
    private ServiceProvider? _services;
    private IKeyVaultProvider? _keyVaultProvider;
    private ColdStartFixture? _fixture;
    private byte[]? _wideEncryptedBytes;

    private readonly MetricsCollector _metrics = new();
    private ParquetWriter? _parquetWriter;

    [GlobalSetup]
    public async Task SetupAsync()
    {
        _services = BenchmarkServiceFactory.BuildServiceProvider();
        var config = _services.GetRequiredService<IConfiguration>();
        _keyVaultProvider = _services.GetRequiredService<IKeyVaultProvider>();
        _fixture = new ColdStartFixture();
        var providerType = config.GetValue<string>("KeyVault:Provider") ?? "Internal";
        _parquetWriter = new ParquetWriter(_keyVaultProvider, providerType);

        // Generate the wide dataset once - 2000 columns provides sufficient granularity
        var unencrypted = await WideDatasetGenerator.GenerateAsync(
            2000,
            10000,
            CancellationToken.None
        );
        var (tempStream, _) = await _parquetWriter.WriteParquetAsync(unencrypted, "default");
        await using (tempStream)
        {
            _wideEncryptedBytes = ((MemoryStream)tempStream).ToArray();
        }
    }

    /// <summary>
    /// Creates a safe MemoryStream from encrypted bytes that won't be affected by GC relocation.
    /// </summary>
    private static MemoryStream CreateSafeStream(byte[] bytes)
    {
        return new MemoryStream(bytes, writable: false);
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
        _metrics.Reset();

        if (_wideEncryptedBytes == null || _keyVaultProvider == null || _fixture == null)
            throw new InvalidOperationException("Benchmark not properly initialized");

        const string scenarioKey = "PerColumnLatency_WideDataset";
        var measure = ShouldMeasure("Measurement3", scenarioKey);

        if (measure)
            _metrics.StartMeasurement();

        using var stream = CreateSafeStream(_wideEncryptedBytes);

        // Create reader with latency tracking - will record latency for each column
        var reader = new ParquetReader(
            _keyVaultProvider,
            _fixture.DekCache,
            (latency) =>
            {
                _metrics.RecordKvCall(latency);
                _metrics.RecordOperationLatency(
                    "Measurement3_PerColumnLatency_WideDataset_Decrypt",
                    latency
                );
            },
            latency => _metrics.RecordCachedKvCall(latency)
        );

        _ = await reader.ReadParquetAsync(stream);

        RecordIfMeasured(
            scenarioKey,
            "Measurement3_PerColumnLatency_WideDataset",
            _metrics,
            measure
        );
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        FlushRecordedResults();

        _fixture?.Dispose();
        _services?.Dispose();
    }
}
