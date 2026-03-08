using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using OSWS.Models.Interfaces;
using OSWS.ParquetSolver;
using OSWS.ParquetSolver.Helpers;
using OSWS.Performance.Benchmarks.DatasetGenerators;
using OSWS.Performance.Benchmarks.Fixtures;
using OSWS.Performance.Benchmarks.Helpers;

namespace OSWS.Performance.Benchmarks.Measurements;

/// <summary>
/// Measurement 4: Footer Decryption Latency and Cache Stress Analysis
/// Measures footer decryption latency (the first key retrieval operation) across dataset types.
/// Also tests DEK cache stress with 100 distinct small files to understand cache eviction impact.
/// Cold start shows footer Azure KV latency; warm start shows cached footer performance.
/// </summary>
[Config(typeof(SharedBenchmarkConfig))]
public class Measurement4DekCacheStressTestBenchmark
{
    private ServiceProvider? _services;
    private IKeyVaultProvider? _keyVaultProvider;
    private ColdStartFixture? _coldFixture;
    private WarmStartFixture? _warmFixture;

    private Stream? _smallEncrypted;
    private Stream? _wideEncrypted;
    private Stream? _deepEncrypted;
    private readonly List<Stream> _stressTestFiles = [];

    private readonly MetricsCollector _metrics = new();
    private ParquetWriter? _parquetWriter;

    // Accumulate results across all iterations to write once in GlobalCleanup
    private readonly Dictionary<string, PerformanceMetrics> _accumulatedResults = new();

    [GlobalSetup]
    public async Task SetupAsync()
    {
        _services = BenchmarkServiceFactory.BuildServiceProvider();
        _keyVaultProvider = _services.GetRequiredService<IKeyVaultProvider>();
        _coldFixture = new ColdStartFixture();
        _warmFixture = new WarmStartFixture();
        _parquetWriter = new ParquetWriter(_keyVaultProvider, "Internal");

        // Generate and encrypt dataset types for footer latency tests
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

        // Prepare 100 distinct small files for stress test (one per role to create multiple cache entries)
        for (var i = 0; i < 100; i++)
        {
            var data = await SmallDatasetGenerator.GenerateAsync(5, 5000, CancellationToken.None);
            var encrypted = await _parquetWriter.WriteParquetAsync(data, $"default-{i}");
            _stressTestFiles.Add(encrypted);
        }

        // Warm up the warm fixture caches
        if (_warmFixture != null)
        {
            await WarmupCache(_smallEncrypted, _warmFixture.DekCache, "small");
            await WarmupCache(_wideEncrypted, _warmFixture.DekCache, "wide");
            await WarmupCache(_deepEncrypted, _warmFixture.DekCache, "deep");
        }
    }

    private async Task WarmupCache(Stream? encryptedStream, DekCache cache, string name)
    {
        if (encryptedStream == null || _keyVaultProvider == null)
            return;

        encryptedStream.Position = 0;
        var reader = new ParquetReader(_keyVaultProvider, cache);
        var decrypted = await reader.ReadParquetAsync(encryptedStream);
        var buffer = new byte[8192];
        while (await decrypted.ReadAsync(buffer) > 0) { }
    }

    [Benchmark(Description = "Footer Latency - Cold Start - Small Dataset")]
    public async Task FooterLatency_ColdStart_SmallDataset()
    {
        if (_coldFixture == null || _keyVaultProvider == null || _smallEncrypted == null)
            throw new InvalidOperationException("Benchmark not properly initialized");

        await _coldFixture.ClearCachesAsync();
        _metrics.StartMeasurement();

        _smallEncrypted.Position = 0;
        var reader = new ParquetReader(
            _keyVaultProvider,
            _coldFixture.DekCache,
            (latency) =>
                _metrics.RecordOperationLatency(
                    "Measurement4_FooterLatency_Small_Cold_Decrypt",
                    latency
                )
        );

        var decrypted = await reader.ReadParquetAsync(_smallEncrypted);

        _metrics.StopMeasurement();
        var metrics = _metrics.GetMetrics();
        _accumulatedResults[$"Measurement4_FooterLatency_Small_Cold_{DateTime.UtcNow.Ticks}"] =
            metrics;
        _metrics.Reset();
    }

    [Benchmark(Description = "Footer Latency - Cold Start - Wide Dataset")]
    public async Task FooterLatency_ColdStart_WideDataset()
    {
        if (_coldFixture == null || _keyVaultProvider == null || _wideEncrypted == null)
            throw new InvalidOperationException("Benchmark not properly initialized");

        await _coldFixture.ClearCachesAsync();
        _metrics.StartMeasurement();

        _wideEncrypted.Position = 0;
        var reader = new ParquetReader(
            _keyVaultProvider,
            _coldFixture.DekCache,
            (latency) =>
                _metrics.RecordOperationLatency(
                    "Measurement4_FooterLatency_Wide_Cold_Decrypt",
                    latency
                )
        );

        var decrypted = await reader.ReadParquetAsync(_wideEncrypted);

        _metrics.StopMeasurement();
        var metrics = _metrics.GetMetrics();
        _accumulatedResults[$"Measurement4_FooterLatency_Wide_Cold_{DateTime.UtcNow.Ticks}"] =
            metrics;
        _metrics.Reset();
    }

    [Benchmark(Description = "Footer Latency - Cold Start - Deep Dataset")]
    public async Task FooterLatency_ColdStart_DeepDataset()
    {
        if (_coldFixture == null || _keyVaultProvider == null || _deepEncrypted == null)
            throw new InvalidOperationException("Benchmark not properly initialized");

        await _coldFixture.ClearCachesAsync();
        _metrics.StartMeasurement();

        _deepEncrypted.Position = 0;
        var reader = new ParquetReader(
            _keyVaultProvider,
            _coldFixture.DekCache,
            (latency) =>
                _metrics.RecordOperationLatency(
                    "Measurement4_FooterLatency_Deep_Cold_Decrypt",
                    latency
                )
        );

        var decrypted = await reader.ReadParquetAsync(_deepEncrypted);

        _metrics.StopMeasurement();
        var metrics = _metrics.GetMetrics();
        _accumulatedResults[$"Measurement4_FooterLatency_Deep_Cold_{DateTime.UtcNow.Ticks}"] =
            metrics;
        _metrics.Reset();
    }

    [Benchmark(Description = "Footer Latency - Warm Start - Small Dataset")]
    public async Task FooterLatency_WarmStart_SmallDataset()
    {
        if (_warmFixture == null || _keyVaultProvider == null || _smallEncrypted == null)
            throw new InvalidOperationException("Benchmark not properly initialized");

        _metrics.StartMeasurement();

        _smallEncrypted.Position = 0;
        var reader = new ParquetReader(
            _keyVaultProvider,
            _warmFixture.DekCache,
            (latency) =>
                _metrics.RecordOperationLatency(
                    "Measurement4_FooterLatency_Small_Warm_Decrypt",
                    latency
                )
        );

        var decrypted = await reader.ReadParquetAsync(_smallEncrypted);

        _metrics.StopMeasurement();
        var metrics = _metrics.GetMetrics();
        _accumulatedResults[$"Measurement4_FooterLatency_Small_Warm_{DateTime.UtcNow.Ticks}"] =
            metrics;
        _metrics.Reset();
    }

    [Benchmark(Description = "Footer Latency - Warm Start - Wide Dataset")]
    public async Task FooterLatency_WarmStart_WideDataset()
    {
        if (_warmFixture == null || _keyVaultProvider == null || _wideEncrypted == null)
            throw new InvalidOperationException("Benchmark not properly initialized");

        _metrics.StartMeasurement();

        _wideEncrypted.Position = 0;
        var reader = new ParquetReader(
            _keyVaultProvider,
            _warmFixture.DekCache,
            (latency) =>
                _metrics.RecordOperationLatency(
                    "Measurement4_FooterLatency_Wide_Warm_Decrypt",
                    latency
                )
        );

        var decrypted = await reader.ReadParquetAsync(_wideEncrypted);

        _metrics.StopMeasurement();
        var metrics = _metrics.GetMetrics();
        _accumulatedResults[$"Measurement4_FooterLatency_Wide_Warm_{DateTime.UtcNow.Ticks}"] =
            metrics;
        _metrics.Reset();
    }

    [Benchmark(Description = "Footer Latency - Warm Start - Deep Dataset")]
    public async Task FooterLatency_WarmStart_DeepDataset()
    {
        if (_warmFixture == null || _keyVaultProvider == null || _deepEncrypted == null)
            throw new InvalidOperationException("Benchmark not properly initialized");

        _metrics.StartMeasurement();

        _deepEncrypted.Position = 0;
        var reader = new ParquetReader(
            _keyVaultProvider,
            _warmFixture.DekCache,
            (latency) =>
                _metrics.RecordOperationLatency(
                    "Measurement4_FooterLatency_Deep_Warm_Decrypt",
                    latency
                )
        );

        var decrypted = await reader.ReadParquetAsync(_deepEncrypted);

        _metrics.StopMeasurement();
        var metrics = _metrics.GetMetrics();
        _accumulatedResults[$"Measurement4_FooterLatency_Deep_Warm_{DateTime.UtcNow.Ticks}"] =
            metrics;
        _metrics.Reset();
    }

    [Benchmark(Description = "Cache Stress - 100 Small Files Parallel (Cold Start)")]
    public async Task CacheStress_100SmallFiles_Parallel()
    {
        if (_keyVaultProvider == null || _coldFixture == null)
            throw new InvalidOperationException("Benchmark not properly initialized");

        await _coldFixture.ClearCachesAsync();
        _metrics.StartMeasurement();

        // Parallel reads with shared DEK cache to stress cache contention
        var tasks = _stressTestFiles
            .Select(stream =>
                Task.Run(async () =>
                {
                    stream.Position = 0;
                    var reader = new ParquetReader(
                        _keyVaultProvider,
                        _coldFixture.DekCache,
                        (latency) =>
                            _metrics.RecordOperationLatency(
                                "Measurement4_CacheStress_100Files_Parallel_Decrypt",
                                latency
                            )
                    );
                    var decrypted = await reader.ReadParquetAsync(stream);
                    var buffer = new byte[1024];
                    _ = await decrypted.ReadAsync(buffer);
                })
            )
            .ToArray();

        await Task.WhenAll(tasks);

        _metrics.StopMeasurement();
        var metrics = _metrics.GetMetrics();
        _accumulatedResults[$"Measurement4_CacheStress_100Files_Parallel_{DateTime.UtcNow.Ticks}"] =
            metrics;
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
        foreach (var stream in _stressTestFiles)
        {
            stream.Dispose();
        }
        _stressTestFiles.Clear();
        _coldFixture?.Dispose();
        _warmFixture?.Dispose();
        _services?.Dispose();
    }
}
