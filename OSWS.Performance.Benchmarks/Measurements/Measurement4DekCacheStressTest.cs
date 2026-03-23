using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using OSWS.Models.Interfaces;
using OSWS.ParquetSolver;
using OSWS.ParquetSolver.Helpers;
using OSWS.Performance.Benchmarks.DatasetGenerators;
using OSWS.Performance.Benchmarks.Fixtures;
using OSWS.Performance.Benchmarks.Helpers;

namespace OSWS.Performance.Benchmarks.Measurements;

/// <summary>
/// Measurement 4: Initial Key Retrieval and Cache Stress Analysis
/// Measures initial key retrieval latency across dataset types.
/// Also tests DEK cache stress with 100 distinct small files to understand cache eviction impact.
/// Cold start shows external KV latency; warm start shows cached key retrieval performance.
/// </summary>
[Config(typeof(SharedBenchmarkConfig))]
public class Measurement4DekCacheStressTestBenchmark : ScenarioMeasurementBenchmarkBase
{
    private ServiceProvider? _services;
    private IKeyVaultProvider? _keyVaultProvider;
    private ColdStartFixture? _coldFixture;
    private WarmStartFixture? _warmFixture;

    private byte[]? _smallEncryptedBytes;
    private byte[]? _wideEncryptedBytes;
    private byte[]? _deepEncryptedBytes;
    private readonly List<byte[]> _stressTestFilesBytes = [];

    private readonly MetricsCollector _metrics = new();
    private ParquetWriter? _parquetWriter;
    private int _dekCacheCapacity = 2500;

    [GlobalSetup]
    public async Task SetupAsync()
    {
        _services = BenchmarkServiceFactory.BuildServiceProvider();
        var config = _services.GetRequiredService<IConfiguration>();
        _keyVaultProvider = _services.GetRequiredService<IKeyVaultProvider>();
        var providerType = config.GetValue<string>("KeyVault:Provider") ?? "Internal";

        if (
            int.TryParse(
                Environment.GetEnvironmentVariable("BENCH_DEK_CACHE_CAPACITY"),
                out var cap
            )
            && cap > 0
        )
        {
            _dekCacheCapacity = cap;
        }

        _coldFixture = new ColdStartFixture();
        _warmFixture = new WarmStartFixture(_dekCacheCapacity);
        _parquetWriter = new ParquetWriter(_keyVaultProvider, providerType);

        var smallCols = config.GetValue<int>("ParquetSizes:Small:Columns");
        var smallRows = config.GetValue<int>("ParquetSizes:Small:Rows");
        var wideCols = config.GetValue<int>("ParquetSizes:Wide:Columns");
        var wideRows = config.GetValue<int>("ParquetSizes:Wide:Rows");
        var deepCols = config.GetValue<int>("ParquetSizes:Deep:Columns");
        var deepRows = config.GetValue<int>("ParquetSizes:Deep:Rows");

        var stressFileCount = 100;
        if (
            int.TryParse(
                Environment.GetEnvironmentVariable("BENCH_STRESS_FILE_COUNT"),
                out var configuredStressCount
            )
            && configuredStressCount > 0
        )
        {
            stressFileCount = configuredStressCount;
        }

        // Generate and encrypt dataset types for initial key latency tests
        var smallUnencrypted = await SmallDatasetGenerator.GenerateAsync(
            smallCols,
            smallRows,
            CancellationToken.None
        );
        var (smallTemp, _) = await _parquetWriter.WriteParquetAsync(smallUnencrypted, "default");
        using (smallTemp)
        {
            _smallEncryptedBytes = ((MemoryStream)smallTemp).ToArray();
        }

        var wideUnencrypted = await WideDatasetGenerator.GenerateAsync(
            wideCols,
            wideRows,
            CancellationToken.None
        );
        var (wideTemp, _) = await _parquetWriter.WriteParquetAsync(wideUnencrypted, "default");
        using (wideTemp)
        {
            _wideEncryptedBytes = ((MemoryStream)wideTemp).ToArray();
        }

        var deepUnencrypted = await DeepDatasetGenerator.GenerateAsync(
            deepCols,
            deepRows,
            CancellationToken.None
        );
        var (deepTemp, _) = await _parquetWriter.WriteParquetAsync(deepUnencrypted, "default");
        using (deepTemp)
        {
            _deepEncryptedBytes = ((MemoryStream)deepTemp).ToArray();
        }

        // Prepare distinct small files for stress test (one per role to create multiple cache entries)
        for (var i = 0; i < stressFileCount; i++)
        {
            var data = await SmallDatasetGenerator.GenerateAsync(
                smallCols,
                smallRows,
                CancellationToken.None
            );
            var (encrypted, _) = await _parquetWriter.WriteParquetAsync(data, $"default-{i}");
            using (encrypted)
            {
                _stressTestFilesBytes.Add(((MemoryStream)encrypted).ToArray());
            }
        }

        // Warm up the warm fixture caches
        if (_warmFixture != null)
        {
            await WarmupCache(_smallEncryptedBytes, _warmFixture.DekCache, "small");
            await WarmupCache(_wideEncryptedBytes, _warmFixture.DekCache, "wide");
            await WarmupCache(_deepEncryptedBytes, _warmFixture.DekCache, "deep");
        }

        Console.WriteLine(
            $"[Measurement4] Setup complete (DEK cache capacity: {_dekCacheCapacity}, stress files: {_stressTestFilesBytes.Count})"
        );
    }

    /// <summary>
    /// Creates a safe MemoryStream from encrypted bytes that won't be affected by GC relocation.
    /// </summary>
    private static MemoryStream CreateSafeStream(byte[] bytes)
    {
        return new MemoryStream(bytes, writable: false);
    }

    private async Task WarmupCache(byte[]? encryptedBytes, DekCache cache, string name)
    {
        if (encryptedBytes == null || _keyVaultProvider == null)
            return;

        using var stream = CreateSafeStream(encryptedBytes);
        var reader = new ParquetReader(_keyVaultProvider, cache);
        var decrypted = await reader.ReadParquetAsync(stream);
        var buffer = new byte[8192];
        while (await decrypted.ReadAsync(buffer) > 0) { }
    }

    [Benchmark(Description = "Initial Key Latency - Cold Start - Small Dataset")]
    public async Task InitialKeyLatency_ColdStart_SmallDataset()
    {
        if (_coldFixture == null || _keyVaultProvider == null || _smallEncryptedBytes == null)
            throw new InvalidOperationException("Benchmark not properly initialized");

        _metrics.Reset();
        var scenarioKey = "InitialKeyLatency_ColdStart_SmallDataset";
        var measure = ShouldMeasure("Measurement4", scenarioKey);

        await _coldFixture.ClearCachesAsync();
        if (measure)
            _metrics.StartMeasurement();

        using var stream = CreateSafeStream(_smallEncryptedBytes);
        var reader = new ParquetReader(
            _keyVaultProvider,
            _coldFixture.DekCache,
            (latency) =>
            {
                _metrics.RecordKvCall(latency);
                _metrics.RecordOperationLatency(
                    "Measurement4_InitialKeyLatency_Small_Cold_ExternalKv",
                    latency
                );
            },
            latency =>
            {
                _metrics.RecordCachedKvCall(latency);
                _metrics.RecordOperationLatency(
                    "Measurement4_InitialKeyLatency_Small_Cold_CachedKv",
                    latency
                );
            }
        );

        _ = await reader.ReadParquetAsync(stream);
        RecordIfMeasured(
            scenarioKey,
            "Measurement4_InitialKeyLatency_Small_Cold",
            _metrics,
            measure
        );
    }

    [Benchmark(Description = "Initial Key Latency - Cold Start - Wide Dataset")]
    public async Task InitialKeyLatency_ColdStart_WideDataset()
    {
        if (_coldFixture == null || _keyVaultProvider == null || _wideEncryptedBytes == null)
            throw new InvalidOperationException("Benchmark not properly initialized");

        _metrics.Reset();
        var scenarioKey = "InitialKeyLatency_ColdStart_WideDataset";
        var measure = ShouldMeasure("Measurement4", scenarioKey);

        await _coldFixture.ClearCachesAsync();
        if (measure)
            _metrics.StartMeasurement();

        using var stream = CreateSafeStream(_wideEncryptedBytes);
        var reader = new ParquetReader(
            _keyVaultProvider,
            _coldFixture.DekCache,
            (latency) =>
            {
                _metrics.RecordKvCall(latency);
                _metrics.RecordOperationLatency(
                    "Measurement4_InitialKeyLatency_Wide_Cold_ExternalKv",
                    latency
                );
            },
            latency =>
            {
                _metrics.RecordCachedKvCall(latency);
                _metrics.RecordOperationLatency(
                    "Measurement4_InitialKeyLatency_Wide_Cold_CachedKv",
                    latency
                );
            }
        );

        _ = await reader.ReadParquetAsync(stream);
        RecordIfMeasured(
            scenarioKey,
            "Measurement4_InitialKeyLatency_Wide_Cold",
            _metrics,
            measure
        );
    }

    [Benchmark(Description = "Initial Key Latency - Cold Start - Deep Dataset")]
    public async Task InitialKeyLatency_ColdStart_DeepDataset()
    {
        if (_coldFixture == null || _keyVaultProvider == null || _deepEncryptedBytes == null)
            throw new InvalidOperationException("Benchmark not properly initialized");

        _metrics.Reset();
        var scenarioKey = "InitialKeyLatency_ColdStart_DeepDataset";
        var measure = ShouldMeasure("Measurement4", scenarioKey);

        await _coldFixture.ClearCachesAsync();
        if (measure)
            _metrics.StartMeasurement();

        using var stream = CreateSafeStream(_deepEncryptedBytes);
        var reader = new ParquetReader(
            _keyVaultProvider,
            _coldFixture.DekCache,
            (latency) =>
            {
                _metrics.RecordKvCall(latency);
                _metrics.RecordOperationLatency(
                    "Measurement4_InitialKeyLatency_Deep_Cold_ExternalKv",
                    latency
                );
            },
            latency =>
            {
                _metrics.RecordCachedKvCall(latency);
                _metrics.RecordOperationLatency(
                    "Measurement4_InitialKeyLatency_Deep_Cold_CachedKv",
                    latency
                );
            }
        );

        _ = await reader.ReadParquetAsync(stream);
        RecordIfMeasured(
            scenarioKey,
            "Measurement4_InitialKeyLatency_Deep_Cold",
            _metrics,
            measure
        );
    }

    [Benchmark(Description = "Initial Key Latency - Warm Start - Small Dataset")]
    public async Task InitialKeyLatency_WarmStart_SmallDataset()
    {
        if (_warmFixture == null || _keyVaultProvider == null || _smallEncryptedBytes == null)
            throw new InvalidOperationException("Benchmark not properly initialized");

        _metrics.Reset();
        var scenarioKey = "InitialKeyLatency_WarmStart_SmallDataset";
        var measure = ShouldMeasure("Measurement4", scenarioKey);
        if (measure)
            _metrics.StartMeasurement();

        using var stream = CreateSafeStream(_smallEncryptedBytes);
        var reader = new ParquetReader(
            _keyVaultProvider,
            _warmFixture.DekCache,
            (latency) =>
            {
                _metrics.RecordKvCall(latency);
                _metrics.RecordOperationLatency(
                    "Measurement4_InitialKeyLatency_Small_Warm_ExternalKv",
                    latency
                );
            },
            latency =>
            {
                _metrics.RecordCachedKvCall(latency);
                _metrics.RecordOperationLatency(
                    "Measurement4_InitialKeyLatency_Small_Warm_CachedKv",
                    latency
                );
            }
        );

        _ = await reader.ReadParquetAsync(stream);
        RecordIfMeasured(
            scenarioKey,
            "Measurement4_InitialKeyLatency_Small_Warm",
            _metrics,
            measure
        );
    }

    [Benchmark(Description = "Initial Key Latency - Warm Start - Wide Dataset")]
    public async Task InitialKeyLatency_WarmStart_WideDataset()
    {
        if (_warmFixture == null || _keyVaultProvider == null || _wideEncryptedBytes == null)
            throw new InvalidOperationException("Benchmark not properly initialized");

        _metrics.Reset();
        var scenarioKey = "InitialKeyLatency_WarmStart_WideDataset";
        var measure = ShouldMeasure("Measurement4", scenarioKey);
        if (measure)
            _metrics.StartMeasurement();

        using var stream = CreateSafeStream(_wideEncryptedBytes);
        var reader = new ParquetReader(
            _keyVaultProvider,
            _warmFixture.DekCache,
            (latency) =>
            {
                _metrics.RecordKvCall(latency);
                _metrics.RecordOperationLatency(
                    "Measurement4_InitialKeyLatency_Wide_Warm_ExternalKv",
                    latency
                );
            },
            latency =>
            {
                _metrics.RecordCachedKvCall(latency);
                _metrics.RecordOperationLatency(
                    "Measurement4_InitialKeyLatency_Wide_Warm_CachedKv",
                    latency
                );
            }
        );

        _ = await reader.ReadParquetAsync(stream);
        RecordIfMeasured(
            scenarioKey,
            "Measurement4_InitialKeyLatency_Wide_Warm",
            _metrics,
            measure
        );
    }

    [Benchmark(Description = "Initial Key Latency - Warm Start - Deep Dataset")]
    public async Task InitialKeyLatency_WarmStart_DeepDataset()
    {
        if (_warmFixture == null || _keyVaultProvider == null || _deepEncryptedBytes == null)
            throw new InvalidOperationException("Benchmark not properly initialized");

        _metrics.Reset();
        var scenarioKey = "InitialKeyLatency_WarmStart_DeepDataset";
        var measure = ShouldMeasure("Measurement4", scenarioKey);
        if (measure)
            _metrics.StartMeasurement();

        using var stream = CreateSafeStream(_deepEncryptedBytes);
        var reader = new ParquetReader(
            _keyVaultProvider,
            _warmFixture.DekCache,
            (latency) =>
            {
                _metrics.RecordKvCall(latency);
                _metrics.RecordOperationLatency(
                    "Measurement4_InitialKeyLatency_Deep_Warm_ExternalKv",
                    latency
                );
            },
            latency =>
            {
                _metrics.RecordCachedKvCall(latency);
                _metrics.RecordOperationLatency(
                    "Measurement4_InitialKeyLatency_Deep_Warm_CachedKv",
                    latency
                );
            }
        );

        _ = await reader.ReadParquetAsync(stream);
        RecordIfMeasured(
            scenarioKey,
            "Measurement4_InitialKeyLatency_Deep_Warm",
            _metrics,
            measure
        );
    }

    [Benchmark(Description = "Cache Stress - 100 Small Files Parallel (Cold Start)")]
    public async Task CacheStress_100SmallFiles_Parallel()
    {
        if (_keyVaultProvider == null || _coldFixture == null)
            throw new InvalidOperationException("Benchmark not properly initialized");

        _metrics.Reset();
        var scenarioKey = "CacheStress_100SmallFiles_Parallel";
        var measure = ShouldMeasure("Measurement4", scenarioKey);

        await _coldFixture.ClearCachesAsync();
        if (measure)
            _metrics.StartMeasurement();

        // Parallel reads with shared DEK cache to stress cache contention
        var tasks = _stressTestFilesBytes
            .Select(bytes =>
                Task.Run(async () =>
                {
                    using var stream = CreateSafeStream(bytes);
                    // Avoid parallel writes to MetricsCollector from many tasks.
                    var reader = new ParquetReader(_keyVaultProvider, _coldFixture.DekCache);
                    var decrypted = await reader.ReadParquetAsync(stream);
                    var buffer = new byte[1024];
                    _ = await decrypted.ReadAsync(buffer);
                })
            )
            .ToArray();

        await Task.WhenAll(tasks);
        RecordIfMeasured(
            scenarioKey,
            "Measurement4_CacheStress_100Files_Parallel",
            _metrics,
            measure
        );
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        FlushRecordedResults();

        _coldFixture?.Dispose();
        _warmFixture?.Dispose();
        _services?.Dispose();
    }
}
