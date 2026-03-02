using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using OSWS.Models.Interfaces;
using OSWS.ParquetSolver;
using OSWS.Performance.Benchmarks.DatasetGenerators;
using OSWS.Performance.Benchmarks.Fixtures;
using OSWS.Performance.Benchmarks.Helpers;

namespace OSWS.Performance.Benchmarks.Measurements;

/// <summary>
/// Benchmark for Measurement 4: DEK cache stress test with parallel reads.
/// This simulates a scenario where the service receives a burst of requests for many different
/// files that are not in the DEK cache, which should stress test the cache eviction and key
/// retrieval logic under concurrent access. By using many small files we can also simulate
/// a common real‑world pattern where clients request metadata or small subsets of data from many
/// different datasets, which can be challenging for caching strategies.
/// </summary>
[MemoryDiagnoser]
[Config(typeof(SharedBenchmarkConfig))]
public class Measurement4DekCacheStressTestBenchmark
{
    private readonly ColdStartFixture _fixture = new();
    private readonly MetricsCollector _metrics = new();
    private readonly List<Stream> _encryptedFiles = [];

    private ServiceProvider? _services;
    private IKeyVaultProvider? _keyVaultProvider;
    private ParquetWriter? _parquetWriter;

    [GlobalSetup]
    public async Task SetupAsync()
    {
        await _fixture.ClearCachesAsync();
        _services = BenchmarkServiceFactory.BuildServiceProvider();
        _keyVaultProvider = _services.GetRequiredService<IKeyVaultProvider>();

        _parquetWriter = new ParquetWriter(_keyVaultProvider, "azure");

        // prepare 100 distinct small encrypted streams with unique master keys
        for (var i = 0; i < 100; i++)
        {
            var data = await SmallDatasetGenerator.GenerateAsync(5, 5000, CancellationToken.None);
            var encrypted = await _parquetWriter.WriteParquetAsync(data, $"default-{i}");
            _encryptedFiles.Add(encrypted);
        }
    }

    [Benchmark]
    public async Task ParallelRead_100SmallFiles_CacheStress()
    {
        _metrics.StartMeasurement();

        // Use shared DEK cache across all readers to simulate cache contention
        var tasks = _encryptedFiles
            .Select(stream =>
                Task.Run(async () =>
                {
                    stream.Position = 0;
                    // Create a new reader for each stream but share the DEK cache
                    // This simulates concurrent requests hitting the same cache
                    var reader = new ParquetReader(_keyVaultProvider!, _fixture.DekCache);
                    var decrypted = await reader.ReadParquetAsync(stream);
                    // just read a few bytes to simulate a client request
                    var buffer = new byte[1024];
                    _ = await decrypted.ReadAsync(buffer);
                })
            )
            .ToArray();

        await Task.WhenAll(tasks);

        _metrics.StopMeasurement();
        var metrics = _metrics.GetMetrics();
        ResultsRecorder.Record(
            nameof(Measurement4DekCacheStressTestBenchmark) + ".ParallelRead_100SmallFiles",
            metrics
        );
        _metrics.Reset();
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        foreach (var stream in _encryptedFiles)
        {
            stream.Dispose();
        }
        _encryptedFiles.Clear();
        _services?.Dispose();
        _fixture.Dispose();
    }
}
