using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using OSWS.Models.Interfaces;
using OSWS.ParquetSolver;
using OSWS.Performance.Benchmarks.DatasetGenerators;
using OSWS.Performance.Benchmarks.Fixtures;
using OSWS.Performance.Benchmarks.Helpers;

namespace OSWS.Performance.Benchmarks.Measurements;

/// <summary>
/// Benchmark for Measurement 3: full decryption throughput with multi‑core scaling.
/// This simulates a scenario where the service needs to decrypt an entire large dataset,
/// which is the worst‑case scenario for performance.
/// By varying the number of cores used we can also see how well the decryption process scales with parallelism,
/// which is important for understanding how it will perform under heavy load.
/// </summary>
[MemoryDiagnoser]
[Config(typeof(SharedBenchmarkConfig))]
public class Measurement3FullDecryptionThroughputBenchmark
{
    private readonly WarmStartFixture _fixture = new();
    private readonly MetricsCollector _metrics = new();

    private ServiceProvider? _services;
    private IKeyVaultProvider? _keyVaultProvider;
    private ParquetWriter? _parquetWriter;
    private ParquetReader? _parquetReader;
    private Stream? _encryptedStream;
    private int _originalMaxWorkerThreads;
    private int _originalMaxIoThreads;

    [Params(1, 4, 8)]
    public int CoreCount { get; set; }

    [GlobalSetup]
    public async Task SetupAsync()
    {
        // Save original thread pool settings
        ThreadPool.GetMaxThreads(out _originalMaxWorkerThreads, out _originalMaxIoThreads);

        _services = BenchmarkServiceFactory.BuildServiceProvider();
        _keyVaultProvider = _services.GetRequiredService<IKeyVaultProvider>();

        _fixture.PrepopulateCaches();

        // generate and encrypt a deep dataset once
        _parquetWriter = new ParquetWriter(_keyVaultProvider, "azure");
        var unencrypted = await DeepDatasetGenerator.GenerateAsync(
            10,
            10_000_000,
            CancellationToken.None
        );
        _encryptedStream = await _parquetWriter.WriteParquetAsync(unencrypted, "default");
    }

    [Benchmark]
    public async Task FullDecryption_DeepDataset_MultiCore()
    {
        _metrics.StartMeasurement();

        // hint thread pool to use requested core count
        ThreadPool.SetMaxThreads(CoreCount, CoreCount);

        _encryptedStream!.Position = 0;
        _parquetReader ??= new ParquetReader(_keyVaultProvider!, _fixture.DekCache);

        var decrypted = await _parquetReader.ReadParquetAsync(_encryptedStream);
        // consume stream fully
        var buffer = new byte[8192];
        while (await decrypted.ReadAsync(buffer) > 0) { }

        _metrics.StopMeasurement();
        var metrics = _metrics.GetMetrics();
        ResultsRecorder.Record(
            nameof(Measurement3FullDecryptionThroughputBenchmark)
                + $".FullDecryption_MultiCore_{CoreCount}cores",
            metrics
        );
        _metrics.Reset();
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        // Restore original thread pool settings
        ThreadPool.SetMaxThreads(_originalMaxWorkerThreads, _originalMaxIoThreads);
        _encryptedStream?.Dispose();
        _services?.Dispose();
        _fixture.Dispose();
    }
}
