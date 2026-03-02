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
/// Benchmark for Measurement 2: warm start with a deep dataset and selective column reads.
/// This simulates a common real‑world scenario where the service has been running for a while
/// (so caches are warm) and receives a request that only needs specific columns from a large dataset,
/// which should exercise the selective decryption and footer parsing logic.
/// </summary>
[MemoryDiagnoser]
[Config(typeof(SharedBenchmarkConfig))]
public class Measurement2WarmDekColumnSelectBenchmark
{
    private readonly MetricsCollector _metrics = new();
    private ServiceProvider? _services;
    private IKeyVaultProvider? _keyVaultProvider;
    private ParquetWriter? _parquetWriter;
    private ParquetReader? _parquetReader;
    private Stream? _encryptedStream;

    [GlobalSetup]
    public async Task SetupAsync()
    {
        _services = BenchmarkServiceFactory.BuildServiceProvider();
        _keyVaultProvider = _services.GetRequiredService<IKeyVaultProvider>();

        var fixture = new WarmStartFixture();
        fixture.PrepopulateCaches();

        _parquetWriter = new ParquetWriter(_keyVaultProvider, "azure");
        _parquetReader = new ParquetReader(_keyVaultProvider, fixture.DekCache);

        var unencrypted = await DeepDatasetGenerator.GenerateAsync(
            columns: 10,
            rows: 100_000,
            CancellationToken.None
        );

        _encryptedStream = await _parquetWriter.WriteParquetAsync(unencrypted, "default");
    }

    [Benchmark]
    [Description(
        "Warm start with deep dataset, selecting specific columns to stress selective decryption and footer parsing."
    )]
    public async Task WarmDek_DeepDataset_ColumnSelect()
    {
        _metrics.StartMeasurement();

        _encryptedStream?.Position = 0;
        if (_encryptedStream == null)
            throw new InvalidOperationException("Encrypted stream is not initialized.");
        if (_parquetReader == null)
            throw new InvalidOperationException("Parquet reader is not initialized.");
        _ = await _parquetReader.ReadParquetAsync(_encryptedStream);

        // later we can parse metadata and issue range reads for columns 1 & 10

        _metrics.StopMeasurement();
        var metrics = _metrics.GetMetrics();
        ResultsRecorder.Record(
            nameof(Measurement2WarmDekColumnSelectBenchmark) + ".WarmDek_DeepDataset_ColumnSelect",
            metrics
        );
        _metrics.Reset();
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _encryptedStream?.Dispose();
        _services?.Dispose();
    }
}
