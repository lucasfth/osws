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
/// Benchmark for Measurement 1: cold start with a wide dataset.
/// Supports selecting the key‑vault provider via configuration just like
/// the WebApi's service registration.
/// </summary>
[MemoryDiagnoser]
[Config(typeof(SharedBenchmarkConfig))]
public class Measurement1ColdWideRangeRequestBenchmark
{
    private ServiceProvider? _services;
    private IKeyVaultProvider? _keyVaultProvider;
    private ParquetWriter? _parquetWriter;
    private ParquetReader? _parquetReader;
    private Stream? _encryptedStream;

    // range offsets will be computed per invocation based on the decrypted stream length
    private const int RequestedSize = 2 * 1024 * 1024;

    // metrics collector used by the decorators
    private readonly MetricsCollector _metrics = new();

    [GlobalSetup]
    public async Task GlobalSetupAsync()
    {
        _services = BenchmarkServiceFactory.BuildServiceProvider();
        _keyVaultProvider = _services.GetRequiredService<IKeyVaultProvider>();

        var fixture = new ColdStartFixture();
        await fixture.ClearCachesAsync();

        _parquetWriter = new ParquetWriter(_keyVaultProvider, "azure");
        _parquetReader = new ParquetReader(_keyVaultProvider, fixture.DekCache);

        var unencrypted = await WideDatasetGenerator.GenerateAsync(
            2000,
            10000,
            CancellationToken.None
        );

        _encryptedStream = await _parquetWriter.WriteParquetAsync(unencrypted, "default");
        // range start/end are determined later when the decrypted length is known
    }

    [Benchmark]
    [Description("Measurement 1: Cold start with wide dataset (2MB range request)")]
    public async Task ColdStart_WideDataset_RangeRequest_2MB()
    {
        _metrics.StartMeasurement();

        _encryptedStream?.Position = 0;
        if (_encryptedStream == null)
            throw new InvalidOperationException("Encrypted stream is not initialized.");
        if (_parquetReader == null)
            throw new InvalidOperationException("Parquet reader is not initialized.");
        var decrypted = await _parquetReader.ReadParquetAsync(_encryptedStream);

        // compute range relative to decrypted length
        var decryptedLen = decrypted.Length;
        var start = Math.Max(0, decryptedLen - RequestedSize);
        var bytesToRead = (int)(decryptedLen - start);
        decrypted.Position = start;
        var buffer = new byte[bytesToRead];
        var totalRead = 0;
        while (totalRead < bytesToRead)
        {
            var read = await decrypted.ReadAsync(
                buffer.AsMemory(totalRead, bytesToRead - totalRead),
                CancellationToken.None
            );
            if (read == 0)
                break;
            totalRead += read;
        }

        _metrics.StopMeasurement();
        var metrics = _metrics.GetMetrics();
        ResultsRecorder.Record(
            nameof(Measurement1ColdWideRangeRequestBenchmark)
                + ".ColdStart_WideDataset_RangeRequest_2MB",
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
