using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using OSWS.Models.Interfaces;
using OSWS.ParquetSolver;
using OSWS.Performance.Benchmarks.DatasetGenerators;
using OSWS.Performance.Benchmarks.Fixtures;
using OSWS.Performance.Benchmarks.Helpers;

namespace OSWS.Performance.Benchmarks.Measurements;

/// <summary>
/// Measurement 8: Column Decryption Throughput
/// Measures the time to decrypt columns across different dataset sizes (row counts).
/// This isolates the decryption performance independent of key unwrapping.
/// Varies only the number of rows (5000, 10000, 10M) while keeping column structure constant.
/// Uses a wide dataset structure (2000 columns) to represent realistic encrypted Parquet files.
/// </summary>
[Config(typeof(SharedBenchmarkConfig))]
public class Measurement8ColumnDecryptionBenchmark
{
    [Params(5000, 10000, 1000000)]
    public int RowCount { get; set; }

    private ServiceProvider? _services;
    private IKeyVaultProvider? _keyVaultProvider;
    private ILogger<Measurement8ColumnDecryptionBenchmark>? _logger;
    private WarmStartFixture? _fixture;
    private ParquetWriter? _parquetWriter;
    private ParquetReader? _parquetReader;
    private byte[]? _encryptedFileBytes;

    [GlobalSetup]
    public async Task GlobalSetupAsync()
    {
        Console.WriteLine($"    Setting up Column Decryption benchmark for {RowCount:N0} rows...");

        _services = BenchmarkServiceFactory.BuildServiceProvider();
        var config = _services.GetRequiredService<IConfiguration>();
        _keyVaultProvider = _services.GetRequiredService<IKeyVaultProvider>();
        _logger = _services.GetRequiredService<ILogger<Measurement8ColumnDecryptionBenchmark>>();

        // Use warm start fixture to pre-populate key cache
        // This isolates column decryption time from key unwrapping time
        _fixture = new WarmStartFixture(dekCacheCapacity: 2500);

        var providerType = config.GetValue<string>("KeyVault:Provider") ?? "Internal";
        _parquetWriter = new ParquetWriter(_keyVaultProvider, providerType, logger: _logger);
        _parquetReader = new ParquetReader(_keyVaultProvider, _fixture.DekCache);

        // Generate dataset with varying row counts (2000 columns, rows vary)
        // Short columns: smaller data type (e.g., int), Long columns: larger data type (e.g., string)
        // For simplicity, we generate the same structure but vary rows
        Console.WriteLine($"   Generating wide dataset (2000 cols × {RowCount:N0} rows)...");
        var unencrypted = await WideDatasetGenerator.GenerateAsync(
            2000,
            RowCount,
            CancellationToken.None
        );

        Console.WriteLine("   Encrypting dataset for column decryption testing...");
        await using (var encrypted = await _parquetWriter.WriteParquetAsync(unencrypted, "default"))
        {
            _encryptedFileBytes = ((MemoryStream)encrypted).ToArray();
        }

        // Pre-warm the cache with the DEK so we measure pure decryption time
        Console.WriteLine("   Pre-warming cache with DEK...");
        var warmStream = new MemoryStream(_encryptedFileBytes);
        await _parquetReader.ReadParquetAsync(warmStream);
        warmStream.Dispose();

        Console.WriteLine($"   ✅ Setup complete for column decryption benchmark ({RowCount:N0} rows)");
    }

    [Benchmark(Description = "Decrypt Columns - Time to fully decrypt all columns")]
    public async Task MeasureColumnDecryption()
    {
        if (_encryptedFileBytes == null || _parquetReader == null)
            throw new InvalidOperationException("Benchmark setup incomplete");

        var encryptedStream = new MemoryStream(_encryptedFileBytes);
        var decrypted = await _parquetReader.ReadParquetAsync(encryptedStream);

        // Consume the decrypted stream to force full decryption
        var buffer = new byte[8192];
        while (await decrypted.ReadAsync(buffer) > 0)
        {
            // consume
        }

        encryptedStream.Dispose();
        decrypted.Dispose();
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        Console.WriteLine($"   Cleaning up Column Decryption benchmark ({RowCount:N0} rows)");
        _fixture?.Dispose();
        _services?.Dispose();
    }
}
