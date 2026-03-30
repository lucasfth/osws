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
/// Micro-benchmark: Column Decryption Latency
///
/// Measures how long it takes to decrypt a column by varying the number of rows.
/// Column width (number of columns) remains constant at 2,000.
///
/// Parameterized by row count: 5,000, 10,000, 100,000
///
/// Expected outcome: Understanding decryption latency scaling with data volume.
/// Uses warm cache (pre-populated with DEKs) to isolate decryption time from key unwrap.
/// </summary>
[Config(typeof(SharedBenchmarkConfig))]
public class DecryptionBenchmark
{
    private const int ColumnCount = 2000;

    [Params(5000, 10000, 100_000)]
    public int RowCount { get; set; }

    private ServiceProvider? _services;
    private IKeyVaultProvider? _keyVaultProvider;
    private ILogger<DecryptionBenchmark>? _logger;
    private WarmStartFixture? _fixture;
    private ParquetWriter? _parquetWriter;
    private ParquetReader? _parquetReader;
    private byte[]? _encryptedDatasetBytes;
    private readonly byte[] _readBuffer = new byte[8192];
    private string? _plainDatasetPath;
    private string? _encryptedDatasetPath;

    [GlobalSetup]
    public async Task GlobalSetupAsync()
    {
        Console.WriteLine(
            $"    Setting up Decryption benchmark (RowCount={RowCount:N0}, ColumnCount={ColumnCount})..."
        );

        _services = BenchmarkServiceFactory.BuildServiceProvider();
        var config = _services.GetRequiredService<IConfiguration>();
        _keyVaultProvider = _services.GetRequiredService<IKeyVaultProvider>();
        _logger = _services.GetRequiredService<ILogger<DecryptionBenchmark>>();

        // Use warm start fixture to pre-populate key cache
        // This isolates column decryption time from key unwrapping time
        _fixture = new WarmStartFixture(dekCacheCapacity: 2500);

        var providerType = config.GetValue<string>("KeyVault:Provider") ?? "Internal";
        _parquetWriter = new ParquetWriter(_keyVaultProvider, providerType, logger: _logger);
        _parquetReader = new ParquetReader(_keyVaultProvider, _fixture.DekCache);

        _plainDatasetPath = Path.Combine(
            Path.GetTempPath(),
            $"osws-bench-plain-{Guid.NewGuid():N}.parquet"
        );
        _encryptedDatasetPath = Path.Combine(
            Path.GetTempPath(),
            $"osws-bench-encrypted-{Guid.NewGuid():N}.parquet"
        );

        // Generate dataset with varying row counts (2000 columns, rows vary)
        Console.WriteLine(
            $"   Generating wide dataset ({ColumnCount} cols × {RowCount:N0} rows)..."
        );
        await using var plainOutput = new FileStream(
            _plainDatasetPath,
            FileMode.Create,
            FileAccess.ReadWrite,
            FileShare.None
        );
        await using var unencrypted = await WideDatasetGenerator.GenerateAsync(
            ColumnCount,
            RowCount,
            output: plainOutput,
            cancellationToken: CancellationToken.None
        );

        Console.WriteLine("   Encrypting dataset for column decryption testing...");
        await using (
            var encryptedOutput = new FileStream(
                _encryptedDatasetPath,
                FileMode.Create,
                FileAccess.ReadWrite,
                FileShare.None
            )
        )
        {
            await _parquetWriter.WriteParquetAsync(unencrypted, "default", output: encryptedOutput);
        }

        // Pre-warm the cache with the DEK so we measure pure decryption time, not key unwrap
        Console.WriteLine("   Pre-warming cache with DEK...");
        await using (
            var warmStream = new FileStream(
                _encryptedDatasetPath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read
            )
        )
        {
            var warmResult = await _parquetReader.ReadParquetAsync(warmStream);
            warmResult.Dispose();
        }

        _encryptedDatasetBytes = await File.ReadAllBytesAsync(_encryptedDatasetPath);

        Console.WriteLine(
            $"   ✅ Setup complete for column decryption benchmark ({RowCount:N0} rows)"
        );
    }

    [Benchmark(Description = "Column Decryption - Time to fully decrypt all columns")]
    public async Task<long> MeasureColumnDecryption()
    {
        if (_encryptedDatasetBytes == null || _parquetReader == null)
            throw new InvalidOperationException("Benchmark setup incomplete");

        await using var encryptedStream = new MemoryStream(_encryptedDatasetBytes, writable: false);
        using var decrypted = await _parquetReader.ReadParquetAsync(encryptedStream);

        // Consume the decrypted stream to force full decryption
        long totalBytesRead = 0;
        int bytesRead;
        while ((bytesRead = await decrypted.ReadAsync(_readBuffer)) > 0)
        {
            totalBytesRead += bytesRead;
        }

        return totalBytesRead;
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        Console.WriteLine($"   Cleaning up Decryption benchmark ({RowCount:N0} rows)");
        _fixture?.Dispose();
        _services?.Dispose();

        if (!string.IsNullOrWhiteSpace(_plainDatasetPath) && File.Exists(_plainDatasetPath))
        {
            File.Delete(_plainDatasetPath);
        }

        if (!string.IsNullOrWhiteSpace(_encryptedDatasetPath) && File.Exists(_encryptedDatasetPath))
        {
            File.Delete(_encryptedDatasetPath);
        }
    }
}
