using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using OSWS.Models.Interfaces;
using OSWS.ParquetSolver;
using OSWS.Performance.Benchmarks.Fixtures;
using OSWS.Performance.Benchmarks.Helpers;

namespace OSWS.Performance.Benchmarks.Measurements;

/// <summary>
/// Measures column decryption latency using pre-generated e2e corpus files.
/// Each instance corresponds to one corpus size label (tiny/small/medium/large/xlarge).
/// Uses a warm DEK cache to isolate decryption time from key unwrap.
/// </summary>
public class DecryptionBenchmark : IMicroBenchmark
{
    private readonly string _sizeLabel;

    public string Name => "Decryption";
    public string Parameters => $"size={_sizeLabel}";

    private ServiceProvider? _services;
    private IKeyVaultProvider? _keyVaultProvider;
    private ParquetWriter? _parquetWriter;
    private ParquetReader? _parquetReader;
    private WarmStartFixture? _fixture;
    private byte[]? _encryptedBytes;
    private readonly byte[] _readBuffer = new byte[8192];
    private string? _encryptedDatasetPath;

    public DecryptionBenchmark(string sizeLabel)
    {
        _sizeLabel = sizeLabel;
    }

    public async Task SetupAsync()
    {
        Console.WriteLine($"    Decryption setup ({_sizeLabel})...");

        _services = BenchmarkServiceFactory.BuildServiceProvider();
        var config = _services.GetRequiredService<IConfiguration>();
        _keyVaultProvider = _services.GetRequiredService<IKeyVaultProvider>();
        var logger = _services.GetRequiredService<ILogger<DecryptionBenchmark>>();

        _fixture = new WarmStartFixture(dekCacheCapacity: 2500);

        var providerType = config.GetValue<string>("KeyVault:Provider") ?? "Internal";
        _parquetWriter = new ParquetWriter(_keyVaultProvider, providerType, logger: logger);
        _parquetReader = new ParquetReader(_keyVaultProvider, _fixture.DekCache);

        // Load corpus file from disk
        var corpusPath = MicroBenchmarkRunner.FindCorpusFile(_sizeLabel);
        var fileSizeMb = new FileInfo(corpusPath).Length / 1024.0 / 1024.0;
        Console.WriteLine(
            $"      Loaded corpus: {Path.GetFileName(corpusPath)} ({fileSizeMb:F1} MB)"
        );

        // Encrypt the corpus file
        _encryptedDatasetPath = Path.Combine(
            Path.GetTempPath(),
            $"osws-bench-dec-{Guid.NewGuid():N}.parquet"
        );

        // Scope write stream so file lock is released before pre-warm read
        {
            await using var plainStream = File.OpenRead(corpusPath);
            await using var encryptedOutput = new FileStream(
                _encryptedDatasetPath,
                FileMode.Create,
                FileAccess.ReadWrite,
                FileShare.None
            );
            await _parquetWriter.WriteParquetAsync(plainStream, "default", output: encryptedOutput);
        }

        // Pre-warm DEK cache so measurement isolates decryption time
        Console.WriteLine("      Pre-warming DEK cache...");
        await using var warmStream = new FileStream(
            _encryptedDatasetPath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read
        );
        var warmResult = await _parquetReader.ReadParquetAsync(warmStream);
        warmResult.Dispose();

        _encryptedBytes = await File.ReadAllBytesAsync(_encryptedDatasetPath);
        Console.WriteLine($"      Setup complete ({_sizeLabel})");
    }

    public async Task RunAsync(MetricsCollector metrics)
    {
        if (_encryptedBytes == null || _parquetReader == null)
            throw new InvalidOperationException("Setup not completed");

        await using var encryptedStream = new MemoryStream(_encryptedBytes, writable: false);
        using var decrypted = await _parquetReader.ReadParquetAsync(encryptedStream);

        long totalBytesRead = 0;
        int bytesRead;
        while ((bytesRead = await decrypted.ReadAsync(_readBuffer)) > 0)
            totalBytesRead += bytesRead;

        if (totalBytesRead == 0)
            throw new InvalidOperationException("No data was decrypted");
    }

    public async Task CleanupAsync()
    {
        Console.WriteLine($"    Decryption cleanup ({_sizeLabel})");
        _fixture?.Dispose();
        _fixture = null;
        _services?.Dispose();
        _services = null;

        if (!string.IsNullOrWhiteSpace(_encryptedDatasetPath) && File.Exists(_encryptedDatasetPath))
            File.Delete(_encryptedDatasetPath);
    }

    public void Dispose()
    {
        _fixture?.Dispose();
        _services?.Dispose();
    }
}
