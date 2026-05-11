using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using OSWS.Common.Configuration;
using OSWS.Models.Interfaces;
using OSWS.ParquetSolver;
using OSWS.Performance.Benchmarks.Fixtures;
using OSWS.Performance.Benchmarks.Helpers;

namespace OSWS.Performance.Benchmarks.Measurements;

/// <summary>
/// Measures key unwrap (AKV/KV) latency using pre-generated e2e corpus files
/// re-encrypted with varying DEK sizes. Each instance corresponds to one
/// (corpus size label, DEK size bits) combination.
/// Clears the DEK cache before every iteration to force cold unwraps.
/// </summary>
public class KeyUnwrapBenchmark : IMicroBenchmark
{
    private readonly string _sizeLabel;
    private readonly int _dekSizeBits;

    public string Name => "KeyUnwrap";
    public string Parameters => $"size={_sizeLabel},dek_bits={_dekSizeBits}";

    private ServiceProvider? _services;
    private IKeyVaultProvider? _keyVaultProvider;
    private ILogger<KeyUnwrapBenchmark>? _logger;
    private ColdStartFixture? _fixture;
    private byte[]? _encryptedBytes;
    private readonly byte[] _readBuffer = new byte[8192];
    private string? _encryptedDatasetPath;

    public KeyUnwrapBenchmark(string sizeLabel, int dekSizeBits)
    {
        _sizeLabel = sizeLabel;
        _dekSizeBits = dekSizeBits;
    }

    public async Task SetupAsync()
    {
        Console.WriteLine($"    KeyUnwrap setup ({_sizeLabel}, DEK {_dekSizeBits} bits)...");

        _services = BenchmarkServiceFactory.BuildServiceProvider();
        var config = _services.GetRequiredService<IConfiguration>();
        _keyVaultProvider = _services.GetRequiredService<IKeyVaultProvider>();
        _logger = _services.GetRequiredService<ILogger<KeyUnwrapBenchmark>>();

        _fixture = new ColdStartFixture();

        var providerType = config.GetValue<string>("KeyVault:Provider") ?? "Internal";
        var writer = new ParquetWriter(
            _keyVaultProvider,
            providerType,
            logger: _logger,
            encryptionSettings: new EncryptionSettings { DekSizeBits = _dekSizeBits }
        );

        // Load corpus file from disk
        var corpusPath = MicroBenchmarkRunner.FindCorpusFile(_sizeLabel);
        Console.WriteLine($"      Loaded corpus: {Path.GetFileName(corpusPath)}");

        // Re-encrypt the corpus with the target DEK size
        _encryptedDatasetPath = Path.Combine(
            Path.GetTempPath(),
            $"osws-bench-kw-{Guid.NewGuid():N}.parquet"
        );

        // Scope write stream so file lock is released before reading back
        {
            await using var plainStream = File.OpenRead(corpusPath);
            await using var encryptedOutput = new FileStream(
                _encryptedDatasetPath,
                FileMode.Create,
                FileAccess.ReadWrite,
                FileShare.None
            );
            await writer.WriteParquetAsync(plainStream, "default", output: encryptedOutput);
        }

        _encryptedBytes = await File.ReadAllBytesAsync(_encryptedDatasetPath);
        Console.WriteLine($"      Setup complete ({_sizeLabel}, DEK {_dekSizeBits})");
    }

    public async Task RunAsync(MetricsCollector metrics)
    {
        if (_encryptedBytes == null || _keyVaultProvider == null || _fixture == null)
            throw new InvalidOperationException("Setup not completed");

        // Clear cache to force cold key unwrap on every iteration
        _fixture.DekCache.Clear();

        var reader = new ParquetReader(_keyVaultProvider, _fixture.DekCache);

        await using var encryptedStream = new MemoryStream(_encryptedBytes, writable: false);
        using var result = await reader.ReadParquetAsync(encryptedStream);

        long totalBytesRead = 0;
        int bytesRead;
        while ((bytesRead = await result.ReadAsync(_readBuffer)) > 0)
            totalBytesRead += bytesRead;

        if (totalBytesRead == 0)
            throw new InvalidOperationException("No data was read during key unwrap");
    }

    public async Task CleanupAsync()
    {
        Console.WriteLine($"    KeyUnwrap cleanup ({_sizeLabel}, DEK {_dekSizeBits})");
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
