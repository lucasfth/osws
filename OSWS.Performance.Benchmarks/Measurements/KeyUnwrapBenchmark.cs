using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using OSWS.Common.Configuration;
using OSWS.Models.Interfaces;
using OSWS.ParquetSolver;
using OSWS.Performance.Benchmarks.DatasetGenerators;
using OSWS.Performance.Benchmarks.Fixtures;
using OSWS.Performance.Benchmarks.Helpers;

namespace OSWS.Performance.Benchmarks.Measurements;

/// <summary>
/// Micro-benchmark: Key Unwrap Latency
///
/// Measures how long it takes to unwrap (decrypt) a Data Encryption Key (DEK)
/// from the Key Encryption Key (KEK).
///
/// DEK size is parameterized per benchmark case (128, 192, 256 bits).
/// This benchmark measures key unwrap time using a cold cache scenario.
///
/// Method: We measure key unwrap time by using a cold cache and reading encrypted parquet.
/// This forces the system to unwrap DEKs without benefit of cached keys.
///
/// Dataset: 10 cols × 100 rows — sized to trigger ~10 KV calls (~1 s total unwrap)
/// without burying the per-call signal under thousands of sequential round-trips.
/// (The original 2,000-col × 10,000-row dataset caused ~218 s of KV calls per iteration.)
///
/// Expected outcome: Understanding key unwrap overhead independent of decryption.
/// </summary>
[Config(typeof(SharedBenchmarkConfig))]
[IterationCount(30)]
[WarmupCount(8)]
public class KeyUnwrapBenchmark
{
    [Params(128, 192, 256)]
    public int DekSizeBits { get; set; }

    private ServiceProvider? _services;
    private IKeyVaultProvider? _keyVaultProvider;
    private ILogger<KeyUnwrapBenchmark>? _logger;
    private ColdStartFixture? _fixture;
    private ParquetWriter? _parquetWriter;
    private ParquetReader? _parquetReader;
    private byte[]? _encryptedFileBytes;
    private readonly byte[] _readBuffer = new byte[8192];

    [GlobalSetup]
    public async Task GlobalSetupAsync()
    {
        Console.WriteLine($"    Setting up Key Unwrap benchmark (DekSizeBits={DekSizeBits})...");

        _services = BenchmarkServiceFactory.BuildServiceProvider();
        var config = _services.GetRequiredService<IConfiguration>();
        _keyVaultProvider = _services.GetRequiredService<IKeyVaultProvider>();
        _logger = _services.GetRequiredService<ILogger<KeyUnwrapBenchmark>>();

        // Use cold start fixture - forces cache to be empty, so each read must unwrap keys
        _fixture = new ColdStartFixture();

        var providerType = config.GetValue<string>("KeyVault:Provider") ?? "Internal";
        _parquetWriter = new ParquetWriter(
            _keyVaultProvider,
            providerType,
            logger: _logger,
            encryptionSettings: new EncryptionSettings { DekSizeBits = DekSizeBits }
        );

        // Generate a narrow dataset (10 cols × 100 rows) for key unwrap testing.
        // 10 columns triggers ~10 cold KV calls (~1s), which is enough to measure
        // per-call unwrap latency without running for minutes per iteration.
        Console.WriteLine("   Generating dataset (10 cols × 100 rows) for key unwrap testing...");
        var unencrypted = await ParquetGenerator.GenerateAsync(
            10,
            100,
            cancellationToken: CancellationToken.None
        );

        Console.WriteLine("   Encrypting dataset for key unwrap benchmarks...");
        var (encryptedStream, _) = await _parquetWriter.WriteParquetAsync(unencrypted, "default");
        _encryptedFileBytes = ((MemoryStream)encryptedStream).ToArray();
        encryptedStream.Dispose();

        Console.WriteLine($"   ✅ Setup complete for key unwrap benchmark");
    }

    [IterationSetup]
    public void IterationSetup()
    {
        // Cold start: clear caches before each iteration to force key unwrapping
        if (_fixture == null)
        {
            _fixture = new ColdStartFixture();
        }

        _fixture.DekCache.Clear();

        if (_keyVaultProvider != null)
        {
            _parquetReader = new ParquetReader(_keyVaultProvider, _fixture.DekCache);
        }
    }

    [Benchmark(Description = "Key Unwrap - Time to unwrap DEKs and read encrypted parquet")]
    public async Task MeasureKeyUnwrap()
    {
        if (_encryptedFileBytes == null || _parquetReader == null)
            throw new InvalidOperationException("Benchmark setup incomplete");

        var encryptedStream = new MemoryStream(_encryptedFileBytes);
        var result = await _parquetReader.ReadParquetAsync(encryptedStream);

        long totalBytesRead = 0;
        int bytesRead;
        while ((bytesRead = await result.ReadAsync(_readBuffer)) > 0)
        {
            totalBytesRead += bytesRead;
        }

        encryptedStream.Dispose();
        result.Dispose();

        if (totalBytesRead == 0)
            throw new InvalidOperationException("No data was read during key unwrap");
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        Console.WriteLine($"   Cleaning up Key Unwrap benchmark");
        _fixture?.Dispose();
        _parquetWriter = null;
        _parquetReader = null;
        _services?.Dispose();
    }
}
