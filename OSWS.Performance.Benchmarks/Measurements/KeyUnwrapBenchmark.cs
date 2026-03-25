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
/// DEK size is parameterized per benchmark case (16, 24, 32 bytes).
/// This benchmark measures key unwrap time using a cold cache scenario.
/// 
/// Method: We measure key unwrap time by using a cold cache and reading encrypted parquet.
/// This forces the system to unwrap DEKs without benefit of cached keys.
/// 
/// Expected outcome: Understanding key unwrap overhead independent of decryption.
/// </summary>
[Config(typeof(SharedBenchmarkConfig))]
public class KeyUnwrapBenchmark
{
    [Params(16, 24, 32)]
    public int DekSizeBytes { get; set; }

    private ServiceProvider? _services;
    private IKeyVaultProvider? _keyVaultProvider;
    private ILogger<KeyUnwrapBenchmark>? _logger;
    private ColdStartFixture? _fixture;
    private ParquetWriter? _parquetWriter;
    private ParquetReader? _parquetReader;
    private byte[]? _encryptedFileBytes;

    [GlobalSetup]
    public async Task GlobalSetupAsync()
    {
        var dekSizeBits = DekSizeBytes * 8;
        Console.WriteLine($"    Setting up Key Unwrap benchmark (DekSizeBytes={DekSizeBytes}, DekSizeBits={dekSizeBits})...");

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
            encryptionSettings: new EncryptionSettings { DekSizeBits = dekSizeBits }
        );

        // Generate a wide dataset (2000 cols × 10000 rows) for key unwrap testing
        Console.WriteLine("   Generating wide dataset (2000 cols × 10000 rows) for key unwrap testing...");
        var unencrypted = await WideDatasetGenerator.GenerateAsync(
            2000,
            10000,
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
        _fixture?.Dispose();
        _fixture = new ColdStartFixture();

        // Recreate parquetReader with fresh cache
        if (_keyVaultProvider != null && _fixture.DekCache != null)
        {
            _parquetReader = new ParquetReader(_keyVaultProvider, _fixture.DekCache);
        }
    }

    [Benchmark(Description = "Key Unwrap - Time to unwrap DEKs and read encrypted parquet")]
    public async Task MeasureKeyUnwrap()
    {
        if (_encryptedFileBytes == null || _parquetReader == null)
            throw new InvalidOperationException("Benchmark setup incomplete");

        // Create fresh stream for this iteration
        var encryptedStream = new MemoryStream(_encryptedFileBytes);

        // ReadParquetAsync will trigger DEK unwrapping
        var result = await _parquetReader.ReadParquetAsync(encryptedStream);

        // Consume some bytes to ensure unwrapping actually happens
        var buffer = new byte[8192];
        await result.ReadAsync(buffer);

        encryptedStream.Dispose();
        result.Dispose();
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
