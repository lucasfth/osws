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
/// Measurement 7: Key Unwrapping Performance
/// Measures Data Encryption Key (DEK) unwrapping latency for different key sizes.
/// This is the time it takes to unwrap/decrypt a DEK before it can be used for column decryption.
/// Varies only the key size (256 bytes vs 512 bytes) while keeping dataset structure constant.
/// Uses a middle-ground wide dataset (2000 cols × 10000 rows) for consistent comparison.
/// </summary>
[Config(typeof(SharedBenchmarkConfig))]
public class Measurement7KeyUnwrapBenchmark
{
    [Params(256, 512)]
    public int KeySizeBytes { get; set; }

    private ServiceProvider? _services;
    private IKeyVaultProvider? _keyVaultProvider;
    private ILogger<Measurement7KeyUnwrapBenchmark>? _logger;
    private ColdStartFixture? _fixture;
    private ParquetWriter? _parquetWriter;
    private byte[]? _encryptedFileBytes;

    [GlobalSetup]
    public async Task GlobalSetupAsync()
    {
        Console.WriteLine($"    Setting up Key Unwrap benchmark for {KeySizeBytes}-byte keys...");

        _services = BenchmarkServiceFactory.BuildServiceProvider();
        var config = _services.GetRequiredService<IConfiguration>();
        _keyVaultProvider = _services.GetRequiredService<IKeyVaultProvider>();
        _logger = _services.GetRequiredService<ILogger<Measurement7KeyUnwrapBenchmark>>();
        _fixture = new ColdStartFixture();

        var providerType = config.GetValue<string>("KeyVault:Provider") ?? "Internal";
        _parquetWriter = new ParquetWriter(_keyVaultProvider, providerType, logger: _logger);

        // Generate a middle-ground wide dataset (2000 cols × 10000 rows)
        Console.WriteLine("   Generating wide dataset (2000 cols × 10000 rows) for key unwrap testing...");
        var unencrypted = await WideDatasetGenerator.GenerateAsync(
            2000,
            10000,
            CancellationToken.None
        );

        Console.WriteLine("   Encrypting dataset for key unwrap benchmarks...");
        await using (var encrypted = await _parquetWriter.WriteParquetAsync(unencrypted, "default"))
        {
            _encryptedFileBytes = ((MemoryStream)encrypted).ToArray();
        }

        Console.WriteLine($"   ✅ Setup complete for {KeySizeBytes}-byte key unwrap benchmark");
    }

    [IterationSetup]
    public void IterationSetup()
    {
        // Cold start: clear caches before each iteration to measure key unwrapping independently
        _fixture?.Dispose();
        _fixture = new ColdStartFixture();
    }

    [Benchmark(Description = "DEK Unwrap - Time to unwrap DEK from encrypted file")]
    public async Task MeasureKeyUnwrap()
    {
        if (_encryptedFileBytes == null || _fixture?.DekCache == null || _keyVaultProvider == null)
            throw new InvalidOperationException("Benchmark setup incomplete");

        // Create a fresh ParquetReader with empty cache to force key unwrapping
        var parquetReader = new ParquetReader(_keyVaultProvider, _fixture.DekCache);

        // Reading the parquet will trigger DEK unwrapping/decryption
        var encryptedStream = new MemoryStream(_encryptedFileBytes);
        var result = await parquetReader.ReadParquetAsync(encryptedStream);

        // Consume some bytes to ensure reading actually happens
        var buffer = new byte[8192];
        await result.ReadAsync(buffer);

        encryptedStream.Dispose();
        result.Dispose();
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        Console.WriteLine($"   Cleaning up Key Unwrap benchmark ({KeySizeBytes}-byte keys)");
        _fixture?.Dispose();
        _services?.Dispose();
    }
}
