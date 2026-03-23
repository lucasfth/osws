using Amazon.S3;
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
/// Measurement 9: S3 Write Performance (Direct vs OSWS Encrypted)
/// Compares the overhead of writing encrypted data to S3 vs direct S3 writes.
/// Measures throughput and latency for uploading data of varying sizes.
/// Varies only the number of rows (5000, 10000, 10M) while keeping column structure constant.
/// Shows the performance impact of encryption + S3 upload for different dataset sizes.
/// </summary>
[MemoryDiagnoser]
[Config(typeof(SharedBenchmarkConfig))]
public class Measurement9S3WriteDirectVsOSWSBenchmark
{
    [Params(5000, 10000, 1000000)]
    public int RowCount { get; set; }

    private ServiceProvider? _services;
    private IKeyVaultProvider? _keyVaultProvider;
    private IAmazonS3? _s3Client;
    private ILogger<Measurement9S3WriteDirectVsOSWSBenchmark>? _logger;
    private ColdStartFixture? _fixture;
    private ParquetWriter? _parquetWriter;
    private byte[]? _unencryptedFileBytes;
    private string _bucketName = "osws-benchmark-write-test";
    private bool _cleanupAfterRun = true;
    private readonly List<string> _uploadedKeys = [];

    [GlobalSetup]
    public async Task GlobalSetupAsync()
    {
        Console.WriteLine($"    Setting up S3 Write benchmark for {RowCount:N0} rows...");

        _services = BenchmarkServiceFactory.BuildServiceProvider();
        var config = _services.GetRequiredService<IConfiguration>();
        _keyVaultProvider = _services.GetRequiredService<IKeyVaultProvider>();
        _s3Client = _services.GetRequiredService<IAmazonS3>();
        _logger = _services.GetRequiredService<ILogger<Measurement9S3WriteDirectVsOSWSBenchmark>>();
        _fixture = new ColdStartFixture();

        // Configuration
        _bucketName = config.GetValue<string>("BenchmarkSettings:S3BucketName") ?? "osws-benchmark-write-test";
        _cleanupAfterRun = config.GetValue("BenchmarkSettings:CleanupAfterRun", true);

        Console.WriteLine($"   Bucket: {_bucketName}");

        // Ensure bucket exists
        await S3BenchmarkHelper.EnsureBucketAsync(_s3Client, _bucketName);

        var providerType = config.GetValue<string>("KeyVault:Provider") ?? "Internal";
        _parquetWriter = new ParquetWriter(_keyVaultProvider, providerType, logger: _logger);

        // Generate unencrypted dataset once
        Console.WriteLine($"   Generating wide dataset (2000 cols × {RowCount:N0} rows)...");
        var unencrypted = await WideDatasetGenerator.GenerateAsync(
            2000,
            RowCount,
            CancellationToken.None
        );

        // Serialize to bytes for direct S3 write baseline
        Console.WriteLine("   Serializing unencrypted dataset for direct S3 write baseline...");
        using (var temp = new MemoryStream())
        {
            // Write unencrypted parquet to memory
            var parquetBytes = (MemoryStream)unencrypted;
            _unencryptedFileBytes = parquetBytes.ToArray();
        }

        Console.WriteLine($"   ✅ Setup complete for S3 Write benchmark ({RowCount:N0} rows)");
    }

    [Benchmark(Baseline = true, Description = "Direct S3 Write - Upload unencrypted file directly to S3")]
    public async Task DirectS3Write()
    {
        if (_unencryptedFileBytes == null)
            throw new InvalidOperationException("Benchmark setup incomplete");

        var key = $"direct-write-{RowCount}-{Guid.NewGuid()}";
        var stream = new MemoryStream(_unencryptedFileBytes);

        await S3BenchmarkHelper.UploadAsync(_s3Client!, _bucketName, stream, key);
        
        _uploadedKeys.Add(key);
        stream.Dispose();
    }

    [Benchmark(Description = "OSWS Encrypted S3 Write - Encrypt then upload to S3")]
    public async Task OSWSEncryptedS3Write()
    {
        if (_unencryptedFileBytes == null)
            throw new InvalidOperationException("Benchmark setup incomplete");

        // Encrypt the dataset
        var unencryptedStream = new MemoryStream(_unencryptedFileBytes);
        var encryptedStream = await _parquetWriter!.WriteParquetAsync(unencryptedStream, "default");

        // Upload encrypted file to S3
        var key = $"osws-encrypted-write-{RowCount}-{Guid.NewGuid()}";
        await S3BenchmarkHelper.UploadAsync(_s3Client!, _bucketName, encryptedStream, key);

        _uploadedKeys.Add(key);
        unencryptedStream.Dispose();
        encryptedStream.Dispose();
    }

    [GlobalCleanup]
    public async Task GlobalCleanupAsync()
    {
        Console.WriteLine($"   Cleaning up S3 Write benchmark ({RowCount:N0} rows)");

        if (_cleanupAfterRun && _uploadedKeys.Count > 0)
        {
            await S3BenchmarkHelper.DeleteAsync(_s3Client!, _bucketName, _uploadedKeys);
            Console.WriteLine($"   ✅ Deleted {_uploadedKeys.Count} file(s) from S3");
        }
        else if (_uploadedKeys.Count > 0)
        {
            Console.WriteLine($"   ⚠️  Cleanup disabled - {_uploadedKeys.Count} file(s) remain in S3");
        }

        _fixture?.Dispose();
        _services?.Dispose();
    }
}
