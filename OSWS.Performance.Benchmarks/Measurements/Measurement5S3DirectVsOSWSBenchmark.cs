using System.ComponentModel;
using Amazon.S3;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using OSWS.Models.Interfaces;
using OSWS.ParquetSolver;
using OSWS.Performance.Benchmarks.DatasetGenerators;
using OSWS.Performance.Benchmarks.Fixtures;
using OSWS.Performance.Benchmarks.Helpers;

namespace OSWS.Performance.Benchmarks.Measurements;

/// <summary>
/// Benchmark for Measurement 5: Direct S3 access vs OSWS encrypted pipeline.
/// Compares raw S3 download performance against the full OSWS pipeline (download + decrypt).
/// This shows the overhead introduced by encryption/decryption in real-world scenarios.
/// </summary>
[MemoryDiagnoser]
[Config(typeof(SharedBenchmarkConfig))]
public class Measurement5S3DirectVsOSWSBenchmark
{
    private ServiceProvider? _services;
    private IKeyVaultProvider? _keyVaultProvider;
    private IAmazonS3? _s3Client;
    private ParquetWriter? _parquetWriter;
    private ParquetReader? _parquetReader;
    private ColdStartFixture? _fixture;
    private List<string> _uploadedKeys = [];

    private string _bucketName = "osws-benchmark-test";
    private string _uploadedEncryptedKey = string.Empty;
    private bool _cleanupAfterRun = true;

    [GlobalSetup]
    public async Task GlobalSetupAsync()
    {
        Console.WriteLine("    Setting up S3 Direct vs OSWS benchmark...");

        _services = BenchmarkServiceFactory.BuildServiceProvider();
        _keyVaultProvider = _services.GetRequiredService<IKeyVaultProvider>();
        _s3Client = _services.GetRequiredService<IAmazonS3>();

        // Load benchmark settings from config
        var config = _services.GetRequiredService<IConfiguration>();
        _bucketName =
            config.GetValue<string>("BenchmarkSettings:S3BucketName") ?? "osws-benchmark-test";
        _cleanupAfterRun = config.GetValue("BenchmarkSettings:CleanupAfterRun", true);

        Console.WriteLine($"   Bucket: {_bucketName}");

        // Ensure bucket exists
        await S3BenchmarkHelper.EnsureBucketAsync(_s3Client, _bucketName);

        _fixture = new ColdStartFixture();
        await _fixture.ClearCachesAsync();

        var providerType = config.GetValue<string>("KeyVault:Provider") ?? "Internal";
        _parquetWriter = new ParquetWriter(_keyVaultProvider, providerType);
        _parquetReader = new ParquetReader(_keyVaultProvider, _fixture.DekCache);

        // Generate and encrypt a wide dataset
        Console.WriteLine("   Generating wide dataset (2000 cols × 10000 rows)...");
        var unencrypted = await WideDatasetGenerator.GenerateAsync(
            2000,
            10000,
            CancellationToken.None
        );

        Console.WriteLine("   Encrypting dataset...");
        var (encrypted, _) = await _parquetWriter.WriteParquetAsync(unencrypted, "default");

        // Upload to S3
        Console.WriteLine("   Uploading encrypted file to S3...");
        _uploadedEncryptedKey = await S3BenchmarkHelper.UploadAsync(
            _s3Client,
            _bucketName,
            encrypted,
            "wide-dataset-benchmark"
        );
        _uploadedKeys.Add(_uploadedEncryptedKey);
        Console.WriteLine($"   ✅ Upload complete: {_uploadedEncryptedKey}");
        Console.WriteLine();
    }

    [Benchmark(Baseline = true)]
    [Description("Baseline: Direct S3 download (no decryption)")]
    public async Task DirectS3Download_WideDataset()
    {
        var stream = await S3BenchmarkHelper.DownloadAsync(
            _s3Client!,
            _bucketName,
            _uploadedEncryptedKey
        );

        // Consume the stream to simulate actual usage
        var buffer = new byte[8192];
        while (await stream.ReadAsync(buffer) > 0) { }

        stream.Dispose();
    }

    [Benchmark]
    [Description("OSWS Pipeline: S3 download + decrypt")]
    public async Task OSWSPipeline_WideDataset_Decrypt()
    {
        // Download encrypted file from S3
        var encryptedStream = await S3BenchmarkHelper.DownloadAsync(
            _s3Client!,
            _bucketName,
            _uploadedEncryptedKey
        );

        // Decrypt through OSWS pipeline
        var decrypted = await _parquetReader!.ReadParquetAsync(encryptedStream);

        // Consume the decrypted stream
        var buffer = new byte[8192];
        while (await decrypted.ReadAsync(buffer) > 0) { }

        encryptedStream.Dispose();
        decrypted.Dispose();
    }

    [GlobalCleanup]
    public async Task GlobalCleanupAsync()
    {
        Console.WriteLine();
        Console.WriteLine("🧹 Cleaning up S3 benchmark files...");

        if (_cleanupAfterRun && _uploadedKeys.Count > 0)
        {
            await S3BenchmarkHelper.DeleteAsync(_s3Client!, _bucketName, _uploadedKeys);
            Console.WriteLine($"   ✅ Deleted {_uploadedKeys.Count} file(s) from S3");
        }
        else if (_uploadedKeys.Count > 0)
        {
            Console.WriteLine($"   ⚠️  Cleanup disabled - {_uploadedKeys.Count} file(s) remain:");
            foreach (var key in _uploadedKeys)
            {
                Console.WriteLine($"      - s3://{_bucketName}/{key}");
            }
        }

        _fixture?.Dispose();
        _services?.Dispose();
    }
}
