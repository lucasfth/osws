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
/// Benchmark for Measurement 6: S3 Cache Effectiveness.
/// Measures the benefit of EncryptedFileCache by comparing:
/// - First access: Cold cache (S3 download + cache miss)
/// - Second access: Warm cache (cache hit, no S3 download)
/// - Third access: Verify cache consistency
/// This shows how much the cache reduces S3 API calls and improves latency.
/// </summary>
[MemoryDiagnoser]
[Config(typeof(SharedBenchmarkConfig))]
public class Measurement6S3CacheEffectivenessBenchmark
{
    private ServiceProvider? _services;
    private IKeyVaultProvider? _keyVaultProvider;
    private IAmazonS3? _s3Client;
    private ParquetWriter? _parquetWriter;
    private ParquetReader? _parquetReader;
    private WarmStartFixture? _fixture;
    private readonly List<string> _uploadedKeys = [];

    private string _bucketName = "osws-benchmark-test";
    private string _uploadedKey = string.Empty;
    private bool _cleanupAfterRun = true;

    [GlobalSetup]
    public async Task GlobalSetupAsync()
    {
        Console.WriteLine("    Setting up S3 Cache Effectiveness benchmark...");

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

        _fixture = new WarmStartFixture();

        var providerType = config.GetValue<string>("KeyVault:Provider") ?? "Internal";
        _parquetWriter = new ParquetWriter(_keyVaultProvider, providerType);
        _parquetReader = new ParquetReader(_keyVaultProvider, _fixture.DekCache);

        // Generate and encrypt a deep dataset
        Console.WriteLine("   Generating deep dataset (10 cols × 100,000 rows)...");
        var unencrypted = await DeepDatasetGenerator.GenerateAsync(
            10,
            100_000,
            CancellationToken.None
        );

        Console.WriteLine("   Encrypting dataset...");
        var encrypted = await _parquetWriter.WriteParquetAsync(unencrypted, "default");

        // Upload to S3
        Console.WriteLine("   Uploading encrypted file to S3...");
        _uploadedKey = await S3BenchmarkHelper.UploadAsync(
            _s3Client,
            _bucketName,
            encrypted,
            "cache-test-dataset"
        );
        _uploadedKeys.Add(_uploadedKey);
        Console.WriteLine($"   ✅ Upload complete: {_uploadedKey}");
        Console.WriteLine();
    }

    [IterationSetup]
    public void IterationSetup()
    {
        // Clear file cache before each iteration to ensure consistent "cold cache" measurements
        // DEK cache remains warm after first iteration
        if (_fixture?.FileCache != null)
        {
            _fixture.FileCache.ClearAsync().GetAwaiter().GetResult();
        }
    }

    [Benchmark(Baseline = true)]
    [Description("First access: Cold cache (S3 download + decrypt)")]
    public async Task FirstAccess_ColdCache_S3Download()
    {
        // Download from S3 (cache miss)
        var encryptedStream = await S3BenchmarkHelper.DownloadAsync(
            _s3Client!,
            _bucketName,
            _uploadedKey
        );

        // Cache the encrypted stream (simulating OSWS behavior)
        var cacheKey = ParquetSolver.Helpers.EncryptedFileCache.GenerateCacheKey(
            _bucketName,
            _uploadedKey
        );
        await _fixture!.FileCache.SetAsync(cacheKey, encryptedStream, CancellationToken.None);

        // Decrypt
        encryptedStream.Position = 0;
        var decrypted = await _parquetReader!.ReadParquetAsync(encryptedStream);

        // Consume stream
        var buffer = new byte[8192];
        while (await decrypted.ReadAsync(buffer) > 0) { }

        encryptedStream.Dispose();
        decrypted.Dispose();
    }

    [Benchmark]
    [Description("Second access: Warm cache (cache hit, no S3)")]
    public async Task SecondAccess_WarmCache_CacheHit()
    {
        // First, populate the cache
        var cacheKey = OSWS.ParquetSolver.Helpers.EncryptedFileCache.GenerateCacheKey(
            _bucketName,
            _uploadedKey
        );

        // Try to get from cache first
        if (!_fixture!.FileCache.TryGet(cacheKey, out var cachedStream) || cachedStream == null)
        {
            // Cache miss - download and cache
            var downloaded = await S3BenchmarkHelper.DownloadAsync(
                _s3Client!,
                _bucketName,
                _uploadedKey
            );
            await _fixture.FileCache.SetAsync(cacheKey, downloaded, CancellationToken.None);
            downloaded.Position = 0;
            _ = downloaded;
        }

        // Now get from cache (should be a hit)
        _fixture.FileCache.TryGet(cacheKey, out var encryptedStream);

        if (encryptedStream == null)
        {
            throw new InvalidOperationException("Cache miss when cache hit was expected!");
        }

        // Decrypt from cached stream
        var decrypted = await _parquetReader!.ReadParquetAsync(encryptedStream);

        // Consume stream
        var buffer = new byte[8192];
        while (await decrypted.ReadAsync(buffer) > 0) { }

        encryptedStream.Dispose();
        decrypted.Dispose();
    }

    [GlobalCleanup]
    public async Task GlobalCleanupAsync()
    {
        Console.WriteLine();
        Console.WriteLine("  Cleaning up S3 cache benchmark files...");

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

        // Clear cache
        if (_fixture?.FileCache != null)
        {
            await _fixture.FileCache.ClearAsync();
        }

        _fixture?.Dispose();
        _services?.Dispose();
    }
}
