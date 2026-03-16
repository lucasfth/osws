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
/// Measurement 2: Warm Start Latency Breakdown
/// Measures decryption latency with pre-populated caches for Small, Wide, and Deep datasets.
/// Uses S3 for stable encrypted data storage to avoid GC relocation issues.
/// Compares against cold start (Measurement 1) to quantify cache mitigation impact.
/// Shows that warm caches eliminate Azure KV latency and accelerate footer + column decryption.
/// </summary>
[Config(typeof(SharedBenchmarkConfig))]
public class Measurement2WarmDekColumnSelectBenchmark : ScenarioMeasurementBenchmarkBase
{
    [ParamsSource(nameof(GetDekCacheCapacities))]
    public int DekCacheCapacity { get; set; }

    private ServiceProvider? _services;
    private IKeyVaultProvider? _keyVaultProvider;
    private IAmazonS3? _s3Client;
    private WarmStartFixture? _fixture;

    private Stream? _smallEncrypted;
    private Stream? _wideEncrypted;
    private Stream? _deepEncrypted;

    private string _smallEncryptedKey = "";
    private string _wideEncryptedKey = "";
    private string _deepEncryptedKey = "";

    private string? _bucketName;

    private readonly MetricsCollector _metrics = new();
    private ParquetWriter? _parquetWriter;

    // configuration values (read once in GlobalSetup)
    private int _smallCols,
        _smallRows,
        _wideCols,
        _wideRows,
        _deepCols,
        _deepRows;

    // track cache warm-ups per dataset
    private readonly HashSet<string> _cacheWarmups = new();

    [GlobalSetup]
    public async Task GlobalSetupAsync()
    {
        _services = BenchmarkServiceFactory.BuildServiceProvider();
        var config = _services.GetRequiredService<IConfiguration>();
        _keyVaultProvider = _services.GetRequiredService<IKeyVaultProvider>();
        _s3Client = _services.GetRequiredService<IAmazonS3>();
        _fixture = new WarmStartFixture(DekCacheCapacity);
        var providerType = config.GetValue<string>("KeyVault:Provider") ?? "Internal";
        _parquetWriter = new ParquetWriter(_keyVaultProvider, providerType);
        _bucketName =
            config.GetValue<string>("BenchmarkSettings:S3BucketName") ?? "osws-benchmark-warm";

        _smallCols = config.GetValue<int>("ParquetSizes:Small:Columns");
        _smallRows = config.GetValue<int>("ParquetSizes:Small:Rows");
        _wideCols = config.GetValue<int>("ParquetSizes:Wide:Columns");
        _wideRows = config.GetValue<int>("ParquetSizes:Wide:Rows");
        _deepCols = config.GetValue<int>("ParquetSizes:Deep:Columns");
        _deepRows = config.GetValue<int>("ParquetSizes:Deep:Rows");

        if (
            _smallCols <= 0
            || _smallRows <= 0
            || _wideCols <= 0
            || _wideRows <= 0
            || _deepCols <= 0
            || _deepRows <= 0
        )
            throw new Exception("Invalid dataset size configuration in appsettings.json");

        // Ensure S3 bucket exists
        await S3BenchmarkHelper.EnsureBucketAsync(_s3Client, _bucketName);

        // do not build or upload any dataset yet; they'll be created lazily when the benchmark method first runs

        Console.WriteLine(
            $"[Measurement2] Global setup complete (DEK cache capacity: {DekCacheCapacity})"
        );
    }

    public IEnumerable<int> GetDekCacheCapacities()
    {
        var raw = Environment.GetEnvironmentVariable("BENCH_DEK_CACHE_CAPACITIES");
        if (string.IsNullOrWhiteSpace(raw))
            return [2500];

        var capacities = new HashSet<int>();
        foreach (var token in raw.Split(',', StringSplitOptions.RemoveEmptyEntries))
        {
            if (int.TryParse(token.Trim(), out var capacity) && capacity > 0)
                capacities.Add(capacity);
        }

        if (capacities.Count == 0)
            capacities.Add(2500);

        return capacities.OrderBy(c => c).ToArray();
    }

    /// <summary>
    /// Warms up the DEK cache by downloading and reading a dataset from S3.
    /// Each read gets a fresh MemoryStream from S3, avoiding GC relocation issues.
    /// </summary>
    private async Task WarmupDekCache(string name, string s3Key)
    {
        if (_s3Client == null || _keyVaultProvider == null || _fixture == null)
            return;

        Console.WriteLine($"\tWarming DEK cache for {name} dataset from S3...");

        // Download fresh from S3 - creates new MemoryStream
        await using var encryptedStream = await S3BenchmarkHelper.DownloadAsync(
            _s3Client,
            _bucketName!,
            s3Key
        );

        // Read to populate DEK cache
        var reader = new ParquetReader(_keyVaultProvider, _fixture.DekCache);
        var decrypted = await reader.ReadParquetAsync(encryptedStream);

        // Consume the stream to ensure full decryption
        var scratch = new byte[8192];
        while (await decrypted.ReadAsync(scratch) > 0) { }
    }

    [Benchmark(Description = "Warm Start - Small Dataset (5 cols × 5K rows)")]
    public async Task WarmStart_SmallDataset()
    {
        await RunWarmStartBenchmark("SmallDataset");
    }

    [Benchmark(Description = "Warm Start - Wide Dataset (2000 cols × 10K rows)")]
    public async Task WarmStart_WideDataset()
    {
        await RunWarmStartBenchmark("WideDataset");
    }

    [Benchmark(Description = "Warm Start - Deep Dataset (10 cols × 10M rows)")]
    public async Task WarmStart_DeepDataset()
    {
        await RunWarmStartBenchmark("DeepDataset");
    }

    private async Task RunWarmStartBenchmark(string datasetType)
    {
        // reset collector so warmups don't pollute later measurements
        _metrics.Reset();

        // lazily prepare dataset and ensure DEK cache warmed for this type
        await EnsureDatasetAsync(datasetType);

        string s3Key = datasetType switch
        {
            "SmallDataset" => _smallEncryptedKey,
            "WideDataset" => _wideEncryptedKey,
            "DeepDataset" => _deepEncryptedKey,
            _ => throw new ArgumentException("Unknown dataset type: " + datasetType),
        };

        if (
            string.IsNullOrEmpty(s3Key)
            || _s3Client == null
            || _keyVaultProvider == null
            || _fixture == null
        )
            throw new InvalidOperationException("Benchmark not properly initialized");

        var measure = ShouldMeasure("Measurement2", datasetType);

        if (measure)
            _metrics.StartMeasurement();

        // Download fresh from S3 each time - creates new MemoryStream, avoids GC relocation
        await using var encryptedStream = await S3BenchmarkHelper.DownloadAsync(
            _s3Client,
            _bucketName!,
            s3Key
        );

        // warm up DEK cache exactly once for this dataset type
        if (!_cacheWarmups.Contains(datasetType))
        {
            await WarmupDekCache(datasetType.Replace("Dataset", ""), s3Key);
            _cacheWarmups.Add(datasetType);
        }

        // Create reader with latency tracking callback - should see cache hits from warmed DEK cache
        var reader = new ParquetReader(
            _keyVaultProvider,
            _fixture.DekCache,
            latency => _metrics.RecordKvCall(latency),
            latency => _metrics.RecordCachedKvCall(latency)
        );
        _ = await reader.ReadParquetAsync(encryptedStream);

        RecordIfMeasured(datasetType, $"Measurement2_WarmStart_{datasetType}", _metrics, measure);
    }

    private async Task EnsureDatasetAsync(string datasetType)
    {
        switch (datasetType)
        {
            case "SmallDataset":
                if (string.IsNullOrEmpty(_smallEncryptedKey))
                {
                    Console.WriteLine("[Measurement2] generating & uploading small dataset");
                    var unenc = await SmallDatasetGenerator.GenerateAsync(
                        _smallCols,
                        _smallRows,
                        CancellationToken.None
                    );
                    _smallEncrypted = await _parquetWriter!.WriteParquetAsync(unenc, "default");
                    _smallEncryptedKey = await S3BenchmarkHelper.UploadAsync(
                        _s3Client!,
                        _bucketName!,
                        _smallEncrypted,
                        "measurement2-small"
                    );
                    _smallEncrypted.Dispose();
                    _smallEncrypted = null;
                }
                break;
            case "WideDataset":
                if (string.IsNullOrEmpty(_wideEncryptedKey))
                {
                    Console.WriteLine("[Measurement2] generating & uploading wide dataset");
                    var unenc = await WideDatasetGenerator.GenerateAsync(
                        _wideCols,
                        _wideRows,
                        CancellationToken.None
                    );
                    _wideEncrypted = await _parquetWriter!.WriteParquetAsync(unenc, "default");
                    _wideEncryptedKey = await S3BenchmarkHelper.UploadAsync(
                        _s3Client!,
                        _bucketName!,
                        _wideEncrypted,
                        "measurement2-wide"
                    );
                    _wideEncrypted.Dispose();
                    _wideEncrypted = null;
                }
                break;
            case "DeepDataset":
                if (string.IsNullOrEmpty(_deepEncryptedKey))
                {
                    Console.WriteLine("[Measurement2] generating & uploading deep dataset");
                    var unenc = await DeepDatasetGenerator.GenerateAsync(
                        _deepCols,
                        _deepRows,
                        CancellationToken.None
                    );
                    _deepEncrypted = await _parquetWriter!.WriteParquetAsync(unenc, "default");
                    _deepEncryptedKey = await S3BenchmarkHelper.UploadAsync(
                        _s3Client!,
                        _bucketName!,
                        _deepEncrypted,
                        "measurement2-deep"
                    );
                    _deepEncrypted.Dispose();
                    _deepEncrypted = null;
                }
                break;
            default:
                throw new ArgumentException("Unknown dataset type: " + datasetType);
        }
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        FlushRecordedResults();

        _fixture?.Dispose();
        _services?.Dispose();
    }
}
