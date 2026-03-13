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
/// Measurement 1: Cold Start Latency Breakdown
/// Measures decryption latency with empty caches for Small, Wide, and Deep datasets.
/// Captures per-operation latencies (footer, columns) to understand which operations are bottlenecks.
/// </summary>
[Config(typeof(SharedBenchmarkConfig))]
public class Measurement1ColdWideRangeRequestBenchmark : ScenarioMeasurementBenchmarkBase
{
    private ServiceProvider? _services;
    private IKeyVaultProvider? _keyVaultProvider;
    private IAmazonS3? _s3Client;
    private ColdStartFixture? _fixture;

    private Stream? _smallEncrypted;
    private Stream? _wideEncrypted;
    private Stream? _deepEncrypted;

    private string _smallEncryptedKey = "";
    private string _wideEncryptedKey = "";
    private string _deepEncryptedKey = "";

    private string _bucketName = "osws-benchmark-cold";

    private readonly MetricsCollector _metrics = new();
    private ParquetWriter? _parquetWriter;

    // configuration values (read once in GlobalSetup)
    private int _smallCols,
        _smallRows,
        _wideCols,
        _wideRows,
        _deepCols,
        _deepRows;

    [GlobalSetup]
    public async Task GlobalSetupAsync()
    {
        _services = BenchmarkServiceFactory.BuildServiceProvider();
        var config = _services.GetRequiredService<IConfiguration>();
        _keyVaultProvider = _services.GetRequiredService<IKeyVaultProvider>();
        _s3Client = _services.GetRequiredService<IAmazonS3>();
        _fixture = new ColdStartFixture();
        var providerType = config.GetValue<string>("KeyVault:Provider") ?? "Internal";
        _parquetWriter = new ParquetWriter(_keyVaultProvider, providerType);
        _bucketName =
            config.GetValue<string>("BenchmarkSettings:S3BucketName") ?? "osws-benchmark-cold";

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

        // Do not generate anything yet; datasets are built lazily when first needed.

        Console.WriteLine("[Measurement1] Global setup complete (datasets deferred to first use)");
    }

    [IterationSetup]
    public void IterationSetup()
    {
        // Cold start: create a fresh fixture so caches start empty
        _fixture?.Dispose();
        _fixture = new ColdStartFixture();
    }

    [Benchmark(Description = "Cold Start - Small Dataset (5 cols × 5K rows)")]
    public async Task ColdStart_SmallDataset()
    {
        await RunColdStartBenchmark("SmallDataset");
    }

    [Benchmark(Description = "Cold Start - Wide Dataset (2000 cols × 10K rows)")]
    public async Task ColdStart_WideDataset()
    {
        await RunColdStartBenchmark("WideDataset");
    }

    [Benchmark(Description = "Cold Start - Deep Dataset (10 cols × 10M rows)")]
    public async Task ColdStart_DeepDataset()
    {
        await RunColdStartBenchmark("DeepDataset");
    }

    private async Task RunColdStartBenchmark(string datasetType)
    {
        // reset collector so warmups don't pollute later measurements
        _metrics.Reset();

        // lazily create/upload the dataset needed for this benchmark
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

        var measure = ShouldMeasure("Measurement1", datasetType);

        if (measure)
            _metrics.StartMeasurement();

        // Download fresh from S3 - creates new MemoryStream
        await using var encryptedStream = await S3BenchmarkHelper.DownloadAsync(
            _s3Client,
            _bucketName,
            s3Key,
            latency => _metrics.RecordS3Call(latency)
        );

        // Create reader with latency tracking callback
        var reader = new ParquetReader(
            _keyVaultProvider,
            _fixture.DekCache,
            latency => _metrics.RecordKvCall(latency),
            latency => _metrics.RecordCachedKvCall(latency)
        );
        _ = await reader.ReadParquetAsync(encryptedStream);

        RecordIfMeasured(datasetType, $"Measurement1_ColdStart_{datasetType}", _metrics, measure);
    }

    private async Task EnsureDatasetAsync(string datasetType)
    {
        // this method populates the S3 key/stream for the requested type if not already done
        switch (datasetType)
        {
            case "SmallDataset":
                if (string.IsNullOrEmpty(_smallEncryptedKey))
                {
                    Console.WriteLine("[Measurement1] generating & uploading small dataset");
                    var unenc = await SmallDatasetGenerator.GenerateAsync(
                        _smallCols,
                        _smallRows,
                        CancellationToken.None
                    );
                    _smallEncrypted = await _parquetWriter!.WriteParquetAsync(unenc, "default");
                    _smallEncryptedKey = await S3BenchmarkHelper.UploadAsync(
                        _s3Client!,
                        _bucketName,
                        _smallEncrypted,
                        "measurement1-small"
                    );
                    _smallEncrypted.Dispose();
                    _smallEncrypted = null;
                }
                break;
            case "WideDataset":
                if (string.IsNullOrEmpty(_wideEncryptedKey))
                {
                    Console.WriteLine("[Measurement1] generating & uploading wide dataset");
                    var unenc = await WideDatasetGenerator.GenerateAsync(
                        _wideCols,
                        _wideRows,
                        CancellationToken.None
                    );
                    _wideEncrypted = await _parquetWriter!.WriteParquetAsync(unenc, "default");
                    _wideEncryptedKey = await S3BenchmarkHelper.UploadAsync(
                        _s3Client!,
                        _bucketName,
                        _wideEncrypted,
                        "measurement1-wide"
                    );
                    _wideEncrypted.Dispose();
                    _wideEncrypted = null;
                }
                break;
            case "DeepDataset":
                if (string.IsNullOrEmpty(_deepEncryptedKey))
                {
                    Console.WriteLine("[Measurement1] generating & uploading deep dataset");
                    var unenc = await DeepDatasetGenerator.GenerateAsync(
                        _deepCols,
                        _deepRows,
                        CancellationToken.None
                    );
                    _deepEncrypted = await _parquetWriter!.WriteParquetAsync(unenc, "default");
                    _deepEncryptedKey = await S3BenchmarkHelper.UploadAsync(
                        _s3Client!,
                        _bucketName,
                        _deepEncrypted,
                        "measurement1-deep"
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
