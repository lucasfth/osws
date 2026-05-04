using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using OSWS.Performance.Benchmarks.DatasetGenerators;

namespace OSWS.Performance.Benchmarks.Infrastructure;

/// <summary>
/// Generates and uploads the benchmark corpus to S3.
///
/// Corpus layout:
///   bench/s3-direct/{size}.parquet        — plaintext, uploaded directly to R2
///   bench/osws/warm/{size}.parquet        — uploaded through OSWS (encrypted in R2)
///   bench/osws/cold/{size}/{001..N}.parquet — cold copies through OSWS (distinct DEKs)
/// </summary>
public static class BenchmarkCorpusUploader
{
    public static async Task<int> RunAsync(string[] args)
    {
        var oswsEndpoint = GetArg(args, "--osws-endpoint") ?? GetEnv("OSWS_ENDPOINT");
        var oswsAccessKey = GetArg(args, "--osws-access-key") ?? GetEnv("BENCH_OSWS_ACCESS_KEY");
        var oswsSecretKey = GetArg(args, "--osws-secret-key") ?? GetEnv("BENCH_OSWS_SECRET_KEY");

        var s3Endpoint = GetArg(args, "--s3-endpoint") ?? GetEnv("S3Settings__EndpointHostname");
        var s3AccessKey = GetArg(args, "--s3-access-key") ?? GetEnv("S3Settings__AccessKeyId");
        var s3SecretKey = GetArg(args, "--s3-secret-key") ?? GetEnv("S3Settings__SecretAccessKey");

        var bucket = GetArg(args, "--bucket") ?? GetEnv("BENCH_BUCKET") ?? "osws-benchmark";
        var coldCopiesStr = GetArg(args, "--cold-copies") ?? GetEnv("BENCH_REPETITIONS") ?? "10";
        var skipExisting = HasFlag(args, "--skip-existing");
        var datasetDir = GetArg(args, "--dataset-dir") ?? "benchmark-datasets";
        if (
            string.IsNullOrWhiteSpace(oswsEndpoint)
            || string.IsNullOrWhiteSpace(oswsAccessKey)
            || string.IsNullOrWhiteSpace(oswsSecretKey)
        )
        {
            Console.Error.WriteLine("generate-corpus: OSWS endpoint and credentials are required.");
            Console.Error.WriteLine(
                "  Set OSWS_ENDPOINT, BENCH_OSWS_ACCESS_KEY, BENCH_OSWS_SECRET_KEY in .env"
            );
            Console.Error.WriteLine(
                "  or pass --osws-endpoint, --osws-access-key, --osws-secret-key"
            );
            return 1;
        }

        if (
            string.IsNullOrWhiteSpace(s3Endpoint)
            || string.IsNullOrWhiteSpace(s3AccessKey)
            || string.IsNullOrWhiteSpace(s3SecretKey)
        )
        {
            Console.Error.WriteLine(
                "generate-corpus: S3 (direct) endpoint and credentials are required."
            );
            Console.Error.WriteLine(
                "  Set S3Settings__EndpointHostname, S3Settings__AccessKeyId, S3Settings__SecretAccessKey in .env"
            );
            return 1;
        }

        if (!int.TryParse(coldCopiesStr, out var coldCopies) || coldCopies < 1)
        {
            Console.Error.WriteLine("generate-corpus: --cold-copies must be a positive integer");
            return 1;
        }

        using var osws = BuildS3Client(oswsEndpoint, oswsAccessKey, oswsSecretKey);
        using var s3Direct = BuildS3Client(s3Endpoint, s3AccessKey, s3SecretKey);

        Console.WriteLine("Starting upload of benchmark datasets");
        Console.WriteLine("Configuration:");
        Console.WriteLine($"  OSWS endpoint : {oswsEndpoint}");
        Console.WriteLine($"  S3 endpoint   : {s3Endpoint}");
        Console.WriteLine($"  Bucket        : {bucket}");
        Console.WriteLine($"  Dataset dir   : {Path.GetFullPath(datasetDir)}");
        Console.WriteLine(
            $"  File sizes    : {string.Join(", ", ParquetGenerator.CorpusSizes.Keys)}"
        );
        Console.WriteLine($"  Cold copies   : {coldCopies}");

        foreach (var (sizeLabel, rowCount) in ParquetGenerator.CorpusSizes)
        {
            Console.WriteLine($"{sizeLabel} ({rowCount:N0} rows):");

            var localFile = Path.Combine(datasetDir, $"{sizeLabel}.parquet");
            if (!File.Exists(localFile))
            {
                Console.Error.WriteLine($"generate-corpus: dataset file not found: {localFile}");
                Console.Error.WriteLine("Run first:  dotnet run -- generate-datasets");
                return 1;
            }

            var fileSizeMb = new FileInfo(localFile).Length / 1024.0 / 1024.0;
            Console.WriteLine($"  Dataset: {localFile} ({fileSizeMb:F1} MB)");

            // 1. Upload directly to S3 (plaintext — for s3-direct config)
            var directKey = $"bench/s3-direct/{sizeLabel}.parquet";
            await UploadFileAsync(
                s3Direct,
                bucket,
                directKey,
                localFile,
                "s3-direct",
                skipExisting
            );

            // 2. Upload warm copy through OSWS
            var warmKey = $"bench/osws/warm/{sizeLabel}.parquet";
            await UploadFileAsync(osws, bucket, warmKey, localFile, "osws-warm", skipExisting);

            // 3. Upload cold copies through OSWS (each gets a distinct DEK)
            for (var i = 1; i <= coldCopies; i++)
            {
                var coldKey = $"bench/osws/cold/{sizeLabel}/{i:D3}.parquet";
                await UploadFileAsync(
                    osws,
                    bucket,
                    coldKey,
                    localFile,
                    $"osws-cold-{i:D3}",
                    skipExisting
                );
            }

            Console.WriteLine();
        }

        Console.WriteLine("Finished upload of benchmark datasets");
        Console.WriteLine();
        Console.WriteLine("Next steps:");
        Console.WriteLine("  1. Start OSWS in the desired configuration");
        Console.WriteLine("  2. Run: python Infrastructure/run-benchmark.py --config <name>");
        return 0;
    }

    private static async Task UploadFileAsync(
        IAmazonS3 s3,
        string bucket,
        string key,
        string localPath,
        string label,
        bool skipExisting
    )
    {
        if (skipExisting)
        {
            try
            {
                await s3.GetObjectMetadataAsync(bucket, key);
                Console.WriteLine($"  ↷  {label}: already exists, skipping ({key})");
                return;
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                // Not found — proceed with upload
            }
        }

        Console.Write($"  ↑  {label}: uploading {key}... ");
        await using var fs = File.OpenRead(localPath);
        await s3.PutObjectAsync(
            new PutObjectRequest
            {
                BucketName = bucket,
                Key = key,
                InputStream = fs,
                ContentType = "application/vnd.apache.parquet",
                UseChunkEncoding = false,
            }
        );
        Console.WriteLine("done");
    }

    private static AmazonS3Client BuildS3Client(string endpoint, string accessKey, string secretKey)
    {
        var config = new AmazonS3Config { ForcePathStyle = true, ServiceURL = endpoint };
        return new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey), config);
    }

    private static string? GetArg(string[] args, string key)
    {
        for (var i = 0; i < args.Length - 1; i++)
            if (string.Equals(args[i], key, StringComparison.OrdinalIgnoreCase))
                return args[i + 1];
        return null;
    }

    private static bool HasFlag(string[] args, string flag) =>
        args.Any(a => string.Equals(a, flag, StringComparison.OrdinalIgnoreCase));

    private static string? GetEnv(string key) => Environment.GetEnvironmentVariable(key); }
