using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using OSWS.Library;

namespace OSWS.Performance.Benchmarks.Infrastructure;

public static class ParquetSeedUploader
{
    public static async Task<int> RunAsync(string[] args)
    {
        var endpoint = GetRequiredArg(args, "--endpoint");
        var accessKey = GetRequiredArg(args, "--access-key");
        var secretKey = GetRequiredArg(args, "--secret-key");
        var bucket = GetRequiredArg(args, "--bucket");
        var prefix = GetArg(args, "--prefix") ?? "parquet/";
        var sampleDir = GetRequiredArg(args, "--sample-dir");

        var endpointNormalized = AwsCredentialHelper.NormalizeEndpoint(endpoint);
        if (string.IsNullOrWhiteSpace(endpointNormalized))
        {
            Console.Error.WriteLine("seed-parquet: invalid endpoint");
            return 1;
        }

        if (!Directory.Exists(sampleDir))
        {
            Console.Error.WriteLine($"seed-parquet: sample directory not found: {sampleDir}");
            return 1;
        }

        var sampleFiles = Directory
            .GetFiles(sampleDir, "*.parquet", SearchOption.TopDirectoryOnly)
            .OrderBy(x => x, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        if (sampleFiles.Length == 0)
        {
            Console.Error.WriteLine($"seed-parquet: no .parquet files found in: {sampleDir}");
            return 1;
        }

        var s3Config = new AmazonS3Config
        {
            ServiceURL = endpointNormalized,
            ForcePathStyle = true,
        };

        var creds = new BasicAWSCredentials(accessKey, secretKey);
        using var s3 = new AmazonS3Client(creds, s3Config);

        await EnsureBucketExistsAsync(s3, bucket);

        var normalizedPrefix = prefix.EndsWith("/", StringComparison.Ordinal) ? prefix : prefix + "/";

        // If parquet objects are already present under prefix, keep existing corpus.
        var existing = await s3.ListObjectsV2Async(new ListObjectsV2Request
        {
            BucketName = bucket,
            Prefix = normalizedPrefix,
            MaxKeys = 100,
        });

        var existingObjects = existing.S3Objects ?? [];
        if (existingObjects.Any(o => (o.Key ?? string.Empty).EndsWith(".parquet", StringComparison.OrdinalIgnoreCase)))
        {
            Console.WriteLine($"seed-parquet: found existing parquet objects under s3://{bucket}/{normalizedPrefix}");
            return 0;
        }

        foreach (var samplePath in sampleFiles)
        {
            var key = normalizedPrefix + Path.GetFileName(samplePath);
            await using var fs = File.OpenRead(samplePath);

            var put = new PutObjectRequest
            {
                BucketName = bucket,
                Key = key,
                InputStream = fs,
                ContentType = "application/vnd.apache.parquet",
                UseChunkEncoding = false,
            };

            var response = await s3.PutObjectAsync(put);
            Console.WriteLine($"seed-parquet: uploaded {key} (etag={response.ETag})");
        }

        return 0;
    }

    private static async Task EnsureBucketExistsAsync(IAmazonS3 s3, string bucket)
    {
        var buckets = await s3.ListBucketsAsync();
        var bucketList = buckets.Buckets ?? [];
        if (bucketList.Any(b => string.Equals(b.BucketName, bucket, StringComparison.Ordinal)))
        {
            return;
        }

        await s3.PutBucketAsync(new PutBucketRequest { BucketName = bucket });
    }

    private static string GetRequiredArg(string[] args, string key)
    {
        return GetArg(args, key)
            ?? throw new ArgumentException($"Missing required argument: {key}");
    }

    private static string? GetArg(string[] args, string key)
    {
        for (var i = 0; i < args.Length - 1; i++)
        {
            if (string.Equals(args[i], key, StringComparison.OrdinalIgnoreCase))
            {
                return args[i + 1];
            }
        }

        return null;
    }
}
