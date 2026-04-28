using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using OSWS.Library;

namespace OSWS.Performance.Benchmarks.Infrastructure;

public static class S3BucketEnsurer
{
    public static async Task<int> RunAsync(string[] args)
    {
        var endpoint = GetRequiredArg(args, "--endpoint");
        var accessKey = GetRequiredArg(args, "--access-key");
        var secretKey = GetRequiredArg(args, "--secret-key");
        var bucket = GetRequiredArg(args, "--bucket");

        var endpointNormalized = AwsCredentialHelper.NormalizeEndpoint(endpoint);
        if (string.IsNullOrWhiteSpace(endpointNormalized))
        {
            Console.Error.WriteLine("ensure-bucket: invalid endpoint");
            return 1;
        }

        var s3Config = new AmazonS3Config
        {
            ServiceURL = endpointNormalized,
            ForcePathStyle = true,
        };

        var creds = new BasicAWSCredentials(accessKey, secretKey);
        using var s3 = new AmazonS3Client(creds, s3Config);

        var buckets = await s3.ListBucketsAsync();
        var bucketList = buckets.Buckets ?? [];
        if (bucketList.Any(b => string.Equals(b.BucketName, bucket, StringComparison.Ordinal)))
        {
            Console.WriteLine($"ensure-bucket: exists s3://{bucket}");
            return 0;
        }

        await s3.PutBucketAsync(new PutBucketRequest { BucketName = bucket });
        Console.WriteLine($"ensure-bucket: created s3://{bucket}");
        return 0;
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
