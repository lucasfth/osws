using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using OSWS.Library;

namespace OSWS.Performance.Benchmarks.Infrastructure;

public static class S3BucketEnsurer
{
    public static async Task<int> RunAsync()
    {
        var endpoint = Environment.GetEnvironmentVariable("S3Settings__EndpointHostname");
        var accessKey = Environment.GetEnvironmentVariable("S3Settings__AccessKeyId");
        var secretKey = Environment.GetEnvironmentVariable("S3Settings__SecretAccessKey");
        var bucket = Environment.GetEnvironmentVariable("BENCH_BUCKET");

        if (
            string.IsNullOrWhiteSpace(endpoint)
            || string.IsNullOrWhiteSpace(accessKey)
            || string.IsNullOrWhiteSpace(secretKey)
            || string.IsNullOrWhiteSpace(bucket)
        )
        {
            Console.Error.WriteLine(
                "ensure-bucket: set S3Settings__EndpointHostname, S3Settings__AccessKeyId, "
                    + "S3Settings__SecretAccessKey, and BENCH_BUCKET in .env"
            );
            return 1;
        }

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
        using var s3 = new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey), s3Config);

        var buckets = await s3.ListBucketsAsync();
        if (
            (buckets.Buckets ?? []).Any(b =>
                string.Equals(b.BucketName, bucket, StringComparison.Ordinal)
            )
        )
        {
            Console.WriteLine($"ensure-bucket: exists s3://{bucket}");
            return 0;
        }

        await s3.PutBucketAsync(new PutBucketRequest { BucketName = bucket });
        Console.WriteLine($"ensure-bucket: created s3://{bucket}");
        return 0;
    }
}
