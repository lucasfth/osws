using Amazon.S3;
using Amazon.S3.Model;

namespace OSWS.Performance.Benchmarks.Helpers;

/// <summary>
/// Simple S3/R2 operations for benchmarks.
/// </summary>
public static class S3BenchmarkHelper
{
    public static async Task<string> UploadAsync(
        IAmazonS3 s3Client,
        string bucket,
        Stream stream,
        string keyPrefix
    )
    {
        var key = GenerateKey(keyPrefix);
        stream.Position = 0;

        await s3Client.PutObjectAsync(
            new PutObjectRequest
            {
                BucketName = bucket,
                Key = key,
                InputStream = stream,
                ContentType = "application/octet-stream",
            }
        );

        return key;
    }

    public static async Task<Stream> DownloadAsync(IAmazonS3 s3Client, string bucket, string key)
    {
        var response = await s3Client.GetObjectAsync(bucket, key);
        var memory = new MemoryStream();
        await response.ResponseStream.CopyToAsync(memory);
        memory.Position = 0;
        return memory;
    }

    public static string GenerateKey(string prefix)
    {
        var timestamp = DateTime.UtcNow.ToString("yyyyMMdd-HHmmss");
        var guid = Guid.NewGuid().ToString("N")[..8];
        return $"{prefix}-{timestamp}-{guid}.parquet";
    }

    public static async Task DeleteAsync(IAmazonS3 s3Client, string bucket, List<string> keys)
    {
        if (keys.Count == 0)
            return;

        foreach (var batch in keys.Chunk(1000))
        {
            await s3Client.DeleteObjectsAsync(
                new DeleteObjectsRequest
                {
                    BucketName = bucket,
                    Objects = batch.Select(k => new KeyVersion { Key = k }).ToList(),
                }
            );
        }
    }

    public static async Task EnsureBucketAsync(IAmazonS3 s3Client, string bucket)
    {
        try
        {
            await s3Client.ListObjectsV2Async(
                new ListObjectsV2Request { BucketName = bucket, MaxKeys = 1 }
            );
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            await s3Client.PutBucketAsync(bucket);
            Console.WriteLine($"Created bucket: {bucket}");
        }
    }
}
