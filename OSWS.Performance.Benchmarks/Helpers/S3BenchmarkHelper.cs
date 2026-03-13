using System.Diagnostics;
using Amazon.S3;
using Amazon.S3.Model;
using Azure;
using OSWS.ParquetSolver;
using OSWS.Performance.Benchmarks.DatasetGenerators;

namespace OSWS.Performance.Benchmarks.Helpers;

/// <summary>
/// Simple S3/R2 operations for benchmarks.
/// </summary>
public static class S3BenchmarkHelper
{
    private const int Wait = 1000;

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
                // Cloudflare R2 does not support AWS streaming payload trailers.
                // Force a regular signed payload upload instead of chunked streaming.
                UseChunkEncoding = false,
            }
        );

        return key;
    }

    public static async Task<Stream> DownloadAsync(
        IAmazonS3 s3Client,
        string bucket,
        string key,
        Action<TimeSpan>? onS3OperationLatency = null
    )
    {
        var stopwatch = onS3OperationLatency != null ? Stopwatch.StartNew() : null;
        var response = await s3Client.GetObjectAsync(bucket, key);
        if (stopwatch != null)
        {
            stopwatch.Stop();
            onS3OperationLatency?.Invoke(stopwatch.Elapsed);
        }
        var memory = new MemoryStream();
        await response.ResponseStream.CopyToAsync(memory);
        memory.Position = 0;
        return memory;
    }

    private static string GenerateKey(string prefix)
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

    /// <summary>
    /// Retry with exponential backoff for operations that may hit Azure Key Vault rate limits.
    /// Handles 429 Throttled responses with configurable backoff.
    /// Max 10 attempts with delays: 3s, 6s, 12s, 24s, 48s, 60s, 60s, 60s, 60s
    /// </summary>
    private static async Task<T> RetryWithBackoffAsync<T>(
        Func<Task<T>> operation,
        string operationName,
        int maxRetries = 10,
        int initialDelayMs = 3000
    )
    {
        int delayMs = initialDelayMs;
        for (int attempt = 1; attempt <= maxRetries; attempt++)
        {
            try
            {
                return await operation();
            }
            catch (Azure.RequestFailedException ex) when (ex.Status == 429)
            {
                if (attempt == maxRetries)
                {
                    Console.WriteLine(
                        $"[{operationName}] Max retries ({maxRetries}) exceeded. Giving up."
                    );
                    throw;
                }

                Console.WriteLine(
                    $"[{operationName}] Throttled (429). Retry {attempt}/{maxRetries - 1}, waiting {delayMs}ms..."
                );
                await Task.Delay(delayMs);
                delayMs = Math.Min(delayMs * 2, 60000); // cap at 60 seconds
            }
        }

        throw new InvalidOperationException(
            $"[{operationName}] Operation did not complete after {maxRetries} attempts."
        );
    }

    public static async Task InitEncryptionFiles(EncryptionObj encryptionObj)
    {
        if (
            encryptionObj.ParquetWriter == null
            || encryptionObj.S3Client == null
            || string.IsNullOrEmpty(encryptionObj.BucketName)
            || string.IsNullOrEmpty(encryptionObj.KeyPrefix)
        )
            throw new InvalidOperationException("EncryptionObj not properly initialized");

        var smallInitialized =
            encryptionObj.SmallEnc != null && !string.IsNullOrWhiteSpace(encryptionObj.SmallKey);
        var wideInitialized =
            encryptionObj.WideEnc != null && !string.IsNullOrWhiteSpace(encryptionObj.WideKey);
        var deepInitialized =
            encryptionObj.DeepEnc != null && !string.IsNullOrWhiteSpace(encryptionObj.DeepKey);

        if (smallInitialized && wideInitialized && deepInitialized)
            return;

        var hasAnyInitialized =
            smallInitialized
            || wideInitialized
            || deepInitialized
            || encryptionObj.SmallEnc != null
            || encryptionObj.WideEnc != null
            || encryptionObj.DeepEnc != null
            || !string.IsNullOrWhiteSpace(encryptionObj.SmallKey)
            || !string.IsNullOrWhiteSpace(encryptionObj.WideKey)
            || !string.IsNullOrWhiteSpace(encryptionObj.DeepKey);

        if (hasAnyInitialized)
            throw new InvalidOperationException(
                "EncryptionObj is partially initialized. Provide all keys and streams, or none."
            );

        var smallUnenc = await SmallDatasetGenerator.GenerateAsync(
            encryptionObj.SmallCols,
            encryptionObj.SmallRows,
            CancellationToken.None
        );
        encryptionObj.SmallEnc = await RetryWithBackoffAsync(
            () => encryptionObj.ParquetWriter.WriteParquetAsync(smallUnenc, "default"),
            "Small dataset encryption"
        );
        {
            encryptionObj.SmallKey = await UploadAsync(
                encryptionObj.S3Client,
                encryptionObj.BucketName,
                encryptionObj.SmallEnc,
                $"{encryptionObj.KeyPrefix}-small"
            );
            Console.WriteLine($"[Encryption Setup] Uploaded small: {encryptionObj.SmallKey}");
        }

        // Wait to allow Azure Key Vault rate limit window to reset before next dataset
        Console.WriteLine(
            $"[Encryption Setup] Waiting {Wait / 1000}s for KV rate limits to reset..."
        );
        await Task.Delay(Wait);

        var wideUnenc = await WideDatasetGenerator.GenerateAsync(
            encryptionObj.WideCols,
            encryptionObj.WideRows,
            CancellationToken.None
        );
        encryptionObj.WideEnc = await RetryWithBackoffAsync(
            () => encryptionObj.ParquetWriter.WriteParquetAsync(wideUnenc, "default"),
            "Wide dataset encryption"
        );
        {
            encryptionObj.WideKey = await UploadAsync(
                encryptionObj.S3Client,
                encryptionObj.BucketName,
                encryptionObj.WideEnc,
                $"{encryptionObj.KeyPrefix}-wide"
            );
            Console.WriteLine($"[Encryption Setup] Uploaded wide: {encryptionObj.WideKey}");
        }

        // Wait to allow Azure Key Vault rate limit window to reset before next dataset
        Console.WriteLine(
            $"[Encryption Setup] Waiting {Wait / 1000}s for KV rate limits to reset..."
        );
        await Task.Delay(Wait);

        var deepUnenc = await DeepDatasetGenerator.GenerateAsync(
            encryptionObj.DeepCols,
            encryptionObj.DeepRows,
            CancellationToken.None
        );
        encryptionObj.DeepEnc = await RetryWithBackoffAsync(
            () => encryptionObj.ParquetWriter.WriteParquetAsync(deepUnenc, "default"),
            "Deep dataset encryption"
        );
        {
            encryptionObj.DeepKey = await UploadAsync(
                encryptionObj.S3Client,
                encryptionObj.BucketName,
                encryptionObj.DeepEnc,
                $"{encryptionObj.KeyPrefix}-deep"
            );
            Console.WriteLine($"[Encryption Setup] Uploaded deep: {encryptionObj.DeepKey}");
        }
    }

    public class EncryptionObj
    {
        public required ParquetWriter ParquetWriter { get; init; }
        public required IAmazonS3 S3Client { get; init; }
        public required string BucketName { get; init; }
        public required string KeyPrefix { get; init; }
        public int SmallCols { get; init; }
        public int SmallRows { get; init; }
        public Stream? SmallEnc { get; set; }
        public string? SmallKey { get; set; }
        public int WideCols { get; init; }
        public int WideRows { get; init; }
        public Stream? WideEnc { get; set; }
        public string? WideKey { get; set; }
        public int DeepCols { get; init; }
        public int DeepRows { get; init; }
        public Stream? DeepEnc { get; set; }
        public string? DeepKey { get; set; }
    }
}
