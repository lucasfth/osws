using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.Extensions.Logging;
using OSWS.ParquetSolver.Helpers;

namespace OSWS.WebApi.Services;

/// <summary>
/// Fetches S3 objects with disk cache support for encrypted parquet files.
/// </summary>
public sealed class S3ObjectFetcher(
    IAmazonS3 s3Client,
    EncryptedFileCache fileCache,
    ILogger<S3ObjectFetcher> logger
)
{
    /// <summary>
    /// Result of fetching an S3 object — either from cache or from S3.
    /// </summary>
    public record FetchResult(Stream? EncryptedStream, GetObjectResponse? S3Response);

    /// <summary>
    /// Fetch a parquet file, checking the disk cache first.
    /// If not cached, fetches from S3 and caches the encrypted stream.
    /// </summary>
    public async Task<FetchResult> FetchParquetAsync(
        string bucket,
        string key,
        CancellationToken cancellationToken
    )
    {
        var cacheKey = EncryptedFileCache.GenerateCacheKey(bucket, key);

        if (fileCache.TryGet(cacheKey, out var cachedStream))
            return new FetchResult(cachedStream, null);

        var req = new GetObjectRequest { BucketName = bucket, Key = key };
        var resp = await s3Client.GetObjectAsync(req, cancellationToken).ConfigureAwait(false);

        var memStream = new MemoryStream();
        await resp.ResponseStream.CopyToAsync(memStream, cancellationToken);
        memStream.Position = 0;

        _ = fileCache
            .SetAsync(cacheKey, memStream, cancellationToken)
            .ContinueWith(
                task =>
                {
                    if (task.IsFaulted)
                        logger.LogWarning(
                            "Cache failure for {CacheKey}: {Error}",
                            cacheKey,
                            task.Exception?.InnerException?.Message
                        );
                },
                TaskScheduler.Default
            );

        return new FetchResult(memStream, resp);
    }

    /// <summary>
    /// Fetch a non-parquet object directly from S3.
    /// </summary>
    public async Task<GetObjectResponse> FetchObjectAsync(
        string bucket,
        string key,
        CancellationToken cancellationToken
    )
    {
        var req = new GetObjectRequest { BucketName = bucket, Key = key };
        return await s3Client.GetObjectAsync(req, cancellationToken).ConfigureAwait(false);
    }
}
