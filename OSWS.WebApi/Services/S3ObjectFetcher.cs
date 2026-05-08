using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.Extensions.Logging;
using OSWS.ParquetSolver.Helpers;
using OSWS.ParquetSolver.Interfaces;

namespace OSWS.WebApi.Services;

/// <summary>
/// Fetches parquet files as fully-decrypted plaintext bytes, using DecryptedParquetCache.
/// On a cache miss, fetches the encrypted file from S3, decrypts it, caches the
/// plaintext, and returns it. Subsequent calls return from cache with no key vault interaction.
/// </summary>
public sealed class S3ObjectFetcher(
    IAmazonS3 s3Client,
    DecryptedParquetCache plaintextCache,
    IParquetReader parquetReader,
    ILogger<S3ObjectFetcher> logger
)
{
    /// <summary>
    /// Fetch a parquet file as decrypted plaintext bytes.
    /// Returns (bytes, metadata). metadata is non-null only on a cache miss (S3 fetch).
    /// </summary>
    public async Task<(byte[] Plaintext, GetObjectResponse? S3Response)> FetchParquetAsync(
        string bucket,
        string key,
        CancellationToken cancellationToken
    )
    {
        var cacheKey = DecryptedParquetCache.GenerateCacheKey(bucket, key);

        if (plaintextCache.TryGet(cacheKey, out var cached) && cached != null)
        {
            logger.LogDebug("[S3ObjectFetcher] Cache hit for {Bucket}/{Key}", bucket, key);
            return (cached, null);
        }

        logger.LogDebug(
            "[S3ObjectFetcher] Cache miss. Fetching + decrypting {Bucket}/{Key}",
            bucket,
            key
        );
        var req = new GetObjectRequest { BucketName = bucket, Key = key };
        var resp = await s3Client.GetObjectAsync(req, cancellationToken).ConfigureAwait(false);

        var encryptedStream = new MemoryStream();
        await resp.ResponseStream.CopyToAsync(encryptedStream, cancellationToken);
        encryptedStream.Position = 0;

        var decryptedStream = await parquetReader.ReadParquetAsync(
            encryptedStream,
            allowedColumns: null
        );
        var plaintext = decryptedStream.ToArray();

        plaintextCache.Set(cacheKey, plaintext);
        logger.LogDebug(
            "[S3ObjectFetcher] Cached decrypted plaintext for {Bucket}/{Key}",
            bucket,
            key
        );

        return (plaintext, resp);
    }

    /// <summary>
    /// Fetch a non-parquet object directly from S3 (no caching).
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
