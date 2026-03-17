namespace OSWS.ParquetSolver.Interfaces;

/// <summary>
/// Abstraction over the encrypted parquet file cache.
/// Implementations: <see cref="Helpers.EncryptedFileCache"/> (local disk) and
/// <see cref="Helpers.RedisEncryptedFileCache"/> (Redis).
/// </summary>
public interface IEncryptedFileCache
{
    /// <summary>Try to retrieve a cached encrypted file stream.</summary>
    bool TryGet(string cacheKey, out Stream? stream);

    /// <summary>Write an encrypted file stream into the cache.</summary>
    Task SetAsync(string cacheKey, Stream stream, CancellationToken cancellationToken = default);

    /// <summary>Remove all entries from the cache.</summary>
    Task ClearAsync();

    /// <summary>Return basic cache statistics (file count, total bytes, max bytes).</summary>
    (int FileCount, long TotalBytes, long MaxBytes) GetStats();

    /// <summary>Return a human-readable debug dump of cache state.</summary>
    string GetDebugInfo();
}
