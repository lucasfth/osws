using System.Collections.Concurrent;
using BitFaster.Caching.Lru;

namespace OSWS.ParquetSolver.Helpers;

/// <summary>
/// Thread-safe in-memory cache for unwrapped Data Encryption Keys (DEKs).
/// Unwrapped DEKs are cached by a caller-provided cache key.
/// In practice this is the KEK identifier plus encrypted DEK identity, which allows
/// multiple DEKs to share the same file-level KEK without cache collisions.
/// </summary>
public class DekCache
{
    private readonly ConcurrentLru<string, byte[]> _cache;

    /// <summary>
    /// Create a new cache capable of holding <paramref name="capacity"/> entries before evicting.
    /// The wide dataset used in benchmarks has 2 000 columns, so the default capacity was raised
    /// to avoid evictions during benchmark runs.  Production services may use a smaller value.
    /// </summary>
    /// <param name="capacity">Maximum number of cached DEKs (default 2500).</param>
    public DekCache(int capacity = 2500)
    {
        _cache = new ConcurrentLru<string, byte[]>(capacity);
    }

    /// <summary>
    /// Try to retrieve a cached decrypted DEK by its cache key.
    /// </summary>
    /// <param name="kekId">The cache key for the decrypted DEK</param>
    /// <param name="dek">The decrypted DEK bytes if found, null otherwise</param>
    /// <returns>True if the DEK was cached, false otherwise</returns>
    public bool TryGet(string kekId, out byte[]? dek)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(kekId);
        return _cache.TryGet(kekId, out dek);
    }

    /// <summary>
    /// Cache a decrypted DEK under its cache key.
    /// </summary>
    /// <param name="kekId">The cache key for the decrypted DEK</param>
    /// <param name="dek">The decrypted DEK bytes (must not be null)</param>
    public void Set(string kekId, byte[] dek)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(kekId);
        ArgumentNullException.ThrowIfNull(dek);
        _cache.AddOrUpdate(kekId, dek);
    }

    /// <summary>
    /// Clear all cached DEKs.
    /// </summary>
    public void Clear()
    {
        _cache.Clear();
    }

    /// <summary>
    /// Get the current number of cached DEKs.
    /// </summary>
    public int Count => _cache.Count;
}
