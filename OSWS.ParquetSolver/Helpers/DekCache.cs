using System.Collections.Concurrent;
using BitFaster.Caching.Lru;
using OSWS.ParquetSolver.Interfaces;

namespace OSWS.ParquetSolver.Helpers;

/// <summary>
/// Thread-safe in-memory cache for unwrapped Data Encryption Keys (DEKs).
/// Unwrapped DEKs are cached by a caller-provided cache key.
/// In practice this is the KEK identifier plus encrypted DEK identity, which allows
/// multiple DEKs to share the same file-level KEK without cache collisions.
/// </summary>
public class DekCache : IDekCache
{
    private readonly ConcurrentLru<string, byte[]> _cache;

    /// <summary>
    /// Create a new cache capable of holding <paramref name="capacity"/> entries before evicting.
    /// </summary>
    /// <param name="capacity">Maximum number of cached DEKs (default 2500).</param>
    public DekCache(int capacity = 2500)
    {
        _cache = new ConcurrentLru<string, byte[]>(capacity);
    }

    public bool TryGet(string kekId, out byte[]? dek)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(kekId);
        return _cache.TryGet(kekId, out dek);
    }

    public void Set(string kekId, byte[] dek)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(kekId);
        ArgumentNullException.ThrowIfNull(dek);
        _cache.AddOrUpdate(kekId, dek);
    }

    public void Clear() => _cache.Clear();

    public int Count => _cache.Count;
}
