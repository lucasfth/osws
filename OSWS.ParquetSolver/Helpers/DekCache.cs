using System.Collections.Concurrent;
using BitFaster.Caching.Lru;

namespace OSWS.ParquetSolver.Helpers;

/// <summary>
/// Thread-safe in-memory cache for unwrapped Data Encryption Keys (DEKs).
/// Unwrapped DEKs are cached by their unique Key Encryption Key (KEK) identifier.
/// This avoids repeated Key Vault unwrap calls for the same DEK.
/// Each parquet column and footer has its own unique GUID-based KEK ID.
/// </summary>
public class DekCache
{
    private readonly ConcurrentLru<string, byte[]> _cache = new(500);

    /// <summary>
    /// Try to retrieve a cached decrypted DEK by its KEK ID.
    /// </summary>
    /// <param name="kekId">The unique Key Encryption Key identifier (e.g., Azure KV key ID)</param>
    /// <param name="dek">The decrypted DEK bytes if found, null otherwise</param>
    /// <returns>True if the DEK was cached, false otherwise</returns>
    public bool TryGet(string kekId, out byte[]? dek)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(kekId);
        return _cache.TryGet(kekId, out dek);
    }

    /// <summary>
    /// Cache a decrypted DEK under its KEK ID.
    /// </summary>
    /// <param name="kekId">The unique Key Encryption Key identifier</param>
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
