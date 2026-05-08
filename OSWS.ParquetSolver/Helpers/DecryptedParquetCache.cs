using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using OSWS.Common.Configuration;

namespace OSWS.ParquetSolver.Helpers;

/// <summary>
/// Thread-safe in-memory cache of fully-decrypted (all-columns) parquet bytes with LRU eviction.
/// Column masking is applied at serve time. No KV interactions or crypto needed on cache hits.
/// </summary>
public sealed class DecryptedParquetCache(CacheSettings settings)
{
    private readonly CacheSettings _settings =
        settings ?? throw new ArgumentNullException(nameof(settings));
    private readonly ConcurrentDictionary<string, CacheEntry> _entries = new();
    private readonly Lock _lock = new();
    private long _currentSize;

    private sealed class CacheEntry
    {
        public required byte[] Bytes { get; init; }
        public DateTime LastAccessTime { get; set; }
    }

    public static string GenerateCacheKey(string bucket, string key)
    {
        var combined = $"{bucket}::{key}";
        var hashBytes = SHA256.HashData(Encoding.UTF8.GetBytes(combined));
        return Convert.ToHexString(hashBytes).ToLowerInvariant();
    }

    public bool TryGet(string cacheKey, out byte[]? bytes)
    {
        bytes = null;
        if (!_settings.EnableFileCache)
            return false;

        if (!_entries.TryGetValue(cacheKey, out var entry))
            return false;

        entry.LastAccessTime = DateTime.UtcNow;
        bytes = entry.Bytes;
        return true;
    }

    public void Set(string cacheKey, byte[] bytes)
    {
        if (!_settings.EnableFileCache)
            return;

        lock (_lock)
        {
            while (_currentSize + bytes.Length > _settings.MaxCacheSizeBytes && !_entries.IsEmpty)
                EvictLru();

            if (_entries.TryGetValue(cacheKey, out var existing))
                Interlocked.Add(ref _currentSize, -existing.Bytes.Length);

            _entries[cacheKey] = new CacheEntry { Bytes = bytes, LastAccessTime = DateTime.UtcNow };
            Interlocked.Add(ref _currentSize, bytes.Length);
        }
    }

    public void Invalidate(string cacheKey)
    {
        if (_entries.TryRemove(cacheKey, out var entry))
            Interlocked.Add(ref _currentSize, -entry.Bytes.Length);
    }

    private void EvictLru()
    {
        var lru = _entries.MinBy(kvp => kvp.Value.LastAccessTime);
        if (_entries.TryRemove(lru.Key, out var removed))
            Interlocked.Add(ref _currentSize, -removed.Bytes.Length);
    }

    public (int Count, long SizeBytes, long MaxSizeBytes) GetStats() =>
        (_entries.Count, _currentSize, _settings.MaxCacheSizeBytes);
}
