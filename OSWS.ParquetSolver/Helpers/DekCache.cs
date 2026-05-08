using System.Collections.Concurrent;
using OSWS.Common.Configuration;
using OSWS.ParquetSolver.Interfaces;

namespace OSWS.ParquetSolver.Helpers;

/// <summary>
/// Thread-safe in-memory DEK cache with role-based TTL expiry and capacity-bounded eviction.
/// Admin users get shorter TTL to reduce exposure window for privileged access.
/// </summary>
public class DekCache : IDekCache
{
    private record Entry(byte[] Dek, DateTime ExpiresAt);

    private readonly bool _enabled;
    private readonly ConcurrentDictionary<string, Entry> _cache = new();
    private readonly int _capacity;
    private readonly TimeSpan? _ttl;
    private readonly TimeSpan? _adminTtl;

    /// <param name="enabled">Whether the cache is enabled</param>
    /// <param name="capacity">Maximum number of entries before forced eviction (default 2500).</param>
    /// <param name="ttl">Entry lifetime for standard users. Null means no expiry.</param>
    /// <param name="adminTtl">Entry lifetime for admin users. Null falls back to standard ttl.</param>
    public DekCache(
        bool enabled = true,
        int capacity = 2500,
        TimeSpan? ttl = null,
        TimeSpan? adminTtl = null
    )
    {
        _enabled = enabled;
        _capacity = capacity;
        _ttl = ttl;
        _adminTtl = adminTtl;
    }

    public bool TryGet(string kekId, out byte[]? dek)
    {
        dek = null;
        if (!_enabled)
            return false;

        if (!_cache.TryGetValue(kekId, out var entry))
            return false;

        if (DateTime.UtcNow > entry.ExpiresAt)
        {
            _cache.TryRemove(kekId, out _);
            return false;
        }

        dek = entry.Dek;
        return true;
    }

    public void Set(string kekId, byte[] dek, bool isAdmin = false)
    {
        if (!_enabled)
            return;

        ArgumentException.ThrowIfNullOrWhiteSpace(kekId);
        ArgumentNullException.ThrowIfNull(dek);

        var effectiveTtl = isAdmin ? (_adminTtl ?? _ttl) : _ttl;
        var expiresAt = effectiveTtl.HasValue
            ? DateTime.UtcNow.Add(effectiveTtl.Value)
            : DateTime.MaxValue;

        _cache.AddOrUpdate(kekId, new Entry(dek, expiresAt), (_, _) => new Entry(dek, expiresAt));

        if (_cache.Count > _capacity)
            EvictExpiredOrOldest();
    }

    public void Clear() => _cache.Clear();

    private void EvictExpiredOrOldest()
    {
        var now = DateTime.UtcNow;

        foreach (var (key, entry) in _cache)
            if (entry.ExpiresAt < now)
                _cache.TryRemove(key, out _);

        if (_cache.Count > _capacity)
        {
            var oldest = _cache.MinBy(kv => kv.Value.ExpiresAt).Key;
            _cache.TryRemove(oldest, out _);
        }
    }
}
