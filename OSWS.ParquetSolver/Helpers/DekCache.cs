using System.Collections.Concurrent;
using OSWS.ParquetSolver.Interfaces;

namespace OSWS.ParquetSolver.Helpers;

/// <summary>
/// Thread-safe in-memory DEK cache with TTL-based expiry and capacity-bounded eviction.
/// </summary>
/// <remarks>
/// TODO (RBAC): When RBAC is implemented, pass the caller's role into <see cref="Set"/> so
/// that TTL can be derived per-entry. Higher privilege → shorter TTL; lower privilege → longer TTL.
/// </remarks>
public class DekCache : IDekCache
{
    private record Entry(byte[] Dek, DateTime ExpiresAt);

    private readonly ConcurrentDictionary<string, Entry> _cache = new();
    private readonly int _capacity;
    private readonly TimeSpan? _ttl;

    /// <param name="capacity">Maximum number of entries before forced eviction (default 2500).</param>
    /// <param name="ttl">Entry lifetime. Null means no expiry until RBAC TTLs are wired in.</param>
    public DekCache(int capacity = 2500, TimeSpan? ttl = null)
    {
        _capacity = capacity;
        _ttl = ttl;
    }

    public bool TryGet(string kekId, out byte[]? dek)
    {
        dek = null;
        if (!_cache.TryGetValue(kekId, out var entry))
            return false;

        if (_ttl.HasValue && DateTime.UtcNow > entry.ExpiresAt)
        {
            _cache.TryRemove(kekId, out _);
            return false;
        }

        dek = entry.Dek;
        return true;
    }

    public void Set(string kekId, byte[] dek)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(kekId);
        ArgumentNullException.ThrowIfNull(dek);

        // TODO (RBAC): Derive TTL from caller's role instead of the configured default.
        var expiresAt = _ttl.HasValue ? DateTime.UtcNow.Add(_ttl.Value) : DateTime.MaxValue;
        _cache.AddOrUpdate(kekId, new Entry(dek, expiresAt), (_, _) => new Entry(dek, expiresAt));

        if (_cache.Count > _capacity)
            EvictExpiredOrOldest();
    }

    public void Clear() => _cache.Clear();

    private void EvictExpiredOrOldest()
    {
        var now = DateTime.UtcNow;

        // Remove all expired entries first
        foreach (var (key, entry) in _cache)
            if (entry.ExpiresAt < now)
                _cache.TryRemove(key, out _);

        // If still over capacity, evict the entry expiring soonest
        if (_cache.Count > _capacity)
        {
            var oldest = _cache.MinBy(kv => kv.Value.ExpiresAt).Key;
            _cache.TryRemove(oldest, out _);
        }
    }
}
