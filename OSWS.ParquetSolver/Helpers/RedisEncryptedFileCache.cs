using OSWS.Common.Configuration;
using OSWS.ParquetSolver.Interfaces;
using StackExchange.Redis;

namespace OSWS.ParquetSolver.Helpers;

/// <summary>
/// Redis-backed cache for encrypted parquet files.
/// Files are stored as binary strings under keys: <c>{prefix}:file:{cacheKey}</c>.
/// Eviction is handled by Redis maxmemory policy rather than manual LRU.
/// </summary>
public class RedisEncryptedFileCache : IEncryptedFileCache
{
    private readonly IDatabase _db;
    private readonly string _prefix;
    private readonly bool _enabled;
    private readonly TimeSpan? _ttl;

    public RedisEncryptedFileCache(IConnectionMultiplexer redis, CacheSettings settings)
    {
        _db = redis.GetDatabase();
        _prefix = settings.RedisKeyPrefix ?? "osws";
        _enabled = settings.EnableFileCache;
        _ttl = settings.RedisTtlSeconds > 0
            ? TimeSpan.FromSeconds(settings.RedisTtlSeconds)
            : null;
    }

    private string Key(string cacheKey) => $"{_prefix}:file:{cacheKey}";

    public bool TryGet(string cacheKey, out Stream? stream)
    {
        stream = null;
        if (!_enabled) return false;

        var value = _db.StringGet(Key(cacheKey));
        if (!value.HasValue) return false;

        stream = new MemoryStream((byte[])value!);
        return true;
    }

    public async Task SetAsync(string cacheKey, Stream inputStream, CancellationToken cancellationToken = default)
    {
        if (!_enabled) return;

        var originalPosition = inputStream.Position;
        inputStream.Position = 0;
        using var ms = new MemoryStream();
        await inputStream.CopyToAsync(ms, cancellationToken);
        inputStream.Position = originalPosition;

        var bytes = ms.ToArray();
        await _db.StringSetAsync(Key(cacheKey), bytes, _ttl);
    }

    public async Task ClearAsync()
    {
        // SCAN for all keys with our prefix and delete them
        var server = _db.Multiplexer.GetServers().FirstOrDefault();
        if (server == null) return;

        var keys = server.Keys(pattern: $"{_prefix}:file:*").ToArray();
        if (keys.Length > 0)
            await _db.KeyDeleteAsync(keys);
    }

    public (int FileCount, long TotalBytes, long MaxBytes) GetStats()
    {
        var server = _db.Multiplexer.GetServers().FirstOrDefault();
        if (server == null) return (0, 0, -1);

        var count = server.Keys(pattern: $"{_prefix}:file:*").Count();
        return (count, -1, -1); // byte totals not tracked in Redis mode
    }

    public string GetDebugInfo()
    {
        var (fileCount, _, _) = GetStats();
        return $"=== Redis Encrypted File Cache ===\nEnabled: {_enabled}\nPrefix: {_prefix}\nFiles Cached: {fileCount}\nTTL: {(_ttl.HasValue ? _ttl.Value.ToString() : "none (Redis maxmemory policy)")}"
    }
}
