using OSWS.Common.Configuration;
using OSWS.ParquetSolver.Interfaces;
using StackExchange.Redis;

namespace OSWS.ParquetSolver.Helpers;

/// <summary>
/// Redis-backed cache for unwrapped Data Encryption Keys (DEKs).
/// Keys are stored under: <c>{prefix}:dek:{kekId}</c>.
/// </summary>
public class RedisDekCache : IDekCache
{
    private readonly IDatabase _db;
    private readonly string _prefix;
    private readonly TimeSpan? _ttl;

    public RedisDekCache(IConnectionMultiplexer redis, CacheSettings settings)
    {
        _db = redis.GetDatabase();
        _prefix = settings.RedisKeyPrefix ?? "osws";
        _ttl = settings.RedisTtlSeconds > 0
            ? TimeSpan.FromSeconds(settings.RedisTtlSeconds)
            : null;
    }

    private string Key(string kekId) => $"{_prefix}:dek:{kekId}";

    public bool TryGet(string kekId, out byte[]? dek)
    {
        dek = null;
        var value = _db.StringGet(Key(kekId));
        if (!value.HasValue) return false;
        dek = (byte[])value!;
        return true;
    }

    public void Set(string kekId, byte[] dek)
    {
        _db.StringSet(Key(kekId), dek, _ttl);
    }

    public void Clear()
    {
        var server = _db.Multiplexer.GetServers().FirstOrDefault();
        if (server == null) return;

        var keys = server.Keys(pattern: $"{_prefix}:dek:*").ToArray();
        if (keys.Length > 0)
            _db.KeyDelete(keys);
    }

    public int Count
    {
        get
        {
            var server = _db.Multiplexer.GetServers().FirstOrDefault();
            return server?.Keys(pattern: $"{_prefix}:dek:*").Count() ?? 0;
        }
    }
}
