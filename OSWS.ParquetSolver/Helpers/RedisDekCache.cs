using OSWS.Common.Configuration;
using OSWS.ParquetSolver.Interfaces;
using StackExchange.Redis;

namespace OSWS.ParquetSolver.Helpers;

/// <summary>
/// Redis-backed DEK cache. DEKs are stored as binary strings under <c>{prefix}:dek:{kekId}</c>.
/// </summary>
/// <remarks>
/// TODO (RBAC): When RBAC is implemented, derive TTL per-entry from the caller's role
/// instead of the configured default. Higher privilege → shorter TTL; lower privilege → longer TTL.
/// </remarks>
public class RedisDekCache : IDekCache
{
    private readonly IDatabase _db;
    private readonly string _prefix;
    private readonly TimeSpan? _ttl;

    public RedisDekCache(IConnectionMultiplexer redis, CacheSettings settings)
    {
        _db = redis.GetDatabase();
        _prefix = settings.RedisKeyPrefix ?? "osws";
        _ttl = settings.DekTtlSeconds > 0 ? TimeSpan.FromSeconds(settings.DekTtlSeconds) : null;
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
        // TODO (RBAC): Derive TTL from caller's role instead of the configured default.
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
}
