namespace OSWS.Common.Configuration;

/// <summary>
/// Configuration for encrypted parquet file caching.
/// Bind from appsettings.json section "Cache".
/// </summary>
public class CacheSettings
{
    /// <summary>
    /// Cache provider to use. "Local" (default) uses disk-based caching on the current node.
    /// "Redis" uses a shared Redis instance — required for multi-node deployments.
    /// </summary>
    public string Provider { get; set; } = "Local";

    /// <summary>
    /// Enable caching of encrypted parquet files.
    /// When disabled, files are always fetched from S3.
    /// </summary>
    public bool EnableFileCache { get; set; } = true;

    // -------------------------
    // Local provider settings
    // -------------------------

    /// <summary>
    /// Maximum total size of cached files in bytes (default: 10GB). Local provider only.
    /// When the limit is reached, least recently used files are evicted.
    /// </summary>
    public long MaxCacheSizeBytes { get; set; } = 10L * 1024 * 1024 * 1024; // 10GB

    /// <summary>
    /// Directory path for cached files. Defaults to system temp directory. Local provider only.
    /// </summary>
    public string? CacheDirectory { get; set; }

    // -------------------------
    // Redis provider settings
    // -------------------------

    /// <summary>
    /// Redis connection string (e.g. "localhost:6379" or a full StackExchange.Redis config string).
    /// Required when Provider is "Redis".
    /// </summary>
    public string RedisConnectionString { get; set; } = "localhost:6379";

    /// <summary>
    /// Key prefix used for all Redis entries to avoid collisions. Defaults to "osws".
    /// </summary>
    public string RedisKeyPrefix { get; set; } = "osws";

    /// <summary>
    /// Optional TTL in seconds for Redis cache entries. 0 means no TTL (rely on Redis maxmemory policy).
    /// </summary>
    public int RedisTtlSeconds { get; set; } = 0;
}
