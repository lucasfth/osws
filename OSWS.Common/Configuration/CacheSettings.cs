namespace OSWS.Common.Configuration;

/// <summary>
/// Configuration for all caching in OSWS. Bind from appsettings.json section "Cache".
/// </summary>
public class CacheSettings
{
    // -----------------------------------------------------------------------
    // DEK cache
    // -----------------------------------------------------------------------

    /// <summary>
    /// Provider for the DEK (Data Encryption Key) cache.
    /// "Local" (default) — in-memory, single-node only.
    /// "Redis" — shared Redis instance, required for multi-node deployments.
    /// </summary>
    public string DekCacheProvider { get; set; } = "Local";

    /// <summary>
    /// Maximum in-memory DEK entries for the Local provider (default 2500).
    /// </summary>
    public int DekCacheCapacity { get; set; } = 2500;

    /// <summary>
    /// TTL in seconds for cached DEKs (both Local and Redis providers).
    /// 0 means no expiry until RBAC TTLs are wired in.
    /// TODO (RBAC): This will be superseded by per-entry TTLs derived from the caller's role.
    /// </summary>
    public int DekTtlSeconds { get; set; } = 0;

    // -----------------------------------------------------------------------
    // Redis provider settings (used when DekCacheProvider = "Redis")
    // -----------------------------------------------------------------------

    /// <summary>
    /// Redis connection string (e.g. "localhost:6379"). Required when DekCacheProvider is "Redis".
    /// </summary>
    public string RedisConnectionString { get; set; } = "localhost:6379";

    /// <summary>
    /// Key prefix for all Redis entries to avoid collisions. Defaults to "osws".
    /// </summary>
    public string RedisKeyPrefix { get; set; } = "osws";

    // -----------------------------------------------------------------------
    // Encrypted parquet file cache (always local disk — not Redis)
    // -----------------------------------------------------------------------

    /// <summary>
    /// Enable disk caching of encrypted parquet files. When disabled, files are always fetched from S3.
    /// </summary>
    public bool EnableFileCache { get; set; } = true;

    /// <summary>
    /// Maximum total size of cached files in bytes (default 10 GB). LRU eviction when exceeded.
    /// </summary>
    public long MaxCacheSizeBytes { get; set; } = 10L * 1024 * 1024 * 1024;

    /// <summary>
    /// Directory for cached parquet files. Defaults to system temp directory.
    /// </summary>
    public string? CacheDirectory { get; set; }
}
