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
    /// </summary>
    public string DekCacheProvider { get; set; } = "Local";

    /// <summary>
    /// Maximum in-memory DEK entries (default 2500).
    /// </summary>
    public int DekCacheCapacity { get; set; } = 2500;

    /// <summary>
    /// TTL in seconds for cached DEKs. Default: 900 (15 minutes).
    /// Set to 0 to disable expiry.
    /// </summary>
    public int DekTtlSeconds { get; set; } = 900;

    /// <summary>
    /// TTL in seconds for cached DEKs when the requesting user is an admin.
    /// Shorter TTL reduces exposure window for privileged access.
    /// Default: 300 (5 minutes). Set to 0 to use the standard DekTtlSeconds.
    /// </summary>
    public int DekAdminTtlSeconds { get; set; } = 300;

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
