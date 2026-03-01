namespace OSWS.Common.Configuration;

/// <summary>
/// Configuration for encrypted parquet file caching.
/// Bind from appsettings.json section "Cache".
/// </summary>
public class CacheSettings
{
    /// <summary>
    /// Enable disk-based caching of encrypted parquet files.
    /// When enabled, encrypted files fetched from S3 are cached locally to reduce S3 API calls.
    /// </summary>
    public bool EnableFileCache { get; set; } = true;

    /// <summary>
    /// Maximum total size of cached files in bytes (default: 10GB).
    /// When limit is reached, least recently used files are evicted.
    /// </summary>
    public long MaxCacheSizeBytes { get; set; } = 10L * 1024 * 1024 * 1024; // 10GB

    /// <summary>
    /// Directory path for cached files. If null or empty, uses system temp directory.
    /// Directory will be created if it doesn't exist.
    /// </summary>
    public string? CacheDirectory { get; set; }
}
