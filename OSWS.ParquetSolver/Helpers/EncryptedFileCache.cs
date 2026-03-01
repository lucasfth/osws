using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using OSWS.Common.Configuration;

namespace OSWS.ParquetSolver.Helpers;

/// <summary>
/// Thread-safe disk-based cache for encrypted parquet files with LRU eviction.
/// Reduces S3 API calls by storing encrypted files locally until cache size limit is reached.
/// Files are identified by a hash of their S3 bucket+key to ensure uniqueness.
/// </summary>
public class EncryptedFileCache : IDisposable
{
    private readonly CacheSettings _settings;
    private readonly string _cacheDirectory;
    private readonly ConcurrentDictionary<string, CacheEntry> _entries = new();
    private readonly SemaphoreSlim _lock = new(1, 1);
    private long _currentCacheSize = 0;

    private class CacheEntry
    {
        public string FilePath { get; init; } = string.Empty;
        public long FileSize { get; init; }
        public DateTime LastAccessTime { get; set; }
        public string CacheKey { get; init; } = string.Empty;
    }

    public EncryptedFileCache(CacheSettings settings)
    {
        _settings = settings ?? throw new ArgumentNullException(nameof(settings));

        // Determine cache directory
        _cacheDirectory = string.IsNullOrWhiteSpace(_settings.CacheDirectory) ? Path.Combine(Path.GetTempPath(), "osws-cache") : _settings.CacheDirectory;

        // Create cache directory if it doesn't exist
        if (_settings.EnableFileCache && !Directory.Exists(_cacheDirectory))
        {
            Directory.CreateDirectory(_cacheDirectory);
        }

        // Initialize cache by scanning existing files (in case of restart)
        if (_settings.EnableFileCache && Directory.Exists(_cacheDirectory))
        {
            InitializeFromDisk();
        }
    }

    /// <summary>
    /// Generate a cache key from bucket and S3 key.
    /// Uses SHA256 hash to ensure valid filenames and uniqueness.
    /// </summary>
    public static string GenerateCacheKey(string bucket, string key)
    {
        var combined = $"{bucket}::{key}";
        var hashBytes = SHA256.HashData(Encoding.UTF8.GetBytes(combined));
        return Convert.ToHexString(hashBytes).ToLowerInvariant();
    }

    /// <summary>
    /// Try to retrieve a cached encrypted file.
    /// Updates access time on cache hit for LRU tracking.
    /// </summary>
    /// <param name="cacheKey">The cache key (generated from bucket+key)</param>
    /// <param name="stream">The cached file stream if found, null otherwise</param>
    /// <returns>True if file was in cache, false otherwise</returns>
    public bool TryGet(string cacheKey, out Stream? stream)
    {
        stream = null;

        if (_settings.EnableFileCache)
            return false;

        if (!_entries.TryGetValue(cacheKey, out var entry))
            return false;

        // Verify file still exists on disk
        if (!File.Exists(entry.FilePath))
        {
            _entries.TryRemove(cacheKey, out _);
            return false;
        }

        // Update access time for LRU
        entry.LastAccessTime = DateTime.UtcNow;

        // Return a read-only file stream
        try
        {
            stream = new FileStream(entry.FilePath, FileMode.Open, FileAccess.Read, FileShare.Read);
            return true;
        }
        catch (IOException)
        {
            // File might have been deleted or is inaccessible
            _entries.TryRemove(cacheKey, out _);
            return false;
        }
    }

    /// <summary>
    /// Cache an encrypted file to disk.
    /// Evicts least recently used files if cache size limit would be exceeded.
    /// </summary>
    /// <param name="cacheKey">The cache key (generated from bucket+key)</param>
    /// <param name="stream">The encrypted file stream to cache</param>
    /// <param name="cancellationToken"></param>
    public async Task SetAsync(string cacheKey, Stream stream, CancellationToken cancellationToken = default)
    {
        if (_settings.EnableFileCache)
            return;

        ArgumentNullException.ThrowIfNull(stream);
        ArgumentException.ThrowIfNullOrWhiteSpace(cacheKey);

        await _lock.WaitAsync(cancellationToken);
        try
        {
            // Check if already cached
            if (_entries.ContainsKey(cacheKey))
            {
                // Update access time and return
                if (_entries.TryGetValue(cacheKey, out var existingEntry))
                {
                    existingEntry.LastAccessTime = DateTime.UtcNow;
                }
                return;
            }

            // Create folder if not yet exist
            if (!Directory.Exists(_cacheDirectory))
                Directory.CreateDirectory(_cacheDirectory);
            var filePath = Path.Combine(_cacheDirectory, $"{cacheKey}.parquet");

            // Write stream to disk
            await using (var fileStream = new FileStream(filePath, FileMode.Create, FileAccess.Write, FileShare.None))
            {
                var originalPosition = stream.Position;
                stream.Position = 0;
                await stream.CopyToAsync(fileStream, cancellationToken);
                stream.Position = originalPosition; // Restore original position
            }

            var fileInfo = new FileInfo(filePath);
            var fileSize = fileInfo.Length;

            // Evict if necessary to make room
            while (_currentCacheSize + fileSize > _settings.MaxCacheSizeBytes && _entries.Any())
            {
                await EvictLruAsync();
            }

            // Add to cache
            var entry = new CacheEntry
            {
                FilePath = filePath,
                FileSize = fileSize,
                LastAccessTime = DateTime.UtcNow,
                CacheKey = cacheKey
            };

            _entries.TryAdd(cacheKey, entry);
            Interlocked.Add(ref _currentCacheSize, fileSize);
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <summary>
    /// Evict the least recently used file from cache.
    /// </summary>
    private async Task EvictLruAsync()
    {
        // Find LRU entry
        var lruEntry = _entries.Values
            .OrderBy(e => e.LastAccessTime)
            .FirstOrDefault();

        if (lruEntry == null)
            return;

        // Remove from dictionary
        _entries.TryRemove(lruEntry.CacheKey, out _);

        // Delete file from disk
        try
        {
            if (File.Exists(lruEntry.FilePath))
            {
                File.Delete(lruEntry.FilePath);
            }
            Interlocked.Add(ref _currentCacheSize, -lruEntry.FileSize);
        }
        catch (IOException)
        {
            // File might already be deleted or locked
        }

        await Task.CompletedTask;
    }

    /// <summary>
    /// Clear all cached files and reset cache state.
    /// </summary>
    public async Task ClearAsync()
    {
        await _lock.WaitAsync();
        try
        {
            foreach (var entry in _entries.Values)
            {
                try
                {
                    if (File.Exists(entry.FilePath))
                    {
                        File.Delete(entry.FilePath);
                    }
                }
                catch (IOException)
                {
                    // Ignore errors during cleanup
                }
            }

            _entries.Clear();
            _currentCacheSize = 0;
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <summary>
    /// Get current cache statistics.
    /// </summary>
    public (int FileCount, long TotalBytes, long MaxBytes) GetStats()
    {
        return (_entries.Count, _currentCacheSize, _settings.MaxCacheSizeBytes);
    }

    /// <summary>
    /// Initialize cache state by scanning existing files in cache directory.
    /// Used when restarting the service to restore cache state.
    /// </summary>
    private void InitializeFromDisk()
    {
        try
        {
            var files = Directory.GetFiles(_cacheDirectory, "*.parquet");
            foreach (var filePath in files)
            {
                var fileInfo = new FileInfo(filePath);
                var cacheKey = Path.GetFileNameWithoutExtension(filePath);

                var entry = new CacheEntry
                {
                    FilePath = filePath,
                    FileSize = fileInfo.Length,
                    LastAccessTime = fileInfo.LastAccessTimeUtc,
                    CacheKey = cacheKey
                };

                _entries.TryAdd(cacheKey, entry);
                Interlocked.Add(ref _currentCacheSize, fileInfo.Length);
            }
        }
        catch (Exception)
        {
            // If initialization fails, start with empty cache
            _entries.Clear();
            _currentCacheSize = 0;
        }
    }

    /// <summary>
    /// Returns detailed debug information about cache state.
    /// Useful for verifying cache is working and monitoring cache health.
    /// </summary>
    public string GetDebugInfo()
    {
        _lock.Wait();
        try
        {
            var sb = new System.Text.StringBuilder();
            sb.AppendLine($"=== Encrypted File Cache Debug Info ===");
            sb.AppendLine($"Enabled: {_settings.EnableFileCache}");
            sb.AppendLine($"Directory: {_cacheDirectory}");
            sb.AppendLine($"Files Cached: {_entries.Count}");
            sb.AppendLine($"Cache Size: {_currentCacheSize / (1024 * 1024):N2} MB / {_settings.MaxCacheSizeBytes / (1024 * 1024 * 1024):N2} GB");
            sb.AppendLine($"Directory Exists: {Directory.Exists(_cacheDirectory)}");

            if (_entries.Count > 0)
            {
                sb.AppendLine($"\nCached Files (sorted by access time):");
                foreach (var entry in _entries.OrderByDescending(e => e.Value.LastAccessTime))
                {
                    var filePath = Path.Combine(_cacheDirectory, entry.Key + ".parquet");
                    var fileSize = File.Exists(filePath) ? new FileInfo(filePath).Length : 0;
                    sb.AppendLine($"  - {entry.Key}");
                    sb.AppendLine($"    Size: {fileSize / (1024 * 1024):N2} MB, Last Access: {entry.Value.LastAccessTime:O}");
                }
            }

            return sb.ToString();
        }
        finally
        {
            _lock.Release();
        }
    }

    public void Dispose()
    {
        _lock?.Dispose();
    }
}
