using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using OSWS.Common.Configuration;
using OSWS.ParquetSolver.Interfaces;

namespace OSWS.ParquetSolver.Helpers;

/// <summary>
/// Thread-safe disk-based cache for encrypted parquet files with LRU eviction.
/// Reduces S3 API calls by storing encrypted files locally until cache size limit is reached.
/// Files are identified by a hash of their S3 bucket+key to ensure uniqueness.
/// </summary>
public class EncryptedFileCache : IEncryptedFileCache, IDisposable
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

        _cacheDirectory = string.IsNullOrWhiteSpace(_settings.CacheDirectory)
            ? Path.Combine(Path.GetTempPath(), "osws-cache")
            : _settings.CacheDirectory;

        if (_settings.EnableFileCache && !Directory.Exists(_cacheDirectory))
            Directory.CreateDirectory(_cacheDirectory);

        if (_settings.EnableFileCache && Directory.Exists(_cacheDirectory))
            InitializeFromDisk();
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

    public bool TryGet(string cacheKey, out Stream? stream)
    {
        stream = null;

        if (!_settings.EnableFileCache)
            return false;

        if (!_entries.TryGetValue(cacheKey, out var entry))
            return false;

        if (!File.Exists(entry.FilePath))
        {
            _entries.TryRemove(cacheKey, out _);
            return false;
        }

        entry.LastAccessTime = DateTime.UtcNow;

        try
        {
            stream = new FileStream(entry.FilePath, FileMode.Open, FileAccess.Read, FileShare.Read);
            return true;
        }
        catch (IOException)
        {
            _entries.TryRemove(cacheKey, out _);
            return false;
        }
    }

    public async Task SetAsync(
        string cacheKey,
        Stream stream,
        CancellationToken cancellationToken = default
    )
    {
        if (!_settings.EnableFileCache)
            return;

        ArgumentNullException.ThrowIfNull(stream);
        ArgumentException.ThrowIfNullOrWhiteSpace(cacheKey);

        await _lock.WaitAsync(cancellationToken);
        try
        {
            if (_entries.ContainsKey(cacheKey))
            {
                if (_entries.TryGetValue(cacheKey, out var existingEntry))
                    existingEntry.LastAccessTime = DateTime.UtcNow;
                return;
            }

            if (!Directory.Exists(_cacheDirectory))
                Directory.CreateDirectory(_cacheDirectory);

            var filePath = Path.Combine(_cacheDirectory, $"{cacheKey}.parquet");

            await using (
                var fileStream = new FileStream(
                    filePath,
                    FileMode.Create,
                    FileAccess.Write,
                    FileShare.None
                )
            )
            {
                var originalPosition = stream.Position;
                stream.Position = 0;
                await stream.CopyToAsync(fileStream, cancellationToken);
                stream.Position = originalPosition;
            }

            var fileInfo = new FileInfo(filePath);
            var fileSize = fileInfo.Length;

            while (_currentCacheSize + fileSize > _settings.MaxCacheSizeBytes && _entries.Any())
                await EvictLruAsync();

            var entry = new CacheEntry
            {
                FilePath = filePath,
                FileSize = fileSize,
                LastAccessTime = DateTime.UtcNow,
                CacheKey = cacheKey,
            };

            _entries.TryAdd(cacheKey, entry);
            Interlocked.Add(ref _currentCacheSize, fileSize);
        }
        finally
        {
            _lock.Release();
        }
    }

    private async Task EvictLruAsync()
    {
        var lruEntry = _entries.Values.OrderBy(e => e.LastAccessTime).FirstOrDefault();
        if (lruEntry == null) return;

        _entries.TryRemove(lruEntry.CacheKey, out _);

        try
        {
            if (File.Exists(lruEntry.FilePath))
                File.Delete(lruEntry.FilePath);

            Interlocked.Add(ref _currentCacheSize, -lruEntry.FileSize);
        }
        catch (IOException) { }

        await Task.CompletedTask;
    }

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
                        File.Delete(entry.FilePath);
                }
                catch (IOException) { }
            }

            _entries.Clear();
            _currentCacheSize = 0;
        }
        finally
        {
            _lock.Release();
        }
    }

    public (int FileCount, long TotalBytes, long MaxBytes) GetStats() =>
        (_entries.Count, _currentCacheSize, _settings.MaxCacheSizeBytes);

    private void InitializeFromDisk()
    {
        try
        {
            foreach (var filePath in Directory.GetFiles(_cacheDirectory, "*.parquet"))
            {
                var fileInfo = new FileInfo(filePath);
                var cacheKey = Path.GetFileNameWithoutExtension(filePath);

                var entry = new CacheEntry
                {
                    FilePath = filePath,
                    FileSize = fileInfo.Length,
                    LastAccessTime = fileInfo.LastAccessTimeUtc,
                    CacheKey = cacheKey,
                };

                _entries.TryAdd(cacheKey, entry);
                Interlocked.Add(ref _currentCacheSize, fileInfo.Length);
            }
        }
        catch (Exception)
        {
            _entries.Clear();
            _currentCacheSize = 0;
        }
    }

    public string GetDebugInfo()
    {
        _lock.Wait();
        try
        {
            var sb = new StringBuilder();
            sb.AppendLine("=== Encrypted File Cache Debug Info ===");
            sb.AppendLine($"Provider: Local");
            sb.AppendLine($"Enabled: {_settings.EnableFileCache}");
            sb.AppendLine($"Directory: {_cacheDirectory}");
            sb.AppendLine($"Files Cached: {_entries.Count}");
            sb.AppendLine(
                $"Cache Size: {_currentCacheSize / (1024 * 1024):N2} MB / {_settings.MaxCacheSizeBytes / (1024 * 1024 * 1024):N2} GB"
            );
            sb.AppendLine($"Directory Exists: {Directory.Exists(_cacheDirectory)}");

            if (_entries.Count > 0)
            {
                sb.AppendLine($"\nCached Files (sorted by access time):");
                foreach (var entry in _entries.OrderByDescending(e => e.Value.LastAccessTime))
                {
                    var filePath = Path.Combine(_cacheDirectory, entry.Key + ".parquet");
                    var fileSize = File.Exists(filePath) ? new FileInfo(filePath).Length : 0;
                    sb.AppendLine($"  - {entry.Key}");
                    sb.AppendLine(
                        $"    Size: {fileSize / (1024 * 1024):N2} MB, Last Access: {entry.Value.LastAccessTime:O}"
                    );
                }
            }

            return sb.ToString();
        }
        finally
        {
            _lock.Release();
        }
    }

    public void Dispose() => _lock?.Dispose();
}
