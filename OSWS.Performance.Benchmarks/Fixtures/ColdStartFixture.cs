using OSWS.ParquetSolver.Helpers;

namespace OSWS.Performance.Benchmarks.Fixtures;

/// <summary>
/// Test fixture for cold-start scenarios.
/// Clears all caches before each test to simulate first-time access.
/// </summary>
public class ColdStartFixture : IDisposable
{
    public DekCache DekCache { get; } = new();

    private EncryptedFileCache FileCache { get; } =
        new(
            new Common.Configuration.CacheSettings
            {
                EnableFileCache = true,
                MaxCacheSizeBytes = 10L * 1024 * 1024 * 1024, // 10GB
            }
        );

    /// <summary>
    /// Clear all caches to simulate cold start.
    /// Call this before each measurement.
    /// </summary>
    public async Task ClearCachesAsync()
    {
        DekCache.Clear();
        await FileCache.ClearAsync();
    }

    public void Dispose()
    {
        FileCache?.Dispose();
    }
}
