using OSWS.ParquetSolver.Helpers;

namespace OSWS.Performance.Benchmarks.Fixtures;

/// <summary>
/// Test fixture for warm-start scenarios.
/// Pre-populates caches before tests to simulate repeated access patterns.
/// </summary>
public class WarmStartFixture : IDisposable
{
    public DekCache DekCache { get; }
    public EncryptedFileCache FileCache { get; } =
        new(
            new Common.Configuration.CacheSettings
            {
                EnableFileCache = true,
                MaxCacheSizeBytes = 10L * 1024 * 1024 * 1024, // 10GB
            }
        );

    public WarmStartFixture(int dekCacheCapacity = 2500)
    {
        DekCache = new DekCache(dekCacheCapacity);
    }

    public void Dispose()
    {
        FileCache?.Dispose();
    }
}
