using OSWS.ParquetSolver.Helpers;

namespace OSWS.Performance.Benchmarks.Fixtures;

/// <summary>
/// Test fixture for warm-start scenarios.
/// Pre-populates caches before tests to simulate repeated access patterns.
/// </summary>
public class WarmStartFixture : IDisposable
{
    public DekCache DekCache { get; } = new();
    public EncryptedFileCache FileCache { get; } =
        new(
            new OSWS.Common.Configuration.CacheSettings
            {
                EnableFileCache = true,
                MaxCacheSizeBytes = 10L * 1024 * 1024 * 1024, // 10GB
            }
        );

    /// <summary>
    /// Pre-populate caches for warm start scenarios.
    /// This should be called during test setup after initial operations.
    /// </summary>
    public void PrepopulateCaches()
    {
        // Caches are populated naturally during first test run
        // This method is here for completeness and documentation
    }

    public void Dispose()
    {
        FileCache?.Dispose();
    }
}
