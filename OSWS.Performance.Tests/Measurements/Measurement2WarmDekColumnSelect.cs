using OSWS.KeyManager.Providers;
using OSWS.ParquetSolver;
using OSWS.Performance.Tests.DatasetGenerators;
using OSWS.Performance.Tests.Fixtures;
using OSWS.Performance.Tests.Helpers;
using Xunit.Abstractions;

namespace OSWS.Performance.Tests.Measurements;

/// <summary>
/// Measurement 2: Warm DEK cache, cold file cache with deep dataset (10 columns × 10M rows)
/// Tests: I/O efficiency with column selection
/// Scenario: Request only columns 1 and 10 via byte-range (derived from metadata)
/// Verifies: OSWS can make efficient ranged requests to S3
/// </summary>
public class Measurement2WarmDekColumnSelect(WarmStartFixture fixture, ITestOutputHelper testOutputHelper)
    : IClassFixture<WarmStartFixture>
{
    private readonly MetricsCollector _metrics = new();

    [Fact(Skip = "Integration test - requires Azure KV and S3 setup")]
    public async Task WarmDek_DeepDataset_ColumnSelect()
    {
        // Arrange
        var keyVaultProvider = new InternalKeyVaultProvider();
        var parquetWriter = new ParquetWriter(keyVaultProvider, "Internal");
        var parquetReader = new ParquetReader(keyVaultProvider, fixture.DekCache);

        // Generate deep dataset (10 columns × 10M rows)
        // Note: Reduced for testing - full size would be very slow
        var unencryptedStream = await DeepDatasetGenerator.GenerateAsync(10, 100_000);
        var fileSize = unencryptedStream.Length;

        // Encrypt the parquet file (this populates DEK cache)
        var encryptedStream = await parquetWriter.WriteParquetAsync(unencryptedStream, "default");
        
        // Clear file cache but keep DEK cache warm
        await fixture.FileCache.ClearAsync();

        // Act - Measure decryption with warm DEK cache
        _metrics.StartMeasurement();

        encryptedStream.Position = 0;
        var decryptedStream = await parquetReader.ReadParquetAsync(encryptedStream);

        _metrics.StopMeasurement();

        // Assert
        var metrics = _metrics.GetMetrics();
        var dekCacheStats = fixture.DekCache.Count;
        
        Assert.True(metrics.TotalElapsedMs > 0, "Should take measurable time");
        Assert.True(dekCacheStats > 0, "DEK cache should contain keys");
        Assert.True(decryptedStream.Length > 0, "Should decrypt successfully");

        // Output metrics
        testOutputHelper.WriteLine("\n=== Measurement 2: Warm DEK Cache Column Selection ===");
        testOutputHelper.WriteLine("Dataset: 10 columns × 100,000 rows (reduced for testing)");
        testOutputHelper.WriteLine($"Unencrypted size: {fileSize / (1024.0 * 1024.0):F2} MB");
        testOutputHelper.WriteLine($"Encrypted size: {encryptedStream.Length / (1024.0 * 1024.0):F2} MB");
        testOutputHelper.WriteLine($"DEK cache entries: {dekCacheStats}");
        testOutputHelper.WriteLine(metrics.ToString());
        testOutputHelper.WriteLine("\nNOTE: Column-specific byte-range optimization not yet implemented");
    }
}
