using OSWS.KeyManager.Providers;
using OSWS.ParquetSolver;
using OSWS.Performance.Tests.DatasetGenerators;
using OSWS.Performance.Tests.Fixtures;
using OSWS.Performance.Tests.Helpers;
using Xunit.Abstractions;

namespace OSWS.Performance.Tests.Measurements;

/// <summary>
/// Measurement 4: DEK cache stress test with 100 distinct small files
/// Tests: Cache eviction behavior, Azure KV rate limiting resistance
/// Scenario: Read 100 distinct parquet files in parallel
/// Success criteria: No Azure KV 429 errors, latency < 1 second per file
/// </summary>
public class Measurement4DekCacheStressTest(ColdStartFixture fixture, ITestOutputHelper testOutputHelper)
    : IClassFixture<ColdStartFixture>
{
    private readonly MetricsCollector _metrics = new();

    [Fact(Skip = "Integration test - requires Azure KV setup")]
    public async Task ParallelRead_100SmallFiles_CacheStress()
    {
        // Arrange
        await fixture.ClearCachesAsync();

        var keyVaultProvider = new InternalKeyVaultProvider();
        var parquetWriter = new ParquetWriter(keyVaultProvider, "Internal");
        var parquetReader = new ParquetReader(keyVaultProvider, fixture.DekCache);

        const int fileCount = 100;
        var encryptedFiles = new List<Stream>();

        // Generate and encrypt 100 distinct small parquet files
        testOutputHelper.WriteLine($"Generating {fileCount} small datasets...");
        for (var i = 0; i < fileCount; i++)
        {
            var unencryptedStream = await SmallDatasetGenerator.GenerateAsync(5, 5000);
            var encryptedStream = await parquetWriter.WriteParquetAsync(unencryptedStream, $"role-{i}");
            encryptedFiles.Add(encryptedStream);
        }

        // Clear caches for fresh start
        await fixture.ClearCachesAsync();

        // Act - Read all files in parallel
        _metrics.StartMeasurement();

        var tasks = encryptedFiles.Select(async (stream, index) =>
        {
            try
            {
                stream.Position = 0;
                var decrypted = await parquetReader.ReadParquetAsync(stream);
                return (Success: true, Index: index, Latency: 0.0);
            }
            catch (Exception ex)
            {
                return (Success: false, Index: index, Latency: 0.0);
            }
        }).ToList();

        var results = await Task.WhenAll(tasks);

        _metrics.StopMeasurement();

        // Assert
        var metrics = _metrics.GetMetrics();
        var successCount = results.Count(r => r.Success);
        var avgLatencyPerFile = metrics.TotalElapsedMs / fileCount;
        var cacheStats = fixture.DekCache.Count;

        Assert.Equal(fileCount, successCount);
        Assert.True(avgLatencyPerFile < 1000, $"Average latency per file should be < 1s, was {avgLatencyPerFile:F2}ms");

        // Output metrics
        testOutputHelper.WriteLine("\n=== Measurement 4: DEK Cache Stress Test (100 Files) ===");
        testOutputHelper.WriteLine($"Files processed: {successCount}/{fileCount}");
        testOutputHelper.WriteLine($"Average latency per file: {avgLatencyPerFile:F2} ms");
        testOutputHelper.WriteLine($"DEK cache entries: {cacheStats}");
        testOutputHelper.WriteLine($"Success rate: {(successCount / (double)fileCount) * 100:F1}%");
        testOutputHelper.WriteLine(metrics.ToString());

        // Check for rate limiting indicators
        if (metrics.TotalElapsedMs > fileCount * 1000)
        {
            testOutputHelper.WriteLine("\nWARNING: Total time suggests possible rate limiting or performance issues");
        }

        // Cleanup
        foreach (var stream in encryptedFiles)
        {
            await stream.DisposeAsync();
        }
    }
}
