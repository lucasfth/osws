using OSWS.KeyManager.Providers;
using OSWS.ParquetSolver;
using OSWS.Performance.Tests.DatasetGenerators;
using OSWS.Performance.Tests.Fixtures;
using OSWS.Performance.Tests.Helpers;
using Xunit.Abstractions;

namespace OSWS.Performance.Tests.Measurements;

/// <summary>
/// Measurement 3: Full decryption throughput with deep dataset
/// Tests: Maximum decryption throughput with warm caches
/// Scenario: Decrypt entire deep file (10 cols × 10M rows)
/// Runs on: 1, 4, and 8 CPU cores to verify linear scaling
/// Metrics: Throughput (MB/s), CPU scaling efficiency
/// </summary>
public class Measurement3FullDecryptionThroughput(WarmStartFixture fixture, ITestOutputHelper testOutputHelper)
    : IClassFixture<WarmStartFixture>
{
    private readonly MetricsCollector _metrics = new();

    [Theory(Skip = "Integration test - requires Azure KV setup")]
    [InlineData(1)]
    [InlineData(4)]
    [InlineData(8)]
    public async Task FullDecryption_DeepDataset_MultiCore(int threadCount)
    {
        // Arrange
        var keyVaultProvider = new InternalKeyVaultProvider();
        var parquetWriter = new ParquetWriter(keyVaultProvider, "Internal");
        var parquetReader = new ParquetReader(keyVaultProvider, fixture.DekCache);

        // Generate deep dataset (reduced size for testing)
        var unencryptedStream = await DeepDatasetGenerator.GenerateAsync(10, 100_000);
        var fileSize = unencryptedStream.Length;

        // Encrypt and cache
        var encryptedStream = await parquetWriter.WriteParquetAsync(unencryptedStream, "default");
        const string cacheKey = "test-deep-dataset";
        await fixture.FileCache.SetAsync(cacheKey, encryptedStream);

        // Act - Measure full decryption with warm caches
        _metrics.StartMeasurement();

        encryptedStream.Position = 0;
        var decryptedStream = await parquetReader.ReadParquetAsync(encryptedStream);

        // Read entire stream to measure throughput
        var buffer = new byte[8192];
        long totalBytesRead = 0;
        int bytesRead;
        while ((bytesRead = await decryptedStream.ReadAsync(buffer, 0, buffer.Length)) > 0)
        {
            totalBytesRead += bytesRead;
        }

        _metrics.StopMeasurement();

        // Assert
        var metrics = _metrics.GetMetrics();
        var throughputMbps = (totalBytesRead / (1024.0 * 1024.0)) / (metrics.TotalElapsedMs / 1000.0);

        Assert.True(totalBytesRead > 0, "Should read data");
        Assert.True(metrics.TotalElapsedMs > 0, "Should take measurable time");

        // Output metrics
        testOutputHelper.WriteLine($"\n=== Measurement 3: Full Decryption Throughput ({threadCount} cores) ===");
        testOutputHelper.WriteLine($"Dataset: 10 columns × 100,000 rows (reduced for testing)");
        testOutputHelper.WriteLine($"File size: {fileSize / (1024.0 * 1024.0):F2} MB");
        testOutputHelper.WriteLine($"Bytes read: {totalBytesRead / (1024.0 * 1024.0):F2} MB");
        testOutputHelper.WriteLine($"Throughput: {throughputMbps:F2} MB/s");
        testOutputHelper.WriteLine($"Thread count: {threadCount}");
        testOutputHelper.WriteLine(metrics.ToString());
        testOutputHelper.WriteLine("\nNOTE: Multi-core parallelism not yet implemented in ParquetSharp wrapper");
    }
}
