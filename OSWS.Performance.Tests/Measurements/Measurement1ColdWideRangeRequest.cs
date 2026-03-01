using OSWS.KeyManager.Providers;
using OSWS.Models.Interfaces;
using OSWS.ParquetSolver;
using OSWS.Performance.Tests.DatasetGenerators;
using OSWS.Performance.Tests.Fixtures;
using OSWS.Performance.Tests.Helpers;
using Xunit.Abstractions;

namespace OSWS.Performance.Tests.Measurements;

/// <summary>
/// Measurement 1: Cold-start with wide dataset (2,000 columns × 10,000 rows)
/// Tests: Footer parsing overhead and key retrieval latency
/// Scenario: Client requests final 2MB via byte-range
/// Compares: OSWS latency vs direct S3 access
/// </summary>
public class Measurement1ColdWideRangeRequest(ColdStartFixture fixture, ITestOutputHelper testOutputHelper)
    : IClassFixture<ColdStartFixture>
{
    private readonly MetricsCollector _metrics = new();

    [Fact(Skip = "Integration test - requires Azure KV and S3 setup")]
    public async Task ColdStart_WideDataset_RangeRequest_2MB()
    {
        // Arrange
        await fixture.ClearCachesAsync();

        var keyVaultProvider = new InternalKeyVaultProvider(); // Use Internal for testing
        var parquetWriter = new ParquetWriter(keyVaultProvider, "Internal");
        var parquetReader = new ParquetReader(keyVaultProvider, fixture.DekCache);

        // Generate wide dataset (2000 columns × 10,000 rows)
        var unencryptedStream = await WideDatasetGenerator.GenerateAsync(2000, 10000);
        var fileSize = unencryptedStream.Length;

        // Encrypt the parquet file
        var encryptedStream = await parquetWriter.WriteParquetAsync(unencryptedStream, "default");

        // Calculate 2MB range (final 2MB of file)
        var requestedSize = 2 * 1024 * 1024; // 2MB
        var rangeStart = Math.Max(0, encryptedStream.Length - requestedSize);
        var rangeEnd = encryptedStream.Length - 1;

        // Act - Measure decryption with cold caches
        _metrics.StartMeasurement();

        // Simulate range request: full decryption needed for parquet structure
        encryptedStream.Position = 0;
        var decryptedStream = await parquetReader.ReadParquetAsync(encryptedStream);

        // Simulate extracting the requested range
        decryptedStream.Position = rangeStart;
        var buffer = new byte[requestedSize];
        await decryptedStream.ReadExactlyAsync(buffer, 0, (int)(rangeEnd - rangeStart + 1));

        _metrics.StopMeasurement();

        // Assert
        var metrics = _metrics.GetMetrics();
        Assert.True(metrics.TotalElapsedMs > 0, "Should take measurable time");
        Assert.True(decryptedStream.Length > 0, "Should decrypt successfully");

        // Output metrics
        testOutputHelper.WriteLine("\n=== Measurement 1: Cold Start Wide Dataset Range Request ===");
        testOutputHelper.WriteLine($"Dataset: 2,000 columns × 10,000 rows");
        testOutputHelper.WriteLine($"Unencrypted size: {fileSize / (1024.0 * 1024.0):F2} MB");
        testOutputHelper.WriteLine($"Encrypted size: {encryptedStream.Length / (1024.0 * 1024.0):F2} MB");
        testOutputHelper.WriteLine($"Requested range: {requestedSize / (1024 * 1024)} MB (bytes {rangeStart}-{rangeEnd})");
        testOutputHelper.WriteLine(metrics.ToString());
    }

    [Fact(Skip = "Integration test - requires S3 setup")]
    public async Task DirectS3_WideDataset_RangeRequest_Baseline()
    {
        // This test would measure direct S3 access for comparison
        // Implementation depends on S3 test setup
        await Task.CompletedTask;
        
        testOutputHelper.WriteLine("\n=== Measurement 1 Baseline: Direct S3 Access ===");
        testOutputHelper.WriteLine("NOTE: Implement this test to compare OSWS overhead vs direct S3");
    }
}
