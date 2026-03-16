using Microsoft.Extensions.DependencyInjection;
using OSWS.Models.Interfaces;
using OSWS.ParquetSolver;
using OSWS.ParquetSolver.Helpers;
using OSWS.Performance.Benchmarks.DatasetGenerators;
using OSWS.Performance.Benchmarks.Helpers;

namespace OSWS.Performance.Benchmarks;

/// <summary>
/// Simple test to verify encryption/decryption roundtrip works with cache clearing
/// </summary>
public class TestEncryptionRoundtrip
{
    public static async Task Main()
    {
        Console.WriteLine("=== Testing Encryption Roundtrip with Cache Clearing ===");

        var services = BenchmarkServiceFactory.BuildServiceProvider();
        var keyVaultProvider = services.GetRequiredService<IKeyVaultProvider>();
        var dekCache = new DekCache();

        Console.WriteLine($"KeyVaultProvider: {keyVaultProvider.GetType().Name}");

        // Generate small dataset
        Console.WriteLine("\n1. Generating dataset...");
        var unencrypted = await SmallDatasetGenerator.GenerateAsync(
            5,
            1000,
            CancellationToken.None
        );

        // Encrypt
        Console.WriteLine("2. Encrypting...");
        var writer = new ParquetWriter(keyVaultProvider, "Internal");
        var encrypted = await writer.WriteParquetAsync(unencrypted, "default");
        Console.WriteLine($"   Encrypted size: {encrypted.Length} bytes");

        // Decrypt (first time - should populate cache)
        Console.WriteLine("3. Decrypting (first time)...");
        encrypted.Position = 0;
        var reader1 = new ParquetReader(keyVaultProvider, dekCache);
        var decrypted1 = await reader1.ReadParquetAsync(encrypted);
        Console.WriteLine($"   Decrypted size: {decrypted1.Length} bytes");

        // Clear cache
        Console.WriteLine("4. Clearing cache...");
        dekCache.Clear();

        // Decrypt again (should re-fetch keys from KeyVaultProvider)
        Console.WriteLine("5. Decrypting (after cache clear)...");
        try
        {
            encrypted.Position = 0;
            var reader2 = new ParquetReader(keyVaultProvider, dekCache);
            var decrypted2 = await reader2.ReadParquetAsync(encrypted);
            Console.WriteLine($"   ✓ Decrypted size: {decrypted2.Length} bytes");
            Console.WriteLine("\n=== SUCCESS ===");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"\n=== FAILED ===");
            Console.WriteLine($"Error: {ex.Message}");
            Console.WriteLine($"\nStack trace:\n{ex.StackTrace}");
        }

        services.Dispose();
    }
}
