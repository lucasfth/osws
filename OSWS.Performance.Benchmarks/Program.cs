using BenchmarkDotNet.Running;
using OSWS.Performance.Benchmarks.Infrastructure;
using OSWS.Performance.Benchmarks.Measurements;

namespace OSWS.Performance.Benchmarks;

public static class Program
{
    public static async Task Main(string[] args)
    {
        if (args.Length > 0 && string.Equals(args[0], "seed-parquet", StringComparison.OrdinalIgnoreCase))
        {
            var exitCode = await ParquetSeedUploader.RunAsync(args.Skip(1).ToArray());
            Environment.ExitCode = exitCode;
            return;
        }

        Console.WriteLine("╔══════════════════════════════════════════════════════════╗");
        Console.WriteLine("║        OSWS Performance Benchmarks (New Suite)           ║");
        Console.WriteLine("╚══════════════════════════════════════════════════════════╝");
        Console.WriteLine();

        // allow selecting a specific benchmark via environment variable or first CLI arg.
        // values: "auth", "unwrap", "decrypt" or the full type name.
        var choice = Environment.GetEnvironmentVariable("BENCH_MEASUREMENT");
        if (string.IsNullOrWhiteSpace(choice) && args.Length > 0)
        {
            choice = args[0];
        }

        var benchmarkType = choice switch
        {
            "auth" or "AuthorizationBenchmark" =>
                typeof(AuthorizationBenchmark),
            "unwrap" or "KeyUnwrapBenchmark" =>
                typeof(KeyUnwrapBenchmark),
            "decrypt" or "DecryptionBenchmark" =>
                typeof(DecryptionBenchmark),
            _ => null, // run all benchmarks
        };

        if (benchmarkType == null)
        {
            Console.WriteLine("  Running all micro-benchmarks...");
            Console.WriteLine();
            Console.WriteLine("  Micro-benchmarks:");
            Console.WriteLine("  • Authorization (4, 64, 256 roles)");
            Console.WriteLine("  • Key Unwrap (16, 24, 32 bytes)");
            Console.WriteLine("  • Decryption (5K, 10K, 100K rows with 2,000 columns)");
            Console.WriteLine();

            BenchmarkRunner.Run<AuthorizationBenchmark>();
            BenchmarkRunner.Run<KeyUnwrapBenchmark>();
            BenchmarkRunner.Run<DecryptionBenchmark>();
        }
        else
        {
            var benchmarkName = choice switch
            {
                "auth" or "AuthorizationBenchmark" => "Authorization",
                "unwrap" or "KeyUnwrapBenchmark" => "Key Unwrap",
                "decrypt" or "DecryptionBenchmark" => "Decryption",
                _ => "Unknown"
            };
            Console.WriteLine($"  Running benchmark: {benchmarkName}");
            Console.WriteLine($"   Type: {benchmarkType.Name}");
            Console.WriteLine();
            BenchmarkRunner.Run(benchmarkType);
        }

        Console.WriteLine();
        Console.WriteLine("╔══════════════════════════════════════════════════════════╗");
        Console.WriteLine("║  Benchmarks Complete                                     ║");
        Console.WriteLine("╚══════════════════════════════════════════════════════════╝");
        Console.WriteLine();
        Console.WriteLine("  Results saved to:");
        Console.WriteLine("   BenchmarkDotNet.Artifacts/results/ - Detailed reports");
        Console.WriteLine("   benchmark-metrics.csv - Custom metrics CSV");
        Console.WriteLine();
    }
}
