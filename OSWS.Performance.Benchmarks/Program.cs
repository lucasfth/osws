using BenchmarkDotNet.Running;
using dotenv.net;
using OSWS.Performance.Benchmarks.DatasetGenerators;
using OSWS.Performance.Benchmarks.Infrastructure;
using OSWS.Performance.Benchmarks.Measurements;

namespace OSWS.Performance.Benchmarks;

public static class Program
{
    public static async Task Main(string[] args)
    {
        DotEnv.Fluent().WithoutExceptions().Load();

        switch (args.Length)
        {
            case > 0
                when string.Equals(
                    args[0],
                    "generate-datasets",
                    StringComparison.OrdinalIgnoreCase
                ):
            {
                var dir = args.Skip(1).FirstOrDefault() ?? "benchmark-datasets";
                Console.WriteLine($"Output dir: {Path.GetFullPath(dir)}");
                await ParquetGenerator.GenerateCorpusToDiskAsync(dir);
                Console.WriteLine("Datasets ready. Run next:");
                Console.WriteLine("  dotnet run -- generate-corpus");
                return;
            }
            case > 0
                when string.Equals(args[0], "generate-corpus", StringComparison.OrdinalIgnoreCase):
            {
                var exitCode = await BenchmarkCorpusUploader.RunAsync();
                Environment.ExitCode = exitCode;
                return;
            }
            case > 0
                when string.Equals(
                    args[0],
                    "seed-s3-credential",
                    StringComparison.OrdinalIgnoreCase
                ):
            {
                var exitCode = await BenchmarkS3CredentialSeeder.RunSeedAsync(
                    args.Skip(1).ToArray()
                );
                Environment.ExitCode = exitCode;
                return;
            }
            case > 0
                when string.Equals(
                    args[0],
                    "cleanup-s3-credential",
                    StringComparison.OrdinalIgnoreCase
                ):
            {
                var exitCode = await BenchmarkS3CredentialSeeder.RunCleanupAsync(
                    args.Skip(1).ToArray()
                );
                Environment.ExitCode = exitCode;
                return;
            }
            case > 0
                when string.Equals(args[0], "ensure-bucket", StringComparison.OrdinalIgnoreCase):
            {
                var exitCode = await S3BucketEnsurer.RunAsync();
                Environment.ExitCode = exitCode;
                return;
            }
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
            "auth" or "PermissionServiceBenchmark" => typeof(PermissionServiceBenchmark),
            "hierarchy" or "PermissionHierarchyBenchmark" => typeof(PermissionHierarchyBenchmark),
            "unwrap" or "KeyUnwrapBenchmark" => typeof(KeyUnwrapBenchmark),
            "decrypt" or "DecryptionBenchmark" => typeof(DecryptionBenchmark),
            _ => null, // run all benchmarks
        };

        if (benchmarkType == null)
        {
            Console.WriteLine("  Running all micro-benchmarks...");
            Console.WriteLine();
            Console.WriteLine("  Micro-benchmarks:");
            Console.WriteLine("  • Permission Service — flat hierarchy (4, 64, 256 direct roles)");
            Console.WriteLine("  • Permission Hierarchy — chain depth (0, 4, 16, 64)");
            Console.WriteLine("  • Key Unwrap (16, 24, 32 bytes)");
            Console.WriteLine("  • Decryption (5K, 10K, 100K rows with 2,000 columns)");
            Console.WriteLine();

            BenchmarkRunner.Run<PermissionServiceBenchmark>();
            BenchmarkRunner.Run<PermissionHierarchyBenchmark>();
            BenchmarkRunner.Run<KeyUnwrapBenchmark>();
            BenchmarkRunner.Run<DecryptionBenchmark>();
        }
        else
        {
            var benchmarkName = choice switch
            {
                "auth" or "PermissionServiceBenchmark" => "Permission Service (flat)",
                "hierarchy" or "PermissionHierarchyBenchmark" =>
                    "Permission Hierarchy (chain depth)",
                "unwrap" or "KeyUnwrapBenchmark" => "Key Unwrap",
                "decrypt" or "DecryptionBenchmark" => "Decryption",
                _ => "Unknown",
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
