using dotenv.net;
using OSWS.Performance.Benchmarks.DatasetGenerators;
using OSWS.Performance.Benchmarks.Helpers;
using OSWS.Performance.Benchmarks.Infrastructure;
using OSWS.Performance.Benchmarks.Measurements;

namespace OSWS.Performance.Benchmarks;

public static class Program
{
    private static readonly string[] AllCorpusSizes =
    [
        "tiny",
        "small",
        "medium",
        "large",
        "xlarge",
    ];
    private static readonly string[] KeyUnwrapCorpusSizes = ["tiny", "small"];
    private static readonly int[] DekSizeBitsOptions = [128, 192, 256];
    private static readonly int[] RoleCounts = [4, 64, 256];
    private static readonly int[] HierarchyDepths = [0, 4, 16, 64];

    public static async Task Main(string[] args)
    {
        DotEnv.Fluent().WithoutExceptions().Load();

        // Infrastructure commands (unchanged)
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
        Console.WriteLine("║        OSWS Performance Benchmarks                       ║");
        Console.WriteLine("╚══════════════════════════════════════════════════════════╝");
        Console.WriteLine();

        var choice = Environment.GetEnvironmentVariable("BENCH_MEASUREMENT");
        if (string.IsNullOrWhiteSpace(choice) && args.Length > 0)
            choice = args[0];

        List<IMicroBenchmark> benchmarks;

        switch (choice)
        {
            case "decrypt":
            case "DecryptionBenchmark":
                Console.WriteLine("  Running: Decryption (all corpus sizes)");
                benchmarks = AllCorpusSizes
                    .Select(s => (IMicroBenchmark)new DecryptionBenchmark(s))
                    .ToList();
                break;

            case "unwrap":
            case "KeyUnwrapBenchmark":
                Console.WriteLine("  Running: Key Unwrap (tiny+small × 128/192/256 bits)");
                benchmarks = KeyUnwrapCorpusSizes
                    .SelectMany(
                        size => DekSizeBitsOptions,
                        (size, bits) => (IMicroBenchmark)new KeyUnwrapBenchmark(size, bits)
                    )
                    .ToList();
                break;

            case "auth":
            case "PermissionServiceBenchmark":
                Console.WriteLine("  Running: Permission Service (4, 64, 256 direct roles)");
                benchmarks = RoleCounts
                    .Select(r => (IMicroBenchmark)new PermissionServiceBenchmark(r))
                    .ToList();
                break;

            case "hierarchy":
            case "PermissionHierarchyBenchmark":
                Console.WriteLine("  Running: Permission Hierarchy (depth 0, 4, 16, 64)");
                benchmarks = HierarchyDepths
                    .Select(d => (IMicroBenchmark)new PermissionHierarchyBenchmark(d))
                    .ToList();
                break;

            default:
                Console.WriteLine("  Running all micro-benchmarks...");
                Console.WriteLine();
                Console.WriteLine("  Micro-benchmarks:");
                Console.WriteLine(
                    "  • Permission Service — flat hierarchy (4, 64, 256 direct roles)"
                );
                Console.WriteLine("  • Permission Hierarchy — chain depth (0, 4, 16, 64)");
                Console.WriteLine("  • Key Unwrap — tiny/small corpus × DEK 128/192/256 bits");
                Console.WriteLine("  • Decryption — tiny/small/medium/large/xlarge corpus sizes");
                Console.WriteLine();

                benchmarks =
                [
                    .. AllCorpusSizes.Select(s => (IMicroBenchmark)new DecryptionBenchmark(s)),
                    .. KeyUnwrapCorpusSizes.SelectMany(
                        size => DekSizeBitsOptions,
                        (size, bits) => (IMicroBenchmark)new KeyUnwrapBenchmark(size, bits)
                    ),
                    .. RoleCounts.Select(r => (IMicroBenchmark)new PermissionServiceBenchmark(r)),
                    .. HierarchyDepths.Select(d =>
                        (IMicroBenchmark)new PermissionHierarchyBenchmark(d)
                    ),
                ];
                break;
        }

        Console.WriteLine();
        await MicroBenchmarkRunner.RunAsync(benchmarks);

        Console.WriteLine();
        Console.WriteLine("╔══════════════════════════════════════════════════════════╗");
        Console.WriteLine("║  Benchmarks Complete                                     ║");
        Console.WriteLine("╚══════════════════════════════════════════════════════════╝");
        Console.WriteLine();
        Console.WriteLine("  Results saved to:");
        Console.WriteLine("   benchmark-results/micro-<timestamp>.csv — Per-iteration results");
        Console.WriteLine();
        Console.WriteLine("  Analyse with:");
        Console.WriteLine("   python Infrastructure/analyse-micro-results.py benchmark-results/");
        Console.WriteLine();
    }
}
