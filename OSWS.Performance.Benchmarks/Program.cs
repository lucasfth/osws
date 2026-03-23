using BenchmarkDotNet.Running;
using OSWS.Performance.Benchmarks.Measurements;

namespace OSWS.Performance.Benchmarks;

public static class Program
{
    public static void Main(string[] args)
    {
        Console.WriteLine("╔══════════════════════════════════════════════════════════╗");
        Console.WriteLine("║        OSWS Performance Benchmarks                       ║");
        Console.WriteLine("╚══════════════════════════════════════════════════════════╝");
        Console.WriteLine();

        // allow selecting a specific benchmark via environment variable or first CLI arg.
        // values: "1"-"10" or the full type name.
        var choice = Environment.GetEnvironmentVariable("BENCH_MEASUREMENT");
        if (string.IsNullOrWhiteSpace(choice) && args.Length > 0)
        {
            choice = args[0];
        }

        var benchmarkType = choice switch
        {
            "1" or "Measurement1ColdWideRangeRequestBenchmark" =>
                typeof(Measurement1ColdWideRangeRequestBenchmark),
            "2" or "Measurement2WarmDekColumnSelectBenchmark" =>
                typeof(Measurement2WarmDekColumnSelectBenchmark),
            "3" or "Measurement3FullDecryptionThroughputBenchmark" =>
                typeof(Measurement3FullDecryptionThroughputBenchmark),
            "4" or "Measurement4DekCacheStressTestBenchmark" =>
                typeof(Measurement4DekCacheStressTestBenchmark),
            "5" or "Measurement5S3DirectVsOSWSBenchmark" =>
                typeof(Measurement5S3DirectVsOSWSBenchmark),
            "6" or "Measurement6S3CacheEffectivenessBenchmark" =>
                typeof(Measurement6S3CacheEffectivenessBenchmark),
            "7" or "Measurement7KeyUnwrapBenchmark" =>
                typeof(Measurement7KeyUnwrapBenchmark),
            "8" or "Measurement8ColumnDecryptionBenchmark" =>
                typeof(Measurement8ColumnDecryptionBenchmark),
            "9" or "Measurement9S3WriteDirectVsOSWSBenchmark" =>
                typeof(Measurement9S3WriteDirectVsOSWSBenchmark),
            "10" or "Measurement10RBACAuthorizationBenchmark" =>
                typeof(Measurement10RBACAuthorizationBenchmark),
            _ => null, // run all benchmarks
        };

        if (benchmarkType == null)
        {
            Console.WriteLine("  Running all benchmarks...");
            Console.WriteLine();
            Console.WriteLine("  Measurements 1-4: In-memory crypto performance");
            BenchmarkRunner.Run<Measurement1ColdWideRangeRequestBenchmark>();
            BenchmarkRunner.Run<Measurement2WarmDekColumnSelectBenchmark>();
            BenchmarkRunner.Run<Measurement3FullDecryptionThroughputBenchmark>();
            BenchmarkRunner.Run<Measurement4DekCacheStressTestBenchmark>();

            Console.WriteLine();
            Console.WriteLine("  Measurements 5-6: S3/R2 integration performance");
            BenchmarkRunner.Run<Measurement5S3DirectVsOSWSBenchmark>();
            BenchmarkRunner.Run<Measurement6S3CacheEffectivenessBenchmark>();

            Console.WriteLine();
            Console.WriteLine("  Measurements 7-10: Micro-benchmarks (single parameter variation)");
            BenchmarkRunner.Run<Measurement7KeyUnwrapBenchmark>();
            BenchmarkRunner.Run<Measurement8ColumnDecryptionBenchmark>();
            BenchmarkRunner.Run<Measurement9S3WriteDirectVsOSWSBenchmark>();
            BenchmarkRunner.Run<Measurement10RBACAuthorizationBenchmark>();
        }
        else
        {
            var benchmarkCategory = choice switch
            {
                "1" or "2" or "3" or "4" => "In-memory crypto",
                "5" or "6" => "S3/R2 integration",
                "7" or "8" or "9" or "10" => "Micro-benchmark",
                _ => "Unknown"
            };
            Console.WriteLine($"  Running benchmark: {benchmarkType.Name}");
            Console.WriteLine($"   Category: {benchmarkCategory}");
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
