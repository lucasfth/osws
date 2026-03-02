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
        // values: "1","2","3","4","5","6" or the full type name.
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
        }
        else
        {
            var isS3Benchmark =
                benchmarkType == typeof(Measurement5S3DirectVsOSWSBenchmark)
                || benchmarkType == typeof(Measurement6S3CacheEffectivenessBenchmark);
            var type = isS3Benchmark ? "S3/R2 integration" : "In-memory crypto";
            Console.WriteLine($"  Running benchmark: {benchmarkType.Name}");
            Console.WriteLine($"   Type: {type}");
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
