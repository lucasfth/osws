using System.Globalization;

namespace OSWS.Performance.Benchmarks.Helpers;

/// <summary>
/// Generic runner for IMicroBenchmark instances.
/// Replaces BenchmarkDotNet. Handles warmup/measurement cycles, timing,
/// and CSV output via PerIterationCsvWriter.
/// </summary>
public static class MicroBenchmarkRunner
{
    public static async Task RunAsync(List<IMicroBenchmark> benchmarks)
    {
        var iterationCount = GetConfiguredIterationCount();
        var warmupCount = GetConfiguredWarmupCount();
        var outputPath = GetOutputPath();

        Console.WriteLine(
            $"Micro Benchmarks: {benchmarks.Count} config(s), "
                + $"{warmupCount} warmup + {iterationCount} iterations each"
        );
        Console.WriteLine();

        using var writer = new PerIterationCsvWriter(outputPath);

        foreach (var benchmark in benchmarks)
        {
            Console.WriteLine($"  {benchmark.Name} ({benchmark.Parameters})");

            await benchmark.SetupAsync();

            // Warmup iterations
            Console.Write($"    Warmup x{warmupCount}: ");
            for (int i = 1; i <= warmupCount; i++)
            {
                var metrics = new MetricsCollector();
                metrics.StartMeasurement();
                await benchmark.RunAsync(metrics);
                metrics.StopMeasurement();
                var m = metrics.GetMetrics();
                writer.WriteRow(
                    benchmark.Name,
                    benchmark.Parameters,
                    i,
                    isWarmup: true,
                    m,
                    iterationCount
                );
                Console.Write($"{m.TotalElapsedMs:F0}ms ");
            }
            Console.WriteLine();

            // Measurement iterations
            Console.Write($"    Measure x{iterationCount}: ");
            for (int i = 1; i <= iterationCount; i++)
            {
                var metrics = new MetricsCollector();
                metrics.StartMeasurement();
                await benchmark.RunAsync(metrics);
                metrics.StopMeasurement();
                var m = metrics.GetMetrics();
                writer.WriteRow(
                    benchmark.Name,
                    benchmark.Parameters,
                    i,
                    isWarmup: false,
                    m,
                    iterationCount
                );
                Console.Write($"{m.TotalElapsedMs:F0}ms ");
            }
            Console.WriteLine();

            await benchmark.CleanupAsync();
            benchmark.Dispose();
            Console.WriteLine();
        }

        Console.WriteLine($"Results: {Path.GetFullPath(outputPath)}");
    }

    // -----------------------------------------------------------------------
    // Configuration helpers (extracted from now-deleted SharedBenchmarkConfig)
    // -----------------------------------------------------------------------

    public static int GetConfiguredIterationCount()
    {
        if (
            int.TryParse(Environment.GetEnvironmentVariable("BENCH_ITERATIONS"), out var envIter)
            && envIter > 0
        )
            return envIter;

        var args = Environment.GetCommandLineArgs();
        for (var i = 0; i < args.Length; i++)
        {
            if (
                string.Equals(args[i], "--iterationCount", StringComparison.OrdinalIgnoreCase)
                && i + 1 < args.Length
                && int.TryParse(args[i + 1], out var argIter)
                && argIter > 0
            )
                return argIter;

            const string prefix = "--iterationCount=";
            if (
                args[i].StartsWith(prefix, StringComparison.OrdinalIgnoreCase)
                && int.TryParse(args[i][prefix.Length..], out var inlineArgIter)
                && inlineArgIter > 0
            )
                return inlineArgIter;
        }

        return 15; // default
    }

    public static int GetConfiguredWarmupCount()
    {
        if (
            int.TryParse(
                Environment.GetEnvironmentVariable("BENCH_WARMUP_COUNT"),
                out var envWarmup
            )
            && envWarmup > 0
        )
            return envWarmup;

        return 3; // default
    }

    // -----------------------------------------------------------------------
    // File-system helpers
    // -----------------------------------------------------------------------

    /// <summary>
    /// Resolve the benchmark project root by walking up from the current
    /// directory (same logic as the old ResultsRecorder.GetOutputPath).
    /// </summary>
    private static string GetProjectRoot()
    {
        var startDirs = new[]
        {
            new DirectoryInfo(Directory.GetCurrentDirectory()),
            new DirectoryInfo(AppContext.BaseDirectory),
        };

        foreach (var start in startDirs)
        {
            var current = start;
            while (current != null)
            {
                if (
                    string.Equals(
                        current.Name,
                        "OSWS.Performance.Benchmarks",
                        StringComparison.OrdinalIgnoreCase
                    )
                    && File.Exists(
                        Path.Combine(current.FullName, "OSWS.Performance.Benchmarks.csproj")
                    )
                )
                {
                    return current.FullName;
                }
                current = current.Parent;
            }
        }

        return Directory.GetCurrentDirectory();
    }

    private static string GetOutputPath()
    {
        var overridePath = Environment.GetEnvironmentVariable("BENCH_RESULTS_OUTPUT_PATH");
        if (!string.IsNullOrWhiteSpace(overridePath))
        {
            var outputDir = Path.GetFullPath(overridePath);
            Directory.CreateDirectory(outputDir);
            return Path.Combine(outputDir, "micro-results.csv");
        }

        var root = GetProjectRoot();
        var resultsDir = Path.Combine(root, "benchmark-results");
        Directory.CreateDirectory(resultsDir);
        var ts = DateTime.UtcNow.ToString("yyyyMMddTHHmmssZ", CultureInfo.InvariantCulture);
        return Path.Combine(resultsDir, $"micro-{ts}.csv");
    }

    /// <summary>
    /// Resolve a corpus parquet file by size label.
    /// Walks up to the project root and looks in benchmark-datasets/.
    /// Throws FileNotFoundException with a helpful message if missing.
    /// </summary>
    public static string FindCorpusFile(string sizeLabel)
    {
        var root = GetProjectRoot();
        var path = Path.Combine(root, "benchmark-datasets", $"{sizeLabel}.parquet");
        if (!File.Exists(path))
            throw new FileNotFoundException(
                $"Corpus file not found: {path}. Run 'dotnet run -- generate-datasets' first."
            );
        return path;
    }
}
