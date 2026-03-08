using System.Globalization;

namespace OSWS.Performance.Benchmarks.Helpers
{
    /// <summary>
    /// CSV recorder for benchmark metrics. Outputs a single file with statistical percentiles
    /// including median, 99th percentile, min, max, mean, and standard deviation.
    /// Each invocation appends a new line to <c>benchmark-results.csv</c> in the parent directory
    /// of the benchmark results artifacts (BenchmarkDotNet.Artifacts/results).
    /// </summary>
    public static class ResultsRecorder
    {
        private static readonly Lock Lock = new();
        private static bool _headerWritten;

        private static string GetOutputPath()
        {
            // When running under BenchmarkDotNet, we're in a job subprocess folder.
            // Find the nearest BenchmarkDotNet.Artifacts folder and write there.
            var current = new System.IO.DirectoryInfo(System.IO.Directory.GetCurrentDirectory());
            while (current != null)
            {
                var artifactsDir = System.IO.Path.Combine(
                    current.FullName,
                    "BenchmarkDotNet.Artifacts"
                );
                if (System.IO.Directory.Exists(artifactsDir))
                {
                    return System.IO.Path.Combine(artifactsDir, "benchmark-results.csv");
                }
                current = current.Parent;
            }

            // Fallback: write to current directory
            return "benchmark-results.csv";
        }

        public static void Record(string benchmarkName, PerformanceMetrics metrics)
        {
            var csvPath = GetOutputPath();
            Console.WriteLine(
                $"[ResultsRecorder] Recording {benchmarkName} with {metrics.OperationLatencies.Count} operation latencies to {System.IO.Path.GetFullPath(csvPath)}"
            );

            lock (Lock)
            {
                using var writer = new StreamWriter(csvPath, append: true);
                if (!_headerWritten)
                {
                    var headerFields = new List<string>
                    {
                        "Benchmark",
                        "TotalElapsedMs",
                        "TotalElapsedMedianMs",
                        "TotalElapsedP99Ms",
                        "InitialMemoryMb",
                        "PeakMemoryMb",
                        "MemoryIncreaseMb",
                        "AzureKvCallCount",
                        "S3CallCount",
                        "AzureKvAvgLatencyMs",
                        "AzureKvTotalLatencyMs",
                        "S3AvgLatencyMs",
                        "S3TotalLatencyMs",
                    };

                    // Add operation latency columns dynamically
                    var operationNames = new SortedSet<string>();
                    if (metrics.OperationLatencies.Any())
                    {
                        foreach (var opName in metrics.OperationLatencies.Keys)
                        {
                            operationNames.Add(opName);
                        }

                        foreach (var opName in operationNames)
                        {
                            headerFields.Add($"{opName}_Count");
                            headerFields.Add($"{opName}_MinMs");
                            headerFields.Add($"{opName}_MaxMs");
                            headerFields.Add($"{opName}_MeanMs");
                            headerFields.Add($"{opName}_MedianMs");
                            headerFields.Add($"{opName}_P99Ms");
                            headerFields.Add($"{opName}_StdDevMs");
                        }
                    }

                    writer.WriteLine(string.Join(",", headerFields));
                    _headerWritten = true;
                }

                var values = new List<string>
                {
                    benchmarkName,
                    metrics.TotalElapsedMs.ToString(CultureInfo.InvariantCulture),
                    metrics.TotalElapsedMs.ToString(CultureInfo.InvariantCulture), // Placeholder for median
                    metrics.TotalElapsedMs.ToString(CultureInfo.InvariantCulture), // Placeholder for p99
                    metrics.InitialMemoryMb.ToString(CultureInfo.InvariantCulture),
                    metrics.PeakMemoryMb.ToString(CultureInfo.InvariantCulture),
                    metrics.MemoryIncreaseMb.ToString(CultureInfo.InvariantCulture),
                    metrics.AzureKvCallCount.ToString(CultureInfo.InvariantCulture),
                    metrics.S3CallCount.ToString(CultureInfo.InvariantCulture),
                    metrics.AzureKvAvgLatencyMs.ToString(CultureInfo.InvariantCulture),
                    metrics.AzureKvTotalLatencyMs.ToString(CultureInfo.InvariantCulture),
                    metrics.S3AvgLatencyMs.ToString(CultureInfo.InvariantCulture),
                    metrics.S3TotalLatencyMs.ToString(CultureInfo.InvariantCulture),
                };

                // Add operation latency statistics in consistent order
                var operationNamesForValues = new SortedSet<string>();
                if (metrics.OperationLatencies.Any())
                {
                    foreach (var opName in metrics.OperationLatencies.Keys)
                    {
                        operationNamesForValues.Add(opName);
                    }

                    foreach (var opName in operationNamesForValues)
                    {
                        if (metrics.OperationLatencies.TryGetValue(opName, out var stats))
                        {
                            values.Add(stats.Count.ToString(CultureInfo.InvariantCulture));
                            values.Add(stats.MinMs.ToString(CultureInfo.InvariantCulture));
                            values.Add(stats.MaxMs.ToString(CultureInfo.InvariantCulture));
                            values.Add(stats.MeanMs.ToString(CultureInfo.InvariantCulture));
                            values.Add(stats.MedianMs.ToString(CultureInfo.InvariantCulture));
                            values.Add(stats.P99Ms.ToString(CultureInfo.InvariantCulture));
                            values.Add(stats.StdDevMs.ToString(CultureInfo.InvariantCulture));
                        }
                    }
                }

                writer.WriteLine(string.Join(",", values));
            }
        }
    }
}
