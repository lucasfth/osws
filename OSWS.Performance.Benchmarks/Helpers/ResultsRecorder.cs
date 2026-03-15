using System.Globalization;

namespace OSWS.Performance.Benchmarks.Helpers
{
    /// <summary>
    /// CSV recorder for benchmark metrics. Outputs a single file with statistical percentiles
    /// including median, 99th percentile, min, max, mean, and standard deviation.
    /// Each invocation appends a new line to <c>benchmark-results.csv</c> in the
    /// <c>OSWS.Performance.Benchmarks</c> project root directory.
    /// </summary>
    public static class ResultsRecorder
    {
        private static readonly Lock Lock = new();
        private static bool _headerWritten;
        private static readonly int ExpectedRunCount =
            SharedBenchmarkConfig.GetConfiguredIterationCount();
        private static readonly Dictionary<string, List<PerformanceMetrics>> PendingSamples = new();

        private static string GetOutputPath()
        {
            // Optional explicit override for CI/local custom workflows.
            var overridePath = Environment.GetEnvironmentVariable("BENCH_RESULTS_OUTPUT_PATH");
            if (!string.IsNullOrWhiteSpace(overridePath))
            {
                var outputDir = Path.GetFullPath(overridePath);
                Directory.CreateDirectory(outputDir);
                return Path.Combine(outputDir, "benchmark-results.csv");
            }

            // BenchmarkDotNet executes benchmarks from generated job folders under bin/.
            // Walk up from both current directory and base directory to find the real benchmark project root.
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
                        return Path.Combine(current.FullName, "benchmark-results.csv");
                    }

                    current = current.Parent;
                }
            }

            // Fallback: write to current directory if project root could not be resolved.
            return "benchmark-results.csv";
        }

        public static void Record(string benchmarkName, PerformanceMetrics metrics)
        {
            lock (Lock)
            {
                var groupKey = NormalizeBenchmarkName(benchmarkName);
                if (!PendingSamples.TryGetValue(groupKey, out var samples))
                {
                    samples = [];
                    PendingSamples[groupKey] = samples;
                }

                samples.Add(metrics);

                if (samples.Count >= ExpectedRunCount)
                {
                    WriteAggregatedRow(groupKey, samples);
                    PendingSamples.Remove(groupKey);
                }
            }
        }

        /// <summary>
        /// Flush any remaining benchmark sample groups that did not reach the expected run count.
        /// </summary>
        public static void FlushPending()
        {
            lock (Lock)
            {
                foreach (var (groupKey, samples) in PendingSamples.ToList())
                {
                    if (samples.Count == 0)
                        continue;

                    WriteAggregatedRow(groupKey, samples);
                }

                PendingSamples.Clear();
            }
        }

        private static string NormalizeBenchmarkName(string benchmarkName)
        {
            var parts = benchmarkName.Split('_');
            if (parts.Length <= 1)
                return benchmarkName;

            var lastPart = parts[^1];
            if (lastPart.Length >= 10 && lastPart.All(char.IsDigit))
                return string.Join("_", parts[..^1]);

            return benchmarkName;
        }

        private static void WriteAggregatedRow(
            string benchmarkName,
            List<PerformanceMetrics> samples
        )
        {
            var csvPath = GetOutputPath();
            var aggregated = Aggregate(samples);

            Console.WriteLine(
                $"[ResultsRecorder] Writing aggregate for {benchmarkName} from {samples.Count}/{ExpectedRunCount} iterations (min: {aggregated.TotalElapsedMinMs:F2}ms, median: {aggregated.TotalElapsedMedianMs:F2}ms, max: {aggregated.TotalElapsedMaxMs:F2}ms, variance: {(aggregated.TotalElapsedMaxMs - aggregated.TotalElapsedMinMs):F2}ms) to {Path.GetFullPath(csvPath)}"
            );

            using var writer = new StreamWriter(csvPath, append: true);
            WriteHeaderIfNeeded(csvPath, writer, aggregated);
            WriteRow(writer, benchmarkName, aggregated);
        }

        private static void WriteHeaderIfNeeded(
            string csvPath,
            StreamWriter writer,
            PerformanceMetrics metrics
        )
        {
            if (_headerWritten)
                return;

            // If the file already exists and contains a header, avoid writing it again.
            if (File.Exists(csvPath) && new FileInfo(csvPath).Length > 0)
            {
                var firstLine = File.ReadLines(csvPath).FirstOrDefault();
                if (!string.IsNullOrWhiteSpace(firstLine) && firstLine.StartsWith("Benchmark,"))
                {
                    _headerWritten = true;
                    return;
                }
            }

            var headerFields = new List<string>
            {
                "Benchmark",
                "SampleCount",
                // TotalElapsed time
                "TotalElapsedMs_Avg",
                "TotalElapsedMs_Min",
                "TotalElapsedMs_Max",
                "TotalElapsedMs_Median",
                "TotalElapsedMs_P99",
                "TotalElapsedMs_StdDev",
                // Memory
                "InitialMemoryMb_Avg",
                "InitialMemoryMb_Min",
                "InitialMemoryMb_Max",
                "InitialMemoryMb_Median",
                "InitialMemoryMb_P99",
                "InitialMemoryMb_StdDev",
                "PeakMemoryMb_Avg",
                "PeakMemoryMb_Min",
                "PeakMemoryMb_Max",
                "PeakMemoryMb_Median",
                "PeakMemoryMb_P99",
                "PeakMemoryMb_StdDev",
                "MemoryIncreaseMb_Avg",
                "MemoryIncreaseMb_Min",
                "MemoryIncreaseMb_Max",
                "MemoryIncreaseMb_Median",
                "MemoryIncreaseMb_P99",
                "MemoryIncreaseMb_StdDev",
                // Call counts
                "KvCallCount_Avg",
                "KvCallCount_Min",
                "KvCallCount_Max",
                "KvCallCount_Median",
                "KvCallCount_P99",
                "KvCallCount_StdDev",
                "CachedKvCallCount_Avg",
                "CachedKvCallCount_Min",
                "CachedKvCallCount_Max",
                "CachedKvCallCount_Median",
                "CachedKvCallCount_P99",
                "CachedKvCallCount_StdDev",
                "S3CallCount_Avg",
                "S3CallCount_Min",
                "S3CallCount_Max",
                "S3CallCount_Median",
                "S3CallCount_P99",
                "S3CallCount_StdDev",
                // Latencies
                "KvAvgLatencyMs_Avg",
                "KvAvgLatencyMs_Min",
                "KvAvgLatencyMs_Max",
                "KvAvgLatencyMs_Median",
                "KvAvgLatencyMs_P99",
                "KvAvgLatencyMs_StdDev",
                "CachedKvAvgLatencyMs_Avg",
                "CachedKvAvgLatencyMs_Min",
                "CachedKvAvgLatencyMs_Max",
                "CachedKvAvgLatencyMs_Median",
                "CachedKvAvgLatencyMs_P99",
                "CachedKvAvgLatencyMs_StdDev",
                "S3AvgLatencyMs_Avg",
                "S3AvgLatencyMs_Min",
                "S3AvgLatencyMs_Max",
                "S3AvgLatencyMs_Median",
                "S3AvgLatencyMs_P99",
                "S3AvgLatencyMs_StdDev",
            };

            var operationNames = new SortedSet<string>();
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

            writer.WriteLine(string.Join(",", headerFields));
            _headerWritten = true;
        }

        private static void WriteRow(
            StreamWriter writer,
            string benchmarkName,
            PerformanceMetrics metrics
        )
        {
            var values = new List<string>
            {
                benchmarkName,
                metrics.SampleCount.ToString(CultureInfo.InvariantCulture),
                // TotalElapsed
                metrics.TotalElapsedMs.ToString(CultureInfo.InvariantCulture),
                metrics.TotalElapsedMinMs.ToString(CultureInfo.InvariantCulture),
                metrics.TotalElapsedMaxMs.ToString(CultureInfo.InvariantCulture),
                metrics.TotalElapsedMedianMs.ToString(CultureInfo.InvariantCulture),
                metrics.TotalElapsedP99Ms.ToString(CultureInfo.InvariantCulture),
                metrics.TotalElapsedStdDevMs.ToString(CultureInfo.InvariantCulture),
                // Memory stats
                metrics.InitialMemoryStats.Avg.ToString(CultureInfo.InvariantCulture),
                metrics.InitialMemoryStats.Min.ToString(CultureInfo.InvariantCulture),
                metrics.InitialMemoryStats.Max.ToString(CultureInfo.InvariantCulture),
                metrics.InitialMemoryStats.Median.ToString(CultureInfo.InvariantCulture),
                metrics.InitialMemoryStats.P99.ToString(CultureInfo.InvariantCulture),
                metrics.InitialMemoryStats.StdDev.ToString(CultureInfo.InvariantCulture),
                metrics.PeakMemoryStats.Avg.ToString(CultureInfo.InvariantCulture),
                metrics.PeakMemoryStats.Min.ToString(CultureInfo.InvariantCulture),
                metrics.PeakMemoryStats.Max.ToString(CultureInfo.InvariantCulture),
                metrics.PeakMemoryStats.Median.ToString(CultureInfo.InvariantCulture),
                metrics.PeakMemoryStats.P99.ToString(CultureInfo.InvariantCulture),
                metrics.PeakMemoryStats.StdDev.ToString(CultureInfo.InvariantCulture),
                metrics.MemoryIncreaseStats.Avg.ToString(CultureInfo.InvariantCulture),
                metrics.MemoryIncreaseStats.Min.ToString(CultureInfo.InvariantCulture),
                metrics.MemoryIncreaseStats.Max.ToString(CultureInfo.InvariantCulture),
                metrics.MemoryIncreaseStats.Median.ToString(CultureInfo.InvariantCulture),
                metrics.MemoryIncreaseStats.P99.ToString(CultureInfo.InvariantCulture),
                metrics.MemoryIncreaseStats.StdDev.ToString(CultureInfo.InvariantCulture),
                // Call counts
                metrics.KvCallCountStats.Avg.ToString(CultureInfo.InvariantCulture),
                metrics.KvCallCountStats.Min.ToString(CultureInfo.InvariantCulture),
                metrics.KvCallCountStats.Max.ToString(CultureInfo.InvariantCulture),
                metrics.KvCallCountStats.Median.ToString(CultureInfo.InvariantCulture),
                metrics.KvCallCountStats.P99.ToString(CultureInfo.InvariantCulture),
                metrics.KvCallCountStats.StdDev.ToString(CultureInfo.InvariantCulture),
                metrics.CachedKvCallCountStats.Avg.ToString(CultureInfo.InvariantCulture),
                metrics.CachedKvCallCountStats.Min.ToString(CultureInfo.InvariantCulture),
                metrics.CachedKvCallCountStats.Max.ToString(CultureInfo.InvariantCulture),
                metrics.CachedKvCallCountStats.Median.ToString(CultureInfo.InvariantCulture),
                metrics.CachedKvCallCountStats.P99.ToString(CultureInfo.InvariantCulture),
                metrics.CachedKvCallCountStats.StdDev.ToString(CultureInfo.InvariantCulture),
                metrics.S3CallCountStats.Avg.ToString(CultureInfo.InvariantCulture),
                metrics.S3CallCountStats.Min.ToString(CultureInfo.InvariantCulture),
                metrics.S3CallCountStats.Max.ToString(CultureInfo.InvariantCulture),
                metrics.S3CallCountStats.Median.ToString(CultureInfo.InvariantCulture),
                metrics.S3CallCountStats.P99.ToString(CultureInfo.InvariantCulture),
                metrics.S3CallCountStats.StdDev.ToString(CultureInfo.InvariantCulture),
                // Latencies
                metrics.KvAvgLatencyStats.Avg.ToString(CultureInfo.InvariantCulture),
                metrics.KvAvgLatencyStats.Min.ToString(CultureInfo.InvariantCulture),
                metrics.KvAvgLatencyStats.Max.ToString(CultureInfo.InvariantCulture),
                metrics.KvAvgLatencyStats.Median.ToString(CultureInfo.InvariantCulture),
                metrics.KvAvgLatencyStats.P99.ToString(CultureInfo.InvariantCulture),
                metrics.KvAvgLatencyStats.StdDev.ToString(CultureInfo.InvariantCulture),
                metrics.CachedKvAvgLatencyStats.Avg.ToString(CultureInfo.InvariantCulture),
                metrics.CachedKvAvgLatencyStats.Min.ToString(CultureInfo.InvariantCulture),
                metrics.CachedKvAvgLatencyStats.Max.ToString(CultureInfo.InvariantCulture),
                metrics.CachedKvAvgLatencyStats.Median.ToString(CultureInfo.InvariantCulture),
                metrics.CachedKvAvgLatencyStats.P99.ToString(CultureInfo.InvariantCulture),
                metrics.CachedKvAvgLatencyStats.StdDev.ToString(CultureInfo.InvariantCulture),
                metrics.S3AvgLatencyStats.Avg.ToString(CultureInfo.InvariantCulture),
                metrics.S3AvgLatencyStats.Min.ToString(CultureInfo.InvariantCulture),
                metrics.S3AvgLatencyStats.Max.ToString(CultureInfo.InvariantCulture),
                metrics.S3AvgLatencyStats.Median.ToString(CultureInfo.InvariantCulture),
                metrics.S3AvgLatencyStats.P99.ToString(CultureInfo.InvariantCulture),
                metrics.S3AvgLatencyStats.StdDev.ToString(CultureInfo.InvariantCulture),
            };

            var operationNamesForValues = new SortedSet<string>();
            foreach (var opName in metrics.OperationLatencies.Keys)
            {
                operationNamesForValues.Add(opName);
            }

            foreach (var opName in operationNamesForValues)
            {
                if (!metrics.OperationLatencies.TryGetValue(opName, out var stats))
                    continue;
                values.Add(stats.Count.ToString(CultureInfo.InvariantCulture));
                values.Add(stats.MinMs.ToString(CultureInfo.InvariantCulture));
                values.Add(stats.MaxMs.ToString(CultureInfo.InvariantCulture));
                values.Add(stats.MeanMs.ToString(CultureInfo.InvariantCulture));
                values.Add(stats.MedianMs.ToString(CultureInfo.InvariantCulture));
                values.Add(stats.P99Ms.ToString(CultureInfo.InvariantCulture));
                values.Add(stats.StdDevMs.ToString(CultureInfo.InvariantCulture));
            }

            writer.WriteLine(string.Join(",", values));
        }

        private static PerformanceMetrics Aggregate(List<PerformanceMetrics> samples)
        {
            var elapsed = samples.Select(s => s.TotalElapsedMs).OrderBy(x => x).ToArray();
            var elapsedMedian = Percentile(elapsed, 0.5);
            var elapsedP99 = Percentile(elapsed, 0.99);
            var elapsedMin = elapsed.Length > 0 ? elapsed[0] : 0;
            var elapsedMax = elapsed.Length > 0 ? elapsed[^1] : 0;
            var elapsedStdDev = StdDev(elapsed);

            // Compute stats for each metric
            var initialMemStats = ComputeStats(samples.Select(s => s.InitialMemoryMb).ToArray());
            var peakMemStats = ComputeStats(samples.Select(s => s.PeakMemoryMb).ToArray());
            var memIncStats = ComputeStats(samples.Select(s => s.MemoryIncreaseMb).ToArray());
            var kvCallStats = ComputeStats(samples.Select(s => (double)s.KvCallCount).ToArray());
            var cachedKvCallStats = ComputeStats(
                samples.Select(s => (double)s.CachedKvCallCount).ToArray()
            );
            var s3CallStats = ComputeStats(samples.Select(s => (double)s.S3CallCount).ToArray());
            var kvLatencyStats = ComputeStats(samples.Select(s => s.KvAvgLatencyMs).ToArray());
            var cachedKvLatencyStats = ComputeStats(
                samples.Select(s => s.CachedKvAvgLatencyMs).ToArray()
            );
            var s3LatencyStats = ComputeStats(samples.Select(s => s.S3AvgLatencyMs).ToArray());

            var operationNames = samples
                .SelectMany(s => s.OperationLatencies.Keys)
                .Distinct(StringComparer.Ordinal)
                .OrderBy(n => n, StringComparer.Ordinal)
                .ToArray();

            var opAggregate = new Dictionary<string, OperationLatencyStats>(StringComparer.Ordinal);
            foreach (var opName in operationNames)
            {
                var perRun = samples
                    .Select(s =>
                        s.OperationLatencies.TryGetValue(opName, out var stats)
                            ? stats
                            : new OperationLatencyStats()
                    )
                    .ToList();

                var count = perRun.Sum(x => x.Count);
                var min = perRun
                    .Where(x => x.Count > 0)
                    .Select(x => x.MinMs)
                    .DefaultIfEmpty(0)
                    .Min();
                var max = perRun
                    .Where(x => x.Count > 0)
                    .Select(x => x.MaxMs)
                    .DefaultIfEmpty(0)
                    .Max();

                var weightedMean = count > 0 ? perRun.Sum(x => x.MeanMs * x.Count) / count : 0;

                var medians = perRun
                    .Where(x => x.Count > 0)
                    .Select(x => x.MedianMs)
                    .OrderBy(x => x)
                    .ToArray();
                var median = medians.Length > 0 ? Percentile(medians, 0.5) : 0;
                var p99 = perRun
                    .Where(x => x.Count > 0)
                    .Select(x => x.P99Ms)
                    .DefaultIfEmpty(0)
                    .Max();
                var stdDev = perRun
                    .Where(x => x.Count > 0)
                    .Select(x => x.StdDevMs)
                    .DefaultIfEmpty(0)
                    .Average();

                opAggregate[opName] = new OperationLatencyStats
                {
                    Count = count,
                    MinMs = min,
                    MaxMs = max,
                    MeanMs = weightedMean,
                    MedianMs = median,
                    P99Ms = p99,
                    StdDevMs = stdDev,
                };
            }

            return new PerformanceMetrics
            {
                SampleCount = samples.Count,
                TotalElapsedMs = elapsed.Length > 0 ? elapsed.Average() : 0,
                TotalElapsedMedianMs = elapsedMedian,
                TotalElapsedP99Ms = elapsedP99,
                TotalElapsedMinMs = elapsedMin,
                TotalElapsedMaxMs = elapsedMax,
                TotalElapsedStdDevMs = elapsedStdDev,
                InitialMemoryStats = initialMemStats,
                PeakMemoryStats = peakMemStats,
                MemoryIncreaseStats = memIncStats,
                KvCallCountStats = kvCallStats,
                CachedKvCallCountStats = cachedKvCallStats,
                S3CallCountStats = s3CallStats,
                KvAvgLatencyStats = kvLatencyStats,
                CachedKvAvgLatencyStats = cachedKvLatencyStats,
                S3AvgLatencyStats = s3LatencyStats,
                // Backward compatibility: single-run values (use averages)
                InitialMemoryMb = samples.Count > 0 ? samples.Average(s => s.InitialMemoryMb) : 0,
                PeakMemoryMb = samples.Count > 0 ? samples.Max(s => s.PeakMemoryMb) : 0,
                MemoryIncreaseMb = samples.Count > 0 ? samples.Average(s => s.MemoryIncreaseMb) : 0,
                KvCallCount =
                    samples.Count > 0 ? (int)Math.Round(samples.Average(s => s.KvCallCount)) : 0,
                CachedKvCallCount =
                    samples.Count > 0
                        ? (int)Math.Round(samples.Average(s => s.CachedKvCallCount))
                        : 0,
                S3CallCount =
                    samples.Count > 0 ? (int)Math.Round(samples.Average(s => s.S3CallCount)) : 0,
                KvAvgLatencyMs = samples.Count > 0 ? samples.Average(s => s.KvAvgLatencyMs) : 0,
                CachedKvAvgLatencyMs =
                    samples.Count > 0 ? samples.Average(s => s.CachedKvAvgLatencyMs) : 0,
                S3AvgLatencyMs = samples.Count > 0 ? samples.Average(s => s.S3AvgLatencyMs) : 0,
                KvTotalLatencyMs =
                    samples.Count > 0 ? samples.Sum(s => s.KvTotalLatencyMs) / samples.Count : 0,
                CachedKvTotalLatencyMs =
                    samples.Count > 0
                        ? samples.Sum(s => s.CachedKvTotalLatencyMs) / samples.Count
                        : 0,
                S3TotalLatencyMs =
                    samples.Count > 0 ? samples.Sum(s => s.S3TotalLatencyMs) / samples.Count : 0,
                OperationLatencies = opAggregate,
            };
        }

        private static MetricStats ComputeStats(double[] values)
        {
            if (values.Length == 0)
                return new MetricStats();

            var sorted = values.OrderBy(x => x).ToArray();
            var avg = values.Average();
            var median = Percentile(sorted, 0.5);
            var p99 = Percentile(sorted, 0.99);
            var min = sorted[0];
            var max = sorted[^1];
            var stdDev = StdDev(values);

            return new MetricStats
            {
                Count = values.Length,
                Avg = avg,
                Median = median,
                P99 = p99,
                Min = min,
                Max = max,
                StdDev = stdDev,
            };
        }

        private static double StdDev(double[] values)
        {
            if (values.Length < 2)
                return 0;
            var avg = values.Average();
            var variance = values.Sum(x => Math.Pow(x - avg, 2)) / values.Length;
            return Math.Sqrt(variance);
        }

        private static double Percentile(double[] sortedValues, double percentile)
        {
            if (sortedValues.Length == 0)
                return 0;
            if (sortedValues.Length == 1)
                return sortedValues[0];

            var clamped = Math.Clamp(percentile, 0.0, 1.0);
            var rank = clamped * (sortedValues.Length - 1);
            var lower = (int)Math.Floor(rank);
            var upper = (int)Math.Ceiling(rank);

            if (lower == upper)
                return sortedValues[lower];

            var weight = rank - lower;
            return sortedValues[lower] + (sortedValues[upper] - sortedValues[lower]) * weight;
        }
    }
}
