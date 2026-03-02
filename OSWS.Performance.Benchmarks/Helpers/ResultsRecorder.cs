using System.Globalization;

namespace OSWS.Performance.Benchmarks.Helpers
{
    /// <summary>
    /// Simple CSV recorder for benchmark metrics.  Each invocation appends a new
    /// line to <c>benchmark-metrics.csv</c> in the current working directory.
    /// The file can be opened in Excel/Numbers or parsed by any tool for easy
    /// inspection after running a sequence of measurements.
    /// </summary>
    public static class ResultsRecorder
    {
        private static readonly object _lock = new();
        private static bool _headerWritten;
        private static readonly string _path = "benchmark-metrics.csv";

        public static void Record(string benchmarkName, PerformanceMetrics metrics)
        {
            lock (_lock)
            {
                using var writer = new StreamWriter(_path, append: true);
                if (!_headerWritten)
                {
                    writer.WriteLine(
                        "Benchmark,TotalElapsedMs,InitialMemoryMb,PeakMemoryMb,MemoryIncreaseMb,AzureKvCallCount,S3CallCount,AzureKvAvgLatencyMs,S3AvgLatencyMs"
                    );
                    _headerWritten = true;
                }

                writer.WriteLine(
                    string.Join(
                        ",",
                        benchmarkName,
                        metrics.TotalElapsedMs.ToString(CultureInfo.InvariantCulture),
                        metrics.InitialMemoryMb.ToString(CultureInfo.InvariantCulture),
                        metrics.PeakMemoryMb.ToString(CultureInfo.InvariantCulture),
                        metrics.MemoryIncreaseMb.ToString(CultureInfo.InvariantCulture),
                        metrics.AzureKvCallCount,
                        metrics.S3CallCount,
                        metrics.AzureKvAvgLatencyMs.ToString(CultureInfo.InvariantCulture),
                        metrics.S3AvgLatencyMs.ToString(CultureInfo.InvariantCulture)
                    )
                );
            }
        }
    }
}
