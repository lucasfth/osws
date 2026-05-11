using System.Globalization;
using System.Text;

namespace OSWS.Performance.Benchmarks.Helpers;

/// <summary>
/// Writes one CSV row per benchmark iteration (warmup + measurement).
/// </summary>
public class PerIterationCsvWriter : IDisposable
{
    private static readonly string[] Fields =
    [
        "benchmark",
        "parameters",
        "run_index",
        "is_warmup",
        "duration_ms",
        "initial_memory_mb",
        "peak_memory_mb",
        "memory_increase_mb",
        "kv_call_count",
        "cached_kv_call_count",
        "s3_call_count",
        "kv_avg_latency_ms",
        "cached_kv_avg_latency_ms",
        "s3_avg_latency_ms",
        "iterations_configured",
        "timestamp_utc",
    ];

    private readonly StreamWriter _writer;

    public PerIterationCsvWriter(string filePath)
    {
        var dir = Path.GetDirectoryName(filePath);
        if (!string.IsNullOrEmpty(dir))
            Directory.CreateDirectory(dir);

        _writer = new StreamWriter(filePath, append: false, encoding: new UTF8Encoding(false));
        _writer.WriteLine(string.Join(",", Fields));
        Console.WriteLine($"  Writing results to: {Path.GetFullPath(filePath)}");
    }

    public void WriteRow(
        string benchmark,
        string parameters,
        int runIndex,
        bool isWarmup,
        PerformanceMetrics metrics,
        int iterationsConfigured
    )
    {
        var values = new List<string>
        {
            EscapeCsv(benchmark),
            EscapeCsv(parameters),
            runIndex.ToString(CultureInfo.InvariantCulture),
            isWarmup ? "true" : "false",
            metrics.TotalElapsedMs.ToString("F2", CultureInfo.InvariantCulture),
            metrics.InitialMemoryMb.ToString("F2", CultureInfo.InvariantCulture),
            metrics.PeakMemoryMb.ToString("F2", CultureInfo.InvariantCulture),
            metrics.MemoryIncreaseMb.ToString("F2", CultureInfo.InvariantCulture),
            metrics.KvCallCount.ToString(CultureInfo.InvariantCulture),
            metrics.CachedKvCallCount.ToString(CultureInfo.InvariantCulture),
            metrics.S3CallCount.ToString(CultureInfo.InvariantCulture),
            metrics.KvAvgLatencyMs.ToString("F2", CultureInfo.InvariantCulture),
            metrics.CachedKvAvgLatencyMs.ToString("F2", CultureInfo.InvariantCulture),
            metrics.S3AvgLatencyMs.ToString("F2", CultureInfo.InvariantCulture),
            iterationsConfigured.ToString(CultureInfo.InvariantCulture),
            DateTime.UtcNow.ToString("o", CultureInfo.InvariantCulture),
        };
        _writer.WriteLine(string.Join(",", values));
        _writer.Flush();
    }

    private static string EscapeCsv(string value)
    {
        if (value.Contains(',') || value.Contains('"') || value.Contains('\n'))
            return $"\"{value.Replace("\"", "\"\"")}\"";
        return value;
    }

    public void Dispose() => _writer.Dispose();
}
