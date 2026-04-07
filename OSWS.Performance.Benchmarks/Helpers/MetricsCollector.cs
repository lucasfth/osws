using System.Diagnostics;

namespace OSWS.Performance.Benchmarks.Helpers;

/// <summary>
/// Collects performance metrics for measurement tests.
/// Tracks latency, throughput, memory usage, and external service calls.
/// Supports per-operation latency tracking and percentile calculations.
/// </summary>
public class MetricsCollector
{
    private readonly Stopwatch _stopwatch = new();
    private long _initialMemoryBytes;
    private long _peakMemoryBytes;
    private int _kvCallCount;
    private int _cachedKvCallCount;
    private int _s3CallCount;
    private readonly List<TimeSpan> _kvLatencies = new();
    private readonly List<TimeSpan> _cachedKvLatencies = new();
    private readonly List<TimeSpan> _s3Latencies = new();
    private readonly Dictionary<string, List<TimeSpan>> _operationLatencies = new();
    private readonly List<double> _elapsedSamples = new();

    public void StartMeasurement()
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        _initialMemoryBytes = GC.GetTotalMemory(true);
        _peakMemoryBytes = _initialMemoryBytes;
        _stopwatch.Restart();
    }

    public void StopMeasurement()
    {
        _stopwatch.Stop();
        UpdatePeakMemory();
        _elapsedSamples.Add(_stopwatch.Elapsed.TotalMilliseconds);
    }

    public void RecordKvCall(TimeSpan latency)
    {
        _kvCallCount++;
        _kvLatencies.Add(latency);
    }

    public void RecordCachedKvCall(TimeSpan latency)
    {
        _cachedKvCallCount++;
        _cachedKvLatencies.Add(latency);
    }

    public void RecordS3Call(TimeSpan latency)
    {
        _s3CallCount++;
        _s3Latencies.Add(latency);
    }

    public void RecordOperationLatency(string operation, TimeSpan latency)
    {
        if (!_operationLatencies.ContainsKey(operation))
        {
            _operationLatencies[operation] = new List<TimeSpan>();
        }

        _operationLatencies[operation].Add(latency);
    }

    private void UpdatePeakMemory()
    {
        var currentMemory = GC.GetTotalMemory(false);
        if (currentMemory > _peakMemoryBytes)
        {
            _peakMemoryBytes = currentMemory;
        }
    }

    public PerformanceMetrics GetMetrics()
    {
        var elapsedMs = _stopwatch.Elapsed.TotalMilliseconds;
        var elapsedStats = ComputeElapsedStats();

        return new PerformanceMetrics
        {
            TotalElapsedMs = elapsedStats.MeanMs,
            TotalElapsedMinMs = elapsedStats.MinMs,
            TotalElapsedMaxMs = elapsedStats.MaxMs,
            TotalElapsedMedianMs = elapsedStats.MedianMs,
            TotalElapsedP99Ms = elapsedStats.P99Ms,
            TotalElapsedStdDevMs = elapsedStats.StdDevMs,
            InitialMemoryMb = _initialMemoryBytes / (1024.0 * 1024.0),
            PeakMemoryMb = _peakMemoryBytes / (1024.0 * 1024.0),
            MemoryIncreaseMb = (_peakMemoryBytes - _initialMemoryBytes) / (1024.0 * 1024.0),
            KvCallCount = _kvCallCount,
            S3CallCount = _s3CallCount,
            KvAvgLatencyMs = _kvLatencies.Any()
                ? _kvLatencies.Average(l => l.TotalMilliseconds)
                : 0,
            S3AvgLatencyMs = _s3Latencies.Any()
                ? _s3Latencies.Average(l => l.TotalMilliseconds)
                : 0,
            KvTotalLatencyMs = _kvLatencies.Sum(l => l.TotalMilliseconds),
            S3TotalLatencyMs = _s3Latencies.Sum(l => l.TotalMilliseconds),
            CachedKvCallCount = _cachedKvCallCount,
            CachedKvAvgLatencyMs = _cachedKvLatencies.Any()
                ? _cachedKvLatencies.Average(l => l.TotalMilliseconds)
                : 0,
            CachedKvTotalLatencyMs = _cachedKvLatencies.Sum(l => l.TotalMilliseconds),
            OperationLatencies = new Dictionary<string, OperationLatencyStats>(
                _operationLatencies.ToDictionary(
                    kvp => kvp.Key,
                    kvp => CalculateStatistics(kvp.Value)
                )
            ),
        };
    }

    private ElapsedStats ComputeElapsedStats()
    {
        if (_elapsedSamples.Count == 0)
        {
            return new ElapsedStats { MinMs = 0, MaxMs = 0, MeanMs = 0, MedianMs = 0, P99Ms = 0, StdDevMs = 0 };
        }

        var sorted = _elapsedSamples.OrderBy(x => x).ToList();
        var mean = sorted.Average();
        var variance = sorted.Sum(x => Math.Pow(x - mean, 2)) / sorted.Count;
        var stdDev = Math.Sqrt(variance);

        return new ElapsedStats
        {
            MinMs = sorted[0],
            MaxMs = sorted[^1],
            MeanMs = mean,
            MedianMs = Percentile(sorted, 0.5),
            P99Ms = Percentile(sorted, 0.99),
            StdDevMs = stdDev,
        };
    }

    private static double Percentile(List<double> sortedValues, double percentile)
    {
        if (sortedValues.Count == 0)
            return 0;
        if (sortedValues.Count == 1)
            return sortedValues[0];

        var rank = percentile * (sortedValues.Count - 1);
        var lower = (int)Math.Floor(rank);
        var upper = (int)Math.Ceiling(rank);

        if (lower == upper)
            return sortedValues[lower];

        var weight = rank - lower;
        return sortedValues[lower] + (sortedValues[upper] - sortedValues[lower]) * weight;
    }

    private class ElapsedStats
    {
        public double MinMs { get; init; }
        public double MaxMs { get; init; }
        public double MeanMs { get; init; }
        public double MedianMs { get; init; }
        public double P99Ms { get; init; }
        public double StdDevMs { get; init; }
    }

    private static OperationLatencyStats CalculateStatistics(List<TimeSpan> latencies)
    {
        if (latencies.Count == 0)
        {
            return new OperationLatencyStats();
        }

        var sortedLatencies = latencies.OrderBy(l => l.TotalMilliseconds).ToList();
        var count = sortedLatencies.Count;
        var sum = latencies.Sum(l => l.TotalMilliseconds);
        var mean = sum / count;
        var variance = latencies.Sum(l => Math.Pow(l.TotalMilliseconds - mean, 2)) / count;
        var stdDev = Math.Sqrt(variance);

        return new OperationLatencyStats
        {
            Count = count,
            MinMs = sortedLatencies[0].TotalMilliseconds,
            MaxMs = sortedLatencies[count - 1].TotalMilliseconds,
            MeanMs = mean,
            MedianMs = PercentileLatency(sortedLatencies, 0.5),
            P99Ms = PercentileLatency(sortedLatencies, 0.99),
            StdDevMs = stdDev,
        };
    }

    private static double PercentileLatency(List<TimeSpan> sorted, double percentile)
    {
        if (sorted.Count == 0)
            return 0;
        if (sorted.Count == 1)
            return sorted[0].TotalMilliseconds;

        var rank = percentile * (sorted.Count - 1);
        var lower = (int)Math.Floor(rank);
        var upper = (int)Math.Ceiling(rank);

        if (lower == upper)
            return sorted[lower].TotalMilliseconds;

        var weight = rank - lower;
        var lowerVal = sorted[lower].TotalMilliseconds;
        var upperVal = sorted[upper].TotalMilliseconds;
        return lowerVal + (upperVal - lowerVal) * weight;
    }

    public Dictionary<string, OperationLatencyStats> GetAggregatedLatencies()
    {
        return new Dictionary<string, OperationLatencyStats>(
            _operationLatencies.ToDictionary(kvp => kvp.Key, kvp => CalculateStatistics(kvp.Value))
        );
    }

    public void Reset()
    {
        _stopwatch.Reset();
        _initialMemoryBytes = 0;
        _peakMemoryBytes = 0;
        _kvCallCount = 0;
        _cachedKvCallCount = 0;
        _s3CallCount = 0;
        _kvLatencies.Clear();
        _cachedKvLatencies.Clear();
        _s3Latencies.Clear();
        _operationLatencies.Clear();
    }
}

public class OperationLatencyStats
{
    public int Count { get; init; }
    public double MinMs { get; init; }
    public double MaxMs { get; init; }
    public double MeanMs { get; init; }
    public double MedianMs { get; init; }
    public double P99Ms { get; init; }
    public double StdDevMs { get; init; }
}

/// <summary>
/// Aggregate statistics for a single metric across multiple samples.
/// </summary>
public class MetricStats
{
    public int Count { get; init; }
    public double Min { get; init; }
    public double Max { get; init; }
    public double Avg { get; init; }
    public double Median { get; init; }
    public double P99 { get; init; }
    public double StdDev { get; init; }
}

public class PerformanceMetrics
{
    public int SampleCount { get; init; }

    // Total elapsed time stats
    public double TotalElapsedMs { get; init; }
    public double TotalElapsedMinMs { get; init; }
    public double TotalElapsedMaxMs { get; init; }
    public double TotalElapsedMedianMs { get; init; }
    public double TotalElapsedP99Ms { get; init; }
    public double TotalElapsedStdDevMs { get; init; }

    // Memory stats (aggregate)
    public MetricStats InitialMemoryStats { get; init; } = new();
    public MetricStats PeakMemoryStats { get; init; } = new();
    public MetricStats MemoryIncreaseStats { get; init; } = new();

    // Call count stats
    public MetricStats KvCallCountStats { get; init; } = new();
    public MetricStats CachedKvCallCountStats { get; init; } = new();
    public MetricStats S3CallCountStats { get; init; } = new();

    // Latency stats
    public MetricStats KvAvgLatencyStats { get; init; } = new();
    public MetricStats CachedKvAvgLatencyStats { get; init; } = new();
    public MetricStats S3AvgLatencyStats { get; init; } = new();

    // Single-run values for backward compatibility
    public double InitialMemoryMb { get; init; }
    public double PeakMemoryMb { get; init; }
    public double MemoryIncreaseMb { get; init; }
    public int KvCallCount { get; init; }
    public int S3CallCount { get; init; }
    public double KvAvgLatencyMs { get; init; }
    public double S3AvgLatencyMs { get; init; }
    public double KvTotalLatencyMs { get; init; }
    public double S3TotalLatencyMs { get; init; }

    // Cached KV metrics
    public int CachedKvCallCount { get; init; }
    public double CachedKvAvgLatencyMs { get; init; }
    public double CachedKvTotalLatencyMs { get; init; }

    public Dictionary<string, OperationLatencyStats> OperationLatencies { get; init; } = new();

    public override string ToString()
    {
        return $"""
            Performance Metrics:
              Total Elapsed: {TotalElapsedMs:F2} ms
              Initial Memory: {InitialMemoryMb:F2} MB
              Peak Memory: {PeakMemoryMb:F2} MB
              Memory Increase: {MemoryIncreaseMb:F2} MB
              KV Calls: {KvCallCount} (Avg: {KvAvgLatencyMs:F2} ms, Total: {KvTotalLatencyMs:F2} ms)
              Cached KV Calls: {CachedKvCallCount} (Avg: {CachedKvAvgLatencyMs:F2} ms, Total: {CachedKvTotalLatencyMs:F2} ms)
              S3 Calls: {S3CallCount} (Avg: {S3AvgLatencyMs:F2} ms, Total: {S3TotalLatencyMs:F2} ms)
            """;
    }
}
