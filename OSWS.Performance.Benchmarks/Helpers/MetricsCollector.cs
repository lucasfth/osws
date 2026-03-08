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
    private int _azureKvCallCount;
    private int _s3CallCount;
    private readonly List<TimeSpan> _azureKvLatencies = new();
    private readonly List<TimeSpan> _s3Latencies = new();
    private readonly Dictionary<string, List<TimeSpan>> _operationLatencies = new();

    public void StartMeasurement()
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        _initialMemoryBytes = GC.GetTotalMemory(false);
        _peakMemoryBytes = _initialMemoryBytes;
        _stopwatch.Restart();
    }

    public void StopMeasurement()
    {
        _stopwatch.Stop();
        UpdatePeakMemory();
    }

    public void RecordAzureKvCall(TimeSpan latency)
    {
        _azureKvCallCount++;
        _azureKvLatencies.Add(latency);
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
        return new PerformanceMetrics
        {
            TotalElapsedMs = _stopwatch.Elapsed.TotalMilliseconds,
            InitialMemoryMb = _initialMemoryBytes / (1024.0 * 1024.0),
            PeakMemoryMb = _peakMemoryBytes / (1024.0 * 1024.0),
            MemoryIncreaseMb = (_peakMemoryBytes - _initialMemoryBytes) / (1024.0 * 1024.0),
            AzureKvCallCount = _azureKvCallCount,
            S3CallCount = _s3CallCount,
            AzureKvAvgLatencyMs = _azureKvLatencies.Any()
                ? _azureKvLatencies.Average(l => l.TotalMilliseconds)
                : 0,
            S3AvgLatencyMs = _s3Latencies.Any()
                ? _s3Latencies.Average(l => l.TotalMilliseconds)
                : 0,
            AzureKvTotalLatencyMs = _azureKvLatencies.Sum(l => l.TotalMilliseconds),
            S3TotalLatencyMs = _s3Latencies.Sum(l => l.TotalMilliseconds),
            OperationLatencies = new Dictionary<string, OperationLatencyStats>(
                _operationLatencies.ToDictionary(
                    kvp => kvp.Key,
                    kvp => CalculateStatistics(kvp.Value)
                )
            ),
        };
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
            MedianMs =
                count % 2 == 0
                    ? (
                        sortedLatencies[count / 2 - 1].TotalMilliseconds
                        + sortedLatencies[count / 2].TotalMilliseconds
                    ) / 2
                    : sortedLatencies[count / 2].TotalMilliseconds,
            P99Ms = sortedLatencies[(int)Math.Ceiling(count * 0.99) - 1].TotalMilliseconds,
            StdDevMs = stdDev,
        };
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
        _azureKvCallCount = 0;
        _s3CallCount = 0;
        _azureKvLatencies.Clear();
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

public class PerformanceMetrics
{
    public double TotalElapsedMs { get; init; }
    public double InitialMemoryMb { get; init; }
    public double PeakMemoryMb { get; init; }
    public double MemoryIncreaseMb { get; init; }
    public int AzureKvCallCount { get; init; }
    public int S3CallCount { get; init; }
    public double AzureKvAvgLatencyMs { get; init; }
    public double S3AvgLatencyMs { get; init; }
    public double AzureKvTotalLatencyMs { get; init; }
    public double S3TotalLatencyMs { get; init; }
    public Dictionary<string, OperationLatencyStats> OperationLatencies { get; init; } = new();

    public override string ToString()
    {
        return $"""
            Performance Metrics:
              Total Elapsed: {TotalElapsedMs:F2} ms
              Initial Memory: {InitialMemoryMb:F2} MB
              Peak Memory: {PeakMemoryMb:F2} MB
              Memory Increase: {MemoryIncreaseMb:F2} MB
              Azure KV Calls: {AzureKvCallCount} (Avg: {AzureKvAvgLatencyMs:F2} ms, Total: {AzureKvTotalLatencyMs:F2} ms)
              S3 Calls: {S3CallCount} (Avg: {S3AvgLatencyMs:F2} ms, Total: {S3TotalLatencyMs:F2} ms)
            """;
    }
}
