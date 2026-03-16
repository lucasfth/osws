using OSWS.Performance.Benchmarks.Helpers;

namespace OSWS.Performance.Benchmarks.Measurements;

public abstract class ScenarioMeasurementBenchmarkBase
{
    private readonly Dictionary<string, int> _iterationCounts = new();
    private readonly Dictionary<string, int> _recordedWorkloadCounts = new();
    private readonly Dictionary<string, PerformanceMetrics> _accumulatedResults = new();

    protected static int WarmupCount => SharedBenchmarkConfig.DefaultWarmupCount;
    protected static int WorkloadIterationCount =>
        SharedBenchmarkConfig.GetConfiguredIterationCount();

    protected bool ShouldMeasure(string benchmarkLabel, string scenarioKey)
    {
        if (!_iterationCounts.ContainsKey(scenarioKey))
            _iterationCounts[scenarioKey] = 0;
        if (!_recordedWorkloadCounts.ContainsKey(scenarioKey))
            _recordedWorkloadCounts[scenarioKey] = 0;

        _iterationCounts[scenarioKey]++;

        var isWarmup = _iterationCounts[scenarioKey] <= WarmupCount;
        var hasRemainingWorkloadSlots =
            _recordedWorkloadCounts[scenarioKey] < WorkloadIterationCount;
        var measure = !isWarmup && hasRemainingWorkloadSlots;

        Console.WriteLine(
            $"[{benchmarkLabel}] {scenarioKey} iteration {_iterationCounts[scenarioKey]} "
                + $"(warmup={isWarmup}, measure={measure}, "
                + $"recorded={_recordedWorkloadCounts[scenarioKey]}/{WorkloadIterationCount})"
        );

        return measure;
    }

    protected void RecordIfMeasured(
        string scenarioKey,
        string resultPrefix,
        MetricsCollector metrics,
        bool measure
    )
    {
        if (!measure)
            return;

        metrics.StopMeasurement();
        var snapshot = metrics.GetMetrics();
        _accumulatedResults[$"{resultPrefix}_{DateTime.UtcNow.Ticks}"] = snapshot;
        _recordedWorkloadCounts[scenarioKey]++;
        metrics.Reset();
    }

    protected void FlushRecordedResults()
    {
        foreach (var (resultKey, metrics) in _accumulatedResults)
        {
            ResultsRecorder.Record(resultKey, metrics);
        }

        _accumulatedResults.Clear();
        ResultsRecorder.FlushPending();
    }
}
