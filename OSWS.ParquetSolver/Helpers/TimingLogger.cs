using System.Diagnostics;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Logging;

namespace OSWS.ParquetSolver.Helpers;

/// <summary>
/// Structured timing logger for operation measurements.
/// Logs timing data in a format suitable for benchmarking analysis.
/// </summary>
public class TimingLogger
{
    private readonly ILogger? _logger;
    private readonly bool _enabled;

    public TimingLogger(ILogger? logger = null, bool enabled = false)
    {
        _logger = logger;
        _enabled = enabled;
    }

    /// <summary>
    /// Logs a single operation timing with structured data.
    /// </summary>
    public void LogOperationTiming(
        string operationName,
        long elapsedMilliseconds,
        Dictionary<string, object>? additionalData = null
    )
    {
        if (!_enabled || _logger == null)
            return;

        var timingData = new OperationTiming
        {
            Operation = operationName,
            ElapsedMs = elapsedMilliseconds,
            Timestamp = DateTime.UtcNow,
            AdditionalData = additionalData,
        };

        _logger.LogInformation("OperationTiming: {@TimingData}", timingData);
    }

    /// <summary>
    /// Executes an async operation and logs its timing.
    /// </summary>
    public async Task<T> MeasureAsync<T>(
        string operationName,
        Func<Task<T>> operation,
        Dictionary<string, object>? additionalData = null
    )
    {
        if (!_enabled)
            return await operation();

        var sw = Stopwatch.StartNew();
        try
        {
            return await operation();
        }
        finally
        {
            sw.Stop();
            LogOperationTiming(operationName, sw.ElapsedMilliseconds, additionalData);
        }
    }

    /// <summary>
    /// Executes a sync operation and logs its timing.
    /// </summary>
    public T Measure<T>(
        string operationName,
        Func<T> operation,
        Dictionary<string, object>? additionalData = null
    )
    {
        if (!_enabled)
            return operation();

        var sw = Stopwatch.StartNew();
        try
        {
            return operation();
        }
        finally
        {
            sw.Stop();
            LogOperationTiming(operationName, sw.ElapsedMilliseconds, additionalData);
        }
    }

    /// <summary>
    /// Records a timing measurement without executing an operation.
    /// Useful when timing is measured externally.
    /// </summary>
    public void RecordTiming(
        string operationName,
        long elapsedMilliseconds,
        Dictionary<string, object>? additionalData = null
    )
    {
        LogOperationTiming(operationName, elapsedMilliseconds, additionalData);
    }

    /// <summary>
    /// Represents a single operation timing measurement.
    /// </summary>
    [Serializable]
    public class OperationTiming
    {
        [JsonPropertyName("operation")]
        public string Operation { get; set; } = "";

        [JsonPropertyName("elapsed_ms")]
        public long ElapsedMs { get; set; }

        [JsonPropertyName("timestamp")]
        public DateTime Timestamp { get; set; }

        [JsonPropertyName("additional_data")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public Dictionary<string, object>? AdditionalData { get; set; }
    }
}
