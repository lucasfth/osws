namespace OSWS.Performance.Benchmarks.Helpers;

/// <summary>
/// Contract for a microbenchmark scenario.
/// One instance represents one parameter combination (e.g., size=tiny, dek_bits=128).
/// The MicroBenchmarkRunner manages measurement timing;
/// benchmarks only implement the work.
/// </summary>
public interface IMicroBenchmark : IDisposable
{
    /// <summary>Label for the benchmark category, e.g. "Decryption", "KeyUnwrap".</summary>
    string Name { get; }

    /// <summary>Structured parameter string, e.g. "size=tiny" or "size=small,dek_bits=256".</summary>
    string Parameters { get; }

    /// <summary>One-time setup before iterations begin (build services, load files, encrypt).</summary>
    Task SetupAsync();

    /// <summary>
    /// Execute one measured iteration of the benchmark work.
    /// The runner calls MetricsCollector.StartMeasurement before
    /// and MetricsCollector.StopMeasurement after this method.
    /// Benchmarks may record sub-metrics (KV calls, S3 calls) via the collector.
    /// </summary>
    Task RunAsync(MetricsCollector metrics);

    /// <summary>One-time teardown after all iterations complete.</summary>
    Task CleanupAsync();
}
