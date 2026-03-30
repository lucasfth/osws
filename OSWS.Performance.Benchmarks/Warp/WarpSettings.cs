namespace OSWS.Performance.Benchmarks.Warp;

/// <summary>
/// Configuration for Warp baseline benchmarks.
/// Loaded from appsettings.json section "WarpSettings".
/// </summary>
public class WarpSettings
{
    /// <summary>
    /// Whether Warp benchmarks are enabled.
    /// Default: true
    /// </summary>
    public bool Enabled { get; set; } = true;

    /// <summary>
    /// Path to Warp executable (must be in PATH or provide full path).
    /// Default: "warp"
    /// </summary>
    public string WarpExecutablePath { get; set; } = "warp";

    /// <summary>
    /// Base port for OSWS instances (encrypted mode).
    /// Instances will use: BasePort, BasePort+1, BasePort+2, etc.
    /// For each instance, encrypted = BasePort+i, non-encrypted = BasePort+i+1
    /// Default: 5000
    /// </summary>
    public int OswsBasePort { get; set; } = 5000;

    /// <summary>
    /// Number of ports allocated per OSWS instance.
    /// 2 = one for encrypted, one for non-encrypted
    /// Default: 2
    /// </summary>
    public int OswsPortsPerInstance { get; set; } = 2;

    /// <summary>
    /// OSWS instance counts to test: 1, 2, 4, 8
    /// Each configuration will be benchmarked with Warp.
    /// Default: [1, 2, 4, 8]
    /// </summary>
    public int[] InstanceCounts { get; set; } = [1, 2, 4, 8];

    /// <summary>
    /// Number of concurrent Warp clients.
    /// Default: 16
    /// </summary>
    public int WarpConcurrency { get; set; } = 16;

    /// <summary>
    /// Duration of each Warp benchmark run in seconds.
    /// Default: 60
    /// </summary>
    public int WarpDurationSeconds { get; set; } = 60;

    /// <summary>
    /// Warp workload profile: "get", "put", "delete", "mixed"
    /// Default: "mixed"
    /// </summary>
    public string WorkloadProfile { get; set; } = "mixed";

    /// <summary>
    /// Directory where Warp results will be saved.
    /// Default: "./warp-results"
    /// </summary>
    public string ResultsDirectory { get; set; } = "./warp-results";

    /// <summary>
    /// Port for the load balancer (nginx).
    /// Encrypted traffic = LoadBalancerPort
    /// Non-encrypted traffic = LoadBalancerPort + 1
    /// Default: 8000
    /// </summary>
    public int LoadBalancerPort { get; set; } = 8000;

    /// <summary>
    /// Number of ports allocated per load balancer mode.
    /// 2 = one for encrypted, one for non-encrypted
    /// Default: 2
    /// </summary>
    public int LoadBalancerPortsPerMode { get; set; } = 2;

    /// <summary>
    /// Validates the configuration.
    /// </summary>
    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(WarpExecutablePath))
            throw new InvalidOperationException("WarpExecutablePath cannot be empty");

        if (OswsBasePort <= 0)
            throw new InvalidOperationException("OswsBasePort must be positive");

        if (OswsPortsPerInstance <= 0)
            throw new InvalidOperationException("OswsPortsPerInstance must be positive");

        if (InstanceCounts == null || InstanceCounts.Length == 0)
            throw new InvalidOperationException("InstanceCounts cannot be empty");

        if (WarpConcurrency <= 0)
            throw new InvalidOperationException("WarpConcurrency must be positive");

        if (WarpDurationSeconds <= 0)
            throw new InvalidOperationException("WarpDurationSeconds must be positive");

        if (!new[] { "get", "put", "delete", "mixed" }.Contains(WorkloadProfile.ToLower()))
            throw new InvalidOperationException(
                "WorkloadProfile must be one of: get, put, delete, mixed"
            );

        if (LoadBalancerPort <= 0)
            throw new InvalidOperationException("LoadBalancerPort must be positive");
    }
}
