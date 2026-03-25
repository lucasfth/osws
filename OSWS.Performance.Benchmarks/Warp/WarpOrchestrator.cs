using Microsoft.Extensions.Logging;

namespace OSWS.Performance.Benchmarks.Warp;

/// <summary>
/// Orchestrates Warp baseline benchmarks.
/// 
/// Responsibilities:
/// - Start/stop multiple OSWS instances
/// - Configure and run Warp with varying parameters
/// - Collect and parse results
/// - Generate results report
/// </summary>
public class WarpOrchestrator
{
    private readonly WarpSettings _settings;
    private readonly ILogger<WarpOrchestrator>? _logger;

    public WarpOrchestrator(WarpSettings settings, ILogger<WarpOrchestrator>? logger = null)
    {
        _settings = settings ?? throw new ArgumentNullException(nameof(settings));
        _logger = logger;

        _settings.Validate();
    }

    /// <summary>
    /// Run all configured Warp benchmarks.
    /// </summary>
    public async Task RunAllBenchmarks()
    {
        _logger?.LogInformation("Starting Warp baseline benchmarks");

        try
        {
            // Create results directory
            Directory.CreateDirectory(_settings.ResultsDirectory);

            // Run benchmark for each instance count configuration
            foreach (var instanceCount in _settings.InstanceCounts)
            {
                await RunBenchmarkForInstanceCount(instanceCount);
            }

            _logger?.LogInformation("All Warp benchmarks completed successfully");
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Warp benchmarks failed");
            throw;
        }
    }

    /// <summary>
    /// Run Warp benchmark with a specific number of OSWS instances.
    /// </summary>
    private async Task RunBenchmarkForInstanceCount(int instanceCount)
    {
        _logger?.LogInformation(
            "Starting Warp benchmark with {InstanceCount} OSWS instances",
            instanceCount
        );

        try
        {
            // TODO: Implement the following steps:
            // 1. Start OSWS instances (encrypted and non-encrypted modes)
            // 2. Wait for instances to be ready
            // 3. Configure load balancer
            // 4. Run Warp against encrypted mode
            // 5. Run Warp against non-encrypted mode
            // 6. Parse and store results
            // 7. Stop instances

            _logger?.LogInformation("Benchmark completed for {InstanceCount} instances", instanceCount);
        }
        catch (Exception ex)
        {
            _logger?.LogError(
                ex,
                "Benchmark failed for {InstanceCount} instances",
                instanceCount
            );
            throw;
        }
    }

    /// <summary>
    /// Check if Warp executable is available.
    /// </summary>
    public bool IsWarpAvailable()
    {
        try
        {
            var result = ExecuteCommand(_settings.WarpExecutablePath, "--version");
            _logger?.LogInformation("Warp version check: {Output}", result);
            return true;
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex, "Warp executable not found at {Path}", _settings.WarpExecutablePath);
            return false;
        }
    }

    /// <summary>
    /// Execute a shell command and return output.
    /// </summary>
    private string ExecuteCommand(string command, string arguments)
    {
        var processInfo = new System.Diagnostics.ProcessStartInfo
        {
            FileName = command,
            Arguments = arguments,
            UseShellExecute = false,
            RedirectStandardOutput = true,
            CreateNoWindow = true
        };

        using var process = System.Diagnostics.Process.Start(processInfo);
        if (process == null)
            throw new InvalidOperationException($"Failed to start process: {command}");

        var output = process.StandardOutput.ReadToEnd();
        process.WaitForExit();

        if (process.ExitCode != 0)
            throw new InvalidOperationException($"Command failed with exit code {process.ExitCode}");

        return output;
    }
}
