using OSWS.Performance.Benchmarks.Helpers;
using OSWS.Performance.Benchmarks.Infrastructure;
using OSWS.WebApi.Services;

namespace OSWS.Performance.Benchmarks.Measurements;

/// <summary>
/// Measures the cost of PermissionService.GetAllowedColumnsAsync
/// across varying numbers of directly-assigned roles (flat hierarchy).
/// </summary>
public class PermissionServiceBenchmark : IMicroBenchmark
{
    private readonly int _roleCount;

    public string Name => "PermissionService";
    public string Parameters => $"role_count={_roleCount}";

    private PermissionBenchmarkFixture? _fixture;

    public PermissionServiceBenchmark(int roleCount)
    {
        _roleCount = roleCount;
    }

    public async Task SetupAsync()
    {
        await Task.Run(() =>
        {
            _fixture = new PermissionBenchmarkFixture(_roleCount);
            Console.WriteLine($"    Setup complete ({_roleCount} roles)");
        });
    }

    public async Task RunAsync(MetricsCollector metrics)
    {
        var (permissionService, context) = _fixture!.CreatePermissionService();
        try
        {
            await permissionService.GetAllowedColumnsAsync(
                _fixture!.UserId,
                CancellationToken.None
            );
        }
        finally
        {
            context.Dispose();
        }
    }

    public async Task CleanupAsync()
    {
        await Task.Run(() =>
        {
            Console.WriteLine($"    Cleanup ({_roleCount} roles)");
            _fixture?.Dispose();
            _fixture = null;
        });
    }

    public void Dispose()
    {
        _fixture?.Dispose();
    }
}
