using OSWS.Performance.Benchmarks.Helpers;
using OSWS.Performance.Benchmarks.Infrastructure;
using OSWS.WebApi.Services;

namespace OSWS.Performance.Benchmarks.Measurements;

/// <summary>
/// Measures the cost of PermissionService.GetAllowedColumnsAsync
/// as the role-inheritance chain depth grows, isolating the
/// WITH RECURSIVE CTE overhead.
/// </summary>
public class PermissionHierarchyBenchmark : IMicroBenchmark
{
    private readonly int _hierarchyDepth;

    public string Name => "PermissionHierarchy";
    public string Parameters => $"hierarchy_depth={_hierarchyDepth}";

    private PermissionBenchmarkFixture? _fixture;

    public PermissionHierarchyBenchmark(int hierarchyDepth)
    {
        _hierarchyDepth = hierarchyDepth;
    }

    public async Task SetupAsync()
    {
        await Task.Run(() =>
        {
            _fixture = new PermissionBenchmarkFixture(
                directRoles: 1,
                hierarchyDepth: _hierarchyDepth
            );
            Console.WriteLine($"    Setup complete (depth={_hierarchyDepth})");
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
            Console.WriteLine($"    Cleanup (depth={_hierarchyDepth})");
            _fixture?.Dispose();
            _fixture = null;
        });
    }

    public void Dispose()
    {
        _fixture?.Dispose();
    }
}
