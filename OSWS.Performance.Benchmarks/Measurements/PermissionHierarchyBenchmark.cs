using BenchmarkDotNet.Attributes;
using OSWS.Performance.Benchmarks.Infrastructure;
using OSWS.WebApi.Services;

namespace OSWS.Performance.Benchmarks.Measurements;

/// <summary>
/// Measures the cost of <see cref="PermissionService.GetAllowedColumnsAsync"/> as the
/// role-inheritance chain depth grows, isolating the <c>WITH RECURSIVE</c> CTE overhead.
/// </summary>
[Config(typeof(SharedBenchmarkConfig))]
[BenchmarkCategory("Authorization")]
public class PermissionHierarchyBenchmark
{
    [Params(0, 4, 16, 64)]
    public int HierarchyDepth { get; set; }

    private PermissionBenchmarkFixture? _fixture;
    private PermissionService? _permissionService;
    private OSWS.KeyManager.Persistence.OswsContext? _iterationContext;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _fixture = new PermissionBenchmarkFixture(directRoles: 1, hierarchyDepth: HierarchyDepth);
        Console.WriteLine($"   ✅ Setup complete (HierarchyDepth={HierarchyDepth})");
    }

    [IterationSetup]
    public void IterationSetup() =>
        (_permissionService, _iterationContext) = _fixture!.CreatePermissionService();

    [Benchmark(Description = "GetAllowedColumns: varies by hierarchy chain depth")]
    public async Task<HashSet<string>> MeasureGetAllowedColumns() =>
        await _permissionService!.GetAllowedColumnsAsync(_fixture!.UserId, CancellationToken.None);

    [IterationCleanup]
    public void IterationCleanup()
    {
        _iterationContext?.Dispose();
        _iterationContext = null;
        _permissionService = null;
    }

    [GlobalCleanup]
    public void GlobalCleanup() => _fixture?.Dispose();
}
