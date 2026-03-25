using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using OSWS.Performance.Benchmarks.Helpers;
using OSWS.Performance.Benchmarks.Infrastructure.MockRbac;

namespace OSWS.Performance.Benchmarks.Measurements;

/// <summary>
/// Micro-benchmark: RBAC Authorization Latency
/// 
/// Measures how long it takes to authorize a user's access to columns
/// by varying the number of roles in the RBAC system.
/// 
/// Parameterized by role count: 4, 64, 256
/// 
/// This benchmark measures authorization latency with mock RBAC logic:
/// - Time to check if a user (with N roles) can access a column
/// - Performs 100 authorization checks per iteration
/// - Simulates realistic access control with ~30% resource accessibility
/// - Measures how authorization latency scales with role count
/// </summary>
[Config(typeof(SharedBenchmarkConfig))]
public class AuthorizationBenchmark
{
    [Params(4, 64, 256)]
    public int RoleCount { get; set; }

    private ServiceProvider? _services;
    private ILogger<AuthorizationBenchmark>? _logger;
    private IRbacAuthorizationService? _rbacService;
    private string[]? _testUserIds;
    private string[]? _testResourceIds;
    private string[][]? _userRoleAssignments;

    [GlobalSetup]
    public void GlobalSetup()
    {
        Console.WriteLine($"    Setting up RBAC Authorization benchmark (RoleCount={RoleCount})...");

        _services = BenchmarkServiceFactory.BuildServiceProvider();
        _logger = _services.GetRequiredService<ILogger<AuthorizationBenchmark>>();

        // Create mock RBAC service with RoleCount roles
        _rbacService = new MockRbacAuthorizationService(RoleCount, resourceCount: 1000);

        // Generate test users, resources, and role assignments
        _testUserIds = new string[100];
        _testResourceIds = new string[1000];
        _userRoleAssignments = new string[100][];

        for (int i = 0; i < 100; i++)
            _testUserIds[i] = $"user_{i}";
        for (int i = 0; i < 1000; i++)
            _testResourceIds[i] = $"resource_{i}";

        // Assign each user 2-5 roles from the available roles
        var availableRoles = _rbacService.GetAllRoles();
        var random = new Random(42); // Fixed seed for reproducibility

        for (int i = 0; i < 100; i++)
        {
            // Random number of roles per user (2-5)
            var roleCount = random.Next(2, 6);
            var userRoles = new string[Math.Min(roleCount, availableRoles.Count)];

            for (int j = 0; j < userRoles.Length; j++)
            {
                userRoles[j] = availableRoles[random.Next(availableRoles.Count)];
            }

            _userRoleAssignments[i] = userRoles;
        }

        Console.WriteLine($"   ✅ Setup complete for RBAC Authorization benchmark (RoleCount={RoleCount}, Users=100, Resources=1000)");
    }

    [Benchmark(Description = "RBAC Authorization Check - varies by role count")]
    public int MeasureRBACAuthorization()
    {
        if (_rbacService == null || _testUserIds == null || _testResourceIds == null || _userRoleAssignments == null)
            throw new InvalidOperationException("Benchmark setup incomplete");

        int authorizedCount = 0;

        // Perform multiple authorization checks to get stable measurements
        for (int i = 0; i < 100; i++)
        {
            var userIndex = i % _testUserIds.Length;
            var userId = _testUserIds[userIndex];
            var userRoles = _userRoleAssignments[userIndex];
            var resourceId = _testResourceIds[i % _testResourceIds.Length];

            // Measure authorization check latency through the RBAC service
            var isAuthorized = _rbacService.IsAuthorized(userId, userRoles, resourceId);
            if (isAuthorized)
                authorizedCount++;
        }

        return authorizedCount; // Return result to prevent optimization
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        Console.WriteLine($"   Cleaning up RBAC Authorization benchmark (RoleCount={RoleCount})");
        _services?.Dispose();
    }
}
