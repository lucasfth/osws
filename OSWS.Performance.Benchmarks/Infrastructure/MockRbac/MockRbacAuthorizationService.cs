namespace OSWS.Performance.Benchmarks.Infrastructure.MockRbac;

/// <summary>
/// Mock in-memory implementation of RBAC authorization service for benchmarking.
/// Uses a simple role-to-resources matrix approach for predictable and consistent results.
/// </summary>
public class MockRbacAuthorizationService : IRbacAuthorizationService
{
    private readonly string[] _allRoles;
    private readonly Dictionary<string, HashSet<string>> _rolePermissions;
    private readonly Random _random;

    /// <summary>
    /// Initialize mock RBAC service with specified number of roles.
    /// Each role has access to approximately 30% of total resources (simulating realistic scenarios).
    /// </summary>
    /// <param name="roleCount">Number of roles to create (4, 64, 256, etc.)</param>
    /// <param name="resourceCount">Total number of resources in the system (default: 1000)</param>
    public MockRbacAuthorizationService(int roleCount = 256, int resourceCount = 1000)
    {
        _allRoles = new string[roleCount];
        _rolePermissions = new Dictionary<string, HashSet<string>>();
        _random = new Random(42); // Fixed seed for reproducibility

        // Initialize roles
        for (int i = 0; i < roleCount; i++)
        {
            _allRoles[i] = $"role_{i}";
        }

        // Assign resources to roles: ~30% of resources per role
        var resourcesPerRole = (int)(resourceCount * 0.3);
        for (int i = 0; i < roleCount; i++)
        {
            var role = _allRoles[i];
            var permissions = new HashSet<string>();

            for (int j = 0; j < resourcesPerRole; j++)
            {
                permissions.Add($"resource_{_random.Next(resourceCount)}");
            }

            _rolePermissions[role] = permissions;
        }
    }

    /// <summary>
    /// Check if user with specified roles can access the resource.
    /// Authorization succeeds if any of the user's roles has permission to the resource.
    /// </summary>
    public bool IsAuthorized(string userId, IEnumerable<string> userRoles, string resourceId)
    {
        // User is authorized if ANY of their roles has permission
        foreach (var role in userRoles)
        {
            if (
                _rolePermissions.TryGetValue(role, out var permissions)
                && permissions.Contains(resourceId)
            )
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Get all roles in the system.
    /// </summary>
    public IReadOnlyList<string> GetAllRoles() => _allRoles.AsReadOnly();

    /// <summary>
    /// Get the total number of roles configured.
    /// </summary>
    public int GetRoleCount() => _allRoles.Length;
}
