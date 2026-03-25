namespace OSWS.Performance.Benchmarks.Infrastructure.MockRbac;

/// <summary>
/// Interface for RBAC (Role-Based Access Control) authorization service.
/// Defines contract for checking if a user with specific roles can access a resource.
/// </summary>
public interface IRbacAuthorizationService
{
    /// <summary>
    /// Check if a user with specified roles is authorized to access a resource.
    /// </summary>
    /// <param name="userId">The user identifier</param>
    /// <param name="userRoles">Collection of role identifiers for the user</param>
    /// <param name="resourceId">The resource identifier to access</param>
    /// <returns>True if user is authorized, false otherwise</returns>
    bool IsAuthorized(string userId, IEnumerable<string> userRoles, string resourceId);

    /// <summary>
    /// Get all roles in the system.
    /// </summary>
    IReadOnlyList<string> GetAllRoles();

    /// <summary>
    /// Get the total number of roles configured in the system.
    /// </summary>
    int GetRoleCount();
}
