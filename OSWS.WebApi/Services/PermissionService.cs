using Microsoft.EntityFrameworkCore;
using OSWS.KeyManager.Persistence;

namespace OSWS.WebApi.Services;

/// <summary>
/// Resolves which parquet columns a user is permitted to decrypt,
/// based on their effective roles (direct + inherited).
/// </summary>
public sealed class PermissionService(RoleHierarchyService roleHierarchy, OswsContext db)
{
    /// <summary>
    /// Returns the set of column names the given user may decrypt.
    /// </summary>
    public async Task<HashSet<string>> GetAllowedColumnsAsync(
        int userId,
        CancellationToken cancellationToken = default
    )
    {
        var effectiveRoles = await roleHierarchy.GetEffectiveRolesAsync(userId, cancellationToken);
        var roleIds = effectiveRoles.Select(r => r.Id).ToList();

        var allowedColumns = await db
            .Permissions.Where(p => roleIds.Contains(p.RoleId))
            .Select(p => p.Column.Name)
            .Distinct()
            .ToListAsync(cancellationToken);

        return [.. allowedColumns];
    }

    /// <summary>
    /// Returns the effective role IDs for logging/debugging purposes.
    /// </summary>
    public async Task<List<int>> GetEffectiveRoleIdsAsync(
        int userId,
        CancellationToken cancellationToken = default
    )
    {
        var effectiveRoles = await roleHierarchy.GetEffectiveRolesAsync(userId, cancellationToken);
        return effectiveRoles.Select(r => r.Id).ToList();
    }
}
