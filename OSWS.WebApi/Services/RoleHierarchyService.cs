using Microsoft.EntityFrameworkCore;
using OSWS.KeyManager.Persistence;
using OSWS.Models.Entities;

namespace OSWS.WebApi.Services;

/// <summary>
///     Resolves transitive role membership via recursive SQL CTEs.
///     EF navigation properties only model direct descendants, this service handles the full tree traversal.
/// </summary>
public sealed class RoleHierarchyService(OswsContext db)
{
    /// <summary>
    ///     Returns all roles transitively reachable <em>below</em> <paramref name="roleId" />
    ///     in the hierarchy. That is, every role that <paramref name="roleId" /> inherits,
    ///     directly or indirectly. The role itself is not included.
    ///     Example: for owner &gt; admin &gt; moderator &gt; user,
    ///     calling with <c>admin</c> returns [moderator, user].
    /// </summary>
    public Task<List<Role>> GetDescendantRolesAsync(int roleId, CancellationToken ct = default) =>
        db
            .Roles.FromSqlInterpolated(
                $"""
                WITH RECURSIVE descendants AS (
                  SELECT "ChildRoleId" AS "Id"
                  FROM "RoleInheritances"
                  WHERE "ParentRoleId" = {roleId}

                  UNION

                  SELECT ri."ChildRoleId"
                  FROM "RoleInheritances" ri
                  JOIN descendants d ON ri."ParentRoleId" = d."Id"
                )
                SELECT r."Id", r."Name"
                FROM "Roles" r
                JOIN descendants d ON r."Id" = d."Id"
                """
            )
            .ToListAsync(ct);

    /// <summary>
    ///     Returns every role a user effectively holds: their directly assigned roles
    ///     plus every role transitively reachable downward through the hierarchy.
    ///     Example: if user has <c>admin</c> and the hierarchy is admin &gt; moderator &gt; user,
    ///     this returns [admin, moderator, user].
    /// </summary>
    public Task<List<Role>> GetEffectiveRolesAsync(int userId, CancellationToken ct = default) =>
        db
            .Roles.FromSqlInterpolated(
                $"""
                WITH RECURSIVE effective AS (
                  SELECT ra."RoleId" AS "Id"
                  FROM "RoleAssignments" ra
                  WHERE ra."UserId" = {userId}

                  UNION

                  SELECT ri."ChildRoleId"
                  FROM "RoleInheritances" ri
                  JOIN effective e ON ri."ParentRoleId" = e."Id"
                )
                SELECT DISTINCT r."Id", r."Name"
                FROM "Roles" r
                JOIN effective e ON r."Id" = e."Id"
                """
            )
            .ToListAsync(ct);

    /// <summary>
    ///     Returns <c>true</c> if adding the edge <paramref name="parentId" /> → <paramref name="childId" />
    ///     would create a cycle in the hierarchy.
    ///     This is the case when <paramref name="parentId" /> is already reachable as a descendant
    ///     of <paramref name="childId" />, or when the two IDs are equal (self-loop).
    /// </summary>
    public async Task<bool> WouldCreateCycleAsync(
        int parentId,
        int childId,
        CancellationToken ct = default
    )
    {
        if (parentId == childId)
            return true;

        var hits = await db
            .Database.SqlQuery<int>(
                $"""
                WITH RECURSIVE descendants AS (
                  SELECT "ChildRoleId" AS "Value"
                  FROM "RoleInheritances"
                  WHERE "ParentRoleId" = {childId}

                  UNION

                  SELECT ri."ChildRoleId"
                  FROM "RoleInheritances" ri
                  JOIN descendants d ON ri."ParentRoleId" = d."Value"
                )
                SELECT 1 AS "Value" FROM descendants WHERE "Value" = {parentId}
                LIMIT 1
                """
            )
            .ToListAsync(ct);

        return hits.Count > 0;
    }
}
