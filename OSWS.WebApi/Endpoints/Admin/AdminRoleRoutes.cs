using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using OSWS.KeyManager.Persistence;
using OSWS.Models.Entities;
using OSWS.WebApi.Services;

namespace OSWS.WebApi.Endpoints.Admin;

public static class AdminRoleRoutes
{
    public static void MapRoleRoutes(this RouteGroupBuilder roleGroup)
    {
        // GET /api/admin/roles
        roleGroup.MapGet(
            "/roles",
            async ([FromServices] OswsContext db, CancellationToken ct) =>
            {
                var roles = await db
                    .Roles.Select(r => new
                    {
                        r.Id,
                        r.Name,
                        ChildRoles = db
                            .RoleInheritances.Where(ri => ri.ParentRoleId == r.Id)
                            .Select(ri => new { ri.ChildRole.Id, ri.ChildRole.Name })
                            .ToList(),
                    })
                    .ToListAsync(ct);
                return Results.Ok(roles);
            }
        );

        // POST /api/admin/roles
        roleGroup.MapPost(
            "/roles",
            async (
                [FromBody] CreateRoleRequest body,
                [FromServices] OswsContext db,
                CancellationToken ct
            ) =>
            {
                if (await db.Roles.AnyAsync(r => r.Name == body.Name, ct))
                    return Results.Conflict("A role with that name already exists.");

                var role = new Role { Name = body.Name };
                db.Roles.Add(role);
                await db.SaveChangesAsync(ct);
                return Results.Ok(new { role.Id, role.Name });
            }
        );

        // DELETE /api/admin/roles/{id}
        roleGroup.MapDelete(
            "/roles/{id:int}",
            async (int id, [FromServices] OswsContext db, CancellationToken ct) =>
            {
                var role = await db.Roles.FindAsync([id], ct);
                if (role is null)
                    return Results.NotFound();

                // Cascade: remove RoleAssignments, Permissions, and RoleInheritances first
                var assignments = db.RoleAssignments.Where(ra => ra.RoleId == id);
                db.RoleAssignments.RemoveRange(assignments);

                var permissions = db.Permissions.Where(p => p.RoleId == id);
                db.Permissions.RemoveRange(permissions);

                var inheritances = db.RoleInheritances.Where(ri =>
                    ri.ParentRoleId == id || ri.ChildRoleId == id
                );
                db.RoleInheritances.RemoveRange(inheritances);

                db.Roles.Remove(role);
                await db.SaveChangesAsync(ct);
                return Results.NoContent();
            }
        );

        // POST /api/admin/roles/{parentId}/inherit/{childId}
        roleGroup.MapPost(
            "/roles/{parentId:int}/inherit/{childId:int}",
            async (
                int parentId,
                int childId,
                [FromServices] OswsContext db,
                [FromServices] RoleHierarchyService roleHierarchy,
                CancellationToken ct
            ) =>
            {
                if (await roleHierarchy.WouldCreateCycleAsync(parentId, childId, ct))
                    return Results.Conflict(
                        "This inheritance would create a cycle in the role hierarchy."
                    );

                var parent = await db.Roles.FindAsync([parentId], ct);
                if (parent is null)
                    return Results.NotFound("Parent role not found.");

                var child = await db.Roles.FindAsync([childId], ct);
                if (child is null)
                    return Results.NotFound("Child role not found.");

                var exists = await db.RoleInheritances.AnyAsync(
                    ri => ri.ParentRoleId == parentId && ri.ChildRoleId == childId,
                    ct
                );
                if (exists)
                    return Results.Conflict("Role already inherits the target role.");

                db.RoleInheritances.Add(
                    new RoleInheritance
                    {
                        ParentRoleId = parentId,
                        ParentRole = parent,
                        ChildRoleId = childId,
                        ChildRole = child,
                    }
                );
                await db.SaveChangesAsync(ct);
                return Results.NoContent();
            }
        );

        // DELETE /api/admin/roles/{parentId}/inherit/{childId}
        roleGroup.MapDelete(
            "/roles/{parentId:int}/inherit/{childId:int}",
            async (
                int parentId,
                int childId,
                [FromServices] OswsContext db,
                CancellationToken ct
            ) =>
            {
                var inheritance = await db.RoleInheritances.FirstOrDefaultAsync(
                    ri => ri.ParentRoleId == parentId && ri.ChildRoleId == childId,
                    ct
                );
                if (inheritance is null)
                    return Results.NotFound();

                db.RoleInheritances.Remove(inheritance);
                await db.SaveChangesAsync(ct);
                return Results.NoContent();
            }
        );
    }
}

public class CreateRoleRequest
{
    public required string Name { get; set; }
}
