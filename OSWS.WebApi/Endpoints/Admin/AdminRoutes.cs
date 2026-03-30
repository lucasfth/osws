using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using OSWS.KeyManager.Persistence;
using OSWS.Models.Entities;
using OSWS.WebApi.Services;

namespace OSWS.WebApi.Endpoints.Admin;

public static class AdminRoutes
{
    public static void MapAdminRoutes(this IEndpointRouteBuilder app)
    {
        var admin = app.MapGroup("/api/admin").RequireAuthorization("AdminPolicy");

        MapRoleEndpoints(admin);
        MapUserEndpoints(admin);
        MapColumnEndpoints(admin);
    }

    private static void MapRoleEndpoints(RouteGroupBuilder roleGroup)
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
                    return Results.Conflict("This inheritance would create a cycle in the role hierarchy.");

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

    private static void MapUserEndpoints(RouteGroupBuilder userGroup)
    {
        // GET /api/admin/users
        userGroup.MapGet(
            "/users",
            async (
                [FromServices] OswsContext db,
                [FromServices] RoleHierarchyService roleHierarchy,
                CancellationToken ct
            ) =>
            {
                var users = await db.Users
                    .Select(u => new { u.Id, u.Name, u.Email })
                    .ToListAsync(ct);

                var result = new List<object>(users.Count);
                foreach (var u in users)
                {
                    var roles = await roleHierarchy.GetEffectiveRolesAsync(u.Id, ct);
                    result.Add(new
                    {
                        u.Id,
                        u.Name,
                        u.Email,
                        Roles = roles.Select(r => new { r.Id, r.Name }),
                    });
                }

                return Results.Ok(result);
            }
        );

        // POST /api/admin/users/{userId}/roles/{roleId}
        userGroup.MapPost(
            "/users/{userId:int}/roles/{roleId:int}",
            async (int userId, int roleId, [FromServices] OswsContext db, CancellationToken ct) =>
            {
                var user = await db.Users.FindAsync([userId], ct);
                if (user is null)
                    return Results.NotFound("User not found.");

                var role = await db.Roles.FindAsync([roleId], ct);
                if (role is null)
                    return Results.NotFound("Role not found.");

                var exists = await db.RoleAssignments.AnyAsync(
                    ra => ra.UserId == userId && ra.RoleId == roleId,
                    ct
                );
                if (exists)
                    return Results.Conflict("User already has this role.");

                db.RoleAssignments.Add(
                    new RoleAssignment
                    {
                        UserId = userId,
                        RoleId = roleId,
                        User = user,
                        Role = role,
                    }
                );
                await db.SaveChangesAsync(ct);
                return Results.NoContent();
            }
        );

        // DELETE /api/admin/users/{userId}/roles/{roleId}
        userGroup.MapDelete(
            "/users/{userId:int}/roles/{roleId:int}",
            async (int userId, int roleId, [FromServices] OswsContext db, CancellationToken ct) =>
            {
                var assignment = await db.RoleAssignments.FirstOrDefaultAsync(
                    ra => ra.UserId == userId && ra.RoleId == roleId,
                    ct
                );
                if (assignment is null)
                    return Results.NotFound();

                db.RoleAssignments.Remove(assignment);
                await db.SaveChangesAsync(ct);
                return Results.NoContent();
            }
        );
    }

    private static void MapColumnEndpoints(RouteGroupBuilder columnGroup)
    {
        // GET /api/admin/columns
        columnGroup.MapGet(
            "/columns",
            async ([FromServices] OswsContext db, CancellationToken ct) =>
            {
                var columns = await db
                    .Columns.Include(c => c.Roles)
                    .Select(c => new
                    {
                        c.Id,
                        c.Name,
                        Roles = c.Roles.Select(r => new { r.Id, r.Name }),
                    })
                    .ToListAsync(ct);
                return Results.Ok(columns);
            }
        );

        // POST /api/admin/columns/{columnId}/roles/{roleId}
        columnGroup.MapPost(
            "/columns/{columnId:int}/roles/{roleId:int}",
            async (int columnId, int roleId, [FromServices] OswsContext db, CancellationToken ct) =>
            {
                var column = await db.Columns.FindAsync([columnId], ct);
                if (column is null)
                    return Results.NotFound("Column not found.");

                var role = await db.Roles.FindAsync([roleId], ct);
                if (role is null)
                    return Results.NotFound("Role not found.");

                var exists = await db.Permissions.AnyAsync(
                    p => p.ColumnId == columnId && p.RoleId == roleId,
                    ct
                );
                if (exists)
                    return Results.Conflict("Role already has access to this column.");

                db.Permissions.Add(
                    new Permission
                    {
                        ColumnId = columnId,
                        RoleId = roleId,
                        Column = column,
                        Role = role,
                    }
                );
                await db.SaveChangesAsync(ct);
                return Results.NoContent();
            }
        );

        // DELETE /api/admin/columns/{columnId}/roles/{roleId}
        columnGroup.MapDelete(
            "/columns/{columnId:int}/roles/{roleId:int}",
            async (int columnId, int roleId, [FromServices] OswsContext db, CancellationToken ct) =>
            {
                var permission = await db.Permissions.FirstOrDefaultAsync(
                    p => p.ColumnId == columnId && p.RoleId == roleId,
                    ct
                );
                if (permission is null)
                    return Results.NotFound();

                db.Permissions.Remove(permission);
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
