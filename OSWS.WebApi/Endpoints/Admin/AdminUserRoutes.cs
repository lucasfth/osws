using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using OSWS.KeyManager.Persistence;
using OSWS.Models.Entities;
using OSWS.WebApi.Services;

namespace OSWS.WebApi.Endpoints.Admin;

public static class AdminUserRoutes
{
    public static void MapUserRoutes(this RouteGroupBuilder userGroup)
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
                var users = await db
                    .Users.Select(u => new
                    {
                        u.Id,
                        u.Name,
                        u.Email,
                    })
                    .ToListAsync(ct);

                var result = new List<object>(users.Count);
                foreach (var u in users)
                {
                    var roles = await roleHierarchy.GetEffectiveRolesAsync(u.Id, ct);
                    result.Add(
                        new
                        {
                            u.Id,
                            u.Name,
                            u.Email,
                            Roles = roles.Select(r => new { r.Id, r.Name }),
                        }
                    );
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
}
