using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using OSWS.KeyManager.Persistence;
using OSWS.Models.Entities;

namespace OSWS.WebApi.Endpoints.Admin;

public static class AdminColumnRoutes
{
    public static void MapColumnRoutes(this RouteGroupBuilder columnGroup)
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
