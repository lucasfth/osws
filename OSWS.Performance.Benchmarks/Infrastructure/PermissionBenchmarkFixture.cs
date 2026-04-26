using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using OSWS.KeyManager.Persistence;
using OSWS.Models.Entities;
using OSWS.WebApi.Services;

namespace OSWS.Performance.Benchmarks.Infrastructure;

/// <summary>
/// Provides a Postgres database seeded with benchmark data for
/// <see cref="PermissionService"/> and <see cref="RoleHierarchyService"/> benchmarks.
///
/// Assumes a dedicated benchmark database — all existing rows are cleared on
/// construction and on <see cref="Dispose"/>.
///
/// Each call to <see cref="CreatePermissionService"/> returns a fresh, untracked
/// <see cref="OswsContext"/> — matching the per-request scoped lifetime in production.
/// </summary>
public sealed class PermissionBenchmarkFixture : IDisposable
{
    private readonly DbContextOptions<OswsContext> _dbOptions;
    private bool _disposed;

    public int UserId { get; private set; }

    public PermissionBenchmarkFixture(
        int directRoles,
        int hierarchyDepth = 0,
        int columnCount = 100,
        double permissionDensity = 0.80,
        string? connectionString = null
    )
    {
        var cs =
            connectionString
            ?? new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", optional: true)
                .AddJsonFile("appsettings.Development.json", optional: true)
                .AddEnvironmentVariables()
                .Build()
                .GetConnectionString("OswsContext")
            ?? throw new InvalidOperationException("ConnectionStrings:OswsContext is not set.");

        _dbOptions = new DbContextOptionsBuilder<OswsContext>().UseNpgsql(cs).Options;

        using var ctx = new OswsContext(_dbOptions);
        ClearAll(ctx);
        UserId = Seed(ctx, directRoles, hierarchyDepth, columnCount, permissionDensity);
    }

    public (PermissionService service, OswsContext context) CreatePermissionService()
    {
        var ctx = new OswsContext(_dbOptions);
        return (new PermissionService(new RoleHierarchyService(ctx), ctx), ctx);
    }

    private static void ClearAll(OswsContext db)
    {
        db.Database.ExecuteSqlRaw("DELETE FROM \"Permissions\"");
        db.Database.ExecuteSqlRaw("DELETE FROM \"RoleInheritances\"");
        db.Database.ExecuteSqlRaw("DELETE FROM \"RoleAssignments\"");
        db.Database.ExecuteSqlRaw("DELETE FROM \"Roles\"");
        db.Database.ExecuteSqlRaw("DELETE FROM \"Columns\"");
        db.Database.ExecuteSqlRaw("DELETE FROM \"Users\"");
    }

    private static int Seed(
        OswsContext db,
        int directRoles,
        int hierarchyDepth,
        int columnCount,
        double permissionDensity
    )
    {
        var user = new User { Name = "bench-user" };
        db.Users.Add(user);

        var totalRoles = directRoles + hierarchyDepth;
        var roles = Enumerable
            .Range(1, totalRoles)
            .Select(i => new Role { Name = $"bench_role_{i}" })
            .ToList();
        db.Roles.AddRange(roles);

        var columns = Enumerable
            .Range(1, columnCount)
            .Select(i => new Column { Name = $"bench_col_{i}" })
            .ToList();
        db.Columns.AddRange(columns);

        db.SaveChanges();

        db.RoleAssignments.AddRange(
            roles
                .Take(directRoles)
                .Select(r => new RoleAssignment
                {
                    UserId = user.Id,
                    User = user,
                    RoleId = r.Id,
                    Role = r,
                })
        );

        if (hierarchyDepth > 0)
        {
            for (int i = 0; i < hierarchyDepth; i++)
            {
                var parent = i == 0 ? roles[0] : roles[directRoles + i - 1];
                var child = roles[directRoles + i];
                db.RoleInheritances.Add(
                    new RoleInheritance
                    {
                        ParentRoleId = parent.Id,
                        ParentRole = parent,
                        ChildRoleId = child.Id,
                        ChildRole = child,
                    }
                );
            }
        }

        var rng = new Random(42);
        var permissions = new List<Permission>();
        foreach (var role in roles)
        foreach (var col in columns)
            if (rng.NextDouble() < permissionDensity)
                permissions.Add(
                    new Permission
                    {
                        RoleId = role.Id,
                        Role = role,
                        ColumnId = col.Id,
                        Column = col,
                    }
                );

        db.Permissions.AddRange(permissions);
        db.SaveChanges();

        return user.Id;
    }

    public void Dispose()
    {
        if (_disposed)
            return;
        _disposed = true;
        using var ctx = new OswsContext(_dbOptions);
        ClearAll(ctx);
    }
}
