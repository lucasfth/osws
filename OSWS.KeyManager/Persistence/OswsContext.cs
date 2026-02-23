using Microsoft.EntityFrameworkCore;
using OSWS.Models.Entities;

namespace OSWS.KeyManager.Persistence;

public class OswsContext(DbContextOptions<OswsContext> options) : DbContext(options)
{
    public DbSet<User> Users { get; set; }
    public DbSet<Role> Roles { get; set; }
    public DbSet<RoleAssignment> RoleAssignments { get; set; }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        modelBuilder
            .Entity<User>()
            .HasMany(u => u.Roles)
            .WithMany(r => r.Users)
            .UsingEntity<RoleAssignment>();

        modelBuilder
            .Entity<Role>()
            .HasMany(r => r.Users)
            .WithMany(r => r.Roles)
            .UsingEntity<RoleAssignment>();
    }
}
