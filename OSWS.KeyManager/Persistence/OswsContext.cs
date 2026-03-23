using Microsoft.EntityFrameworkCore;
using OSWS.Models.Entities;

namespace OSWS.KeyManager.Persistence;

public class OswsContext(DbContextOptions<OswsContext> options) : DbContext(options)
{
    public DbSet<User> Users { get; set; }
    public DbSet<Role> Roles { get; set; }
    public DbSet<RoleAssignment> RoleAssignments { get; set; }
    public DbSet<ExternalIdentity> ExternalIdentities { get; set; }
    public DbSet<S3Credential> S3Credentials { get; set; }

    public DbSet<Column> Columns { get; set; }
    public DbSet<Permission> Permissions { get; set; }
    public DbSet<Key> Keys { get; set; }

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

        modelBuilder.Entity<ExternalIdentity>(entity =>
        {
            // Composite unique index: one record per (provider, subject) pair
            entity.HasIndex(e => new { e.Provider, e.Subject }).IsUnique();

            entity
                .HasOne(e => e.User)
                .WithMany(u => u.ExternalIdentities)
                .HasForeignKey(e => e.UserId)
                .OnDelete(DeleteBehavior.Cascade);
        });

        modelBuilder.Entity<S3Credential>(entity =>
        {
            entity.HasIndex(e => e.AccessKeyId).IsUnique();

            entity
                .HasOne(e => e.User)
                .WithMany(u => u.S3Credentials)
                .HasForeignKey(e => e.UserId)
                .OnDelete(DeleteBehavior.Cascade);

            entity
                .HasOne(e => e.DefaultRole)
                .WithMany(r => r.S3Credentials)
                .HasForeignKey(e => e.DefaultRoleId);
        });

        modelBuilder
            .Entity<Column>()
            .HasMany(col => col.Roles)
            .WithMany(r => r.Columns)
            .UsingEntity<Permission>();

        modelBuilder.Entity<Key>(entity =>
        {
            // for testing, ignore this for now
            // entity.HasIndex(e => e.KeyVaultId).IsUnique();
            entity.HasOne(e => e.Column).WithMany(c => c.Keys).HasForeignKey(e => e.ColumnId);
        });
    }
}
