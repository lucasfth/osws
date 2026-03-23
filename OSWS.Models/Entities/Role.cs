namespace OSWS.Models.Entities;

public class Role
{
    public int Id { get; set; }

    public required string Name { get; set; }

    public ICollection<User> Users { get; set; } = [];

    // navigational property for User many-to-many
    public ICollection<RoleAssignment> RoleAssignments { get; set; } = [];

    // navigational property for Column many-to-many
    public ICollection<Permission> Permissions { get; set; } = [];

    public ICollection<Column> Columns { get; set; } = [];

    /// <summary>
    /// S3-Credentials that default to assuming this role (maybe we don't need this)
    /// </summary>
    public ICollection<S3Credential> S3Credentials { get; set; } = [];
}
