namespace OSWS.Models.Entities;

public class Role
{
    public int Id { get; set; }

    public required string Name { get; set; }

    public ICollection<User> Users { get; set; } = [];

    // navigational property for many-to-many

    public ICollection<RoleAssignment> RoleAssignments { get; set; } = [];
}
