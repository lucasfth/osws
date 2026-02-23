namespace OSWS.Models.Entities;

public class User
{
    public int Id { get; set; }

    public required string Name { get; set; }

    public ICollection<Role> Roles { get; set; } = [];

    public ICollection<RoleAssignment> RoleAssignments { get; set; } = [];
}
