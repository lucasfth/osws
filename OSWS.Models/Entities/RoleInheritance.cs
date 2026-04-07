namespace OSWS.Models.Entities;

public class RoleInheritance
{
    public int ParentRoleId { get; set; } // the role that gains permissions
    public required Role ParentRole { get; set; }
    public int ChildRoleId { get; set; } // the role being inherited
    public required Role ChildRole { get; set; }
}
