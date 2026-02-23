namespace OSWS.Models.Entities;

public class RoleAssignment
{
    public int UserId { get; set; }
    public required User User { get; set; }
    public int RoleId { get; set; }
    public required Role Role { get; set; }
}
