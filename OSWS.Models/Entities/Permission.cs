namespace OSWS.Models.Entities;

/// <summary>
/// Join table between a role and a column; i.e. it gives a role permission to interact with a column.
/// In the future, this should likely also have an Operation - so read/write, decrypt/encrypt or such.
/// </summary>
public class Permission
{
    public int Id { get; set; }

    public int RoleId { get; set; }
    public required Role Role { get; set; }
    public int ColumnId { get; set; }
    public required Column Column { get; set; }
}
