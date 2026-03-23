namespace OSWS.Models.Entities;

public class Column
{
    public int Id { get; set; }

    public required string Name { get; set; }

    public ICollection<Role> Roles { get; set; } = [];

    public ICollection<Permission> Permissions { get; set; } = [];

    public ICollection<Key> Keys { get; set; } = [];
}
