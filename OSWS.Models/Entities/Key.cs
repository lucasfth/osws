namespace OSWS.Models.Entities;

public class Key
{
    public int Id { get; set; }

    public required string Name { get; set; }

    /// <summary>
    ///  the external ID of the key, a UUID. provided by the key vault
    /// </summary>
    public required string KeyVaultId { get; set; }

    public int ColumnId { get; set; }

    public required Column Column { get; set; }
}
