namespace OSWS.Models.DTOs;

public class EncryptedColumnInfo
{
    public required string ColumnName { get; set; }
    public required string KeyVaultId { get; set; }
    public required string KeyName { get; set; }
}

public class EncryptionResult
{
    public required string FileKeyVaultId { get; set; }
    public required string FileKeyName { get; set; }
    public required List<EncryptedColumnInfo> Columns { get; set; }
}
