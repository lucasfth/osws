namespace OSWS.ParquetSolver;

/// <summary>
/// Dummy AES-128 encryption parameters for development/testing.
/// In production, use a proper Key Management Service.
/// </summary>
internal static class DummyCryptoParameters
{
    /// <summary>
    /// 128-bit AES key used for footer encryption.
    /// </summary>
    public static readonly byte[] FooterKey = "0123456789abcdef"u8.ToArray(); // 16 bytes = AES-128

    /// <summary>
    /// 128-bit AES key used for column encryption.
    /// </summary>
    public static readonly byte[] ColumnKey = "abcdef0123456789"u8.ToArray(); // 16 bytes = AES-128

    /// <summary>
    /// Metadata identifier for the footer key.
    /// </summary>
    public const string FooterKeyMetadata = "footer_key";

    /// <summary>
    /// Metadata identifier for the column key.
    /// </summary>
    public const string ColumnKeyMetadata = "column_key";
}
