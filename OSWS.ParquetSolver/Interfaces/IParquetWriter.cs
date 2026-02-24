namespace OSWS.ParquetSolver.Interfaces;

public interface IParquetWriter
{
    /// <summary>
    /// Read an unencrypted parquet file and write an encrypted version.
    /// Keys are created in the vault and associated with the given role.
    /// The encrypted DEK and vault key reference are stored in parquet footer metadata for later decryption.
    /// </summary>
    /// <param name="input">Stream containing plaintext parquet data.</param>
    /// <param name="role">The role to associate encryption keys with.</param>
    /// <param name="columnsToEncrypt">Column names to encrypt, or null for all columns.</param>
    Task<Stream> WriteParquetAsync(Stream input, string role, string[]? columnsToEncrypt = null);
}
