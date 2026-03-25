using OSWS.Models.DTOs;

namespace OSWS.ParquetSolver.Interfaces;

public interface IParquetWriter
{
    /// <summary>
    /// Read an unencrypted parquet file and write an encrypted version.
    /// Keys are created in the vault and associated with the given role.
    /// Each encrypted column gets its own DEK, and all column DEKs for a file are wrapped by a single KEK.
    /// The parquet footer remains plaintext.
    /// </summary>
    /// <param name="input">Stream containing plaintext parquet data.</param>
    /// <param name="role">The role to associate encryption keys with.</param>
    /// <param name="columnsToEncrypt">Column names to encrypt, or null for all columns.</param>
    /// <param name="output">Optional destination stream for encrypted parquet output.</param>
    Task<(Stream Stream, EncryptionResult Metadata)> WriteParquetAsync(
        Stream input,
        string role,
        string[]? columnsToEncrypt = null,
        Stream? output = null
    );
}
