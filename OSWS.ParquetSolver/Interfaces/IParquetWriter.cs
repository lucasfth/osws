namespace OSWS.ParquetSolver.Interfaces;

public interface IParquetWriter
{
    /// <summary>
    /// Read an unencrypted parquet file and write an encrypted version.
    /// Encrypts specified columns (or all columns if null) with dummy AES key and adds key references.
    /// Returns a Stream containing the encrypted parquet content (positioned at 0).
    /// </summary>
    Task<Stream> WriteParquetAsync(Stream input, string[]? columnsToEncrypt = null);
}
