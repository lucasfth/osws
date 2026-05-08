namespace OSWS.ParquetSolver.Interfaces;

public interface IParquetReader
{
    /// <summary>
    /// Read and recreate a parquet file, attempting to decrypt columns when possible.
    /// Implementations may apply configured fallback behavior for columns that cannot be decrypted,
    /// and return a recreated parquet stream (positioned at 0).
    /// </summary>
    public Task<MemoryStream> ReadParquetAsync(Stream stream, ISet<string>? allowedColumns = null);

    /// <summary>
    /// Apply column masking to a plaintext (unencrypted) parquet stream without any key vault interaction.
    /// Forbidden columns are masked according to the configured failure behavior.
    /// Returns a new plaintext parquet stream positioned at 0.
    /// </summary>
    public Task<MemoryStream> MaskPlaintextAsync(Stream input, ISet<string>? allowedColumns = null);
}
