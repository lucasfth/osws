namespace OSWS.ParquetSolver.Interfaces;

public interface IParquetReader
{
    /// <summary>
    /// Read and recreate a parquet file, attempting to decrypt columns when possible.
    /// Implementations may apply configured fallback behavior for columns that cannot be decrypted,
    /// and return a recreated parquet stream (positioned at 0).
    /// </summary>
    public Task<MemoryStream> ReadParquetAsync(Stream stream, ISet<string>? allowedColumns = null);
}
