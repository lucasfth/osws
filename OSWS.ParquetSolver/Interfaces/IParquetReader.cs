namespace OSWS.ParquetSolver.Interfaces;

public interface IParquetReader
{
    /// <summary>
    /// Read and recreate a parquet file, attempting to decrypt columns when possible. Returns a Stream
    /// containing the recreated parquet content (positioned at 0).
    /// </summary>
    public Task<MemoryStream> ReadParquetAsync(Stream stream);
}
