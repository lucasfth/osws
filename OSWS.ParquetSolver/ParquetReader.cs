using OSWS.ParquetSolver.Helpers;
using OSWS.ParquetSolver.Interfaces;
using ParquetSharp;
using ParquetSharp.IO;

namespace OSWS.ParquetSolver;

public class ParquetReader : IParquetReader
{
    /// <summary>
    /// Read and recreate a parquet file, attempting to decrypt columns when possible. Returns a Stream
    /// containing the recreated parquet content (positioned at 0).
    /// </summary>
    /// <param name="input"></param>
    /// <returns>A Stream containing the recreated parquet content (positioned at 0)</returns>
    /// <remarks>ParquetSharp operates synchronously via native C++ calls, so we wrap in Task.Run to avoid blocking.</remarks>
    public Task<MemoryStream> ReadParquetAsync(Stream input) =>
        Task.Run(() => ReadParquetInternal(input));

    /// <summary>
    /// Internal method to read and recreate a parquet file, attempting to decrypt columns when possible. Returns a MemoryStream
    /// containing the recreated parquet content (positioned at 0).
    /// </summary>
    /// <param name="input"></param>
    /// <returns></returns>
    private static MemoryStream ReadParquetInternal(Stream input)
    {
        // Build decryption properties using the same keys used for encryption
        using var decryptionProperties = Cryptography.BuildDecryptionProperties();
        using var readerProperties = ReaderProperties.GetDefaultReaderProperties();
        readerProperties.FileDecryptionProperties = decryptionProperties;

        // Read the encrypted parquet file from stream
        using var inputRaf = new ManagedRandomAccessFile(input, leaveOpen: true);
        using var reader = new ParquetFileReader(inputRaf, readerProperties);

        var fileMetaData = reader.FileMetaData;
        var numColumns = fileMetaData.NumColumns;
        var numRowGroups = fileMetaData.NumRowGroups;
        var schema = fileMetaData.Schema;
        var keyValueMetadata = fileMetaData.KeyValueMetadata;

        // Write decrypted parquet file to output stream (no encryption)
        var outputStream = new MemoryStream();
        using var outputMos = new ManagedOutputStream(outputStream, leaveOpen: true);

        using var defaultWriterProperties = WriterProperties.GetDefaultWriterProperties();
        using var writer = new ParquetFileWriter(
            outputMos,
            schema.GroupNode,
            defaultWriterProperties,
            keyValueMetadata
        );

        Copy.CopyRowGroups(writer, reader, numColumns, numRowGroups);

        writer.Close();
        reader.Close();

        outputStream.Position = 0;
        return outputStream;
    }
}
