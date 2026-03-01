using OSWS.Models.Interfaces;
using OSWS.ParquetSolver.Helpers;
using OSWS.ParquetSolver.Interfaces;
using ParquetSharp;
using ParquetSharp.IO;

namespace OSWS.ParquetSolver;

public class ParquetReader(IKeyVaultProvider keyVaultProvider, DekCache dekCache) : IParquetReader
{
    private readonly IKeyVaultProvider _keyVaultProvider =
        keyVaultProvider ?? throw new ArgumentNullException(nameof(keyVaultProvider));
    private readonly DekCache _dekCache =
        dekCache ?? throw new ArgumentNullException(nameof(dekCache));

    /// <summary>
    /// Read and recreate a parquet file, decrypting columns via the configured
    /// <see cref="IKeyVaultProvider"/>. Returns a Stream containing the recreated
    /// parquet content (positioned at 0).
    /// </summary>
    /// <remarks>ParquetSharp operates synchronously via native C++ calls, so we wrap in Task.Run to avoid blocking.</remarks>
    public Task<MemoryStream> ReadParquetAsync(Stream input) =>
        Task.Run(() => ReadParquetInternal(input));

    private MemoryStream ReadParquetInternal(Stream input)
    {
        // Build decryption properties - the KeyRetriever will call IKeyVaultProvider
        // to decrypt DEKs stored in the parquet footer metadata, with caching to avoid repeated calls
        using var decryptionProperties = Cryptography.BuildDecryptionProperties(_keyVaultProvider, _dekCache);
        using var readerProperties = ReaderProperties.GetDefaultReaderProperties();
        readerProperties.FileDecryptionProperties = decryptionProperties;

        using var inputRaf = new ManagedRandomAccessFile(input, leaveOpen: true);
        using var reader = new ParquetFileReader(inputRaf, readerProperties);

        var fileMetaData = reader.FileMetaData;
        var numColumns = fileMetaData.NumColumns;
        var numRowGroups = fileMetaData.NumRowGroups;
        var schema = fileMetaData.Schema;
        var keyValueMetadata = fileMetaData.KeyValueMetadata;

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
