using Microsoft.Extensions.Logging;
using OSWS.Common.Configuration;
using OSWS.Models.Interfaces;
using OSWS.ParquetSolver.Helpers;
using OSWS.ParquetSolver.Interfaces;
using ParquetSharp;
using ParquetSharp.IO;

namespace OSWS.ParquetSolver;

public class ParquetReader(
    IKeyVaultProvider keyVaultProvider,
    IDekCache dekCache,
    ILogger? logger = null,
    EncryptionSettings? encryptionSettings = null,
    Action<TimeSpan>? onExternalKvOperationLatency = null,
    Action<TimeSpan>? onCachedKvOperationLatency = null
) : IParquetReader
{
    private readonly EncryptionSettings _encryptionSettings =
        encryptionSettings ?? new EncryptionSettings();
    private readonly TimingLogger _timingLogger = new(
        logger,
        encryptionSettings?.EnableOperationLogging ?? false
    );

    /// <summary>
    /// Controls behavior when a column cannot be decrypted.
    /// Defaults to writing dummy values for that column so the remaining columns can still be read.
    /// </summary>
    public ColumnDecryptionFailureBehavior ColumnDecryptionFailureBehavior { get; init; } =
        ColumnDecryptionFailureBehavior.DummyValues;

    /// <summary>
    /// Optional callback invoked when a column decryption/read operation fails and fallback behaviour is applied.
    /// </summary>
    public Action<string, Exception>? OnColumnDecryptionError { get; set; }

    /// <summary>
    /// Read and recreate a parquet file, decrypting columns via the configured
    /// <see cref="IKeyVaultProvider"/>. Returns a Stream containing the recreated
    /// parquet content (positioned at 0).
    /// </summary>
    /// <remarks>
    /// Columns are processed one at a time. If decrypting a specific column fails,
    /// behaviour is controlled by <see cref="ColumnDecryptionFailureBehavior"/>.
    /// </remarks>
    public Task<MemoryStream> ReadParquetAsync(Stream input, ISet<string>? allowedColumns = null) =>
        Task.Run(() => ReadParquetInternal(input, allowedColumns));

    private MemoryStream ReadParquetInternal(Stream input, ISet<string>? allowedColumns)
    {
        // If encryption is disabled, just pass through the input stream
        if (_encryptionSettings.DisableEncryption)
        {
            logger?.LogInformation(
                "[ParquetReader] Encryption disabled, reading plaintext parquet"
            );
            var plainTextStream = new MemoryStream();
            input.CopyTo(plainTextStream);
            plainTextStream.Position = 0;
            return plainTextStream;
        }

        logger?.LogInformation("[ParquetReader] Decrypting parquet file");

        // Build decryption properties - the KeyRetriever will call IKeyVaultProvider
        // to decrypt DEKs stored in parquet key metadata, with caching to avoid repeated calls
        using var decryptionProperties = Cryptography.BuildDecryptionProperties(
            keyVaultProvider,
            dekCache,
            onExternalKvOperationLatency,
            onCachedKvOperationLatency
        );
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

        Copy.CopyRowGroups(
            writer,
            reader,
            numColumns,
            numRowGroups,
            ColumnDecryptionFailureBehavior,
            OnColumnDecryptionError,
            allowedColumns
        );

        writer.Close();
        reader.Close();

        outputStream.Position = 0;
        return outputStream;
    }
}
