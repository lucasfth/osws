using Microsoft.Extensions.Logging;
using Microsoft.IO;
using OSWS.Common.Configuration;
using OSWS.Models.Interfaces;
using OSWS.ParquetSolver.Helpers;
using OSWS.ParquetSolver.Interfaces;
using ParquetSharp;
using ParquetSharp.IO;
using System.Diagnostics;

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
    /// <summary>
    /// Shared pool manager — one instance per process is the recommended pattern.
    /// Avoids LOH allocations and fragmentation by pooling fixed-size blocks.
    /// </summary>
    private static readonly RecyclableMemoryStreamManager _streamManager = new();
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

    private MemoryStream ReadParquetInternal(Stream input, ISet<string>? allowedColumns = null)
    {
        // If encryption is disabled, just pass through the input stream
        if (encryptionSettings is { DisableEncryption: true })
        {
            logger?.LogInformation(
                "[ParquetReader] Encryption disabled, reading plaintext parquet"
            );
            var plainTextStream = _streamManager.GetStream("parquet-passthrough");
            input.CopyTo(plainTextStream);
            plainTextStream.Position = 0;
            return plainTextStream;
        }

        logger?.LogInformation("[ParquetReader] Decrypting parquet file");
        logger?.LogInformation(
            "[ParquetReader] Input: type={StreamType}, length={Length}B, canSeek={CanSeek}",
            input.GetType().Name,
            input.CanSeek ? input.Length : -1,
            input.CanSeek
        );

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

        logger?.LogInformation(
            "[ParquetReader] Schema: numColumns={NumColumns}, numRowGroups={NumRowGroups}",
            numColumns, numRowGroups
        );
        // RecyclableMemoryStream uses pooled segments — no LOH allocation, no doubling.
        var outputCapacity = input.CanSeek ? (int)Math.Min(input.Length, int.MaxValue) : 0;
        var outputStream = _streamManager.GetStream("parquet-decrypt", outputCapacity);
        using var outputMos = new ManagedOutputStream(outputStream, leaveOpen: true);

        using var defaultWriterProperties = WriterProperties.GetDefaultWriterProperties();
        using var writer = new ParquetFileWriter(
            outputMos,
            schema.GroupNode,
            defaultWriterProperties,
            keyValueMetadata
        );

        var sw = Stopwatch.StartNew();

        RowGroupCopier.CopyRowGroups(
            writer,
            reader,
            numColumns,
            numRowGroups,
            ColumnDecryptionFailureBehavior,
            OnColumnDecryptionError,
            allowedColumns,
            onRowGroupCopied: logger == null ? null : (rg, ms) =>
                logger.LogDebug("[ParquetReader] Row group {Rg}/{Total} copied ({ElapsedMs}ms)", rg, numRowGroups, ms)
        );

        logger?.LogInformation("[ParquetReader] CopyRowGroups done ({ElapsedMs}ms)", sw.ElapsedMilliseconds);

        sw.Restart();
        writer.Close();
        reader.Close();

        outputStream.Position = 0;
        logger?.LogInformation("[ParquetReader] Writer flushed ({ElapsedMs}ms)", sw.ElapsedMilliseconds);
        return outputStream;
    }
}
