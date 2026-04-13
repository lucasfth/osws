using Microsoft.Extensions.Logging;
using OSWS.Common.Configuration;
using OSWS.Models.DTOs;
using OSWS.Models.Interfaces;
using OSWS.ParquetSolver.Helpers;
using OSWS.ParquetSolver.Interfaces;
using ParquetSharp;
using ParquetSharp.IO;

namespace OSWS.ParquetSolver;

public class ParquetWriter(
    IKeyVaultProvider keyVaultProvider,
    string providerType = "Azure",
    ILogger? logger = null,
    EncryptionSettings? encryptionSettings = null
) : IParquetWriter
{
    /// <summary>
    /// Read an unencrypted parquet file and write an encrypted version.
    /// Encrypts specified columns (or all columns if null) using the configured
    /// <see cref="IKeyVaultProvider"/> which holds the encryption key.
    /// </summary>
    /// <param name="input">Stream containing plaintext parquet data.</param>
    /// <param name="role">The role to associate encryption keys with.</param>
    /// <param name="columnsToEncrypt">Column names to encrypt, or null for all columns.</param>
    /// <returns>A Stream containing the encrypted parquet content (positioned at 0).</returns>
    /// <remarks>ParquetSharp operates synchronously via native C++ calls, so we wrap in Task.Run to avoid blocking.</remarks>
    public Task<(Stream Stream, EncryptionResult Metadata)> WriteParquetAsync(
        Stream input,
        string role,
        string[]? columnsToEncrypt = null,
        Stream? output = null
    )
    {
        return Task.Run(() => WriteParquetInternal(input, role, columnsToEncrypt, output));
    }

    private (Stream Stream, EncryptionResult Metadata) WriteParquetInternal(
        Stream input,
        string role,
        string[]? columnsToEncrypt,
        Stream? output
    )
    {
        // If encryption is disabled, just pass through the input stream
        if (encryptionSettings is { DisableEncryption: true })
        {
            logger?.LogInformation(
                "[ParquetWriter] Encryption disabled, writing plaintext parquet"
            );
            var plainTextStream = new MemoryStream();
            input.CopyTo(plainTextStream);
            plainTextStream.Position = 0;
            return (
                plainTextStream,
                new EncryptionResult
                {
                    FileKeyVaultId = string.Empty,
                    FileKeyName = string.Empty,
                    Columns = new List<EncryptedColumnInfo>(),
                }
            );
        }

        logger?.LogInformation(
            "[ParquetWriter] Encrypting parquet with role: {Role}, columns: {ColumnCount}",
            role,
            columnsToEncrypt?.Length.ToString() ?? "all"
        );

        using var inputRaf = new ManagedRandomAccessFile(input, leaveOpen: true);
        using var reader = new ParquetFileReader(inputRaf);

        var fileMetaData = reader.FileMetaData;
        var numColumns = fileMetaData.NumColumns;
        var numRowGroups = fileMetaData.NumRowGroups;
        var schema = fileMetaData.Schema;
        var keyValueMetadata = fileMetaData.KeyValueMetadata;

        // Build encryption properties via the key vault provider
        var (encryptionProperties, encryptionMetadata) = Cryptography.BuildEncryptionProperties(
            schema,
            columnsToEncrypt,
            keyVaultProvider,
            role,
            providerType,
            encryptionSettings
        );

        logger?.LogInformation(
            "Encrypting columns with role: {Role}, columnsToEncrypt: {ColumnCount}",
            role,
            columnsToEncrypt?.Length.ToString() ?? "all"
        );

        using var writerPropertiesBuilder = new WriterPropertiesBuilder();
        writerPropertiesBuilder.Encryption(encryptionProperties);
        using var writerProperties = writerPropertiesBuilder.Build();

        var outputStream = output ?? new MemoryStream();
        using var outputMos = new ManagedOutputStream(outputStream, leaveOpen: true);
        using var writer = new ParquetFileWriter(
            outputMos,
            schema.GroupNode,
            writerProperties,
            keyValueMetadata
        );

        RowGroupCopier.CopyRowGroups(writer, reader, numColumns, numRowGroups);

        writer.Close();
        reader.Close();

        if (outputStream.CanSeek)
        {
            outputStream.Position = 0;
        }
        return (outputStream, encryptionMetadata);
    }
}
