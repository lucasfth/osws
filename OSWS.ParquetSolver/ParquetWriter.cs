using OSWS.Models.Interfaces;
using OSWS.ParquetSolver.Helpers;
using OSWS.ParquetSolver.Interfaces;
using ParquetSharp;
using ParquetSharp.IO;

namespace OSWS.ParquetSolver;

public class ParquetWriter(IKeyVaultProvider keyVaultProvider, string providerType = "Azure")
    : IParquetWriter
{
    private readonly IKeyVaultProvider _keyVaultProvider =
        keyVaultProvider ?? throw new ArgumentNullException(nameof(keyVaultProvider));

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
    public Task<Stream> WriteParquetAsync(
        Stream input,
        string role,
        string[]? columnsToEncrypt = null
    )
    {
        return Task.Run(() => WriteParquetInternal(input, role, columnsToEncrypt));
    }

    private Stream WriteParquetInternal(Stream input, string role, string[]? columnsToEncrypt)
    {
        using var inputRaf = new ManagedRandomAccessFile(input, leaveOpen: true);
        using var reader = new ParquetFileReader(inputRaf);

        var fileMetaData = reader.FileMetaData;
        var numColumns = fileMetaData.NumColumns;
        var numRowGroups = fileMetaData.NumRowGroups;
        var schema = fileMetaData.Schema;
        var keyValueMetadata = fileMetaData.KeyValueMetadata;

        // Build encryption properties via the key vault provider
        var encryptionProperties = Cryptography.BuildEncryptionProperties(
            schema,
            columnsToEncrypt,
            _keyVaultProvider,
            role,
            providerType
        );

        using var writerPropertiesBuilder = new WriterPropertiesBuilder();
        writerPropertiesBuilder.Encryption(encryptionProperties);
        using var writerProperties = writerPropertiesBuilder.Build();

        var outputStream = new MemoryStream();
        using var outputMos = new ManagedOutputStream(outputStream, leaveOpen: true);
        using var writer = new ParquetFileWriter(
            outputMos,
            schema.GroupNode,
            writerProperties,
            keyValueMetadata
        );

        Copy.CopyRowGroups(writer, reader, numColumns, numRowGroups);

        writer.Close();
        reader.Close();

        outputStream.Position = 0;
        return outputStream;
    }
}
