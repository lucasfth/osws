using OSWS.ParquetSolver.Helpers;
using OSWS.ParquetSolver.Interfaces;
using ParquetSharp;
using ParquetSharp.IO;

namespace OSWS.ParquetSolver;

public class ParquetWriter : IParquetWriter
{
    /// <summary>
    /// Read an unencrypted parquet file and write an encrypted version. Encrypts specified columns (or all columns if null).
    /// </summary>
    /// <param name="input"></param>
    /// <param name="columnsToEncrypt"></param>
    /// <returns>A Stream containing the encrypted parquet content (positioned at 0)</returns>
    /// <remarks>ParquetSharp operates synchronously via native C++ calls, so we wrap in Task.Run to avoid blocking.</remarks>
    public Task<Stream> WriteParquetAsync(Stream input, string[]? columnsToEncrypt = null)
    {
        return Task.Run(() => WriteParquetInternal(input, columnsToEncrypt));
    }

    /// <summary>
    /// Internal method to read an unencrypted parquet file and write an encrypted version. Encrypts specified columns (or all columns if null).
    /// </summary>
    /// <param name="input"></param>
    /// <param name="columnsToEncrypt"></param>
    /// <returns></returns>
    private static Stream WriteParquetInternal(Stream input, string[]? columnsToEncrypt)
    {
        // Read original parquet file from stream
        using var inputRaf = new ManagedRandomAccessFile(input, leaveOpen: true);
        using var reader = new ParquetFileReader(inputRaf);

        var fileMetaData = reader.FileMetaData;
        var numColumns = fileMetaData.NumColumns;
        var numRowGroups = fileMetaData.NumRowGroups;
        var schema = fileMetaData.Schema;
        var keyValueMetadata = fileMetaData.KeyValueMetadata;

        // Build encryption properties with native Parquet Modular Encryption
        var encryptionProperties = Cryptography.BuildEncryptionProperties(schema, columnsToEncrypt);

        // Build writer properties with encryption
        using var writerPropertiesBuilder = new WriterPropertiesBuilder();
        writerPropertiesBuilder.Encryption(encryptionProperties);

        using var writerProperties = writerPropertiesBuilder.Build();

        // Write to output stream
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

    private static FileEncryptionProperties BuildEncryptionProperties(
        SchemaDescriptor schema,
        string[]? columnsToEncrypt
    )
    {
        using var builder = new FileEncryptionPropertiesBuilder(DummyCryptoParameters.FooterKey);
        builder.FooterKeyMetadata(DummyCryptoParameters.FooterKeyMetadata);
        builder.SetPlaintextFooter(); // Keep footer readable; only encrypt column data

        // Build column encryption for specified columns (or all if null)
        var numColumns = schema.NumColumns;
        var columnProperties = new ColumnEncryptionProperties[numColumns];

        for (var i = 0; i < numColumns; i++)
        {
            var colName = schema.Column(i).Name;
            var shouldEncrypt =
                columnsToEncrypt == null
                || Array.Exists(
                    columnsToEncrypt,
                    c => string.Equals(c, colName, StringComparison.OrdinalIgnoreCase)
                );

            if (!shouldEncrypt)
                continue;

            using var colBuilder = new ColumnEncryptionPropertiesBuilder(colName);
            colBuilder.Key(DummyCryptoParameters.ColumnKey);
            colBuilder.KeyMetadata(DummyCryptoParameters.ColumnKeyMetadata);
            columnProperties[i] = colBuilder.Build();
        }

        // Filter out nulls (unencrypted columns)
        var encryptedCols = Array.FindAll(columnProperties, p => true);
        if (encryptedCols.Length > 0)
        {
            builder.EncryptedColumns(encryptedCols!);
        }

        return builder.Build();
    }
}
