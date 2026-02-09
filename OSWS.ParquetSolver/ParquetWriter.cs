using System;
using System.IO;
using System.Threading.Tasks;
using OSWS.ParquetSolver.Interfaces;
using ParquetSharp;
using ParquetSharp.IO;

namespace OSWS.ParquetSolver;

public class ParquetWriter : IParquetWriter
{
    public Task<Stream> WriteParquetAsync(Stream input, string[]? columnsToEncrypt = null)
    {
        // ParquetSharp operates synchronously via native C++ calls, so we wrap in Task.Run
        return Task.Run(() => WriteParquetInternal(input, columnsToEncrypt));
    }

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
        var encryptionProperties = BuildEncryptionProperties(schema, columnsToEncrypt);

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

        // Copy all row groups
        for (var rg = 0; rg < numRowGroups; rg++)
        {
            using var rowGroupReader = reader.RowGroup(rg);
            var numRows = checked((int)rowGroupReader.MetaData.NumRows);
            using var rowGroupWriter = writer.AppendRowGroup();

            for (var col = 0; col < numColumns; col++)
            {
                CopyColumn(rowGroupReader, rowGroupWriter, col, numRows);
            }
        }

        writer.Close();
        reader.Close();

        outputStream.Position = 0;
        return (Stream)outputStream;
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

            if (shouldEncrypt)
            {
                using var colBuilder = new ColumnEncryptionPropertiesBuilder(colName);
                colBuilder.Key(DummyCryptoParameters.ColumnKey);
                colBuilder.KeyMetadata(DummyCryptoParameters.ColumnKeyMetadata);
                columnProperties[i] = colBuilder.Build();
            }
        }

        // Filter out nulls (unencrypted columns)
        var encryptedCols = Array.FindAll(columnProperties, p => p != null);
        if (encryptedCols.Length > 0)
        {
            builder.EncryptedColumns(encryptedCols!);
        }

        return builder.Build();
    }

    private static void CopyColumn(
        RowGroupReader rowGroupReader,
        RowGroupWriter rowGroupWriter,
        int colIndex,
        int numRows
    )
    {
        using var colReader = rowGroupReader.Column(colIndex);
        using var colWriter = rowGroupWriter.NextColumn();

        // Use physical type to copy raw data
        switch (colReader.Type)
        {
            case PhysicalType.Boolean:
                CopyTypedColumn<bool>(colReader, colWriter, numRows);
                break;
            case PhysicalType.Int32:
                CopyTypedColumn<int>(colReader, colWriter, numRows);
                break;
            case PhysicalType.Int64:
                CopyTypedColumn<long>(colReader, colWriter, numRows);
                break;
            case PhysicalType.Int96:
                CopyTypedColumn<ParquetSharp.Int96>(colReader, colWriter, numRows);
                break;
            case PhysicalType.Float:
                CopyTypedColumn<float>(colReader, colWriter, numRows);
                break;
            case PhysicalType.Double:
                CopyTypedColumn<double>(colReader, colWriter, numRows);
                break;
            case PhysicalType.ByteArray:
                CopyByteArrayColumn(colReader, colWriter, numRows);
                break;
            case PhysicalType.FixedLenByteArray:
                CopyFixedLenByteArrayColumn(colReader, colWriter, numRows);
                break;
            default:
                throw new NotSupportedException($"Unsupported physical type: {colReader.Type}");
        }
    }

    private static void CopyTypedColumn<T>(
        ColumnReader colReader,
        ColumnWriter colWriter,
        int numRows
    )
        where T : unmanaged
    {
        var values = new T[numRows];
        var defLevels = new short[numRows];
        var repLevels = new short[numRows];

        var typedReader =
            colReader as ColumnReader<T>
            ?? throw new InvalidOperationException($"Expected ColumnReader<{typeof(T).Name}>");
        var typedWriter =
            colWriter as ColumnWriter<T>
            ?? throw new InvalidOperationException($"Expected ColumnWriter<{typeof(T).Name}>");

        long totalRead = 0;
        while (totalRead < numRows)
        {
            var read = typedReader.ReadBatch(
                numRows,
                defLevels.AsSpan(),
                repLevels.AsSpan(),
                values.AsSpan(),
                out var valuesRead
            );
            if (read == 0)
                break;
            typedWriter.WriteBatch(
                (int)read,
                defLevels.AsSpan(0, (int)read),
                repLevels.AsSpan(0, (int)read),
                values.AsSpan(0, (int)valuesRead)
            );
            totalRead += read;
        }
    }

    private static void CopyByteArrayColumn(
        ColumnReader colReader,
        ColumnWriter colWriter,
        int numRows
    )
    {
        var values = new ByteArray[numRows];
        var defLevels = new short[numRows];
        var repLevels = new short[numRows];

        var typedReader =
            colReader as ColumnReader<ByteArray>
            ?? throw new InvalidOperationException("Expected ColumnReader<ByteArray>");
        var typedWriter =
            colWriter as ColumnWriter<ByteArray>
            ?? throw new InvalidOperationException("Expected ColumnWriter<ByteArray>");

        long totalRead = 0;
        while (totalRead < numRows)
        {
            var read = typedReader.ReadBatch(
                numRows,
                defLevels.AsSpan(),
                repLevels.AsSpan(),
                values.AsSpan(),
                out var valuesRead
            );
            if (read == 0)
                break;
            typedWriter.WriteBatch(
                (int)read,
                defLevels.AsSpan(0, (int)read),
                repLevels.AsSpan(0, (int)read),
                values.AsSpan(0, (int)valuesRead)
            );
            totalRead += read;
        }
    }

    private static void CopyFixedLenByteArrayColumn(
        ColumnReader colReader,
        ColumnWriter colWriter,
        int numRows
    )
    {
        var values = new FixedLenByteArray[numRows];
        var defLevels = new short[numRows];
        var repLevels = new short[numRows];

        var typedReader =
            colReader as ColumnReader<FixedLenByteArray>
            ?? throw new InvalidOperationException("Expected ColumnReader<FixedLenByteArray>");
        var typedWriter =
            colWriter as ColumnWriter<FixedLenByteArray>
            ?? throw new InvalidOperationException("Expected ColumnWriter<FixedLenByteArray>");

        long totalRead = 0;
        while (totalRead < numRows)
        {
            var read = typedReader.ReadBatch(
                numRows,
                defLevels.AsSpan(),
                repLevels.AsSpan(),
                values.AsSpan(),
                out var valuesRead
            );
            if (read == 0)
                break;
            typedWriter.WriteBatch(
                (int)read,
                defLevels.AsSpan(0, (int)read),
                repLevels.AsSpan(0, (int)read),
                values.AsSpan(0, (int)valuesRead)
            );
            totalRead += read;
        }
    }
}
