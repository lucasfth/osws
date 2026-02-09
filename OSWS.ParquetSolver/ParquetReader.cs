using System;
using System.IO;
using System.Threading.Tasks;
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

    private static MemoryStream ReadParquetInternal(Stream input)
    {
        // Build decryption properties using the same keys used for encryption
        using var decryptionProperties = BuildDecryptionProperties();
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

        // Copy all row groups (data is automatically decrypted by the reader)
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
        return outputStream;
    }

    private static FileDecryptionProperties BuildDecryptionProperties()
    {
        using var builder = new FileDecryptionPropertiesBuilder();
        builder.KeyRetriever(new DummyKeyRetriever());
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
                CopyTypedColumn<Int96>(colReader, colWriter, numRows);
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

/// <summary>
/// Key retriever that returns decryption keys based on key metadata stored in the parquet footer.
/// </summary>
internal sealed class DummyKeyRetriever : DecryptionKeyRetriever
{
    public override byte[] GetKey(string keyMetadata)
    {
        return keyMetadata switch
        {
            DummyCryptoParameters.FooterKeyMetadata => DummyCryptoParameters.FooterKey,
            DummyCryptoParameters.ColumnKeyMetadata => DummyCryptoParameters.ColumnKey,
            _ => throw new InvalidOperationException($"Unknown key metadata: {keyMetadata}"),
        };
    }
}
