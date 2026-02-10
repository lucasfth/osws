using ParquetSharp;

namespace OSWS.ParquetSolver.Helpers;

public static class Copy
{
    /// <summary>
    /// Copy row groups from a ParquetFileReader to a ParquetFileWriter, using the physical type to copy raw data.
    /// </summary>
    /// <param name="parquetFileWriter"></param>
    /// <param name="parquetFileReader"></param>
    /// <param name="numColumns"></param>
    /// <param name="numRowGroups"></param>
    public static void CopyRowGroups(
        ParquetFileWriter parquetFileWriter,
        ParquetFileReader parquetFileReader,
        int numColumns,
        int numRowGroups
    )
    {
        for (var rg = 0; rg < numRowGroups; rg++)
        {
            using var rowGroupReader = parquetFileReader.RowGroup(rg);
            var numRows = checked((int)rowGroupReader.MetaData.NumRows);
            using var rowGroupWriter = parquetFileWriter.AppendRowGroup();

            for (var col = 0; col < numColumns; col++)
            {
                CopyColumn(rowGroupReader, rowGroupWriter, col, numRows);
            }
        }
    }

    /// <summary>
    /// Copy a column from a RowGroupReader to a RowGroupWriter, using the physical type to copy raw data.
    /// </summary>
    /// <param name="rowGroupReader"></param>
    /// <param name="rowGroupWriter"></param>
    /// <param name="colIndex"></param>
    /// <param name="numRows"></param>
    /// <exception cref="NotSupportedException"></exception>
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
