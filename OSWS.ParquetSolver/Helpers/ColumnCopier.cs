using ParquetSharp;

namespace OSWS.ParquetSolver.Helpers;

/// <summary>
/// Handles copying individual Parquet columns between readers and writers,
/// dispatching on physical type. Used by RowGroupCopier.
/// </summary>
internal static class ColumnCopier
{
    internal static void CopyColumn(ColumnReader colReader, ColumnWriter colWriter, int numRows)
    {
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
            case PhysicalType.Undefined:
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

        var totalRowsRead = 0;
        var totalValuesRead = 0;
        while (totalRowsRead < numRows)
        {
            var rowsToRead = numRows - totalRowsRead;
            var read = typedReader.ReadBatch(
                rowsToRead,
                defLevels.AsSpan(totalRowsRead, rowsToRead),
                repLevels.AsSpan(totalRowsRead, rowsToRead),
                values.AsSpan(totalValuesRead, rowsToRead),
                out var valuesRead
            );
            if (read == 0)
                break;

            totalRowsRead += (int)read;
            totalValuesRead += (int)valuesRead;
        }

        if (totalRowsRead > 0)
        {
            typedWriter.WriteBatch(
                totalRowsRead,
                defLevels.AsSpan(0, totalRowsRead),
                repLevels.AsSpan(0, totalRowsRead),
                values.AsSpan(0, totalValuesRead)
            );
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

        var totalRowsRead = 0;
        var totalValuesRead = 0;
        while (totalRowsRead < numRows)
        {
            var rowsToRead = numRows - totalRowsRead;
            var read = typedReader.ReadBatch(
                rowsToRead,
                defLevels.AsSpan(totalRowsRead, rowsToRead),
                repLevels.AsSpan(totalRowsRead, rowsToRead),
                values.AsSpan(totalValuesRead, rowsToRead),
                out var valuesRead
            );
            if (read == 0)
                break;

            totalRowsRead += (int)read;
            totalValuesRead += (int)valuesRead;
        }

        if (totalRowsRead > 0)
        {
            typedWriter.WriteBatch(
                totalRowsRead,
                defLevels.AsSpan(0, totalRowsRead),
                repLevels.AsSpan(0, totalRowsRead),
                values.AsSpan(0, totalValuesRead)
            );
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

        var totalRowsRead = 0;
        var totalValuesRead = 0;
        while (totalRowsRead < numRows)
        {
            var rowsToRead = numRows - totalRowsRead;
            var read = typedReader.ReadBatch(
                rowsToRead,
                defLevels.AsSpan(totalRowsRead, rowsToRead),
                repLevels.AsSpan(totalRowsRead, rowsToRead),
                values.AsSpan(totalValuesRead, rowsToRead),
                out var valuesRead
            );
            if (read == 0)
                break;

            totalRowsRead += (int)read;
            totalValuesRead += (int)valuesRead;
        }

        if (totalRowsRead > 0)
        {
            typedWriter.WriteBatch(
                totalRowsRead,
                defLevels.AsSpan(0, totalRowsRead),
                repLevels.AsSpan(0, totalRowsRead),
                values.AsSpan(0, totalValuesRead)
            );
        }
    }

    internal static void WriteDummyColumn(
        ColumnWriter colWriter,
        ColumnDescriptor columnDescriptor,
        int numRows
    )
    {
        switch (columnDescriptor.PhysicalType)
        {
            case PhysicalType.Boolean:
                WriteDummyTypedColumn<bool>(colWriter, columnDescriptor, numRows);
                break;
            case PhysicalType.Int32:
                WriteDummyTypedColumn<int>(colWriter, columnDescriptor, numRows);
                break;
            case PhysicalType.Int64:
                WriteDummyTypedColumn<long>(colWriter, columnDescriptor, numRows);
                break;
            case PhysicalType.Int96:
                WriteDummyTypedColumn<Int96>(colWriter, columnDescriptor, numRows);
                break;
            case PhysicalType.Float:
                WriteDummyTypedColumn<float>(colWriter, columnDescriptor, numRows);
                break;
            case PhysicalType.Double:
                WriteDummyTypedColumn<double>(colWriter, columnDescriptor, numRows);
                break;
            case PhysicalType.ByteArray:
                WriteDummyByteArrayColumn(colWriter, columnDescriptor, numRows);
                break;
            case PhysicalType.FixedLenByteArray:
                WriteDummyFixedLenByteArrayColumn(colWriter, columnDescriptor, numRows);
                break;
            case PhysicalType.Undefined:
            default:
                throw new NotSupportedException(
                    $"Unsupported physical type for fallback: {columnDescriptor.PhysicalType}"
                );
        }
    }

    private static void WriteDummyTypedColumn<T>(
        ColumnWriter colWriter,
        ColumnDescriptor columnDescriptor,
        int numRows
    )
        where T : unmanaged
    {
        var typedWriter =
            colWriter as ColumnWriter<T>
            ?? throw new InvalidOperationException($"Expected ColumnWriter<{typeof(T).Name}>");

        if (numRows <= 0)
            return;

        var values = new T[numRows];
        var defLevels = new short[numRows];
        var repLevels = new short[numRows];

        if (columnDescriptor.MaxDefinitionLevel == 0)
        {
            typedWriter.WriteBatch(values.AsSpan());
            return;
        }

        typedWriter.WriteBatch(numRows, defLevels.AsSpan(), repLevels.AsSpan(), values.AsSpan());
    }

    private static void WriteDummyByteArrayColumn(
        ColumnWriter colWriter,
        ColumnDescriptor columnDescriptor,
        int numRows
    )
    {
        var typedWriter =
            colWriter as ColumnWriter<ByteArray>
            ?? throw new InvalidOperationException("Expected ColumnWriter<ByteArray>");

        if (numRows <= 0)
            return;

        var values = new ByteArray[numRows];
        var defLevels = new short[numRows];
        var repLevels = new short[numRows];

        if (columnDescriptor.MaxDefinitionLevel == 0)
        {
            typedWriter.WriteBatch(values.AsSpan());
            return;
        }

        typedWriter.WriteBatch(numRows, defLevels.AsSpan(), repLevels.AsSpan(), values.AsSpan());
    }

    private static void WriteDummyFixedLenByteArrayColumn(
        ColumnWriter colWriter,
        ColumnDescriptor columnDescriptor,
        int numRows
    )
    {
        var typedWriter =
            colWriter as ColumnWriter<FixedLenByteArray>
            ?? throw new InvalidOperationException("Expected ColumnWriter<FixedLenByteArray>");

        if (numRows <= 0)
            return;

        var values = new FixedLenByteArray[numRows];
        var defLevels = new short[numRows];
        var repLevels = new short[numRows];

        if (columnDescriptor.MaxDefinitionLevel == 0)
        {
            typedWriter.WriteBatch(values.AsSpan());
            return;
        }

        typedWriter.WriteBatch(numRows, defLevels.AsSpan(), repLevels.AsSpan(), values.AsSpan());
    }
}
