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
    /// <param name="failureBehavior"></param>
    /// <param name="onColumnDecryptionError"></param>
    public static void CopyRowGroups(
        ParquetFileWriter parquetFileWriter,
        ParquetFileReader parquetFileReader,
        int numColumns,
        int numRowGroups,
        ColumnDecryptionFailureBehavior failureBehavior = ColumnDecryptionFailureBehavior.Throw,
        Action<string, Exception>? onColumnDecryptionError = null
    )
    {
        var schema = parquetFileReader.FileMetaData.Schema;

        for (var rg = 0; rg < numRowGroups; rg++)
        {
            using var rowGroupReader = parquetFileReader.RowGroup(rg);
            var numRows = checked((int)rowGroupReader.MetaData.NumRows);
            using var rowGroupWriter = parquetFileWriter.AppendRowGroup();

            for (var col = 0; col < numColumns; col++)
            {
                CopyColumnWithFallback(
                    rowGroupReader,
                    rowGroupWriter,
                    schema.Column(col),
                    col,
                    numRows,
                    failureBehavior,
                    onColumnDecryptionError
                );
            }
        }
    }

    /// <summary>
    /// Copy a column from a RowGroupReader to a RowGroupWriter, using the physical type to copy raw data.
    /// </summary>
    /// <param name="rowGroupReader"></param>
    /// <param name="rowGroupWriter"></param>
    /// <param name="columnDescriptor"></param>
    /// <param name="colIndex"></param>
    /// <param name="numRows"></param>
    /// <param name="failureBehavior"></param>
    /// <param name="onColumnDecryptionError"></param>
    /// <exception cref="NotSupportedException"></exception>
    private static void CopyColumnWithFallback(
        RowGroupReader rowGroupReader,
        RowGroupWriter rowGroupWriter,
        ColumnDescriptor columnDescriptor,
        int colIndex,
        int numRows,
        ColumnDecryptionFailureBehavior failureBehavior,
        Action<string, Exception>? onColumnDecryptionError
    )
    {
        ColumnWriter? colWriter = null;

        try
        {
            using var colReader = rowGroupReader.Column(colIndex);
            colWriter = rowGroupWriter.NextColumn();
            CopyColumn(colReader, colWriter, numRows);
        }
        catch (Exception ex) when (failureBehavior != ColumnDecryptionFailureBehavior.Throw)
        {
            onColumnDecryptionError?.Invoke(columnDescriptor.Name, ex);

            colWriter ??= rowGroupWriter.NextColumn();

            // Copying encrypted column chunks directly into a rewritten output file is
            // not currently feasible with ParquetSharp's managed API surface, so we
            // fall back to writing dummy values.
            // If it becomes possible in the future then make following if check and
            // implement copying encrypted data without decryption here.
            // if (failureBehavior == ColumnDecryptionFailureBehavior.CopyEncrypted)

            WriteDummyColumn(colWriter, columnDescriptor, numRows);
        }
        finally
        {
            colWriter?.Dispose();
        }
    }

    /// <summary>
    /// Copy a column from a ColumnReader to a ColumnWriter, using the physical type to copy raw data.
    /// </summary>
    /// <param name="colReader"></param>
    /// <param name="colWriter"></param>
    /// <param name="numRows"></param>
    /// <exception cref="NotSupportedException"></exception>
    private static void CopyColumn(ColumnReader colReader, ColumnWriter colWriter, int numRows)
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

    /// <summary>
    /// Copy a column of a specific physical type from a ColumnReader to a ColumnWriter, using typed APIs for efficiency.
    /// </summary>
    /// <param name="colReader"></param>
    /// <param name="colWriter"></param>
    /// <param name="numRows"></param>
    /// <typeparam name="T"></typeparam>
    /// <exception cref="InvalidOperationException"></exception>
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

    /// <summary>
    /// Copy a column of ByteArray physical type from a ColumnReader to a ColumnWriter, using typed APIs for efficiency.
    /// </summary>
    /// <param name="colReader"></param>
    /// <param name="colWriter"></param>
    /// <param name="numRows"></param>
    /// <exception cref="InvalidOperationException"></exception>
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

    /// <summary>
    /// Copy a column of FixedLenByteArray physical type from a ColumnReader to a ColumnWriter, using typed APIs for efficiency.
    /// </summary>
    /// <param name="colReader"></param>
    /// <param name="colWriter"></param>
    /// <param name="numRows"></param>
    /// <exception cref="InvalidOperationException"></exception>
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

    /// <summary>
    /// Write dummy values for a column based on its physical type.
    /// This is used as a fallback when copying fails (e.g. due to decryption failure)
    /// to allow the rest of the columns to be read.
    /// </summary>
    /// <param name="colWriter"></param>
    /// <param name="columnDescriptor"></param>
    /// <param name="numRows"></param>
    /// <exception cref="NotSupportedException"></exception>
    private static void WriteDummyColumn(
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

    /// <summary>
    /// Write dummy values for a column of a specific physical type using typed APIs for efficiency.
    /// </summary>
    /// <param name="colWriter"></param>
    /// <param name="columnDescriptor"></param>
    /// <param name="numRows"></param>
    /// <typeparam name="T"></typeparam>
    /// <exception cref="InvalidOperationException"></exception>
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

        if (columnDescriptor.MaxDefinitionLevel == 0)
        {
            var values = new T[numRows];
            typedWriter.WriteBatch(values.AsSpan());
            return;
        }

        var defLevels = new short[numRows];
        var repLevels = new short[numRows];
        typedWriter.WriteBatch(
            numRows,
            defLevels.AsSpan(),
            repLevels.AsSpan(),
            ReadOnlySpan<T>.Empty
        );
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

        if (columnDescriptor.MaxDefinitionLevel == 0)
        {
            var values = new ByteArray[numRows];
            typedWriter.WriteBatch(values.AsSpan());
            return;
        }

        var defLevels = new short[numRows];
        var repLevels = new short[numRows];
        typedWriter.WriteBatch(
            numRows,
            defLevels.AsSpan(),
            repLevels.AsSpan(),
            ReadOnlySpan<ByteArray>.Empty
        );
    }

    /// <summary>
    /// Write dummy values for a column of FixedLenByteArray physical type using typed APIs for efficiency.
    /// </summary>
    /// <param name="colWriter"></param>
    /// <param name="columnDescriptor"></param>
    /// <param name="numRows"></param>
    /// <exception cref="InvalidOperationException"></exception>
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

        if (columnDescriptor.MaxDefinitionLevel == 0)
        {
            var values = new FixedLenByteArray[numRows];
            typedWriter.WriteBatch(values.AsSpan());
            return;
        }

        var defLevels = new short[numRows];
        var repLevels = new short[numRows];
        typedWriter.WriteBatch(
            numRows,
            defLevels.AsSpan(),
            repLevels.AsSpan(),
            ReadOnlySpan<FixedLenByteArray>.Empty
        );
    }
}
