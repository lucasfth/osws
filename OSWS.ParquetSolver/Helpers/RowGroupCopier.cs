using ParquetSharp;

namespace OSWS.ParquetSolver.Helpers;

/// <summary>
/// Orchestrates copying row groups from a ParquetFileReader to a ParquetFileWriter.
/// Delegates individual column copying to ColumnCopier.
/// </summary>
public static class RowGroupCopier
{
    /// <summary>
    /// Copies all row groups from parquetFileReader to parquetFileWriter.
    /// Columns not in allowedColumns (when specified) receive dummy values instead of their real data.
    /// </summary>
    public static void CopyRowGroups(
        ParquetFileWriter parquetFileWriter,
        ParquetFileReader parquetFileReader,
        int numColumns,
        int numRowGroups,
        ColumnDecryptionFailureBehavior failureBehavior = ColumnDecryptionFailureBehavior.Throw,
        Action<string, Exception>? onColumnDecryptionError = null,
        ISet<string>? allowedColumns = null
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
                var columnDescriptor = schema.Column(col);

                if (allowedColumns != null && !allowedColumns.Contains(columnDescriptor.Name))
                {
                    using var colWriter = rowGroupWriter.NextColumn();
                    ColumnCopier.WriteDummyColumn(colWriter, columnDescriptor, numRows);
                    continue;
                }

                CopyColumnWithFallback(
                    rowGroupReader,
                    rowGroupWriter,
                    columnDescriptor,
                    col,
                    numRows,
                    failureBehavior,
                    onColumnDecryptionError
                );
            }
        }
    }

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
            ColumnCopier.CopyColumn(colReader, colWriter, numRows);
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

            ColumnCopier.WriteDummyColumn(colWriter, columnDescriptor, numRows);
        }
        finally
        {
            colWriter?.Dispose();
        }
    }
}
