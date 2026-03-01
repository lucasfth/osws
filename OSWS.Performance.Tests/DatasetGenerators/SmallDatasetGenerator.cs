using ParquetSharp;
using ParquetSharp.IO;

namespace OSWS.Performance.Tests.DatasetGenerators;

/// <summary>
/// Generates small parquet datasets to stress request overhead and key retrieval.
/// Default: 5 columns × 5,000 rows (~1MB)
/// </summary>
public static class SmallDatasetGenerator
{
    public static async Task<Stream> GenerateAsync(
        int columns = 5,
        int rows = 5000,
        CancellationToken cancellationToken = default
    )
    {
        await Task.CompletedTask; // For async signature consistency

        var outputStream = new MemoryStream();

        // Create schema
        var schemaColumns = new List<Column>();
        for (var i = 0; i < columns; i++)
        {
            schemaColumns.Add(new Column<int>($"col_{i}"));
        }

        using var outputMos = new ManagedOutputStream(outputStream, leaveOpen: true);
        using var writerProperties = WriterProperties.GetDefaultWriterProperties();
        using var writer = new ParquetFileWriter(outputMos, schemaColumns.ToArray(), writerProperties);

        // Write data
        using var rowGroup = writer.AppendRowGroup();

        for (var i = 0; i < columns; i++)
        {
            using var colWriter = rowGroup.NextColumn().LogicalWriter<int>();
            var data = new int[rows];
            for (var j = 0; j < rows; j++)
            {
                data[j] = i * rows + j;
            }
            colWriter.WriteBatch(data);
        }

        writer.Close();
        outputStream.Position = 0;

        return outputStream;
    }
}
