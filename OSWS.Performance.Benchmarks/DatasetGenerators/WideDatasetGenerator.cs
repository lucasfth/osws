using ParquetSharp;
using ParquetSharp.IO;

namespace OSWS.Performance.Benchmarks.DatasetGenerators;

/// <summary>
/// Generates wide parquet datasets with many columns to stress footer parsing and key retrieval.
/// Default: 2,000 columns × 10,000 rows (~150MB)
/// </summary>
public static class WideDatasetGenerator
{
    public static async Task<Stream> GenerateAsync(
        int columns = 2000,
        int rows = 10000,
        CancellationToken cancellationToken = default
    )
    {
        await Task.CompletedTask; // For async signature consistency

        var outputStream = new MemoryStream();

        // Create schema with many columns
        var schemaColumns = new List<Column>();
        for (var i = 0; i < columns; i++)
        {
            schemaColumns.Add(new Column<double>($"col_{i}"));
        }

        using var outputMos = new ManagedOutputStream(outputStream, leaveOpen: true);
        using var writerProperties = WriterProperties.GetDefaultWriterProperties();
        using var writer = new ParquetFileWriter(
            outputMos,
            schemaColumns.ToArray(),
            writerProperties
        );

        // Write data
        using var rowGroup = writer.AppendRowGroup();

        for (var i = 0; i < columns; i++)
        {
            using var colWriter = rowGroup.NextColumn().LogicalWriter<double>();
            var data = new double[rows];
            for (var j = 0; j < rows; j++)
            {
                data[j] = (i * rows + j) * 0.123; // Some deterministic data
            }
            colWriter.WriteBatch(data);
        }

        writer.Close();
        outputStream.Position = 0;

        return outputStream;
    }
}
