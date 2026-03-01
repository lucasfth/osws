using ParquetSharp;
using ParquetSharp.IO;

namespace OSWS.Performance.Tests.DatasetGenerators;

/// <summary>
/// Generates deep parquet datasets with many rows to stress cryptographic operations.
/// Default: 10 columns × 10,000,000 rows (~500MB)
/// </summary>
public static class DeepDatasetGenerator
{
    public static async Task<Stream> GenerateAsync(
        int columns = 10,
        int rows = 10_000_000,
        CancellationToken cancellationToken = default
    )
    {
        await Task.CompletedTask; // For async signature consistency

        var outputStream = new MemoryStream();

        // Create schema with few columns
        var schemaColumns = new List<Column>();
        for (var i = 0; i < columns; i++)
        {
            schemaColumns.Add(new Column<long>($"col_{i}"));
        }

        using var outputMos = new ManagedOutputStream(outputStream, leaveOpen: true);
        using var writerProperties = WriterProperties.GetDefaultWriterProperties();
        using var writer = new ParquetFileWriter(outputMos, schemaColumns.ToArray(), writerProperties);

        // Write data in chunks to avoid memory issues
        const int chunkSize = 100_000;
        var numChunks = (rows + chunkSize - 1) / chunkSize;

        for (var chunk = 0; chunk < numChunks; chunk++)
        {
            var chunkRows = Math.Min(chunkSize, rows - chunk * chunkSize);

            using var rowGroup = writer.AppendRowGroup();

            for (var i = 0; i < columns; i++)
            {
                using var colWriter = rowGroup.NextColumn().LogicalWriter<long>();
                var data = new long[chunkRows];
                for (var j = 0; j < chunkRows; j++)
                {
                    data[j] = (long)i * rows + chunk * chunkSize + j;
                }
                colWriter.WriteBatch(data);
            }
        }

        writer.Close();
        outputStream.Position = 0;

        return outputStream;
    }
}
