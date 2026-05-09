using ParquetSharp;
using ParquetSharp.IO;

namespace OSWS.Performance.Benchmarks.DatasetGenerators;

/// <summary>
/// Generates synthetic parquet files for benchmarking.
/// </summary>
public static class ParquetGenerator
{
    private const int ChunkRows = 50_000;

    /// <summary>Named row counts for the PUT/GET latency benchmark corpus.</summary>
    public static readonly IReadOnlyDictionary<string, int> CorpusSizes = new Dictionary<
        string,
        int
    >
    {
        ["tiny"] = 1_000,
        ["small"] = 10_000,
        ["medium"] = 250_000,
        ["large"] = 1_000_000,
        ["xlarge"] = 2_000_000,
    };

    /// <summary>Column count used for the PUT/GET latency benchmark corpus.</summary>
    public const int CorpusColumns = 50;

    /// <summary>
    /// Generates a parquet file with <paramref name="columns"/> double columns and
    /// <paramref name="rows"/> rows. Writes directly to <paramref name="output"/>;
    /// if null, returns a new <see cref="MemoryStream"/> positioned at 0.
    /// </summary>
    public static async Task<Stream> GenerateAsync(
        int columns,
        int rows,
        Stream? output = null,
        CancellationToken cancellationToken = default
    )
    {
        return await Task.Run(() => GenerateInternal(columns, rows, output), cancellationToken);
    }

    /// <summary>
    /// Generates all corpus size files to <paramref name="directory"/>,
    /// skipping any that already exist.
    /// </summary>
    public static async Task GenerateCorpusToDiskAsync(
        string directory,
        CancellationToken cancellationToken = default
    )
    {
        Directory.CreateDirectory(directory);

        foreach (var (sizeLabel, rowCount) in CorpusSizes)
        {
            var path = Path.Combine(directory, $"{sizeLabel}.parquet");

            if (File.Exists(path))
            {
                var existingMb = new FileInfo(path).Length / 1024.0 / 1024.0;
                Console.WriteLine(
                    $" -> {sizeLabel}: already exists ({existingMb:F1} MB), skipping"
                );
                continue;
            }

            Console.Write(
                $"  ↓  {sizeLabel}: generating ({rowCount:N0} rows X {CorpusColumns} cols)... "
            );
            await using var fs = new FileStream(path, FileMode.Create, FileAccess.Write);
            await GenerateAsync(CorpusColumns, rowCount, fs, cancellationToken);
            var sizeMb = new FileInfo(path).Length / 1024.0 / 1024.0;
            Console.WriteLine($"done ({sizeMb:F1} MB) -> {path}");
        }
    }

    private static Stream GenerateInternal(int columns, int rows, Stream? output)
    {
        var schemaColumns = Enumerable
            .Range(0, columns)
            .Select(i => (Column)new Column<double>($"col_{i}"))
            .ToArray();

        var outputStream = output ?? new MemoryStream();
        using var outputMos = new ManagedOutputStream(outputStream, leaveOpen: true);
        using var writerProperties = WriterProperties.GetDefaultWriterProperties();
        using var writer = new ParquetFileWriter(outputMos, schemaColumns, writerProperties);

        var written = 0;
        while (written < rows)
        {
            var chunk = Math.Min(ChunkRows, rows - written);
            using var rowGroup = writer.AppendRowGroup();

            for (var col = 0; col < columns; col++)
            {
                using var colWriter = rowGroup.NextColumn().LogicalWriter<double>();
                var data = new double[chunk];
                for (var i = 0; i < chunk; i++)
                    data[i] = (double)(col * rows + written + i);
                colWriter.WriteBatch(data);
            }

            written += chunk;
        }

        writer.Close();

        if (outputStream.CanSeek)
            outputStream.Position = 0;

        return outputStream;
    }
}
