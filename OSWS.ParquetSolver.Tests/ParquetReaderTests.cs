using System.Collections.Concurrent;
using System.Security.Cryptography;
using OSWS.Models.Interfaces;
using OSWS.ParquetSolver.Helpers;
using ParquetSharp;
using ParquetSharp.IO;
using Xunit;

namespace OSWS.ParquetSolver.Tests;

public class ParquetReaderTests
{
    [Fact]
    public async Task ReadParquetAsync_WithAllKeysAvailable_DecryptsAllColumns()
    {
        var inputA = new[] { 10, 20, 30, 40 };
        var inputB = new[] { 1, 2, 3, 4 };
        using var plaintext = CreateTwoColumnParquet(inputA, inputB);

        var keyVaultProvider = new InMemoryKeyVaultProvider();
        var parquetWriter = new ParquetWriter(keyVaultProvider, providerType: "InMemory");

        var (encResult, _) = await parquetWriter.WriteParquetAsync(
            plaintext,
            role: "test-role"
        );
        await using var encrypted = encResult;

        var keys = await keyVaultProvider.ListKeysAsync();

        encrypted.Position = 0;

        var parquetReader = new ParquetReader(keyVaultProvider, new DekCache());
        using var decrypted = await parquetReader.ReadParquetAsync(encrypted);

        var (actualA, actualB) = ReadTwoIntColumns(decrypted);

        Assert.Equal(inputA, actualA);
        Assert.Equal(inputB, actualB);
        Assert.Single(keys);
    }

    [Fact]
    public async Task WriteParquetAsync_WithEncryptedColumns_LeavesFooterPlaintext()
    {
        using var plaintext = CreateTwoColumnParquet([10, 20], [30, 40]);

        var keyVaultProvider = new InMemoryKeyVaultProvider();
        var parquetWriter = new ParquetWriter(keyVaultProvider, providerType: "InMemory");

        var (encResult2, _) = await parquetWriter.WriteParquetAsync(
            plaintext,
            role: "test-role"
        );
        await using var encrypted = encResult2;
        encrypted.Position = 0;

        using var inputRaf = new ManagedRandomAccessFile(encrypted, leaveOpen: true);
        using var reader = new ParquetFileReader(inputRaf);

        Assert.Equal(2, reader.FileMetaData.NumColumns);
    }

    [Fact]
    public async Task ReadParquetAsync_InternalProvider_DecryptsAllColumns()
    {
        var inputA = new[] { 5, 6, 7, 8 };
        var inputB = new[] { 9, 10, 11, 12 };
        using var plaintext = CreateTwoColumnParquet(inputA, inputB);

        var keyVaultProvider = new KeyManager.Providers.InternalKeyVaultProvider();
        var parquetWriter = new ParquetWriter(
            keyVaultProvider,
            providerType: KeyManager.Providers.InternalKeyVaultProvider.ProviderTypeName
        );

        var (encResult3, _) = await parquetWriter.WriteParquetAsync(
            plaintext,
            role: "test-role"
        );
        await using var encrypted = encResult3;
        encrypted.Position = 0;

        var parquetReader = new ParquetReader(keyVaultProvider, new DekCache());
        using var decrypted = await parquetReader.ReadParquetAsync(encrypted);

        var (actualA, actualB) = ReadTwoIntColumns(decrypted);

        Assert.Equal(inputA, actualA);
        Assert.Equal(inputB, actualB);
    }

    [Fact]
    public async Task ReadParquetAsync_InternalProvider_WithSmallDatasetGenerator_Succeeds()
    {
        // regress the failure seen in benchmark global setup when warming up
        using var plaintext = await GenerateParquetAsync(columns: 5, rows: 5000);

        var keyVaultProvider = new KeyManager.Providers.InternalKeyVaultProvider();
        var parquetWriter = new ParquetWriter(
            keyVaultProvider,
            providerType: KeyManager.Providers.InternalKeyVaultProvider.ProviderTypeName
        );

        var (encResult4, _) = await parquetWriter.WriteParquetAsync(
            plaintext,
            role: "test-role"
        );
        await using var encrypted = encResult4;
        encrypted.Position = 0;

        var parquetReader = new ParquetReader(keyVaultProvider, new DekCache());
        using var decrypted = await parquetReader.ReadParquetAsync(encrypted);

        // just ensure we can read a few bytes from the decrypted output
        Assert.True(decrypted.Length > 0);
    }

    [Fact]
    public async Task ReadParquetAsync_InternalProvider_SecondFileWithSameCache_Succeeds()
    {
        // Reproduces benchmark warmup behavior: decrypt file A, then file B
        // with the same DEK cache and overlapping key names.
        var keyVaultProvider = new KeyManager.Providers.InternalKeyVaultProvider();
        var parquetWriter = new ParquetWriter(
            keyVaultProvider,
            providerType: KeyManager.Providers.InternalKeyVaultProvider.ProviderTypeName
        );
        var dekCache = new DekCache();

        using var firstPlain = await GenerateParquetAsync(columns: 5, rows: 250);
        using var secondPlain = await GenerateParquetAsync(columns: 5, rows: 300);

        var (firstEncResult, _) = await parquetWriter.WriteParquetAsync(
            firstPlain,
            role: "test-role"
        );
        await using var firstEncrypted = firstEncResult;
        var (secondEncResult, _) = await parquetWriter.WriteParquetAsync(
            secondPlain,
            role: "test-role"
        );
        await using var secondEncrypted = secondEncResult;

        firstEncrypted.Position = 0;
        var firstReader = new ParquetReader(keyVaultProvider, dekCache);
        using var firstDecrypted = await firstReader.ReadParquetAsync(firstEncrypted);
        Assert.True(firstDecrypted.Length > 0);

        secondEncrypted.Position = 0;
        var secondReader = new ParquetReader(keyVaultProvider, dekCache);
        using var secondDecrypted = await secondReader.ReadParquetAsync(secondEncrypted);
        Assert.True(secondDecrypted.Length > 0);
    }

    [Fact]
    public async Task WriteMultipleEncryptedStreams_ThenReadFirst_Fails()
    {
        // simulate benchmark global setup: write small, wide, deep then read small
        var keyVaultProvider = new KeyManager.Providers.InternalKeyVaultProvider();
        var parquetWriter = new ParquetWriter(
            keyVaultProvider,
            providerType: KeyManager.Providers.InternalKeyVaultProvider.ProviderTypeName
        );

        using var smallPlain = await GenerateParquetAsync(columns: 5, rows: 5000);
        var (smallEncrypted, _) = await parquetWriter.WriteParquetAsync(smallPlain, role: "test-role");

        // generate a large "wide" dataset similar to benchmark dimensions
        using var widePlain = await GenerateParquetAsync(columns: 2000, rows: 10000);
        var (wideEncrypted, _) = await parquetWriter.WriteParquetAsync(widePlain, role: "test-role");

        using var deepPlain = await GenerateParquetAsync(columns: 10, rows: 1000000);
        var (deepEncrypted, _) = await parquetWriter.WriteParquetAsync(deepPlain, role: "test-role");

        // simulate potential GC/LOH compaction that could move the underlying
        // buffer of the smallEncrypted MemoryStream. ParquetSharp's
        // ManagedRandomAccessFile holds an unsafe pointer; if the buffer is not
        // pinned the pointer becomes stale and crypto operations can fail.
        // Our benchmark suffered "Parquet crypto signature verification failed"
        // after writing large datasets, so force a compaction here to reproduce.
        System.Runtime.GCSettings.LargeObjectHeapCompactionMode = System
            .Runtime
            .GCLargeObjectHeapCompactionMode
            .CompactOnce;
        GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, true, true);
        GC.WaitForPendingFinalizers();
        System.Runtime.GCSettings.LargeObjectHeapCompactionMode = System
            .Runtime
            .GCLargeObjectHeapCompactionMode
            .Default;

        smallEncrypted.Position = 0;

        var parquetReader = new ParquetReader(keyVaultProvider, new DekCache());
        using var decrypted = await parquetReader.ReadParquetAsync(smallEncrypted);

        // if we reach here without exception we consider success
        Assert.True(decrypted.Length > 0);
    }

    [Fact]
    public async Task ReadParquetAsync_WithMissingFileKey_ThrowsBeforeColumnFallback()
    {
        var inputA = new[] { 10, 20, 30, 40 };
        var inputB = new[] { 1, 2, 3, 4 };
        using var plaintext = CreateTwoColumnParquet(inputA, inputB);

        var keyVaultProvider = new InMemoryKeyVaultProvider();
        var parquetWriter = new ParquetWriter(keyVaultProvider, providerType: "InMemory");

        var (encResult5, _) = await parquetWriter.WriteParquetAsync(
            plaintext,
            role: "test-role"
        );
        await using var encrypted = encResult5;
        encrypted.Position = 0;

        var fileKey = Assert.Single(await keyVaultProvider.ListKeysAsync());
        keyVaultProvider.ForgetKeyByName(fileKey.KeyName);

        var errors = new List<string>();
        var parquetReader = new ParquetReader(keyVaultProvider, new DekCache())
        {
            ColumnDecryptionFailureBehavior = ColumnDecryptionFailureBehavior.DummyValues,
            OnColumnDecryptionError = (columnName, _) => errors.Add(columnName),
        };

        await Assert.ThrowsAnyAsync<Exception>(() => parquetReader.ReadParquetAsync(encrypted));
        Assert.Empty(errors);
    }

    private static MemoryStream CreateTwoColumnParquet(int[] columnA, int[] columnB)
    {
        var output = new MemoryStream();
        var columns = new Column[] { new Column<int>("A"), new Column<int>("B") };

        using (var fileWriter = new ParquetFileWriter(output, columns))
        {
            using var rowGroupWriter = fileWriter.AppendRowGroup();
            using (var writerA = rowGroupWriter.NextColumn().LogicalWriter<int>())
            {
                writerA.WriteBatch(columnA);
            }

            using (var writerB = rowGroupWriter.NextColumn().LogicalWriter<int>())
            {
                writerB.WriteBatch(columnB);
            }

            fileWriter.Close();
        }

        return new MemoryStream(output.ToArray());
    }

    private static async Task<MemoryStream> GenerateParquetAsync(int columns, int rows)
    {
        await Task.CompletedTask; // preserve async signature
        var outputStream = new MemoryStream();
        var schemaColumns = new List<Column>();
        for (var i = 0; i < columns; i++)
        {
            schemaColumns.Add(new Column<int>($"col_{i}"));
        }

        using var outputMos = new ManagedOutputStream(outputStream, leaveOpen: true);
        using var writerProperties = WriterProperties.GetDefaultWriterProperties();
        using var writer = new ParquetFileWriter(
            outputMos,
            schemaColumns.ToArray(),
            writerProperties
        );

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

    private static (int[] columnA, int[] columnB) ReadTwoIntColumns(Stream input)
    {
        input.Position = 0;

        using var inputRaf = new ManagedRandomAccessFile(input, leaveOpen: true);
        using var reader = new ParquetFileReader(inputRaf);
        using var rowGroupReader = reader.RowGroup(0);

        var numRows = checked((int)rowGroupReader.MetaData.NumRows);

        int[] columnA;
        int[] columnB;

        using (var aReader = rowGroupReader.Column(0))
        {
            columnA = aReader.LogicalReader<int>().ReadAll(numRows);
        }

        using (var bReader = rowGroupReader.Column(1))
        {
            columnB = bReader.LogicalReader<int>().ReadAll(numRows);
        }

        return (columnA, columnB);
    }

    private sealed class InMemoryKeyVaultProvider : IKeyVaultProvider
    {
        private readonly ConcurrentDictionary<string, byte[]> _keysById = new();
        private readonly ConcurrentDictionary<string, string> _keyIdByName = new();

        public Task<string> CreateKeyAsync(string keyName, string role)
        {
            var keyId = _keyIdByName.GetOrAdd(
                keyName,
                _ =>
                {
                    var generated = $"inmemory://{keyName}/{Guid.NewGuid():N}";
                    _keysById[generated] = RandomNumberGenerator.GetBytes(32);
                    return generated;
                }
            );

            return Task.FromResult(keyId);
        }

        public Task<byte[]> EncryptAsync(string keyName, byte[] plaintext)
        {
            if (!_keysById.TryGetValue(keyName, out var key))
            {
                throw new KeyNotFoundException($"Key not found: {keyName}");
            }

            return Task.FromResult(Xor(plaintext, key));
        }

        public Task<byte[]> DecryptAsync(string keyName, byte[] ciphertext)
        {
            if (!_keysById.TryGetValue(keyName, out var key))
            {
                throw new KeyNotFoundException($"Key not found: {keyName}");
            }

            return Task.FromResult(Xor(ciphertext, key));
        }

        public Task<KeyVaultKeyInfo?> GetKeyInfoAsync(string keyName)
        {
            if (_keyIdByName.TryGetValue(keyName, out var keyId))
            {
                return Task.FromResult<KeyVaultKeyInfo?>(
                    new KeyVaultKeyInfo
                    {
                        KeyName = keyName,
                        KeyId = keyId,
                        Role = null,
                        Enabled = _keysById.ContainsKey(keyId),
                    }
                );
            }

            return Task.FromResult<KeyVaultKeyInfo?>(null);
        }

        public Task<IReadOnlyList<KeyVaultKeyInfo>> ListKeysAsync(string? role = null)
        {
            var keys = _keyIdByName
                .Select(kvp => new KeyVaultKeyInfo
                {
                    KeyName = kvp.Key,
                    KeyId = kvp.Value,
                    Role = role,
                    Enabled = _keysById.ContainsKey(kvp.Value),
                })
                .ToList();

            return Task.FromResult<IReadOnlyList<KeyVaultKeyInfo>>(keys);
        }

        public void ForgetKeyByName(string keyName)
        {
            if (_keyIdByName.TryGetValue(keyName, out var keyId))
            {
                _keysById.TryRemove(keyId, out _);
            }
        }

        private static byte[] Xor(byte[] data, byte[] key)
        {
            var output = new byte[data.Length];
            for (var i = 0; i < data.Length; i++)
            {
                output[i] = (byte)(data[i] ^ key[i % key.Length]);
            }

            return output;
        }
    }
}
