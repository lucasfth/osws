using System.Collections.Concurrent;
using System.Security.Cryptography;
using OSWS.Models.Interfaces;
using OSWS.ParquetSolver;
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

        await using var encrypted = await parquetWriter.WriteParquetAsync(plaintext, role: "test-role");
        encrypted.Position = 0;

        var parquetReader = new ParquetReader(keyVaultProvider, new DekCache());
        using var decrypted = await parquetReader.ReadParquetAsync(encrypted);

        var (actualA, actualB) = ReadTwoIntColumns(decrypted);

        Assert.Equal(inputA, actualA);
        Assert.Equal(inputB, actualB);
    }

    [Fact]
    public async Task ReadParquetAsync_WithMissingColumnKey_WritesDummyValuesForThatColumn()
    {
        var inputA = new[] { 10, 20, 30, 40 };
        var inputB = new[] { 1, 2, 3, 4 };
        using var plaintext = CreateTwoColumnParquet(inputA, inputB);

        var keyVaultProvider = new InMemoryKeyVaultProvider();
        var parquetWriter = new ParquetWriter(keyVaultProvider, providerType: "InMemory");

        await using var encrypted = await parquetWriter.WriteParquetAsync(plaintext, role: "test-role");
        encrypted.Position = 0;

        keyVaultProvider.ForgetKeyByName("test-role-column-B");

        var errors = new List<string>();
        var parquetReader = new ParquetReader(keyVaultProvider, new DekCache())
        {
            ColumnDecryptionFailureBehavior = ColumnDecryptionFailureBehavior.DummyValues,
            OnColumnDecryptionError = (columnName, _) => errors.Add(columnName),
        };

        using var decrypted = await parquetReader.ReadParquetAsync(encrypted);
        var (actualA, actualB) = ReadTwoIntColumns(decrypted);

        Assert.Equal(inputA, actualA);
        Assert.Equal([0, 0, 0, 0], actualB);
        Assert.Contains("B", errors);
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
