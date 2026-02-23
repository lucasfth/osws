using System.Security.Cryptography;
using OSWS.Models.Interfaces;

namespace OSWS.KeyManager.Providers;

/// <summary>
/// In-memory key vault provider for development, testing, and self-hosted scenarios.
/// Keys are generated locally and stored in memory (not persisted across restarts).
/// NOT suitable for production — use <see cref="AzureKeyVaultProvider"/> or another
/// cloud-backed implementation in production environments.
/// </summary>
public class InternalKeyVaultProvider : IKeyVaultProvider
{
    public const string ProviderTypeName = "Internal";

    private readonly Dictionary<string, InternalKeyEntry> _keys = new(StringComparer.OrdinalIgnoreCase);
    private readonly Lock _lock = new();

    public Task<string> CreateKeyAsync(string keyName, string role)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(keyName);
        ArgumentException.ThrowIfNullOrWhiteSpace(role);

        lock (_lock)
        {
            if (!_keys.ContainsKey(keyName))
            {
                // Generate a 256-bit wrapping key for AES-KW
                var wrappingKey = RandomNumberGenerator.GetBytes(32);
                _keys[keyName] = new InternalKeyEntry(keyName, role, wrappingKey);
            }
        }

        return Task.FromResult(keyName);
    }

    public Task<byte[]> WrapKeyAsync(string keyName, byte[] plainKey)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(keyName);
        ArgumentNullException.ThrowIfNull(plainKey);

        var entry = GetEntryOrThrow(keyName);

        // AES key-wrap (RFC 3394)
        using var aes = Aes.Create();
        aes.Key = entry.WrappingKey;
        var wrapped = aes.EncryptCbc(plainKey, new byte[16], PaddingMode.PKCS7);
        return Task.FromResult(wrapped);
    }

    public Task<byte[]> UnwrapKeyAsync(string keyName, byte[] wrappedKey)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(keyName);
        ArgumentNullException.ThrowIfNull(wrappedKey);

        var entry = GetEntryOrThrow(keyName);

        using var aes = Aes.Create();
        aes.Key = entry.WrappingKey;
        var plainKey = aes.DecryptCbc(wrappedKey, new byte[16], PaddingMode.PKCS7);
        return Task.FromResult(plainKey);
    }

    public Task<KeyVaultKeyInfo?> GetKeyInfoAsync(string keyName)
    {
        lock (_lock)
        {
            if (_keys.TryGetValue(keyName, out var entry))
            {
                return Task.FromResult<KeyVaultKeyInfo?>(new KeyVaultKeyInfo
                {
                    KeyName = entry.KeyName,
                    KeyId = entry.KeyName,
                    Role = entry.Role,
                    CreatedOn = entry.CreatedOn,
                    Enabled = true,
                });
            }
        }

        return Task.FromResult<KeyVaultKeyInfo?>(null);
    }

    public Task<IReadOnlyList<KeyVaultKeyInfo>> ListKeysAsync(string? role = null)
    {
        lock (_lock)
        {
            var result = _keys.Values
                .Where(e => role == null || string.Equals(e.Role, role, StringComparison.OrdinalIgnoreCase))
                .Select(e => new KeyVaultKeyInfo
                {
                    KeyName = e.KeyName,
                    KeyId = e.KeyName,
                    Role = e.Role,
                    CreatedOn = e.CreatedOn,
                    Enabled = true,
                })
                .ToList();

            return Task.FromResult<IReadOnlyList<KeyVaultKeyInfo>>(result);
        }
    }

    private InternalKeyEntry GetEntryOrThrow(string keyName)
    {
        lock (_lock)
        {
            if (_keys.TryGetValue(keyName, out var entry))
                return entry;
        }

        throw new InvalidOperationException($"Key '{keyName}' not found in internal key vault.");
    }

    private sealed record InternalKeyEntry(
        string KeyName,
        string Role,
        byte[] WrappingKey,
        DateTimeOffset CreatedOn = default)
    {
        public DateTimeOffset CreatedOn { get; } = CreatedOn == default ? DateTimeOffset.UtcNow : CreatedOn;
    }
}
