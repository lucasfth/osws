using Azure.Identity;
using Azure.Security.KeyVault.Keys;
using Azure.Security.KeyVault.Keys.Cryptography;
using OSWS.Common.Configuration;
using OSWS.Models.Interfaces;

namespace OSWS.KeyManager.Providers;

/// <summary>
/// Azure Key Vault implementation of <see cref="IKeyVaultProvider"/>.
/// Azure holds the encryption key (DEK); encrypt/decrypt operations are performed server-side.
/// No plaintext keys are stored locally - only the encrypted DEK is persisted in parquet metadata.
/// </summary>
public class AzureKeyVaultProvider : IKeyVaultProvider
{
    private readonly KeyClient _keyClient;
    private readonly KeyVaultSettings _settings;

    /// <summary>
    /// Tag key used to store the associated role on Azure KV key properties.
    /// </summary>
    private const string RoleTagKey = "osws-role";

    public AzureKeyVaultProvider(KeyVaultSettings settings)
    {
        _settings = settings ?? throw new ArgumentNullException(nameof(settings));

        if (string.IsNullOrWhiteSpace(settings.VaultUri))
            throw new ArgumentException(
                "KeyVault VaultUri is required for Azure provider.",
                nameof(settings)
            );

        var credential = BuildCredential(settings);
        _keyClient = new KeyClient(new Uri(settings.VaultUri), credential);
    }

    public async Task<string> CreateKeyAsync(string keyName, string role)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(keyName);
        ArgumentException.ThrowIfNullOrWhiteSpace(role);

        // Create an RSA key in Azure KV used to encrypt/decrypt ephemeral DEKs
        var options = new CreateRsaKeyOptions(keyName)
        {
            KeySize = 2048,
            KeyOperations = { KeyOperation.Encrypt, KeyOperation.Decrypt },
        };

        options.Tags[RoleTagKey] = role;

        var response = await _keyClient.CreateRsaKeyAsync(options);
        return response.Value.Id.ToString();
    }

    public async Task<byte[]> EncryptAsync(string keyName, byte[] plaintext)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(keyName);
        ArgumentNullException.ThrowIfNull(plaintext);

        var cryptoClient = _keyClient.GetCryptographyClient(keyName);
        var result = await cryptoClient.EncryptAsync(EncryptionAlgorithm.RsaOaep256, plaintext);
        return result.Ciphertext;
    }

    public async Task<byte[]> DecryptAsync(string keyName, byte[] ciphertext)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(keyName);
        ArgumentNullException.ThrowIfNull(ciphertext);

        var cryptoClient = _keyClient.GetCryptographyClient(keyName);
        var result = await cryptoClient.DecryptAsync(EncryptionAlgorithm.RsaOaep256, ciphertext);
        return result.Plaintext;
    }

    public async Task<KeyVaultKeyInfo?> GetKeyInfoAsync(string keyName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(keyName);

        try
        {
            var response = await _keyClient.GetKeyAsync(keyName);
            var key = response.Value;
            return MapToKeyInfo(key);
        }
        catch (Azure.RequestFailedException ex) when (ex.Status == 404)
        {
            return null;
        }
    }

    public async Task<IReadOnlyList<KeyVaultKeyInfo>> ListKeysAsync(string? role = null)
    {
        var keys = new List<KeyVaultKeyInfo>();

        await foreach (var keyProperties in _keyClient.GetPropertiesOfKeysAsync())
        {
            if (role != null)
            {
                if (
                    !keyProperties.Tags.TryGetValue(RoleTagKey, out var tagRole)
                    || !string.Equals(tagRole, role, StringComparison.OrdinalIgnoreCase)
                )
                {
                    continue;
                }
            }

            keyProperties.Tags.TryGetValue(RoleTagKey, out var roleTag);
            keys.Add(
                new KeyVaultKeyInfo
                {
                    KeyName = keyProperties.Name,
                    KeyId = keyProperties.Id.ToString(),
                    Role = roleTag,
                    CreatedOn = keyProperties.CreatedOn,
                    ExpiresOn = keyProperties.ExpiresOn,
                    Enabled = keyProperties.Enabled ?? true,
                }
            );
        }

        return keys;
    }

    private static KeyVaultKeyInfo MapToKeyInfo(KeyVaultKey key)
    {
        key.Properties.Tags.TryGetValue(RoleTagKey, out var roleTag);
        return new KeyVaultKeyInfo
        {
            KeyName = key.Name,
            KeyId = key.Id.ToString(),
            Role = roleTag,
            CreatedOn = key.Properties.CreatedOn,
            ExpiresOn = key.Properties.ExpiresOn,
            Enabled = key.Properties.Enabled ?? true,
        };
    }

    private static DefaultAzureCredential BuildCredential(KeyVaultSettings settings)
    {
        var options = new DefaultAzureCredentialOptions();

        if (!string.IsNullOrWhiteSpace(settings.TenantId))
            options.TenantId = settings.TenantId;

        // When ClientId + ClientSecret are set, the EnvironmentCredential path
        // inside DefaultAzureCredential will pick them up via env vars,
        // or you can use ClientSecretCredential directly. DefaultAzureCredential
        // also supports managed identity, CLI, VS, etc.
        return new DefaultAzureCredential(options);
    }
}
