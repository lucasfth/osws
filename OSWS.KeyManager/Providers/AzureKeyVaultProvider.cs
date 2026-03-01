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

    /// <summary>
    /// Create a new RSA key in Azure Key Vault with a unique GUID-based name and role tag.
    /// Each column and footer encryption uses a fresh key - no key reuse across parquet files or versions.
    /// The keyName parameter is ignored; a GUID is generated as the actual key name.
    /// The role tag is preserved for access control via Azure RBAC.
    /// </summary>
    /// <param name="keyName">Ignored; a fresh GUID is used instead</param>
    /// <param name="role">Role tag for access control and key organization</param>
    /// <returns>The full Key ID (URI) of the newly created key</returns>
    public async Task<string> CreateKeyAsync(string keyName, string role)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(keyName);
        ArgumentException.ThrowIfNullOrWhiteSpace(role);

        // Generate a unique GUID-based key name for this encryption key.
        // Each call creates a new key - no reuse across file versions or columns.
        var uniqueKeyName = Guid.NewGuid().ToString("N");

        // Create an RSA key in Azure KV used to encrypt/decrypt ephemeral DEKs
        var options = new CreateRsaKeyOptions(uniqueKeyName)
        {
            KeySize = 2048,
            KeyOperations = { KeyOperation.Encrypt, KeyOperation.Decrypt },
            Tags =
            {
                [RoleTagKey] = role
            }
        };

        var response = await _keyClient.CreateRsaKeyAsync(options);
        return response.Value.Id.ToString();
    }

    public async Task<byte[]> EncryptAsync(string keyName, byte[] plaintext)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(keyName);
        ArgumentNullException.ThrowIfNull(plaintext);

        // If keyName is a full URI, extract just the key name part
        var actualKeyName = ExtractKeyNameFromUri(keyName);
        var cryptoClient = _keyClient.GetCryptographyClient(actualKeyName);
        var result = await cryptoClient.EncryptAsync(EncryptionAlgorithm.RsaOaep256, plaintext);
        return result.Ciphertext;
    }

    public async Task<byte[]> DecryptAsync(string keyName, byte[] ciphertext)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(keyName);
        ArgumentNullException.ThrowIfNull(ciphertext);

        // If keyName is a full URI, extract just the key name part
        var actualKeyName = ExtractKeyNameFromUri(keyName);
        var cryptoClient = _keyClient.GetCryptographyClient(actualKeyName);
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

    /// <summary>
    /// Extract the key name from a full Key URI.
    /// URI format: https://vault-name.vault.azure.net/keys/key-name/version-id
    /// Returns just the key-name part.
    /// If the input is not a URI, returns it as-is (assumes it's already a key name).
    /// </summary>
    private static string ExtractKeyNameFromUri(string keyIdentifier)
    {
        // If it's already a simple key name (not a URI), return as-is
        if (!keyIdentifier.Contains("/"))
        {
            return keyIdentifier;
        }

        // Parse as URI: https://vault.net/keys/key-name/version
        if (Uri.TryCreate(keyIdentifier, UriKind.Absolute, out var uri))
        {
            var segments = uri.Segments;
            // Segments would be: "/", "keys/", "key-name/", "version"
            // We want the key-name segment (index 2), trimming the trailing slash
            if (segments.Length >= 3)
            {
                return segments[2].TrimEnd('/');
            }
        }

        // Fallback: return as-is
        return keyIdentifier;
    }
}
