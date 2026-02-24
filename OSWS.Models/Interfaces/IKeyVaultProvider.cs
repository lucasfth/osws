namespace OSWS.Models.Interfaces;

/// <summary>
/// Provider-agnostic interface for key management and encryption-at-rest.
/// Implementations can target Azure Key Vault, AWS KMS, Cloudflare, or an internal KMS.
/// The vault holds the data encryption key (DEK); no keys are stored locally.
/// </summary>
public interface IKeyVaultProvider
{
    /// <summary>
    /// Create (or ensure existence of) a data encryption key (DEK) in the vault.
    /// The key is logically associated with the given role for access control.
    /// </summary>
    /// <param name="keyName">Unique name for the key (e.g. "role-admin-footer").</param>
    /// <param name="role">The role that owns/can access this key.</param>
    /// <returns>The full key identifier (vault URI including version) for later reference.</returns>
    Task<string> CreateKeyAsync(string keyName, string role);

    /// <summary>
    /// Encrypt plaintext bytes using the named key held in the vault.
    /// Used during parquet encryption to protect an ephemeral DEK.
    /// </summary>
    Task<byte[]> EncryptAsync(string keyName, byte[] plaintext);

    /// <summary>
    /// Decrypt ciphertext bytes using the named key held in the vault.
    /// Used during parquet decryption to recover the original DEK.
    /// </summary>
    Task<byte[]> DecryptAsync(string keyName, byte[] ciphertext);

    /// <summary>
    /// Retrieve metadata about a key in the vault.
    /// Returns null if the key does not exist.
    /// </summary>
    Task<KeyVaultKeyInfo?> GetKeyInfoAsync(string keyName);

    /// <summary>
    /// List keys, optionally filtered by role.
    /// </summary>
    Task<IReadOnlyList<KeyVaultKeyInfo>> ListKeysAsync(string? role = null);
}

/// <summary>
/// Metadata about a key stored in the vault.
/// </summary>
public class KeyVaultKeyInfo
{
    public required string KeyName { get; set; }
    public required string KeyId { get; set; }
    public string? Role { get; set; }
    public DateTimeOffset? CreatedOn { get; set; }
    public DateTimeOffset? ExpiresOn { get; set; }
    public bool Enabled { get; set; } = true;
}
