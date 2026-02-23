namespace OSWS.Models.Interfaces;

/// <summary>
/// Provider-agnostic interface for key management and envelope encryption.
/// Implementations can target Azure Key Vault, AWS KMS, Cloudflare, or an internal KMS.
/// </summary>
public interface IKeyVaultProvider
{
    /// <summary>
    /// Create (or ensure existence of) a key encryption key (KEK) in the vault.
    /// The key is logically associated with the given role for access control.
    /// </summary>
    /// <param name="keyName">Unique name for the key (e.g. "role-admin-footer").</param>
    /// <param name="role">The role that owns/can access this key.</param>
    /// <returns>The key identifier (vault URI or name) for later reference.</returns>
    Task<string> CreateKeyAsync(string keyName, string role);

    /// <summary>
    /// Wrap (encrypt) a locally-generated data encryption key (DEK) using
    /// the named KEK stored in the vault. Used during parquet encryption.
    /// </summary>
    Task<byte[]> WrapKeyAsync(string keyName, byte[] plainKey);

    /// <summary>
    /// Unwrap (decrypt) a previously wrapped DEK using the named KEK.
    /// Used during parquet decryption to recover the original DEK.
    /// </summary>
    Task<byte[]> UnwrapKeyAsync(string keyName, byte[] wrappedKey);

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
