using System.Text.Json;
using System.Text.Json.Serialization;

namespace OSWS.Models.DTOs;

/// <summary>
/// Serialized into parquet key-metadata strings.
/// Contains everything needed to decrypt the DEK during decryption:
/// the vault key identifier, algorithm, and the encrypted data encryption key.
/// No plaintext key material is stored - the vault holds the key.
/// </summary>
public class KeyMetadata
{
    /// <summary>
    /// Full key identifier (vault URI including version) for the encryption key
    /// held in the vault. Used to route decrypt calls to the correct key.
    /// </summary>
    public required string KeyId { get; set; }

    /// <summary>
    /// Short name of the encryption key in the vault (for display/logging).
    /// </summary>
    public required string KeyName { get; set; }

    /// <summary>
    /// The role associated with this key (for access control).
    /// </summary>
    public string? Role { get; set; }

    /// <summary>
    /// Base64-encoded encrypted data encryption key.
    /// The vault encrypts the ephemeral DEK; this ciphertext is stored in metadata.
    /// </summary>
    public required string EncryptedKey { get; set; }

    /// <summary>
    /// The algorithm the vault used to encrypt the DEK (e.g. "RSA-OAEP-256").
    /// </summary>
    public required string Algorithm { get; set; }

    /// <summary>
    /// The key vault provider type that created this key (e.g. "Azure", "Aws", "Internal").
    /// Allows the system to route to the correct provider during decryption.
    /// </summary>
    public required string ProviderType { get; set; }

    public string Serialize() =>
        JsonSerializer.Serialize(this, KeyMetadataJsonContext.Default.KeyMetadata);

    public static KeyMetadata? Deserialize(string json)
    {
        try
        {
            return JsonSerializer.Deserialize(json, KeyMetadataJsonContext.Default.KeyMetadata);
        }
        catch (JsonException)
        {
            return null;
        }
    }
}

[JsonSerializable(typeof(KeyMetadata))]
internal partial class KeyMetadataJsonContext : JsonSerializerContext;
