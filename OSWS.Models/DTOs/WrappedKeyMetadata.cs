using System.Text.Json;
using System.Text.Json.Serialization;

namespace OSWS.Models.DTOs;

/// <summary>
/// Serialized into the parquet footer key-metadata string.
/// Contains everything needed to unwrap the DEK during decryption:
/// the vault key name and the wrapped (encrypted) data encryption key.
/// </summary>
public class WrappedKeyMetadata
{
    /// <summary>
    /// Name of the KEK in the key vault used to wrap this DEK.
    /// </summary>
    public required string KeyName { get; set; }

    /// <summary>
    /// The role associated with this key (for access control).
    /// </summary>
    public string? Role { get; set; }

    /// <summary>
    /// Base64-encoded wrapped (encrypted) data encryption key.
    /// </summary>
    public required string WrappedKey { get; set; }

    /// <summary>
    /// The key vault provider type that created this key (e.g. "Azure", "Aws", "Internal").
    /// Allows the system to route to the correct provider during decryption.
    /// </summary>
    public required string ProviderType { get; set; }

    public string Serialize() =>
        JsonSerializer.Serialize(this, WrappedKeyMetadataJsonContext.Default.WrappedKeyMetadata);

    public static WrappedKeyMetadata? Deserialize(string json)
    {
        try
        {
            return JsonSerializer.Deserialize(json, WrappedKeyMetadataJsonContext.Default.WrappedKeyMetadata);
        }
        catch (JsonException)
        {
            return null;
        }
    }
}

[JsonSerializable(typeof(WrappedKeyMetadata))]
internal partial class WrappedKeyMetadataJsonContext : JsonSerializerContext;
