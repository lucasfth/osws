using OSWS.Models.DTOs;
using OSWS.Models.Interfaces;
using ParquetSharp;

namespace OSWS.ParquetSolver;

/// <summary>
/// Key retriever that recovers DEKs via an <see cref="IKeyVaultProvider"/>.
/// Parquet footer metadata contains a JSON <see cref="KeyMetadata"/> with
/// the vault key identifier and the encrypted DEK. This retriever deserializes
/// the metadata and calls the provider to decrypt the DEK.
/// </summary>
public sealed class KeyRetriever(IKeyVaultProvider keyVaultProvider) : DecryptionKeyRetriever
{
    private readonly IKeyVaultProvider _keyVaultProvider = keyVaultProvider ?? throw new ArgumentNullException(nameof(keyVaultProvider));

    /// <summary>
    /// Retrieves the DEK by deserializing the Parquet footer metadata and calling the key vault provider to decrypt it.
    /// Indirectly calls the provider's DecryptAsync method, which in turn calls the vault's Decrypt API.
    /// </summary>
    /// <param name="keyMetadata"></param>
    /// <returns>DEK as byte array</returns>
    /// <exception cref="InvalidOperationException"></exception>
    public override byte[] GetKey(string keyMetadata)
    {
        var metadata =
            KeyMetadata.Deserialize(keyMetadata)
            ?? throw new InvalidOperationException(
                $"Failed to deserialize key metadata: {keyMetadata}"
            );

        var encryptedKey = Convert.FromBase64String(metadata.EncryptedKey);

        // ParquetSharp's GetKey is synchronous but IKeyVaultProvider is async.
        return _keyVaultProvider
            .DecryptAsync(metadata.KeyName, encryptedKey)
            .GetAwaiter()
            .GetResult();
    }
}
