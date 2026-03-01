using OSWS.Models.DTOs;
using OSWS.Models.Interfaces;
using OSWS.ParquetSolver.Helpers;
using ParquetSharp;

namespace OSWS.ParquetSolver;

/// <summary>
/// Key retriever that recovers DEKs via an <see cref="IKeyVaultProvider"/>.
/// Parquet footer metadata contains a JSON <see cref="KeyMetadata"/> with
/// the vault key identifier and the encrypted DEK. This retriever deserializes
/// the metadata and calls the provider to decrypt the DEK.
///
/// Decrypted DEKs are cached by their unique Key Encryption Key (KEK) ID to avoid
/// repeated calls to Azure Key Vault for the same key.
/// </summary>
public sealed class KeyRetriever(IKeyVaultProvider keyVaultProvider, DekCache dekCache) : DecryptionKeyRetriever
{
    private readonly IKeyVaultProvider _keyVaultProvider =
        keyVaultProvider ?? throw new ArgumentNullException(nameof(keyVaultProvider));
    private readonly DekCache _dekCache =
        dekCache ?? throw new ArgumentNullException(nameof(dekCache));

    /// <summary>
    /// Retrieves the DEK by deserializing the Parquet footer metadata and calling the key vault provider to decrypt it.
    /// Checks the cache first; if the DEK has been decrypted before (by KEK ID), returns the cached value.
    /// Otherwise, calls the provider's DecryptAsync method, caches the result, and returns it.
    /// </summary>
    /// <param name="keyMetadata">JSON-serialized KeyMetadata containing KEK ID and encrypted DEK</param>
    /// <returns>DEK as byte array</returns>
    /// <exception cref="InvalidOperationException">If metadata deserialization fails</exception>
    public override byte[] GetKey(string keyMetadata)
    {
        var metadata =
            KeyMetadata.Deserialize(keyMetadata)
            ?? throw new InvalidOperationException(
                $"Failed to deserialize key metadata: {keyMetadata}"
            );

        // Try to retrieve from cache first using the KEK ID
        if (_dekCache.TryGet(metadata.KeyId, out var cachedDek))
        {
            return cachedDek!;
        }

        var encryptedKey = Convert.FromBase64String(metadata.EncryptedKey);

        // ParquetSharp's GetKey is synchronous but IKeyVaultProvider is async.
        // Use KeyId (full URI with GUID) not KeyName (formatted name) since keys are created with GUID-based names
        var decryptedDek = _keyVaultProvider
            .DecryptAsync(metadata.KeyId, encryptedKey)
            .GetAwaiter()
            .GetResult();

        // Cache the decrypted DEK for future use
        _dekCache.Set(metadata.KeyId, decryptedDek);

        return decryptedDek;
    }
}
