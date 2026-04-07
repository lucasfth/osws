using System.Diagnostics;
using OSWS.Models.DTOs;
using OSWS.Models.Interfaces;
using OSWS.ParquetSolver.Helpers;
using OSWS.ParquetSolver.Interfaces;
using ParquetSharp;

namespace OSWS.ParquetSolver;

/// <summary>
/// Key retriever that recovers DEKs via an <see cref="IKeyVaultProvider"/>.
/// Parquet key metadata contains a JSON <see cref="KeyMetadata"/> with
/// the vault key identifier and the encrypted DEK. This retriever deserializes
/// the metadata and calls the provider to decrypt the DEK.
///
/// Decrypted DEKs are cached by a composite key derived from the KEK ID and encrypted DEK to avoid
/// repeated calls to Azure Key Vault for the same key.
/// </summary>
public sealed class KeyRetriever(
    IKeyVaultProvider keyVaultProvider,
    IDekCache dekCache,
    Action<TimeSpan>? onInternalKvOperationLatency = null,
    Action<TimeSpan>? onCachedKvOperationLatency = null
) : DecryptionKeyRetriever
{
    /// <summary>
    /// Retrieves the DEK by deserializing parquet key metadata and calling the key vault provider to decrypt it.
    /// Checks the cache first; if the DEK has been decrypted before, returns the cached value.
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

        // A provider key (KEK) can encrypt many different DEKs over time (across files/columns).
        // Include encrypted DEK identity in the cache key so we never return a stale DEK.
        var dekCacheKey = $"{metadata.KeyId}:{metadata.EncryptedKey}";

        Stopwatch? localKeyStopWatch = null;
        if (onCachedKvOperationLatency is not null)
            localKeyStopWatch = Stopwatch.StartNew();

        // Try to retrieve from cache first.
        if (dekCache.TryGet(dekCacheKey, out var cachedDek))
        {
            if (onCachedKvOperationLatency is null)
                return cachedDek!;

            localKeyStopWatch?.Stop();
            onCachedKvOperationLatency?.Invoke(localKeyStopWatch!.Elapsed);
            return cachedDek!;
        }

        var encryptedKey = Convert.FromBase64String(metadata.EncryptedKey);

        // ParquetSharp's GetKey is synchronous but IKeyVaultProvider is async.
        // Use KeyId (full URI with GUID) not KeyName (formatted name) since keys are created with GUID-based names
        Stopwatch? remoteKeyStopWatch = null;
        if (onInternalKvOperationLatency is not null)
            remoteKeyStopWatch = Stopwatch.StartNew();
        var decryptedDek = keyVaultProvider
            .DecryptAsync(metadata.KeyId, encryptedKey)
            .GetAwaiter()
            .GetResult();

        remoteKeyStopWatch?.Stop();
        if (remoteKeyStopWatch is not null)
            onInternalKvOperationLatency?.Invoke(remoteKeyStopWatch.Elapsed);

        // Cache the decrypted DEK for future use
        dekCache.Set(dekCacheKey, decryptedDek);

        return decryptedDek;
    }
}
