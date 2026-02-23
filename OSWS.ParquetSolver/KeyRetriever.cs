using OSWS.Models.DTOs;
using OSWS.Models.Interfaces;
using ParquetSharp;

namespace OSWS.ParquetSolver;

/// <summary>
/// Key retriever that unwraps DEKs via an <see cref="IKeyVaultProvider"/>.
/// Parquet footer metadata contains a JSON <see cref="WrappedKeyMetadata"/> with
/// the vault key name and wrapped DEK. This retriever deserializes it and calls
/// the provider to unwrap the actual AES key.
/// </summary>
public sealed class KeyRetriever : DecryptionKeyRetriever
{
    private readonly IKeyVaultProvider _keyVaultProvider;

    public KeyRetriever(IKeyVaultProvider keyVaultProvider)
    {
        _keyVaultProvider = keyVaultProvider ?? throw new ArgumentNullException(nameof(keyVaultProvider));
    }

    public override byte[] GetKey(string keyMetadata)
    {
        var metadata = WrappedKeyMetadata.Deserialize(keyMetadata)
            ?? throw new InvalidOperationException($"Failed to deserialize key metadata: {keyMetadata}");

        var wrappedKey = Convert.FromBase64String(metadata.WrappedKey);

        // ParquetSharp's GetKey is synchronous but IKeyVaultProvider is async.
        // Bridge with GetAwaiter().GetResult()
        return _keyVaultProvider.UnwrapKeyAsync(metadata.KeyName, wrappedKey)
            .GetAwaiter().GetResult();
    }
}
