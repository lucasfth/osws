using ParquetSharp;

namespace OSWS.ParquetSolver;

/// <summary>
/// Key retriever that returns decryption keys based on key metadata stored in the parquet footer.
/// TODO - change out for communication with KMS
/// </summary>
public sealed class KeyRetriever : DecryptionKeyRetriever
{
    public override byte[] GetKey(string keyMetadata)
    {
        return keyMetadata switch
        {
            DummyCryptoParameters.FooterKeyMetadata => DummyCryptoParameters.FooterKey,
            DummyCryptoParameters.ColumnKeyMetadata => DummyCryptoParameters.ColumnKey,
            _ => throw new InvalidOperationException($"Unknown key metadata: {keyMetadata}"),
        };
    }
}
