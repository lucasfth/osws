using ParquetSharp;

namespace OSWS.ParquetSolver.Helpers;

public static class Cryptography
{
    /// <summary>
    /// Builds FileEncryptionProperties for Parquet Modular Encryption, encrypting specified columns (or all if null).
    /// </summary>
    /// <param name="schema"></param>
    /// <param name="columnsToEncrypt"></param>
    /// <returns></returns>
    public static FileEncryptionProperties BuildEncryptionProperties(
        SchemaDescriptor schema,
        string[]? columnsToEncrypt
    )
    {
        using var builder = new FileEncryptionPropertiesBuilder(DummyCryptoParameters.FooterKey);
        builder.FooterKeyMetadata(DummyCryptoParameters.FooterKeyMetadata);
        builder.SetPlaintextFooter(); // Keep footer readable; only encrypt column data

        // Build column encryption for specified columns (or all if null)
        var numColumns = schema.NumColumns;
        var columnProperties = new ColumnEncryptionProperties[numColumns];

        for (var i = 0; i < numColumns; i++)
        {
            var colName = schema.Column(i).Name;
            var shouldEncrypt =
                columnsToEncrypt == null
                || Array.Exists(
                    columnsToEncrypt,
                    c => string.Equals(c, colName, StringComparison.OrdinalIgnoreCase)
                );

            if (!shouldEncrypt)
                continue;

            using var colBuilder = new ColumnEncryptionPropertiesBuilder(colName);
            colBuilder.Key(DummyCryptoParameters.ColumnKey);
            colBuilder.KeyMetadata(DummyCryptoParameters.ColumnKeyMetadata);
            columnProperties[i] = colBuilder.Build();
        }

        // Filter out nulls (unencrypted columns)
        var encryptedCols = Array.FindAll(columnProperties, p => true);
        if (encryptedCols.Length > 0)
        {
            builder.EncryptedColumns(encryptedCols!);
        }

        return builder.Build();
    }

    /// <summary>
    /// Builds FileDecryptionProperties for Parquet Modular Decryption, using the same keys as encryption.
    /// </summary>
    /// <returns></returns>
    public static FileDecryptionProperties BuildDecryptionProperties()
    {
        using var builder = new FileDecryptionPropertiesBuilder();
        builder.KeyRetriever(new KeyRetriever());
        return builder.Build();
    }
}
