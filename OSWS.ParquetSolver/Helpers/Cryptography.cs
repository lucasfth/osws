using System.Security.Cryptography;
using OSWS.Models.DTOs;
using OSWS.Models.Interfaces;
using ParquetSharp;

namespace OSWS.ParquetSolver.Helpers;

public static class Cryptography
{
    /// <summary>
    /// Algorithm identifier stored in metadata so the vault knows how to decrypt later.
    /// </summary>
    private const string EncryptionAlgorithm = "RSA-OAEP-256";

    /// <summary>
    /// Build encryption properties for a parquet file.
    /// An ephemeral AES-128 DEK is generated in-memory, encrypted by the vault (which holds the key),
    /// and the encrypted DEK + key reference are stored in parquet metadata. No plaintext keys are persisted.
    /// </summary>
    public static FileEncryptionProperties BuildEncryptionProperties(
        SchemaDescriptor schema,
        string[]? columnsToEncrypt,
        IKeyVaultProvider keyVaultProvider,
        string role,
        string providerType
    )
    {
        // Generate ephemeral AES-128 footer DEK (in-memory only, never persisted)
        var footerDek = RandomNumberGenerator.GetBytes(16);
        var footerKeyName = $"{role}-footer";

        // Create the encryption key in the vault and encrypt the ephemeral DEK
        var footerKeyId = keyVaultProvider
            .CreateKeyAsync(footerKeyName, role)
            .GetAwaiter()
            .GetResult();
        var encryptedFooterDek = keyVaultProvider
            .EncryptAsync(footerKeyName, footerDek)
            .GetAwaiter()
            .GetResult();

        var footerMetadata = new KeyMetadata
        {
            KeyId = footerKeyId,
            KeyName = footerKeyName,
            Role = role,
            EncryptedKey = Convert.ToBase64String(encryptedFooterDek),
            Algorithm = EncryptionAlgorithm,
            ProviderType = providerType,
        };

        using var builder = new FileEncryptionPropertiesBuilder(footerDek);
        builder.FooterKeyMetadata(footerMetadata.Serialize());
        builder.SetPlaintextFooter();

        // Generate a separate ephemeral DEK for columns
        var columnDek = RandomNumberGenerator.GetBytes(16);
        var columnKeyName = $"{role}-column";

        var columnKeyId = keyVaultProvider
            .CreateKeyAsync(columnKeyName, role)
            .GetAwaiter()
            .GetResult();
        var encryptedColumnDek = keyVaultProvider
            .EncryptAsync(columnKeyName, columnDek)
            .GetAwaiter()
            .GetResult();

        var columnMetadata = new KeyMetadata
        {
            KeyId = columnKeyId,
            KeyName = columnKeyName,
            Role = role,
            EncryptedKey = Convert.ToBase64String(encryptedColumnDek),
            Algorithm = EncryptionAlgorithm,
            ProviderType = providerType,
        };

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
            colBuilder.Key(columnDek);
            colBuilder.KeyMetadata(columnMetadata.Serialize());
            columnProperties[i] = colBuilder.Build();
        }

        var encryptedCols = Array.FindAll(columnProperties, p => p != null!);
        if (encryptedCols.Length > 0)
        {
            builder.EncryptedColumns(encryptedCols!);
        }

        return builder.Build();
    }

    /// <summary>
    /// Build decryption properties using an <see cref="IKeyVaultProvider"/> to decrypt DEKs.
    /// The vault decrypts the encrypted DEK stored in parquet metadata using the referenced key.
    /// </summary>
    public static FileDecryptionProperties BuildDecryptionProperties(
        IKeyVaultProvider keyVaultProvider
    )
    {
        using var builder = new FileDecryptionPropertiesBuilder();
        builder.KeyRetriever(new KeyRetriever(keyVaultProvider));
        return builder.Build();
    }
}
