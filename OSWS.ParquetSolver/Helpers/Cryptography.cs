using System.Security.Cryptography;
using OSWS.Models.DTOs;
using OSWS.Models.Interfaces;
using ParquetSharp;

namespace OSWS.ParquetSolver.Helpers;

public static class Cryptography
{
    /// <summary>
    /// Build encryption properties using envelope encryption via an <see cref="IKeyVaultProvider"/>.
    /// A random AES-128 DEK is generated locally, wrapped by the vault, and stored in parquet key metadata.
    /// </summary>
    public static FileEncryptionProperties BuildEncryptionProperties(
        SchemaDescriptor schema,
        string[]? columnsToEncrypt,
        IKeyVaultProvider keyVaultProvider,
        string role,
        string providerType)
    {
        // Generate random AES-128 footer DEK
        var footerDek = RandomNumberGenerator.GetBytes(16);
        var footerKeyName = $"{role}-footer";

        // Ensure the KEK exists in the vault, then wrap the footer DEK
        keyVaultProvider.CreateKeyAsync(footerKeyName, role).GetAwaiter().GetResult();
        var wrappedFooterDek = keyVaultProvider.WrapKeyAsync(footerKeyName, footerDek).GetAwaiter().GetResult();

        var footerMetadata = new WrappedKeyMetadata
        {
            KeyName = footerKeyName,
            Role = role,
            WrappedKey = Convert.ToBase64String(wrappedFooterDek),
            ProviderType = providerType,
        };

        using var builder = new FileEncryptionPropertiesBuilder(footerDek);
        builder.FooterKeyMetadata(footerMetadata.Serialize());
        builder.SetPlaintextFooter();

        // Generate a separate DEK for columns
        var columnDek = RandomNumberGenerator.GetBytes(16);
        var columnKeyName = $"{role}-column";

        keyVaultProvider.CreateKeyAsync(columnKeyName, role).GetAwaiter().GetResult();
        var wrappedColumnDek = keyVaultProvider.WrapKeyAsync(columnKeyName, columnDek).GetAwaiter().GetResult();

        var columnMetadata = new WrappedKeyMetadata
        {
            KeyName = columnKeyName,
            Role = role,
            WrappedKey = Convert.ToBase64String(wrappedColumnDek),
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
    /// Build decryption properties using an <see cref="IKeyVaultProvider"/> to unwrap DEKs.
    /// </summary>
    public static FileDecryptionProperties BuildDecryptionProperties(IKeyVaultProvider keyVaultProvider)
    {
        using var builder = new FileDecryptionPropertiesBuilder();
        builder.KeyRetriever(new KeyRetriever(keyVaultProvider));
        return builder.Build();
    }
}
