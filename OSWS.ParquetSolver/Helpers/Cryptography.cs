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
        // Use KeyId (full URI with GUID) not KeyName (formatted name) since keys are created with GUID-based names
        var encryptedFooterDek = keyVaultProvider
            .EncryptAsync(footerKeyId, footerDek)
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

        // Each column gets its own unique ephemeral DEK and KEK in Azure Key Vault
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

            // Generate a unique ephemeral AES-128 DEK for this specific column (in-memory only, never persisted)
            var columnDek = RandomNumberGenerator.GetBytes(16);
            var columnKeyName = $"{role}-column-{colName}";

            // Create a unique KEK in Azure Key Vault for this column and encrypt the ephemeral DEK
            var columnKeyId = keyVaultProvider
                .CreateKeyAsync(columnKeyName, role)
                .GetAwaiter()
                .GetResult();
            // Use KeyId (full URI with GUID) not KeyName (formatted name) since keys are created with GUID-based names
            var encryptedColumnDek = keyVaultProvider
                .EncryptAsync(columnKeyId, columnDek)
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
    /// Build decryption properties using an <see cref="IKeyVaultProvider"/> and a <see cref="DekCache"/> to decrypt DEKs.
    /// The vault decrypts the encrypted DEK stored in parquet metadata using the referenced key.
    /// Decrypted DEKs are cached to avoid repeated vault calls for the same key.
    /// </summary>
    public static FileDecryptionProperties BuildDecryptionProperties(
        IKeyVaultProvider keyVaultProvider,
        DekCache dekCache
    )
    {
        using var builder = new FileDecryptionPropertiesBuilder();
        builder.KeyRetriever(new KeyRetriever(keyVaultProvider, dekCache));
        return builder.Build();
    }
}
