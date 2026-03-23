using System.Security.Cryptography;
using OSWS.Common.Configuration;
using OSWS.Models.DTOs;
using OSWS.Models.Interfaces;
using OSWS.ParquetSolver.Interfaces;
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
    /// The parquet footer remains plaintext.
    /// Each encrypted column gets its own ephemeral DEK, and all column DEKs for the file
    /// are wrapped by a single file-level KEK created through the vault provider.
    /// </summary>
    public static FileEncryptionProperties BuildEncryptionProperties(
        SchemaDescriptor schema,
        string[]? columnsToEncrypt,
        IKeyVaultProvider keyVaultProvider,
        string role,
        string providerType,
        EncryptionSettings? encryptionSettings = null
    )
    {
        var dekSizeBytes = encryptionSettings?.GetDekSizeBytes() ?? 16; // Default 128 bits (16 bytes)

        var fileKeyName = $"{role}-file-{Guid.NewGuid():N}";
        var fileKeyId = keyVaultProvider.CreateKeyAsync(fileKeyName, role).GetAwaiter().GetResult();

        // Parquet still expects a footer key and key metadata even when the footer itself is plaintext.
        // Keep the footer plaintext, but wrap the footer key with the file-level KEK so readers can initialize crypto.
        var footerKey = RandomNumberGenerator.GetBytes(16);
        var encryptedFooterKey = keyVaultProvider
            .EncryptAsync(fileKeyId, footerKey)
            .GetAwaiter()
            .GetResult();

        var footerMetadata = new KeyMetadata
        {
            KeyId = fileKeyId,
            KeyName = fileKeyName,
            Role = role,
            EncryptedKey = Convert.ToBase64String(encryptedFooterKey),
            Algorithm = EncryptionAlgorithm,
            ProviderType = providerType,
        };

        using var builder = new FileEncryptionPropertiesBuilder(footerKey);
        builder.FooterKeyMetadata(footerMetadata.Serialize());
        builder.SetPlaintextFooter();

        // Each encrypted column gets its own DEK, but every DEK in the file is wrapped by one KEK.
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

            // Generate a unique ephemeral DEK for this specific column (in-memory only, never persisted)
            // DEK size is configurable via EncryptionSettings
            var columnDek = RandomNumberGenerator.GetBytes(dekSizeBytes);

            var encryptedColumnDek = keyVaultProvider
                .EncryptAsync(fileKeyId!, columnDek)
                .GetAwaiter()
                .GetResult();

            var columnMetadata = new KeyMetadata
            {
                KeyId = fileKeyId!,
                KeyName = fileKeyName!,
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
    /// The vault decrypts the encrypted DEK stored in parquet column metadata using the referenced key.
    /// Decrypted DEKs are cached to avoid repeated vault calls for the same key.
    /// </summary>
    public static FileDecryptionProperties BuildDecryptionProperties(
        IKeyVaultProvider keyVaultProvider,
        IDekCache dekCache,
        Action<TimeSpan>? onExternalKvOperationLatency = null,
        Action<TimeSpan>? onCachedKvOperationLatency = null
    )
    {
        using var builder = new FileDecryptionPropertiesBuilder();
        builder.KeyRetriever(
            new KeyRetriever(
                keyVaultProvider,
                dekCache,
                onExternalKvOperationLatency,
                onCachedKvOperationLatency
            )
        );
        return builder.Build();
    }
}
