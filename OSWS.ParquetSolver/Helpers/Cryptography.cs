using System.Diagnostics;
using System.Security.Cryptography;
using Microsoft.Extensions.Logging;
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
    public static (
        FileEncryptionProperties Properties,
        EncryptionResult Metadata
    ) BuildEncryptionProperties(
        SchemaDescriptor schema,
        string[]? columnsToEncrypt,
        IKeyVaultProvider keyVaultProvider,
        string role,
        string providerType,
        EncryptionSettings? encryptionSettings = null,
        ILogger? logger = null
    )
    {
        var sw = Stopwatch.StartNew();
        var dekSizeBytes = encryptionSettings?.GetDekSizeBytes() ?? 16; // Default 128 bits (16 bytes)

        var fileKeyName = $"{role}-file-{Guid.NewGuid():N}";

        logger?.LogDebug(
            "[Cryptography] Creating KEK in vault: keyName={KeyName}, role={Role}",
            fileKeyName, role
        );
        var kekSw = Stopwatch.StartNew();
        var fileKeyId = keyVaultProvider.CreateKeyAsync(fileKeyName, role).GetAwaiter().GetResult();
        logger?.LogDebug(
            "[Cryptography] KEK created: keyId={KeyId} ({ElapsedMs}ms)",
            fileKeyId, kekSw.ElapsedMilliseconds
        );

        var encryptedColumns = new List<EncryptedColumnInfo>();

        // Parquet still expects a footer key and key metadata even when the footer itself is plaintext.
        // Keep the footer plaintext, but wrap the footer key with the file-level KEK so readers can initialize crypto.
        var footerKey = RandomNumberGenerator.GetBytes(16);

        logger?.LogDebug("[Cryptography] Encrypting footer DEK via vault");
        var footerEncSw = Stopwatch.StartNew();
        var encryptedFooterKey = keyVaultProvider
            .EncryptAsync(fileKeyId, footerKey)
            .GetAwaiter()
            .GetResult();
        logger?.LogDebug(
            "[Cryptography] Footer DEK encrypted ({ElapsedMs}ms)",
            footerEncSw.ElapsedMilliseconds
        );

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
        var encryptedColumnCount = columnsToEncrypt?.Length ?? numColumns;
        logger?.LogDebug(
            "[Cryptography] Starting per-column DEK generation: {TotalColumns} schema columns, encrypting {EncryptedCount}",
            numColumns, encryptedColumnCount
        );

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

            logger?.LogDebug(
                "[Cryptography] Encrypting DEK for column [{ColumnIndex}/{Total}]: {ColumnName}",
                i + 1, numColumns, colName
            );
            var colEncSw = Stopwatch.StartNew();
            var encryptedColumnDek = keyVaultProvider
                .EncryptAsync(fileKeyId!, columnDek)
                .GetAwaiter()
                .GetResult();
            logger?.LogDebug(
                "[Cryptography] Column DEK encrypted: {ColumnName} ({ElapsedMs}ms)",
                colName, colEncSw.ElapsedMilliseconds
            );

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

            encryptedColumns.Add(
                new EncryptedColumnInfo
                {
                    ColumnName = colName,
                    KeyVaultId = fileKeyId,
                    KeyName = fileKeyName,
                }
            );
        }

        var encryptedCols = Array.FindAll(columnProperties, p => p != null!);
        if (encryptedCols.Length > 0)
        {
            builder.EncryptedColumns(encryptedCols!);
        }

        logger?.LogDebug(
            "[Cryptography] BuildEncryptionProperties complete: {EncryptedCols} columns encrypted, total elapsed {ElapsedMs}ms",
            encryptedCols.Length, sw.ElapsedMilliseconds
        );

        return (
            builder.Build(),
            new EncryptionResult
            {
                FileKeyVaultId = fileKeyId,
                FileKeyName = fileKeyName,
                Columns = encryptedColumns,
            }
        );
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
