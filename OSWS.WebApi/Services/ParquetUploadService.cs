using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using OSWS.KeyManager.Persistence;
using OSWS.Models.Entities;
using OSWS.ParquetSolver.Helpers;
using OSWS.ParquetSolver.Interfaces;

namespace OSWS.WebApi.Services;

public class ParquetUploadService(
    IParquetWriter parquetWriter,
    DecryptedParquetCache plaintextCache,
    OswsContext db,
    ILogger<ParquetUploadService> logger
)
{
    public async Task<Stream> ProcessAsync(
        Stream requestBody,
        Role role,
        string bucket,
        string key,
        CancellationToken cancellationToken = default
    )
    {
        // Copy to MemoryStream to make it seekable for Parquet library
        logger.LogDebug("[ParquetUploadService] Copying request body to seekable stream");
        var copySw = Stopwatch.StartNew();
        var seekableStream = new MemoryStream();
        await requestBody.CopyToAsync(seekableStream, cancellationToken);
        seekableStream.Position = 0;
        logger.LogDebug(
            "[ParquetUploadService] Request body copied: {SizeBytes} bytes ({ElapsedMs}ms)",
            seekableStream.Length,
            copySw.ElapsedMilliseconds
        );

        logger.LogDebug(
            "[ParquetUploadService] Starting WriteParquetAsync for role={Role}",
            role.Name
        );
        var encSw = Stopwatch.StartNew();
        var (uploadStream, encryptionResult) = await parquetWriter.WriteParquetAsync(
            seekableStream,
            role.Name
        );
        logger.LogDebug(
            "[ParquetUploadService] WriteParquetAsync done: {EncryptedColumns} columns encrypted ({ElapsedMs}ms)",
            encryptionResult.Columns.Count,
            encSw.ElapsedMilliseconds
        );

        // Persist column, key, and permission records
        foreach (var colInfo in encryptionResult.Columns)
        {
            var column = await db.Columns.FirstOrDefaultAsync(
                c => c.Name == colInfo.ColumnName,
                cancellationToken
            );

            if (column is null)
            {
                column = new Column { Name = colInfo.ColumnName };
                db.Columns.Add(column);
            }

            db.Keys.Add(
                new Key
                {
                    Name = colInfo.KeyName,
                    KeyVaultId = colInfo.KeyVaultId,
                    Column = column,
                }
            );

            var permissionExists = await db.Permissions.AnyAsync(
                p => p.RoleId == role.Id && p.Column == column,
                cancellationToken
            );

            if (!permissionExists)
            {
                db.Permissions.Add(new Permission { Role = role, Column = column });
            }
        }

        logger.LogDebug("[ParquetUploadService] Persisting encryption metadata to DB");
        var dbSw = Stopwatch.StartNew();
        await db.SaveChangesAsync(cancellationToken);
        logger.LogDebug(
            "[ParquetUploadService] DB save done ({ElapsedMs}ms)",
            dbSw.ElapsedMilliseconds
        );

        // Cache the original plaintext (pre-encryption) so HEAD/GET can serve it without key vault
        seekableStream.Position = 0;
        var cacheKey = DecryptedParquetCache.GenerateCacheKey(bucket, key);
        plaintextCache.Set(cacheKey, seekableStream.ToArray());
        logger.LogDebug("[ParquetUploadService] Plaintext cached for {CacheKey}", cacheKey);

        return uploadStream;
    }
}
