using Microsoft.EntityFrameworkCore;
using OSWS.KeyManager.Persistence;
using OSWS.Models.Entities;
using OSWS.ParquetSolver.Helpers;
using OSWS.ParquetSolver.Interfaces;

namespace OSWS.WebApi.Services;

public class ParquetUploadService(
    IParquetWriter parquetWriter,
    EncryptedFileCache fileCache,
    OswsContext db
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
        var seekableStream = new MemoryStream();
        await requestBody.CopyToAsync(seekableStream, cancellationToken);
        seekableStream.Position = 0;

        var (uploadStream, encryptionResult) = await parquetWriter.WriteParquetAsync(
            seekableStream,
            role.Name
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

        await db.SaveChangesAsync(cancellationToken);

        // Cache the encrypted stream asynchronously (don't await to avoid blocking the upload)
        uploadStream.Position = 0;
        var cacheKey = EncryptedFileCache.GenerateCacheKey(bucket, key);
        var cacheStream = new MemoryStream();
        await uploadStream.CopyToAsync(cacheStream, cancellationToken);
        cacheStream.Position = 0;
        uploadStream.Position = 0;

        // TODO: replace with ILogger<ParquetUploadService>
        _ = fileCache
            .SetAsync(cacheKey, cacheStream, cancellationToken)
            .ContinueWith(
                task =>
                {
                    if (task.IsFaulted)
                    {
                        Console.WriteLine(
                            $"[OSWS] Cache failure for {cacheKey}: {task.Exception?.InnerException?.Message}"
                        );
                    }
                    cacheStream.Dispose();
                },
                TaskScheduler.Default
            );

        return uploadStream;
    }
}
