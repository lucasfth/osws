using System.Security.Claims;
using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.EntityFrameworkCore;
using OSWS.KeyManager.Persistence;
using OSWS.Library;
using OSWS.Library.Helpers;
using OSWS.Models.DTOs;
using OSWS.Models.Entities;
using OSWS.ParquetSolver.Helpers;
using OSWS.ParquetSolver.Interfaces;
using OSWS.WebApi.Interfaces;
using OSWS.WebApi.Services;

namespace OSWS.WebApi.Endpoints;

public class S3Put(
    IAmazonS3 s3Client,
    IParquetWriter parquetWriter,
    EncryptedFileCache fileCache,
    CurrentUser currentUser,
    OswsContext db
) : IS3Put
{
    public async Task<IResult> PutObject(
        string bucket,
        string? key,
        Params prms,
        HttpRequest httpRequest,
        int retryOptions = 3,
        int timeoutOptionsMs = 3000,
        CancellationToken cancellationToken = default
    )
    {
        var user = await currentUser.ResolveAsync(cancellationToken);
        if (user is null)
            return Results.Unauthorized();

        if (string.IsNullOrEmpty(bucket))
        {
            httpRequest.HttpContext.Response.StatusCode = 400;
            return Results.Text(ParamValidation.BucketNameIsRequired(), "application/json");
        }

        if (string.IsNullOrEmpty(key))
        {
            httpRequest.HttpContext.Response.StatusCode = 400;
            return Results.Text(ParamValidation.KeyIsRequired(), "application/json");
        }

        var isParquetFile = TypeCheck.IsParquetFile(key, httpRequest.ContentType);

        var req = new PutObjectRequest
        {
            BucketName = bucket,
            Key = key,
            ContentType = httpRequest.ContentType ?? "application/octet-stream",
            UseChunkEncoding = false,
        };

        string? tempFile = null;
        FileStream? tempFs = null;

        // Encrypt parquet files before uploading to S3
        if (isParquetFile)
        {
            MemoryStream? seekableStream = null;
            try
            {
                // for now just take the first role, need to figure out how to do this nicely
                var role = user.Roles.FirstOrDefault();
                if (role is null)
                {
                    httpRequest.HttpContext.Response.StatusCode = 403;
                    return Results.Text(
                        ParamValidation.CreateErrorJson("User has no roles assigned"),
                        "application/json"
                    );
                }

                // Copy to MemoryStream to make it seekable for Parquet library
                seekableStream = new MemoryStream();
                await httpRequest.Body.CopyToAsync(seekableStream, cancellationToken);
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

                // Ensure stream is at position 0 and set content length
                uploadStream.Position = 0;

                // Cache the encrypted parquet stream asynchronously
                // Create a copy for caching to avoid stream position conflicts with S3 upload
                var cacheKey = EncryptedFileCache.GenerateCacheKey(bucket, key);
                var cacheStream = new MemoryStream();
                uploadStream.Position = 0;
                await uploadStream.CopyToAsync(cacheStream, cancellationToken);
                cacheStream.Position = 0;
                uploadStream.Position = 0;

                // Cache asynchronously (don't await to avoid blocking the upload)
                _ = fileCache
                    .SetAsync(cacheKey, cacheStream, cancellationToken)
                    .ContinueWith(
                        task =>
                        {
                            if (task.IsFaulted)
                            {
                                // Log cache failure but don't block the upload
                                Console.WriteLine(
                                    $"[OSWS] Cache failure for {cacheKey}: {task.Exception?.InnerException?.Message}"
                                );
                            }
                            cacheStream.Dispose();
                        },
                        TaskScheduler.Default
                    );

                // For parquet files, we already have a seekable stream, so set it directly
                req.InputStream = uploadStream;

                // Set content length using reflection (ContentLength property)
                var contentLengthProp = typeof(PutObjectRequest).GetProperty("ContentLength");
                if (contentLengthProp != null && contentLengthProp.CanWrite)
                {
                    contentLengthProp.SetValue(req, uploadStream.Length);
                }
            }
            catch (Exception ex)
            {
                httpRequest.HttpContext.Response.StatusCode = 400;
                return Results.Text(
                    ParamValidation.CreateErrorJson(
                        $"Failed to encrypt parquet file: {ex.Message}"
                    ),
                    "application/json"
                );
            }
            finally
            {
                seekableStream?.DisposeAsync();
            }
        }
        else
        {
            // For non-parquet files, use the standard buffering flow
            req.InputStream = httpRequest.Body;
        }

        try
        {
            // Only call PreparePutRequestAsync for non-parquet files
            if (!isParquetFile)
            {
                var forceBuf = ConfigHelper.GetForceUploadBuffering();
                var prep = await HttpHeaderHelper
                    .PreparePutRequestAsync(req, httpRequest, forceBuf, cancellationToken)
                    .ConfigureAwait(false);
                if (prep.IsError)
                {
                    httpRequest.HttpContext.Response.StatusCode = prep.StatusCode;
                    return Results.Text(
                        prep.ErrorJson ?? "{\"error\":\"Upload error\"}",
                        "application/json"
                    );
                }

                tempFile = prep.TempFile;
                tempFs = prep.TempFileStream;
            }
            else
            {
                // For parquet files, handle metadata headers and content length ourselves
                foreach (var h in httpRequest.Headers)
                {
                    var hn = h.Key;
                    if (hn.StartsWith("x-amz-meta-", StringComparison.OrdinalIgnoreCase))
                    {
                        var metaKey = hn.Substring("x-amz-meta-".Length);
                        req.Metadata[metaKey] = h.Value.ToString();
                    }
                }
            }
        }
        catch
        {
            if (tempFs == null)
                throw;
            await tempFs.DisposeAsync().ConfigureAwait(false);
            if (tempFile != null && File.Exists(tempFile))
                File.Delete(tempFile);
            throw;
        }

        PutObjectResponse resp;
        try
        {
            resp = await s3Client.PutObjectAsync(req, cancellationToken).ConfigureAwait(false);
        }
        catch (AmazonS3Exception e)
        {
            S3ErrorHelper.AddBufferingDebugHeaders(httpRequest.HttpContext, tempFile);
            return S3ErrorHelper.HandleS3Exception(e, httpRequest.HttpContext);
        }
        finally
        {
            try
            {
                // Clean up encrypted parquet stream
                if (isParquetFile && req.InputStream != null)
                {
                    await req.InputStream.DisposeAsync().ConfigureAwait(false);
                }

                if (tempFs != null)
                {
                    await tempFs.DisposeAsync().ConfigureAwait(false);
                }

                if (!string.IsNullOrEmpty(tempFile) && File.Exists(tempFile))
                {
                    File.Delete(tempFile);
                }
            }
            catch
            {
                // ignored
            }
        }

        // Forward metadata headers should probably be moved into httpheaderhelper
        await HttpHeaderHelper.AppendS3ETag(resp, httpRequest);

        S3ErrorHelper.AddBufferingDebugHeaders(httpRequest.HttpContext, tempFile);

        var successJson =
            "{"
            + "\"etag\":"
            + (
                resp.ETag == null
                    ? "null"
                    : "\"" + ParamValidation.JsonEscape(resp.ETag).Trim('"') + "\""
            )
            + ","
            + "\"versionId\":"
            + (
                resp.VersionId == null
                    ? "null"
                    : "\"" + ParamValidation.JsonEscape(resp.VersionId).Trim('"') + "\""
            )
            + "}";
        return Results.Text(successJson, "application/json");
    }
}
