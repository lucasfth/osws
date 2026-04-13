using System.Security.Claims;
using OSWS.WebApi.Helpers;
using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.EntityFrameworkCore;
using OSWS.KeyManager.Persistence;
using OSWS.Library.Helpers;
using OSWS.Models.DTOs;
using OSWS.Models.Entities;
using OSWS.ParquetSolver.Helpers;
using OSWS.ParquetSolver.Interfaces;
using OSWS.WebApi.Interfaces;
using OSWS.WebApi.Services;

namespace OSWS.WebApi.Endpoints;

public class S3Get(
    IAmazonS3 s3Client,
    IParquetReader parquetReader,
    S3ObjectFetcher objectFetcher,
    CurrentUser currentUser,
    PermissionService permissionService,
    ILogger<S3Get> logger,
    IWebHostEnvironment env
) : IS3Get
{
    public async Task<IResult> GetObject(
        string bucket,
        string key,
        Params prms,
        HttpRequest httpRequest,
        HttpResponse httpResponse,
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

        // Build GetObjectRequest now (we may add range)
        var req = new GetObjectRequest
        {
            BucketName = bucket,
            Key = key,
            // VersionId = string.IsNullOrEmpty(prms.Version) ? null : prms.Version,
        };

        var rangeSpec = await RangeHelper.ParseRange(httpRequest);
        if (rangeSpec.IsInvalidSpec)
        {
            return Results.StatusCode(400);
        }

        // Due to how OSWS handles encryption, we cannot set byte-range on the S3 request, as we may need to fetch the full object to decrypt before slicing.
        // Instead, we'll handle range slicing in-memory after fetching (and potentially decrypting) the full object.
        // This allows us to support range requests even for encrypted objects without needing to know the content length or encryption details upfront.

        // Check cache first for encrypted parquet files
        var cacheKey = EncryptedFileCache.GenerateCacheKey(bucket, key);
        var isParquetFile = TypeCheck.IsParquetFile(key, httpRequest.ContentType);
        GetObjectResponse? resp = null;
        Stream? encryptedStream = null;

        if (isParquetFile)
        {
            try
            {
                var fetchResult = await objectFetcher.FetchParquetAsync(bucket, key, cancellationToken);
                encryptedStream = fetchResult.EncryptedStream;
                resp = fetchResult.S3Response;
            }
            catch (AmazonS3Exception e)
            {
                return S3ErrorHelper.HandleS3Exception(e, httpRequest.HttpContext);
            }
        }
        else
        {
            try
            {
                resp = await objectFetcher.FetchObjectAsync(bucket, key, cancellationToken);
            }
            catch (AmazonS3Exception e)
            {
                return S3ErrorHelper.HandleS3Exception(e, httpRequest.HttpContext);
            }
        }

        // Decrypt parquet files
        var outputStream = resp?.ResponseStream ?? encryptedStream;

        if (isParquetFile && encryptedStream != null)
        {
            try
            {
                var allowedColumnSet = await permissionService.GetAllowedColumnsAsync(
                    user.Id, cancellationToken);
                var roleIds = await permissionService.GetEffectiveRoleIdsAsync(
                    user.Id, cancellationToken);

                Console.WriteLine(
                    $"[OSWS] Permission check for user {user.Id}: roles=[{string.Join(",", roleIds)}], allowedColumns=[{string.Join(",", allowedColumnSet)}]"
                );

                outputStream = await parquetReader.ReadParquetAsync(
                    encryptedStream,
                    allowedColumnSet
                );
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[OSWS] Parquet decryption/permission error: {ex}");
                httpRequest.HttpContext.Response.StatusCode = 500;
                return Results.Text(
                    ParamValidation.CreateErrorJson(
                        $"Failed to decrypt parquet file: {ex.Message}"
                    ),
                    "application/json"
                );
            }
        }

        if (isParquetFile && outputStream is { CanSeek: false })
        {
            var buffered = new MemoryStream();
            await outputStream.CopyToAsync(buffered, cancellationToken).ConfigureAwait(false);
            buffered.Position = 0;
            outputStream = buffered;
        }

        var contentLength = outputStream is { CanSeek: true }
            ? outputStream.Length
            : (resp?.ContentLength ?? 0);
        if (
            rangeSpec.IsRangeRequested
            && (contentLength <= 0 || outputStream is not { CanSeek: true })
        )
        {
            // Buffer to get a reliable length and enable seeking for ranged reads.
            var buffered = new MemoryStream();
            if (outputStream != null)
            {
                await outputStream.CopyToAsync(buffered, cancellationToken).ConfigureAwait(false);
                buffered.Position = 0;
            }

            outputStream = buffered;
            contentLength = buffered.Length;
        }

        GetObjectMetadataResponse? metadataResp = null;
        if (resp == null)
        {
            try
            {
                metadataResp = await s3Client
                    .GetObjectMetadataAsync(
                        new GetObjectMetadataRequest { BucketName = bucket, Key = key },
                        cancellationToken
                    )
                    .ConfigureAwait(false);
            }
            catch (AmazonS3Exception e)
            {
                return S3ErrorHelper.HandleS3Exception(e, httpRequest.HttpContext);
            }
        }

        var responseContentType =
            !string.IsNullOrWhiteSpace(resp?.Headers?.ContentType) ? resp.Headers.ContentType
            : !string.IsNullOrWhiteSpace(metadataResp?.Headers?.ContentType)
                ? metadataResp.Headers.ContentType
            : "application/octet-stream";

        // If a range was requested, compute bounds and stream only that range using StreamRangeHelper
        if (rangeSpec.IsRangeRequested)
        {
            var bounds = await StreamRangeHelper.ComputeRangeBounds(rangeSpec, contentLength);
            if (bounds.IsUnsatisfiable)
            {
                httpResponse.Headers.ContentRange = $"bytes */{contentLength}";
                return Results.StatusCode(416);
            }

            if (resp != null)
            {
                await S3MetadataHelper.ForwardS3ETag(resp, httpResponse);
                await S3MetadataHelper.ForwardS3LastModified(resp, httpResponse);
            }
            else if (metadataResp != null)
            {
                if (!string.IsNullOrEmpty(metadataResp.ETag))
                    httpResponse.Headers.ETag = metadataResp.ETag;
                if (metadataResp.LastModified != null)
                    httpResponse.Headers.LastModified = metadataResp
                        .LastModified.GetValueOrDefault()
                        .ToString("R");
            }
            await S3MetadataHelper.ForwardS3ContentRelatedHeaders(
                httpResponse,
                bounds.Start,
                bounds.End,
                contentLength,
                bounds.Length,
                responseContentType
            );
            httpResponse.StatusCode = 206;

            if (outputStream != null)
                await StreamRangeHelper
                    .CopyRangeAsync(
                        outputStream,
                        httpResponse.Body,
                        bounds.Start,
                        bounds.Length,
                        cancellationToken
                    )
                    .ConfigureAwait(false);

            return Results.Empty;
        }

        // Full object
        if (resp != null)
        {
            if (!string.IsNullOrEmpty(resp.ETag))
                httpResponse.Headers.ETag = resp.ETag;
            if (resp.LastModified != null)
                httpResponse.Headers.LastModified = resp
                    .LastModified.GetValueOrDefault()
                    .ToString("R");
        }
        else if (metadataResp != null)
        {
            if (!string.IsNullOrEmpty(metadataResp.ETag))
                httpResponse.Headers.ETag = metadataResp.ETag;
            if (metadataResp.LastModified != null)
                httpResponse.Headers.LastModified = metadataResp
                    .LastModified.GetValueOrDefault()
                    .ToString("R");
        }
        httpResponse.Headers.AcceptRanges = "bytes";
        httpResponse.ContentLength = contentLength;
        if (outputStream != null)
            return Results.File(outputStream, responseContentType, fileDownloadName: key);
        return Results.NoContent();
    }
}
