using System.Diagnostics;
using System.Security.Claims;
using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.EntityFrameworkCore;
using OSWS.Common.Configuration;
using OSWS.KeyManager.Persistence;
using OSWS.Library.Helpers;
using OSWS.Models.DTOs;
using OSWS.Models.Entities;
using OSWS.ParquetSolver.Helpers;
using OSWS.ParquetSolver.Interfaces;
using OSWS.WebApi.Helpers;
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
    IWebHostEnvironment env,
    EncryptionSettings encryptionSettings
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
        var totalSw = Stopwatch.StartNew();
        logger.LogInformation("[S3Get] GET {Bucket}/{Key} started", bucket, key);

        var sw = Stopwatch.StartNew();
        var user = await currentUser.ResolveAsync(cancellationToken);
        if (user is null)
        {
            logger.LogWarning("[S3Get] Unauthorized: could not resolve user for {Bucket}/{Key}", bucket, key);
            return Results.Unauthorized();
        }
        logger.LogDebug("[S3Get] Auth resolved: userId={UserId} ({ElapsedMs}ms)", user.Id, sw.ElapsedMilliseconds);

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
            logger.LogWarning("[S3Get] Invalid range spec for {Bucket}/{Key}", bucket, key);
            return Results.StatusCode(400);
        }

        var isParquetFile = TypeCheck.IsParquetFile(key, httpRequest.ContentType);
        logger.LogDebug("[S3Get] isParquet={IsParquet}, rangeRequested={RangeRequested}", isParquetFile, rangeSpec.IsRangeRequested);
        // Due to how OSWS handles encryption, we cannot set byte-range on the S3 request, as we
        // may need to fetch the full object to decrypt before slicing. Range slicing is applied
        // in-memory after fetching (and potentially decrypting) the full object.
        var cacheKey = EncryptedFileCache.GenerateCacheKey(bucket, key);
        GetObjectResponse? resp = null;
        Stream? encryptedStream = null;

        sw.Restart();
        if (isParquetFile)
        {
            try
            {
                var fetchResult = await objectFetcher.FetchParquetAsync(
                    bucket,
                    key,
                    cancellationToken
                );
                encryptedStream = fetchResult.EncryptedStream;
                resp = fetchResult.S3Response;
                var source = resp is null ? "file-cache" : "s3";
                logger.LogInformation(
                    "[S3Get] Fetch parquet: source={Source}, size={SizeBytes}B ({ElapsedMs}ms)",
                    source, encryptedStream?.Length ?? -1, sw.ElapsedMilliseconds
                );
            }
            catch (AmazonS3Exception e)
            {
                logger.LogError(
                    e,
                    "[S3Get] S3 error fetching parquet {Bucket}/{Key}: {StatusCode} {ErrorCode}",
                    bucket, key, e.StatusCode, e.ErrorCode
                );
                return S3ErrorHelper.HandleS3Exception(e, httpRequest.HttpContext);
            }
        }
        else
        {
            try
            {
                resp = await objectFetcher.FetchObjectAsync(bucket, key, cancellationToken);
                logger.LogDebug("[S3Get] Fetch object from S3 ({ElapsedMs}ms)", sw.ElapsedMilliseconds);
            }
            catch (AmazonS3Exception e)
            {
                logger.LogError(
                    e,
                    "[S3Get] S3 error fetching object {Bucket}/{Key}: {StatusCode} {ErrorCode}",
                    bucket, key, e.StatusCode, e.ErrorCode
                );
                return S3ErrorHelper.HandleS3Exception(e, httpRequest.HttpContext);
            }
        }

        // For parquet fetches, prefer the buffered encrypted stream from S3ObjectFetcher
        // (resp.ResponseStream may have been consumed during cache population).
        var outputStream = isParquetFile
            ? (encryptedStream ?? resp?.ResponseStream)
            : resp?.ResponseStream;

        if (isParquetFile && encryptedStream != null && !encryptionSettings.DisableEncryption)
        {
            try
            {
                ISet<string>? allowedColumnSet = null;

                if (!encryptionSettings.BenchmarkMode)
                {
                    sw.Restart();
                    allowedColumnSet = await permissionService.GetAllowedColumnsAsync(
                        user.Id,
                        cancellationToken
                    );

                    var roleIds = await permissionService.GetEffectiveRoleIdsAsync(
                        user.Id,
                        cancellationToken
                    );

                    var allowedColumnsDisplay =
                        allowedColumnSet != null ? string.Join(",", allowedColumnSet) : "* (all)";
                    logger.LogDebug(
                        "[S3Get] Permission check: userId={UserId}, roles=[{Roles}], allowedColumns=[{Columns}] ({ElapsedMs}ms)",
                        user.Id, string.Join(",", roleIds), allowedColumnsDisplay, sw.ElapsedMilliseconds
                    );
                }

                sw.Restart();
                outputStream = await parquetReader.ReadParquetAsync(
                    encryptedStream,
                    allowedColumnSet
                );
                logger.LogInformation(
                    "[S3Get] Decrypt+re-encode complete: outputSize={SizeBytes}B ({ElapsedMs}ms)",
                    (outputStream as MemoryStream)?.Length ?? -1, sw.ElapsedMilliseconds
                );
            }
            catch (Exception ex)
            {
                logger.LogError(
                    ex,
                    "[S3Get] Parquet decryption failed for {Bucket}/{Key} after {ElapsedMs}ms: {Message}",
                    bucket, key, totalSw.ElapsedMilliseconds, ex.Message
                );
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
            sw.Restart();
            try
            {
                metadataResp = await s3Client
                    .GetObjectMetadataAsync(
                        new GetObjectMetadataRequest { BucketName = bucket, Key = key },
                        cancellationToken
                    )
                    .ConfigureAwait(false);
                logger.LogDebug("[S3Get] Metadata fetch (file-cache hit path): ({ElapsedMs}ms)", sw.ElapsedMilliseconds);
            }
            catch (AmazonS3Exception e)
            {
                logger.LogError(
                    e,
                    "[S3Get] S3 error fetching metadata for {Bucket}/{Key}: {StatusCode} {ErrorCode}",
                    bucket, key, e.StatusCode, e.ErrorCode
                );
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

            logger.LogInformation(
                "[S3Get] GET {Bucket}/{Key} complete (range): total={TotalMs}ms",
                bucket, key, totalSw.ElapsedMilliseconds
            );
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
        logger.LogInformation(
            "[S3Get] GET {Bucket}/{Key} complete: contentLength={ContentLength}B, total={TotalMs}ms",
            bucket, key, contentLength, totalSw.ElapsedMilliseconds
        );
        if (outputStream != null)
            return Results.File(outputStream, responseContentType, fileDownloadName: key);
        return Results.NoContent();
    }
}
