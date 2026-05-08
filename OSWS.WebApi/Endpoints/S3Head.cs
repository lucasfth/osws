using Amazon.S3;
using Amazon.S3.Model;
using OSWS.Common.Configuration;
using OSWS.Library.Helpers;
using OSWS.ParquetSolver.Helpers;
using OSWS.ParquetSolver.Interfaces;
using OSWS.WebApi.Interfaces;
using OSWS.WebApi.Services;

namespace OSWS.WebApi.Endpoints;

public class S3Head(
    IAmazonS3 s3Client,
    IParquetReader parquetReader,
    S3ObjectFetcher objectFetcher,
    CurrentUser currentUser,
    PermissionService permissionService,
    EncryptionSettings encryptionSettings
) : IS3Head
{
    public async Task<IResult> HeadBucket(
        string bucket,
        HttpRequest httpRequest,
        HttpResponse httpResponse,
        CancellationToken cancellationToken = default
    )
    {
        if (string.IsNullOrEmpty(bucket))
        {
            httpRequest.HttpContext.Response.StatusCode = 400;
            return Results.Text(ParamValidation.BucketNameIsRequired(), "application/json");
        }

        try
        {
            await s3Client
                .HeadBucketAsync(new HeadBucketRequest { BucketName = bucket }, cancellationToken)
                .ConfigureAwait(false);

            return Results.StatusCode(StatusCodes.Status200OK);
        }
        catch (AmazonS3Exception e)
        {
            return S3ErrorHelper.HandleS3Exception(e, httpRequest.HttpContext);
        }
    }

    public async Task<IResult> HeadObject(
        string bucket,
        string key,
        HttpRequest httpRequest,
        HttpResponse httpResponse,
        int retryOptions = 3,
        int timeoutOptionsMs = 3000,
        CancellationToken cancellationToken = default
    )
    {
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

        var isParquetFile = TypeCheck.IsParquetFile(key, null);

        if (isParquetFile && !encryptionSettings.DisableEncryption)
        {
            var user = await currentUser.ResolveAsync(cancellationToken);
            if (user is null)
                return Results.Unauthorized();

            byte[] plaintext;
            GetObjectResponse? resp;
            try
            {
                (plaintext, resp) = await objectFetcher.FetchParquetAsync(
                    bucket,
                    key,
                    cancellationToken
                );
            }
            catch (AmazonS3Exception e)
            {
                return S3ErrorHelper.HandleS3Exception(e, httpRequest.HttpContext);
            }

            ISet<string>? allowedColumnSet = null;
            if (!encryptionSettings.BenchmarkMode)
            {
                allowedColumnSet = await permissionService.GetAllowedColumnsAsync(
                    user.Id,
                    cancellationToken
                );
            }

            var masked = await parquetReader.MaskPlaintextAsync(
                new MemoryStream(plaintext),
                allowedColumnSet
            );

            // If resp is null (cache hit), fetch metadata separately to populate ETag/Last-Modified.
            // this is because readers like Spark require these fields
            GetObjectMetadataResponse? metaForEncrypted = null;
            if (resp == null)
            {
                try
                {
                    metaForEncrypted = await s3Client
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

            httpResponse.Headers.AcceptRanges = "bytes";
            httpResponse.ContentLength = masked.Length;
            var etag = resp?.ETag ?? metaForEncrypted?.ETag;
            var lastMod = resp?.LastModified ?? metaForEncrypted?.LastModified;
            var contentType = resp?.Headers?.ContentType ?? metaForEncrypted?.Headers?.ContentType;
            if (!string.IsNullOrEmpty(etag))
                httpResponse.Headers.ETag = etag;
            if (lastMod != null)
                httpResponse.Headers.LastModified = lastMod.GetValueOrDefault().ToString("R");
            if (!string.IsNullOrWhiteSpace(contentType))
                httpResponse.ContentType = contentType;

            return Results.StatusCode(200);
        }

        var req = new GetObjectMetadataRequest { BucketName = bucket, Key = key };

        GetObjectMetadataResponse metadataResp;
        try
        {
            metadataResp = await s3Client
                .GetObjectMetadataAsync(req, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (AmazonS3Exception e)
        {
            return S3ErrorHelper.HandleS3Exception(e, httpRequest.HttpContext);
        }

        if (!string.IsNullOrEmpty(metadataResp.ETag))
            httpResponse.Headers.ETag = metadataResp.ETag;
        if (metadataResp.LastModified != null)
            httpResponse.Headers.LastModified = metadataResp
                .LastModified.GetValueOrDefault()
                .ToString("R");
        httpResponse.Headers.AcceptRanges = "bytes";
        if (metadataResp.Headers.ContentLength >= 0)
            httpResponse.ContentLength = metadataResp.Headers.ContentLength;
        if (!string.IsNullOrWhiteSpace(metadataResp.Headers.ContentType))
            httpResponse.ContentType = metadataResp.Headers.ContentType;

        return Results.StatusCode(200);
    }
}
