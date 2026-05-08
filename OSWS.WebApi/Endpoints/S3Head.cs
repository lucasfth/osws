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
                (plaintext, resp) = await objectFetcher.FetchParquetAsync(bucket, key, cancellationToken);
            }
            catch (AmazonS3Exception e)
            {
                return S3ErrorHelper.HandleS3Exception(e, httpRequest.HttpContext);
            }

            ISet<string>? allowedColumnSet = null;
            if (!encryptionSettings.BenchmarkMode)
            {
                allowedColumnSet = await permissionService.GetAllowedColumnsAsync(user.Id, cancellationToken);
            }

            var masked = await parquetReader.MaskPlaintextAsync(new MemoryStream(plaintext), allowedColumnSet);

            httpResponse.Headers.AcceptRanges = "bytes";
            httpResponse.ContentLength = masked.Length;
            if (resp != null)
            {
                if (!string.IsNullOrWhiteSpace(resp.Headers?.ContentType))
                    httpResponse.ContentType = resp.Headers.ContentType;
                if (!string.IsNullOrEmpty(resp.ETag))
                    httpResponse.Headers.ETag = resp.ETag;
                if (resp.LastModified != null)
                    httpResponse.Headers.LastModified = resp.LastModified.GetValueOrDefault().ToString("R");
            }

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
