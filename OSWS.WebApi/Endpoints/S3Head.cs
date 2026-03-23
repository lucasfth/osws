using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.EntityFrameworkCore;
using OSWS.KeyManager.Persistence;
using OSWS.Library;
using OSWS.Library.Helpers;
using OSWS.ParquetSolver.Interfaces;
using OSWS.WebApi.Interfaces;
using OSWS.WebApi.Services;

namespace OSWS.WebApi.Endpoints;

public class S3Head(IAmazonS3 s3Client, IParquetReader parquetReader, CurrentUser currentUser, OswsContext db)
    : IS3Head
{
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

        if (isParquetFile)
        {
            var user = await currentUser.ResolveAsync(cancellationToken);
            if (user is null)
                return Results.Unauthorized();

            using var resp = await s3Client
                .GetObjectAsync(
                    new GetObjectRequest { BucketName = bucket, Key = key },
                    cancellationToken
                )
                .ConfigureAwait(false);

            Stream outputStream = resp.ResponseStream;
            try
            {
                var roleIds = user.Roles.Select(r => r.Id).ToList();
                var allowedColumns = await db
                    .Permissions.Where(p => roleIds.Contains(p.RoleId))
                    .Select(p => p.Column.Name)
                    .Distinct()
                    .ToListAsync(cancellationToken);
                var allowedColumnSet = new HashSet<string>(allowedColumns);

                var seekableStream = new MemoryStream();
                await resp.ResponseStream.CopyToAsync(seekableStream, cancellationToken);
                seekableStream.Position = 0;

                outputStream = await parquetReader.ReadParquetAsync(seekableStream, allowedColumnSet);
            }
            catch (AmazonS3Exception e)
            {
                return S3ErrorHelper.HandleS3Exception(e, httpRequest.HttpContext);
            }

            if (!outputStream.CanSeek)
            {
                var buffered = new MemoryStream();
                await outputStream.CopyToAsync(buffered, cancellationToken).ConfigureAwait(false);
                buffered.Position = 0;
                outputStream = buffered;
            }

            httpResponse.Headers.AcceptRanges = "bytes";
            httpResponse.ContentLength = outputStream.Length;
            if (!string.IsNullOrWhiteSpace(resp.Headers?.ContentType))
                httpResponse.ContentType = resp.Headers.ContentType;
            if (!string.IsNullOrEmpty(resp.ETag))
                httpResponse.Headers.ETag = resp.ETag;
            if (resp.LastModified != null)
                httpResponse.Headers.LastModified = resp
                    .LastModified.GetValueOrDefault()
                    .ToString("R");

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
