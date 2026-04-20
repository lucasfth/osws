using Amazon.S3;
using Amazon.S3.Model;
using OSWS.Library;
using OSWS.Library.Helpers;
using OSWS.Models.DTOs;
using OSWS.Models.Entities;
using OSWS.ParquetSolver.Helpers;
using OSWS.WebApi.Interfaces;
using OSWS.WebApi.Services;

namespace OSWS.WebApi.Endpoints;

public class S3Put(
    IAmazonS3 s3Client,
    CurrentUser currentUser,
    ParquetUploadService parquetUploadService
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

        if (isParquetFile)
        {
            var role = user.Roles.FirstOrDefault();
            if (role is null)
            {
                httpRequest.HttpContext.Response.StatusCode = 403;
                return Results.Text(
                    ParamValidation.CreateErrorJson("User has no roles assigned"),
                    "application/json"
                );
            }

            try
            {
                var uploadStream = await parquetUploadService.ProcessAsync(
                    httpRequest.Body,
                    role,
                    bucket,
                    key,
                    cancellationToken
                );

                req.InputStream = uploadStream;

                // Set content length using reflection (ContentLength property)
                var contentLengthProp = typeof(PutObjectRequest).GetProperty("ContentLength");
                if (contentLengthProp != null && contentLengthProp.CanWrite)
                {
                    contentLengthProp.SetValue(req, uploadStream.Length);
                }

                // Forward metadata headers
                foreach (var h in httpRequest.Headers)
                {
                    var hn = h.Key;
                    if (hn.StartsWith("x-amz-meta-", StringComparison.OrdinalIgnoreCase))
                    {
                        req.Metadata[hn.Substring("x-amz-meta-".Length)] = h.Value.ToString();
                    }
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
        }
        else
        {
            req.InputStream = httpRequest.Body;

            try
            {
                var forceBuf = ConfigHelper.GetForceUploadBuffering();
                var prep = await PutRequestHelper
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
            catch
            {
                if (tempFs == null)
                    throw;
                await tempFs.DisposeAsync().ConfigureAwait(false);
                if (tempFile != null && File.Exists(tempFile))
                    File.Delete(tempFile);
                throw;
            }
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

        await S3MetadataHelper.AppendS3ETag(resp, httpRequest.HttpContext.Response);

        S3ErrorHelper.AddBufferingDebugHeaders(httpRequest.HttpContext, tempFile);

        return Results.Ok();
    }
}
