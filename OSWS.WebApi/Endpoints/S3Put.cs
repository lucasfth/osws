using OSWS.Library;
using OSWS.Models.DTOs;
using OSWS.WebApi.Interfaces;
using Amazon.S3.Model;
using Amazon.S3;
using OSWS.Library.Helpers;

namespace OSWS.WebApi.Endpoints;

public class S3Put(IS3ClientFactory clientFactory) : IS3Put
{
    public async Task<IResult> PutObject(string bucket, string? key, Params prms, S3Options s3Options, HttpRequest httpRequest, int retryOptions = 3,
        int timeoutOptionsMs = 3000, CancellationToken cancellationToken = default)
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

        var s3Client = clientFactory.GetClient(s3Options);

        // TODO (FUTURE): Create keys

        // TODO (FUTURE): Encrypt given keys and grants

        // TODO (FUTURE): Add keys to KMS

        var req = new PutObjectRequest
        {
            BucketName = bucket,
            Key = key,
            InputStream = httpRequest.Body,
            ContentType = httpRequest.ContentType ?? "application/octet-stream",
            UseChunkEncoding = false, // Disable chunked encoding to avoid STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER
        };

        string? tempFile = null;
        FileStream? tempFs = null;
        try
        {
            var forceBuf = ConfigHelper.GetForceUploadBuffering();
            var prep = await HttpHeaderHelper.PreparePutRequestAsync(req, httpRequest, forceBuf, cancellationToken).ConfigureAwait(false);
            if (prep.IsError)
            {
                httpRequest.HttpContext.Response.StatusCode = prep.StatusCode;
                return Results.Text(prep.ErrorJson ?? "{\"error\":\"Upload error\"}", "application/json");
            }

            tempFile = prep.TempFile;
            tempFs = prep.TempFileStream;
        }
        catch
        {
            if (tempFs == null) throw;
            await tempFs.DisposeAsync().ConfigureAwait(false);
            if (tempFile != null && File.Exists(tempFile)) File.Delete(tempFile);
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
            clientFactory.ReleaseClient(s3Client);

            try
            {
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
        if (!string.IsNullOrEmpty(resp.ETag))
            httpRequest.HttpContext.Response.Headers.Append("ETag", resp.ETag);

        S3ErrorHelper.AddBufferingDebugHeaders(httpRequest.HttpContext, tempFile);

        var successJson = "{" + "\"etag\":" + (resp.ETag == null ? "null" : "\"" + ParamValidation.JsonEscape(resp.ETag).Trim('"') + "\"") + "," +
                          "\"versionId\":" + (resp.VersionId == null ? "null" : "\"" + ParamValidation.JsonEscape(resp.VersionId).Trim('"') + "\"") +
                          "}";
        return Results.Text(successJson, "application/json");
    }
}
