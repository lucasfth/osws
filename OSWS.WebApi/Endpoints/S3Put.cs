using Amazon.S3;
using Amazon.S3.Model;
using OSWS.Library;
using OSWS.Library.Helpers;
using OSWS.Models.DTOs;
using OSWS.ParquetSolver.Interfaces;
using OSWS.WebApi.Interfaces;

namespace OSWS.WebApi.Endpoints;

public class S3Put(IS3ClientFactory clientFactory, IParquetWriter parquetWriter) : IS3Put
{
    public async Task<IResult> PutObject(
        string bucket,
        string? key,
        Params prms,
        S3Options s3Options,
        HttpRequest httpRequest,
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

        var s3Client = clientFactory.GetClient(s3Options);

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
                // Copy to MemoryStream to make it seekable for Parquet library
                seekableStream = new MemoryStream();
                await httpRequest.Body.CopyToAsync(seekableStream, cancellationToken);
                seekableStream.Position = 0;

                var uploadStream = await parquetWriter.WriteParquetAsync(seekableStream);

                // Ensure stream is at position 0 and set content length
                uploadStream.Position = 0;

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
                seekableStream?.Dispose();
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
            // (for parquet, we already have a seekable stream)
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
            clientFactory.ReleaseClient(s3Client);

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
