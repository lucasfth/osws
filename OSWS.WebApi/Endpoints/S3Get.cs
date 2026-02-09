using Amazon.S3;
using Amazon.S3.Model;
using OSWS.Library;
using OSWS.Library.Helpers;
using OSWS.Models.DTOs;
using OSWS.ParquetSolver.Interfaces;
using OSWS.WebApi.Interfaces;

namespace OSWS.WebApi.Endpoints;

public class S3Get(IS3ClientFactory clientFactory, IParquetReader parquetReader) : IS3Get
{
    public async Task<IResult> GetObject(
        string bucket,
        string? key,
        Params prms,
        S3Options s3Options,
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

        var s3Client = clientFactory.GetClient(s3Options); // Should probably be moved into try to have finally for release

        // Build GetObjectRequest now (we may add range)
        var req = new GetObjectRequest
        {
            BucketName = bucket,
            Key = key,
            VersionId = string.IsNullOrEmpty(prms.Version) ? null : prms.Version,
        };

        var rangeSpec = await HttpHeaderHelper.ParseRange(httpRequest);
        if (rangeSpec.IsInvalidSpec)
        {
            clientFactory.ReleaseClient(s3Client);
            return Results.StatusCode(400);
        }

        // Due to how OSWS handles encryption, we cannot set byte-range on the S3 request, as we may need to fetch the full object to decrypt before slicing.
        // Instead, we'll handle range slicing in-memory after fetching (and potentially decrypting) the full object.
        // This allows us to support range requests even for encrypted objects without needing to know the content length or encryption details upfront.

        GetObjectResponse resp;
        try
        {
            resp = await s3Client.GetObjectAsync(req, cancellationToken).ConfigureAwait(false);
        }
        catch (AmazonS3Exception e)
        {
            clientFactory.ReleaseClient(s3Client);
            return S3ErrorHelper.HandleS3Exception(e, httpRequest.HttpContext);
        }
        finally
        {
            // release after using response stream (ReleaseClient only disposes non-default clients)
            clientFactory.ReleaseClient(s3Client);
        }

        // Decrypt parquet files after retrieving from S3
        var isParquetFile = TypeCheck.IsParquetFile(key, resp.Headers?.ContentType);
        var outputStream = resp.ResponseStream;

        if (isParquetFile)
        {
            try
            {
                // Copy to MemoryStream to make it seekable for Parquet library
                var seekableStream = new MemoryStream();
                await resp.ResponseStream.CopyToAsync(seekableStream, cancellationToken);
                seekableStream.Position = 0;

                outputStream = await parquetReader.ReadParquetAsync(seekableStream);
            }
            catch (Exception ex)
            {
                httpRequest.HttpContext.Response.StatusCode = 500;
                return Results.Text(
                    ParamValidation.CreateErrorJson(
                        $"Failed to decrypt parquet file: {ex.Message}"
                    ),
                    "application/json"
                );
            }
        }

        var contentLength = outputStream.CanSeek ? outputStream.Length : resp.ContentLength;

        // If a range was requested, compute bounds and stream only that range using StreamRangeHelper
        if (rangeSpec.IsRangeRequested)
        {
            var bounds = await StreamRangeHelper.ComputeRangeBounds(rangeSpec, contentLength);
            if (bounds.IsUnsatisfiable)
            {
                httpResponse.Headers.ContentRange = $"bytes */{contentLength}";
                return Results.StatusCode(416);
            }

            await HttpHeaderHelper.ForwardS3ETag(resp, httpResponse);
            await HttpHeaderHelper.ForwardS3LastModified(resp, httpResponse);
            await HttpHeaderHelper.ForwardS3ContentRelatedHeaders(
                httpResponse,
                bounds.Start,
                bounds.End,
                bounds.Length,
                resp.Headers?.ContentType
            );
            httpResponse.StatusCode = 206;

            await StreamRangeHelper
                .CopyRangeAsync(
                    outputStream,
                    httpResponse.Body,
                    bounds.Start,
                    bounds.Length,
                    cancellationToken
                )
                .ConfigureAwait(false);
            return Results.StatusCode(206);
        }

        // Full object
        if (!string.IsNullOrEmpty(resp.ETag))
            httpResponse.Headers.ETag = resp.ETag;
        if (resp.LastModified != null)
            httpResponse.Headers.LastModified = resp.LastModified.GetValueOrDefault().ToString("R");
        httpResponse.Headers.AcceptRanges = "bytes";
        httpResponse.ContentLength = contentLength;
        return Results.File(
            outputStream,
            resp.Headers?.ContentType ?? "application/octet-stream",
            fileDownloadName: key
        );
    }
}
