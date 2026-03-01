using Amazon.S3;
using Amazon.S3.Model;
using OSWS.Library;
using OSWS.Library.Helpers;
using OSWS.Models.DTOs;
using OSWS.ParquetSolver.Helpers;
using OSWS.ParquetSolver.Interfaces;
using OSWS.WebApi.Interfaces;

namespace OSWS.WebApi.Endpoints;

public class S3Get(IAmazonS3 s3Client, IParquetReader parquetReader, EncryptedFileCache fileCache) : IS3Get
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

        var rangeSpec = await HttpHeaderHelper.ParseRange(httpRequest);
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

        if (isParquetFile && fileCache.TryGet(cacheKey, out var cachedStream))
        {
            // Cache hit - use cached encrypted stream
            encryptedStream = cachedStream;
        }
        else
        {
            // Cache miss - fetch from S3
            try
            {
                resp = await s3Client.GetObjectAsync(req, cancellationToken).ConfigureAwait(false);
            }
            catch (AmazonS3Exception e)
            {
                return S3ErrorHelper.HandleS3Exception(e, httpRequest.HttpContext);
            }

            if (isParquetFile)
            {
                // Copy encrypted stream to memory for caching and decryption
                var memStream = new MemoryStream();
                await resp.ResponseStream.CopyToAsync(memStream, cancellationToken);
                memStream.Position = 0;
                encryptedStream = memStream;

                // Cache the encrypted stream asynchronously (don't await to avoid blocking)
                _ = fileCache.SetAsync(cacheKey, memStream, cancellationToken).ContinueWith(
                    task =>
                    {
                        if (task.IsFaulted)
                        {
                            // Log cache failure but don't block the response
                            Console.WriteLine($"[OSWS] Cache failure for {cacheKey}: {task.Exception?.InnerException?.Message}");
                        }
                    },
                    TaskScheduler.Default
                );
            }
        }

        // Decrypt parquet files
        var outputStream = resp?.ResponseStream ?? encryptedStream;

        if (isParquetFile && encryptedStream != null)
        {
            try
            {
                outputStream = await parquetReader.ReadParquetAsync(encryptedStream);
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

        if (isParquetFile && !outputStream.CanSeek)
        {
            var buffered = new MemoryStream();
            await outputStream.CopyToAsync(buffered, cancellationToken).ConfigureAwait(false);
            buffered.Position = 0;
            outputStream = buffered;
        }

        var contentLength = outputStream.CanSeek ? outputStream.Length : (resp?.ContentLength ?? 0);
        if (rangeSpec.IsRangeRequested && (contentLength <= 0 || !outputStream.CanSeek))
        {
            // Buffer to get a reliable length and enable seeking for ranged reads.
            var buffered = new MemoryStream();
            await outputStream.CopyToAsync(buffered, cancellationToken).ConfigureAwait(false);
            buffered.Position = 0;
            outputStream = buffered;
            contentLength = buffered.Length;
        }

        var responseContentType = string.IsNullOrWhiteSpace(resp?.Headers?.ContentType)
            ? "application/octet-stream"
            : resp.Headers.ContentType;

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
                await HttpHeaderHelper.ForwardS3ETag(resp, httpResponse);
                await HttpHeaderHelper.ForwardS3LastModified(resp, httpResponse);
            }
            await HttpHeaderHelper.ForwardS3ContentRelatedHeaders(
                httpResponse,
                bounds.Start,
                bounds.End,
                contentLength,
                bounds.Length,
                responseContentType
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

            return Results.Empty;
        }

        // Full object
        if (resp != null)
        {
            if (!string.IsNullOrEmpty(resp.ETag))
                httpResponse.Headers.ETag = resp.ETag;
            if (resp.LastModified != null)
                httpResponse.Headers.LastModified = resp.LastModified.GetValueOrDefault().ToString("R");
        }
        httpResponse.Headers.AcceptRanges = "bytes";
        httpResponse.ContentLength = contentLength;
        return Results.File(outputStream, responseContentType, fileDownloadName: key);
    }
}
