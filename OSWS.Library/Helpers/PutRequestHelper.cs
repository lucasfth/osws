using Amazon.S3.Model;
using Microsoft.AspNetCore.Http;

namespace OSWS.Library.Helpers;

/// <summary>
/// Result for preparing an incoming PUT request for S3 upload. Contains temp file info
/// when buffering was used, and any early error information to return to the client.
/// </summary>
public class PreparePutRequestResult
{
    public bool IsError { get; set; }
    public int StatusCode { get; set; }
    public string? ErrorJson { get; set; }
    public string? TempFile { get; set; }
    public FileStream? TempFileStream { get; set; }
}

/// <summary>
/// Prepares incoming PUT requests for S3 upload, handling stream buffering and content length.
/// </summary>
public static class PutRequestHelper
{
    /// <summary>
    /// Ensures ContentLength is set on the PutObjectRequest, buffering the body to a temp file
    /// when the stream is not seekable or when forced buffering is enabled.
    /// Also copies x-amz-meta- headers into req.Metadata.
    /// </summary>
    public static async Task<PreparePutRequestResult> PreparePutRequestAsync(
        PutObjectRequest req,
        HttpRequest httpRequest,
        bool forceBuffer = false,
        CancellationToken cancellationToken = default
    )
    {
        var res = new PreparePutRequestResult();

        if (forceBuffer)
        {
            var maxBytesF = ConfigHelper.GetMaxUploadBufferBytes();

            var tempFileF = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            var tempFsF = File.Create(tempFileF);

            var bufferF = new byte[81920];
            long totalF = 0;
            int readF;
            while (
                (
                    readF = await httpRequest
                        .Body.ReadAsync(bufferF.AsMemory(0, bufferF.Length), cancellationToken)
                        .ConfigureAwait(false)
                ) > 0
            )
            {
                totalF += readF;
                if (totalF > maxBytesF)
                {
                    await tempFsF.DisposeAsync().ConfigureAwait(false);
                    File.Delete(tempFileF);
                    res.IsError = true;
                    res.StatusCode = 413;
                    res.ErrorJson = ParamValidation.CreateErrorJson(
                        "Upload exceeds max buffer size"
                    );
                    return res;
                }

                await tempFsF
                    .WriteAsync(bufferF.AsMemory(0, readF), cancellationToken)
                    .ConfigureAwait(false);
            }

            await tempFsF.FlushAsync(cancellationToken).ConfigureAwait(false);
            await tempFsF.DisposeAsync().ConfigureAwait(false);

            var readFsF = File.OpenRead(tempFileF);
            req.InputStream = readFsF;

            res.TempFile = tempFileF;
            res.TempFileStream = readFsF;
            return res;
        }

        var contentLengthHeader = httpRequest.ContentLength;
        var canSeek = httpRequest.Body.CanSeek;

        var didSetLength = false;

        if (contentLengthHeader.HasValue)
        {
            if (!canSeek)
            {
                var maxBytes2 = ConfigHelper.GetMaxUploadBufferBytes();

                var tempFile = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
                var tempFs = File.Create(tempFile);

                var buffer2 = new byte[81920];
                long total2 = 0;
                int read2;
                while (
                    (
                        read2 = await httpRequest
                            .Body.ReadAsync(buffer2.AsMemory(0, buffer2.Length), cancellationToken)
                            .ConfigureAwait(false)
                    ) > 0
                )
                {
                    total2 += read2;
                    if (total2 > maxBytes2)
                    {
                        await tempFs.DisposeAsync().ConfigureAwait(false);
                        File.Delete(tempFile);
                        res.IsError = true;
                        res.StatusCode = 413;
                        res.ErrorJson = ParamValidation.CreateErrorJson(
                            "Upload exceeds max buffer size"
                        );
                        return res;
                    }

                    await tempFs
                        .WriteAsync(buffer2.AsMemory(0, read2), cancellationToken)
                        .ConfigureAwait(false);
                }

                await tempFs.FlushAsync(cancellationToken).ConfigureAwait(false);
                await tempFs.DisposeAsync().ConfigureAwait(false);

                var readFs2 = File.OpenRead(tempFile);
                req.InputStream = readFs2;

                res.TempFile = tempFile;
                res.TempFileStream = readFs2;
                return res;
            }

            var prop = typeof(PutObjectRequest).GetProperty("ContentLength");
            if (prop != null && prop.CanWrite)
            {
                prop.SetValue(req, contentLengthHeader.Value);
                didSetLength = true;
            }
        }

        if (!didSetLength && canSeek)
        {
            try
            {
                var remaining = httpRequest.Body.Length - httpRequest.Body.Position;
                var prop = typeof(PutObjectRequest).GetProperty("ContentLength");
                if (prop != null && prop.CanWrite)
                {
                    prop.SetValue(req, remaining);
                    didSetLength = true;
                }
            }
            catch
            {
                didSetLength = false;
            }
        }

        if (!didSetLength)
        {
            var maxBytes = ConfigHelper.GetMaxUploadBufferBytes();

            var tempFile = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            var tempFs = File.Create(tempFile);

            var buffer = new byte[81920];
            long total = 0;
            int read;
            while (
                (
                    read = await httpRequest
                        .Body.ReadAsync(buffer.AsMemory(0, buffer.Length), cancellationToken)
                        .ConfigureAwait(false)
                ) > 0
            )
            {
                total += read;
                if (total > maxBytes)
                {
                    await tempFs.DisposeAsync().ConfigureAwait(false);
                    File.Delete(tempFile);
                    res.IsError = true;
                    res.StatusCode = 413;
                    res.ErrorJson = ParamValidation.CreateErrorJson(
                        "Upload exceeds max buffer size"
                    );
                    return res;
                }

                await tempFs
                    .WriteAsync(buffer.AsMemory(0, read), cancellationToken)
                    .ConfigureAwait(false);
            }

            await tempFs.FlushAsync(cancellationToken).ConfigureAwait(false);
            await tempFs.DisposeAsync().ConfigureAwait(false);

            var readFs = File.OpenRead(tempFile);
            req.InputStream = readFs;

            res.TempFile = tempFile;
            res.TempFileStream = readFs;
        }

        foreach (var h in httpRequest.Headers)
        {
            var hn = h.Key;
            if (!hn.StartsWith("x-amz-meta-", StringComparison.OrdinalIgnoreCase))
                continue;
            var metaKey = hn.Substring("x-amz-meta-".Length);
            req.Metadata[metaKey] = h.Value.ToString();
        }

        return res;
    }
}
