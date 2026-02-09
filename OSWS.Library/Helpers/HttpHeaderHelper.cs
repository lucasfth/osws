using Amazon.S3.Model;
using Microsoft.AspNetCore.Http;

namespace OSWS.Library.Helpers;

/// <summary>
/// Represents the result of parsing a Range header from an HTTP request, including whether a range was requested,
/// whether the specification was invalid, and the details of the requested range (start, end, suffix length
/// if applicable).
/// Provides a method to convert the parsed result into a ByteRange object for use in S3 GetObject requests.
/// </summary>
public class RangeParseResult
{
    public bool IsRangeRequested { get; set; }
    public bool IsInvalidSpec { get; set; }
    public bool IsSuffix { get; set; }
    public long? Start { get; set; }
    public long? End { get; set; }
    public long? SuffixLength { get; set; }

    public ByteRange ToByteRange(long contentLength)
    {
        if (!IsRangeRequested) return null!;
        if (IsSuffix)
        {
            var s = Math.Max(0, contentLength - (SuffixLength ?? 0));
            var e = contentLength - 1;
            return new ByteRange(s, e);
        }

        var start = Start ?? 0;
        var end = End ?? contentLength - 1;
        return new ByteRange(start, end);
    }
}

/// <summary>
/// Helper class for parsing the header of an HTTP request to extract useful information.
/// </summary>
public static class HttpHeaderHelper
{
    /// <summary>
    /// Parses the Range header from the given HttpRequest and returns a RangeParseResult indicating
    /// whether a valid byte range was requested, and if so, what the start and end positions are.
    /// <param name="httpRequest">The HttpRequest to parse the Range header from.</param>
    /// <returns>A RangeParseResult indicating the details of the parsed Range header.</returns>
    /// </summary>
    public static Task<RangeParseResult> ParseRange(HttpRequest httpRequest)
    {
        var result = new RangeParseResult();

        if (!httpRequest.Headers.TryGetValue("Range", out var rangeHdr))
        {
            result.IsRangeRequested = false;
            return Task.FromResult(result);
        }

        var r = rangeHdr.ToString();
        if (!r.StartsWith("bytes=", StringComparison.OrdinalIgnoreCase))
        {
            result.IsInvalidSpec = true;
            return Task.FromResult(result);
        }

        var spec = r[6..].Split(',')[0].Trim();

        if (spec.StartsWith('-'))
        {
            if (!long.TryParse(spec.AsSpan(1), out var suffixLen) || suffixLen <= 0)
            {
                result.IsInvalidSpec = true;
                return Task.FromResult(result);
            }

            result.IsRangeRequested = true;
            result.IsSuffix = true;
            result.SuffixLength = suffixLen;
            return Task.FromResult(result);
        }

        var parts = spec.Split('-');
        if (parts.Length != 2)
        {
            result.IsInvalidSpec = true;
            return Task.FromResult(result);
        }

        if (string.IsNullOrEmpty(parts[1]))
        {
            if (!long.TryParse(parts[0], out var s))
            {
                result.IsInvalidSpec = true;
                return Task.FromResult(result);
            }

            result.IsRangeRequested = true;
            result.Start = s;
            result.End = null;
            return Task.FromResult(result);
        }

        if (!long.TryParse(parts[0], out var s2) || !long.TryParse(parts[1], out var e2))
        {
            result.IsInvalidSpec = true;
            return Task.FromResult(result);
        }

        result.IsRangeRequested = true;
        result.Start = s2;
        result.End = e2;
        return Task.FromResult(result);
    }

    /// <summary>
    /// Forwards relevant S3 metadata headers (ETag, Last-Modified, Accept-Ranges)
    /// from the GetObjectResponse to the HttpResponse.
    /// </summary>
    /// <param name="from"></param>
    /// <param name="to"></param>
    /// <returns></returns>
    public static Task ForwardS3Metadata(GetObjectResponse from, HttpResponse to)
    {
        if (!string.IsNullOrEmpty(from.ETag))
            to.Headers.ETag = from.ETag;
        if (from.LastModified != null)
            to.Headers.LastModified = from.LastModified.GetValueOrDefault().ToString("R");
        to.Headers.AcceptRanges = "bytes";

        return Task.CompletedTask;
    }

    /// <summary>
    /// Result for preparing an incoming PUT request for S3 upload. Contains temp file info when buffering was used
    /// and any early error information (StatusCode + ErrorJson) to send back to the client without further processing.
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
    /// Prepares a PutObjectRequest by ensuring ContentLength is set and by buffering the incoming request body
    /// into a temp file when needed (for non-seekable streams or when streaming-signature would be used). Returns
    /// a PreparePutRequestResult indicating success or the error that should be returned to the client.
    /// The method will set req.InputStream and req.ContentLength when applicable and will also copy any
    /// x-amz-meta- headers into req.Metadata.
    /// </summary>
    public static async Task<PreparePutRequestResult> PreparePutRequestAsync(PutObjectRequest req,
        HttpRequest httpRequest, bool forceBuffer = false, CancellationToken cancellationToken = default)
    {
        var res = new PreparePutRequestResult();

        // If caller requested forced buffering (to avoid streaming-signature/trailer flows), perform it up-front.
        if (forceBuffer)
        {
            var maxBytesF = ConfigHelper.GetMaxUploadBufferBytes();

            var tempFileF = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            var tempFsF = File.Create(tempFileF);

            var bufferF = new byte[81920];
            long totalF = 0;
            int readF;
            while ((readF = await httpRequest.Body.ReadAsync(bufferF.AsMemory(0, bufferF.Length), cancellationToken).ConfigureAwait(false)) > 0)
            {
                totalF += readF;
                if (totalF > maxBytesF)
                {
                    await tempFsF.DisposeAsync().ConfigureAwait(false);
                    File.Delete(tempFileF);
                    res.IsError = true;
                    res.StatusCode = 413;
                    res.ErrorJson = ParamValidation.CreateErrorJson("Upload exceeds max buffer size");
                    return res;
                }

                await tempFsF.WriteAsync(bufferF.AsMemory(0, readF), cancellationToken).ConfigureAwait(false);
            }

            await tempFsF.FlushAsync(cancellationToken).ConfigureAwait(false);
            // Close the writer to release file handle
            await tempFsF.DisposeAsync().ConfigureAwait(false);

            // Open a read stream for the SDK (don't set both FilePath and InputStream - SDK expects only one)
            var readFsF = File.OpenRead(tempFileF);
            req.InputStream = readFsF;

            res.TempFile = tempFileF;
            res.TempFileStream = readFsF;
            return res;
        }

        var contentLengthHeader = httpRequest.ContentLength;
        var canSeek = httpRequest.Body.CanSeek;

        var didSetLength = false;

        // If the incoming request provides a Content-Length header, use it. Buffer if the stream is not seekable
        // to avoid streaming-signature/trailer flows that some providers don't support.
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
                while ((read2 = await httpRequest.Body.ReadAsync(buffer2.AsMemory(0, buffer2.Length), cancellationToken).ConfigureAwait(false)) > 0)
                {
                    total2 += read2;
                    if (total2 > maxBytes2)
                    {
                        await tempFs.DisposeAsync().ConfigureAwait(false);
                        File.Delete(tempFile);
                        res.IsError = true;
                        res.StatusCode = 413;
                        res.ErrorJson = ParamValidation.CreateErrorJson("Upload exceeds max buffer size");
                        return res;
                    }

                    await tempFs.WriteAsync(buffer2.AsMemory(0, read2), cancellationToken).ConfigureAwait(false);
                }

                await tempFs.FlushAsync(cancellationToken).ConfigureAwait(false);
                // Close the writer stream so the SDK can open the file independently and avoid any file-lock issues.
                await tempFs.DisposeAsync().ConfigureAwait(false);

                // Open a read stream for the SDK (don't set both FilePath and InputStream - SDK expects only one)
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

        // If seekable and we haven't set length yet, try to compute remaining length
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
                didSetLength = false; // ensure buffering happens
            }
        }

        // If we still don't know the length, buffer the request body to a temp file
        if (!didSetLength)
        {
            var maxBytes = ConfigHelper.GetMaxUploadBufferBytes();

            var tempFile = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            var tempFs = File.Create(tempFile);

            var buffer = new byte[81920];
            long total = 0;
            int read;
            while ((read = await httpRequest.Body.ReadAsync(buffer.AsMemory(0, buffer.Length), cancellationToken).ConfigureAwait(false)) > 0)
            {
                total += read;
                if (total > maxBytes)
                {
                    await tempFs.DisposeAsync().ConfigureAwait(false);
                    File.Delete(tempFile);
                    res.IsError = true;
                    res.StatusCode = 413;
                    res.ErrorJson = ParamValidation.CreateErrorJson("Upload exceeds max buffer size");
                    return res;
                }

                await tempFs.WriteAsync(buffer.AsMemory(0, read), cancellationToken).ConfigureAwait(false);
            }

            await tempFs.FlushAsync(cancellationToken).ConfigureAwait(false);
            // Close the writer to release file handle
            await tempFs.DisposeAsync().ConfigureAwait(false);

            // Open a read stream for the SDK (don't set both FilePath and InputStream - SDK expects only one)
            var readFs = File.OpenRead(tempFile);
            req.InputStream = readFs;

            res.TempFile = tempFile;
            res.TempFileStream = readFs;
        }

        // Forward any x-amz-meta- headers into S3 metadata
        foreach (var h in httpRequest.Headers)
        {
            var hn = h.Key;
            if (!hn.StartsWith("x-amz-meta-", StringComparison.OrdinalIgnoreCase)) continue;
            var metaKey = hn.Substring("x-amz-meta-".Length);
            req.Metadata[metaKey] = h.Value.ToString();
        }

        return res;
    }
}