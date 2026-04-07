using Amazon.S3.Model;
using Microsoft.AspNetCore.Http;

namespace OSWS.Library.Helpers;

/// <summary>
/// Represents the result of parsing a Range header from an HTTP request.
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
        if (!IsRangeRequested)
            return null!;
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
/// Parses HTTP Range headers for byte-range request handling.
/// </summary>
public static class RangeHelper
{
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
}
