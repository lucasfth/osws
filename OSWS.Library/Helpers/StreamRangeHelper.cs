namespace OSWS.Library.Helpers;

/// <summary>
/// Represents the computed byte range bounds (start and end) for a requested range, along with flags indicating
/// whether a range was requested and whether the specified range is unsatisfiable given the content length.
/// </summary>
public class RangeBoundsResult
{
    public bool IsRequested { get; set; }
    public bool IsUnsatisfiable { get; set; }
    public long Start { get; set; }
    public long End { get; set; }

    public long Length => End - Start + 1;
}

/// <summary>
/// Helper class for computing range related information.
/// </summary>
public static class StreamRangeHelper
{
    /// <summary>
    /// Computes the actual byte range bounds (start and end) for a given RangeParseResult and content length.
    /// Handles both standard and suffix byte range specifications, and checks for unsatisfiable ranges.
    /// </summary>
    /// <param name="spec"></param>
    /// <param name="contentLength"></param>
    /// <returns></returns>
    public static Task<RangeBoundsResult> ComputeRangeBounds(
        RangeParseResult spec,
        long contentLength
    )
    {
        var res = new RangeBoundsResult { IsRequested = false };
        if (!spec.IsRangeRequested)
            return Task.FromResult(res);

        long start;
        long end;

        if (spec.IsSuffix && spec.SuffixLength.HasValue)
        {
            var suffix = spec.SuffixLength.Value;
            if (suffix <= 0)
            {
                res.IsUnsatisfiable = true;
                return Task.FromResult(res);
            }

            start = Math.Max(0, contentLength - suffix);
            end = contentLength - 1;
        }
        else
        {
            start = spec.Start ?? 0;
            end = spec.End ?? (contentLength - 1);
        }

        if (start >= contentLength || start > end)
        {
            res.IsUnsatisfiable = true;
            return Task.FromResult(res);
        }

        res.IsRequested = true;
        res.Start = start;
        res.End = end;
        return Task.FromResult(res);
    }

    /// <summary>
    /// Copies a specified byte range from a source stream to a destination stream.
    /// Handles both seekable and non-seekable streams.
    /// </summary>
    /// <param name="source"></param>
    /// <param name="destination"></param>
    /// <param name="start"></param>
    /// <param name="length"></param>
    /// <param name="cancellationToken"></param>
    public static async Task CopyRangeAsync(
        Stream source,
        Stream destination,
        long start,
        long length,
        CancellationToken cancellationToken
    )
    {
        if (source.CanSeek)
        {
            source.Seek(start, SeekOrigin.Begin);
            var remaining = length;
            var buffer = new byte[81920];
            while (remaining > 0)
            {
                var toRead = (int)Math.Min(buffer.Length, remaining);
                var read = await source
                    .ReadAsync(buffer.AsMemory(0, toRead), cancellationToken)
                    .ConfigureAwait(false);
                if (read == 0)
                    break;
                await destination
                    .WriteAsync(buffer.AsMemory(0, read), cancellationToken)
                    .ConfigureAwait(false);
                remaining -= read;
            }
            return;
        }

        // If not seekable, read-and-discard until start, then copy length bytes
        var toSkip = start;
        var buffer2 = new byte[81920];
        while (toSkip > 0)
        {
            var read = await source
                .ReadAsync(
                    buffer2.AsMemory(0, (int)Math.Min(buffer2.Length, toSkip)),
                    cancellationToken
                )
                .ConfigureAwait(false);
            if (read == 0)
                return; // EOF reached early
            toSkip -= read;
        }

        var remaining2 = length;
        while (remaining2 > 0)
        {
            var read = await source
                .ReadAsync(
                    buffer2.AsMemory(0, (int)Math.Min(buffer2.Length, remaining2)),
                    cancellationToken
                )
                .ConfigureAwait(false);
            if (read == 0)
                break;
            await destination
                .WriteAsync(buffer2.AsMemory(0, read), cancellationToken)
                .ConfigureAwait(false);
            remaining2 -= read;
        }
    }
}
