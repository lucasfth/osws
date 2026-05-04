using Amazon.S3.Model;
using Microsoft.AspNetCore.Http;

namespace OSWS.Library.Helpers;

/// <summary>
/// Helpers for forwarding S3 response metadata to HTTP responses and vice versa.
/// </summary>
public static class S3MetadataHelper
{
    /// <summary>
    /// Forwards ETag, LastModified, and Accept-Ranges from a GetObjectResponse to an HttpResponse.
    /// </summary>
    public static Task ForwardS3ETag(GetObjectResponse from, HttpResponse to)
    {
        if (!string.IsNullOrEmpty(from.ETag))
            to.Headers.ETag = from.ETag;
        if (from.LastModified != null)
            to.Headers.LastModified = from.LastModified.GetValueOrDefault().ToString("R");
        to.Headers.AcceptRanges = "bytes";

        return Task.CompletedTask;
    }

    /// <summary>
    /// Appends the ETag from a PutObjectResponse to the outgoing HttpResponse headers.
    /// </summary>
    public static Task AppendS3ETag(PutObjectResponse from, HttpResponse to)
    {
        if (!string.IsNullOrEmpty(from.ETag))
            to.Headers.ETag = from.ETag;
        return Task.CompletedTask;
    }

    /// <summary>
    /// Appends the ETag from a PutObjectResponse to the outgoing HttpResponse headers.
    /// </summary>
    public static Task AppendS3ETag(PutObjectResponse from, HttpRequest request)
    {
        return AppendS3ETag(from, request.HttpContext.Response);
    }

    /// <summary>
    /// Forwards the LastModified header from a GetObjectResponse to an HttpResponse.
    /// </summary>
    public static Task ForwardS3LastModified(GetObjectResponse from, HttpResponse to)
    {
        if (from.LastModified != null)
            to.Headers.LastModified = from.LastModified.GetValueOrDefault().ToString("R");
        return Task.CompletedTask;
    }

    /// <summary>
    /// Forwards content-related headers (Accept-Ranges, Content-Range, Content-Length, Content-Type).
    /// </summary>
    public static Task ForwardS3ContentRelatedHeaders(
        HttpResponse to,
        long contentRangeStart,
        long contentRangeEnd,
        long totalLength,
        long rangeLength,
        string? contentType
    )
    {
        var resolvedContentType = string.IsNullOrWhiteSpace(contentType)
            ? "application/octet-stream"
            : contentType;

        to.Headers.AcceptRanges = "bytes";
        to.Headers.ContentRange = $"bytes {contentRangeStart}-{contentRangeEnd}/{totalLength}";
        to.Headers.ContentLength = rangeLength;
        to.Headers.ContentType = resolvedContentType;
        return Task.CompletedTask;
    }
}
