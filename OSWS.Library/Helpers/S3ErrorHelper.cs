using Amazon.S3;
using Microsoft.AspNetCore.Http;

namespace OSWS.Library.Helpers;

/// <summary>
/// Helper for handling S3 exceptions and converting them to appropriate HTTP responses.
/// </summary>
public static class S3ErrorHelper
{
    /// <summary>
    /// Converts an AmazonS3Exception to an appropriate IResult and sets the response status code.
    /// </summary>
    public static IResult HandleS3Exception(AmazonS3Exception e, HttpContext httpContext)
    {
        var msg = e.Message ?? "S3 error";

        switch (e.StatusCode)
        {
            case System.Net.HttpStatusCode.NotFound:
                httpContext.Response.StatusCode = 404;
                return Results.Text(
                    ParamValidation.CreateErrorJson("Bucket or object not found"),
                    "application/json"
                );

            case System.Net.HttpStatusCode.Forbidden:
                return Results.StatusCode(403);

            case System.Net.HttpStatusCode.Unauthorized:
                return Results.StatusCode(401);

            case System.Net.HttpStatusCode.BadRequest:
                return Results.StatusCode(400);

            default:
                httpContext.Response.StatusCode = 500;
                return Results.Text(ParamValidation.CreateErrorJson(msg), "application/json");
        }
    }

    /// <summary>
    /// Adds upload buffering debug headers to the response.
    /// </summary>
    public static void AddBufferingDebugHeaders(HttpContext httpContext, string? tempFile)
    {
        if (!string.IsNullOrEmpty(tempFile) && File.Exists(tempFile))
        {
            try
            {
                var fi = new FileInfo(tempFile);
                httpContext.Response.Headers.Append("X-Upload-Buffered", "true");
                httpContext.Response.Headers.Append("X-Upload-Buffered-Size", fi.Length.ToString());
            }
            catch
            {
                // ignore errors getting file info
            }
        }
        else
        {
            httpContext.Response.Headers.Append("X-Upload-Buffered", "false");
        }
    }
}
