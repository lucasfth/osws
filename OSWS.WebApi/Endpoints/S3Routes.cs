using Microsoft.AspNetCore.Mvc;
using OSWS.Models.DTOs;
using OSWS.WebApi.Interfaces;

namespace OSWS.WebApi.Endpoints;

public static class S3Routes
{
    private const int DefaultRetryOptions = 3;
    private const int DefaultTimeoutOptionsMs = 3000;
    public static IEndpointRouteBuilder MapS3Routes(this IEndpointRouteBuilder app)
    {
        // S3 GET - path-style routing for S3 compatibility: /{bucket}/{*key} and /{bucket}
        var s3GetHandler = async (
                [FromServices] IS3Get s3Get,
                string bucket,
                string? key,
                [AsParameters] Params prms,
                [AsParameters] S3Options s3Options,
                HttpRequest httpRequest,
                HttpResponse httpResponse,
                [FromQuery] int retryOptions = DefaultRetryOptions,
                [FromQuery] int timeoutOptionsMs = DefaultTimeoutOptionsMs,
                CancellationToken cancellationToken = default) =>
        {
            return await s3Get.GetObject(bucket, key, prms, s3Options, httpRequest, httpResponse, retryOptions, timeoutOptionsMs, cancellationToken);
        };

        app.MapGet("/{bucket}/{*key}", s3GetHandler);

        // S3 PUT - path-style routing for S3 compatibility: /{bucket}/{*key}
        var s3PutHandler = async (
                [FromServices] IS3Put s3Put,
                string bucket,
                string? key,
                [AsParameters] Params prms,
                [AsParameters] S3Options s3Options,
                HttpRequest httpRequest,
                [FromQuery] int retryOptions = DefaultRetryOptions,
                [FromQuery] int timeoutOptionsMs = DefaultTimeoutOptionsMs,
                CancellationToken cancellationToken = default) =>
        {
            return await s3Put.PutObject(bucket, key, prms, s3Options, httpRequest, retryOptions, timeoutOptionsMs, cancellationToken);
        };

        app.MapPut("/{bucket}/{*key}", s3PutHandler);

        return app;
    }
}
