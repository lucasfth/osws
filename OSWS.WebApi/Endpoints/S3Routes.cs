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
        app.MapGet(
            "/{bucket}/{*key}",
            async (
                [FromServices] IS3Get s3Get,
                string bucket,
                string? key,
                [AsParameters] Params prms,
                [AsParameters] S3Options s3Options,
                HttpRequest httpRequest,
                HttpResponse httpResponse,
                [FromQuery] int retryOptions = DefaultRetryOptions,
                [FromQuery] int timeoutOptionsMs = DefaultTimeoutOptionsMs,
                CancellationToken cancellationToken = default
            ) =>
                await s3Get.GetObject(
                    bucket,
                    key,
                    prms,
                    s3Options,
                    httpRequest,
                    httpResponse,
                    retryOptions,
                    timeoutOptionsMs,
                    cancellationToken
                )
        );

        // S3 PUT - path-style routing for S3 compatibility: /{bucket}/{*key}
        app.MapPut(
            "/{bucket}/{*key}",
            async (
                [FromServices] IS3Put s3Put,
                string bucket,
                string? key,
                [AsParameters] Params prms,
                [AsParameters] S3Options s3Options,
                HttpRequest httpRequest,
                [FromQuery] int retryOptions = DefaultRetryOptions,
                [FromQuery] int timeoutOptionsMs = DefaultTimeoutOptionsMs,
                CancellationToken cancellationToken = default
            ) =>
                await s3Put.PutObject(
                    bucket,
                    key,
                    prms,
                    s3Options,
                    httpRequest,
                    retryOptions,
                    timeoutOptionsMs,
                    cancellationToken
                )
        );

        return app;
    }
}
