using System.Security.Claims;
using Microsoft.AspNetCore.Mvc;
using OSWS.Models.DTOs;
using OSWS.WebApi.Interfaces;
using OSWS.WebApi.Services;

namespace OSWS.WebApi.Endpoints;

public static class S3Routes
{
    private const int DefaultRetryOptions = 3;
    private const int DefaultTimeoutOptionsMs = 3000;

    // Reserved routes that should not be treated as bucket names
    private static readonly HashSet<string> ReservedRoutes = new(StringComparer.OrdinalIgnoreCase)
    {
        "health",
        "cache-stats",
        "api",
        "swagger",
        "openapi",
    };

    public static IEndpointRouteBuilder MapS3Routes(this IEndpointRouteBuilder app)
    {
        var s3 = app.MapGroup(prefix: "")
            .RequireAuthorization("SigV4Policy")
            .RequireRateLimiting("s3");

        // S3 GET - path-style routing for S3 compatibility: /{bucket}/{**key}
        s3.MapGet(
            "/{bucket}/{**key}",
            async (
                [FromServices] IS3Get s3Get,
                [FromServices] IS3List s3List,
                [FromServices] CurrentUser currentUser,
                string bucket,
                string? key,
                [AsParameters] Params prms,
                HttpRequest httpRequest,
                HttpResponse httpResponse,
                [FromQuery] int retryOptions = DefaultRetryOptions,
                [FromQuery] int timeoutOptionsMs = DefaultTimeoutOptionsMs,
                CancellationToken cancellationToken = default
            ) =>
            {
                if (ReservedRoutes.Contains(bucket))
                {
                    return Results.NotFound();
                }
                if (string.IsNullOrEmpty(key))
                {
                    return Results.NotFound();
                }

                if (httpRequest.Query.TryGetValue("uploadId", out var uploadIdValues))
                {
                    var uploadId = uploadIdValues.FirstOrDefault();
                    if (!string.IsNullOrWhiteSpace(uploadId))
                    {
                        return await s3List.ListParts(
                            bucket,
                            key,
                            uploadId,
                            httpRequest,
                            retryOptions,
                            timeoutOptionsMs,
                            cancellationToken
                        );
                    }
                }

                return await s3Get.GetObject(
                    bucket,
                    key,
                    prms,
                    httpRequest,
                    httpResponse,
                    retryOptions,
                    timeoutOptionsMs,
                    cancellationToken
                );
            }
        );

        // S3 PUT - path-style routing for S3 compatibility: /{bucket}/{*key}
        s3.MapPut(
            "/{bucket}/{*key}",
            async (
                [FromServices] IS3Put s3Put,
                string bucket,
                string? key,
                [AsParameters] Params prms,
                HttpRequest httpRequest,
                [FromQuery] int retryOptions = DefaultRetryOptions,
                [FromQuery] int timeoutOptionsMs = DefaultTimeoutOptionsMs,
                CancellationToken cancellationToken = default
            ) =>
            {
                if (key == null && ReservedRoutes.Contains(bucket))
                {
                    return Results.NotFound();
                }
                return await s3Put.PutObject(
                    bucket,
                    key,
                    prms,
                    httpRequest,
                    retryOptions,
                    timeoutOptionsMs,
                    cancellationToken
                );
            }
        );

        // S3 LIST BUCKETS - path-style routing for S3 compatibility: /
        s3.MapGet(
            "/",
            async (
                [FromServices] IS3List s3List,
                HttpRequest httpRequest,
                [FromQuery] int retryOptions = DefaultRetryOptions,
                [FromQuery] int timeoutOptionsMs = DefaultTimeoutOptionsMs,
                CancellationToken cancellationToken = default
            ) =>
                await s3List.ListBuckets(
                    httpRequest,
                    retryOptions,
                    timeoutOptionsMs,
                    cancellationToken
                )
        );

        // S3 LIST OBJECTS - path-style routing for S3 compatibility: /{bucket}
        s3.MapGet(
            "/{bucket}",
            async (
                [FromServices] IS3List s3List,
                string bucket,
                HttpRequest httpRequest,
                [FromQuery] int retryOptions = DefaultRetryOptions,
                [FromQuery] int timeoutOptionsMs = DefaultTimeoutOptionsMs,
                CancellationToken cancellationToken = default
            ) =>
            {
                if (ReservedRoutes.Contains(bucket))
                {
                    return Results.NotFound();
                }

                if (httpRequest.Query.ContainsKey("uploads"))
                {
                    return await s3List.ListMultipartUploads(
                        bucket,
                        httpRequest,
                        retryOptions,
                        timeoutOptionsMs,
                        cancellationToken
                    );
                }

                return await s3List.ListObjects(
                    bucket,
                    httpRequest,
                    retryOptions,
                    timeoutOptionsMs,
                    cancellationToken
                );
            }
        );

        // S3 HEAD - path-style routing for S3 compatibility: /{bucket}/{**key}
        s3.MapMethods(
            "/{bucket}/{**key}",
            new[] { "HEAD" },
            async (
                [FromServices] IS3Head s3Head,
                string bucket,
                string? key,
                HttpRequest httpRequest,
                HttpResponse httpResponse,
                [FromQuery] int retryOptions = DefaultRetryOptions,
                [FromQuery] int timeoutOptionsMs = DefaultTimeoutOptionsMs,
                CancellationToken cancellationToken = default
            ) =>
            {
                if (ReservedRoutes.Contains(bucket))
                {
                    return Results.NotFound();
                }
                if (string.IsNullOrEmpty(key))
                {
                    return Results.NotFound();
                }
                return await s3Head.HeadObject(
                    bucket,
                    key,
                    httpRequest,
                    httpResponse,
                    retryOptions,
                    timeoutOptionsMs,
                    cancellationToken
                );
            }
        );

        // S3 MULTI-OBJECT DELETE - path-style routing: POST /{bucket}?delete
        s3.MapPost(
            "/{bucket}",
            async (
                [FromServices] IS3Delete s3Delete,
                string bucket,
                HttpRequest httpRequest,
                CancellationToken cancellationToken = default
            ) =>
            {
                if (ReservedRoutes.Contains(bucket))
                {
                    return Results.NotFound();
                }

                if (httpRequest.Query.ContainsKey("delete"))
                {
                    return await s3Delete.DeleteObjects(bucket, httpRequest, cancellationToken);
                }

                return Results.StatusCode(StatusCodes.Status405MethodNotAllowed);
            }
        );

        // S3 HEAD BUCKET - path-style routing for S3 compatibility: HEAD /{bucket}
        s3.MapMethods(
            "/{bucket}",
            new[] { "HEAD" },
            async (
                [FromServices] IS3Head s3Head,
                string bucket,
                HttpRequest httpRequest,
                HttpResponse httpResponse,
                CancellationToken cancellationToken = default
            ) =>
            {
                if (ReservedRoutes.Contains(bucket))
                {
                    return Results.NotFound();
                }

                return await s3Head.HeadBucket(
                    bucket,
                    httpRequest,
                    httpResponse,
                    cancellationToken
                );
            }
        );

        // S3 CREATE BUCKET - path-style routing for S3 compatibility: PUT /{bucket}
        s3.MapPut(
            "/{bucket}",
            async (
                [FromServices] IS3Put s3Put,
                string bucket,
                HttpContext httpContext,
                CancellationToken cancellationToken = default
            ) =>
            {
                if (ReservedRoutes.Contains(bucket))
                {
                    return Results.NotFound();
                }
                return await s3Put.CreateBucket(bucket, httpContext, cancellationToken);
            }
        );

        // S3 DELETE OBJECT - path-style routing for S3 compatibility: DELETE /{bucket}/{**key}
        s3.MapDelete(
            "/{bucket}/{**key}",
            async (
                [FromServices] IS3Delete s3Delete,
                string bucket,
                string key,
                [AsParameters] Params prms,
                HttpRequest httpRequest,
                [FromQuery] int retryOptions = DefaultRetryOptions,
                [FromQuery] int timeoutOptionsMs = DefaultTimeoutOptionsMs,
                CancellationToken cancellationToken = default
            ) =>
            {
                if (ReservedRoutes.Contains(bucket))
                {
                    return Results.NotFound();
                }
                return await s3Delete.DeleteObject(
                    bucket,
                    key,
                    prms,
                    httpRequest,
                    retryOptions,
                    timeoutOptionsMs,
                    cancellationToken
                );
            }
        );

        // S3 DELETE BUCKET - path-style routing for S3 compatibility: DELETE /{bucket}
        s3.MapDelete(
            "/{bucket}",
            async (
                [FromServices] IS3Delete s3Delete,
                string bucket,
                HttpRequest httpRequest,
                CancellationToken cancellationToken = default
            ) =>
            {
                if (ReservedRoutes.Contains(bucket))
                {
                    return Results.NotFound();
                }

                return await s3Delete.DeleteBucket(bucket, httpRequest, cancellationToken);
            }
        );

        return app;
    }
}
