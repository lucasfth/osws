using System;
using System.Linq;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using OSWS.Common.Configuration;
using OSWS.ParquetSolver.Helpers;
using OSWS.WebApi.Endpoints;
using OSWS.WebApi.Endpoints.Admin;

namespace OSWS.WebApi.Extensions;

public static class EndpointExtensions
{
    public static WebApplication MapOswsEndpoints(
        this WebApplication app,
        EncryptionSettings encryptionSettings
    )
    {
        var logger = app.Logger;

        app.MapGet("/health", () => "OSWS Web API running");

        if (app.Environment.IsDevelopment())
        {
            app.MapGet(
                "/cache-stats",
                (EncryptedFileCache fileCache) => Results.Text(fileCache.GetDebugInfo())
            );

            app.MapOpenApi();
        }

        app.MapS3Routes();

        if (encryptionSettings.BenchmarkMode)
        {
            if (app.Environment.IsDevelopment())
            {
                logger.LogWarning("BenchmarkMode is ACTIVE — S3 benchmark mode enabled.");
            }
            else
            {
                logger.LogWarning(
                    "BenchmarkMode is enabled but ignored outside Development environment."
                );
            }
        }

        // Map App API routes (OIDC-protected, for the React frontend)
        app.MapAppRoutes();
        app.MapCredentialRoutes();
        app.MapAdminRoutes();

        return app;
    }
}
