using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Authorization;
using OSWS.Common.Configuration;
using OSWS.WebApi.Authentication;

namespace OSWS.WebApi.Extensions;

public static class AuthenticationExtensions
{
    /// <summary>
    ///     Registers OIDC JWT Bearer schemes (one per configured provider), the SigV4 scheme,
    ///     and the OidcPolicy / AdminPolicy / SigV4Policy authorization policies.
    ///     Also registers <see cref="RbacAdminHandler" /> as an <see cref="IAuthorizationHandler" />.
    /// </summary>
    public static IServiceCollection AddOswsAuthentication(
        this IServiceCollection services,
        IConfiguration configuration,
        bool isDevelopment
    )
    {
        // --- OIDC Authentication (App API scope) ---
        // Load provider list from config; each entry becomes its own JWT Bearer scheme.
        // Adding a new provider only requires a new entry in appsettings.json — no code changes.
        var oidcProviders =
            configuration.GetSection("OidcProviders").Get<List<OidcProviderSettings>>() ?? [];

        if (oidcProviders.Count == 0)
        {
            Console.WriteLine(
                "WARNING: No OidcProviders configured. /api routes will be inaccessible."
            );
        }

        var authBuilder = services.AddAuthentication();

        foreach (var provider in oidcProviders)
        {
            // Each provider is registered as a distinct JWT Bearer scheme named after the provider.
            // This allows per-scheme token validation while sharing a single authorization policy.
            authBuilder.AddJwtBearer(
                provider.Name,
                options =>
                {
                    options.Authority = provider.Authority;
                    options.Audience = provider.Audience;

                    // The AuthenticationType on the resulting ClaimsIdentity will be set to the scheme
                    // name so AppRoutes can identify which provider authenticated the user.
                    options.TokenValidationParameters.AuthenticationType = provider.Name;

                    // We want to token as-is so we can discover claims and info from the provider
                    options.MapInboundClaims = false;

                    // Allow HTTP authorities in development (e.g. local PocketID without TLS)
                    if (isDevelopment)
                        options.RequireHttpsMetadata = false;
                }
            );
        }

        // SigV4 scheme — used exclusively by the /s3 route group.
        authBuilder.AddScheme<SigV4AuthenticationOptions, SigV4AuthenticationHandler>(
            "SigV4",
            _ => { }
        );

        // OidcPolicy: accepts a valid JWT from ANY configured provider.
        // SigV4Policy: accepts a valid SigV4-signed request (used by S3 routes).
        services.AddAuthorization(authOpts =>
        {
            var schemeNames = oidcProviders.Select(p => p.Name).ToArray();

            authOpts.AddPolicy(
                "OidcPolicy",
                policy =>
                {
                    policy.RequireAuthenticatedUser();
                    if (schemeNames.Length > 0)
                        policy.AddAuthenticationSchemes(schemeNames);
                }
            );

            authOpts.AddPolicy(
                "AdminPolicy",
                policy =>
                {
                    policy.RequireAuthenticatedUser();
                    if (schemeNames.Length > 0)
                        policy.AddAuthenticationSchemes(schemeNames);
                    policy.AddRequirements(new RbacAdminRequirement());
                }
            );

            authOpts.AddPolicy(
                "SigV4Policy",
                policy =>
                {
                    policy.RequireAuthenticatedUser();
                    policy.AddAuthenticationSchemes("SigV4");
                }
            );
        });

        services.AddScoped<IAuthorizationHandler, RbacAdminHandler>();

        return services;
    }
}
