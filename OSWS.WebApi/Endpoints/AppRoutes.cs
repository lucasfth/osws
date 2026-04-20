using System.Security.Claims;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using OSWS.Common.Configuration;
using OSWS.KeyManager.Persistence;
using OSWS.Models.Entities;
using OSWS.WebApi.Services;

namespace OSWS.WebApi.Endpoints;

public static class AppRoutes
{
    public static void MapAppRoutes(this IEndpointRouteBuilder app)
    {
        var api = app.MapGroup(prefix: "/api")
            .RequireAuthorization("OidcPolicy")
            .RequireRateLimiting("api");

        // GET /api/me
        // Returns the current user's profile. Creates the user on first login (JIT provisioning).
        api.MapGet(
            "/me",
            async (
                ClaimsPrincipal principal,
                HttpRequest httpRequest,
                [FromServices] OswsContext db,
                [FromServices] UserInfoService userInfoService,
                [FromServices] IConfiguration configuration,
                CancellationToken cancellationToken
            ) =>
            {
                var (provider, subject) = ExtractProviderAndSubject(principal);
                if (provider is null || subject is null)
                    return Results.Unauthorized();

                // Pull the raw access token from the Authorization header so we can call userinfo
                var accessToken = httpRequest
                    .Headers.Authorization.ToString()
                    .Replace("Bearer ", "", StringComparison.OrdinalIgnoreCase)
                    .Trim();

                // Fetch full profile from the provider's userinfo endpoint
                var providerSettings = configuration
                    .GetSection("OidcProviders")
                    .Get<List<OidcProviderSettings>>()
                    ?.FirstOrDefault(p => p.Name == provider);

                var userInfo = providerSettings is not null
                    ? await userInfoService.GetUserInfoAsync(
                        providerSettings,
                        accessToken,
                        cancellationToken
                    )
                    : null;

                // Resolve display name: userinfo > token claims > subject fallback
                var name =
                    userInfo?.Name
                    ?? userInfo?.PreferredUsername
                    ?? principal.FindFirstValue("name")
                    ?? principal.FindFirstValue("preferred_username")
                    ?? subject;

                var email = userInfo?.Email;

                var identity = await db
                    .ExternalIdentities.Include(e => e.User)
                        .ThenInclude(u => u.Roles)
                    .FirstOrDefaultAsync(
                        e => e.Provider == provider && e.Subject == subject,
                        cancellationToken
                    );

                var isRbacAdmin =
                    (userInfo?.IsAdmin ?? false)
                    || principal.HasClaim(c =>
                        c.Type == "isRbacAdmin" && (c.Value == "true" || c.Value == "True")
                    );

                if (identity is null)
                {
                    // JIT: provision a new user on first OIDC login
                    var user = new User
                    {
                        Name = name,
                        Email = email,
                        IsRbacAdmin = isRbacAdmin,
                    };

                    identity = new ExternalIdentity
                    {
                        Provider = provider,
                        Subject = subject,
                        Email = email,
                        User = user,
                    };

                    db.Users.Add(user);
                    db.ExternalIdentities.Add(identity);
                }
                else
                {
                    // Sync name, email, and admin status on every login
                    identity.LastSeenAt = DateTime.UtcNow;
                    identity.User.Name = name;
                    identity.User.IsRbacAdmin = isRbacAdmin;

                    if (email is not null)
                    {
                        identity.Email = email;
                        identity.User.Email = email;
                    }
                }

                await db.SaveChangesAsync(cancellationToken);

                return Results.Ok(
                    new
                    {
                        identity.User.Id,
                        identity.User.Name,
                        identity.User.Email,
                        Provider = provider,
                        Roles = identity.User.Roles.Select(r => new { r.Id, r.Name }),
                        IsRbacAdmin = isRbacAdmin,
                    }
                );
            }
        );
    }

    /// <summary>
    /// Extracts the logical provider name and the 'sub' claim from the JWT principal.
    /// The authentication scheme name (set in Program.cs per provider) is stored as the
    /// identity's AuthenticationType, which matches the provider Name from config.
    /// </summary>
    private static (string? provider, string? subject) ExtractProviderAndSubject(
        ClaimsPrincipal principal
    )
    {
        foreach (var identity in principal.Identities)
        {
            if (!identity.IsAuthenticated)
                continue;

            var subject =
                identity.FindFirst(ClaimTypes.NameIdentifier)?.Value
                ?? identity.FindFirst("sub")?.Value;

            if (subject is null)
                continue;

            return (identity.AuthenticationType, subject);
        }

        return (null, null);
    }
}
