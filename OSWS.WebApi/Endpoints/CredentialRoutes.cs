using System.Security.Claims;
using System.Security.Cryptography;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using OSWS.KeyManager.Persistence;
using OSWS.Models.Entities;

namespace OSWS.WebApi.Endpoints;

public static class CredentialRoutes
{
    public static void MapCredentialRoutes(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/api/credentials").RequireAuthorization("OidcPolicy").RequireRateLimiting("credential-create");

        group.MapGet(
            "/",
            async (
                ClaimsPrincipal principal,
                [FromServices] OswsContext db,
                CancellationToken ct
            ) =>
            {
                var user = await ResolveUserAsync(principal, db, ct);
                if (user is null)
                    return Results.Unauthorized();

                var credentials = await db
                    .S3Credentials.Where(c => c.UserId == user.Id && c.IsActive)
                    .Include(c => c.DefaultRole)
                    .Select(c => new
                    {
                        c.Id,
                        c.AccessKeyId,
                        c.IsActive,
                        c.CreatedAt,
                        c.DefaultRoleId,
                        DefaultRoleName = c.DefaultRole != null ? c.DefaultRole.Name : null,
                    })
                    .ToListAsync(ct);

                return Results.Ok(credentials);
            }
        );

        group.MapPost(
            "/",
            async (
                ClaimsPrincipal principal,
                [FromBody] CreateCredentialRequest? body,
                [FromServices] OswsContext db,
                CancellationToken ct
            ) =>
            {
                var user = await ResolveUserAsync(principal, db, ct);
                if (user is null)
                    return Results.Unauthorized();

                Role? defaultRole = null;
                if (body?.DefaultRoleId is int roleId)
                {
                    defaultRole = await db.Roles.FindAsync([roleId], ct);
                    if (defaultRole is null)
                        return Results.BadRequest("Role not found.");
                }

                var credential = new S3Credential
                {
                    AccessKeyId = GenerateAccessKeyId(),
                    SecretKey = GenerateSecretKey(),
                    UserId = user.Id,
                    User = user,
                    IsActive = true,
                    DefaultRole = defaultRole,
                };

                db.S3Credentials.Add(credential);
                await db.SaveChangesAsync(ct);

                return Results.Ok(
                    new
                    {
                        credential.Id,
                        credential.AccessKeyId,
                        credential.SecretKey,
                        credential.CreatedAt,
                        credential.DefaultRoleId,
                        DefaultRoleName = defaultRole?.Name,
                    }
                );
            }
        );

        group.MapDelete(
            "/{id:int}",
            async (
                int id,
                ClaimsPrincipal principal,
                [FromServices] OswsContext db,
                CancellationToken ct
            ) =>
            {
                var user = await ResolveUserAsync(principal, db, ct);
                if (user is null)
                    return Results.Unauthorized();

                var credential = await db.S3Credentials.FindAsync([id], ct);
                if (credential is null)
                    return Results.NotFound();
                if (credential.UserId != user.Id)
                    return Results.Forbid();

                credential.IsActive = false;
                await db.SaveChangesAsync(ct);
                return Results.NoContent();
            }
        );
    }

    private static async Task<User?> ResolveUserAsync(
        ClaimsPrincipal principal,
        OswsContext db,
        CancellationToken ct
    )
    {
        var (provider, subject) = ExtractProviderAndSubject(principal);
        if (provider is null || subject is null)
            return null;

        var identity = await db
            .ExternalIdentities.Include(e => e.User)
            .FirstOrDefaultAsync(e => e.Provider == provider && e.Subject == subject, ct);

        return identity?.User;
    }

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

    private static string GenerateAccessKeyId()
    {
        const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        var bytes = RandomNumberGenerator.GetBytes(20);
        return new string(bytes.Select(b => chars[b % chars.Length]).ToArray());
    }

    private static string GenerateSecretKey()
    {
        var bytes = RandomNumberGenerator.GetBytes(40);
        return Convert.ToBase64String(bytes).Replace('+', '-').Replace('/', '_').TrimEnd('=');
    }
}

public class CreateCredentialRequest
{
    public int? DefaultRoleId { get; set; }
}
