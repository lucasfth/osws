using System.Security.Claims;
using Microsoft.EntityFrameworkCore;
using OSWS.KeyManager.Persistence;
using OSWS.Models.Entities;

namespace OSWS.WebApi.Services;

/// <summary>
///     Scoped service that exposes the authenticated <see cref="User" /> entity for the
///     current HTTP request. Works for both OIDC-authenticated (app routes) and
///     SigV4-authenticated (S3 routes) requests — both schemes place the user's database
///     ID in the <see cref="ClaimTypes.NameIdentifier" /> claim.
///     Inject this wherever you need the real User entity; the DB lookup is performed
///     lazily on first access and cached for the lifetime of the request.
///     Usage:
///     app.MapGet("/example", async (CurrentUser currentUser) =>
///     {
///     var user = await currentUser.ResolveAsync();
///     // user is null if the request is unauthenticated
///     });
/// </summary>
public sealed class CurrentUser(IHttpContextAccessor httpContextAccessor, OswsContext db)
{
    private User? _cached;
    private bool _resolved;

    /// <summary>
    ///     Whether the current request has an authenticated principal with a valid user ID claim.
    ///     Does NOT perform a DB lookup.
    /// </summary>
    public bool IsAuthenticated =>
        httpContextAccessor.HttpContext?.User?.Identity?.IsAuthenticated == true
        && httpContextAccessor.HttpContext.User.FindFirstValue(ClaimTypes.NameIdentifier)
            is not null;

    /// <summary>
    ///     Resolves and returns the <see cref="User" /> entity for the current request.
    ///     Returns <c>null</c> if the request is unauthenticated or the user ID claim is missing/invalid.
    ///     The result is cached for the lifetime of the request scope.
    /// </summary>
    public async Task<User?> ResolveAsync(CancellationToken cancellationToken = default)
    {
        if (_resolved)
            return _cached;

        _resolved = true;

        var principal = httpContextAccessor.HttpContext?.User;
        var idClaim = principal?.FindFirstValue(ClaimTypes.NameIdentifier);

        if (idClaim is null || !int.TryParse(idClaim, out var userId))
            return null;

        _cached = await db
            .Users.Include(u => u.RoleAssignments)
            .Include(u => u.Roles)
            .Include(u => u.ExternalIdentities)
            .FirstOrDefaultAsync(u => u.Id == userId, cancellationToken);

        return _cached;
    }
}
