using System.Security.Claims;
using Microsoft.EntityFrameworkCore;
using OSWS.KeyManager.Persistence;
using OSWS.Models.Entities;

namespace OSWS.WebApi.Services;

/// <summary>
///     Scoped service that exposes the authenticated <see cref="User" /> entity for the
///     current HTTP request. Supports two authentication paths:
///     <list type="bullet">
///         <item>
///             <b>SigV4</b> — the handler places the DB user ID in
///             <see cref="ClaimTypes.NameIdentifier" />; resolved via a direct PK lookup.
///         </item>
///         <item>
///             <b>OIDC JWT Bearer</b> — <c>MapInboundClaims = false</c> means the <c>sub</c>
///             claim is stored as <c>"sub"</c>, not mapped to NameIdentifier; resolved via
///             <c>ExternalIdentities</c>.
///         </item>
///     </list>
///     The result is cached for the lifetime of the request scope.
/// </summary>
public sealed class CurrentUser(IHttpContextAccessor httpContextAccessor, OswsContext db)
{
    private User? _cached;
    private bool _resolved;

    /// <summary>
    ///     Whether the current request has an authenticated principal.
    ///     Does NOT perform a DB lookup.
    /// </summary>
    public bool IsAuthenticated =>
        httpContextAccessor.HttpContext?.User?.Identity?.IsAuthenticated == true;

    /// <summary>
    ///     Resolves and returns the <see cref="User" /> entity for the current request.
    ///     Returns <c>null</c> if the request is unauthenticated or the principal cannot
    ///     be matched to a user in the database.
    ///     The result is cached for the lifetime of the request scope.
    /// </summary>
    public async Task<User?> ResolveAsync(CancellationToken cancellationToken = default)
    {
        if (_resolved)
            return _cached;

        _resolved = true;

        var principal = httpContextAccessor.HttpContext?.User;
        if (principal is null)
            return null;

        // SigV4 path: the handler stores the integer DB user ID as NameIdentifier
        var idClaim = principal.FindFirstValue(ClaimTypes.NameIdentifier);
        if (idClaim is not null && int.TryParse(idClaim, out var userId))
        {
            _cached = await db
                .Users.Include(u => u.RoleAssignments)
                .Include(u => u.Roles)
                .Include(u => u.ExternalIdentities)
                .FirstOrDefaultAsync(u => u.Id == userId, cancellationToken);
            return _cached;
        }

        // OIDC path: Look up via ExternalIdentities using the raw sub claim.
        var subject = principal.FindFirstValue("sub");
        if (subject is not null)
        {
            _cached = await db
                .ExternalIdentities.Include(e => e.User)
                    .ThenInclude(u => u.RoleAssignments)
                .Include(e => e.User)
                    .ThenInclude(u => u.Roles)
                .Where(e => e.Subject == subject)
                .Select(e => e.User)
                .FirstOrDefaultAsync(cancellationToken);
            return _cached;
        }

        return null;
    }
}
