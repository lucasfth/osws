using System.Security.Claims;
using System.Text.Encodings.Web;
using Microsoft.AspNetCore.Authentication;
using Microsoft.Extensions.Options;

namespace OSWS.WebApi.Authentication;

/// <summary>
/// Dev-only authentication handler that trusts the X-E2E-User-Id header.
/// Creates a ClaimsPrincipal with ClaimTypes.NameIdentifier set to the header value,
/// allowing CurrentUser.ResolveAsync() to look up the user by integer ID.
///
/// Only registered when App:E2EMode=true AND ASPNETCORE_ENVIRONMENT=Development.
/// </summary>
public class E2EAuthenticationHandler(
    IOptionsMonitor<AuthenticationSchemeOptions> options,
    ILoggerFactory logger,
    UrlEncoder encoder
) : AuthenticationHandler<AuthenticationSchemeOptions>(options, logger, encoder)
{
    public const string SchemeName = "E2EScheme";
    public const string HeaderName = "X-E2E-User-Id";

    protected override Task<AuthenticateResult> HandleAuthenticateAsync()
    {
        if (!Request.Headers.TryGetValue(HeaderName, out var headerValue))
            return Task.FromResult(AuthenticateResult.NoResult());

        var userId = headerValue.ToString();
        if (string.IsNullOrWhiteSpace(userId) || !int.TryParse(userId, out _))
            return Task.FromResult(
                AuthenticateResult.Fail("X-E2E-User-Id must be a valid integer user ID.")
            );

        var claims = new[]
        {
            new Claim(ClaimTypes.NameIdentifier, userId),
        };

        var identity = new ClaimsIdentity(claims, SchemeName);
        var principal = new ClaimsPrincipal(identity);
        var ticket = new AuthenticationTicket(principal, SchemeName);

        return Task.FromResult(AuthenticateResult.Success(ticket));
    }
}
