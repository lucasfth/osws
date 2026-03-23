namespace OSWS.WebApi.Models;

/// <summary>
/// Normalised user profile fetched from an OIDC userinfo endpoint.
/// Only fields that are commonly available across providers are included here.
/// </summary>
public record OidcUserInfo(
    string Subject,
    string? Name,
    string? PreferredUsername,
    string? Email
);
