namespace OSWS.Common.Configuration;

/// <summary>
/// Configuration for a single OIDC provider.
/// </summary>
public class OidcProviderSettings
{
    /// <summary>
    /// Logical name used as the authentication scheme name, e.g. "pocketid".
    /// Must be URL-safe and unique across providers.
    /// </summary>
    public required string Name { get; set; }

    /// <summary>
    /// OIDC Authority / Issuer URL, e.g. "https://id.example.com".
    /// The middleware will auto-discover /.well-known/openid-configuration.
    /// </summary>
    public required string Authority { get; set; }

    /// <summary>
    /// Expected 'aud' claim value — typically the OAuth2 client ID registered with the provider.
    /// </summary>
    public required string Audience { get; set; }

    /// <summary>
    /// Human-readable display name shown in UIs/logs, e.g. "PocketID".
    /// </summary>
    public string? DisplayName { get; set; }
}
