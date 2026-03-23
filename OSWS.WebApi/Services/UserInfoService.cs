using System.Net.Http.Headers;
using System.Text.Json;
using OSWS.Common.Configuration;
using OSWS.WebApi.Models;

namespace OSWS.WebApi.Services;

/// <summary>
/// Fetches user profile claims from an OIDC provider's userinfo endpoint.
/// The userinfo URL is derived from the provider's authority using standard OIDC discovery,
/// falling back to the conventional {authority}/userinfo path.
/// </summary>
public class UserInfoService(IHttpClientFactory httpClientFactory, ILogger<UserInfoService> logger)
{
    // Cache discovery results so we don't hit /.well-known on every request
    private readonly Dictionary<string, string> _userInfoEndpointCache = new();
    private readonly SemaphoreSlim _lock = new(1, 1);

    public async Task<OidcUserInfo?> GetUserInfoAsync(
        OidcProviderSettings provider,
        string accessToken,
        CancellationToken cancellationToken = default
    )
    {
        var userInfoUrl = await ResolveUserInfoEndpointAsync(provider, cancellationToken);
        if (userInfoUrl is null)
            return null;

        try
        {
            var client = httpClientFactory.CreateClient("UserInfo");
            using var request = new HttpRequestMessage(HttpMethod.Get, userInfoUrl);
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);

            using var response = await client.SendAsync(request, cancellationToken);
            if (!response.IsSuccessStatusCode)
            {
                logger.LogWarning(
                    "Userinfo endpoint {Url} returned {Status} for provider {Provider}",
                    userInfoUrl,
                    response.StatusCode,
                    provider.Name
                );
                return null;
            }

            var json = await response.Content.ReadAsStringAsync(cancellationToken);
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;

            return new OidcUserInfo(
                Subject: root.GetStringOrNull("sub") ?? string.Empty,
                Name: root.GetStringOrNull("name"),
                PreferredUsername: root.GetStringOrNull("preferred_username"),
                Email: root.GetStringOrNull("email")
            );
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to fetch userinfo from {Provider}", provider.Name);
            return null;
        }
    }

    private async Task<string?> ResolveUserInfoEndpointAsync(
        OidcProviderSettings provider,
        CancellationToken cancellationToken
    )
    {
        await _lock.WaitAsync(cancellationToken);
        try
        {
            if (_userInfoEndpointCache.TryGetValue(provider.Name, out var cached))
                return cached;

            // Try OIDC discovery first
            var discoveryUrl =
                provider.Authority.TrimEnd('/') + "/.well-known/openid-configuration";
            try
            {
                var client = httpClientFactory.CreateClient("UserInfo");
                using var resp = await client.GetAsync(discoveryUrl, cancellationToken);
                if (resp.IsSuccessStatusCode)
                {
                    var json = await resp.Content.ReadAsStringAsync(cancellationToken);
                    using var doc = JsonDocument.Parse(json);
                    if (
                        doc.RootElement.TryGetProperty("userinfo_endpoint", out var ep)
                        && ep.GetString() is string discovered
                    )
                    {
                        _userInfoEndpointCache[provider.Name] = discovered;
                        return discovered;
                    }
                }
            }
            catch (Exception ex)
            {
                logger.LogWarning(
                    ex,
                    "OIDC discovery failed for {Provider}, falling back to conventional URL",
                    provider.Name
                );
            }

            // Fall back to conventional {authority}/userinfo
            var fallback = provider.Authority.TrimEnd('/') + "/userinfo";
            _userInfoEndpointCache[provider.Name] = fallback;
            return fallback;
        }
        finally
        {
            _lock.Release();
        }
    }
}

internal static class JsonElementExtensions
{
    public static string? GetStringOrNull(this JsonElement element, string propertyName) =>
        element.TryGetProperty(propertyName, out var prop) ? prop.GetString() : null;
}
