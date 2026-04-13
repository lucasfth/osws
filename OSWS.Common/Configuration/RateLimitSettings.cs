namespace OSWS.Common.Configuration;

/// <summary>
/// Configuration for rate limiting. Bind from appsettings.json section "RateLimiting".
/// Set any value to 0 to disable that limit.
/// </summary>
public class RateLimitSettings
{
    /// <summary>
    /// Maximum S3 API requests per minute per client. Default: 600.
    /// </summary>
    public int S3RequestsPerMinute { get; set; } = 600;

    /// <summary>
    /// Maximum App API requests per minute per client. Default: 120.
    /// </summary>
    public int ApiRequestsPerMinute { get; set; } = 120;

    /// <summary>
    /// Maximum Admin API requests per minute per client. Default: 60.
    /// </summary>
    public int AdminRequestsPerMinute { get; set; } = 60;

    /// <summary>
    /// Maximum credential creation requests per hour per client. Default: 10.
    /// </summary>
    public int CredentialCreationsPerHour { get; set; } = 10;
}
