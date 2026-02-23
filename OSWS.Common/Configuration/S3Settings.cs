namespace OSWS.Common.Configuration;

/// <summary>
/// S3 Compatible Storage Options
/// </summary>
public class S3Settings
{
    /// <summary>
    /// AWS Region
    /// </summary>
    public string? Region { get; set; }

    /// <summary>
    /// Access key ID
    /// </summary>
    public required string AccessKeyId { get; set; }

    /// <summary>
    /// Secret access key
    /// </summary>
    public required string SecretAccessKey { get; set; }

    /// <summary>
    /// Endpoint Hostname for S3 Compatible Storage
    /// </summary>
    public required string EndpointHostname { get; set; }

    /// <summary>
    /// User Agent String
    /// </summary>
    public string? Agent { get; set; }
}
