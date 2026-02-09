namespace OSWS.Models.DTOs;

/// <summary>
/// S3 Compatible Storage Options
/// </summary>
public class S3Options
{
    /// <summary>
    /// AWS Region
    /// </summary>
    public required string Region { get; set; }

    /// <summary>
    /// AWS SDK V2 Credentials in JSON format
    /// </summary>
    public string? V2AwsSdkCredentials { get; set; }

    /// <summary>
    /// Endpoint Hostname for S3 Compatible Storage
    /// </summary>
    public required string EndpointHostname { get; set; }

    /// <summary>
    /// User Agent String
    /// </summary>
    public string? Agent { get; set; }

    /// <summary>
    /// AWS SDK V3 Credentials in JSON format
    /// </summary>
    public string? V3AwsSdkCredentials { get; set; }
}
