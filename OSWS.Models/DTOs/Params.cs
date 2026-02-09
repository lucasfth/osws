namespace OSWS.Models.DTOs;

/// <summary>
/// Parameters for S3 Get Object Request
/// </summary>
public class Params
{
    /// <summary>
    /// Version Identifier
    /// </summary>
    public string? Version { get; set; }

    /// <summary>
    /// Part Number for multipart objects
    /// </summary>
    public string? PartNumber { get; set; }
}
