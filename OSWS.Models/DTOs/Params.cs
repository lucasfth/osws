namespace OSWS.Models.DTOs;

/// <summary>
/// Parameters for S3 Get Object Request
/// </summary>
public class Params
{
    /// <summary>
    /// Bucket Name
    /// </summary>
    public required string Bucket { get; set; }
    
    /// <summary>
    /// Object Key
    /// </summary>
    public string? Key { get; set; }
    
    /// <summary>
    /// Version Identifier
    /// </summary>
    public string? Version { get; set; }
    
    /// <summary>
    /// Part Number for multipart objects
    /// </summary>
    public string? PartNumber { get; set; }
    
    /// <summary>
    /// Range Header
    /// </summary>
    public string? Range { get; set; }
}
