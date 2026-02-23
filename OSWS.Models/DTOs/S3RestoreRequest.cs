namespace OSWS.Models.DTOs;

public class S3RestoreRequest
{
    public int Days { get; set; } = 1;
    public string? Tier { get; set; }
}
