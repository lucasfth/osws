namespace OSWS.Models.DTOs;

public class S3TaggingRequest
{
    public List<S3TagDto> Tags { get; set; } = new();
}
