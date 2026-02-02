using Microsoft.AspNetCore.Mvc;
using OSWS.Models.DTOs;

namespace OSWS.WebApi.Interfaces;

/// <summary>
/// S3 Get Interfaces
/// </summary>
public interface IS3Get
{
    
    /// <summary>
    /// Get Object from S3 Compatible Storage
    /// </summary>
    /// <param name="prms"></param>
    /// <param name="s3Options"></param>
    /// <param name="retryOptions"></param>
    /// <param name="timeoutOptionsMs"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    /// <remarks>https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html</remarks>
    public Task<IActionResult> GetObject([FromQuery] Params prms, [FromQuery] S3Options s3Options, int retryOptions = 3,
        int timeoutOptionsMs = 3000, CancellationToken cancellationToken = default);
    
    
}
