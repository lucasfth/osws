using Microsoft.AspNetCore.Mvc;
using OSWS.Models.DTOs;

namespace OSWS.WebApi.Interfaces;

/// <summary>
/// S3 Put Interfaces
/// </summary>
public interface IS3Put
{
    /// <summary>
    /// Put Object to S3 Compatible Storage
    /// </summary>
    /// <remarks>https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html</remarks>
    public Task<IResult> PutObject(
        string bucket,
        string? key,
        [FromQuery] Params prms,
        HttpRequest httpRequest,
        [FromQuery] int retryOptions = 3,
        [FromQuery] int timeoutOptionsMs = 3000,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Create a bucket in S3 Compatible Storage
    /// </summary>
    /// <remarks>https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucket.html</remarks>
    public Task<IResult> CreateBucket(
        string bucket,
        HttpContext httpContext,
        CancellationToken cancellationToken = default
    );
}
