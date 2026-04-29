using Microsoft.AspNetCore.Mvc;
using OSWS.Models.DTOs;

namespace OSWS.WebApi.Interfaces;

/// <summary>
/// S3 Delete Interface
/// </summary>
public interface IS3Delete
{
    /// <summary>
    /// Delete Object from S3 Compatible Storage
    /// </summary>
    /// <remarks>https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html</remarks>
    public Task<IResult> DeleteObject(
        string bucket,
        string key,
        [FromQuery] Params prms,
        HttpRequest httpRequest,
        [FromQuery] int retryOptions = 3,
        [FromQuery] int timeoutOptionsMs = 3000,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Delete multiple objects in a single request.
    /// </summary>
    /// <remarks>https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html</remarks>
    public Task<IResult> DeleteObjects(
        string bucket,
        HttpRequest httpRequest,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Delete a bucket.
    /// </summary>
    /// <remarks>https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucket.html</remarks>
    public Task<IResult> DeleteBucket(
        string bucket,
        HttpRequest httpRequest,
        CancellationToken cancellationToken = default
    );
}
