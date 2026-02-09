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
    /// <param name="bucket"></param>
    /// <param name="key"></param>
    /// <param name="prms"></param>
    /// <param name="s3Options"></param>
    /// <param name="retryOptions"></param>
    /// <param name="timeoutOptionsMs"></param>
    /// <param name="cancellationToken"></param>
    /// <returns>IResult</returns>
    /// <remarks>https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html</remarks>
    public Task<IResult> PutObject(string bucket, string? key, [FromQuery] Params prms,
        [FromQuery] S3Options s3Options, HttpRequest httpRequest, [FromQuery] int retryOptions = 3, [FromQuery] int timeoutOptionsMs = 3000,
        CancellationToken cancellationToken = default);
}
