using Microsoft.AspNetCore.Http;

namespace OSWS.WebApi.Interfaces;

public interface IS3List
{
    Task<IResult> ListBuckets(
        HttpRequest httpRequest,
        int retryOptions = 3,
        int timeoutOptionsMs = 3000,
        CancellationToken cancellationToken = default
    );

    Task<IResult> ListObjects(
        string bucket,
        HttpRequest httpRequest,
        int retryOptions = 3,
        int timeoutOptionsMs = 3000,
        CancellationToken cancellationToken = default
    );

    Task<IResult> ListMultipartUploads(
        string bucket,
        HttpRequest httpRequest,
        int retryOptions = 3,
        int timeoutOptionsMs = 3000,
        CancellationToken cancellationToken = default
    );

    Task<IResult> ListParts(
        string bucket,
        string key,
        string uploadId,
        HttpRequest httpRequest,
        int retryOptions = 3,
        int timeoutOptionsMs = 3000,
        CancellationToken cancellationToken = default
    );
}
