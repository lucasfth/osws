using Microsoft.AspNetCore.Http;

namespace OSWS.WebApi.Interfaces;

public interface IS3Head
{
    Task<IResult> HeadObject(
        string bucket,
        string key,
        HttpRequest httpRequest,
        HttpResponse httpResponse,
        int retryOptions = 3,
        int timeoutOptionsMs = 3000,
        CancellationToken cancellationToken = default
    );
}
