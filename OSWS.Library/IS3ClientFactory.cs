using Amazon.S3;
using OSWS.Models.DTOs;

namespace OSWS.Library;

public interface IS3ClientFactory
{
    /// <summary>
    /// Gets an IAmazonS3 client instance based on the provided S3Options. This allows for dynamic configuration of the S3 client
    /// </summary>
    /// <param name="options"></param>
    /// <returns></returns>
    IAmazonS3 GetClient(S3Options? options);

    /// <summary>
    /// Releases the provided IAmazonS3 client instance, allowing for cleanup of resources or returning the client to a pool if pooling is implemented.
    /// </summary>
    /// <param name="client"></param>
    void ReleaseClient(IAmazonS3 client);
}
