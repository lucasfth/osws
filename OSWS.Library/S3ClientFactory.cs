using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using OSWS.Models.DTOs;

namespace OSWS.Library;

public class S3ClientFactory : IS3ClientFactory
{
    private readonly IAmazonS3 _defaultClient;

    public S3ClientFactory(IAmazonS3 defaultClient)
    {
        _defaultClient = defaultClient;
    }

    public IAmazonS3 GetClient(S3Options? opts)
    {
        if (opts == null)
            return _defaultClient;

        var hasEndpoint = !string.IsNullOrWhiteSpace(opts.EndpointHostname);
        var hasCredsJson = !string.IsNullOrWhiteSpace(opts.V2AwsSdkCredentials) ||
                           !string.IsNullOrWhiteSpace(opts.V3AwsSdkCredentials);

        // If neither endpoint nor credentials provided, use injected default client
        if (!hasEndpoint && !hasCredsJson)
            return _defaultClient;

        AWSCredentials? creds = null;
        var json = !string.IsNullOrWhiteSpace(opts.V3AwsSdkCredentials)
            ? opts.V3AwsSdkCredentials
            : opts.V2AwsSdkCredentials;
        if (!string.IsNullOrWhiteSpace(json) &&
            AwsCredentialHelper.TryParseCredentials(json!, out var accessKey, out var secretKey, out var sessionToken))
        {
            creds = string.IsNullOrEmpty(sessionToken)
                ? new BasicAWSCredentials(accessKey, secretKey)
                : new SessionAWSCredentials(accessKey, secretKey, sessionToken);
        }

        // If no creds parsed and no endpoint, fallback
        if (creds == null && !hasEndpoint)
            return _defaultClient;

        // Build config
        var endpoint = AwsCredentialHelper.NormalizeEndpoint(opts.EndpointHostname);

        var config = new AmazonS3Config
        {
            ServiceURL = string.IsNullOrEmpty(endpoint ?? string.Empty) ? null : endpoint,
            ForcePathStyle = true,
        };

        if (!string.IsNullOrWhiteSpace(opts.Region) && !opts.Region.Equals("auto", StringComparison.OrdinalIgnoreCase))
        {
            try
            {
                config.RegionEndpoint = RegionEndpoint.GetBySystemName(opts.Region);
            }
            catch
            {
                // ignore invalid region names
            }
        }

        // If creds missing, but endpoint provided, fall back to default client (so default credentials can be used)
        return creds == null ? _defaultClient : new AmazonS3Client(creds, config);
    }

    public void ReleaseClient(IAmazonS3 client)
    {
        if (ReferenceEquals(client, _defaultClient))
            return;

        if (client is IDisposable d)
            d.Dispose();
    }
}
