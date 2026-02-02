using System.Text.Json;
using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Mvc;
using OSWS.Models.DTOs;
using OSWS.WebApi.Interfaces;

namespace OSWS.WebApi.Endpoints;

public class S3Get(IAmazonS3 s3Client) : IS3Get
{
    public async Task<IResult> GetObject(Params prms, S3Options s3Options, int retryOptions = 3,
        int timeoutOptionsMs = 3000,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(prms.Bucket))
            return Results.BadRequest(new { error = "Bucket name is required" });
        if (string.IsNullOrEmpty(prms.Key))
            return Results.BadRequest(new { error = "Key is required" });
        
        var localS3Client = CreateClientFromOptionsOrDefault(s3Options);

        var req = new GetObjectRequest()
        {
            BucketName = prms.Bucket,
            Key = prms.Key,
            VersionId = string.IsNullOrEmpty(prms.Version) ? null : prms.Version,
        };

        if (!string.IsNullOrEmpty(prms.Version))
            req.VersionId = prms.Version;

        GetObjectResponse resp;
        try
        {
            resp = await localS3Client.GetObjectAsync(req, cancellationToken).ConfigureAwait(false);
        }
        catch (AmazonS3Exception e)
        {
            return Results.InternalServerError(e.Message);
        }

        // Fetch needed keys

        // Decrypt

        var contentType = resp.Headers?.ContentType ?? "application/octet-stream";
        return Results.File(resp.ResponseStream, contentType, fileDownloadName: prms.Key);

    }

    private IAmazonS3 CreateClientFromOptionsOrDefault(S3Options? opts)
    {
        if (opts == null)
            return s3Client;

        var hasEndpoint = !string.IsNullOrWhiteSpace(opts.EndpointHostname);
        var hasCredsJson = !string.IsNullOrWhiteSpace(opts.V2AwsSdkCredentials) ||
                           !string.IsNullOrWhiteSpace(opts.V3AwsSdkCredentials);

        // If neither endpoint nor credentials provided, use injected default client
        if (!hasEndpoint && !hasCredsJson)
            return s3Client;

        AWSCredentials? creds = null;
        var json = !string.IsNullOrWhiteSpace(opts.V3AwsSdkCredentials)
            ? opts.V3AwsSdkCredentials
            : opts.V2AwsSdkCredentials;
        if (!string.IsNullOrWhiteSpace(json) &&
            TryParseCredentials(json!, out var accessKey, out var secretKey, out var sessionToken))
        {
            creds = string.IsNullOrEmpty(sessionToken)
                ? new BasicAWSCredentials(accessKey, secretKey)
                : new SessionAWSCredentials(accessKey, secretKey, sessionToken);
        }

        // If no creds parsed and no endpoint, fallback
        if (creds == null && !hasEndpoint)
            return s3Client;

        // Build config
        var endpoint = (opts.EndpointHostname ?? string.Empty).Trim();
        if (!string.IsNullOrEmpty(endpoint))
        {
            if (!endpoint.StartsWith("http://", StringComparison.OrdinalIgnoreCase) &&
                !endpoint.StartsWith("https://", StringComparison.OrdinalIgnoreCase))
            {
                endpoint = "https://" + endpoint.TrimEnd('/');
            }
            else
            {
                endpoint = endpoint.TrimEnd('/');
            }
        }

        var config = new AmazonS3Config
        {
            ServiceURL = string.IsNullOrEmpty(endpoint) ? null : endpoint,
            ForcePathStyle = true
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
        return creds == null ? s3Client : new AmazonS3Client(creds, config);
    }

    private static bool TryParseCredentials(string json, out string accessKey, out string secretKey,
        out string sessionToken)
    {
        accessKey = secretKey = sessionToken = string.Empty;
        try
        {
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;

            string? GetValue(params string[] keys)
            {
                foreach (var prop in root.EnumerateObject())
                {
                    foreach (var k in keys)
                    {
                        if (string.Equals(prop.Name, k, StringComparison.OrdinalIgnoreCase))
                            return prop.Value.GetString();
                    }
                }

                return null;
            }

            accessKey = GetValue("accessKeyId", "accessKey", "AccessKeyId") ?? string.Empty;
            secretKey = GetValue("secretAccessKey", "secretKey", "SecretAccessKey") ?? string.Empty;
            sessionToken = GetValue("sessionToken", "token", "SessionToken") ?? string.Empty;

            return !string.IsNullOrEmpty(accessKey) && !string.IsNullOrEmpty(secretKey);
        }
        catch
        {
            return false;
        }
    }
}