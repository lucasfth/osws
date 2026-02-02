using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Mvc;
using OSWS.Models.DTOs;
using OSWS.WebApi.Interfaces;

namespace OSWS.WebApi.Endpoints;

public class S3Get(IAmazonS3 s3Client) : IS3Get
{
    public async Task<IResult> GetObject(Params prms, S3Options s3Options, int retryOptions = 3, int timeoutOptionsMs = 3000,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(prms?.Bucket))
            return Results.BadRequest(new { error = "Bucket name is required" });
        if (string.IsNullOrEmpty(prms.Key))
            return Results.BadRequest(new { error = "Key is required" });

        var req = new GetObjectRequest()
        {
            BucketName = prms.Bucket,
            Key = prms.Key,
        };
        
        if (!string.IsNullOrEmpty(prms.Version))
            req.VersionId = prms.Version;

        GetObjectResponse resp;
        try
        {
            resp = await s3Client.GetObjectAsync(req, cancellationToken).ConfigureAwait(false);
        }
        catch (AmazonS3Exception e)
        {
            return Results.StatusCode(e.StatusCode == 0 ? 500 : (int) e.StatusCode);
        }
        
        // Fetch needed keys
        
        // Decrypt
        
        var contentType = resp.Headers?.ContentType ?? "application/octet-stream";
        return Results.File(resp.ResponseStream, contentType, fileDownloadName: prms.Key);

    }
}