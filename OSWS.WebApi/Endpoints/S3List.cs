using Amazon.S3;
using Amazon.S3.Model;
using OSWS.Library;
using OSWS.Library.Helpers;
using OSWS.Models.DTOs;
using OSWS.WebApi.Interfaces;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace OSWS.WebApi.Endpoints;

#pragma warning disable IL2026, IL3050

public class S3List(IS3ClientFactory clientFactory) : IS3List
{
    private static readonly JsonSerializerOptions _reflectionSerializerOptions = new()
    {
        TypeInfoResolver = new DefaultJsonTypeInfoResolver()
    };

    public async Task<IResult> ListBuckets(
        S3Options s3Options,
        HttpRequest httpRequest,
        int retryOptions = 3,
        int timeoutOptionsMs = 3000,
        CancellationToken cancellationToken = default
    )
    {
        var s3Client = clientFactory.GetClient(s3Options);

        try
        {
            var resp = await s3Client.ListBucketsAsync(cancellationToken).ConfigureAwait(false);
            var dto = new
            {
                Buckets = resp.Buckets?.Select(b => new
                {
                    Name = b.BucketName,
                    Created = b.CreationDate
                }).ToList(),
                Owner = resp.Owner is null ? null : new { resp.Owner.DisplayName, resp.Owner.Id }
            };

            var json = JsonSerializer.Serialize(dto, _reflectionSerializerOptions);
            return Results.Text(json, "application/json");
        }
        catch (AmazonS3Exception e)
        {
            return S3ErrorHelper.HandleS3Exception(e, httpRequest.HttpContext);
        }
        finally
        {
            clientFactory.ReleaseClient(s3Client);
        }
    }

    public async Task<IResult> ListObjects(
        string bucket,
        S3Options s3Options,
        HttpRequest httpRequest,
        int retryOptions = 3,
        int timeoutOptionsMs = 3000,
        CancellationToken cancellationToken = default
    )
    {
        if (string.IsNullOrEmpty(bucket))
        {
            httpRequest.HttpContext.Response.StatusCode = 400;
            return Results.Text(ParamValidation.BucketNameIsRequired(), "application/json");
        }

        var s3Client = clientFactory.GetClient(s3Options);

        var query = httpRequest.Query;
        var req = new ListObjectsV2Request
        {
            BucketName = bucket,
            Prefix = query["prefix"],
            Delimiter = query["delimiter"],
            ContinuationToken = query["continuation-token"],
            StartAfter = query["start-after"],
        };

        if (int.TryParse(query["max-keys"], out var maxKeys))
            req.MaxKeys = maxKeys;

        try
        {
            var resp = await s3Client.ListObjectsV2Async(req, cancellationToken).ConfigureAwait(false);
            var dto = new
            {
                Bucket = bucket,
                resp.IsTruncated,
                resp.MaxKeys,
                resp.KeyCount,
                resp.NextContinuationToken,
                Objects = resp.S3Objects?.Select(o => new
                {
                    Key = o.Key,
                    Size = o.Size,
                    LastModified = o.LastModified,
                    ETag = o.ETag,
                    StorageClass = o.StorageClass
                }).ToList()
            };
            return Results.Text(JsonSerializer.Serialize(dto, _reflectionSerializerOptions), "application/json");
        }
        catch (AmazonS3Exception e)
        {
            return S3ErrorHelper.HandleS3Exception(e, httpRequest.HttpContext);
        }
        finally
        {
            clientFactory.ReleaseClient(s3Client);
        }
    }

    public async Task<IResult> ListMultipartUploads(
        string bucket,
        S3Options s3Options,
        HttpRequest httpRequest,
        int retryOptions = 3,
        int timeoutOptionsMs = 3000,
        CancellationToken cancellationToken = default
    )
    {
        if (string.IsNullOrEmpty(bucket))
        {
            httpRequest.HttpContext.Response.StatusCode = 400;
            return Results.Text(ParamValidation.BucketNameIsRequired(), "application/json");
        }

        var s3Client = clientFactory.GetClient(s3Options);
        var query = httpRequest.Query;
        var req = new ListMultipartUploadsRequest
        {
            BucketName = bucket,
            Prefix = query["prefix"],
            Delimiter = query["delimiter"],
            KeyMarker = query["key-marker"],
            UploadIdMarker = query["upload-id-marker"],
        };

        if (int.TryParse(query["max-uploads"], out var maxUploads))
            req.MaxUploads = maxUploads;

        try
        {
            var resp = await s3Client.ListMultipartUploadsAsync(req, cancellationToken)
                .ConfigureAwait(false);
            var json = JsonSerializer.Serialize(resp, _reflectionSerializerOptions);
            return Results.Text(json, "application/json");
        }
        catch (AmazonS3Exception e)
        {
            return S3ErrorHelper.HandleS3Exception(e, httpRequest.HttpContext);
        }
        finally
        {
            clientFactory.ReleaseClient(s3Client);
        }
    }

    public async Task<IResult> ListParts(
        string bucket,
        string key,
        string uploadId,
        S3Options s3Options,
        HttpRequest httpRequest,
        int retryOptions = 3,
        int timeoutOptionsMs = 3000,
        CancellationToken cancellationToken = default
    )
    {
        if (string.IsNullOrEmpty(bucket))
        {
            httpRequest.HttpContext.Response.StatusCode = 400;
            return Results.Text(ParamValidation.BucketNameIsRequired(), "application/json");
        }

        if (string.IsNullOrEmpty(key))
        {
            httpRequest.HttpContext.Response.StatusCode = 400;
            return Results.Text(ParamValidation.KeyIsRequired(), "application/json");
        }

        if (string.IsNullOrEmpty(uploadId))
        {
            httpRequest.HttpContext.Response.StatusCode = 400;
            return Results.Text(ParamValidation.CreateErrorJson("UploadId is required"), "application/json");
        }

        var s3Client = clientFactory.GetClient(s3Options);
        var query = httpRequest.Query;
        var req = new ListPartsRequest
        {
            BucketName = bucket,
            Key = key,
            UploadId = uploadId,
        };

        var partMarker = query["part-number-marker"].ToString();
        if (!string.IsNullOrEmpty(partMarker))
            req.PartNumberMarker = partMarker;
        if (int.TryParse(query["max-parts"], out var maxParts))
            req.MaxParts = maxParts;

        try
        {
            var resp = await s3Client.ListPartsAsync(req, cancellationToken).ConfigureAwait(false);
            var json = JsonSerializer.Serialize(resp, _reflectionSerializerOptions);
            return Results.Text(json, "application/json");
        }
        catch (AmazonS3Exception e)
        {
            return S3ErrorHelper.HandleS3Exception(e, httpRequest.HttpContext);
        }
        finally
        {
            clientFactory.ReleaseClient(s3Client);
        }
    }
}

#pragma warning restore IL2026, IL3050
