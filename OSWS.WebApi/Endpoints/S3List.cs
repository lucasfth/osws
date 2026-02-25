using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using System.Xml.Linq;
using Amazon.S3;
using Amazon.S3.Model;
using OSWS.Library;
using OSWS.Library.Extensions;
using OSWS.Library.Helpers;
using OSWS.WebApi.Interfaces;

namespace OSWS.WebApi.Endpoints;

#pragma warning disable IL2026, IL3050

public class S3List(IAmazonS3 s3Client) : IS3List
{
    private static readonly JsonSerializerOptions ReflectionSerializerOptions = new()
    {
        TypeInfoResolver = new DefaultJsonTypeInfoResolver(),
    };

    public async Task<IResult> ListBuckets(
        HttpRequest httpRequest,
        int retryOptions = 3,
        int timeoutOptionsMs = 3000,
        CancellationToken cancellationToken = default
    )
    {
        try
        {
            var resp = await s3Client.ListBucketsAsync(cancellationToken).ConfigureAwait(false);

            var doc = resp.ToListBucketsXml();

            return Results.Text(doc.ToString(SaveOptions.DisableFormatting), "application/xml");
        }
        catch (AmazonS3Exception e)
        {
            return S3ErrorHelper.HandleS3Exception(e, httpRequest.HttpContext);
        }
    }

    public async Task<IResult> ListObjects(
        string bucket,
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
            var resp = await s3Client
                .ListObjectsV2Async(req, cancellationToken)
                .ConfigureAwait(false);

            var doc = resp.ToListObjectsV2Xml(bucket);

            return Results.Text(doc.ToString(SaveOptions.DisableFormatting), "application/xml");
        }
        catch (AmazonS3Exception e)
        {
            return S3ErrorHelper.HandleS3Exception(e, httpRequest.HttpContext);
        }
    }

    public async Task<IResult> ListMultipartUploads(
        string bucket,
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
            var resp = await s3Client
                .ListMultipartUploadsAsync(req, cancellationToken)
                .ConfigureAwait(false);
            var json = JsonSerializer.Serialize(resp, ReflectionSerializerOptions);
            return Results.Text(json, "application/json");
        }
        catch (AmazonS3Exception e)
        {
            return S3ErrorHelper.HandleS3Exception(e, httpRequest.HttpContext);
        }
    }

    public async Task<IResult> ListParts(
        string bucket,
        string key,
        string uploadId,
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
            return Results.Text(
                ParamValidation.CreateErrorJson("UploadId is required"),
                "application/json"
            );
        }

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
            var json = JsonSerializer.Serialize(resp, ReflectionSerializerOptions);
            return Results.Text(json, "application/json");
        }
        catch (AmazonS3Exception e)
        {
            return S3ErrorHelper.HandleS3Exception(e, httpRequest.HttpContext);
        }
    }
}

#pragma warning restore IL2026, IL3050
