using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using System.Xml.Linq;
using Amazon.S3;
using Amazon.S3.Model;
using OSWS.Library;
using OSWS.Library.Helpers;
using OSWS.WebApi.Interfaces;

namespace OSWS.WebApi.Endpoints;

#pragma warning disable IL2026, IL3050

public class S3List(IAmazonS3 s3Client) : IS3List
{
    private static readonly JsonSerializerOptions _reflectionSerializerOptions = new()
    {
        TypeInfoResolver = new DefaultJsonTypeInfoResolver(),
    };

    private static readonly XNamespace S3XmlNs = "http://s3.amazonaws.com/doc/2006-03-01/";

    private static string FormatS3Date(DateTime dt) =>
        dt.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ", CultureInfo.InvariantCulture);

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

            var owner = resp.Owner is null
                ? null
                : new XElement(
                    S3XmlNs + "Owner",
                    new XElement(S3XmlNs + "ID", resp.Owner.Id ?? string.Empty),
                    new XElement(S3XmlNs + "DisplayName", resp.Owner.DisplayName ?? string.Empty)
                );

            var buckets = new XElement(
                S3XmlNs + "Buckets",
                resp.Buckets?.Select(b => new XElement(
                    S3XmlNs + "Bucket",
                    new XElement(S3XmlNs + "Name", b.BucketName),
                    new XElement(
                        S3XmlNs + "CreationDate",
                        FormatS3Date(b.CreationDate ?? new DateTime())
                    )
                ))
            );

            var doc = new XDocument(
                new XElement(S3XmlNs + "ListAllMyBucketsResult", owner, buckets)
            );

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

            var contents = resp.S3Objects?.Select(o => new XElement(
                S3XmlNs + "Contents",
                new XElement(S3XmlNs + "Key", o.Key ?? string.Empty),
                new XElement(
                    S3XmlNs + "LastModified",
                    FormatS3Date(o.LastModified ?? new DateTime())
                ),
                new XElement(S3XmlNs + "ETag", o.ETag ?? string.Empty),
                new XElement(S3XmlNs + "Size", o.Size),
                new XElement(S3XmlNs + "StorageClass", o.StorageClass ?? "STANDARD")
            ));

            var commonPrefixes = resp.CommonPrefixes?.Select(p => new XElement(
                S3XmlNs + "CommonPrefixes",
                new XElement(S3XmlNs + "Prefix", p ?? string.Empty)
            ));

            var doc = new XDocument(
                new XElement(
                    S3XmlNs + "ListBucketResult",
                    new XElement(S3XmlNs + "Name", bucket),
                    new XElement(S3XmlNs + "Prefix", resp.Prefix ?? string.Empty),
                    new XElement(S3XmlNs + "KeyCount", resp.KeyCount),
                    new XElement(S3XmlNs + "MaxKeys", resp.MaxKeys),
                    new XElement(S3XmlNs + "Delimiter", resp.Delimiter ?? string.Empty),
                    new XElement(
                        S3XmlNs + "IsTruncated",
                        resp.IsTruncated.ToString().ToLowerInvariant()
                    ),
                    string.IsNullOrEmpty(resp.NextContinuationToken)
                        ? null
                        : new XElement(
                            S3XmlNs + "NextContinuationToken",
                            resp.NextContinuationToken
                        ),
                    contents,
                    commonPrefixes
                )
            );

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
            var json = JsonSerializer.Serialize(resp, _reflectionSerializerOptions);
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
            var json = JsonSerializer.Serialize(resp, _reflectionSerializerOptions);
            return Results.Text(json, "application/json");
        }
        catch (AmazonS3Exception e)
        {
            return S3ErrorHelper.HandleS3Exception(e, httpRequest.HttpContext);
        }
    }
}

#pragma warning restore IL2026, IL3050
