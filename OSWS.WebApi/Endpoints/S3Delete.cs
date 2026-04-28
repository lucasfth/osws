using System.Xml.Linq;
using Amazon.S3;
using Amazon.S3.Model;
using OSWS.Library;
using OSWS.Library.Helpers;
using OSWS.Models.DTOs;
using OSWS.WebApi.Interfaces;
using OSWS.WebApi.Services;

namespace OSWS.WebApi.Endpoints;

public class S3Delete(
    IAmazonS3 s3Client,
    CurrentUser currentUser
) : IS3Delete
{
    private static readonly XNamespace S3XmlNs = "http://s3.amazonaws.com/doc/2006-03-01/";

    public async Task<IResult> DeleteObject(
        string bucket,
        string key,
        Params prms,
        HttpRequest httpRequest,
        int retryOptions = 3,
        int timeoutOptionsMs = 3000,
        CancellationToken cancellationToken = default
    )
    {
        var user = await currentUser.ResolveAsync(cancellationToken);
        if (user is null)
            return Results.Unauthorized();

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

        try
        {
            await s3Client.DeleteObjectAsync(
                new DeleteObjectRequest
                {
                    BucketName = bucket,
                    Key = key
                },
                cancellationToken
            ).ConfigureAwait(false);

            return Results.NoContent();
        }
        catch (AmazonS3Exception e)
        {
            return S3ErrorHelper.HandleS3Exception(e, httpRequest.HttpContext);
        }
    }

    public async Task<IResult> DeleteObjects(
        string bucket,
        HttpRequest httpRequest,
        CancellationToken cancellationToken = default
    )
    {
        var user = await currentUser.ResolveAsync(cancellationToken);
        if (user is null)
            return Results.Unauthorized();

        if (string.IsNullOrWhiteSpace(bucket))
        {
            httpRequest.HttpContext.Response.StatusCode = 400;
            return Results.Text(ParamValidation.BucketNameIsRequired(), "application/json");
        }

        DeleteObjectsRequest req;
        try
        {
            req = await ParseDeleteObjectsRequestAsync(bucket, httpRequest, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (InvalidDataException ex)
        {
            httpRequest.HttpContext.Response.StatusCode = 400;
            var xml =
                $"<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>MalformedXML</Code><Message>{System.Security.SecurityElement.Escape(ex.Message)}</Message><Resource>/{bucket}</Resource><RequestId></RequestId></Error>";
            return Results.Text(xml, "application/xml");
        }

        try
        {
            var resp = await s3Client.DeleteObjectsAsync(req, cancellationToken).ConfigureAwait(false);

            var deletedObjects = resp.DeletedObjects ?? [];
            var deleteErrors = resp.DeleteErrors ?? [];

            var doc = new XDocument(
                new XElement(
                    S3XmlNs + "DeleteResult",
                    deletedObjects.Select(d =>
                        new XElement(
                            S3XmlNs + "Deleted",
                            new XElement(S3XmlNs + "Key", d.Key ?? string.Empty),
                            string.IsNullOrEmpty(d.VersionId)
                                ? null
                                : new XElement(S3XmlNs + "VersionId", d.VersionId)
                        )
                    ),
                    deleteErrors.Select(e =>
                        new XElement(
                            S3XmlNs + "Error",
                            new XElement(S3XmlNs + "Key", e.Key ?? string.Empty),
                            new XElement(S3XmlNs + "Code", e.Code ?? "InternalError"),
                            new XElement(S3XmlNs + "Message", e.Message ?? "Delete failed")
                        )
                    )
                )
            );

            return Results.Text(doc.ToString(SaveOptions.DisableFormatting), "application/xml");
        }
        catch (AmazonS3Exception e)
        {
            return S3ErrorHelper.HandleS3Exception(e, httpRequest.HttpContext);
        }
    }

    public async Task<IResult> DeleteBucket(
        string bucket,
        HttpRequest httpRequest,
        CancellationToken cancellationToken = default
    )
    {
        var user = await currentUser.ResolveAsync(cancellationToken);
        if (user is null)
            return Results.Unauthorized();

        if (string.IsNullOrWhiteSpace(bucket))
        {
            httpRequest.HttpContext.Response.StatusCode = 400;
            return Results.Text(ParamValidation.BucketNameIsRequired(), "application/json");
        }

        try
        {
            await s3Client.DeleteBucketAsync(new DeleteBucketRequest { BucketName = bucket }, cancellationToken)
                .ConfigureAwait(false);
            return Results.NoContent();
        }
        catch (AmazonS3Exception e)
        {
            return S3ErrorHelper.HandleS3Exception(e, httpRequest.HttpContext);
        }
    }

    private static async Task<DeleteObjectsRequest> ParseDeleteObjectsRequestAsync(
        string bucket,
        HttpRequest httpRequest,
        CancellationToken cancellationToken
    )
    {
        var doc = await XDocument.LoadAsync(httpRequest.Body, LoadOptions.None, cancellationToken)
            .ConfigureAwait(false);
        var root = doc.Root;
        if (root is null || !string.Equals(root.Name.LocalName, "Delete", StringComparison.OrdinalIgnoreCase))
            throw new InvalidDataException("DeleteObjects request body must contain a root <Delete> element.");

        var objects = root
            .Elements()
            .Where(e => string.Equals(e.Name.LocalName, "Object", StringComparison.OrdinalIgnoreCase))
            .Select(e =>
            {
                var key = e
                    .Elements()
                    .FirstOrDefault(x =>
                        string.Equals(x.Name.LocalName, "Key", StringComparison.OrdinalIgnoreCase)
                    )
                    ?.Value;
                var versionId = e
                    .Elements()
                    .FirstOrDefault(x =>
                        string.Equals(
                            x.Name.LocalName,
                            "VersionId",
                            StringComparison.OrdinalIgnoreCase
                        )
                    )
                    ?.Value;

                return new KeyVersion
                {
                    Key = key,
                    VersionId = versionId
                };
            })
            .Where(x => !string.IsNullOrWhiteSpace(x.Key))
            .ToList();

        if (objects.Count == 0)
            throw new InvalidDataException("DeleteObjects request body must include at least one <Object><Key>...</Key></Object> entry.");

        var quietValue = root
            .Elements()
            .FirstOrDefault(e => string.Equals(e.Name.LocalName, "Quiet", StringComparison.OrdinalIgnoreCase))
            ?.Value;

        var req = new DeleteObjectsRequest
        {
            BucketName = bucket,
            Quiet = bool.TryParse(quietValue, out var quiet) && quiet,
            Objects = objects
        };

        return req;
    }
}
