using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.AspNetCore.Mvc;

namespace OSWS.WebApi.Endpoints;

public static class BenchmarkS3PassthroughRoutes
{
    public static IEndpointRouteBuilder MapBenchmarkS3PassthroughRoutes(
        this IEndpointRouteBuilder app
    )
    {
        // Add root-level S3 bucket creation endpoint for Warp compatibility.
        app.MapPut(
            "/{bucket}",
            async (
                [FromServices] IAmazonS3 s3Client,
                string bucket,
                CancellationToken cancellationToken
            ) =>
            {
                if (string.IsNullOrEmpty(bucket))
                    return Results.BadRequest(new { error = "Bucket name required" });

                try
                {
                    // Check if bucket exists first.
                    var buckets = await s3Client.ListBucketsAsync(cancellationToken);
                    if (buckets.Buckets.Any(b => b.BucketName == bucket))
                        return Results.Ok(new { message = "Bucket already exists" });

                    // Create the bucket.
                    await s3Client.PutBucketAsync(bucket, cancellationToken);
                    return Results.Ok(new { message = "Bucket created" });
                }
                catch (AmazonS3Exception e)
                {
                    // Some S3-compatible backends don't support bucket creation.
                    if (e.ErrorCode == "NotImplemented" || e.ErrorCode == "AccessDenied")
                    {
                        // Return OK anyway to allow warp to proceed.
                        return Results.Ok(
                            new { message = "Bucket creation skipped (not supported by backend)" }
                        );
                    }
                    return Results.Problem(
                        statusCode: (int)e.StatusCode,
                        title: e.ErrorCode,
                        detail: e.Message
                    );
                }
            }
        );

        // Root-level S3 routes for benchmark traffic, without /s3 prefix and without auth.
        var s3Root = app.MapGroup("");

        s3Root.MapPut(
            "/{bucket}/{*key}",
            async (
                [FromServices] IAmazonS3 s3Client,
                string bucket,
                string? key,
                HttpRequest httpRequest,
                HttpResponse httpResponse,
                CancellationToken cancellationToken = default
            ) =>
            {
                if (string.IsNullOrEmpty(bucket) || string.IsNullOrEmpty(key))
                {
                    httpResponse.StatusCode = 400;
                    return Results.Empty;
                }

                try
                {
                    // Buffer the body to get content length (AWS SDK requires it).
                    using var memoryStream = new MemoryStream();
                    await httpRequest.Body.CopyToAsync(memoryStream, cancellationToken);
                    memoryStream.Position = 0;

                    var req = new PutObjectRequest
                    {
                        BucketName = bucket,
                        Key = key,
                        ContentType = httpRequest.ContentType ?? "application/octet-stream",
                        InputStream = memoryStream,
                        UseChunkEncoding = false,
                    };

                    // Forward x-amz-meta-* headers.
                    foreach (var h in httpRequest.Headers)
                    {
                        if (h.Key.StartsWith("x-amz-meta-", StringComparison.OrdinalIgnoreCase))
                        {
                            var metaKey = h.Key.Substring("x-amz-meta-".Length);
                            req.Metadata[metaKey] = h.Value.ToString();
                        }
                    }

                    var resp = await s3Client.PutObjectAsync(req, cancellationToken);
                    httpResponse.Headers["ETag"] = resp.ETag;
                    if (!string.IsNullOrEmpty(resp.VersionId))
                        httpResponse.Headers["x-amz-version-id"] = resp.VersionId;
                    httpResponse.StatusCode = 200;
                    return Results.Empty;
                }
                catch (AmazonS3Exception e)
                {
                    var statusCode = (int)e.StatusCode;
                    httpResponse.StatusCode = statusCode > 0 ? statusCode : 500;
                    var xml =
                        $"<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>{e.ErrorCode}</Code><Message>{System.Security.SecurityElement.Escape(e.Message)}</Message></Error>";
                    return Results.Text(xml, "application/xml");
                }
                catch (Exception e)
                {
                    httpResponse.StatusCode = 500;
                    var xml =
                        $"<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>InternalError</Code><Message>{System.Security.SecurityElement.Escape(e.Message)}</Message></Error>";
                    return Results.Text(xml, "application/xml");
                }
            }
        );

        s3Root.MapGet(
            "/{bucket}/{*key}",
            async (
                [FromServices] IAmazonS3 s3Client,
                string bucket,
                string? key,
                HttpResponse httpResponse,
                CancellationToken cancellationToken = default
            ) =>
            {
                if (string.IsNullOrEmpty(bucket) || string.IsNullOrEmpty(key))
                {
                    httpResponse.StatusCode = 400;
                    return Results.Empty;
                }

                try
                {
                    var req = new GetObjectRequest { BucketName = bucket, Key = key };
                    var resp = await s3Client.GetObjectAsync(req, cancellationToken);

                    httpResponse.ContentType =
                        resp.Headers.ContentType ?? "application/octet-stream";
                    httpResponse.Headers["ETag"] = resp.ETag;
                    httpResponse.Headers["Content-Length"] = resp.ContentLength.ToString();
                    httpResponse.Headers["Last-Modified"] = (resp.LastModified ?? DateTime.UtcNow)
                        .ToUniversalTime()
                        .ToString("R");
                    if (!string.IsNullOrEmpty(resp.VersionId))
                        httpResponse.Headers["x-amz-version-id"] = resp.VersionId;

                    await resp.ResponseStream.CopyToAsync(httpResponse.Body, cancellationToken);
                    return Results.Empty;
                }
                catch (AmazonS3Exception e)
                {
                    httpResponse.StatusCode = (int)e.StatusCode;
                    var xml =
                        $"<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>{e.ErrorCode}</Code><Message>{e.Message}</Message></Error>";
                    return Results.Text(xml, "application/xml");
                }
            }
        );

        s3Root.MapMethods(
            "/{bucket}",
            new[] { "HEAD" },
            async (
                [FromServices] IAmazonS3 s3Client,
                string bucket,
                HttpResponse httpResponse,
                CancellationToken cancellationToken = default
            ) =>
            {
                try
                {
                    var buckets = await s3Client.ListBucketsAsync(cancellationToken);
                    if (buckets.Buckets?.Any(b => b.BucketName == bucket) == true)
                    {
                        httpResponse.StatusCode = 200;
                        httpResponse.Headers["x-amz-bucket-region"] = "auto";
                        return Results.Empty;
                    }
                    httpResponse.StatusCode = 404;
                    return Results.Empty;
                }
                catch (AmazonS3Exception e)
                {
                    httpResponse.StatusCode = (int)e.StatusCode;
                    return Results.Empty;
                }
            }
        );

        s3Root.MapMethods(
            "/{bucket}/{*key}",
            new[] { "HEAD" },
            async (
                [FromServices] IAmazonS3 s3Client,
                string bucket,
                string? key,
                HttpResponse httpResponse,
                CancellationToken cancellationToken = default
            ) =>
            {
                if (string.IsNullOrEmpty(bucket) || string.IsNullOrEmpty(key))
                {
                    httpResponse.StatusCode = 400;
                    return Results.Empty;
                }

                try
                {
                    var req = new GetObjectMetadataRequest { BucketName = bucket, Key = key };
                    var resp = await s3Client.GetObjectMetadataAsync(req, cancellationToken);

                    httpResponse.Headers["ETag"] = resp.ETag;
                    httpResponse.Headers["Content-Length"] = resp.ContentLength.ToString();
                    httpResponse.Headers["Last-Modified"] = (resp.LastModified ?? DateTime.UtcNow)
                        .ToUniversalTime()
                        .ToString("R");
                    if (!string.IsNullOrEmpty(resp.VersionId))
                        httpResponse.Headers["x-amz-version-id"] = resp.VersionId;
                    httpResponse.StatusCode = 200;

                    return Results.Empty;
                }
                catch (AmazonS3Exception e)
                {
                    httpResponse.StatusCode = (int)e.StatusCode;
                    return Results.Empty;
                }
            }
        );

        s3Root.MapGet(
            "/{bucket}",
            async (
                [FromServices] IAmazonS3 s3Client,
                string bucket,
                HttpRequest httpRequest,
                HttpResponse httpResponse,
                [FromQuery] string? prefix,
                [FromQuery] string? delimiter,
                [FromQuery(Name = "continuation-token")] string? continuationToken,
                [FromQuery(Name = "start-after")] string? startAfter,
                [FromQuery(Name = "max-keys")] int? maxKeys,
                CancellationToken cancellationToken = default
            ) =>
            {
                try
                {
                    var req = new ListObjectsV2Request
                    {
                        BucketName = bucket,
                        Prefix = prefix,
                        Delimiter = delimiter,
                        ContinuationToken = continuationToken,
                        StartAfter = startAfter,
                        MaxKeys = maxKeys ?? 1000,
                    };
                    var resp = await s3Client.ListObjectsV2Async(req, cancellationToken);

                    var contents = new System.Text.StringBuilder();
                    foreach (var obj in resp.S3Objects ?? new List<S3Object>())
                    {
                        contents.Append(
                            $"<Contents><Key>{System.Security.SecurityElement.Escape(obj.Key)}</Key>"
                        );
                        contents.Append(
                            $"<LastModified>{obj.LastModified:yyyy-MM-ddTHH:mm:ss.fffZ}</LastModified>"
                        );
                        contents.Append($"<ETag>{obj.ETag}</ETag><Size>{obj.Size}</Size>");
                        contents.Append(
                            $"<StorageClass>{obj.StorageClass?.Value ?? "STANDARD"}</StorageClass></Contents>"
                        );
                    }

                    var xml =
                        $"<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                        + $"<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
                        + $"<Name>{resp.Name}</Name>"
                        + $"<Prefix>{resp.Prefix ?? ""}</Prefix>"
                        + $"<MaxKeys>{resp.MaxKeys}</MaxKeys>"
                        + $"<IsTruncated>{resp.IsTruncated.ToString().ToLower()}</IsTruncated>"
                        + $"<KeyCount>{resp.KeyCount}</KeyCount>"
                        + $"{(string.IsNullOrEmpty(resp.ContinuationToken) ? "" : $"<ContinuationToken>{System.Security.SecurityElement.Escape(resp.ContinuationToken)}</ContinuationToken>")}"
                        + $"{(string.IsNullOrEmpty(resp.NextContinuationToken) ? "" : $"<NextContinuationToken>{System.Security.SecurityElement.Escape(resp.NextContinuationToken)}</NextContinuationToken>")}"
                        + $"{contents}"
                        + $"</ListBucketResult>";

                    httpResponse.ContentType = "application/xml";
                    return Results.Text(xml, "application/xml");
                }
                catch (AmazonS3Exception e)
                {
                    httpResponse.StatusCode = (int)e.StatusCode;
                    var xml =
                        $"<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>{e.ErrorCode}</Code><Message>{System.Security.SecurityElement.Escape(e.Message)}</Message></Error>";
                    return Results.Text(xml, "application/xml");
                }
            }
        );

        s3Root.MapGet(
            "/",
            async (
                [FromServices] IAmazonS3 s3Client,
                HttpResponse httpResponse,
                CancellationToken cancellationToken = default
            ) =>
            {
                try
                {
                    var resp = await s3Client.ListBucketsAsync(cancellationToken);

                    var buckets = new System.Text.StringBuilder();
                    foreach (var b in resp.Buckets ?? new List<S3Bucket>())
                    {
                        buckets.Append(
                            $"<Bucket><Name>{System.Security.SecurityElement.Escape(b.BucketName)}</Name>"
                        );
                        buckets.Append(
                            $"<CreationDate>{b.CreationDate:yyyy-MM-ddTHH:mm:ss.fffZ}</CreationDate></Bucket>"
                        );
                    }

                    var xml =
                        $"<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                        + $"<ListAllMyBucketsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
                        + $"<Owner><ID>{resp.Owner?.Id ?? ""}</ID><DisplayName>{resp.Owner?.DisplayName ?? ""}</DisplayName></Owner>"
                        + $"<Buckets>{buckets}</Buckets>"
                        + $"</ListAllMyBucketsResult>";

                    httpResponse.ContentType = "application/xml";
                    return Results.Text(xml, "application/xml");
                }
                catch (AmazonS3Exception e)
                {
                    httpResponse.StatusCode = (int)e.StatusCode;
                    var xml =
                        $"<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>{e.ErrorCode}</Code><Message>{System.Security.SecurityElement.Escape(e.Message)}</Message></Error>";
                    return Results.Text(xml, "application/xml");
                }
            }
        );

        s3Root.MapPost(
            "/{bucket}/{*key}",
            async (
                [FromServices] IAmazonS3 s3Client,
                string bucket,
                string? key,
                HttpRequest httpRequest,
                HttpResponse httpResponse,
                CancellationToken cancellationToken = default
            ) =>
            {
                // Warp mixed can issue POST on bucket root for multi-delete batches.
                var isBucketRoot = string.IsNullOrEmpty(key);
                if (!isBucketRoot)
                {
                    httpResponse.StatusCode = 405;
                    httpResponse.Headers["Allow"] = "DELETE, GET, HEAD, PUT";
                    return Results.Empty;
                }

                try
                {
                    using var reader = new StreamReader(httpRequest.Body);
                    var body = await reader.ReadToEndAsync(cancellationToken);
                    if (string.IsNullOrWhiteSpace(body))
                    {
                        httpResponse.StatusCode = 400;
                        var xml =
                            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>MalformedXML</Code><Message>Request body is required for POST on bucket root.</Message></Error>";
                        return Results.Text(xml, "application/xml");
                    }

                    var doc = System.Xml.Linq.XDocument.Parse(body);
                    var ns = doc.Root?.Name.Namespace ?? System.Xml.Linq.XNamespace.None;

                    var keys = doc.Descendants(ns + "Object")
                        .Select(x => x.Element(ns + "Key")?.Value)
                        .Where(x => !string.IsNullOrWhiteSpace(x))
                        .Cast<string>()
                        .ToList();

                    if (keys.Count == 0)
                    {
                        httpResponse.StatusCode = 400;
                        var xml =
                            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>MalformedXML</Code><Message>No object keys found in delete request.</Message></Error>";
                        return Results.Text(xml, "application/xml");
                    }

                    var req = new DeleteObjectsRequest
                    {
                        BucketName = bucket,
                        Objects = keys.Select(k => new KeyVersion { Key = k }).ToList(),
                        Quiet = false,
                    };

                    var resp = await s3Client.DeleteObjectsAsync(req, cancellationToken);

                    var deletedXml = new System.Text.StringBuilder();
                    foreach (var obj in resp.DeletedObjects ?? new List<DeletedObject>())
                    {
                        deletedXml.Append(
                            $"<Deleted><Key>{System.Security.SecurityElement.Escape(obj.Key)}</Key></Deleted>"
                        );
                    }

                    var resultXml =
                        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                        + "<DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
                        + deletedXml
                        + "</DeleteResult>";

                    httpResponse.StatusCode = 200;
                    return Results.Text(resultXml, "application/xml");
                }
                catch (AmazonS3Exception e)
                {
                    httpResponse.StatusCode = (int)e.StatusCode;
                    var xml =
                        $"<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>{e.ErrorCode}</Code><Message>{System.Security.SecurityElement.Escape(e.Message)}</Message></Error>";
                    return Results.Text(xml, "application/xml");
                }
            }
        );

        s3Root.MapDelete(
            "/{bucket}/{*key}",
            async (
                [FromServices] IAmazonS3 s3Client,
                string bucket,
                string? key,
                HttpResponse httpResponse,
                CancellationToken cancellationToken = default
            ) =>
            {
                if (string.IsNullOrEmpty(bucket) || string.IsNullOrEmpty(key))
                {
                    httpResponse.StatusCode = 400;
                    return Results.Empty;
                }

                try
                {
                    var req = new DeleteObjectRequest { BucketName = bucket, Key = key };
                    await s3Client.DeleteObjectAsync(req, cancellationToken);
                    httpResponse.StatusCode = 204;
                    return Results.Empty;
                }
                catch (AmazonS3Exception e)
                {
                    httpResponse.StatusCode = (int)e.StatusCode;
                    var xml =
                        $"<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>{e.ErrorCode}</Code><Message>{System.Security.SecurityElement.Escape(e.Message)}</Message></Error>";
                    return Results.Text(xml, "application/xml");
                }
            }
        );

        return app;
    }
}
