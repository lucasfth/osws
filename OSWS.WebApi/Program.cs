using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using OSWS.Common.Configuration;
using OSWS.KeyManager.Persistence;
using OSWS.KeyManager.Providers;
using OSWS.Library;
using OSWS.Models.DTOs;
using OSWS.Models.Interfaces;
using OSWS.ParquetSolver;
using OSWS.ParquetSolver.Helpers;
using OSWS.ParquetSolver.Interfaces;
using OSWS.WebApi.Authentication;
using OSWS.WebApi.Endpoints;
using OSWS.WebApi.Endpoints.Admin;
using OSWS.WebApi.Interfaces;
using OSWS.WebApi.Services;

var builder = WebApplication.CreateSlimBuilder(args);

builder.Services.ConfigureHttpJsonOptions(options =>
{
    // options.SerializerOptions.TypeInfoResolverChain.Insert(0, AppJsonSerializerContext.Default);
});

builder.Logging.ClearProviders();
builder.Logging.AddConsole();
builder.Services.AddHttpLogging(o => { });

builder.Configuration.AddEnvironmentVariables();
builder.Services.AddDbContext<OswsContext>(opts =>
    opts.UseNpgsql(builder.Configuration.GetConnectionString("OswsContext"))
);

builder.Services.Configure<S3Settings>(builder.Configuration.GetSection("S3Settings"));

// --- Encryption Settings ---
builder.Services.Configure<EncryptionSettings>(builder.Configuration.GetSection("Encryption"));
var encryptionSettings =
    builder.Configuration.GetSection("Encryption").Get<EncryptionSettings>()
    ?? new EncryptionSettings();
encryptionSettings.Validate();
builder.Services.AddSingleton(encryptionSettings);

// --- Cache ---
builder.Services.Configure<CacheSettings>(builder.Configuration.GetSection("Cache"));
var cacheSettings =
    builder.Configuration.GetSection("Cache").Get<CacheSettings>()
    ?? throw new InvalidOperationException("Missing Cache configuration.");
builder.Services.AddSingleton(cacheSettings);

// Parquet file cache is always local disk
builder.Services.AddSingleton<EncryptedFileCache>();

// DEK cache is always local in-memory
var dekTtl =
    cacheSettings.DekTtlSeconds > 0
        ? TimeSpan.FromSeconds(cacheSettings.DekTtlSeconds)
        : (TimeSpan?)null;
builder.Services.AddSingleton<IDekCache>(_ => new DekCache(cacheSettings.DekCacheCapacity, dekTtl));

builder.Services.AddTransient<IS3Get, S3Get>();
builder.Services.AddTransient<IS3Put, S3Put>();
builder.Services.AddTransient<IS3List, S3List>();
builder.Services.AddTransient<IS3Head, S3Head>();

// --- Key Vault Provider ---
// Configure from appsettings.json "KeyVault" section or environment variables.
// Set Provider to "Azure" for production (requires VaultUri), or "Internal" for dev/testing though not yet set fully up
var kvSettings =
    builder.Configuration.GetSection("KeyVault").Get<KeyVaultSettings>()
    ?? new KeyVaultSettings { Provider = "Internal" };

builder.Services.AddSingleton(kvSettings);

builder.Services.AddSingleton<IKeyVaultProvider>(sp =>
{
    var settings = sp.GetRequiredService<KeyVaultSettings>();
    return settings.Provider?.ToLowerInvariant() switch
    {
        "azure" => new AzureKeyVaultProvider(settings),
        _ => new InternalKeyVaultProvider(),
    };
});

builder.Services.AddTransient<IParquetWriter>(sp =>
{
    var provider = sp.GetRequiredService<IKeyVaultProvider>();
    var kvSettings = sp.GetRequiredService<KeyVaultSettings>();
    var encSettings = sp.GetRequiredService<EncryptionSettings>();
    var logger = sp.GetRequiredService<ILogger<ParquetWriter>>();
    return new ParquetWriter(provider, kvSettings.Provider ?? "Internal", logger, encSettings);
});
builder.Services.AddTransient<IParquetReader>(sp =>
{
    var provider = sp.GetRequiredService<IKeyVaultProvider>();
    var dekCache = sp.GetRequiredService<IDekCache>();
    var encSettings = sp.GetRequiredService<EncryptionSettings>();
    var logger = sp.GetRequiredService<ILogger<ParquetReader>>();
    return new ParquetReader(provider, dekCache, logger, encSettings);
});
builder.Services.AddSingleton<IAmazonS3>(sp =>
{
    var opts = sp.GetRequiredService<Microsoft.Extensions.Options.IOptions<S3Settings>>().Value;
    var creds = new BasicAWSCredentials(opts.AccessKeyId, opts.SecretAccessKey);
    var endpoint = AwsCredentialHelper.NormalizeEndpoint(opts.EndpointHostname);
    var config = new AmazonS3Config
    {
        ServiceURL = string.IsNullOrEmpty(endpoint ?? string.Empty) ? null : endpoint,
        ForcePathStyle = true,
    };
    if (
        !string.IsNullOrWhiteSpace(opts.Region)
        && !opts.Region.Equals("auto", StringComparison.OrdinalIgnoreCase)
    )
        config.RegionEndpoint = RegionEndpoint.GetBySystemName(opts.Region);
    return new AmazonS3Client(creds, config);
});

builder.Services.AddOpenApi();

builder.Services.AddHttpClient("UserInfo");
builder.Services.AddSingleton<UserInfoService>();

// Provides access to the authenticated User entity for the current request.
// IHttpContextAccessor is required by CurrentUser to read the ClaimsPrincipal.
builder.Services.AddHttpContextAccessor();
builder.Services.AddScoped<CurrentUser>();

// --- CORS ---
if (builder.Environment.IsDevelopment())
{
    builder.Services.AddCors(options =>
    {
        options.AddPolicy(
            "DevCors",
            policy =>
            {
                policy
                    .WithOrigins("http://localhost:5173")
                    .AllowAnyHeader()
                    .AllowAnyMethod()
                    .AllowCredentials();
            }
        );
    });
}

// --- OIDC Authentication (App API scope) ---
// Load provider list from config; each entry becomes its own JWT Bearer scheme.
// Adding a new provider only requires a new entry in appsettings.json — no code changes.
var oidcProviders =
    builder.Configuration.GetSection("OidcProviders").Get<List<OidcProviderSettings>>() ?? [];

if (oidcProviders.Count == 0)
{
    Console.WriteLine("WARNING: No OidcProviders configured. /api routes will be inaccessible.");
}

var authBuilder = builder.Services.AddAuthentication();

foreach (var provider in oidcProviders)
{
    // Each provider is registered as a distinct JWT Bearer scheme named after the provider.
    // This allows per-scheme token validation while sharing a single authorization policy.
    authBuilder.AddJwtBearer(
        provider.Name,
        options =>
        {
            options.Authority = provider.Authority;
            options.Audience = provider.Audience;

            // The AuthenticationType on the resulting ClaimsIdentity will be set to the scheme
            // name so AppRoutes can identify which provider authenticated the user.
            options.TokenValidationParameters.AuthenticationType = provider.Name;

            // We want to token as-is so we can discover claims and info from the provider
            options.MapInboundClaims = false;

            // Allow HTTP authorities in development (e.g. local PocketID without TLS)
            if (builder.Environment.IsDevelopment())
                options.RequireHttpsMetadata = false;
        }
    );
}

// SigV4 scheme — used exclusively by the /s3 route group.
authBuilder.AddScheme<SigV4AuthenticationOptions, SigV4AuthenticationHandler>("SigV4", _ => { });

// OidcPolicy: accepts a valid JWT from ANY configured provider.
// SigV4Policy: accepts a valid SigV4-signed request (used by S3 routes).
builder.Services.AddAuthorization(authOpts =>
{
    var schemeNames = oidcProviders.Select(p => p.Name).ToArray();

    authOpts.AddPolicy(
        "OidcPolicy",
        policy =>
        {
            policy.RequireAuthenticatedUser();
            if (schemeNames.Length > 0)
                policy.AddAuthenticationSchemes(schemeNames);
        }
    );

    authOpts.AddPolicy(
        "AdminPolicy",
        policy =>
        {
            policy.RequireAuthenticatedUser();
            if (schemeNames.Length > 0)
                policy.AddAuthenticationSchemes(schemeNames);
            policy.RequireAssertion(ctx =>
                ctx.User.HasClaim(c =>
                    c.Type == "isAdmin" && (c.Value == "true" || c.Value == "True")
                )
            );
        }
    );

    authOpts.AddPolicy(
        "SigV4Policy",
        policy =>
        {
            policy.RequireAuthenticatedUser();
            policy.AddAuthenticationSchemes("SigV4");
        }
    );
});

var app = builder.Build();

app.UseHttpLogging();

if (app.Environment.IsDevelopment())
    app.UseCors("DevCors");

app.UseAuthentication();
app.UseAuthorization();

app.MapGet("/health", () => "OSWS Web API running");
app.MapGet(
    "/cache-stats",
    (EncryptedFileCache fileCache) => Results.Text(fileCache.GetDebugInfo())
);

app.MapS3Routes();

// Add root-level S3 bucket creation endpoint for Warp compatibility
// This creates the bucket on the actual S3/R2 backend
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
            // Check if bucket exists first
            var buckets = await s3Client.ListBucketsAsync(cancellationToken);
            if (buckets.Buckets.Any(b => b.BucketName == bucket))
                return Results.Ok(new { message = "Bucket already exists" });

            // Create the bucket
            await s3Client.PutBucketAsync(bucket, cancellationToken);
            return Results.Ok(new { message = "Bucket created" });
        }
        catch (Amazon.S3.AmazonS3Exception e)
        {
            // Some S3-compatible backends don't support bucket creation or return specific errors
            // For R2, buckets must be created via dashboard or API, not via S3 protocol
            if (e.ErrorCode == "NotImplemented" || e.ErrorCode == "AccessDenied")
            {
                // Return OK anyway to allow warp to proceed
                return Results.Ok(new { message = "Bucket creation skipped (not supported by backend)" });
            }
            return Results.Problem(
                statusCode: (int)e.StatusCode,
                title: e.ErrorCode,
                detail: e.Message
            );
        }
    }
);

// Add root-level S3 routes for Warp compatibility (without /s3 prefix, without auth for benchmarking)
// These endpoints bypass OSWS authentication and directly forward to S3 backend.
// For production, these should require proper S3 authentication.
var s3Root = app.MapGroup("");

// S3 PUT - path-style routing for S3 compatibility: /{bucket}/{*key}
// Benchmark mode: bypasses user authentication and encryption for raw S3 passthrough
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
            // Buffer the body to get content length (AWS SDK requires it)
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

            // Forward x-amz-meta-* headers
            foreach (var h in httpRequest.Headers)
            {
                if (h.Key.StartsWith("x-amz-meta-", StringComparison.OrdinalIgnoreCase))
                {
                    var metaKey = h.Key.Substring("x-amz-meta-".Length);
                    req.Metadata[metaKey] = h.Value.ToString();
                }
            }

            var resp = await s3Client.PutObjectAsync(req, cancellationToken);
            // Return S3-style headers
            httpResponse.Headers["ETag"] = resp.ETag;
            if (!string.IsNullOrEmpty(resp.VersionId))
                httpResponse.Headers["x-amz-version-id"] = resp.VersionId;
            httpResponse.StatusCode = 200;
            return Results.Empty;
        }
        catch (Amazon.S3.AmazonS3Exception e)
        {
            var statusCode = (int)e.StatusCode;
            httpResponse.StatusCode = statusCode > 0 ? statusCode : 500;
            var xml = $"<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>{e.ErrorCode}</Code><Message>{System.Security.SecurityElement.Escape(e.Message)}</Message></Error>";
            return Results.Text(xml, "application/xml");
        }
        catch (Exception e)
        {
            httpResponse.StatusCode = 500;
            var xml = $"<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>InternalError</Code><Message>{System.Security.SecurityElement.Escape(e.Message)}</Message></Error>";
            return Results.Text(xml, "application/xml");
        }
    }
);

// S3 GET - path-style routing for S3 compatibility: /{bucket}/{*key}
// Benchmark mode: bypasses user authentication and decryption for raw S3 passthrough
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

            httpResponse.ContentType = resp.Headers.ContentType ?? "application/octet-stream";
            httpResponse.Headers["ETag"] = resp.ETag;
            httpResponse.Headers["Content-Length"] = resp.ContentLength.ToString();
            httpResponse.Headers["Last-Modified"] = (resp.LastModified ?? DateTime.UtcNow).ToUniversalTime().ToString("R");
            if (!string.IsNullOrEmpty(resp.VersionId))
                httpResponse.Headers["x-amz-version-id"] = resp.VersionId;

            await resp.ResponseStream.CopyToAsync(httpResponse.Body, cancellationToken);
            return Results.Empty;
        }
        catch (Amazon.S3.AmazonS3Exception e)
        {
            httpResponse.StatusCode = (int)e.StatusCode;
            var xml = $"<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>{e.ErrorCode}</Code><Message>{e.Message}</Message></Error>";
            return Results.Text(xml, "application/xml");
        }
    }
);

// S3 HEAD BUCKET - path-style routing for S3 compatibility: /{bucket}
// Returns 200 if bucket exists, 404 if not
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
            // Try to get bucket location to check if it exists
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
        catch (Amazon.S3.AmazonS3Exception e)
        {
            httpResponse.StatusCode = (int)e.StatusCode;
            return Results.Empty;
        }
    }
);

// S3 HEAD - path-style routing for S3 compatibility: /{bucket}/{key}
// Benchmark mode: bypasses user authentication for raw S3 passthrough
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
            httpResponse.Headers["Last-Modified"] = (resp.LastModified ?? DateTime.UtcNow).ToUniversalTime().ToString("R");
            if (!string.IsNullOrEmpty(resp.VersionId))
                httpResponse.Headers["x-amz-version-id"] = resp.VersionId;
            httpResponse.StatusCode = 200;

            return Results.Empty;
        }
        catch (Amazon.S3.AmazonS3Exception e)
        {
            httpResponse.StatusCode = (int)e.StatusCode;
            return Results.Empty;
        }
    }
);

// S3 LIST OBJECTS - path-style routing for S3 compatibility: /{bucket}
// Benchmark mode: bypasses user authentication for raw S3 passthrough
// Returns S3-style XML response
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
                MaxKeys = maxKeys ?? 1000
            };
            var resp = await s3Client.ListObjectsV2Async(req, cancellationToken);

            // Build S3-compatible XML response
            var contents = new System.Text.StringBuilder();
            foreach (var obj in resp.S3Objects ?? new List<S3Object>())
            {
                contents.Append($"<Contents><Key>{System.Security.SecurityElement.Escape(obj.Key)}</Key>");
                contents.Append($"<LastModified>{obj.LastModified:yyyy-MM-ddTHH:mm:ss.fffZ}</LastModified>");
                contents.Append($"<ETag>{obj.ETag}</ETag><Size>{obj.Size}</Size>");
                contents.Append($"<StorageClass>{obj.StorageClass?.Value ?? "STANDARD"}</StorageClass></Contents>");
            }

            var xml = $"<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                $"<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">" +
                $"<Name>{resp.Name}</Name>" +
                $"<Prefix>{resp.Prefix ?? ""}</Prefix>" +
                $"<MaxKeys>{resp.MaxKeys}</MaxKeys>" +
                $"<IsTruncated>{resp.IsTruncated.ToString().ToLower()}</IsTruncated>" +
                $"<KeyCount>{resp.KeyCount}</KeyCount>" +
                $"{(string.IsNullOrEmpty(resp.ContinuationToken) ? "" : $"<ContinuationToken>{System.Security.SecurityElement.Escape(resp.ContinuationToken)}</ContinuationToken>")}" +
                $"{(string.IsNullOrEmpty(resp.NextContinuationToken) ? "" : $"<NextContinuationToken>{System.Security.SecurityElement.Escape(resp.NextContinuationToken)}</NextContinuationToken>")}" +
                $"{contents}" +
                $"</ListBucketResult>";

            httpResponse.ContentType = "application/xml";
            return Results.Text(xml, "application/xml");
        }
        catch (Amazon.S3.AmazonS3Exception e)
        {
            httpResponse.StatusCode = (int)e.StatusCode;
            var xml = $"<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>{e.ErrorCode}</Code><Message>{System.Security.SecurityElement.Escape(e.Message)}</Message></Error>";
            return Results.Text(xml, "application/xml");
        }
    }
);

// S3 LIST BUCKETS - path-style routing for S3 compatibility: /
// Benchmark mode: bypasses user authentication for raw S3 passthrough
// Returns S3-style XML response
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
                buckets.Append($"<Bucket><Name>{System.Security.SecurityElement.Escape(b.BucketName)}</Name>");
                buckets.Append($"<CreationDate>{b.CreationDate:yyyy-MM-ddTHH:mm:ss.fffZ}</CreationDate></Bucket>");
            }

            var xml = $"<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                $"<ListAllMyBucketsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">" +
                $"<Owner><ID>{resp.Owner?.Id ?? ""}</ID><DisplayName>{resp.Owner?.DisplayName ?? ""}</DisplayName></Owner>" +
                $"<Buckets>{buckets}</Buckets>" +
                $"</ListAllMyBucketsResult>";

            httpResponse.ContentType = "application/xml";
            return Results.Text(xml, "application/xml");
        }
        catch (Amazon.S3.AmazonS3Exception e)
        {
            httpResponse.StatusCode = (int)e.StatusCode;
            var xml = $"<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>{e.ErrorCode}</Code><Message>{System.Security.SecurityElement.Escape(e.Message)}</Message></Error>";
            return Results.Text(xml, "application/xml");
        }
    }
);

// S3 POST - path-style routing for S3 compatibility: /{bucket}/{*key}
// Benchmark mode support for multi-object delete (POST /{bucket}/?delete)
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
                var xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>MalformedXML</Code><Message>Request body is required for POST on bucket root.</Message></Error>";
                return Results.Text(xml, "application/xml");
            }

            var doc = System.Xml.Linq.XDocument.Parse(body);
            var ns = doc.Root?.Name.Namespace ?? System.Xml.Linq.XNamespace.None;

            var keys = doc
                .Descendants(ns + "Object")
                .Select(x => x.Element(ns + "Key")?.Value)
                .Where(x => !string.IsNullOrWhiteSpace(x))
                .Cast<string>()
                .ToList();

            if (keys.Count == 0)
            {
                httpResponse.StatusCode = 400;
                var xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>MalformedXML</Code><Message>No object keys found in delete request.</Message></Error>";
                return Results.Text(xml, "application/xml");
            }

            var req = new DeleteObjectsRequest
            {
                BucketName = bucket,
                Objects = keys.Select(k => new KeyVersion { Key = k }).ToList(),
                Quiet = false
            };

            var resp = await s3Client.DeleteObjectsAsync(req, cancellationToken);

            var deletedXml = new System.Text.StringBuilder();
            foreach (var obj in resp.DeletedObjects ?? new List<DeletedObject>())
            {
                deletedXml.Append($"<Deleted><Key>{System.Security.SecurityElement.Escape(obj.Key)}</Key></Deleted>");
            }

            var resultXml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                + "<DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
                + deletedXml
                + "</DeleteResult>";

            httpResponse.StatusCode = 200;
            return Results.Text(resultXml, "application/xml");
        }
        catch (Amazon.S3.AmazonS3Exception e)
        {
            httpResponse.StatusCode = (int)e.StatusCode;
            var xml = $"<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>{e.ErrorCode}</Code><Message>{System.Security.SecurityElement.Escape(e.Message)}</Message></Error>";
            return Results.Text(xml, "application/xml");
        }
    }
);

// S3 DELETE - path-style routing for S3 compatibility: /{bucket}/{key}
// Benchmark mode: bypasses user authentication for raw S3 passthrough
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
        catch (Amazon.S3.AmazonS3Exception e)
        {
            httpResponse.StatusCode = (int)e.StatusCode;
            var xml = $"<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>{e.ErrorCode}</Code><Message>{System.Security.SecurityElement.Escape(e.Message)}</Message></Error>";
            return Results.Text(xml, "application/xml");
        }
    }
);

// Map App API routes (OIDC-protected, for the React frontend)
app.MapAppRoutes();
app.MapCredentialRoutes();
app.MapAdminRoutes();

if (app.Environment.IsDevelopment())
    app.MapOpenApi();

app.Run();
