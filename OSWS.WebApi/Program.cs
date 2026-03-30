using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Authorization;
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
builder.Services.AddScoped<IAuthorizationHandler, RbacAdminHandler>();
builder.Services.AddScoped<RoleHierarchyService>();

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
            policy.AddRequirements(new RbacAdminRequirement());
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
if (encryptionSettings.BenchmarkMode)
{
    app.MapBenchmarkS3PassthroughRoutes();
}

// Map App API routes (OIDC-protected, for the React frontend)
app.MapAppRoutes();
app.MapCredentialRoutes();
app.MapAdminRoutes();

if (app.Environment.IsDevelopment())
    app.MapOpenApi();

app.Run();
