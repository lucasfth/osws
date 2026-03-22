using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using Microsoft.EntityFrameworkCore;
using OSWS.Common.Configuration;
using OSWS.KeyManager.Persistence;
using OSWS.KeyManager.Providers;
using OSWS.Library;
using OSWS.Models.Interfaces;
using OSWS.ParquetSolver;
using OSWS.ParquetSolver.Helpers;
using OSWS.ParquetSolver.Interfaces;
using OSWS.WebApi.Endpoints;
using OSWS.WebApi.Interfaces;

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
var encryptionSettings = builder.Configuration.GetSection("Encryption").Get<EncryptionSettings>()
    ?? new EncryptionSettings();
encryptionSettings.Validate();
builder.Services.AddSingleton(encryptionSettings);

// --- Cache ---
builder.Services.Configure<CacheSettings>(builder.Configuration.GetSection("Cache"));
var cacheSettings = builder.Configuration.GetSection("Cache").Get<CacheSettings>()
    ?? throw new InvalidOperationException("Missing Cache configuration.");
builder.Services.AddSingleton(cacheSettings);

// Parquet file cache is always local disk
builder.Services.AddSingleton<EncryptedFileCache>();

// DEK cache is always local in-memory
var dekTtl =
    cacheSettings.DekTtlSeconds > 0
        ? TimeSpan.FromSeconds(cacheSettings.DekTtlSeconds)
        : (TimeSpan?)null;
builder.Services.AddSingleton<IDekCache>(_ => new DekCache(
    cacheSettings.DekCacheCapacity,
    dekTtl
));

builder.Services.AddTransient<IS3Get, S3Get>();
builder.Services.AddTransient<IS3Put, S3Put>();
builder.Services.AddTransient<IS3List, S3List>();
builder.Services.AddTransient<IS3Head, S3Head>();

// --- Key Vault Provider ---
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

var app = builder.Build();

app.UseHttpLogging();

app.MapGet("/health", () => "OSWS Web API running");
app.MapGet(
    "/cache-stats",
    (EncryptedFileCache fileCache) => Results.Text(fileCache.GetDebugInfo())
);

app.MapS3Routes();

if (app.Environment.IsDevelopment())
    app.MapOpenApi();

app.Run();
