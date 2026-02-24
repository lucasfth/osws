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
using OSWS.ParquetSolver.Interfaces;
using OSWS.WebApi.Endpoints;
using OSWS.WebApi.Interfaces;

var builder = WebApplication.CreateSlimBuilder(args);

builder.Services.ConfigureHttpJsonOptions(options =>
{
    // options.SerializerOptions.TypeInfoResolverChain.Insert(0, AppJsonSerializerContext.Default);
});

// Configure DatabaseSettings from appsettings.json
builder.Configuration.AddEnvironmentVariables();
builder.Services.AddDbContext<OswsContext>(opts =>
    opts.UseNpgsql(builder.Configuration.GetConnectionString("OswsContext"))
);

builder.Services.Configure<S3Settings>(builder.Configuration.GetSection("S3Settings"));

builder.Services.AddTransient<IS3Get, S3Get>();
builder.Services.AddTransient<IS3Put, S3Put>();

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
    var settings = sp.GetRequiredService<KeyVaultSettings>();
    return new ParquetWriter(provider, settings.Provider ?? "Internal");
});
builder.Services.AddTransient<IParquetReader>(sp =>
{
    var provider = sp.GetRequiredService<IKeyVaultProvider>();
    return new ParquetReader(provider);
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
    {
        config.RegionEndpoint = RegionEndpoint.GetBySystemName(opts.Region);
    }

    return new AmazonS3Client(creds, config);
});

builder.Services.AddOpenApi();

var app = builder.Build();

app.MapGet("/health", () => "OSWS Web API running");

// Map S3 routes (GET, PUT) to their handlers
app.MapS3Routes();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.Run();
