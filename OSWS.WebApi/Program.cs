using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using Microsoft.EntityFrameworkCore;
using OSWS.Common.Configuration;
using OSWS.KeyManager.Persistence;
using OSWS.Library;
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

var r2Endpoint =
    Environment.GetEnvironmentVariable("R2_ENDPOINT")
    ?? "https://f2087fdd677819f9e9095e98c80537b9.r2.cloudflarestorage.com";
var r2AccessKey = Environment.GetEnvironmentVariable("R2_ACCESS_KEY_ID") ?? "";
var r2SecretKey = Environment.GetEnvironmentVariable("R2_SECRET_ACCESS_KEY") ?? "";
var r2Region = Environment.GetEnvironmentVariable("R2_REGION") ?? "auto"; // can be any string for S3-compatible providers

builder.Services.AddTransient<IS3Get, S3Get>();
builder.Services.AddTransient<IS3Put, S3Put>();
builder.Services.AddTransient<IParquetWriter, ParquetWriter>();
builder.Services.AddTransient<IParquetReader, ParquetReader>();
builder.Services.AddSingleton<IAmazonS3>(sp =>
{
    var creds = new BasicAWSCredentials(r2AccessKey, r2SecretKey);

    var config = new AmazonS3Config
    {
        ServiceURL = r2Endpoint?.TrimEnd('/'),
        ForcePathStyle = true,
        // Optionally set RegionEndpoint if you have a meaningful region name:
        // RegionEndpoint = RegionEndpoint.GetBySystemName(r2Region)
    };

    if (
        !string.IsNullOrEmpty(r2Region)
        && !r2Region.Equals("auto", StringComparison.OrdinalIgnoreCase)
    )
    {
        config.RegionEndpoint = RegionEndpoint.GetBySystemName(r2Region);
    }

    return new AmazonS3Client(creds, config);
});

builder.Services.AddSingleton<IS3ClientFactory, S3ClientFactory>();

builder.Services.AddOpenApi();

var app = builder.Build();

app.MapGet("/", () => "Hello World!");

// Map S3 routes (GET, PUT) to their handlers
app.MapS3Routes();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

// var pingApi = app.MapGet("/", () => "Hello World!");

app.Run();
