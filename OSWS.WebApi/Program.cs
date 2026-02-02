using System.Text.Json.Serialization;
using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Mvc;
using OSWS.Models.DTOs;
using OSWS.WebApi.Interfaces;

var builder = WebApplication.CreateSlimBuilder(args);

builder.Services.ConfigureHttpJsonOptions(options =>
{
    // options.SerializerOptions.TypeInfoResolverChain.Insert(0, AppJsonSerializerContext.Default);
});

var r2Endpoint = Environment.GetEnvironmentVariable("R2_ENDPOINT") ??
                 "https://f2087fdd677819f9e9095e98c80537b9.r2.cloudflarestorage.com";
var r2AccessKey = Environment.GetEnvironmentVariable("R2_ACCESS_KEY_ID") ?? "";
var r2SecretKey = Environment.GetEnvironmentVariable("R2_SECRET_ACCESS_KEY") ?? "";
var r2Region = Environment.GetEnvironmentVariable("R2_REGION") ?? "auto"; // can be any string for S3-compatible providers

// Configure AmazonS3Client for Cloudflare R2
builder.Services.AddSingleton<IAmazonS3>(sp =>
{
    var creds = new BasicAWSCredentials(r2AccessKey, r2SecretKey);

    var config = new AmazonS3Config
    {
        ServiceURL = r2Endpoint,
        ForcePathStyle = true,
        // Optionally set RegionEndpoint if you have a meaningful region name:
        RegionEndpoint = RegionEndpoint.GetBySystemName(r2Region)
    };

    return new AmazonS3Client(creds, config);
});

builder.Services.AddOpenApi();

var app = builder.Build();

app.MapGet("/s3/get", async (
    IS3Get s3Get,
    [AsParameters] Params prms,
    [AsParameters] S3Options s3Options,
    [FromQuery] int retryOptions = 3,
    [FromQuery] int timeoutOptionsMs = 3000,
    CancellationToken cancellationToken = default) =>
{
    return await s3Get.GetObject(prms, s3Options, retryOptions, timeoutOptionsMs, cancellationToken);
});
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

// var pingApi = app.MapGet("/", () => "Hello World!");

app.Run();
