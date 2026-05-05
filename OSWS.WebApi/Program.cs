using Microsoft.EntityFrameworkCore;
using OSWS.Common.Configuration;
using OSWS.KeyManager.Persistence;
using OSWS.WebApi.Extensions;

var builder = WebApplication.CreateSlimBuilder(args);

builder.Logging.ClearProviders();
builder.Logging.AddConsole();
builder.Services.AddHttpLogging(o => { });

builder.Configuration.AddEnvironmentVariables();

// --- Service Registration ---
builder.Services.AddOswsDatabase(builder.Configuration);
builder.Services.AddOswsKeyVault(builder.Configuration);
builder.Services.AddOswsEncryption(builder.Configuration);
builder.Services.AddOswsCaching(builder.Configuration);
builder.Services.AddOswsS3(builder.Configuration);
builder.Services.AddOswsParquet();
builder.Services.AddOswsUserServices();
builder.Services.AddOswsAuthentication(builder.Configuration, builder.Environment.IsDevelopment());
builder.Services.AddOswsRateLimiting(builder.Configuration);
builder.Services.AddOpenApi();
builder.Services.AddOpenApiDocument();

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

var app = builder.Build();

app.UseHttpLogging();

if (app.Environment.IsDevelopment())
{
    app.UseCors("DevCors");
    app.UseOpenApi();
    app.UseSwaggerUi();
}

app.UseAuthentication();
app.UseAuthorization();
app.UseRateLimiter();

using (var scope = app.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<OswsContext>();
    await db.Database.MigrateAsync();
}

var encryptionSettings = app.Services.GetRequiredService<EncryptionSettings>();
app.MapOswsEndpoints(encryptionSettings);

app.Run();
