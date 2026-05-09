using Amazon;
using Amazon.Runtime;
using Amazon.S3;
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
using OSWS.WebApi.Endpoints;
using OSWS.WebApi.Interfaces;
using OSWS.WebApi.Services;

namespace OSWS.WebApi.Extensions;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddOswsDatabase(
        this IServiceCollection services,
        IConfiguration configuration
    )
    {
        services.AddDbContext<OswsContext>(opts =>
            opts.UseNpgsql(configuration.GetConnectionString("OswsContext"))
        );
        return services;
    }

    public static IServiceCollection AddOswsEncryption(
        this IServiceCollection services,
        IConfiguration configuration
    )
    {
        services.Configure<EncryptionSettings>(configuration.GetSection("Encryption"));
        var encryptionSettings =
            configuration.GetSection("Encryption").Get<EncryptionSettings>()
            ?? new EncryptionSettings();
        encryptionSettings.Validate();
        services.AddSingleton(encryptionSettings);
        return services;
    }

    public static IServiceCollection AddOswsCaching(
        this IServiceCollection services,
        IConfiguration configuration
    )
    {
        services.Configure<CacheSettings>(configuration.GetSection("Cache"));
        var cacheSettings =
            configuration.GetSection("Cache").Get<CacheSettings>()
            ?? throw new InvalidOperationException("Missing Cache configuration.");
        services.AddSingleton(cacheSettings);

        // Parquet file cache is always local disk
        services.AddSingleton<EncryptedFileCache>();

        // DEK cache is always local in-memory with role-based TTL
        var dekTtl =
            cacheSettings.DekTtlSeconds > 0
                ? TimeSpan.FromSeconds(cacheSettings.DekTtlSeconds)
                : (TimeSpan?)null;
        var dekAdminTtl =
            cacheSettings.DekAdminTtlSeconds > 0
                ? TimeSpan.FromSeconds(cacheSettings.DekAdminTtlSeconds)
                : (TimeSpan?)null;
        services.AddSingleton<IDekCache>(_ => new DekCache(
            cacheSettings.EnableDekCache,
            cacheSettings.DekCacheCapacity,
            dekTtl,
            dekAdminTtl
        ));

        return services;
    }

    public static IServiceCollection AddOswsS3(
        this IServiceCollection services,
        IConfiguration configuration
    )
    {
        services.Configure<S3Settings>(configuration.GetSection("S3Settings"));

        services.AddTransient<IS3Get, S3Get>();
        services.AddTransient<IS3Put, S3Put>();
        services.AddTransient<IS3Delete, S3Delete>();
        services.AddTransient<IS3List, S3List>();
        services.AddTransient<IS3Head, S3Head>();
        services.AddScoped<ParquetUploadService>();
        services.AddTransient<S3ObjectFetcher>();

        services.AddSingleton<IAmazonS3>(sp =>
        {
            var opts =
                sp.GetRequiredService<Microsoft.Extensions.Options.IOptions<S3Settings>>().Value;
            var creds = new BasicAWSCredentials(opts.AccessKeyId, opts.SecretAccessKey);
            var endpoint = AwsCredentialHelper.NormalizeEndpoint(opts.EndpointHostname);
            var hasCustomEndpoint = !string.IsNullOrEmpty(endpoint);
            var config = new AmazonS3Config { ForcePathStyle = true };
            // ServiceURL and RegionEndpoint are mutually exclusive in AWS SDK v4:
            // setting one clears the other. Use ServiceURL for custom endpoints (e.g. MinIO),
            // RegionEndpoint only when connecting to real AWS S3.
            if (hasCustomEndpoint)
            {
                config.ServiceURL = endpoint;
                if (
                    !string.IsNullOrWhiteSpace(opts.Region)
                    && !opts.Region.Equals("auto", StringComparison.OrdinalIgnoreCase)
                )
                    config.AuthenticationRegion = opts.Region;
            }
            else if (
                !string.IsNullOrWhiteSpace(opts.Region)
                && !opts.Region.Equals("auto", StringComparison.OrdinalIgnoreCase)
            )
            {
                config.RegionEndpoint = RegionEndpoint.GetBySystemName(opts.Region);
            }
            return new AmazonS3Client(creds, config);
        });

        return services;
    }

    public static IServiceCollection AddOswsKeyVault(
        this IServiceCollection services,
        IConfiguration configuration
    )
    {
        // Configure from appsettings.json "KeyVault" section or environment variables.
        // Set Provider to "Azure" for production (requires VaultUri), or "Internal" for dev/testing though not yet set fully up
        var kvSettings =
            configuration.GetSection("KeyVault").Get<KeyVaultSettings>()
            ?? new KeyVaultSettings { Provider = "Internal" };

        services.AddSingleton(kvSettings);

        services.AddSingleton<IKeyVaultProvider>(sp =>
        {
            var settings = sp.GetRequiredService<KeyVaultSettings>();
            return settings.Provider?.ToLowerInvariant() switch
            {
                "azure" => new AzureKeyVaultProvider(settings),
                _ => new InternalKeyVaultProvider(),
            };
        });

        return services;
    }

    public static IServiceCollection AddOswsParquet(this IServiceCollection services)
    {
        services.AddTransient<IParquetWriter>(sp =>
        {
            var provider = sp.GetRequiredService<IKeyVaultProvider>();
            var kvSettings = sp.GetRequiredService<KeyVaultSettings>();
            var encSettings = sp.GetRequiredService<EncryptionSettings>();
            var logger = sp.GetRequiredService<ILogger<ParquetWriter>>();
            return new ParquetWriter(
                provider,
                kvSettings.Provider ?? "Internal",
                logger,
                encSettings
            );
        });
        services.AddTransient<IParquetReader>(sp =>
        {
            var provider = sp.GetRequiredService<IKeyVaultProvider>();
            var dekCache = sp.GetRequiredService<IDekCache>();
            var encSettings = sp.GetRequiredService<EncryptionSettings>();
            var logger = sp.GetRequiredService<ILogger<ParquetReader>>();
            return new ParquetReader(provider, dekCache, logger, encSettings);
        });

        return services;
    }

    public static IServiceCollection AddOswsUserServices(this IServiceCollection services)
    {
        services.AddHttpClient("UserInfo");
        services.AddSingleton<UserInfoService>();

        // Provides access to the authenticated User entity for the current request.
        // IHttpContextAccessor is required by CurrentUser to read the ClaimsPrincipal.
        services.AddHttpContextAccessor();
        services.AddScoped<CurrentUser>();
        services.AddScoped<RoleHierarchyService>();
        services.AddScoped<PermissionService>();

        return services;
    }
}
