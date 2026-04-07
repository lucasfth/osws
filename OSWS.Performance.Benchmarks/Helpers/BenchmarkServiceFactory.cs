using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OSWS.Common.Configuration;
using OSWS.KeyManager.Persistence;
using OSWS.KeyManager.Providers;
using OSWS.Library;
using OSWS.Models.Interfaces;
using OSWS.ParquetSolver.Helpers;

namespace OSWS.Performance.Benchmarks.Helpers
{
    /// <summary>
    /// Builds a ServiceProvider configured identically to WebApi.
    /// Configuration:
    ///   • appsettings.json (base config)
    ///   • appsettings.Development.json (optional overrides)
    ///   • Environment variables (highest priority)
    ///
    /// This matches the WebApi Program.cs approach exactly.
    /// </summary>
    public static class BenchmarkServiceFactory
    {
        public static ServiceProvider BuildServiceProvider()
        {
            // Load configuration - same as WebApi Program.cs
            var config = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", optional: true)
                .AddJsonFile("appsettings.Development.json", optional: true)
                .AddEnvironmentVariables()
                .Build();

            var services = new ServiceCollection();
            services.AddSingleton<IConfiguration>(config);
            services.Configure<KeyVaultSettings>(config.GetSection("KeyVault"));
            services.Configure<S3Settings>(config.GetSection("S3Settings"));

            // Cache settings - same as WebApi
            var cacheSettings =
                config.GetSection("Cache").Get<CacheSettings>()
                ?? new CacheSettings { EnableFileCache = false };
            services.AddSingleton(cacheSettings);

            // Logging - for ILogger injection in benchmarks
            services.AddLogging(builder =>
            {
                builder.AddConsole();
                builder.SetMinimumLevel(LogLevel.Information);
            });

            // Key Vault - same as WebApi
            services.AddSingleton<IKeyVaultProvider>(sp =>
            {
                var settings = sp.GetRequiredService<IOptions<KeyVaultSettings>>().Value;
                return settings.Provider.ToLowerInvariant() switch
                {
                    "azure" when !string.IsNullOrWhiteSpace(settings.VaultUri) =>
                        new AzureKeyVaultProvider(settings),
                    _ => new InternalKeyVaultProvider(),
                };
            });

            // S3 Client - same as WebApi
            services.AddSingleton<IAmazonS3>(sp =>
            {
                var opts = sp.GetRequiredService<IOptions<S3Settings>>().Value;
                var creds = new BasicAWSCredentials(opts.AccessKeyId, opts.SecretAccessKey);
                var endpoint = AwsCredentialHelper.NormalizeEndpoint(opts.EndpointHostname);

                var s3Config = new AmazonS3Config
                {
                    ServiceURL = string.IsNullOrEmpty(endpoint ?? string.Empty) ? null : endpoint,
                    ForcePathStyle = true,
                };

                if (
                    !string.IsNullOrWhiteSpace(opts.Region)
                    && !opts.Region.Equals("auto", StringComparison.OrdinalIgnoreCase)
                )
                {
                    s3Config.RegionEndpoint = RegionEndpoint.GetBySystemName(opts.Region);
                }

                return new AmazonS3Client(creds, s3Config);
            });

            return services.BuildServiceProvider();
        }
    }
}
