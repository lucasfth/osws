using System.Threading.RateLimiting;
using Microsoft.AspNetCore.RateLimiting;
using OSWS.Common.Configuration;

namespace OSWS.WebApi.Extensions;

public static class RateLimitExtensions
{
    public static IServiceCollection AddOswsRateLimiting(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        var settings = configuration.GetSection("RateLimiting").Get<RateLimitSettings>()
                       ?? new RateLimitSettings();
        services.AddSingleton(settings);

        services.AddRateLimiter(options =>
        {
            options.RejectionStatusCode = 429;

            if (settings.S3RequestsPerMinute > 0)
            {
                options.AddFixedWindowLimiter("s3", opt =>
                {
                    opt.PermitLimit = settings.S3RequestsPerMinute;
                    opt.Window = TimeSpan.FromMinutes(1);
                    opt.QueueLimit = 0;
                });
            }

            if (settings.ApiRequestsPerMinute > 0)
            {
                options.AddFixedWindowLimiter("api", opt =>
                {
                    opt.PermitLimit = settings.ApiRequestsPerMinute;
                    opt.Window = TimeSpan.FromMinutes(1);
                    opt.QueueLimit = 0;
                });
            }

            if (settings.AdminRequestsPerMinute > 0)
            {
                options.AddFixedWindowLimiter("admin", opt =>
                {
                    opt.PermitLimit = settings.AdminRequestsPerMinute;
                    opt.Window = TimeSpan.FromMinutes(1);
                    opt.QueueLimit = 0;
                });
            }

            if (settings.CredentialCreationsPerHour > 0)
            {
                options.AddFixedWindowLimiter("credential-create", opt =>
                {
                    opt.PermitLimit = settings.CredentialCreationsPerHour;
                    opt.Window = TimeSpan.FromHours(1);
                    opt.QueueLimit = 0;
                });
            }
        });

        return services;
    }
}
