using System.Security.Cryptography;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using OSWS.KeyManager.Persistence;
using OSWS.Models.Entities;

namespace OSWS.Performance.Benchmarks.Infrastructure;

/// <summary>
/// Seeds and cleans up benchmark S3 credentials with related user and role records for OSWS perf runs.
/// Emits credential values as environment variable lines for the benchmark harness.
/// </summary>
public static class BenchmarkS3CredentialSeeder
{
    public static async Task<int> RunSeedAsync(string[] args)
    {
        var options = ParseOptions(args);
        var connectionString = GetConnectionString();
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            Console.Error.WriteLine("ERROR: Missing ConnectionStrings:OswsContext");
            return 1;
        }

        await using var db = CreateContext(connectionString);

        var user = await db.Users.FirstOrDefaultAsync(u => u.Name == options.UserName);
        if (user is null)
        {
            user = new User { Name = options.UserName };
            db.Users.Add(user);
        }

        Role? defaultRole = null;
        if (!string.IsNullOrWhiteSpace(options.RoleName))
        {
            defaultRole = await db.Roles.FirstOrDefaultAsync(r => r.Name == options.RoleName);
            if (defaultRole is null)
            {
                defaultRole = new Role { Name = options.RoleName };
                db.Roles.Add(defaultRole);
            }

            var needsAssignment = user.Id == 0 || defaultRole.Id == 0;
            if (!needsAssignment)
            {
                needsAssignment = !await db.RoleAssignments.AnyAsync(ra =>
                    ra.UserId == user.Id && ra.RoleId == defaultRole.Id
                );
            }

            if (needsAssignment)
            {
                db.RoleAssignments.Add(new RoleAssignment { User = user, Role = defaultRole });
            }
        }

        var accessKeyId = GenerateAccessKeyId();
        var secretKey = GenerateSecretKey();

        var credential = new S3Credential
        {
            AccessKeyId = accessKeyId,
            SecretKey = secretKey,
            User = user,
            IsActive = true,
            DefaultRole = defaultRole,
        };

        db.S3Credentials.Add(credential);
        await db.SaveChangesAsync();

        Console.WriteLine($"WARP_OSWS_ACCESS_KEY={credential.AccessKeyId}");
        Console.WriteLine($"WARP_OSWS_SECRET_KEY={credential.SecretKey}");
        Console.WriteLine($"WARP_OSWS_CREDENTIAL_ID={credential.Id}");
        Console.WriteLine($"WARP_OSWS_USER_ID={credential.UserId}");
        Console.WriteLine($"WARP_OSWS_ROLE_ID={credential.DefaultRoleId}");

        return 0;
    }

    public static async Task<int> RunCleanupAsync(string[] args)
    {
        var options = ParseOptions(args);
        var connectionString = GetConnectionString();
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            Console.Error.WriteLine("ERROR: Missing ConnectionStrings:OswsContext");
            return 1;
        }

        await using var db = CreateContext(connectionString);

        if (!string.IsNullOrWhiteSpace(options.AccessKeyId))
        {
            var credential = await db.S3Credentials.FirstOrDefaultAsync(c =>
                c.AccessKeyId == options.AccessKeyId
            );
            if (credential is not null)
            {
                db.S3Credentials.Remove(credential);
            }
        }

        if (!string.IsNullOrWhiteSpace(options.UserName))
        {
            var user = await db.Users.Include(u => u.S3Credentials)
                .FirstOrDefaultAsync(u => u.Name == options.UserName);
            if (user is not null)
            {
                var assignments = await db.RoleAssignments
                    .Where(ra => ra.UserId == user.Id)
                    .ToListAsync();
                if (assignments.Count > 0)
                {
                    db.RoleAssignments.RemoveRange(assignments);
                }

                if (user.S3Credentials.Count == 0)
                {
                    db.Users.Remove(user);
                }
            }
        }

        if (!string.IsNullOrWhiteSpace(options.RoleName))
        {
            var role = await db.Roles.Include(r => r.S3Credentials)
                .Include(r => r.Users)
                .FirstOrDefaultAsync(r => r.Name == options.RoleName);
            if (role is not null && role.Users.Count == 0 && role.S3Credentials.Count == 0)
            {
                db.Roles.Remove(role);
            }
        }

        await db.SaveChangesAsync();
        return 0;
    }

    private static string? GetConnectionString()
    {
        var configuration = new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.json", optional: true)
            .AddJsonFile("appsettings.Development.json", optional: true)
            .AddEnvironmentVariables()
            .Build();

        return configuration.GetConnectionString("OswsContext");
    }

    private static OswsContext CreateContext(string connectionString)
    {
        var options = new DbContextOptionsBuilder<OswsContext>()
            .UseNpgsql(connectionString)
            .Options;

        return new OswsContext(options);
    }

    private static string GenerateAccessKeyId()
    {
        const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        var bytes = RandomNumberGenerator.GetBytes(20);
        return new string(bytes.Select(b => chars[b % chars.Length]).ToArray());
    }

    private static string GenerateSecretKey()
    {
        var bytes = RandomNumberGenerator.GetBytes(40);
        return Convert.ToBase64String(bytes).Replace('+', '-').Replace('/', '_').TrimEnd('=');
    }

    private static SeederOptions ParseOptions(string[] args)
    {
        return new SeederOptions
        {
            UserName = GetArgValue(args, "--user-name") ?? "warp-benchmark",
            RoleName = GetArgValue(args, "--role-name") ?? "warp-benchmark-role",
            AccessKeyId = GetArgValue(args, "--access-key"),
        };
    }

    private static string? GetArgValue(string[] args, string key)
    {
        for (var i = 0; i < args.Length - 1; i++)
        {
            if (string.Equals(args[i], key, StringComparison.OrdinalIgnoreCase))
            {
                return args[i + 1];
            }
        }

        return null;
    }

    private sealed class SeederOptions
    {
        public string UserName { get; set; } = "warp-benchmark";
        public string RoleName { get; set; } = "warp-benchmark-role";
        public string? AccessKeyId { get; set; }
    }
}
