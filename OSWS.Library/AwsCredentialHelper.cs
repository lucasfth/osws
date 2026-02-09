using System.Text.Json;

namespace OSWS.Library;

public static class AwsCredentialHelper
{
    public static bool TryParseCredentials(string json, out string accessKey, out string secretKey, out string sessionToken)
    {
        accessKey = secretKey = sessionToken = string.Empty;
        try
        {
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;

            string? GetValue(params string[] keys)
            {
                foreach (var prop in root.EnumerateObject())
                {
                    foreach (var k in keys)
                    {
                        if (string.Equals(prop.Name, k, StringComparison.OrdinalIgnoreCase))
                            return prop.Value.GetString();
                    }
                }

                return null;
            }

            accessKey = GetValue("accessKeyId", "accessKey", "AccessKeyId") ?? string.Empty;
            secretKey = GetValue("secretAccessKey", "secretKey", "SecretAccessKey") ?? string.Empty;
            sessionToken = GetValue("sessionToken", "token", "SessionToken") ?? string.Empty;

            return !string.IsNullOrEmpty(accessKey) && !string.IsNullOrEmpty(secretKey);
        }
        catch
        {
            return false;
        }
    }

    public static string? NormalizeEndpoint(string? endpoint)
    {
        if (string.IsNullOrWhiteSpace(endpoint))
            return null;

        var e = endpoint.Trim();
        if (!e.StartsWith("http://", StringComparison.OrdinalIgnoreCase) &&
            !e.StartsWith("https://", StringComparison.OrdinalIgnoreCase))
        {
            e = "https://" + e.TrimEnd('/');
        }
        else
        {
            e = e.TrimEnd('/');
        }

        return e;
    }
}
