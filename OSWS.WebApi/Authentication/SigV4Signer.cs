using System.Security.Cryptography;
using System.Text;
using Microsoft.AspNetCore.Http;

namespace OSWS.WebApi.Authentication;

/// <summary>
/// Pure static helpers implementing AWS Signature Version 4 signing.
/// All methods are side-effect-free; the handler wires them to the HTTP context.
///
/// Algorithm reference:
///   https://docs.aws.amazon.com/general/latest/gr/sigv4_signing.html
/// </summary>
internal static class SigV4Signer
{
    /// <summary>
    /// Derives the SigV4 signing key using the four-step HMAC-SHA256 chain:
    ///   kDate    = HMAC-SHA256("AWS4" + secretKey, date)
    ///   kRegion  = HMAC-SHA256(kDate, region)
    ///   kService = HMAC-SHA256(kRegion, service)
    ///   kSigning = HMAC-SHA256(kService, "aws4_request")
    /// </summary>
    internal static byte[] DeriveSigningKey(
        string secretKey,
        string date,
        string region,
        string service
    )
    {
        var kDate = HmacSha256(Encoding.UTF8.GetBytes("AWS4" + secretKey), date);
        var kRegion = HmacSha256(kDate, region);
        var kService = HmacSha256(kRegion, service);
        return HmacSha256(kService, "aws4_request");
    }

    /// <summary>
    /// URI-encodes each path segment (preserves forward slashes).
    /// Empty path or "/" returns "/".
    /// </summary>
    internal static string CanonicalizeUri(string? path)
    {
        if (string.IsNullOrEmpty(path) || path == "/")
            return "/";

        // Encode each segment individually; slashes are path separators, not content.
        var segments = path.Split('/');
        return string.Join("/", segments.Select(Uri.EscapeDataString));
    }

    /// <summary>
    /// URI-encodes and sorts query parameters per SigV4 spec.
    /// Null / empty / bare "?" returns empty string.
    /// </summary>
    internal static string CanonicalizeQueryString(string? queryString)
    {
        if (string.IsNullOrEmpty(queryString) || queryString == "?")
            return string.Empty;

        var raw = queryString.StartsWith('?') ? queryString[1..] : queryString;

        var pairs = raw.Split('&', StringSplitOptions.RemoveEmptyEntries)
            .Select(p =>
            {
                var eq = p.IndexOf('=');
                if (eq < 0)
                    return (Key: Uri.EscapeDataString(Uri.UnescapeDataString(p)), Value: "");
                return (
                    Key: Uri.EscapeDataString(Uri.UnescapeDataString(p[..eq])),
                    Value: Uri.EscapeDataString(Uri.UnescapeDataString(p[(eq + 1)..]))
                );
            })
            .OrderBy(p => p.Key, StringComparer.Ordinal)
            .ThenBy(p => p.Value, StringComparer.Ordinal);

        return string.Join("&", pairs.Select(p => $"{p.Key}={p.Value}"));
    }

    /// <summary>
    /// Builds the canonical headers block.
    /// <paramref name="signedHeaderNames"/> is the semicolon-separated, lowercase,
    /// sorted list from the Authorization header (e.g. "host;x-amz-date").
    /// Each header appears as "lowercase-name:trimmed-value\n".
    /// </summary>
    internal static string CanonicalizeHeaders(IHeaderDictionary headers, string signedHeaderNames)
    {
        var sb = new StringBuilder();
        foreach (var name in signedHeaderNames.Split(';').OrderBy(n => n, StringComparer.Ordinal))
        {
            // IHeaderDictionary lookup is case-insensitive.
            var value = headers.TryGetValue(name, out var vals)
                ? string.Join(",", vals.Select(v => CollapseWhitespace(v?.Trim() ?? "")))
                : string.Empty;

            sb.Append(name.ToLowerInvariant()).Append(':').Append(value).Append('\n');
        }
        return sb.ToString();
    }

    /// <summary>
    /// Assembles the canonical request string.
    /// Format (each component on its own line):
    ///   HTTPMethod
    ///   CanonicalURI
    ///   CanonicalQueryString
    ///   CanonicalHeaders       (already ends with \n)
    ///   SignedHeaders
    ///   HexPayloadHash
    /// </summary>
    internal static string BuildCanonicalRequest(
        string method,
        string canonicalUri,
        string canonicalQueryString,
        string canonicalHeaders,
        string signedHeaders,
        string payloadHash
    ) =>
        $"{method}\n{canonicalUri}\n{canonicalQueryString}\n{canonicalHeaders}\n{signedHeaders}\n{payloadHash}";

    /// <summary>
    /// Builds the AWS4-HMAC-SHA256 string-to-sign.
    /// Format:
    ///   AWS4-HMAC-SHA256
    ///   &lt;amzDate&gt;             (yyyyMMddTHHmmssZ)
    ///   &lt;credentialScope&gt;    (date/region/service/aws4_request)
    ///   HexEncode(SHA256(canonicalRequest))
    /// </summary>
    internal static string BuildStringToSign(
        string amzDate,
        string credentialScope,
        string canonicalRequest
    )
    {
        var hashedRequest = Sha256Hex(canonicalRequest);
        return $"AWS4-HMAC-SHA256\n{amzDate}\n{credentialScope}\n{hashedRequest}";
    }

    /// <summary>
    /// Computes the hex-encoded HMAC-SHA256 of <paramref name="stringToSign"/>
    /// using the derived <paramref name="signingKey"/>.
    /// </summary>
    internal static string ComputeSignature(byte[] signingKey, string stringToSign) =>
        Convert.ToHexString(HmacSha256(signingKey, stringToSign)).ToLowerInvariant();

    /// <summary>
    /// Constant-time comparison of the computed signature against the provided one.
    /// Returns false if <paramref name="providedSignature"/> cannot be decoded as hex.
    /// </summary>
    internal static bool VerifySignature(
        byte[] signingKey,
        string stringToSign,
        string providedSignature
    )
    {
        // Compute expected first so all valid requests take constant time through this path.
        var expectedBytes = HmacSha256(signingKey, stringToSign);

        byte[] providedBytes;
        try
        {
            providedBytes = Convert.FromHexString(providedSignature);
        }
        catch (FormatException)
        {
            // Run a dummy comparison to avoid timing differences between the
            // "invalid hex" and "wrong signature" paths.
            CryptographicOperations.FixedTimeEquals(expectedBytes, expectedBytes);
            return false;
        }

        // Constant-time comparison to prevent timing attacks.
        return CryptographicOperations.FixedTimeEquals(expectedBytes, providedBytes);
    }

    private static byte[] HmacSha256(byte[] key, string data) =>
        HMACSHA256.HashData(key, Encoding.UTF8.GetBytes(data));

    private static string Sha256Hex(string data) =>
        Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(data))).ToLowerInvariant();

    private static readonly System.Text.RegularExpressions.Regex WhitespaceRegex = new(
        @"\s+",
        System.Text.RegularExpressions.RegexOptions.Compiled
    );

    private static string CollapseWhitespace(string value) => WhitespaceRegex.Replace(value, " ");
}
