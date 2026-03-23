using System.Security.Claims;
using System.Text.Encodings.Web;
using Microsoft.AspNetCore.Authentication;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using OSWS.KeyManager.Persistence;

namespace OSWS.WebApi.Authentication;

/// <summary>
/// Authenticates requests using AWS Signature Version 4 (SigV4).
///
/// Expected Authorization header format:
///   AWS4-HMAC-SHA256 Credential=&lt;AccessKeyId&gt;/&lt;date&gt;/&lt;region&gt;/&lt;service&gt;/aws4_request,
///                   SignedHeaders=&lt;headers&gt;, Signature=&lt;hex&gt;
///
/// The handler resolves the caller's identity by looking up the AccessKeyId in the
/// S3Credentials table. Signature verification is a placeholder and always passes;
/// it should be implemented before deploying to production.
/// </summary>
public class SigV4AuthenticationHandler(
    IOptionsMonitor<SigV4AuthenticationOptions> options,
    ILoggerFactory logger,
    UrlEncoder encoder,
    OswsContext db
) : AuthenticationHandler<SigV4AuthenticationOptions>(options, logger, encoder)
{
    private const string SigV4Prefix = "AWS4-HMAC-SHA256 ";

    protected override async Task<AuthenticateResult> HandleAuthenticateAsync()
    {
        // --- 1. Check for the Authorization header ---
        if (!Request.Headers.TryGetValue("Authorization", out var authHeader))
            return AuthenticateResult.NoResult();

        var authValue = authHeader.ToString();
        if (!authValue.StartsWith(SigV4Prefix, StringComparison.Ordinal))
            return AuthenticateResult.NoResult();

        // --- 2. Parse the SigV4 Authorization header ---
        // Format: AWS4-HMAC-SHA256 Credential=<id>/<date>/<region>/<service>/aws4_request,
        //                          SignedHeaders=<headers>, Signature=<hex>
        if (
            !TryParseSigV4Header(
                authValue,
                out var accessKeyId,
                out var date,
                out var signedHeaders,
                out var signature
            )
        )
            return AuthenticateResult.Fail("Malformed AWS4-HMAC-SHA256 Authorization header.");

        // --- 3. Validate clock skew using x-amz-date ---
        // var amzDate = Request.Headers["x-amz-date"].ToString();
        // if (!TryParseAmzDate(amzDate, out var requestTime))
        //     return AuthenticateResult.Fail("Missing or invalid x-amz-date header.");
        //
        // var skew = Math.Abs((DateTimeOffset.UtcNow - requestTime).TotalSeconds);
        // if (skew > Options.MaxClockSkewSeconds)
        //     return AuthenticateResult.Fail(
        //         $"Request timestamp is outside the allowed clock skew window ({Options.MaxClockSkewSeconds}s)."
        //     );

        // --- 4. Look up the AccessKeyId in the database ---
        var credential = await db
            .S3Credentials.Include(c => c.User)
            .Include(s3Credential => s3Credential.DefaultRole)
            .FirstOrDefaultAsync(c => c.AccessKeyId == accessKeyId && c.IsActive);

        if (credential is null)
            return AuthenticateResult.Fail($"Unknown or inactive access key: {accessKeyId}");

        // --- 5. Verify the SigV4 signature ---
        // TODO: Implement real SigV4 signature verification.
        // Steps:
        //   a) Re-derive the signing key:
        //      kDate    = HMAC-SHA256("AWS4" + credential.SecretKey, date)
        //      kRegion  = HMAC-SHA256(kDate, region)
        //      kService = HMAC-SHA256(kRegion, service)
        //      kSigning = HMAC-SHA256(kService, "aws4_request")
        //   b) Build the canonical request string:
        //      <HTTPMethod>\n<CanonicalURI>\n<CanonicalQueryString>\n
        //      <CanonicalHeaders>\n<SignedHeaders>\n<HashedPayload>
        //      (use x-amz-content-sha256 as the pre-computed payload hash)
        //   c) Build the string-to-sign:
        //      "AWS4-HMAC-SHA256\n<amzDate>\n<credentialScope>\n<SHA256(canonicalRequest)>"
        //   d) Compute expectedSignature = HMAC-SHA256(kSigning, stringToSign).ToHexString()
        //   e) Use CryptographicOperations.FixedTimeEquals() to compare with `signature`.
        //
        // PLACEHOLDER: signature verification always succeeds.
        var signatureValid = VerifySignaturePlaceholder(
            credential.SecretKey,
            signature,
            signedHeaders
        );
        if (!signatureValid)
            return AuthenticateResult.Fail("Signature mismatch.");

        // --- 6. Build the ClaimsPrincipal ---
        var claims = new List<Claim>
        {
            new(ClaimTypes.NameIdentifier, credential.User.Id.ToString()),
            new(ClaimTypes.Name, credential.User.Name),
            new("access_key_id", credential.AccessKeyId),
            new("default_role", credential.DefaultRole?.Id.ToString() ?? "0"),
        };

        if (credential.User.Email is not null)
            claims.Add(new Claim(ClaimTypes.Email, credential.User.Email));

        var identity = new ClaimsIdentity(claims, Scheme.Name);
        var principal = new ClaimsPrincipal(identity);
        var ticket = new AuthenticationTicket(principal, Scheme.Name);

        Logger.LogInformation(
            "SigV4 authenticated user {UserId} ({Name}) via access key {AccessKeyId}",
            credential.User.Id,
            credential.User.Name,
            credential.AccessKeyId
        );

        return AuthenticateResult.Success(ticket);
    }

    // -------------------------------------------------------------------------
    // Parsing helpers
    // -------------------------------------------------------------------------

    /// <summary>
    /// Parses the AWS4-HMAC-SHA256 Authorization header into its components.
    /// </summary>
    private static bool TryParseSigV4Header(
        string headerValue,
        out string accessKeyId,
        out string date,
        out string signedHeaders,
        out string signature
    )
    {
        accessKeyId = date = signedHeaders = signature = string.Empty;

        // Strip the prefix, leaving: Credential=…, SignedHeaders=…, Signature=…
        var payload = headerValue[SigV4Prefix.Length..];

        var parts = payload.Split(',', StringSplitOptions.TrimEntries);
        if (parts.Length < 3)
            return false;

        string? credentialValue = null;
        string? signedHeaderValue = null;
        string? signatureValue = null;

        foreach (var part in parts)
        {
            if (part.StartsWith("Credential=", StringComparison.Ordinal))
                credentialValue = part["Credential=".Length..];
            if (part.StartsWith("SignedHeaders=", StringComparison.Ordinal))
                signedHeaderValue = part["SignedHeaders=".Length..];
            if (part.StartsWith("Signature=", StringComparison.Ordinal))
                signatureValue = part["Signature=".Length..];
        }

        if (credentialValue is null || signedHeaderValue is null || signatureValue is null)
            return false;

        // Credential format: <AccessKeyId>/<YYYYMMDD>/<region>/<service>/aws4_request
        var credParts = credentialValue.Split('/');
        if (credParts.Length < 5)
            return false;

        accessKeyId = credParts[0];
        date = credParts[1];
        signedHeaders = signedHeaderValue;
        signature = signatureValue;
        return true;
    }

    /// <summary>
    /// Parses the x-amz-date header (ISO8601 basic format: yyyyMMddTHHmmssZ).
    /// </summary>
    private static bool TryParseAmzDate(string amzDate, out DateTimeOffset result)
    {
        return DateTimeOffset.TryParseExact(
            amzDate,
            "yyyyMMddTHHmmssZ",
            System.Globalization.CultureInfo.InvariantCulture,
            System.Globalization.DateTimeStyles.AssumeUniversal,
            out result
        );
    }

    // -------------------------------------------------------------------------
    // Signature verification — PLACEHOLDER
    // -------------------------------------------------------------------------

    /// <summary>
    /// Placeholder for SigV4 signature verification. Always returns true.
    /// Replace with real HMAC-SHA256 derivation and constant-time comparison
    /// before deploying to production. See the TODO comment above for steps.
    /// </summary>
    private static bool VerifySignaturePlaceholder(
        string secretKey,
        string signature,
        string signedHeaders
    )
    {
        // TODO: Replace with real SigV4 HMAC-SHA256 verification.
        return true;
    }
}
