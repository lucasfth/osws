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
/// S3Credentials table, validates the SigV4 HMAC-SHA256 signature, and enforces
/// clock-skew validation via the x-amz-date header.
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
        if (!Request.Headers.TryGetValue("Authorization", out var authHeader))
            return AuthenticateResult.NoResult();

        var authValue = authHeader.ToString();
        if (!authValue.StartsWith(SigV4Prefix, StringComparison.Ordinal))
            return AuthenticateResult.NoResult();

        if (
            !TryParseSigV4Header(
                authValue,
                out var accessKeyId,
                out var date,
                out var region,
                out var service,
                out var signedHeaders,
                out var signature
            )
        )
            return AuthenticateResult.Fail("Malformed AWS4-HMAC-SHA256 Authorization header.");

        var amzDate = Request.Headers["x-amz-date"].ToString();
        if (!TryParseAmzDate(amzDate, out var requestTime))
            return AuthenticateResult.Fail("Missing or invalid x-amz-date header.");

        var skew = Math.Abs((DateTimeOffset.UtcNow - requestTime).TotalSeconds);
        if (skew > Options.MaxClockSkewSeconds)
            return AuthenticateResult.Fail(
                $"Request timestamp is outside the allowed clock skew window ({Options.MaxClockSkewSeconds}s)."
            );

        var credential = await db
            .S3Credentials.Include(c => c.User)
            .Include(s3Credential => s3Credential.DefaultRole)
            .FirstOrDefaultAsync(c => c.AccessKeyId == accessKeyId && c.IsActive);

        if (credential is null)
            // Return NoResult() instead of Fail() to allow routes that don't require
            // SigV4Policy to proceed with unauthenticated requests. This enables
            // benchmark tools like Warp to connect without valid credentials.
            return AuthenticateResult.NoResult();

        var payloadHash = Request.Headers["x-amz-content-sha256"].ToString();
        if (string.IsNullOrEmpty(payloadHash))
            return AuthenticateResult.Fail("Missing x-amz-content-sha256 header.");

        var canonicalUri = SigV4Signer.CanonicalizeUri(Request.Path.Value);
        var canonicalQueryString = SigV4Signer.CanonicalizeQueryString(Request.QueryString.Value);
        var canonicalHeaders = SigV4Signer.CanonicalizeHeaders(Request.Headers, signedHeaders);
        var canonicalRequest = SigV4Signer.BuildCanonicalRequest(
            Request.Method,
            canonicalUri,
            canonicalQueryString,
            canonicalHeaders,
            signedHeaders,
            payloadHash
        );

        var credentialScope = $"{date}/{region}/{service}/aws4_request";
        var stringToSign = SigV4Signer.BuildStringToSign(
            amzDate,
            credentialScope,
            canonicalRequest
        );
        var signingKey = SigV4Signer.DeriveSigningKey(credential.SecretKey, date, region, service);

        if (!SigV4Signer.VerifySignature(signingKey, stringToSign, signature))
            return AuthenticateResult.Fail("Signature mismatch.");

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

    /// <summary>
    /// Parses the AWS4-HMAC-SHA256 Authorization header into its components.
    /// </summary>
    private static bool TryParseSigV4Header(
        string headerValue,
        out string accessKeyId,
        out string date,
        out string region,
        out string service,
        out string signedHeaders,
        out string signature
    )
    {
        accessKeyId = date = region = service = signedHeaders = signature = string.Empty;

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

        if (credParts[4] != "aws4_request")
            return false;

        accessKeyId = credParts[0];
        date = credParts[1];
        region = credParts[2];
        service = credParts[3];
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
}
