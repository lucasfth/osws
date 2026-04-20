using System.Security.Cryptography;
using System.Text;
using OSWS.WebApi.Authentication;
using Xunit;

namespace OSWS.WebApi.Tests.Authentication;

/// <summary>
/// Tests for SigV4Signer using AWS official test vectors from:
/// https://docs.aws.amazon.com/general/latest/gr/sigv4-calculate-signature.html
/// </summary>
public class SigV4SignerTests
{
    // AWS test vector credentials (official docs example)
    private const string TestSecretKey = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
    private const string TestDate = "20150830";
    private const string TestRegion = "us-east-1";
    private const string TestService = "iam";

    [Fact]
    public void DeriveSigningKey_WithAwsTestVector_ProducesExpectedKey()
    {
        // Known signing key for the above credentials (verified against Python hmac implementation)
        // Credentials: Secret=wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY, Date=20150830, Region=us-east-1, Service=iam
        // https://docs.aws.amazon.com/general/latest/gr/sigv4-calculate-signature.html
        const string expectedHex =
            "c4afb1cc5771d871763a393e44b703571b55cc28424d1a5e86da6ed3c154a4b9";

        var key = SigV4Signer.DeriveSigningKey(TestSecretKey, TestDate, TestRegion, TestService);

        Assert.Equal(expectedHex, Convert.ToHexString(key).ToLowerInvariant());
    }

    [Fact]
    public void DeriveSigningKey_DifferentSecrets_ProduceDifferentKeys()
    {
        var key1 = SigV4Signer.DeriveSigningKey("secretA", TestDate, TestRegion, TestService);
        var key2 = SigV4Signer.DeriveSigningKey("secretB", TestDate, TestRegion, TestService);

        Assert.False(key1.SequenceEqual(key2));
    }

    [Fact]
    public void DeriveSigningKey_SameInputs_ProduceSameKey()
    {
        var key1 = SigV4Signer.DeriveSigningKey(TestSecretKey, TestDate, TestRegion, TestService);
        var key2 = SigV4Signer.DeriveSigningKey(TestSecretKey, TestDate, TestRegion, TestService);

        Assert.True(key1.SequenceEqual(key2));
    }

    [Theory]
    [InlineData("/", "/")]
    [InlineData("", "/")]
    [InlineData("/bucket/my key", "/bucket/my%20key")]
    [InlineData("/bucket/path/file.txt", "/bucket/path/file.txt")]
    [InlineData("/bucket/path with spaces/file.txt", "/bucket/path%20with%20spaces/file.txt")]
    public void CanonicalizeUri_EncodesPathSegments(string input, string expected)
    {
        Assert.Equal(expected, SigV4Signer.CanonicalizeUri(input));
    }

    [Fact]
    public void CanonicalizeQueryString_Empty_ReturnsEmpty()
    {
        Assert.Equal(string.Empty, SigV4Signer.CanonicalizeQueryString(null));
        Assert.Equal(string.Empty, SigV4Signer.CanonicalizeQueryString(""));
        Assert.Equal(string.Empty, SigV4Signer.CanonicalizeQueryString("?"));
    }

    [Fact]
    public void CanonicalizeQueryString_SortsAlphabetically()
    {
        // z comes before a when sorted in reverse, so sorted output should be Action first
        var result = SigV4Signer.CanonicalizeQueryString("?Version=2010-05-08&Action=ListUsers");

        Assert.Equal("Action=ListUsers&Version=2010-05-08", result);
    }

    [Fact]
    public void CanonicalizeQueryString_EncodesSpecialCharacters()
    {
        var result = SigV4Signer.CanonicalizeQueryString("?key=value with space&b=a+b");

        Assert.Contains("value%20with%20space", result);
    }

    [Fact]
    public void CanonicalizeHeaders_LowercasesAndTrimsValues()
    {
        var headers = new Microsoft.AspNetCore.Http.HeaderDictionary
        {
            ["Host"] = "example.amazonaws.com",
            ["X-Amz-Date"] = "20150830T123600Z",
        };

        var result = SigV4Signer.CanonicalizeHeaders(headers, "host;x-amz-date");

        Assert.Equal("host:example.amazonaws.com\nx-amz-date:20150830T123600Z\n", result);
    }

    [Fact]
    public void CanonicalizeHeaders_SortsHeadersByName()
    {
        var headers = new Microsoft.AspNetCore.Http.HeaderDictionary
        {
            ["X-Amz-Date"] = "20150830T123600Z",
            ["Host"] = "example.amazonaws.com",
        };

        // signedHeaders already sorted (per SigV4 spec the client sends them sorted)
        var result = SigV4Signer.CanonicalizeHeaders(headers, "host;x-amz-date");

        var lines = result.Split('\n', StringSplitOptions.RemoveEmptyEntries);
        Assert.Equal("host:example.amazonaws.com", lines[0]);
        Assert.Equal("x-amz-date:20150830T123600Z", lines[1]);
    }

    [Fact]
    public void BuildCanonicalRequest_ProducesExpectedFormat()
    {
        // Minimal GET request with no query string and empty body
        const string emptyBodyHash =
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

        var result = SigV4Signer.BuildCanonicalRequest(
            method: "GET",
            canonicalUri: "/",
            canonicalQueryString: "",
            canonicalHeaders: "host:iam.amazonaws.com\nx-amz-date:20150830T123600Z\n",
            signedHeaders: "host;x-amz-date",
            payloadHash: emptyBodyHash
        );

        var expected =
            $"GET\n/\n\nhost:iam.amazonaws.com\nx-amz-date:20150830T123600Z\n\nhost;x-amz-date\n{emptyBodyHash}";
        Assert.Equal(expected, result);
    }

    [Fact]
    public void BuildStringToSign_ProducesExpectedFormat()
    {
        const string canonicalRequest =
            "GET\n/\n\nhost:iam.amazonaws.com\nx-amz-date:20150830T123600Z\n\nhost;x-amz-date\ne3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

        var result = SigV4Signer.BuildStringToSign(
            amzDate: "20150830T123600Z",
            credentialScope: "20150830/us-east-1/iam/aws4_request",
            canonicalRequest: canonicalRequest
        );

        Assert.StartsWith("AWS4-HMAC-SHA256\n", result);
        Assert.Contains("20150830T123600Z\n", result);
        Assert.Contains("20150830/us-east-1/iam/aws4_request\n", result);
        // Third line is HexEncode(SHA256(canonicalRequest)) — 64 hex chars
        var lines = result.Split('\n');
        Assert.Equal(4, lines.Length);
        Assert.Equal(64, lines[3].Length);
        Assert.Matches("^[0-9a-f]{64}$", lines[3]);
    }

    [Fact]
    public void ComputeSignature_IsDeterministic()
    {
        var key = SigV4Signer.DeriveSigningKey(TestSecretKey, TestDate, TestRegion, TestService);
        const string stringToSign = "AWS4-HMAC-SHA256\ntest\ntest\ntest";

        var sig1 = SigV4Signer.ComputeSignature(key, stringToSign);
        var sig2 = SigV4Signer.ComputeSignature(key, stringToSign);

        Assert.Equal(sig1, sig2);
        Assert.Equal(64, sig1.Length); // 32 bytes = 64 hex chars
        Assert.Matches("^[0-9a-f]{64}$", sig1);
    }

    [Fact]
    public void VerifySignature_CorrectSignature_ReturnsTrue()
    {
        // Compute a valid signature and immediately verify it
        const string emptyBodyHash =
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        const string amzDate = "20150830T123600Z";
        const string credentialScope = "20150830/us-east-1/iam/aws4_request";

        var canonicalRequest = SigV4Signer.BuildCanonicalRequest(
            "GET",
            "/",
            "",
            "host:iam.amazonaws.com\nx-amz-date:20150830T123600Z\n",
            "host;x-amz-date",
            emptyBodyHash
        );

        var stringToSign = SigV4Signer.BuildStringToSign(
            amzDate,
            credentialScope,
            canonicalRequest
        );
        var signingKey = SigV4Signer.DeriveSigningKey(
            TestSecretKey,
            TestDate,
            TestRegion,
            TestService
        );
        var validSignature = SigV4Signer.ComputeSignature(signingKey, stringToSign);

        Assert.True(SigV4Signer.VerifySignature(signingKey, stringToSign, validSignature));
    }

    [Fact]
    public void VerifySignature_WrongSignature_ReturnsFalse()
    {
        const string stringToSign =
            "AWS4-HMAC-SHA256\n20150830T123600Z\n20150830/us-east-1/iam/aws4_request\nabc";
        var signingKey = SigV4Signer.DeriveSigningKey(
            TestSecretKey,
            TestDate,
            TestRegion,
            TestService
        );

        Assert.False(SigV4Signer.VerifySignature(signingKey, stringToSign, new string('0', 64)));
    }

    [Fact]
    public void VerifySignature_MalformedSignature_ReturnsFalse()
    {
        const string stringToSign =
            "AWS4-HMAC-SHA256\n20150830T123600Z\n20150830/us-east-1/iam/aws4_request\nabc";
        var signingKey = SigV4Signer.DeriveSigningKey(
            TestSecretKey,
            TestDate,
            TestRegion,
            TestService
        );

        // Signature that isn't valid hex or wrong length
        Assert.False(
            SigV4Signer.VerifySignature(signingKey, stringToSign, "not-a-valid-hex-signature")
        );
    }
}
