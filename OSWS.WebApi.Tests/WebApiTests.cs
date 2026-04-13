using System.Net;
using System.Text;
using System.Text.Json;
using Xunit;

namespace OSWS.WebApi.Tests;

public class WebApiTests : IAsyncLifetime
{
    private readonly HttpClient _httpClient;
    private readonly string _baseUrl;
    private readonly string _bucket;
    private string ParquetFilePath =>
        Path.Combine(AppContext.BaseDirectory, "samples", "house-price.parquet");

    public WebApiTests()
    {
        var config = LoadTestConfig();
        _baseUrl = config["BASE_URL"];
        _bucket = config["R2_BUCKET"];
        _httpClient = new HttpClient { Timeout = TimeSpan.FromSeconds(30) };
    }

    private Dictionary<string, string> LoadTestConfig()
    {
        var possibleConfigPaths = new[]
        {
            Path.Combine(Directory.GetCurrentDirectory(), "test-env.json"),
            Path.Combine(Directory.GetCurrentDirectory(), "..", "test-env.json"),
            Path.Combine(Directory.GetCurrentDirectory(), "..", "..", "test-env.json"),
            Path.Combine(Directory.GetCurrentDirectory(), "..", "..", "..", "test-env.json"),
            Path.Combine(
                Directory.GetCurrentDirectory(),
                "..",
                "..",
                "..",
                "..",
                "OSWS.WebApi.Tests",
                "test-env.json"
            ),
        };

        string? configPath = null;
        foreach (var path in possibleConfigPaths)
        {
            var fullPath = Path.GetFullPath(path);
            if (!File.Exists(fullPath))
                continue;
            configPath = fullPath;
            break;
        }

        if (configPath == null)
        {
            throw new FileNotFoundException(
                $"Configuration file 'test-env.json' not found. Please ensure test-env.json exists in OSWS.WebApi.Tests/ directory."
            );
        }

        var json = File.ReadAllText(configPath);
        using var doc = JsonDocument.Parse(json);
        var devConfig = doc.RootElement.GetProperty("dev");

        return new Dictionary<string, string>
        {
            ["BASE_URL"] = devConfig.GetProperty("BASE_URL").GetString() ?? "http://localhost:5161",
            ["R2_BUCKET"] = devConfig.GetProperty("R2_BUCKET").GetString() ?? "test-storage",
            ["R2_REGION"] = devConfig.GetProperty("R2_REGION").GetString() ?? "us-east-1",
            ["R2_ENDPOINT"] = devConfig.GetProperty("R2_ENDPOINT").GetString() ?? "",
            ["V2_AWS_SDK_CREDENTIALS"] =
                devConfig.GetProperty("V2_AWS_SDK_CREDENTIALS").GetString() ?? "",
        };
    }

    public Task InitializeAsync()
    {
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        // _httpClient?.Dispose();
        await Task.CompletedTask;
    }

    /// <summary>
    /// Test: Health check endpoint
    /// GET /health
    /// </summary>
    [Fact]
    public async Task HealthCheck_ShouldReturn200()
    {
        // Arrange
        var url = $"{_baseUrl}/health";

        // Act
        try
        {
            var response = await _httpClient.GetAsync(url);

            // Assert
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        }
        catch (HttpRequestException ex)
        {
            throw new InvalidOperationException(
                $"API is not running on {_baseUrl}. Start the API: cd OSWS.WebApi && dotnet run",
                ex
            );
        }
    }

    /// <summary>
    /// Test: PUT request to upload and encrypt Parquet file
    /// PUT /{{bucket}}/house-price.parquet
    /// Requires: API running on localhost:5161, test file at samples/house-price.parquet
    /// </summary>
    [Fact]
    public async Task PutParquetFile_ShouldEncryptAndReturn200WithMetadata()
    {
        // Arrange
        if (!File.Exists(ParquetFilePath))
        {
            throw new FileNotFoundException(
                $"Test file not found at {ParquetFilePath}. Place house-price.parquet in OSWS.WebApi/samples/"
            );
        }

        var fileContent = await File.ReadAllBytesAsync(ParquetFilePath);
        var requestContent = new ByteArrayContent(fileContent);
        requestContent.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue(
            "application/octet-stream"
        );

        var url = $"{_baseUrl}/{_bucket}/house-price.parquet";

        // Act
        try
        {
            var response = await _httpClient.PutAsync(url, requestContent);
            var responseBody = await response.Content.ReadAsStringAsync();

            // Assert
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Contains("etag", responseBody, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("versionId", responseBody, StringComparison.OrdinalIgnoreCase);
        }
        catch (HttpRequestException ex)
        {
            throw new InvalidOperationException(
                $"API is not running on {_baseUrl}. Start the API: cd OSWS.WebApi && dotnet run",
                ex
            );
        }
    }

    /// <summary>
    /// Test: GET request to download and decrypt Parquet file
    /// GET /{{bucket}}/house-price.parquet
    /// Note: Assumes PUT request has been executed first
    /// Requires: API running on localhost:5161
    /// </summary>
    [Fact]
    public async Task GetParquetFile_ShouldDecryptAndReturn200WithContent()
    {
        // Arrange
        var url = $"{_baseUrl}/{_bucket}/house-price.parquet";

        // Act
        try
        {
            var response = await _httpClient.GetAsync(url);
            var responseBody = await response.Content.ReadAsStringAsync();

            // Assert
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Equal(
                "application/octet-stream",
                response.Content.Headers.ContentType?.MediaType
            );
            Assert.NotEmpty(responseBody);
        }
        catch (HttpRequestException ex)
        {
            throw new InvalidOperationException(
                $"API is not running on {_baseUrl}. Start the API: cd OSWS.WebApi && dotnet run",
                ex
            );
        }
    }

    /// <summary>
    /// Test: GET request with invalid credentials
    ///
    /// This scenario originally verified that supplying bad credentials via a
    /// query parameter triggered an error response. The current API reads its
    /// S3 credentials from configuration at startup, so the request-level
    /// parameter is ignored and no error can be generated. The test is therefore
    /// annotated as skipped until an alternative validation mechanism is added.
    /// Requires: API running on localhost:5161
    /// </summary>
    [Fact(Skip = "invalid-credentials flow removed; API uses configured S3 settings")]
    public async Task GetParquetFile_WithInvalidCredentials_ShouldReturnErrorStatus()
    {
        // Arrange
        var invalidCredentials = Uri.EscapeDataString(
            "{\"accessKeyId\":\"invalid\",\"secretAccessKey\":\"invalid\"}"
        );
        var url = $"{_baseUrl}/{_bucket}/house-price.parquet";

        // Act
        try
        {
            var response = await _httpClient.GetAsync(url);

            // Assert
            Assert.True(
                response.StatusCode
                    is HttpStatusCode.BadRequest
                        or HttpStatusCode.Unauthorized
                        or HttpStatusCode.Forbidden,
                $"Expected 400, 401, or 403, but got {(int)response.StatusCode}"
            );
        }
        catch (HttpRequestException ex)
        {
            throw new InvalidOperationException(
                $"API is not running on {_baseUrl}. Start the API: cd OSWS.WebApi && dotnet run",
                ex
            );
        }
    }

    /// <summary>
    /// Test: PUT and GET workflow (integration test)
    /// Uploads a file, then retrieves it to verify encryption/decryption cycle
    /// Requires: API running on localhost:5161, test file at samples/house-price.parquet
    /// </summary>
    [Fact]
    public async Task PutThenGet_ShouldSuccessfullyEncryptAndDecrypt()
    {
        // Arrange
        if (!File.Exists(ParquetFilePath))
        {
            throw new FileNotFoundException(
                $"Test file not found at {ParquetFilePath}. Place house-price.parquet in OSWS.WebApi/samples/"
            );
        }

        var originalFileContent = await File.ReadAllBytesAsync(ParquetFilePath);
        var requestContent = new ByteArrayContent(originalFileContent);
        requestContent.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue(
            "application/octet-stream"
        );

        var basePutUrl = $"{_baseUrl}/{_bucket}/house-price.parquet";

        try
        {
            // Act - PUT
            var putResponse = await _httpClient.PutAsync(basePutUrl, requestContent);
            var putResponseBody = await putResponse.Content.ReadAsStringAsync();

            // Assert PUT
            Assert.Equal(HttpStatusCode.OK, putResponse.StatusCode);
            Assert.Contains("etag", putResponseBody, StringComparison.OrdinalIgnoreCase);

            // Act - GET
            var getResponse = await _httpClient.GetAsync(basePutUrl);
            var getResponseContent = await getResponse.Content.ReadAsByteArrayAsync();

            // Assert GET
            Assert.Equal(HttpStatusCode.OK, getResponse.StatusCode);
            Assert.NotEmpty(getResponseContent);
            Assert.Equal(
                "application/octet-stream",
                getResponse.Content.Headers.ContentType?.MediaType
            );
        }
        catch (HttpRequestException ex)
        {
            throw new InvalidOperationException(
                $"API is not running on {_baseUrl}. Start the API: cd OSWS.WebApi && dotnet run",
                ex
            );
        }
    }
}
