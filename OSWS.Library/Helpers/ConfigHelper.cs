namespace OSWS.Library.Helpers;

/// <summary>
/// Helper for reading environment configuration with defaults.
/// </summary>
public static class ConfigHelper
{
    private const long MaxUploadBufferBytes = 100L * 1024 * 1024; // 100 MB default

    /// <summary>
    /// Gets whether upload buffering should be forced. Defaults to true if not set.
    /// </summary>
    public static bool GetForceUploadBuffering()
    {
        var forceEnv = Environment.GetEnvironmentVariable("FORCE_UPLOAD_BUFFERING");
        return string.IsNullOrEmpty(forceEnv)
            || forceEnv.Equals("true", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Gets the maximum upload buffer size in bytes. Defaults to 100 MB if not set.
    /// </summary>
    public static long GetMaxUploadBufferBytes()
    {
        var maxBytesStr = Environment.GetEnvironmentVariable("MAX_UPLOAD_BUFFER_BYTES");
        if (!string.IsNullOrEmpty(maxBytesStr) && long.TryParse(maxBytesStr, out var maxBytes))
            return maxBytes;

        return MaxUploadBufferBytes;
    }
}
