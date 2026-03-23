namespace OSWS.Common.Configuration;

/// <summary>
/// Configuration for encryption behavior and operation logging in OSWS.
/// Bind from appsettings.json section "Encryption".
/// </summary>
public class EncryptionSettings
{
    /// <summary>
    /// When true, disables all encryption/decryption. Parquet files are stored and retrieved unencrypted from S3.
    /// Default: false (encryption enabled)
    /// </summary>
    public bool DisableEncryption { get; set; } = false;

    /// <summary>
    /// Data Encryption Key size in bits. Valid values: 256, 512, 1024, 2048.
    /// This controls the size of random AES DEKs generated for each column.
    /// Note: DEK size is different from the KEK (Key Encryption Key) which is RSA-2048.
    /// Default: 256 bits (32 bytes)
    /// </summary>
    public int DekSizeBits { get; set; } = 256;

    /// <summary>
    /// When true, enables structured logging of operation timings.
    /// Logs include: DEK fetch/unwrap times, column decryption times, parquet I/O times.
    /// Timing data is logged in JSON format suitable for benchmarking analysis.
    /// Default: false (logging disabled)
    /// </summary>
    public bool EnableOperationLogging { get; set; } = false;

    /// <summary>
    /// Validates the configuration and throws if DekSizeBits is invalid.
    /// </summary>
    public void Validate()
    {
        var validSizes = new[] { 256, 512, 1024, 2048 };
        if (!validSizes.Contains(DekSizeBits))
        {
            throw new InvalidOperationException(
                $"Invalid DekSizeBits: {DekSizeBits}. Must be one of: {string.Join(", ", validSizes)}"
            );
        }
    }

    /// <summary>
    /// Converts DekSizeBits to bytes for use in cryptographic operations.
    /// </summary>
    public int GetDekSizeBytes() => DekSizeBits / 8;
}
