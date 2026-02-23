namespace OSWS.Common.Configuration;

/// <summary>
/// Configuration for connecting to a key vault provider.
/// Bind from appsettings.json section "KeyVault".
/// </summary>
public class KeyVaultSettings
{
    /// <summary>
    /// The provider to use: "Azure", "Internal".
    /// </summary>
    public string Provider { get; set; } = "Azure";

    /// <summary>
    /// Azure Key Vault URI (e.g. "https://my-vault.vault.azure.net/").
    /// Required when Provider is "Azure".
    /// </summary>
    public string? VaultUri { get; set; }

    /// <summary>
    /// Optional: Azure tenant ID for authentication.
    /// If omitted, DefaultAzureCredential discovers it automatically.
    /// </summary>
    public string? TenantId { get; set; }

    /// <summary>
    /// Optional: Azure client (application) ID for service principal auth.
    /// </summary>
    public string? ClientId { get; set; }

    /// <summary>
    /// Optional: Azure client secret for service principal auth.
    /// Prefer environment variables or managed identity in production.
    /// </summary>
    public string? ClientSecret { get; set; }
}
