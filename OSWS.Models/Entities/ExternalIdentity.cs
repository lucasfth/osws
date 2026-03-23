namespace OSWS.Models.Entities;

/// <summary>
/// Maps an external OIDC identity (provider + subject) to a local User.
/// One user can have multiple external identities (one per provider).
/// </summary>
public class ExternalIdentity
{
    public int Id { get; set; }

    /// <summary>
    /// Logical provider name, e.g. "pocketid", "google", "github".
    /// Must match the key used in OidcProviders config.
    /// </summary>
    public required string Provider { get; set; }

    /// <summary>
    /// The 'sub' claim from the OIDC token — unique per provider.
    /// </summary>
    public required string Subject { get; set; }

    /// <summary>
    /// Optional: the email returned by this provider at the time of login.
    /// </summary>
    public string? Email { get; set; }

    public int UserId { get; set; }
    public User User { get; set; } = null!;

    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    public DateTime LastSeenAt { get; set; } = DateTime.UtcNow;
}
