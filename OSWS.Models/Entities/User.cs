namespace OSWS.Models.Entities;

public class User
{
    public int Id { get; set; }

    public required string Name { get; set; }

    /// <summary>
    /// Primary email address, synced from the OIDC provider on login.
    /// </summary>
    public string? Email { get; set; }

    /// <summary>
    /// Whether this user is an RBAC administrator. Synced from the
    /// <c>isRbacAdmin</c> claim on every login via <c>/api/me</c>.
    /// Can also be set manually in the database.
    /// </summary>
    public bool IsRbacAdmin { get; set; }

    public ICollection<Role> Roles { get; set; } = [];

    public ICollection<RoleAssignment> RoleAssignments { get; set; } = [];

    /// <summary>
    /// External identity records that map OIDC provider + subject claims to this user.
    /// Supports multiple providers (PocketID, future providers, etc.)
    /// </summary>
    public ICollection<ExternalIdentity> ExternalIdentities { get; set; } = [];

    public ICollection<S3Credential> S3Credentials { get; set; } = [];
}
