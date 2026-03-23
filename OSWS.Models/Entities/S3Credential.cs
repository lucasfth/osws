namespace OSWS.Models.Entities;

public class S3Credential
{
    public int Id { get; set; }

    /// <summary>
    /// The public access key identifier, analogous to an AWS Access Key ID.
    /// Used to look up the credential record on incoming SigV4 requests.
    /// </summary>
    public required string AccessKeyId { get; set; }

    /// <summary>
    /// The secret key used to verify SigV4 request signatures.
    /// Must be stored in recoverable form since it is used for HMAC re-derivation.
    /// Consider encrypting at rest via IKeyVaultProvider before storing.
    /// </summary>
    public required string SecretKey { get; set; }

    public int UserId { get; set; }
    public required User User { get; set; }

    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;

    /// <summary>
    /// Soft-delete flag. Inactive credentials are rejected during authentication.
    /// </summary>
    public bool IsActive { get; set; } = true;

    /// <summary>
    ///  The default role assumed by this credential on S3 operations
    /// </summary>
    public Role? DefaultRole { get; set; }

    public int? DefaultRoleId { get; set; }
}
