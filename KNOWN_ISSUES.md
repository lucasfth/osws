# OSWS Known Issues

Issues that are acknowledged but not yet scheduled for a fix.

## Security

### S3 Secret Keys Stored as Plaintext in Database

**Location:** `OSWS.Models/Entities/S3Credential.cs`, `OSWS.WebApi/Endpoints/CredentialRoutes.cs`

S3 credential secret keys are stored in plaintext in PostgreSQL.
The SigV4 authentication handler reads the raw key for HMAC derivation, which means hashing is not an option.

- **Risk:** If the database is compromised, all S3 credentials are immediately usable.
- **Mitigation:** Database access control, network isolation, HTTPS in transit.
- **Eventual fix:** Encrypt secret keys at rest using a separate encryption key (e.g., from Azure Key Vault), decrypt on read during SigV4 verification.

### JIT Admin Provisioning from OIDC Claims

**Location:** `OSWS.WebApi/Endpoints/AppRoutes.cs:72-76`

The `isRbacAdmin` flag is set from OIDC user info or JWT claims on every login.
If the OIDC provider exposes this as a self-service field, users could grant themselves admin.

- **Risk:** Depends on OIDC provider configuration.

## Code Quality

### Reflection for PutObjectRequest.ContentLength

**Location:** `OSWS.WebApi/Endpoints/S3Put.cs:84-87`

Uses reflection to set `ContentLength` on `PutObjectRequest` because the property setter may not be directly accessible.
This is fragile across AWS SDK version upgrades.

- **Eventual fix:** Check if newer AWS SDK versions expose a public setter, or use `Headers["Content-Length"]`.
