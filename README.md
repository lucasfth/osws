# OSWS

Object Store Wrapper Service (OSWS) is an S3-compatible API that applies Parquet Modular Encryption on top of any S3 storage, using Azure Key Vault for key management and a PostgreSQL-backed RBAC system for column-level access control.

Created as part of the thesis [Fine-grained role-based access control by encryption](https://github.com/lucasfth/fine-grained-role-based-access-control-by-encryption) at the IT University of Copenhagen by [Andreas Trøstrup](@duckth) and [Lucas Hanson](@lucasfth).

## Uses

OSWS can be used for later research and development but should not be used in production in its current state. Query engines which do not save metadata for the size of the Parquet file (e.g. DuckDB) can use OSWS without any modifications.

## Architecture

| Project | Role |
| ------- | ---- |
| **OSWS.WebApi** | ASP.NET minimal API host. Registers S3 endpoints, Parquet services, key vault, authentication (SigV4 + OIDC), caching, and rate limiting. |
| **OSWS.Library** | S3 client infrastructure — `S3ClientFactory` dynamically creates `IAmazonS3` clients from per-request options. |
| **OSWS.Models** | Shared DTOs and EF Core entities (`User`, `Role`, `RoleAssignment`, `S3Credential`). Defines `IKeyVaultProvider` interface. |
| **OSWS.KeyManager** | EF Core DbContext (PostgreSQL) and key vault provider implementations (Azure Key Vault, Internal in-memory). |
| **OSWS.ParquetSolver** | Parquet Modular Encryption via ParquetSharp. Uses envelope encryption through `IKeyVaultProvider`. |
| **OSWS.Common** | Shared configuration models (`EncryptionSettings`, `CacheSettings`, `RateLimitSettings`, `OidcProviderSettings`, etc.). |

## Quick Start (Docker)

```bash
cp .env.example .env
# Fill in your values
docker compose up --build
```

This starts OSWS on `http://localhost:5000`. Health check: `GET http://localhost:5000/health`.

You need a PostgreSQL instance reachable from the container and an S3-compatible backend. For local development, use the E2E Docker Compose (see [E2E tests](#e2e-tests)) which includes Postgres and MinIO.

## Authentication

OSWS has two separate authentication schemes for different API surfaces, plus an E2E bypass mode.

### S3 API — SigV4

All S3-compatible routes (`/{bucket}`, `/{bucket}/{key}`, etc.) require **AWS Signature V4** authentication. S3 credentials (access key + secret key) are created through the management API and stored in PostgreSQL.

SigV4 is implemented in `OSWS.WebApi/Authentication/SigV4AuthenticationHandler.cs`. The handler verifies the signature against the stored secret key.

### Management API — OIDC

Routes under `/api/` (admin, credentials, user profile) require **OIDC JWT Bearer** authentication. Multiple OIDC providers can be configured simultaneously — each is registered as its own JWT Bearer scheme.

The management API also enforces an `isRbacAdmin` claim for admin routes (`/api/admin/*`).

### E2E Mode

When `App__E2EMode=true`, the OIDC authentication is bypassed and only SigV4 authentication is used. This is used by the E2E test suite.

## Configuration

Settings are loaded from `appsettings.json`, `appsettings.{Environment}.json`, and environment variables (which use `__` as a separator, e.g. `KeyVault__Provider`).

### Key Vault

| Key | Type | Default | Description |
| --- | ---- | ------- | ----------- |
| `KeyVault:Provider` | string | `"Azure"` | `"Azure"` or `"Internal"` (in-memory, keys lost on restart) |
| `KeyVault:VaultUri` | string | — | Azure Key Vault URI |
| `KeyVault:TenantId` | string | — | Azure tenant ID (for `DefaultAzureCredential`) |
| `KeyVault:ClientId` | string | — | Azure client ID |
| `KeyVault:ClientSecret` | string | — | Azure client secret |

When `Provider` is `"Azure"`, authentication uses `DefaultAzureCredential`: env vars → managed identity → Azure CLI → Visual Studio. For local development, `az login` is sufficient.

### Encryption

| Key | Type | Default | Description |
| --- | ---- | ------- | ----------- |
| `Encryption:DisableEncryption` | bool | `false` | Bypass parquet encryption entirely (forwarding-only proxy) |
| `Encryption:DekSizeBits` | int | `256` | Data Encryption Key size: 128, 192, or 256 |
| `Encryption:EnableOperationLogging` | bool | `false` | Log per-operation KV calls for debugging |

### Cache

| Key | Type | Default | Description |
| --- | ---- | ------- | ----------- |
| `Cache:EnableFileCache` | bool | `true` | Cache decrypted files on disk |
| `Cache:MaxCacheSizeBytes` | long | `10737418240` | Max file cache size (10 GB) |
| `Cache:CacheDirectory` | string | `null` | File cache directory (`null` = temp directory) |
| `Cache:DekCacheProvider` | string | `"Local"` | DEK cache provider |
| `Cache:DekCacheCapacity` | int | `2500` | Max entries in DEK cache |
| `Cache:DekTtlSeconds` | int | `0` | DEK TTL in seconds (`0` = no expiry) |

### Rate Limiting

| Key | Type | Default | Description |
| --- | ---- | ------- | ----------- |
| `RateLimiting:S3RequestsPerMinute` | int | `6000` | S3 API requests per minute |
| `RateLimiting:ApiRequestsPerMinute` | int | `120` | Management API requests per minute |
| `RateLimiting:AdminRequestsPerMinute` | int | `60` | Admin API requests per minute |
| `RateLimiting:CredentialCreationsPerHour` | int | `10` | Max credential creations per hour |

### Database

| Key | Type | Default | Description |
| --- | ---- | ------- | ----------- |
| `ConnectionStrings:OswsContext` | string | — | PostgreSQL connection string |

### S3 Storage

| Key | Type | Default | Description |
| --- | ---- | ------- | ----------- |
| `S3Settings:EndpointHostname` | string | — | S3-compatible endpoint URL (e.g. R2, MinIO) |
| `S3Settings:AccessKeyId` | string | — | S3 access key |
| `S3Settings:SecretAccessKey` | string | — | S3 secret key |
| `S3Settings:Region` | string | `"auto"` | S3 region |

### OIDC Providers

Configured as an array. Each entry:

| Key | Type | Default | Description |
| --- | ---- | ------- | ----------- |
| `Name` | string | — | Unique scheme name (used as JWT Bearer scheme) |
| `DisplayName` | string | — | Human-readable name shown in the UI |
| `Authority` | string | — | OIDC issuer URL |
| `Audience` | string | — | OIDC client ID |

### Other

| Key | Type | Default | Description |
| --- | ---- | ------- | ----------- |
| `App:E2EMode` | bool | `false` | Bypass OIDC auth (E2E testing) |

## Encryption Design

OSWS uses **envelope encryption** for Parquet column-level encryption:

1. **Encrypt**: The parquet footer remains plaintext. For each parquet file, OSWS creates a single KEK in the vault. Each encrypted column gets its own random AES-{128,192,256} DEK, and that DEK is wrapped by the file-level KEK. A wrapped footer key is also stored in metadata because Parquet crypto still requires footer key metadata even when the footer itself is plaintext.
2. **Decrypt**: The wrapped footer key and wrapped column DEKs are read from parquet metadata. Azure Key Vault unwraps them using the referenced file-level KEK. ParquetSharp uses the recovered footer key to initialize crypto and the recovered column DEKs to decrypt data.

Raw keys never leave Azure Key Vault. Access control is enforced at the vault level, while a single file-level KEK can protect multiple column DEKs within the same parquet file.

### Key vault providers

The `IKeyVaultProvider` interface allows swapping providers:

| Provider | Config value | Use case |
| -------- | ------------ | -------- |
| **Azure Key Vault** | `"Azure"` | Production — RSA-2048 KEKs, RSA-OAEP-256 wrapping |
| **Internal (in-memory)** | `"Internal"` | Development/testing only — keys lost on restart |

## Frontend

The admin UI is a separate React application in `frontend/`. See [frontend/README.md](frontend/README.md) for setup.

```bash
cd frontend
bun install
# Edit .env with VITE_API_BASE_URL, VITE_OIDC_AUTHORITY, VITE_OIDC_CLIENT_ID
bun dev
```

## Admin API

All admin routes require OIDC authentication and the `isRbacAdmin` claim. Rate-limited under the `admin` policy.

### Users

| Method | Path | Description |
| ------ | ---- | ----------- |
| `GET` | `/api/me` | Current user profile (JIT-provisions on first login) |
| `GET` | `/api/admin/users` | List all users with their roles |

### Roles

| Method | Path | Description |
| ------ | ---- | ----------- |
| `GET` | `/api/admin/roles` | List all roles with their child roles |
| `POST` | `/api/admin/roles` | Create a role |
| `DELETE` | `/api/admin/roles/{id}` | Delete a role |
| `POST` | `/api/admin/roles/{parentId}/inherit/{childId}` | Make child inherit from parent |
| `DELETE` | `/api/admin/roles/{parentId}/inherit/{childId}` | Remove inheritance |

### Role assignments

| Method | Path | Description |
| ------ | ---- | ----------- |
| `POST` | `/api/admin/users/{userId}/roles/{roleId}` | Assign role to user |
| `DELETE` | `/api/admin/users/{userId}/roles/{roleId}` | Remove role from user |

### Column permissions

| Method | Path | Description |
| ------ | ---- | ----------- |
| `GET` | `/api/admin/columns` | List all columns with their permitted roles |
| `POST` | `/api/admin/columns/{columnId}/roles/{roleId}` | Grant column access to role |
| `DELETE` | `/api/admin/columns/{columnId}/roles/{roleId}` | Revoke column access from role |

### S3 Credentials

| Method | Path | Description |
| ------ | ---- | ----------- |
| `GET` | `/api/credentials` | List S3 credentials |
| `POST` | `/api/credentials` | Create S3 credential (returns secret key once) |
| `DELETE` | `/api/credentials/{id}` | Revoke S3 credential |

## Development Setup

### Prerequisites

- [.NET 10 SDK](https://dotnet.microsoft.com/download)
- Docker (for PostgreSQL and MinIO)
- [Bun](https://bun.sh) (for frontend and scripts)
- EF Core CLI: `dotnet tool install --global dotnet-ef`
- Azure CLI (`az`) for Azure Key Vault setup

### Azure Key Vault

```bash
az login

# Grant yourself Key Vault Crypto Officer
az role assignment create \
  --role "Key Vault Crypto Officer" \
  --assignee $(az ad signed-in-user show --query id -o tsv) \
  --scope $(az keyvault show --name <your-vault-name> --query id -o tsv)
```

### Database

```bash
# Create the database
psql -U postgres -c "CREATE DATABASE osws_dev;"

# Apply migrations
dotnet ef database update --project OSWS.KeyManager --startup-project OSWS.WebApi
```

Connection string in `appsettings.Development.json`:

```json
{
  "ConnectionStrings:OswsContext": "Host=localhost;Port=5432;Database=osws_dev;Username=postgres;Password=postgres"
}
```

### S3 Storage

Set environment variables or configure in `appsettings`:

```bash
export S3Settings__EndpointHostname=https://your-account.r2.cloudflarestorage.com
export S3Settings__AccessKeyId=your-access-key
export S3Settings__SecretAccessKey=your-secret-key
export S3Settings__Region=auto
```

### OIDC Setup

OSWS supports multiple OIDC providers simultaneously. Configure in `appsettings.{Environment}.json`:

```json
{
  "OidcProviders": [
    {
      "Name": "pocketid",
      "DisplayName": "PocketID",
      "Authority": "https://your-pocketid-host",
      "Audience": "your-client-id"
    }
  ]
}
```

#### Setting up PocketID

[PocketID](https://github.com/pocket-id/pocket-id) is a self-hosted OIDC provider:

```bash
curl -LO https://raw.githubusercontent.com/pocket-id/pocket-id/main/docker-compose.yml
curl -LO https://raw.githubusercontent.com/pocket-id/pocket-id/main/.env.example && mv .env.example .env
docker compose up -d
```

Open `http(s)://<your-app-url>/setup` to create the initial admin account.

1. Go to **Administration** → **OIDC Clients**.
2. Fill out **Name**. Set **Client Launch URL** and **Callback URLs** to `http://localhost:5173`.
3. Enable **Public Client** and **PKCE**.
4. Note the **Client ID** and your PocketID URL.
5. Create a user group with a custom claim `isRbacAdmin: true` and assign it to admin users.

The `isRbacAdmin` claim is synced to the database on every `GET /api/me` call.

### Run

```bash
dotnet run --project OSWS.WebApi
```

Health check: `GET http://localhost:5000/health`

## E2E Tests

End-to-end test suite in `e2e/`. Tests S3 API calls through encryption, column-level filtering, DuckDB, and PySpark. See [e2e/README.md](e2e/README.md).

```bash
./e2e/run.sh
```

## Benchmarks

Performance benchmarks in `OSWS.Performance.Benchmarks/`. Measures OSWS latency across encryption configurations, cache states, and file sizes. See [OSWS.Performance.Benchmarks/README.md](OSWS.Performance.Benchmarks/README.md).

## Known Issues

See [KNOWN_ISSUES.md](KNOWN_ISSUES.md).
