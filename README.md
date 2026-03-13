# OSWS

Object Storage Web Service - an S3-compatible API with Parquet Modular Encryption backed by Azure Key Vault.

## Architecture

| Project | Role |
| --- | --- |
| **OSWS.WebApi** | ASP.NET minimal API host. Registers S3 endpoints, Parquet services, and the key vault provider. |
| **OSWS.Library** | S3 client infrastructure - `S3ClientFactory` dynamically creates `IAmazonS3` clients from per-request options. |
| **OSWS.Models** | Shared DTOs and EF Core entities (`User`, `Role`, `RoleAssignment`). Defines `IKeyVaultProvider` interface. |
| **OSWS.KeyManager** | EF Core DbContext (PostgreSQL) and key vault provider implementations (Azure, Internal). |
| **OSWS.ParquetSolver** | Parquet Modular Encryption via ParquetSharp. Uses envelope encryption through `IKeyVaultProvider`. |
| **OSWS.Common** | Shared configuration models. |

## Encryption design

OSWS uses **envelope encryption** for Parquet column-level encryption:

1. **Encrypt**: The parquet footer remains plaintext. For each parquet file, OSWS creates a single KEK in the vault. Each encrypted column gets its own random AES-128 DEK, and that DEK is wrapped by the file-level KEK. A wrapped footer key is also stored in metadata because Parquet crypto still requires footer key metadata even when the footer itself is plaintext.
2. **Decrypt**: The wrapped footer key and wrapped column DEKs are read from parquet metadata. Azure Key Vault unwraps them using the referenced file-level KEK. ParquetSharp uses the recovered footer key to initialize crypto and the recovered column DEKs to decrypt data.

Raw keys never leave Azure Key Vault. Access control is enforced at the vault level, while a single file-level KEK can protect multiple column DEKs within the same parquet file.

### Key vault providers

The `IKeyVaultProvider` interface allows swapping providers:

| Provider | Config value | Use case |
| --- | --- | --- |
| **Azure Key Vault** | `"Azure"` | Production - RSA-2048 KEKs, RSA-OAEP-256 wrapping |
| **Internal (in-memory)** | `"Internal"` | Development/testing only - keys lost on restart |

## Prerequisites

- [.NET 10 SDK](https://dotnet.microsoft.com/download)
- PostgreSQL (for user/role management)
- Azure CLI (`az`) for Azure Key Vault setup
- An Azure Key Vault with RBAC enabled

## Azure Key Vault

```bash
# Login to Azure
az login

# Grant yourself Key Vault Crypto Officer (covers create, get, list, wrapKey, unwrapKey)
az role assignment create \
  --role "Key Vault Crypto Officer" \
  --assignee $(az ad signed-in-user show --query id -o tsv) \
  --scope $(az keyvault show --name <your-vault-name> --query id -o tsv)
```

### Configuration

Edit `OSWS.WebApi/appsettings.json`:

```json
{
  "KeyVault": {
    "Provider": "Azure",
    "VaultUri": "https://<your-vault-name>.vault.azure.net/"
  }
}
```

Authentication uses `DefaultAzureCredential` which picks up (in order):

- Environment variables (`AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`)
- Azure managed identity
- Azure CLI (`az login`)
- Visual Studio / VS Code credentials

For local development, `az login` is sufficient.

### R2 / S3 storage

Set these environment variables for Cloudflare R2 (or any S3-compatible store):

```bash
export R2_ENDPOINT=https://your-account.r2.cloudflarestorage.com
export R2_ACCESS_KEY_ID=your-access-key
export R2_SECRET_ACCESS_KEY=your-secret-key
export R2_REGION=auto
```

### Run

```bash
dotnet run --project OSWS.WebApi
```

Health check: `GET http://localhost:5000/health`
