# OSWS

Object Storage Web Service — an S3-compatible API with Parquet Modular Encryption backed by Azure Key Vault.

## Architecture

| Project | Role |
| --- | --- |
| **OSWS.WebApi** | ASP.NET minimal API host. Registers S3 endpoints, Parquet services, and the key vault provider. |
| **OSWS.Library** | S3 client infrastructure — `S3ClientFactory` dynamically creates `IAmazonS3` clients from per-request options. |
| **OSWS.Models** | Shared DTOs and EF Core entities (`User`, `Role`, `RoleAssignment`). Defines `IKeyVaultProvider` interface. |
| **OSWS.KeyManager** | EF Core DbContext (PostgreSQL) and key vault provider implementations (Azure, Internal). |
| **OSWS.ParquetSolver** | Parquet Modular Encryption via ParquetSharp. Uses envelope encryption through `IKeyVaultProvider`. |
| **OSWS.Common** | Shared configuration models. |

## Encryption design

OSWS uses **envelope encryption** for Parquet column-level encryption:

1. **Encrypt**: A random AES-128 data encryption key (DEK) is generated locally. The DEK is wrapped (encrypted) by a key encryption key (KEK) stored in Azure Key Vault. The wrapped DEK is stored in the parquet footer metadata. ParquetSharp encrypts columns with the raw DEK.
2. **Decrypt**: The wrapped DEK is read from parquet footer metadata. Azure Key Vault unwraps it. ParquetSharp decrypts columns with the recovered DEK.

Raw keys never leave Azure Key Vault. Each role gets its own KEK, so access control is enforced at the vault level.

### Key vault providers

The `IKeyVaultProvider` interface allows swapping providers:

| Provider | Config value | Use case |
| --- | --- | --- |
| **Azure Key Vault** | `"Azure"` | Production — RSA-2048 KEKs, RSA-OAEP-256 wrapping |
| **Internal (in-memory)** | `"Internal"` | Development/testing only — keys lost on restart |

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
