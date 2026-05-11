# OSWS.ParquetSolver

Parquet Modular Encryption wrapper using [ParquetSharp](https://github.com/G-Research/ParquetSharp).

Handles encrypting and decrypting individual columns in Parquet files for OSWS. Each column gets its own random DEK (AES-128/192/256), which is wrapped by a file-level KEK stored in the key vault.

## How it fits

Used by `OSWS.WebApi` during S3 PUT (encrypt columns visible to the uploader's role) and S3 GET (decrypt columns the requester's role has access to).

## Dependencies

- ParquetSharp (.NET bindings for Apache Parquet C++)
- `IKeyVaultProvider` — key wrapping/unwrapping

## Encryption flow

1. On PUT, the service generates a file-level KEK in the vault, creates one DEK per column, wraps each DEK with the KEK, encrypts columns using AES-CTR, and stores wrapped keys in Parquet file metadata.
2. On GET, the wrapped DEKs are read from the Parquet metadata, unwrapped via the vault, and used to decrypt columns the requesting role has access to. Columns the role lacks access to remain encrypted and are discarded.

## Configuration

Encryption behavior is controlled by the `Encryption` section in `appsettings.json` (see root [README.md](../README.md#encryption)).

## Inspecting Parquet files

```bash
# View first row
parquet head -n 1 file.parquet

# View schema (works on encrypted files if footer is plaintext)
parquet schema file.parquet
```
