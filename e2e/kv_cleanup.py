"""
Clean up Azure Key Vault keys created during an E2E test run.

Reads KeyVaultId URIs from the E2E database's Keys table, extracts key names,
then deletes and purges each key from Azure KV. Runs as part of teardown in run.sh.

Requires:
  pip install azure-identity azure-keyvault-keys psycopg2-binary
  Environment: AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, E2E_KV_VAULT_URI
"""

import os
import sys

import psycopg2
from azure.identity import ClientSecretCredential
from azure.keyvault.keys import KeyClient
from azure.core.exceptions import ResourceNotFoundError

DB_CONN = "host=localhost port=5433 dbname=osws_e2e_test user=postgres password=postgres"


def extract_key_name(uri: str) -> str | None:
    """Extract key name from Azure KV key URI.

    URI format: https://vault.vault.azure.net/keys/{name}/{version}
    """
    parts = uri.rstrip("/").split("/")
    if "keys" in parts:
        idx = parts.index("keys")
        if idx + 1 < len(parts):
            return parts[idx + 1]
    return None


def get_test_key_names() -> list[str]:
    """Query the E2E database for KeyVaultId URIs and extract Azure key names."""
    try:
        with psycopg2.connect(DB_CONN) as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT DISTINCT "KeyVaultId" FROM "Keys"')
                uris = [row[0] for row in cur.fetchall()]
        names = []
        for uri in uris:
            name = extract_key_name(uri)
            if name:
                names.append(name)
        return names
    except Exception as e:
        print(f"⚠ Could not query DB for key names: {e}")
        return []


def cleanup_keys(vault_uri: str, key_names: list[str]):
    """Delete and purge keys from Azure Key Vault."""
    credential = ClientSecretCredential(
        tenant_id=os.environ["AZURE_TENANT_ID"],
        client_id=os.environ["AZURE_CLIENT_ID"],
        client_secret=os.environ["AZURE_CLIENT_SECRET"],
    )
    client = KeyClient(vault_url=vault_uri, credential=credential)

    deleted = 0
    failed = 0

    for name in key_names:
        try:
            poller = client.begin_delete_key(name)
            poller.wait()
            deleted += 1
        except ResourceNotFoundError:
            pass
        except Exception as e:
            print(f"  ⚠ Failed to delete key '{name}': {e}")
            failed += 1

    print(f"  Purging {deleted} soft-deleted keys...")
    for name in key_names:
        try:
            client.purge_deleted_key(name)
        except ResourceNotFoundError:
            pass
        except Exception as e:
            print(f"  ⚠ Could not purge key '{name}': {e}")

    print(f"  Cleanup complete: {deleted} deleted, {failed} failed out of {len(key_names)} keys")


if __name__ == "__main__":
    vault_uri = os.environ.get("E2E_KV_VAULT_URI")
    if not vault_uri:
        print("Skipping KV cleanup (E2E_KV_VAULT_URI not set)")
        sys.exit(0)

    key_names = get_test_key_names()
    if not key_names:
        print("No keys found in DB to clean up")
        sys.exit(0)

    print(f"Cleaning up {len(key_names)} keys from {vault_uri}...")
    cleanup_keys(vault_uri, key_names)
