"""
E2E test: column-level permission filtering through OSWS.

Phase 1: Seed 4 users in PostgreSQL (admin, analyst, junior, intern)
Phase 2: Configure roles and assignments via admin API (X-E2E-User-Id header)
Phase 3: Seed S3 credentials (now that role IDs are known)
Phase 4: Create bucket via boto3 → OSWS
Phase 5: Upload titanic.parquet as analyst via boto3 → OSWS
Phase 6: Grant intern permissions for allowed columns via admin API
Phase 7: Assert analyst sees all columns; intern sees only allowed columns
"""

import io
import sys
import os

import requests
import boto3
import pyarrow.parquet as pq
from botocore.config import Config

from e2e.seed import (
    seed_users,
    seed_credentials,
    ANALYST_ACCESS_KEY,
    ANALYST_SECRET_KEY,
    JUNIOR_ACCESS_KEY,
    JUNIOR_SECRET_KEY,
    INTERN_ACCESS_KEY,
    INTERN_SECRET_KEY,
)

OSWS_URL = "http://localhost:5000"
BUCKET = "e2e-test-bucket"
KEY = "titanic.parquet"

# Columns the intern is allowed to see (everything else gets dummy values)
JUNIOR_ALLOWED_COLUMNS = {"Age"}
INTERN_ALLOWED_COLUMNS = {"PassengerId", "Survived", "Pclass", "Sex"}

# Path to sample file — resolve relative to repo root
SAMPLE_FILE = os.path.join(
    os.path.dirname(__file__), "..", "samples", "titanic.parquet"
)


def make_s3_client(access_key, secret_key):
    return boto3.client(
        "s3",
        endpoint_url=OSWS_URL,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )


# ── Admin API helpers ──────────────────────────────────────────────────────────


def admin(method, path, admin_id, **kwargs):
    """Call an admin API endpoint with the E2E auth header."""
    resp = requests.request(
        method,
        f"{OSWS_URL}/api/admin{path}",
        headers={"X-E2E-User-Id": str(admin_id)},
        **kwargs,
    )
    assert resp.status_code < 300, f"{method} {path} → {resp.status_code}: {resp.text}"
    return resp.json() if resp.content else None


def setup_roles_and_assignments(admin_id, analyst_id, junior_id, intern_id):
    """Create roles and assign them to users via admin API."""
    analyst_role = admin("POST", "/roles", admin_id, json={"name": "analyst"})
    junior_role = admin("POST", "/roles", admin_id, json={"name": "junior"})
    intern_role = admin("POST", "/roles", admin_id, json={"name": "intern"})
    admin("POST", f"/users/{analyst_id}/roles/{analyst_role['id']}", admin_id)
    admin("POST", f"/users/{junior_id}/roles/{junior_role['id']}", admin_id)
    admin("POST", f"/users/{intern_id}/roles/{intern_role['id']}", admin_id)
    return analyst_role["id"], junior_role["id"], intern_role["id"]


def grant_junior_column_permissions(admin_id, junior_role_id):
    """Grant the junior role access to only the allowed columns."""
    columns = admin("GET", "/columns", admin_id)
    granted = 0
    for col in columns:
        if col["name"] in JUNIOR_ALLOWED_COLUMNS:
            admin("POST", f"/columns/{col['id']}/roles/{junior_role_id}", admin_id)
            granted += 1
    assert granted == len(JUNIOR_ALLOWED_COLUMNS), (
        f"Expected to grant {len(JUNIOR_ALLOWED_COLUMNS)} columns, but found {granted}"
    )


def grant_intern_to_junior(admin_id, intern_role_id, junior_role_id):
    """Grant the junior role all permissions of the intern role."""
    admin("POST", f"/roles/{junior_role_id}/inherit/{intern_role_id}", admin_id)


def grant_intern_column_permissions(admin_id, intern_role_id):
    """Grant the intern role access to only the allowed columns."""
    columns = admin("GET", "/columns", admin_id)
    granted = 0
    for col in columns:
        if col["name"] in INTERN_ALLOWED_COLUMNS:
            admin("POST", f"/columns/{col['id']}/roles/{intern_role_id}", admin_id)
            granted += 1
    assert granted == len(INTERN_ALLOWED_COLUMNS), (
        f"Expected to grant {len(INTERN_ALLOWED_COLUMNS)} columns, but found {granted}"
    )


# ── S3 helpers ─────────────────────────────────────────────────────────────────


def create_bucket():
    s3 = make_s3_client(ANALYST_ACCESS_KEY, ANALYST_SECRET_KEY)
    s3.create_bucket(Bucket=BUCKET)
    print(f"  Created bucket: {BUCKET}")


def upload_file():
    s3 = make_s3_client(ANALYST_ACCESS_KEY, ANALYST_SECRET_KEY)
    s3.upload_file(SAMPLE_FILE, BUCKET, KEY)
    print(f"  Uploaded: {KEY}")


def download_file(access_key, secret_key):
    s3 = make_s3_client(access_key, secret_key)
    buf = io.BytesIO()
    s3.download_fileobj(BUCKET, KEY, buf)
    buf.seek(0)
    return pq.read_table(buf)


# ── Assertions ─────────────────────────────────────────────────────────────────


def test_analyst_sees_all_columns():
    """Analyst uploaded the file and has full access — all columns should match original."""
    table = download_file(ANALYST_ACCESS_KEY, ANALYST_SECRET_KEY)
    original = pq.read_table(SAMPLE_FILE)
    for col in original.column_names:
        assert col in table.column_names, f"Analyst: missing column '{col}'"
        assert table.column(col).to_pylist() == original.column(col).to_pylist(), (
            f"Analyst: column '{col}' data mismatch"
        )
    print("✓ Analyst sees all columns correctly")


def test_junior_sees_only_allowed_columns_and_intern_columns():
    """Junior has restricted access — forbidden columns should have dummy values."""
    table = download_file(JUNIOR_ACCESS_KEY, JUNIOR_SECRET_KEY)
    original = pq.read_table(SAMPLE_FILE)
    for col in table.column_names:
        original_values = original.column(col).to_pylist()
        actual_values = table.column(col).to_pylist()
        if col in INTERN_ALLOWED_COLUMNS or col in JUNIOR_ALLOWED_COLUMNS:
            assert actual_values == original_values, (
                f"Junior: allowed column '{col}' should have real data"
            )
        else:
            assert actual_values != original_values, (
                f"Junior: forbidden column '{col}' should have dummy data, but got original values"
            )
    print("✓ Junior sees only allowed columns correctly")


def test_intern_sees_only_allowed_columns():
    """Intern has restricted access — forbidden columns should have dummy values."""
    table = download_file(INTERN_ACCESS_KEY, INTERN_SECRET_KEY)
    original = pq.read_table(SAMPLE_FILE)
    for col in table.column_names:
        original_values = original.column(col).to_pylist()
        actual_values = table.column(col).to_pylist()
        if col in INTERN_ALLOWED_COLUMNS:
            assert actual_values == original_values, (
                f"Intern: allowed column '{col}' should have real data"
            )
        else:
            assert actual_values != original_values, (
                f"Intern: forbidden column '{col}' should have dummy data, but got original values"
            )
    print("✓ Intern sees only allowed columns correctly")


# ── Main ───────────────────────────────────────────────────────────────────────


def main():
    print("Phase 1: Seeding users...")
    admin_id, analyst_id, junior_id, intern_id = seed_users()
    print(
        f"  admin={admin_id}, analyst={analyst_id}, junior={junior_id}, intern={intern_id}"
    )

    print("Phase 2: Configuring roles via admin API...")
    analyst_role_id, junior_role_id, intern_role_id = setup_roles_and_assignments(
        admin_id, analyst_id, junior_id, intern_id
    )
    print(
        f"  analyst_role={analyst_role_id}, junior_role={junior_role_id}, intern_role={intern_role_id}"
    )

    print("Phase 3: Seeding S3 credentials...")
    seed_credentials(
        analyst_id,
        analyst_role_id,
        junior_id,
        junior_role_id,
        intern_id,
        intern_role_id,
    )
    print("  Done")

    print("Phase 4: Creating bucket...")
    create_bucket()

    print("Phase 5: Uploading parquet file as analyst...")
    upload_file()

    print("Phase 6: Granting junior column permissions...")
    grant_junior_column_permissions(admin_id, junior_role_id)

    print("Phase 7: Granting intern role to junior role...")
    grant_intern_to_junior(admin_id, intern_role_id, junior_role_id)

    print("Phase 8: Granting intern column permissions...")
    grant_intern_column_permissions(admin_id, intern_role_id)

    print("Phase 9: Running assertions...")
    test_analyst_sees_all_columns()
    test_junior_sees_only_allowed_columns_and_intern_columns()
    test_intern_sees_only_allowed_columns()

    print("\n✅ All E2E tests passed")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ E2E test failed: {e}", file=sys.stderr)
        sys.exit(1)
