"""
E2E test: DuckDB querying parquet files through OSWS's S3 interface.

Demonstrates that a real analytics engine can query OSWS directly,
seeing only the columns the user is permitted to access.

Requires: the environment from test_permissions.py to already be running
(users seeded, roles assigned, credentials seeded, file uploaded).
"""

import sys

import duckdb
import pyarrow.parquet as pq

from e2e.seed import (
    ANALYST_ACCESS_KEY,
    ANALYST_SECRET_KEY,
    JUNIOR_ACCESS_KEY,
    JUNIOR_SECRET_KEY,
    INTERN_ACCESS_KEY,
    INTERN_SECRET_KEY,
)

OSWS_S3_ENDPOINT = "localhost:5000"
BUCKET = "e2e-test-bucket"
KEY = "titanic.parquet"
S3_PATH = f"s3://{BUCKET}/{KEY}"
SAMPLE_FILE = __file__.replace("test_duckdb.py", "../samples/titanic.parquet")

JUNIOR_ALLOWED_COLUMNS = {"Age"}
INTERN_ALLOWED_COLUMNS = {"PassengerId", "Survived", "Pclass", "Sex"}


def make_duckdb_connection(access_key, secret_key):
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""
        CREATE SECRET s3_secret (
            TYPE S3,
            KEY_ID '{access_key}',
            SECRET '{secret_key}',
            REGION 'us-east-1',
            ENDPOINT '{OSWS_S3_ENDPOINT}',
            URL_STYLE 'path',
            USE_SSL false
        )
    """)
    return con


def test_analyst_duckdb():
    """Analyst should see all columns with real data via DuckDB."""
    con = make_duckdb_connection(ANALYST_ACCESS_KEY, ANALYST_SECRET_KEY)
    df = con.execute(f"SELECT * FROM '{S3_PATH}'").fetchdf()
    print(df)
    original = pq.read_table(SAMPLE_FILE).to_pandas()

    assert list(df.columns) == list(original.columns), (
        f"Column mismatch: {list(df.columns)} vs {list(original.columns)}"
    )
    assert len(df) == len(original), f"Row count mismatch: {len(df)} vs {len(original)}"
    for col in original.columns:
        assert df[col].equals(original[col]), (
            f"Analyst: column '{col}' data mismatch via DuckDB"
        )
    print("✓ DuckDB: Analyst sees all columns correctly")


def test_junior_duckdb():
    """Junior should see dummy values for forbidden columns via DuckDB."""
    con = make_duckdb_connection(JUNIOR_ACCESS_KEY, JUNIOR_SECRET_KEY)
    df = con.execute(f"SELECT * FROM '{S3_PATH}'").fetchdf()
    print(df)
    original = pq.read_table(SAMPLE_FILE).to_pandas()

    assert len(df) == len(original), f"Row count mismatch: {len(df)} vs {len(original)}"
    for col in df.columns:
        if col in JUNIOR_ALLOWED_COLUMNS or col in INTERN_ALLOWED_COLUMNS:
            assert df[col].equals(original[col]), (
                f"Intern: allowed column '{col}' should have real data via DuckDB"
            )
        else:
            assert not df[col].equals(original[col]), (
                f"Intern: forbidden column '{col}' should have dummy data via DuckDB, "
                f"but got original values"
            )
    print("✓ DuckDB: Intern sees only allowed columns correctly")


def test_intern_duckdb():
    """Intern should see dummy values for forbidden columns via DuckDB."""
    con = make_duckdb_connection(INTERN_ACCESS_KEY, INTERN_SECRET_KEY)
    df = con.execute(f"SELECT * FROM '{S3_PATH}'").fetchdf()
    print(df)
    original = pq.read_table(SAMPLE_FILE).to_pandas()

    assert len(df) == len(original), f"Row count mismatch: {len(df)} vs {len(original)}"
    for col in df.columns:
        if col in INTERN_ALLOWED_COLUMNS:
            assert df[col].equals(original[col]), (
                f"Intern: allowed column '{col}' should have real data via DuckDB"
            )
        else:
            assert not df[col].equals(original[col]), (
                f"Intern: forbidden column '{col}' should have dummy data via DuckDB, "
                f"but got original values"
            )
    print("✓ DuckDB: Intern sees only allowed columns correctly")


def main():
    print("DuckDB integration test (reuses existing E2E environment)...")
    test_analyst_duckdb()
    test_junior_duckdb()
    test_intern_duckdb()
    print("\n✅ DuckDB E2E tests passed")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ DuckDB E2E test failed: {e}", file=sys.stderr)
        sys.exit(1)
