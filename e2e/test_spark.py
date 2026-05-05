"""
E2E test: Apache Spark querying parquet files through OSWS's S3 interface.

Demonstrates that Spark (via Hadoop S3A) can query OSWS directly,
seeing only the columns the user is permitted to access.

Requires: the environment from test_permissions.py to already be running
(users seeded, roles assigned, credentials seeded, file uploaded).
Requires: Java 17+ and pyspark installed.
"""

import os
import sys

import pyarrow.parquet as pq
from pyspark.sql import SparkSession

from e2e.seed import (
    ANALYST_ACCESS_KEY,
    ANALYST_SECRET_KEY,
    JUNIOR_ACCESS_KEY,
    JUNIOR_SECRET_KEY,
    INTERN_ACCESS_KEY,
    INTERN_SECRET_KEY,
)

OSWS_S3_ENDPOINT = "http://localhost:5000"
BUCKET = "e2e-test-bucket"
KEY = "titanic.parquet"
S3A_PATH = f"s3a://{BUCKET}/{KEY}"
SAMPLE_FILE = os.path.join(
    os.path.dirname(__file__), "..", "samples", "titanic.parquet"
)

JUNIOR_ALLOWED_COLUMNS = {"Age"}
INTERN_ALLOWED_COLUMNS = {"PassengerId", "Survived", "Pclass", "Sex"}


_spark = None


def make_spark_session(access_key, secret_key, app_name="osws-e2e"):
    global _spark
    if _spark is None:
        _spark = (
            SparkSession.builder.appName(app_name)
            .master("local[*]")
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.1")
            .config("spark.hadoop.fs.s3a.endpoint", OSWS_S3_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", access_key)
            .config("spark.hadoop.fs.s3a.secret.key", secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config(
                "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
            )
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
            .config("spark.ui.enabled", "false")
            .config("spark.driver.memory", "512m")
            .getOrCreate()
        )
    else:
        # Reconfigure credentials and clear the cached S3A filesystem
        hconf = _spark.sparkContext._jsc.hadoopConfiguration()
        hconf.set("fs.s3a.access.key", access_key)
        hconf.set("fs.s3a.secret.key", secret_key)
        jvm = _spark.sparkContext._jvm
        jvm.org.apache.hadoop.fs.FileSystem.closeAll()

    return _spark


def test_analyst_spark():
    """Analyst should see all columns with real data via Spark."""
    spark = make_spark_session(ANALYST_ACCESS_KEY, ANALYST_SECRET_KEY, "analyst")
    df = spark.read.parquet(S3A_PATH).toPandas()
    print(df)

    original = pq.read_table(SAMPLE_FILE).to_pandas()

    assert list(df.columns) == list(original.columns), (
        f"Column mismatch: {list(df.columns)} vs {list(original.columns)}"
    )
    assert len(df) == len(original), f"Row count mismatch: {len(df)} vs {len(original)}"
    for col in original.columns:
        assert df[col].equals(original[col]), (
            f"Analyst: column '{col}' data mismatch via Spark"
        )
    print("✓ Spark: Analyst sees all columns correctly")


def test_junior_spark():
    """Junior should see dummy values for forbidden columns via Spark."""
    spark = make_spark_session(JUNIOR_ACCESS_KEY, JUNIOR_SECRET_KEY, "junior")
    df = spark.read.parquet(S3A_PATH).toPandas()
    print(df)

    original = pq.read_table(SAMPLE_FILE).to_pandas()

    assert len(df) == len(original), f"Row count mismatch: {len(df)} vs {len(original)}"
    for col in df.columns:
        if col in JUNIOR_ALLOWED_COLUMNS or col in INTERN_ALLOWED_COLUMNS:
            assert df[col].equals(original[col]), (
                f"Junior: allowed column '{col}' should have real data via Spark"
            )
        else:
            assert not df[col].equals(original[col]), (
                f"Junior: forbidden column '{col}' should have dummy data via Spark, "
                f"but got original values"
            )
    print("✓ Spark: Junior sees only allowed columns correctly")


def test_intern_spark():
    """Intern should see dummy values for forbidden columns via Spark."""
    spark = make_spark_session(INTERN_ACCESS_KEY, INTERN_SECRET_KEY, "intern")
    df = spark.read.parquet(S3A_PATH).toPandas()
    print(df)

    original = pq.read_table(SAMPLE_FILE).to_pandas()

    assert len(df) == len(original), f"Row count mismatch: {len(df)} vs {len(original)}"
    for col in df.columns:
        if col in INTERN_ALLOWED_COLUMNS:
            assert df[col].equals(original[col]), (
                f"Intern: allowed column '{col}' should have real data via Spark"
            )
        else:
            assert not df[col].equals(original[col]), (
                f"Intern: forbidden column '{col}' should have dummy data via Spark, "
                f"but got original values"
            )
    print("✓ Spark: Intern sees only allowed columns correctly")


def main():
    print("Spark integration test (reuses existing E2E environment)...")
    try:
        test_analyst_spark()
        test_junior_spark()
        test_intern_spark()
    finally:
        if _spark is not None:
            _spark.stop()
    print("\n✅ Spark E2E tests passed")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ Spark E2E test failed: {e}", file=sys.stderr)
        sys.exit(1)
