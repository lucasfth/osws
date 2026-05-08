#!/usr/bin/env python3
"""
OSWS Benchmark Runner
=====================
Measures PUT and GET latency for a given configuration.

Usage:
    python run-benchmark.py --config <name> [--bucket <name>] [--repetitions <n>]

Configs:
    s3-direct:             PUT/GET directly to R2, no OSWS
    osws-encrypt-cache:    OSWS with encryption + file cache enabled
    osws-encrypt-no-cache: OSWS with encryption, file cache disabled
    osws-no-encrypt:       OSWS with encryption disabled

Before running:
    1. Copy .env.example to .env and fill in credentials
    2. Run: dotnet run -- seed-s3-credential --user-name bench-user --role-name bench-role
       Copy the output ACCESS_KEY and SECRET_KEY to .env (BENCH_OSWS_ACCESS_KEY / BENCH_OSWS_SECRET_KEY)
    3. Run: dotnet run -- generate-corpus
    4. For osws-* configs: start OSWS with correct env vars, then run this script - or use run-suite.py to run all configs in sequence.

Output:
    <results-dir>/results_<config>_<timestamp>.csv
"""

import argparse
import csv
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).parent
BENCHMARK_DIR = SCRIPT_DIR.parent
RESULTS_DIR = BENCHMARK_DIR / "benchmark-results"
DATASET_DIR = BENCHMARK_DIR / "benchmark-datasets"

load_dotenv(BENCHMARK_DIR / ".env")

FILE_SIZES = ["tiny"] #"small", "medium"]  # large and xlarge currently disabled

# Row counts and column count match C# ParquetGenerator / DecryptionBenchmark
CONFIGS = {
    "s3-direct": "s3-direct",
    "osws-encrypt-cache": "osws",
    "osws-encrypt-no-cache": "osws",
    "osws-no-encrypt": "osws",
}


def make_s3_client(endpoint: str, access_key: str, secret_key: str) -> boto3.client:
    cfg = Config(
        retries={"max_attempts": 1},
        connect_timeout=30,
        read_timeout=300,
    )
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=cfg,
    )


def check_osws_health(endpoint: str) -> bool:
    import urllib.request

    try:
        with urllib.request.urlopen(
            f"{endpoint.rstrip('/')}/health", timeout=5
        ) as resp:
            return resp.status == 200
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Timing helpers
# ---------------------------------------------------------------------------


def time_put(s3, bucket: str, key: str, data: bytes) -> float:
    """Upload bytes and return wall-clock duration in milliseconds."""
    t0 = time.perf_counter()
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=data,
        ContentType="application/vnd.apache.parquet",
    )
    return (time.perf_counter() - t0) * 1000


def time_get(s3, bucket: str, key: str) -> float:
    """Download a file to /dev/null and return wall-clock duration in milliseconds."""
    t0 = time.perf_counter()
    resp = s3.get_object(Bucket=bucket, Key=key)
    # Consume the body fully to measure total transfer time
    resp["Body"].read()
    return (time.perf_counter() - t0) * 1000


# ---------------------------------------------------------------------------
# CSV output
# ---------------------------------------------------------------------------


class ResultsWriter:
    FIELDS = [
        "config",
        "operation",
        "cache_state",
        "file_size_label",
        "run_index",
        "duration_ms",
        "timestamp_utc",
    ]

    def __init__(self, config: str, results_dir: Path | None = None):
        if results_dir is not None:
            results_dir.mkdir(parents=True, exist_ok=True)
            self.path = results_dir / f"{config}.csv"
        else:
            RESULTS_DIR.mkdir(exist_ok=True)
            ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            self.path = RESULTS_DIR / f"results_{config}_{ts}.csv"
        self._f = open(self.path, "w", newline="")
        self._w = csv.DictWriter(self._f, fieldnames=self.FIELDS)
        self._w.writeheader()
        print(f"  Writing results to: {self.path}")

    def write(self, **kwargs):
        kwargs.setdefault("timestamp_utc", datetime.now(timezone.utc).isoformat())
        self._w.writerow(kwargs)
        self._f.flush()

    def close(self):
        self._f.close()


# ---------------------------------------------------------------------------
# Benchmark phases
# ---------------------------------------------------------------------------


def run_put_benchmark(
    s3,
    bucket: str,
    config: str,
    size_label: str,
    data: bytes,
    repetitions: int,
    writer: ResultsWriter,
):
    """PUT the same data N times. Each PUT is a fresh upload."""
    print(f"    PUT x{repetitions}... ", end="", flush=True)
    key_prefix = f"bench/put-bench/{config}/{size_label}"

    for i in range(1, repetitions + 1):
        key = f"{key_prefix}/{i:03d}.parquet"
        duration_ms = time_put(s3, bucket, key, data)
        writer.write(
            config=config,
            operation="PUT",
            cache_state="n/a",
            file_size_label=size_label,
            run_index=i,
            duration_ms=f"{duration_ms:.2f}",
        )
        print(f"{duration_ms:.0f}ms ", end="", flush=True)

    print()


def run_get_benchmark(
    s3,
    bucket: str,
    config: str,
    size_label: str,
    repetitions: int,
    writer: ResultsWriter,
    is_s3_direct: bool,
):
    """
    GET benchmark with cold and warm runs.

    Cold: GET each of the N pre-uploaded copies once (distinct DEKs for OSWS).
          For s3-direct there is no meaningful cold/warm distinction. We still
          run N repetitions but label them 'warm' since there's no DEK cache.

    Warm: GET the same file N+1 times; discard first (cache fill); record N.
    """
    if is_s3_direct:
        # s3-direct: no cache concept, just N repetitions of the same GET
        key = f"bench/s3-direct/{size_label}.parquet"
        print(f"    GET s3-direct x{repetitions}... ", end="", flush=True)
        for i in range(1, repetitions + 1):
            duration_ms = time_get(s3, bucket, key)
            writer.write(
                config=config,
                operation="GET",
                cache_state="warm",
                file_size_label=size_label,
                run_index=i,
                duration_ms=f"{duration_ms:.2f}",
            )
            print(f"{duration_ms:.0f}ms ", end="", flush=True)
        print()
        return

    # Cold GETs: each copy has a distinct DEK → genuine cold AKV unwrap per request
    print(f"    GET cold x{repetitions}... ", end="", flush=True)
    for i in range(1, repetitions + 1):
        key = f"bench/osws/cold/{size_label}/{i:03d}.parquet"
        duration_ms = time_get(s3, bucket, key)
        writer.write(
            config=config,
            operation="GET",
            cache_state="cold",
            file_size_label=size_label,
            run_index=i,
            duration_ms=f"{duration_ms:.2f}",
        )
        print(f"{duration_ms:.0f}ms ", end="", flush=True)
    print()

    # Warm GETs: hit the same file N+1 times; discard first
    warm_key = f"bench/osws/warm/{size_label}.parquet"
    print(f"    GET warm x{repetitions} (1 throwaway first)... ", end="", flush=True)
    _ = time_get(s3, bucket, warm_key)  # throwaway; warms DEK + file cache
    print("(warmed) ", end="", flush=True)
    for i in range(1, repetitions + 1):
        duration_ms = time_get(s3, bucket, warm_key)
        writer.write(
            config=config,
            operation="GET",
            cache_state="warm",
            file_size_label=size_label,
            run_index=i,
            duration_ms=f"{duration_ms:.2f}",
        )
        print(f"{duration_ms:.0f}ms ", end="", flush=True)
    print()


def load_put_data(size_label: str) -> bytes:
    """Read the pre-generated parquet file produced by 'dotnet run -- generate-corpus'."""
    path = DATASET_DIR / f"{size_label}.parquet"
    if not path.exists():
        print(
            f"\nERROR: Dataset file not found: {path}\n"
            "Run first:  dotnet run -- generate-corpus",
            file=sys.stderr,
        )
        sys.exit(1)
    return path.read_bytes()


def main():
    parser = argparse.ArgumentParser(description="OSWS Benchmark Runner")
    parser.add_argument(
        "--config",
        required=True,
        choices=list(CONFIGS.keys()),
        help="Benchmark configuration to run",
    )
    parser.add_argument("--bucket", help="S3 bucket name (overrides .env)")
    parser.add_argument(
        "--repetitions",
        type=int,
        help="Number of repetitions per measurement (overrides .env)",
    )
    parser.add_argument(
        "--skip-put", action="store_true", help="Skip PUT benchmark, only run GET"
    )
    parser.add_argument(
        "--skip-sizes",
        nargs="+",
        choices=FILE_SIZES,
        default=[],
        help="Skip specific file sizes",
    )
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=None,
        help="Directory for CSV output (default: benchmark-results/results_<config>_<ts>.csv)",
    )
    args = parser.parse_args()

    bucket = args.bucket or os.getenv("BENCH_BUCKET") or "osws-benchmark"
    repetitions = args.repetitions or int(os.getenv("BENCH_REPETITIONS") or "10")
    config = args.config
    is_s3_direct = config == "s3-direct"

    # Choose endpoint + credentials based on config
    if is_s3_direct:
        endpoint = os.getenv("S3Settings__EndpointHostname")
        access_key = os.getenv("S3Settings__AccessKeyId")
        secret_key = os.getenv("S3Settings__SecretAccessKey")
        if not all([endpoint, access_key, secret_key]):
            print("ERROR: S3Settings__* credentials not found in .env", file=sys.stderr)
            sys.exit(1)
    else:
        endpoint = os.getenv("OSWS_ENDPOINT") or "http://localhost:5000"
        access_key = os.getenv("BENCH_OSWS_ACCESS_KEY")
        secret_key = os.getenv("BENCH_OSWS_SECRET_KEY")
        if not all([access_key, secret_key]):
            print(
                "ERROR: BENCH_OSWS_ACCESS_KEY / BENCH_OSWS_SECRET_KEY not found in .env\n"
                "Run: dotnet run -- seed-s3-credential --user-name bench-user --role-name bench-role",
                file=sys.stderr,
            )
            sys.exit(1)

        print(f"Checking OSWS health at {endpoint}... ", end="", flush=True)
        if not check_osws_health(endpoint):
            print("FAILED")
            print(
                f"ERROR: OSWS is not responding at {endpoint}.\n"
                f"Start OSWS with the correct configuration for '{config}' before running.",
                file=sys.stderr,
            )
            sys.exit(1)
        print("OK")

    s3 = make_s3_client(endpoint, access_key, secret_key)

    print("Starting OSWS PUT/GET Benchmark")
    print(f"  Config      : {config}")
    print(f"  Endpoint    : {endpoint}")
    print(f"  Bucket      : {bucket}")
    print(f"  Repetitions : {repetitions}")
    print(
        f"  File sizes  : {', '.join(s for s in FILE_SIZES if s not in args.skip_sizes)}"
    )
    print()

    writer = ResultsWriter(config, results_dir=args.results_dir)

    try:
        for size_label in FILE_SIZES:
            if size_label in args.skip_sizes:
                continue

            print(f"{size_label}:")

            if not args.skip_put:
                print(f"    Loading {size_label} dataset... ", end="", flush=True)
                data = load_put_data(size_label)
                print(f"{len(data) / 1024 / 1024:.1f} MB")
                run_put_benchmark(
                    s3, bucket, config, size_label, data, repetitions, writer
                )

            run_get_benchmark(
                s3, bucket, config, size_label, repetitions, writer, is_s3_direct
            )
            print()

    finally:
        writer.close()

    print("Finished benchmarking")
    print(f"  Results: {writer.path}")
    print()
    print("Analyse with:")
    print(f"  python {SCRIPT_DIR}/analyse-results.py {RESULTS_DIR}/")


if __name__ == "__main__":
    main()
