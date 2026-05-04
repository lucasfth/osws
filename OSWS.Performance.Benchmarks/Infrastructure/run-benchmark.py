#!/usr/bin/env python3
"""
OSWS Benchmark Runner
=====================
Measures PUT and GET latency for a given configuration.

Usage:
    python run-benchmark.py --config <name> [--bucket <name>] [--repetitions <n>]

Configs:
    s3-direct           — PUT/GET directly to R2, no OSWS
    osws-encrypt-cache  — OSWS with encryption + file cache enabled
    osws-encrypt-no-cache — OSWS with encryption, file cache disabled
    osws-no-encrypt     — OSWS with encryption disabled

Before running:
    1. Copy .env.example to .env and fill in credentials
    2. Run: dotnet run -- seed-s3-credential --user-name bench-user --role-name bench-role
       Copy the output ACCESS_KEY and SECRET_KEY to .env (BENCH_OSWS_ACCESS_KEY / BENCH_OSWS_SECRET_KEY)
    3. Run: dotnet run -- generate-corpus
    4. For osws-* configs: start OSWS with correct env vars, then run this script

Output:
    benchmark-results/results_<config>_<timestamp>.csv
"""

import argparse
import csv
import io
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.config import Config

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).parent
BENCHMARK_DIR = SCRIPT_DIR.parent
ENV_FILE = BENCHMARK_DIR / ".env"
RESULTS_DIR = BENCHMARK_DIR / "benchmark-results"

FILE_SIZES = ["small", "medium", "large"]

# Row counts and column count match C# ParquetGenerator / DecryptionBenchmark
# (2,000 columns × rows doubles = ~80 MB / ~160 MB / ~1,600 MB uncompressed)
ROW_COUNTS = {
    "small": 5_000,
    "medium": 10_000,
    "large": 100_000,
}
COLUMNS = 2_000
RNG_SEED = 42

CONFIGS = {
    "s3-direct": "s3-direct",
    "osws-encrypt-cache": "osws",
    "osws-encrypt-no-cache": "osws",
    "osws-no-encrypt": "osws",
}


def load_env(path: Path) -> dict:
    """Load .env file into a dict, overriding existing env vars."""
    env = {}
    if not path.exists():
        return env
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def get_cfg(env: dict, key: str, default: str | None = None) -> str | None:
    return env.get(key) or os.environ.get(key) or default


# ---------------------------------------------------------------------------
# S3 client factory
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------


def check_osws_health(endpoint: str) -> bool:
    import urllib.request
    import urllib.error

    try:
        with urllib.request.urlopen(f"{endpoint.rstrip('/')}/health", timeout=5) as resp:
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

    def __init__(self, config: str):
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
    print(f"    PUT ×{repetitions}... ", end="", flush=True)
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
          For s3-direct there is no meaningful cold/warm distinction — we still
          run N repetitions but label them 'warm' since there's no DEK cache.

    Warm: GET the same file N+1 times; discard first (cache fill); record N.
    """
    if is_s3_direct:
        # s3-direct: no cache concept, just N repetitions of the same GET
        key = f"bench/s3-direct/{size_label}.parquet"
        print(f"    GET s3-direct ×{repetitions}... ", end="", flush=True)
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
    print(f"    GET cold ×{repetitions}... ", end="", flush=True)
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
    print(f"    GET warm ×{repetitions} (1 throwaway first)... ", end="", flush=True)
    _ = time_get(s3, bucket, warm_key)  # throwaway — warms DEK + file cache
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


def generate_parquet_bytes(size_label: str) -> bytes:
    """Generate a parquet file as bytes using the same parameters as BenchmarkCorpusGenerator."""
    rows = ROW_COUNTS[size_label]
    rng = np.random.default_rng(RNG_SEED)
    data = {f"col_{i}": rng.random(rows) for i in range(COLUMNS)}
    table = pa.table(data)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Local temp file generation for PUT benchmark
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


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
        "--repetitions", type=int, help="Number of repetitions per measurement (overrides .env)"
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
    args = parser.parse_args()

    env = load_env(ENV_FILE)

    bucket = args.bucket or get_cfg(env, "BENCH_BUCKET") or "osws-benchmark"
    repetitions = args.repetitions or int(get_cfg(env, "BENCH_REPETITIONS") or "10")
    config = args.config
    is_s3_direct = config == "s3-direct"

    # Choose endpoint + credentials based on config
    if is_s3_direct:
        endpoint = get_cfg(env, "S3Settings__EndpointHostname")
        access_key = get_cfg(env, "S3Settings__AccessKeyId")
        secret_key = get_cfg(env, "S3Settings__SecretAccessKey")
        if not all([endpoint, access_key, secret_key]):
            print("ERROR: S3Settings__* credentials not found in .env", file=sys.stderr)
            sys.exit(1)
    else:
        endpoint = get_cfg(env, "OSWS_ENDPOINT") or "http://localhost:5000"
        access_key = get_cfg(env, "BENCH_OSWS_ACCESS_KEY")
        secret_key = get_cfg(env, "BENCH_OSWS_SECRET_KEY")
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

    print()
    print("╔════════════════════════════════════════════════════════╗")
    print("║   OSWS Benchmark Runner                                ║")
    print("╚════════════════════════════════════════════════════════╝")
    print()
    print(f"  Config      : {config}")
    print(f"  Endpoint    : {endpoint}")
    print(f"  Bucket      : {bucket}")
    print(f"  Repetitions : {repetitions}")
    print(f"  File sizes  : {', '.join(s for s in FILE_SIZES if s not in args.skip_sizes)}")
    print()

    writer = ResultsWriter(config)

    try:
        for size_label in FILE_SIZES:
            if size_label in args.skip_sizes:
                continue

            print(f"── {size_label} ─────────────────────────────────────────────────")

            if not args.skip_put:
                print(f"    Generating {size_label} parquet ({ROW_COUNTS[size_label]:,} rows)... ", end="", flush=True)
                data = generate_parquet_bytes(size_label)
                print(f"{len(data)/1024/1024:.1f} MB")
                run_put_benchmark(s3, bucket, config, size_label, data, repetitions, writer)

            run_get_benchmark(
                s3, bucket, config, size_label, repetitions, writer, is_s3_direct
            )
            print()

    finally:
        writer.close()

    print("✓ Done.")
    print(f"  Results: {writer.path}")
    print()
    print("Analyse with:")
    print(f"  python {SCRIPT_DIR}/analyse-results.py {RESULTS_DIR}/")


if __name__ == "__main__":
    main()
