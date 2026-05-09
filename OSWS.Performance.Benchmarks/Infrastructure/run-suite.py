#!/usr/bin/env python3
"""
OSWS Benchmark suite driver

Runs all four benchmark configurations in order, cycling each OSWS Docker
service up/down between configs. Results land in a timestamped directory:
  benchmark-results/run-<YYYYMMDDTHHMMSSZ>/

Usage:
    python Infrastructure/run-suite.py
"""

import os
import shutil
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

SCRIPT_DIR = Path(__file__).parent
BENCHMARK_DIR = SCRIPT_DIR.parent
COMPOSE_FILE = BENCHMARK_DIR / "docker-compose.bench.yml"
CACHE_DIR = BENCHMARK_DIR / "bench-cache"
RESULTS_BASE = BENCHMARK_DIR / "benchmark-results"

load_dotenv(BENCHMARK_DIR / ".env")

OSWS_ENDPOINT = os.getenv("OSWS_ENDPOINT", "http://localhost:5000")
HEALTH_TIMEOUT = 120  # seconds to wait for OSWS to become healthy

OSWS_CONFIGS = [
    "osws-encrypt-cache",
    "osws-encrypt-no-file-cache",
    "osws-encrypt-no-dek-cache",
    "osws-no-encrypt",
]


def compose(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["docker", "compose", "-f", str(COMPOSE_FILE), *args],
        cwd=BENCHMARK_DIR,
        check=check,
    )


def wait_for_health(url: str, timeout: int):
    deadline = time.time() + timeout
    print(f"  Waiting for OSWS at {url}/health ", end="", flush=True)
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(f"{url}/health", timeout=3) as resp:
                if resp.status == 200:
                    print(" ready")
                    return
        except Exception:
            pass
        print(".", end="", flush=True)
        time.sleep(3)
    print()
    raise TimeoutError(f"OSWS did not become healthy within {timeout}s")


def clear_file_cache():
    if CACHE_DIR.exists():
        shutil.rmtree(CACHE_DIR)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    print("  File cache cleared")


def run_benchmark(config: str, results_dir: Path):
    print(f"  Running benchmark: {config}")
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT_DIR / "run-benchmark.py"),
            "--config",
            config,
            "--results-dir",
            str(results_dir),
        ],
        cwd=BENCHMARK_DIR,
    )
    if result.returncode != 0:
        print(
            f"  WARNING: benchmark '{config}' exited with code {result.returncode}",
            file=sys.stderr,
        )


def run_osws_config(config: str, results_dir: Path):
    print(f"\n{'─' * 60}")
    print(f"  Config: {config}")
    print(f"{'─' * 60}")
    clear_file_cache()
    compose("up", "-d", config)
    try:
        wait_for_health(OSWS_ENDPOINT, HEALTH_TIMEOUT)
        run_benchmark(config, results_dir)
    finally:
        compose("stop", config, check=False)
        compose("rm", "-f", config, check=False)


def main():
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    results_dir = RESULTS_BASE / f"run-{ts}"
    results_dir.mkdir(parents=True, exist_ok=True)
    print(f"Results directory: {results_dir}")

    # s3-direct: no OSWS container needed
    print(f"\n{'─' * 60}")
    print("  Config: s3-direct")
    print(f"{'─' * 60}")
    run_benchmark("s3-direct", results_dir)

    for config in OSWS_CONFIGS:
        run_osws_config(config, results_dir)

    print(f"\nSuite complete. Results in: {results_dir}")


if __name__ == "__main__":
    main()
