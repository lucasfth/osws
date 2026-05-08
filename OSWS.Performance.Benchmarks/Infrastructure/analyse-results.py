#!/usr/bin/env python3
"""
OSWS Benchmark Results Analyser
================================
Reads all CSV files in a results directory and prints a summary table.

Usage:
    python analyse-results.py <results-dir>
    python analyse-results.py benchmark-results/
    python analyse-results.py benchmark-results/results_s3-direct_20250504T120000Z.csv
"""

import csv
import sys
from collections import defaultdict
from pathlib import Path


def load_csvs(paths: list[Path]) -> list[dict]:
    rows = []
    for p in paths:
        with open(p) as f:
            rows.extend(csv.DictReader(f))
    return rows


def stats(values: list[float]) -> dict:
    if not values:
        return {}
    s = sorted(values)
    n = len(s)
    mean = sum(s) / n
    variance = sum((x - mean) ** 2 for x in s) / n
    stddev = variance**0.5
    return {
        "n": n,
        "mean": mean,
        "stddev": stddev,
        "min": s[0],
        "p50": s[int(n * 0.50)],
        "p95": s[min(int(n * 0.95), n - 1)],
        "max": s[-1],
    }


def group_rows(rows: list[dict]) -> dict:
    """Group by (config, operation, cache_state, file_size_label)."""
    groups = defaultdict(list)
    for row in rows:
        key = (
            row["config"],
            row["operation"],
            row["cache_state"],
            row["file_size_label"],
        )
        try:
            groups[key].append(float(row["duration_ms"]))
        except (ValueError, KeyError):
            pass
    return groups


def fmt(val: float) -> str:
    if val >= 10_000:
        return f"{val / 1000:6.1f}s "
    return f"{val:7.0f}ms"


def print_table(groups: dict):
    # Define preferred ordering
    config_order = [
        "s3-direct",
        "osws-encrypt-cache",
        "osws-encrypt-no-file-cache",
        "osws-encrypt-no-dek-cache",
        "osws-no-encrypt",
    ]
    op_order = ["PUT", "GET"]
    cache_order = ["n/a", "cold", "warm"]
    size_order = ["tiny", "small", "medium"]

    all_configs = sorted(
        set(k[0] for k in groups),
        key=lambda x: config_order.index(x) if x in config_order else 99,
    )
    all_ops = sorted(
        set(k[1] for k in groups),
        key=lambda x: op_order.index(x) if x in op_order else 99,
    )
    all_caches = sorted(
        set(k[2] for k in groups),
        key=lambda x: cache_order.index(x) if x in cache_order else 99,
    )
    all_sizes = sorted(
        set(k[3] for k in groups),
        key=lambda x: size_order.index(x) if x in size_order else 99,
    )

    col_w = 10
    header_w = 38

    for op in all_ops:
        for cache in all_caches:
            # Check if there's any data for this op+cache combo
            relevant = {k: v for k, v in groups.items() if k[1] == op and k[2] == cache}
            if not relevant:
                continue

            print(f"\n{'-' * (header_w + col_w * len(all_sizes) + 2)}")
            print(f"  {op}  cache={cache}")
            print(f"{'─' * (header_w + col_w * len(all_sizes) + 2)}")

            # Header row
            header = f"  {'config':<28}  {'metric':<6}"
            for size in all_sizes:
                header += f"  {size:>{col_w - 2}}"
            print(header)
            print(f"{'-' * (header_w + col_w * len(all_sizes) + 2)}")

            for config in all_configs:
                s_all = {
                    size: stats(groups.get((config, op, cache, size), []))
                    for size in all_sizes
                }
                if not any(s_all.values()):
                    continue

                metrics = ["mean", "stddev", "p50", "p95"]
                for i, metric in enumerate(metrics):
                    prefix = f"  {config:<28}  " if i == 0 else f"  {'':28}  "
                    row = f"{prefix}{metric:<6}"
                    for size in all_sizes:
                        s = s_all[size]
                        if s and metric in s:
                            row += f"  {fmt(s[metric]):>{col_w - 2}}"
                        else:
                            row += f"  {'-':>{col_w - 2}}"
                    print(row)

                # n= line
                n_row = f"  {'':28}  {'n':<6}"
                for size in all_sizes:
                    s = s_all[size]
                    n_row += f"  {s.get('n', 0):>{col_w - 2}}"
                print(n_row)
                print()

    print(f"{'═' * (header_w + col_w * len(all_sizes) + 2)}")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    target = Path(sys.argv[1])

    if target.is_file():
        csv_files = [target]
    elif target.is_dir():
        csv_files = sorted(target.glob("results_*.csv"))
    else:
        print(f"ERROR: {target} not found", file=sys.stderr)
        sys.exit(1)

    if not csv_files:
        print(f"No results_*.csv files found in {target}", file=sys.stderr)
        sys.exit(1)

    print(f"Loading {len(csv_files)} file(s):")
    for f in csv_files:
        print(f"  {f.name}")

    rows = load_csvs(csv_files)
    print(f"  {len(rows)} measurements total")

    groups = group_rows(rows)
    print_table(groups)


if __name__ == "__main__":
    main()
