#!/usr/bin/env python3
"""
Microbenchmark Results Analyser
================================
Reads per-iteration microbenchmark CSV files, filters warmup rows,
groups by (benchmark, parameters), and prints a summary table.

Usage:
    python analyse-micro-results.py <results-dir-or-file>
    python analyse-micro-results.py benchmark-results/
    python analyse-micro-results.py benchmark-results/micro-20260511T120000Z.csv
"""

import csv
import sys
from collections import defaultdict
from pathlib import Path


def load_csvs(paths: list[Path]) -> list[dict]:
    rows = []
    for p in paths:
        with open(p, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("is_warmup", "true") == "false":
                    rows.append(row)
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
        "p99": s[min(int(n * 0.99), n - 1)],
        "max": s[-1],
    }


def group_rows(rows: list[dict]) -> dict:
    """Group by (benchmark, parameters)."""
    groups = defaultdict(list)
    for row in rows:
        key = (row["benchmark"], row["parameters"])
        try:
            groups[key].append(float(row["duration_ms"]))
        except (ValueError, KeyError):
            pass
    return groups


def fmt_ms(val: float) -> str:
    if val >= 10_000:
        return f"{val / 1000:6.1f}s "
    if val >= 1000:
        return f"{val / 1000:6.2f}s"
    return f"{val:7.1f}ms"


def fmt_n(val: float) -> str:
    return f"{val:3.0f}"


def print_table(groups: dict):
    all_benchmarks = sorted(set(k[0] for k in groups))

    param_w = 30
    col_w = 10

    for bench in all_benchmarks:
        bench_groups = {k: v for k, v in groups.items() if k[0] == bench}
        if not bench_groups:
            continue

        all_params = sorted(set(k[1] for k in bench_groups))
        param_w = max(max(len(p) for p in all_params), 24)

        sep = "─" * (param_w + 4 + 6 + (col_w + 2) * 7 + 2)
        print(f"\n{sep}")
        print(f"  Benchmark: {bench}")
        print(sep)

        header = f"  {'parameters':<{param_w}}  {'n':>3}"
        metrics = ["mean", "stddev", "min", "p50", "p95", "p99", "max"]
        for m in metrics:
            header += f"  {m:>{col_w}}"
        print(header)
        print(f"{'-' * len(header)}")

        for params in all_params:
            s = stats(bench_groups.get((bench, params), []))
            if not s:
                continue
            row = f"  {params:<{param_w}}  {fmt_n(s['n']):>3}"
            for m in metrics:
                row += f"  {fmt_ms(s[m]):>{col_w}}"
            print(row)

    print(f"\n{'═' * (param_w + 4 + 6 + (col_w + 2) * 7 + 2)}")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    target = Path(sys.argv[1])

    if target.is_file():
        csv_files = [target]
    elif target.is_dir():
        csv_files = sorted(target.glob("micro-*.csv"))
    else:
        print(f"ERROR: {target} not found", file=sys.stderr)
        sys.exit(1)

    if not csv_files:
        print(f"No micro-*.csv files found in {target}", file=sys.stderr)
        sys.exit(1)

    print(f"Loading {len(csv_files)} file(s):")
    for f in csv_files:
        print(f"  {f.name}")

    rows = load_csvs(csv_files)
    measurement_rows = len(rows)
    print(f"  {measurement_rows} measurement rows (warmup filtered out)")

    groups = group_rows(rows)
    print_table(groups)


if __name__ == "__main__":
    main()
