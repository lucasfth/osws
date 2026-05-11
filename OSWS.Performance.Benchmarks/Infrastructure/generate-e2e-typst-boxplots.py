#!/usr/bin/env python3
"""
OSWS Benchmark → Lilaq Boxplot Generator
==========================================
Reads benchmark CSV files and generates a Typst source file with Lilaq
boxplots — one figure per file size, each containing one box per config.

Usage:
    python generate-lilaq.py <results-dir> [--output <file.typ>] [--op GET|PUT] [--cache cold|warm|n/a]

Examples:
    python generate-lilaq.py benchmark-results/ --op GET --cache warm
    python generate-lilaq.py benchmark-results/ --op PUT --cache n/a --output put-boxplots.typ
"""

import argparse
import csv
import sys
from collections import defaultdict
from pathlib import Path


# ---------------------------------------------------------------------------
# Data loading (shared with analyse-results.py)
# ---------------------------------------------------------------------------

CONFIG_ORDER = [
    "s3-direct",
    "osws-encrypt-cache",
    "osws-encrypt-no-file-cache",
    "osws-no-encrypt",
]
SIZE_ORDER = ["tiny", "small", "medium", "large"]

# Human-readable labels for configs — short for x-axis ticks
CONFIG_LABELS = {
    "s3-direct": "S3 Direct",
    "osws-encrypt-cache": "OSWS",
    "osws-encrypt-no-file-cache": "No file cache",
    "osws-no-encrypt": "No encryption",
}

# Full labels used in the boxplot legend


def load_csvs(paths: list[Path]) -> list[dict]:
    rows = []
    for p in paths:
        with open(p) as f:
            rows.extend(csv.DictReader(f))
    return rows


def group_rows(rows: list[dict]) -> dict:
    groups = defaultdict(list)
    for row in rows:
        key = (
            row["config"],
            row["operation"],
            row["cache_state"],
            row["file_size_label"],
        )
        try:
            val = float(row["duration_ms"])
        except (ValueError, KeyError):
            continue
        # Skip error responses
        try:
            status = int(row.get("http_status", 200))
            if status < 200 or status >= 300:
                continue
        except (ValueError, TypeError):
            pass
        groups[key].append(val)
    return groups


# ---------------------------------------------------------------------------
# Boxplot statistics
# ---------------------------------------------------------------------------


def boxplot_stats(values: list[float]) -> dict | None:
    if not values:
        return None
    s = sorted(values)
    n = len(s)

    def percentile(p):
        idx = (n - 1) * p
        lo, frac = int(idx), idx % 1
        if frac == 0 or lo + 1 >= n:
            return s[lo]
        return s[lo] + frac * (s[lo + 1] - s[lo])

    q1 = percentile(0.25)
    median = percentile(0.50)
    q3 = percentile(0.75)
    iqr = q3 - q1

    fence_lo = q1 - 1.5 * iqr
    fence_hi = q3 + 1.5 * iqr

    inliers = [v for v in s if fence_lo <= v <= fence_hi]
    outliers = [v for v in s if v < fence_lo or v > fence_hi]

    whisker_low = min(inliers) if inliers else q1
    whisker_high = max(inliers) if inliers else q3

    return {
        "median": median,
        "q1": q1,
        "q3": q3,
        "whisker_low": whisker_low,
        "whisker_high": whisker_high,
        "outliers": outliers,
        "n": n,
    }


# ---------------------------------------------------------------------------
# Typst / Lilaq generation
# ---------------------------------------------------------------------------


def ms_to_typst(ms: float) -> str:
    """Format a millisecond value as a Typst float (keep as ms)."""
    return f"{ms:.2f}"


def outliers_to_typst(outliers: list[float]) -> str:
    if not outliers:
        return "()"
    return "(" + ", ".join(ms_to_typst(v) for v in outliers) + ",)"


def render_boxplot_call(stats: dict, x: int, tick_label: str) -> str:
    lines = [
        f"    lq.boxplot(",
        f"      x: {x},",
        f"      (",
        f"        median:       {ms_to_typst(stats['median'])},",
        f"        q1:           {ms_to_typst(stats['q1'])},",
        f"        q3:           {ms_to_typst(stats['q3'])},",
        f"        whisker-low:  {ms_to_typst(stats['whisker_low'])},",
        f"        whisker-high: {ms_to_typst(stats['whisker_high'])},",
        f"        outliers:     {outliers_to_typst(stats['outliers'])},",
        f"      ),",
        f"    ),",
    ]
    return "\n".join(lines)


def render_figure(
    size: str, op: str, cache: str, configs: list[str], groups: dict, log_scale: bool = False
) -> str:
    """Render a complete lq.diagram for one file size."""
    box_calls = []
    x_labels = []  # (x, tick_label) for x-axis ticks

    present_configs = [c for c in configs if groups.get((c, op, cache, size))]

    if not present_configs:
        return ""

    for x, config in enumerate(present_configs, start=1):
        values = groups.get((config, op, cache, size), [])
        st = boxplot_stats(values)
        if st is None:
            continue
        tick_label = CONFIG_LABELS.get(config, config)
        box_calls.append(render_boxplot_call(st, x, tick_label))
        x_labels.append((x, tick_label))

    if not box_calls:
        return ""

    # Build x-axis ticks as array of (position, [label]) tuples
    tick_entries = ", ".join(f'({x}, [{label}])' for x, label in x_labels)
    x_ticks = f"({tick_entries})"

    caption = f"{op} latency — {size} — cache={cache}"

    lines = [
        f"// {caption}",
        f"#figure(",
        f"  lq.diagram(",
        f"    width: 10cm,",
        f"    title: [{caption}],",
        f"    xaxis: (ticks: {x_ticks}),",
        f"    ylabel: [Latency (ms)],",
    ]
    if log_scale:
        lines.append(f"    yscale: \"log\",")
    for call in box_calls:
        lines.append(call)
    lines += [
        f"  ),",
        f"  caption: [{caption}],",
        f")",
    ]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "results_dir", help="Directory with benchmark CSV files (or a single CSV)"
    )
    parser.add_argument(
        "--output",
        "-o",
        default="boxplots.typ",
        help="Output .typ file (default: boxplots.typ)",
    )
    parser.add_argument(
        "--op",
        default="GET",
        choices=["GET", "PUT"],
        help="Operation to plot (default: GET)",
    )
    parser.add_argument(
        "--cache",
        default="warm",
        help="Cache state to plot: cold, warm, n/a (default: warm)",
    )
    parser.add_argument(
        "--log-scale",
        action="store_true",
        default=True,
        help="Use log scale on the y-axis (default: on; use --no-log-scale to disable)",
    )
    parser.add_argument(
        "--no-log-scale",
        dest="log_scale",
        action="store_false",
        help="Use linear scale on the y-axis",
    )
    args = parser.parse_args()

    target = Path(args.results_dir)
    if target.is_file():
        csv_files = [target]
    elif target.is_dir():
        csv_files = sorted(target.glob("*.csv"))
    else:
        print(f"ERROR: {target} not found", file=sys.stderr)
        sys.exit(1)

    if not csv_files:
        print(f"No *.csv files in {target}", file=sys.stderr)
        sys.exit(1)

    print(f"Loading {len(csv_files)} CSV file(s)...")
    rows = load_csvs(csv_files)
    print(f"  {len(rows)} measurements total")
    groups = group_rows(rows)

    configs = sorted(
        (c for c in set(k[0] for k in groups) if c in CONFIG_ORDER),
        key=lambda x: CONFIG_ORDER.index(x),
    )
    sizes = sorted(
        (s for s in set(k[3] for k in groups) if s in SIZE_ORDER),
        key=lambda x: SIZE_ORDER.index(x),
    )

    header = """\
#import "@preview/lilaq:0.6.0" as lq

// Generated by generate-lilaq.py
// One figure per file size; each box is one benchmark configuration.
"""

    figures = []
    for size in sizes:
        fig = render_figure(size, args.op, args.cache, configs, groups, log_scale=args.log_scale)
        if fig:
            figures.append(fig)

    if not figures:
        print(f"No data found for op={args.op} cache={args.cache}", file=sys.stderr)
        sys.exit(1)

    output = header + "\n\n".join(figures) + "\n"

    out_path = Path(args.output)
    out_path.write_text(output)
    print(f"Written to {out_path}  ({len(figures)} figure(s))")


if __name__ == "__main__":
    main()
