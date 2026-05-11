#!/usr/bin/env python3
"""
Generate Typst boxplots from microbenchmark CSV results.

Reads per-iteration CSV, computes boxplot statistics per (benchmark, parameters),
and writes a standalone Typst document using the Lilaq library.

Usage:
    python generate-boxplots.py benchmark-results/micro-*.csv
    python generate-boxplots.py benchmark-results/micro-20260511T093920Z.csv -o plots.typ

The output Typst file can be compiled with:
    typst compile plots.typ
"""

import argparse
import csv
import math
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


def group_by_benchmark(rows: list[dict]) -> dict:
    groups: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    for row in rows:
        bench = row["benchmark"]
        params = row["parameters"]
        try:
            groups[bench][params].append(float(row["duration_ms"]))
        except (ValueError, KeyError):
            pass
    return {k: dict(v) for k, v in groups.items()}


def boxplot_stats(values: list[float]) -> dict:
    n = len(values)
    if n == 0:
        return {}

    s = sorted(values)

    def percentile(pct: float) -> float:
        k = pct * (n - 1)
        lo = int(math.floor(k))
        hi = int(math.ceil(k))
        if lo == hi:
            return s[lo]
        return s[lo] + (s[hi] - s[lo]) * (k - lo)

    q1 = percentile(0.25)
    q3 = percentile(0.75)
    median = percentile(0.50)
    iqr = q3 - q1

    low_fence = q1 - 1.5 * iqr
    high_fence = q3 + 1.5 * iqr

    whisker_low = min(v for v in s if v >= low_fence)
    whisker_high = max(v for v in s if v <= high_fence)
    outliers = [v for v in s if v < low_fence or v > high_fence]

    mean = sum(s) / n

    return {
        "median": median,
        "q1": q1,
        "q3": q3,
        "whisker_low": whisker_low,
        "whisker_high": whisker_high,
        "mean": mean,
        "outliers": outliers,
        "n": n,
    }


def fmt_val(v: float) -> str:
    return f"{v:.3f}"


def fmt_outliers(outliers: list[float]) -> str:
    if not outliers:
        return "()"
    if len(outliers) == 1:
        return "(" + fmt_val(outliers[0]) + ",)"
    return "(" + ", ".join(fmt_val(o) for o in sorted(outliers)) + ")"


def parameter_sort_key(bench_name: str, parameter: str) -> tuple:
    if bench_name == "Decryption":
        order = {"tiny": 0, "small": 1, "medium": 2, "large": 3, "xlarge": 4}
        if parameter.startswith("size="):
            size = parameter.removeprefix("size=")
            return (order.get(size, 99), size)
        return (99, parameter)

    if bench_name == "KeyUnwrap":
        parts = dict(part.split("=", 1) for part in parameter.split(",") if "=" in part)
        size = parts.get("size", "")
        if size != "tiny":
            return (99, parameter)
        dek_bits = int(parts.get("dek_bits", "999"))
        return (0, dek_bits)

    if bench_name == "PermissionHierarchy" and parameter.startswith("hierarchy_depth="):
        return (0, int(parameter.removeprefix("hierarchy_depth=")))

    if bench_name == "PermissionService" and parameter.startswith("role_count="):
        return (0, int(parameter.removeprefix("role_count=")))

    return (99, parameter)


def generate_boxplot(stats: dict, x: int) -> str:
    return (
        f"  lq.boxplot(\n"
        f"    (median: {fmt_val(stats['median'])}, "
        f"q1: {fmt_val(stats['q1'])}, "
        f"q3: {fmt_val(stats['q3'])},\n"
        f"     whisker-low: {fmt_val(stats['whisker_low'])}, "
        f"whisker-high: {fmt_val(stats['whisker_high'])},\n"
        f"     mean: {fmt_val(stats['mean'])}, "
        f"outliers: {fmt_outliers(stats['outliers'])}),\n"
        f'    x: {x}, mean: "+",\n'
        f"  ),"
    )


def format_parameter_label(bench_name: str, parameter: str) -> str:
    if bench_name == "Decryption" and parameter.startswith("size="):
        return parameter.removeprefix("size=")

    if bench_name == "KeyUnwrap":
        parts = dict(part.split("=", 1) for part in parameter.split(",") if "=" in part)
        dek_bits = parts.get("dek_bits")
        if dek_bits:
            return dek_bits
        return parameter

    if bench_name == "PermissionHierarchy" and parameter.startswith("hierarchy_depth="):
        return parameter.removeprefix("hierarchy_depth=")

    if bench_name == "PermissionService" and parameter.startswith("role_count="):
        return parameter.removeprefix("role_count=")

    return parameter


def determine_log_y_limits(all_stats: list[dict]) -> tuple[float, float]:
    all_vals = []
    for s in all_stats:
        all_vals.extend(
            [s["whisker_low"], s["whisker_high"], s["q1"], s["q3"], s["median"]]
        )
        all_vals.extend(s["outliers"])
    positive_vals = [v for v in all_vals if v > 0]
    if not positive_vals:
        return 0.1, 1.0

    ymin = min(positive_vals)
    ymax = max(positive_vals)
    lower = ymin / 1.25
    upper = ymax * 1.15
    return max(1e-6, lower), upper


def determine_linear_y_limits(all_stats: list[dict]) -> tuple[float, float]:
    all_vals = []
    for s in all_stats:
        all_vals.extend(
            [s["whisker_low"], s["whisker_high"], s["q1"], s["q3"], s["median"]]
        )
        all_vals.extend(s["outliers"])
    if not all_vals:
        return 0, 1

    ymin = min(all_vals)
    ymax = max(all_vals)
    padding = (ymax - ymin) * 0.08 if ymax > ymin else 1
    return max(0, ymin - padding), ymax + padding


def parameter_included(
    bench_name: str, parameter: str, allowed_sizes: set[str] | None = None
) -> bool:
    if allowed_sizes is None:
        return True

    if bench_name in {"Decryption", "KeyUnwrap"} and parameter.startswith("size="):
        size = parameter.removeprefix("size=").split(",", 1)[0]
        return size in allowed_sizes

    return True


def generate_benchmark_diagram(
    bench_name: str, param_groups: dict[str, list[float]]
    ,
    *,
    allowed_sizes: set[str] | None = None,
    use_log_y: bool = True,
    caption_note: str = "",
) -> str:
    sorted_params = [
        p
        for p in sorted(param_groups.keys(), key=lambda p: parameter_sort_key(bench_name, p))
        if parameter_sort_key(bench_name, p)[0] != 99
        and parameter_included(bench_name, p, allowed_sizes)
    ]
    stats_list = [boxplot_stats(param_groups[p]) for p in sorted_params]
    valid = [(p, s) for p, s in zip(sorted_params, stats_list) if s]
    if not valid:
        return f"// No data for benchmark: {bench_name}\n"

    ymin, ymax = (
        determine_log_y_limits([s for _, s in valid])
        if use_log_y
        else determine_linear_y_limits([s for _, s in valid])
    )
    x_tick_labels = ", ".join(
        f"({i}, [{format_parameter_label(bench_name, params)}])"
        for i, (params, _) in enumerate(valid, start=1)
    )

    if bench_name == "Decryption":
        xlabel = "Corpus size"
    elif bench_name == "KeyUnwrap":
        xlabel = "DEK size (bits)"
    elif bench_name == "PermissionHierarchy":
        xlabel = "Hierarchy depth"
    elif bench_name == "PermissionService":
        xlabel = "Direct role count"
    else:
        xlabel = "Parameters"

    ylabel = "Duration (ms, log scale)" if use_log_y else "Duration (ms)"

    lines = [
        f"= {bench_name}",
        "",
        "#figure(",
        "  lq.diagram(",
        f'    width: 100%, height: {min(6 + len(valid) * 1.5, 20):.1f}cm,',
        f'    xlabel: "{xlabel}", ylabel: "{ylabel}",',
        f"    xaxis: (ticks: ({x_tick_labels})),",
        f"    ylim: ({fmt_val(ymin)}, {fmt_val(ymax)}),",
        *(['    yscale: "log",'] if use_log_y else []),
        "",
    ]

    for i, (params, stats) in enumerate(valid, start=1):
        lines.append(generate_boxplot(stats, i))

    lines.extend(
        [
            "  ),",
            f"  caption: [{bench_name} \u2014 boxplot distribution per parameter set{caption_note}],",
            ")",
            "",
        ]
    )

    return "\n".join(lines)


def generate_typst(groups: dict[str, dict[str, list[float]]]) -> str:
    header = (
        '// Auto-generated microbenchmark boxplots\n'
        '#import "@preview/lilaq:0.6.0" as lq\n'
        "\n"
    )

    diagrams = []
    for bench_name in sorted(groups.keys()):
        if bench_name == "Decryption":
            diagrams.append(generate_benchmark_diagram(bench_name, groups[bench_name]))
            diagrams.append(
                generate_benchmark_diagram(
                    bench_name,
                    groups[bench_name],
                    allowed_sizes={"tiny", "small"},
                    use_log_y=False,
                    caption_note=" (zoomed tiny/small view for detail)",
                )
            )
            continue

        if bench_name == "KeyUnwrap":
            diagrams.append(
                generate_benchmark_diagram(
                    bench_name,
                    groups[bench_name],
                    allowed_sizes={"tiny"},
                    use_log_y=False,
                    caption_note=" (tiny corpus only; linear y axis)",
                )
            )
            continue

        diagrams.append(generate_benchmark_diagram(bench_name, groups[bench_name]))

    return header + "\n".join(diagrams)


def main():
    parser = argparse.ArgumentParser(
        description="Generate Typst boxplots from microbenchmark CSV results"
    )
    parser.add_argument(
        "input",
        nargs="+",
        help="CSV file(s) or directory containing micro-*.csv files",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="micro-boxplots.typ",
        help="Output Typst file (default: micro-boxplots.typ)",
    )
    args = parser.parse_args()

    csv_files = []
    for path_str in args.input:
        path = Path(path_str)
        if path.is_dir():
            csv_files.extend(sorted(path.glob("micro-*.csv")))
        elif path.is_file():
            csv_files.append(path)
        else:
            print(f"ERROR: {path} not found", file=sys.stderr)
            sys.exit(1)

    if not csv_files:
        print("No micro-*.csv files found.", file=sys.stderr)
        sys.exit(1)

    print(f"Loading {len(csv_files)} file(s):")
    for f in csv_files:
        print(f"  {f.name}")

    rows = load_csvs(csv_files)
    measurement_rows = len(rows)
    print(f"  {measurement_rows} measurement rows (warmup filtered out)")

    groups = group_by_benchmark(rows)
    print(f"  {len(groups)} benchmark(s) found: {', '.join(groups.keys())}")

    typst_content = generate_typst(groups)

    out_path = Path(args.output)
    out_path.write_text(typst_content, encoding="utf-8")
    print(f"\nBoxplot document written to: {out_path.resolve()}")
    print(f"Compile with: typst compile {out_path.name}")


if __name__ == "__main__":
    main()
