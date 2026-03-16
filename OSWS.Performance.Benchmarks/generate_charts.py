#!/usr/bin/env python3
"""Minimal entrypoint to generate benchmark charts."""

from chart_generator import load_csv, generate_html

import os
import sys


def main() -> None:
    if len(sys.argv) > 1:
        csv_path = os.path.abspath(sys.argv[1])
    else:
        csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "benchmark-results.csv")

    if not os.path.exists(csv_path):
        print(f"Error: CSV not found at {csv_path}", file=sys.stderr)
        sys.exit(1)

    rows = load_csv(csv_path)
    if not rows:
        print("Error: CSV is empty or has no data rows.", file=sys.stderr)
        sys.exit(1)

    html = generate_html(rows, csv_path)

    out_path = os.path.join(os.path.dirname(csv_path), "benchmark-charts.html")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"Charts written to: {out_path}")
    print(f"Open in browser:   file://{out_path}")


if __name__ == "__main__":
    main()
