"""Chart generation utilities for OSWS.Performance.Benchmarks."""

import csv
from dataclasses import dataclass
import json
import math
import os
import re
from datetime import datetime


JSON_STAT_COLUMNS = (
    "MemoryStatsJson",
    "CallCountStatsJson",
    "LatencyStatsJson",
    "OperationStatsJson",
)

NUMERIC_COLUMNS = (
    "SampleCount",
    "TotalElapsedMs_Avg",
    "TotalElapsedMs_Min",
    "TotalElapsedMs_Max",
    "TotalElapsedMs_Median",
    "TotalElapsedMs_P99",
    "TotalElapsedMs_StdDev",
)

OPERATION_STAT_SERIES = (
    ("MinMs", "Min", ("rgba(59,130,246,0.70)", "rgb(59,130,246)")),
    ("MedianMs", "Median", ("rgba(34,197,94,0.70)", "rgb(34,197,94)")),
    ("MeanMs", "Mean", ("rgba(249,115,22,0.70)", "rgb(249,115,22)")),
    ("P99Ms", "P99", ("rgba(239,68,68,0.70)", "rgb(239,68,68)")),
    ("MaxMs", "Max", ("rgba(168,85,247,0.70)", "rgb(168,85,247)")),
)


@dataclass
class OperationScaleGroup:
    """Shared-scale metadata for a family of comparable operation charts."""

    chart_count: int = 0
    max_value: float = 0.0
    min_nonzero: float | None = None

    def register_chart(self) -> None:
        self.chart_count += 1

    def register_value(self, value: float) -> None:
        self.max_value = max(self.max_value, value)
        if value > 0:
            if self.min_nonzero is None:
                self.min_nonzero = value
            else:
                self.min_nonzero = min(self.min_nonzero, value)

    @property
    def has_shared_scale(self) -> bool:
        return self.chart_count > 1

    @property
    def use_log_scale(self) -> bool:
        return bool(
            self.has_shared_scale
            and self.min_nonzero
            and self.max_value / self.min_nonzero > 100
        )

    @property
    def shared_min(self) -> float:
        if self.use_log_scale and self.min_nonzero is not None:
            return log_floor(self.min_nonzero)
        return 0.0

    @property
    def shared_max(self) -> float:
        padded_max = self.max_value * 1.1 if self.max_value > 0 else 1.0
        return log_ceil(padded_max) if self.use_log_scale else padded_max


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_csv(path: str) -> list[dict]:
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            for col in JSON_STAT_COLUMNS:
                row[col] = parse_json_dict(row.get(col))

            for col in NUMERIC_COLUMNS:
                row[col] = parse_float(row.get(col))

            rows.append(row)
    return rows


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def short_name(name: str) -> str:
    """Remove the Measurement<N>_ prefix so labels stay concise."""
    return re.sub(r"^Measurement\d+_", "", name)


def group_of(name: str) -> str:
    m = re.match(r"^(Measurement\d+)", name)
    return m.group(1) if m else "Other"


_PALETTE = {
    "Measurement1": ("59,130,246", "Measurement 1 – Cold Wide Range"),
    "Measurement2": ("34,197,94", "Measurement 2 – Warm DEK Column Select"),
    "Measurement3": ("249,115,22", "Measurement 3 – Full Decryption Throughput"),
    "Measurement4": ("239,68,68", "Measurement 4 – DEK Cache Stress / Key Latency"),
    "Other": ("156,163,175", "Other"),
}


def rgba(group: str, alpha: float = 0.75) -> str:
    r, g, b = _PALETTE.get(group, _PALETTE["Other"])[0].split(",")
    return f"rgba({r},{g},{b},{alpha})"


def rgb(group: str) -> str:
    r, g, b = _PALETTE.get(group, _PALETTE["Other"])[0].split(",")
    return f"rgb({r},{g},{b})"


def js(obj) -> str:
    """Serialize to a JavaScript-safe JSON string."""
    return json.dumps(obj, ensure_ascii=False)


def parse_json_dict(raw_value: str | None) -> dict:
    """Parse a JSON object column from the CSV, defaulting to an empty dict."""
    try:
        return json.loads(raw_value) if raw_value else {}
    except (json.JSONDecodeError, TypeError):
        return {}


def parse_float(raw_value: str | None) -> float:
    """Parse a numeric CSV column, defaulting invalid values to 0.0."""
    try:
        return float(raw_value or 0)
    except (ValueError, TypeError):
        return 0.0


def log_floor(value: float) -> float:
    return 10 ** math.floor(math.log10(value))


def log_ceil(value: float) -> float:
    return 10 ** math.ceil(math.log10(value))


def operation_comparison_group(benchmark: str) -> str:
    """Group operation charts by the comparison family they belong to."""
    chart_key = short_name(benchmark)
    parts = chart_key.split("_")
    if len(parts) >= 3 and parts[-1] in {"Cold", "Warm"}:
        return f"{parts[0]}_{parts[-1]}"
    return parts[0]


# ---------------------------------------------------------------------------
# Chart data builders
# ---------------------------------------------------------------------------

def build_elapsed_chart(rows: list[dict]) -> dict:
    """Grouped bar chart: Min / Avg / P99 elapsed time (log scale)."""
    labels = [short_name(r["Benchmark"]) for r in rows]
    groups = [group_of(r["Benchmark"]) for r in rows]

    def dataset(key: str, label: str, alpha: float) -> dict:
        return {
            "label": label,
            "data": [r[key] for r in rows],
            "backgroundColor": [rgba(g, alpha) for g in groups],
            "borderColor": [rgb(g) for g in groups],
            "borderWidth": 1,
        }

    return {
        "labels": labels,
        "datasets": [
            dataset("TotalElapsedMs_Min", "Min", 0.35),
            dataset("TotalElapsedMs_Median", "Median", 0.60),
            dataset("TotalElapsedMs_Avg", "Avg", 0.75),
            dataset("TotalElapsedMs_P99", "P99", 0.90),
        ],
    }


def build_memory_chart(rows: list[dict]) -> dict:
    """Grouped bar chart: Initial / Peak / Increase memory (MB)."""
    labels = [short_name(r["Benchmark"]) for r in rows]
    groups = [group_of(r["Benchmark"]) for r in rows]

    def extract(row: dict, key: str) -> float:
        return row["MemoryStatsJson"].get(key, {}).get("Avg", 0.0)

    return {
        "labels": labels,
        "datasets": [
            {
                "label": "Initial Memory (MB)",
                "data": [extract(r, "InitialMemoryMb") for r in rows],
                "backgroundColor": [rgba(g, 0.40) for g in groups],
                "borderColor": [rgb(g) for g in groups],
                "borderWidth": 1,
            },
            {
                "label": "Peak Memory (MB)",
                "data": [extract(r, "PeakMemoryMb") for r in rows],
                "backgroundColor": [rgba(g, 0.75) for g in groups],
                "borderColor": [rgb(g) for g in groups],
                "borderWidth": 1,
            },
            {
                "label": "Memory Increase (MB)",
                "data": [extract(r, "MemoryIncreaseMb") for r in rows],
                "backgroundColor": [rgba(g, 0.55) for g in groups],
                "borderColor": [rgb(g) for g in groups],
                "borderWidth": 1,
                "borderDash": [4, 4],
            },
        ],
    }


def build_latency_chart(rows: list[dict]) -> dict:
    """Bar chart: average external KV call latency (ms) – only non-zero rows."""
    filtered = [
        r
        for r in rows
        if r["LatencyStatsJson"].get("KvAvgLatencyMs", {}).get("Avg", 0) > 0
    ]
    labels = [short_name(r["Benchmark"]) for r in filtered]
    groups = [group_of(r["Benchmark"]) for r in filtered]

    def lat(row: dict, key: str) -> float:
        return row["LatencyStatsJson"].get(key, {}).get("Avg", 0.0)

    return {
        "labels": labels,
        "datasets": [
            {
                "label": "External KV Avg Latency (ms)",
                "data": [lat(r, "KvAvgLatencyMs") for r in filtered],
                "backgroundColor": [rgba(g, 0.75) for g in groups],
                "borderColor": [rgb(g) for g in groups],
                "borderWidth": 1,
            },
        ],
        "empty": len(filtered) == 0,
    }


def build_call_counts_chart(rows: list[dict]) -> dict:
    """Stacked bar: KV / CachedKV / S3 call counts per benchmark."""
    labels = [short_name(r["Benchmark"]) for r in rows]
    groups = [group_of(r["Benchmark"]) for r in rows]

    def count(row: dict, key: str) -> float:
        return row["CallCountStatsJson"].get(key, {}).get("Avg", 0.0)

    return {
        "labels": labels,
        "datasets": [
            {
                "label": "External KV Calls",
                "data": [count(r, "KvCallCount") for r in rows],
                "backgroundColor": "rgba(239,68,68,0.70)",
                "borderColor": "rgb(239,68,68)",
                "borderWidth": 1,
            },
            {
                "label": "Cached KV Calls",
                "data": [count(r, "CachedKvCallCount") for r in rows],
                "backgroundColor": "rgba(34,197,94,0.70)",
                "borderColor": "rgb(34,197,94)",
                "borderWidth": 1,
            },
            {
                "label": "S3 Calls",
                "data": [count(r, "S3CallCount") for r in rows],
                "backgroundColor": "rgba(59,130,246,0.70)",
                "borderColor": "rgb(59,130,246)",
                "borderWidth": 1,
            },
        ],
    }


def build_operation_charts(rows: list[dict]) -> list[dict]:
    """One chart per benchmark that has per-operation latency data."""
    charts = []
    for row in rows:
        ops = row["OperationStatsJson"]
        if not ops:
            continue

        labels = list(ops.keys())
        short_labels = [
            re.sub(r"^.+_(Decrypt|Encrypt|Read|Write|Other)", r"\1", l)
            for l in labels
        ]

        datasets = []
        for stat_key, label_text, (bg, bd) in OPERATION_STAT_SERIES:
            datasets.append({
                "label": label_text,
                "data": [ops[k].get(stat_key, 0.0) for k in labels],
                "backgroundColor": bg,
                "borderColor": bd,
                "borderWidth": 1,
            })

        charts.append({
            "title": f"{short_name(row['Benchmark'])} – Operation Latency",
            "benchmark": row["Benchmark"],
            "labels": short_labels,
            "datasets": datasets,
        })
    return charts


# ---------------------------------------------------------------------------
# Legend helpers
# ---------------------------------------------------------------------------

def build_legend_items() -> str:
    html = ""
    for group, (rgb_val, title) in _PALETTE.items():
        if group == "Other":
            continue
        html += (
            f'<span class="legend-item">'
            f'<span class="legend-dot" style="background:rgb({rgb_val})"></span>'
            f'{title}</span>\n'
        )
    return html


def build_typst_legend() -> str:
    """Build a Typst snippet for the measurement legend with matching colors."""
    lines = [
        "// OSWS benchmark legend",
        "#let benchmark_legend = [",
    ]

    for group, (rgb_val, title) in _PALETTE.items():
        if group == "Other":
            continue
        r, g, b = rgb_val.split(",")
        lines.append(f"  #text(fill: rgb({r}, {g}, {b}))[●] {title} \\")

    lines.extend([
        "]",
        "#benchmark_legend",
    ])
    return "\n".join(lines)


def build_operation_scale_groups(op_charts: list[dict]) -> dict[str, OperationScaleGroup]:
    groups: dict[str, OperationScaleGroup] = {}

    for chart in op_charts:
        group_name = operation_comparison_group(chart["benchmark"])
        group = groups.setdefault(group_name, OperationScaleGroup())
        group.register_chart()

        for dataset in chart["datasets"]:
            for raw_value in dataset.get("data", []):
                try:
                    group.register_value(float(raw_value))
                except (TypeError, ValueError):
                    continue

    return groups


def build_kv_latency_js(lat_data: dict) -> str:
    if lat_data.get("empty"):
        return "document.getElementById('kvLatencyCard').style.display='none';"

    return f"""
new Chart(document.getElementById('kvLatencyChart'), {{
  type: 'bar',
  data: {js(lat_data)},
  options: {{
    responsive: true, maintainAspectRatio: false,
    plugins: {{ legend: {{ display: false }}, tooltip: TOOLTIP }},
    scales: {{
      x: {{ grid: {{ color: '#1e3a5f22' }} }},
      y: {{ ...AXIS_DEFAULTS('Avg Latency (ms)', false), min: 0 }},
    }},
  }},
}});"""


def build_operation_scale_js(group: OperationScaleGroup, shared: bool) -> str:
    if not shared or not group.has_shared_scale:
        return "{ ...AXIS_DEFAULTS('Latency (ms)', false), min: 0 }"

    parts = [
        f"...AXIS_DEFAULTS('Latency (ms)', {str(group.use_log_scale).lower()})",
        f"min: {group.shared_min}",
        f"max: {group.shared_max}",
    ]
    return "{ " + ", ".join(parts) + " }"


def build_operation_toggle_button(canvas_id: str, group_name: str, enabled: bool) -> str:
    if not enabled:
        return ""

    return (
        f'<button type="button" class="chart-button chart-toggle" '
        f'data-chart="{canvas_id}" data-group="{group_name}">Shared scale</button>'
    )


def build_operation_chart_script(
    canvas_id: str,
    chart_data: dict,
    group_name: str,
    shared_scale: str,
    auto_scale: str,
    default_mode: str,
) -> str:
    initial_scale = shared_scale if default_mode == "shared" else auto_scale

    return f"""
(function() {{
  const chart = new Chart(document.getElementById('{canvas_id}'), {{
    type: 'bar',
    data: {js(chart_data)},
    options: {{
      responsive: true, maintainAspectRatio: false,
      plugins: {{ legend: {{ position: 'top' }}, tooltip: TOOLTIP }},
      scales: {{
        x: {{ grid: {{ color: '#1e3a5f22' }} }},
        y: {initial_scale},
      }},
    }},
  }});

  chart._scaleConfigs = {{ shared: {shared_scale}, auto: {auto_scale} }};
  chart._currentScale = '{default_mode}';
  chart._group = '{group_name}';
  CHARTS['{canvas_id}'] = chart;

  if (!CHARTS_BY_GROUP[chart._group]) {{
    CHARTS_BY_GROUP[chart._group] = [];
  }}
  CHARTS_BY_GROUP[chart._group].push(chart);

  const toggleBtn = document.querySelector('[data-chart="{canvas_id}"]');
  if (toggleBtn) {{
    const applyScale = (mode) => {{
      const btns = document.querySelectorAll(`[data-group="${{chart._group}}"]`);
      btns.forEach(b => b.textContent = mode === 'shared' ? 'Shared scale' : 'Auto scale');
      (CHARTS_BY_GROUP[chart._group] || []).forEach(c => {{
        c.options.scales.y = c._scaleConfigs[mode];
        c._currentScale = mode;
        c.update();
      }});
    }};

    applyScale(chart._currentScale);

    toggleBtn.addEventListener('click', () => {{
      const next = chart._currentScale === 'shared' ? 'auto' : 'shared';
      applyScale(next);
    }});
  }}
}})();"""


def build_operation_assets(op_charts: list[dict]) -> tuple[str, str]:
    if not op_charts:
        return "", ""

    scale_groups = build_operation_scale_groups(op_charts)
    card_html_parts = ['<div class="op-grid">']
    chart_scripts = []

    for index, chart in enumerate(op_charts):
        canvas_id = f"opChart_{index}"
        group_name = operation_comparison_group(chart["benchmark"])
        scale_group = scale_groups[group_name]
        default_mode = "shared" if scale_group.has_shared_scale else "auto"
        shared_scale = build_operation_scale_js(scale_group, shared=True)
        auto_scale = build_operation_scale_js(scale_group, shared=False)

        card_html_parts.append(
            OP_CARD_TEMPLATE.format(
                title=chart["title"],
                canvas_id=canvas_id,
                toggle_button=build_operation_toggle_button(
                    canvas_id,
                    group_name,
                    scale_group.has_shared_scale,
                ),
            )
        )

        chart_scripts.append(
            build_operation_chart_script(
                canvas_id=canvas_id,
                chart_data={"labels": chart["labels"], "datasets": chart["datasets"]},
                group_name=group_name,
                shared_scale=shared_scale,
                auto_scale=auto_scale,
                default_mode=default_mode,
            )
        )

    card_html_parts.append("</div>")
    return "".join(card_html_parts), "\n".join(chart_scripts)


# ---------------------------------------------------------------------------
# HTML generation
# ---------------------------------------------------------------------------

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>OSWS Benchmark Charts</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.3/dist/chart.umd.min.js"></script>
<style>
  *, *::before, *::after {{ box-sizing: border-box; }}
  :root {{
    --bg: #ffffff; --surface: #f8fafc; --border: #e2e8f0;
    --text: #0f172a; --muted: #64748b; --accent: #0ea5e9;
  }}
  body {{ margin: 0; font-family: system-ui, sans-serif; background: var(--bg); color: var(--text); }}
  header {{ padding: 24px 32px 16px; border-bottom: 1px solid var(--border); }}
  .header-row {{ display: flex; align-items: flex-start; justify-content: space-between; gap: 12px; }}
  .header-actions {{ display: flex; gap: 8px; }}
  header h1 {{ margin: 0 0 4px; font-size: 1.25rem; }}
  header p {{ margin: 0; color: var(--muted); font-size: 0.85rem; }}
  .legend {{ display: flex; flex-wrap: wrap; gap: 16px; margin-top: 12px; }}
  .legend-item {{ display: flex; align-items: center; gap: 6px; font-size: 0.8rem; color: var(--muted); }}
  .legend-dot {{ width: 12px; height: 12px; border-radius: 50%; flex-shrink: 0; }}
  main {{ padding: 24px 32px; display: grid; gap: 32px; }}
  .card {{
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 12px; padding: 20px 24px;
  }}
  .card-header {{ display: flex; justify-content: space-between; align-items: baseline; gap: 12px; margin-bottom: 12px; }}
  .card-header h2 {{ margin: 0; font-size: 1rem; }}
  .card-actions {{ display: flex; gap: 8px; align-items: center; }}
  .chart-button {{
    border: 1px solid var(--border);
    background: var(--bg);
    color: var(--text);
    padding: 6px 10px;
    border-radius: 8px;
    cursor: pointer;
    font-size: 0.8rem;
  }}
  .chart-button:hover {{ background: #eef2ff; }}
  .toast {{
    position: fixed;
    bottom: 20px;
    right: 20px;
    background: rgba(15,23,42,0.9);
    color: #fff;
    padding: 12px 16px;
    border-radius: 10px;
    box-shadow: 0 12px 20px rgba(0,0,0,0.2);
    font-size: 0.9rem;
    opacity: 0;
    transition: opacity 150ms ease-in-out;
    z-index: 1000;
  }}
  .toast.show {{ opacity: 1; }}
  .card p {{ margin: 0 0 16px; color: var(--muted); font-size: 0.8rem; }}
  .chart-wrap {{ position: relative; width: 100%; }}
  canvas {{ background: white; }}
  .op-grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(560px, 1fr)); gap: 24px; }}
</style>
</head>
<body>
<header>
  <div class="header-row">
    <div>
      <h1>OSWS Benchmark Charts</h1>
      <p>Generated {timestamp} &nbsp;·&nbsp; {row_count} benchmark rows &nbsp;·&nbsp; source: {source}</p>
    </div>
    <div class="header-actions">
      <button type="button" class="chart-button" id="copyTypstLegendButton">Copy Typst Legend</button>
    </div>
  </div>
  <div class="legend">{legend}</div>
</header>
<main>

  <!-- Elapsed Time -->
  <div class="card">
    <div class="card-header">
      <h2>Total Elapsed Time</h2>
      <div class="card-actions">
        <button type="button" class="chart-button" data-canvas="elapsedChart">Copy</button>
      </div>
    </div>
    <p>Min / Median / Avg / P99 per benchmark (logarithmic scale). Colour indicates measurement group.</p>
    <div class="chart-wrap" style="height:420px">
      <canvas id="elapsedChart"></canvas>
    </div>
  </div>

  <!-- Memory -->
  <div class="card">
    <div class="card-header">
      <h2>Memory Usage</h2>
      <div class="card-actions">
        <button type="button" class="chart-button" data-canvas="memoryChart">Copy</button>
      </div>
    </div>
    <p>Average Initial, Peak, and Increase memory in MB across benchmark runs.</p>
    <div class="chart-wrap" style="height:420px">
      <canvas id="memoryChart"></canvas>
    </div>
  </div>

  <!-- Call Counts -->
  <div class="card">
    <div class="card-header">
      <h2>Call Counts</h2>
      <div class="card-actions">
        <button type="button" class="chart-button" data-canvas="callCountChart">Copy</button>
      </div>
    </div>
    <p>Average number of External KV, Cached KV, and S3 calls per benchmark execution.</p>
    <div class="chart-wrap" style="height:420px">
      <canvas id="callCountChart"></canvas>
    </div>
  </div>

  <!-- KV Latency -->
  <div class="card" id="kvLatencyCard">
    <div class="card-header">
      <h2>External KV Call Latency</h2>
      <div class="card-actions">
        <button type="button" class="chart-button" data-canvas="kvLatencyChart">Copy</button>
      </div>
    </div>
    <p>Average per-call latency to the external key vault (benchmarks with 0 KV calls excluded).</p>
    <div class="chart-wrap" style="height:360px">
      <canvas id="kvLatencyChart"></canvas>
    </div>
  </div>

  <!-- Per-operation latency -->
  {op_cards}

</main>
<script>
Chart.defaults.color = '#0f172a';
Chart.defaults.borderColor = '#e2e8f0';

const AXIS_DEFAULTS = (title, log) => ({{
  title: {{ display: true, text: title, color: '#64748b', font: {{ size: 11 }}}},
  type: log ? 'logarithmic' : 'linear',
  grid: {{ color: '#64748b22' }},
  ...(log ? {{
    afterBuildTicks: axis => {{
      axis.ticks = axis.ticks.filter(tick => {{
        const value = Number(tick.value);
        if (!Number.isFinite(value) || value <= 0) {{
          return false;
        }}

        const exponent = Math.log10(value);
        return Math.abs(exponent - Math.round(exponent)) < 1e-10;
      }});
    }},
    ticks: {{
      major: {{ enabled: true }},
      callback: value => {{
        const numericValue = Number(value);
        if (!Number.isFinite(numericValue) || numericValue <= 0) {{
          return '';
        }}
        if (numericValue >= 1) {{
          return numericValue.toLocaleString(undefined, {{ maximumFractionDigits: 0 }});
        }}
        if (numericValue >= 0.01) {{
          return numericValue.toLocaleString(undefined, {{ minimumFractionDigits: 0, maximumFractionDigits: 3 }});
        }}
        return numericValue.toExponential(0);
      }},
    }},
  }} : {{}}),
}});

const TOOLTIP = {{
  callbacks: {{
    label: ctx => ` ${{ctx.dataset.label}}: ${{ctx.parsed.y?.toLocaleString(undefined, {{maximumFractionDigits: 2}})}}`,
  }},
}};

const TYPST_LEGEND = {typst_legend};

const showToast = (message) => {{
  let toast = document.getElementById('toast');
  if (!toast) {{
    toast = document.createElement('div');
    toast.id = 'toast';
    toast.className = 'toast';
    document.body.appendChild(toast);
  }}
  toast.textContent = message;
  toast.classList.add('show');
  clearTimeout(toast._timeout);
  toast._timeout = setTimeout(() => toast.classList.remove('show'), 2200);
}};

const copyChart = async (canvasId) => {{
  const canvas = document.getElementById(canvasId);
  if (!canvas) return showToast('Chart not found.');
  if (!navigator.clipboard?.write) return showToast('Clipboard not supported in this browser.');

  canvas.toBlob(async (blob) => {{
    if (!blob) return showToast('Unable to create chart image.');
    try {{
      await navigator.clipboard.write([new ClipboardItem({{ 'image/png': blob }})]);
      showToast('Chart copied to clipboard.');
    }} catch (err) {{
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `${{canvasId}}.png`;
      a.click();
      URL.revokeObjectURL(url);
      showToast('Saved chart as PNG (clipboard not available).');
    }}
  }});
}};

const copyText = async (content, successMessage) => {{
  if (!navigator.clipboard?.writeText) return showToast('Clipboard not supported in this browser.');
  try {{
    await navigator.clipboard.writeText(content);
    showToast(successMessage);
  }} catch (err) {{
    showToast('Unable to copy text to clipboard.');
  }}
}};

document.querySelectorAll('.chart-button[data-canvas]').forEach(btn => {{
  btn.addEventListener('click', () => copyChart(btn.dataset.canvas));
}});

const copyTypstLegendButton = document.getElementById('copyTypstLegendButton');
if (copyTypstLegendButton) {{
  copyTypstLegendButton.addEventListener('click', () =>
    copyText(TYPST_LEGEND, 'Typst legend copied to clipboard.')
  );
}}

const CHARTS = {{}};
const CHARTS_BY_GROUP = {{}};

// --- Elapsed Time ---
new Chart(document.getElementById('elapsedChart'), {{
  type: 'bar',
  data: {elapsed_data},
  options: {{
    responsive: true, maintainAspectRatio: false,
    plugins: {{ legend: {{ position: 'top' }}, tooltip: TOOLTIP }},
    scales: {{
      x: {{ grid: {{ color: '#1e3a5f22' }} }},
      y: {{ ...AXIS_DEFAULTS('Elapsed (ms)', true) }},
    }},
  }},
}});

// --- Memory ---
new Chart(document.getElementById('memoryChart'), {{
  type: 'bar',
  data: {memory_data},
  options: {{
    responsive: true, maintainAspectRatio: false,
    plugins: {{ legend: {{ position: 'top' }}, tooltip: TOOLTIP }},
    scales: {{
      x: {{ grid: {{ color: '#1e3a5f22' }} }},
      y: {{ ...AXIS_DEFAULTS('Memory (MB)', false), min: 0 }},
    }},
  }},
}});

// --- Call Counts ---
new Chart(document.getElementById('callCountChart'), {{
  type: 'bar',
  data: {call_count_data},
  options: {{
    responsive: true, maintainAspectRatio: false,
    plugins: {{ legend: {{ position: 'top' }}, tooltip: TOOLTIP }},
    scales: {{
      x: {{ grid: {{ color: '#1e3a5f22' }}, stacked: true }},
      y: {{ ...AXIS_DEFAULTS('Call Count', false), min: 0, stacked: true }},
    }},
  }},
}});

// --- KV Latency ---
{kv_latency_js}

// --- Operation latency charts ---
{op_js}
</script>
</body>
</html>
"""


OP_CARD_TEMPLATE = """
<div class="card">
  <div class="card-header">
    <h2>{title}</h2>
    <div class="card-actions">
      {toggle_button}
      <button type="button" class="chart-button" data-canvas="{canvas_id}">Copy</button>
    </div>
  </div>
  <p>Per-operation latency distribution across all decrypted/processed operations (ms).</p>
  <div class="chart-wrap" style="height:320px">
    <canvas id="{canvas_id}"></canvas>
  </div>
</div>
"""


def generate_html(rows: list[dict], csv_path: str) -> str:
    elapsed_data = build_elapsed_chart(rows)
    memory_data = build_memory_chart(rows)
    lat_data = build_latency_chart(rows)
    call_data = build_call_counts_chart(rows)
    op_charts = build_operation_charts(rows)
    op_cards_html, op_js = build_operation_assets(op_charts)

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    source = os.path.basename(csv_path)
    row_count = len(rows)
    legend = build_legend_items()

    return HTML_TEMPLATE.format(
        timestamp=timestamp,
        row_count=row_count,
        source=source,
        legend=legend,
        typst_legend=js(build_typst_legend()),
        elapsed_data=js(elapsed_data),
        memory_data=js(memory_data),
        call_count_data=js(call_data),
        kv_latency_js=build_kv_latency_js(lat_data),
        op_cards=op_cards_html,
        op_js=op_js,
    )
