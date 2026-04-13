import { useEffect, useMemo, useRef, useState } from "react";
import "./App.css";

type MicroBenchmarkKind =
  | "AuthorizationBenchmark"
  | "KeyUnwrapBenchmark"
  | "DecryptionBenchmark";

type MicrobenchmarkRow = {
  benchmark: MicroBenchmarkKind;
  parameterName: string;
  parameterValue: number;
  scenario: string;
  meanMs: number;
  errorMs: number;
  stdDevMs: number;
};

type WarpOpStat = {
  op: string;
  requests: number;
  errors: number;
  avgLatencyMs: number;
};

type WarpRun = {
  fileName: string;
  label: string;
  instanceCount: number;
  scenario: string;
  totalRequests: number;
  totalObjects: number;
  totalErrors: number;
  totalBytes: number;
  durationMs: number;
  throughputMBps: number;
  opsPerSecond: number;
  fastestBps: number;
  medianBps: number;
  slowestBps: number;
  opBreakdown: WarpOpStat[];
};

type WarpResultFile = {
  total?: {
    total_requests?: number;
    total_objects?: number;
    total_errors?: number;
    total_bytes?: number;
    throughput?: {
      segmented?: {
        fastest_bps?: number;
        median_bps?: number;
        slowest_bps?: number;
      };
    };
    throughput_by_client?: Record<
      string,
      {
        measure_duration_millis?: number;
      }
    >;
  };
  by_op_type?: Record<
    string,
    {
      total_requests?: number;
      total_errors?: number;
      requests_by_client?: Record<
        string,
        Array<{
          single_sized_requests?: { dur_avg_millis?: number };
          multi_sized_requests?: {
            by_size?: Array<{
              avg_duration_millis?: number;
            }>;
          };
        }>
      >;
    }
  >;
};

type ChartDatum = { label: string; value: number };

const WARP_INSTANCE_COUNTS = [1, 2, 4, 8] as const;
const WARP_SCENARIOS = [
  "osws-encryption-cache",
  "osws-encryption-no-cache",
  "osws-no-encryption",
  "s3-direct",
] as const;
const WARP_FILES = WARP_INSTANCE_COUNTS.flatMap((instanceCount) =>
  WARP_SCENARIOS.map(
    (scenario) => `warp-${instanceCount}instances-${scenario}.json`,
  ),
);

type ScalingTrend =
  | "Improving"
  | "Regressing"
  | "Mixed"
  | "Stable"
  | "Insufficient Data";

type ScenarioScalingRow = {
  scenario: string;
  throughputByInstance: Record<number, number | null>;
  trend: ScalingTrend;
  changePct: number | null;
};

function parseCsvLine(line: string): string[] {
  const cells: string[] = [];
  let current = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i += 1) {
    const char = line[i];

    if (char === '"') {
      if (inQuotes && line[i + 1] === '"') {
        current += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (char === "," && !inQuotes) {
      cells.push(current);
      current = "";
      continue;
    }

    current += char;
  }

  cells.push(current);
  return cells;
}

function toHumanLabel(fileName: string): string {
  return fileName
    .replace(/^warp-\d+instances-/, "")
    .replace(".json", "")
    .split("-")
    .map((segment) => segment.toUpperCase())
    .join(" ");
}

function parseBenchmarkDotNetTimeToMs(raw: string): number {
  const cleaned = raw.replace(/"/g, "").trim();
  const match = cleaned.match(/^([0-9.,]+)\s*([a-zA-Z\u00B5]+)?$/);
  if (!match) {
    return 0;
  }

  const numeric = Number(match[1].replace(/,/g, ""));
  if (!Number.isFinite(numeric)) {
    return 0;
  }

  const unit = (match[2] ?? "ms").toLowerCase();

  if (unit === "ns") {
    return numeric / 1_000_000;
  }

  if (unit === "us" || unit === "\u00b5s" || unit === "μs") {
    return numeric / 1000;
  }

  if (unit === "s") {
    return numeric * 1000;
  }

  if (unit === "m" || unit === "ms") {
    return numeric;
  }

  return numeric;
}

function parseBdnReport(
  csvText: string,
  benchmark: MicroBenchmarkKind,
  parameterName: string,
): MicrobenchmarkRow[] {
  const lines = csvText.split(/\r?\n/).filter((line) => line.trim().length > 0);
  if (lines.length < 2) {
    return [];
  }

  const header = parseCsvLine(lines[0]);
  const indexByHeader = new Map(header.map((name, index) => [name, index]));

  const get = (cells: string[], column: string) =>
    cells[indexByHeader.get(column) ?? -1] ?? "";

  const normalizeMethod = (methodRaw: string) =>
    methodRaw.replace(/^'+|'+$/g, "").trim();

  return lines.slice(1).map((line) => {
    const cells = parseCsvLine(line);
    const parameterRaw = get(cells, parameterName);
    const parameterValue = Number(parameterRaw);
    const meanMs = parseBenchmarkDotNetTimeToMs(get(cells, "Mean"));
    const errorMs = parseBenchmarkDotNetTimeToMs(get(cells, "Error"));
    const stdDevMs = parseBenchmarkDotNetTimeToMs(get(cells, "StdDev"));
    const method = normalizeMethod(get(cells, "Method"));

    return {
      benchmark,
      parameterName,
      parameterValue,
      scenario: method || `${benchmark}-${parameterName}:${parameterRaw}`,
      meanMs,
      errorMs,
      stdDevMs,
    };
  });
}

function extractOpAverageLatency(opData: {
  requests_by_client?: Record<
    string,
    Array<{
      single_sized_requests?: { dur_avg_millis?: number };
      multi_sized_requests?: {
        by_size?: Array<{
          avg_duration_millis?: number;
        }>;
      };
    }>
  >;
}): number {
  const groups = Object.values(opData.requests_by_client ?? {});
  for (const group of groups) {
    for (const segment of group) {
      const singleAvg = segment.single_sized_requests?.dur_avg_millis;
      if (typeof singleAvg === "number" && singleAvg > 0) {
        return singleAvg;
      }

      const multiBySize = segment.multi_sized_requests?.by_size ?? [];
      for (const sizeRow of multiBySize) {
        if (
          typeof sizeRow.avg_duration_millis === "number" &&
          sizeRow.avg_duration_millis > 0
        ) {
          return sizeRow.avg_duration_millis;
        }
      }
    }
  }

  return 0;
}

function toWarpRun(fileName: string, json: WarpResultFile): WarpRun {
  const nameMatch = fileName.match(/^warp-(\d+)instances-(.+)\.json$/);
  const instanceCount = Number(nameMatch?.[1] ?? 1);
  const scenario = nameMatch?.[2] ?? fileName.replace(".json", "");
  const firstClientMetrics = Object.values(
    json.total?.throughput_by_client ?? {},
  )[0];
  const totalRequests = json.total?.total_requests ?? 0;
  const totalObjects = json.total?.total_objects ?? 0;
  const totalErrors = json.total?.total_errors ?? 0;
  const totalBytes = json.total?.total_bytes ?? 0;
  const durationMs = firstClientMetrics?.measure_duration_millis ?? 0;
  const durationSec = durationMs > 0 ? durationMs / 1000 : 0;
  const throughputMBps =
    durationSec > 0 ? totalBytes / durationSec / 1_000_000 : 0;
  const opsPerSecond = durationSec > 0 ? totalRequests / durationSec : 0;
  const fastestBps = json.total?.throughput?.segmented?.fastest_bps ?? 0;
  const medianBps = json.total?.throughput?.segmented?.median_bps ?? 0;
  const slowestBps = json.total?.throughput?.segmented?.slowest_bps ?? 0;

  const opBreakdown = Object.entries(json.by_op_type ?? {}).map(
    ([op, opStats]) => {
      const requests = opStats.total_requests ?? 0;
      const errors = opStats.total_errors ?? 0;
      const avgLatencyMs = extractOpAverageLatency(opStats);
      return { op, requests, errors, avgLatencyMs };
    },
  );

  return {
    fileName,
    label: `${instanceCount}x ${toHumanLabel(fileName)}`,
    instanceCount,
    scenario,
    totalRequests,
    totalObjects,
    totalErrors,
    totalBytes,
    durationMs,
    throughputMBps,
    opsPerSecond,
    fastestBps,
    medianBps,
    slowestBps,
    opBreakdown,
  };
}

function formatNumber(value: number, decimals = 2): string {
  return Number.isFinite(value)
    ? value.toLocaleString(undefined, {
        minimumFractionDigits: decimals,
        maximumFractionDigits: decimals,
      })
    : "0.00";
}

function formatChartValue(value: number, scaleMax: number): string {
  if (!Number.isFinite(value)) {
    return "0";
  }

  const absScale = Math.abs(scaleMax);

  if (absScale < 0.01) {
    return value.toLocaleString(undefined, {
      minimumFractionDigits: 4,
      maximumFractionDigits: 4,
    });
  }

  if (absScale < 0.1) {
    return value.toLocaleString(undefined, {
      minimumFractionDigits: 3,
      maximumFractionDigits: 3,
    });
  }

  if (absScale < 1) {
    return value.toLocaleString(undefined, {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });
  }

  if (absScale < 10) {
    return value.toLocaleString(undefined, {
      minimumFractionDigits: 1,
      maximumFractionDigits: 1,
    });
  }

  if (absScale < 1000) {
    return value.toLocaleString(undefined, {
      minimumFractionDigits: 0,
      maximumFractionDigits: 1,
    });
  }

  return value.toLocaleString(undefined, {
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  });
}

function toCsvFromChartData(data: ChartDatum[], yLabel: string): string {
  return [
    `Label,${yLabel}`,
    ...data.map((row) => `${row.label},${row.value}`),
  ].join("\n");
}

function toMicroShortLabel(row: MicrobenchmarkRow): string {
  if (row.benchmark === "AuthorizationBenchmark") {
    return `Auth-${row.parameterValue}`;
  }

  if (row.benchmark === "KeyUnwrapBenchmark") {
    return `Unwrap-${row.parameterValue}B`;
  }

  return `Decrypt-${row.parameterValue}`;
}

function toScenarioShortCode(scenario: string): string {
  const mapping: Record<string, string> = {
    "osws-encryption-cache": "OSWS-EC",
    "osws-encryption-no-cache": "OSWS-ENC",
    "osws-no-encryption": "OSWS-NE",
    "s3-direct": "S3",
  };

  return mapping[scenario] ?? scenario.toUpperCase();
}

function classifyScalingTrend(values: number[]): ScalingTrend {
  if (values.length < 2) {
    return "Insufficient Data";
  }

  const deltas = values.slice(1).map((current, index) => {
    const previous = values[index];
    if (previous <= 0) {
      return 0;
    }
    return ((current - previous) / previous) * 100;
  });

  const tolerancePct = 2;
  const hasGrowth = deltas.some((delta) => delta > tolerancePct);
  const hasDrop = deltas.some((delta) => delta < -tolerancePct);

  if (!hasGrowth && !hasDrop) {
    return "Stable";
  }

  if (hasGrowth && !hasDrop) {
    return "Improving";
  }

  if (hasDrop && !hasGrowth) {
    return "Regressing";
  }

  return "Mixed";
}

function toSignedPercent(value: number): string {
  const sign = value > 0 ? "+" : "";
  return `${sign}${formatNumber(value, 1)}%`;
}

function maxFinite(values: number[]): number {
  return Math.max(...values.filter(Number.isFinite), 0);
}

async function copyChartSvgAsPng(svgEl: SVGSVGElement | null): Promise<void> {
  if (!svgEl) {
    throw new Error("Chart element not found");
  }

  const serializer = new XMLSerializer();
  const exportSvg = svgEl.cloneNode(true) as SVGSVGElement;
  const styleNode = document.createElementNS(
    "http://www.w3.org/2000/svg",
    "style",
  );
  styleNode.textContent = `
    .axis-line { stroke: #6483a3; stroke-width: 1; }
    .grid-line { stroke: #e1e9f3; stroke-width: 1; }
    .axis-label {
      fill: #48647f;
      font-size: 11px;
      font-weight: 500;
      font-family: "Space Grotesk", "Segoe UI", sans-serif;
    }
    .chart-meta {
      fill: #23507a;
      font-size: 12px;
      font-weight: 700;
      font-family: "Space Grotesk", "Segoe UI", sans-serif;
    }
    .chart-value-label {
      fill: #173a5c;
      font-size: 10px;
      font-weight: 700;
      font-family: "Space Grotesk", "Segoe UI", sans-serif;
    }
    .chart-bar { fill: #2f80c7; }
  `;
  exportSvg.insertBefore(styleNode, exportSvg.firstChild);
  exportSvg.setAttribute("xmlns", "http://www.w3.org/2000/svg");

  const svgText = serializer.serializeToString(exportSvg);
  const svgBlob = new Blob([svgText], { type: "image/svg+xml;charset=utf-8" });
  const svgUrl = URL.createObjectURL(svgBlob);

  try {
    const img = await new Promise<HTMLImageElement>((resolve, reject) => {
      const image = new Image();
      image.onload = () => resolve(image);
      image.onerror = () => reject(new Error("Could not render chart image"));
      image.src = svgUrl;
    });

    const widthAttr = Number(svgEl.getAttribute("width"));
    const heightAttr = Number(svgEl.getAttribute("height"));
    const width =
      Number.isFinite(widthAttr) && widthAttr > 0
        ? widthAttr
        : Number(svgEl.getAttribute("data-export-width") ?? 980);
    const height =
      Number.isFinite(heightAttr) && heightAttr > 0
        ? heightAttr
        : Number(svgEl.getAttribute("data-export-height") ?? 360);
    const canvas = document.createElement("canvas");
    canvas.width = width;
    canvas.height = height;
    const ctx = canvas.getContext("2d");
    if (!ctx) {
      throw new Error("Canvas context is unavailable");
    }

    ctx.fillStyle = "#ffffff";
    ctx.fillRect(0, 0, width, height);
    ctx.drawImage(img, 0, 0, width, height);

    const blob = await new Promise<Blob | null>((resolve) =>
      canvas.toBlob(resolve, "image/png"),
    );

    if (!blob) {
      throw new Error("Failed to encode chart image");
    }

    if (
      "ClipboardItem" in window &&
      navigator.clipboard &&
      "write" in navigator.clipboard
    ) {
      const clipboardItem = new ClipboardItem({ "image/png": blob });
      await navigator.clipboard.write([clipboardItem]);
      return;
    }

    await navigator.clipboard.writeText(svgText);
  } finally {
    URL.revokeObjectURL(svgUrl);
  }
}

type ComparisonBarChartProps = {
  title: string;
  subtitle: string;
  yLabel: string;
  xLabel: string;
  data: ChartDatum[];
  yMax: number;
  svgRef: React.RefObject<SVGSVGElement | null>;
  onCopyImage: () => void;
  onCopyData: () => void;
};

function ComparisonBarChart({
  title,
  subtitle,
  yLabel,
  xLabel,
  data,
  yMax,
  svgRef,
  onCopyImage,
  onCopyData,
}: ComparisonBarChartProps) {
  const width = 980;
  const height = 360;
  const chartLeft = 64;
  const chartBottom = 66;
  const chartTop = 26;
  const chartRight = 26;
  const innerWidth = width - chartLeft - chartRight;
  const innerHeight = height - chartTop - chartBottom;
  const safeMax = yMax > 0 ? yMax : 1;
  const step = innerWidth / Math.max(1, data.length);
  const barWidth = Math.max(14, step * 0.64);
  const ticks = 5;

  return (
    <article className="chart-card panel-soft">
      <div className="panel-topline">
        <div>
          <h3>{title}</h3>
          <p>{subtitle}</p>
        </div>
        <div className="button-group">
          <button type="button" onClick={onCopyImage}>
            Copy Chart PNG
          </button>
          <button type="button" className="ghost-btn" onClick={onCopyData}>
            Copy CSV
          </button>
        </div>
      </div>

      <div className="table-wrap">
        <svg
          ref={svgRef}
          width="100%"
          height={height}
          data-export-width={width}
          data-export-height={height}
          viewBox={`0 0 ${width} ${height}`}
          aria-label={title}
        >
          <rect
            x={0}
            y={0}
            width={width}
            height={height}
            fill="#ffffff"
            rx={14}
          />

          {Array.from({ length: ticks + 1 }).map((_, index) => {
            const y = chartTop + (innerHeight / ticks) * index;
            const tickValue = safeMax - (safeMax / ticks) * index;
            return (
              <g key={`tick-${index}`}>
                <line
                  x1={chartLeft}
                  y1={y}
                  x2={width - chartRight}
                  y2={y}
                  className="grid-line"
                />
                <text
                  x={chartLeft - 10}
                  y={y + 4}
                  className="axis-label"
                  textAnchor="end"
                >
                  {formatChartValue(tickValue, safeMax)}
                </text>
              </g>
            );
          })}

          <line
            x1={chartLeft}
            y1={chartTop + innerHeight}
            x2={width - chartRight}
            y2={chartTop + innerHeight}
            className="axis-line"
          />

          {data.map((point, index) => {
            const ratio = point.value / safeMax;
            const x = chartLeft + step * index + (step - barWidth) / 2;
            const barHeight = Math.max(2, ratio * innerHeight);
            const y = chartTop + innerHeight - barHeight;
            const valueY = Math.max(chartTop + 10, y - 6);
            return (
              <g key={point.label}>
                <rect
                  x={x}
                  y={y}
                  width={barWidth}
                  height={barHeight}
                  rx={6}
                  className="chart-bar"
                >
                  <title>
                    {`${point.label}: ${formatChartValue(point.value, safeMax)} ${yLabel}`}
                  </title>
                </rect>
                <text
                  x={x + barWidth / 2}
                  y={valueY}
                  className="chart-value-label"
                  textAnchor="middle"
                >
                  {formatChartValue(point.value, safeMax)}
                </text>
                <text
                  x={x + barWidth / 2}
                  y={chartTop + innerHeight + 18}
                  className="axis-label"
                  textAnchor="middle"
                >
                  {point.label}
                </text>
              </g>
            );
          })}

          <text
            x={chartLeft + 4}
            y={16}
            className="chart-meta"
            textAnchor="start"
          >
            {yLabel}
          </text>

          <text
            x={chartLeft + innerWidth / 2}
            y={height - 10}
            className="chart-meta"
            textAnchor="middle"
          >
            {xLabel}
          </text>
        </svg>
      </div>
    </article>
  );
}

function App() {
  const [microRows, setMicroRows] = useState<MicrobenchmarkRow[]>([]);
  const [warpRuns, setWarpRuns] = useState<WarpRun[]>([]);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [axisMode, setAxisMode] = useState<"shared" | "auto">("auto");
  const [copyFeedback, setCopyFeedback] = useState<string | null>(null);
  const warpOswsEcSvgRef = useRef<SVGSVGElement>(null);
  const warpOswsEncSvgRef = useRef<SVGSVGElement>(null);
  const warpOswsNeSvgRef = useRef<SVGSVGElement>(null);
  const warpS3SvgRef = useRef<SVGSVGElement>(null);
  const warpScalingSvgRef = useRef<SVGSVGElement>(null);
  const microAuthSvgRef = useRef<SVGSVGElement>(null);
  const microUnwrapSvgRef = useRef<SVGSVGElement>(null);
  const microDecryptSvgRef = useRef<SVGSVGElement>(null);

  useEffect(() => {
    let mounted = true;

    const loadData = async () => {
      try {
        const warpPromises = WARP_FILES.map(async (fileName) => {
          const res = await fetch(`/data/warp/${fileName}`);
          if (!res.ok) {
            return null;
          }

          const payload = (await res.json()) as WarpResultFile;
          return toWarpRun(fileName, payload);
        });

        const microPromises = [
          fetch("/data/micro/OSWS.Performance.Benchmarks.Measurements.AuthorizationBenchmark-report.csv").then((res) => {
            if (!res.ok) {
              throw new Error("Failed to load authorization report");
            }
            return res.text();
          }),
          fetch("/data/micro/OSWS.Performance.Benchmarks.Measurements.KeyUnwrapBenchmark-report.csv").then((res) => {
            if (!res.ok) {
              throw new Error("Failed to load key unwrap report");
            }
            return res.text();
          }),
          fetch("/data/micro/OSWS.Performance.Benchmarks.Measurements.DecryptionBenchmark-report.csv").then((res) => {
            if (!res.ok) {
              throw new Error("Failed to load decryption report");
            }
            return res.text();
          }),
        ];

        const [loadedWarp, authorizationCsv, unwrapCsv, decryptCsv] =
          await Promise.all([Promise.all(warpPromises), ...microPromises]);

        const warp = loadedWarp.filter((run): run is WarpRun => run !== null);

        if (warp.length === 0) {
          throw new Error(
            "No WARP JSON files could be loaded from public/data/warp",
          );
        }

        if (!mounted) {
          return;
        }

        const micro = [
          ...parseBdnReport(
            authorizationCsv,
            "AuthorizationBenchmark",
            "RoleCount",
          ),
          ...parseBdnReport(unwrapCsv, "KeyUnwrapBenchmark", "DekSizeBytes"),
          ...parseBdnReport(decryptCsv, "DecryptionBenchmark", "RowCount"),
        ].sort((a, b) => {
          const kindOrder: Record<MicroBenchmarkKind, number> = {
            AuthorizationBenchmark: 0,
            KeyUnwrapBenchmark: 1,
            DecryptionBenchmark: 2,
          };
          const byKind = kindOrder[a.benchmark] - kindOrder[b.benchmark];
          if (byKind !== 0) {
            return byKind;
          }
          return a.parameterValue - b.parameterValue;
        });

        setMicroRows(micro);
        setWarpRuns(warp);
      } catch (error) {
        if (!mounted) {
          return;
        }
        setLoadError(
          error instanceof Error
            ? error.message
            : "Failed to load benchmark data",
        );
      }
    };

    void loadData();

    return () => {
      mounted = false;
    };
  }, []);

  const topThroughput = useMemo(
    () => Math.max(...warpRuns.map((run) => run.throughputMBps), 0),
    [warpRuns],
  );

  const totalWarpRequests = useMemo(
    () => warpRuns.reduce((sum, run) => sum + run.totalRequests, 0),
    [warpRuns],
  );

  const totalWarpErrors = useMemo(
    () => warpRuns.reduce((sum, run) => sum + run.totalErrors, 0),
    [warpRuns],
  );

  const microAvgMean = useMemo(() => {
    if (microRows.length === 0) {
      return 0;
    }
    const total = microRows.reduce((sum, row) => sum + row.meanMs, 0);
    return total / microRows.length;
  }, [microRows]);

  const microMaxStdDev = useMemo(() => {
    if (microRows.length === 0) {
      return 0;
    }
    return Math.max(...microRows.map((row) => row.stdDevMs));
  }, [microRows]);

  const warpScenarioCharts = useMemo(
    () =>
      WARP_SCENARIOS.map((scenario) => {
        const data = warpRuns
          .filter((run) => run.scenario === scenario)
          .sort((a, b) => a.instanceCount - b.instanceCount)
          .map((run) => ({
            label: `${run.instanceCount}x`,
            value: run.throughputMBps,
          }));

        return { scenario, data };
      }).filter((entry) => entry.data.length > 0),
    [warpRuns],
  );

  const microChartsByBenchmark = useMemo(() => {
    const order: MicroBenchmarkKind[] = [
      "AuthorizationBenchmark",
      "KeyUnwrapBenchmark",
      "DecryptionBenchmark",
    ];

    return order
      .map((benchmark) => {
        const rows = microRows.filter((row) => row.benchmark === benchmark);
        const data = rows.map((row) => ({
          label: toMicroShortLabel(row),
          value: row.meanMs,
        }));

        return {
          benchmark,
          rows,
          data,
          runCount: rows.length,
        };
      })
      .filter((item) => item.data.length > 0);
  }, [microRows]);

  const warpScalingChartData = useMemo<ChartDatum[]>(() => {
    const oswsRuns = warpRuns.filter((run) => run.scenario.includes("osws"));
    const byInstance = new Map<number, number[]>();

    for (const run of oswsRuns) {
      const bucket = byInstance.get(run.instanceCount) ?? [];
      bucket.push(run.throughputMBps);
      byInstance.set(run.instanceCount, bucket);
    }

    return Array.from(byInstance.entries())
      .sort((a, b) => a[0] - b[0])
      .map(([instanceCount, values]) => ({
        label: `${instanceCount}x`,
        value:
          values.reduce((sum, value) => sum + value, 0) /
          Math.max(values.length, 1),
      }));
  }, [warpRuns]);

  const hasScalingComparison = useMemo(
    () => new Set(warpScalingChartData.map((item) => item.label)).size > 1,
    [warpScalingChartData],
  );

  const scalingOverview = useMemo(() => {
    const points = warpScalingChartData
      .map((item) => ({
        instanceCount: Number(item.label.replace(/x$/, "")),
        throughput: item.value,
      }))
      .filter((item) => Number.isFinite(item.instanceCount))
      .sort((a, b) => a.instanceCount - b.instanceCount);

    const values = points.map((point) => point.throughput);
    const trend = classifyScalingTrend(values);

    if (points.length < 2) {
      return {
        trend,
        changePct: null as number | null,
        rangeLabel:
          points.length === 1
            ? `${points[0].instanceCount}x only`
            : "No OSWS data",
      };
    }

    const first = points[0];
    const last = points[points.length - 1];
    const changePct =
      first.throughput > 0
        ? ((last.throughput - first.throughput) / first.throughput) * 100
        : null;

    return {
      trend,
      changePct,
      rangeLabel: `${first.instanceCount}x -> ${last.instanceCount}x`,
    };
  }, [warpScalingChartData]);

  const scenarioScalingRows = useMemo<ScenarioScalingRow[]>(() => {
    const oswsRuns = warpRuns.filter((run) => run.scenario.startsWith("osws-"));
    const byScenario = new Map<string, Map<number, number[]>>();

    for (const run of oswsRuns) {
      const scenarioBucket =
        byScenario.get(run.scenario) ?? new Map<number, number[]>();
      const values = scenarioBucket.get(run.instanceCount) ?? [];
      values.push(run.throughputMBps);
      scenarioBucket.set(run.instanceCount, values);
      byScenario.set(run.scenario, scenarioBucket);
    }

    return Array.from(byScenario.entries())
      .map(([scenario, byInstance]) => {
        const throughputByInstance: Record<number, number | null> =
          Object.fromEntries(
            WARP_INSTANCE_COUNTS.map((instanceCount) => {
              const values = byInstance.get(instanceCount) ?? [];
              if (values.length === 0) {
                return [instanceCount, null];
              }
              const average =
                values.reduce((sum, value) => sum + value, 0) / values.length;
              return [instanceCount, average];
            }),
          );

        const orderedValues = WARP_INSTANCE_COUNTS.map(
          (instanceCount) => throughputByInstance[instanceCount],
        ).filter((value): value is number => value !== null);
        const trend = classifyScalingTrend(orderedValues);

        const firstValue = throughputByInstance[WARP_INSTANCE_COUNTS[0]];
        const lastInstance = [...WARP_INSTANCE_COUNTS]
          .reverse()
          .find(
            (instanceCount) => throughputByInstance[instanceCount] !== null,
          );
        const lastValue = lastInstance
          ? throughputByInstance[lastInstance]
          : null;
        const changePct =
          typeof firstValue === "number" &&
          typeof lastValue === "number" &&
          firstValue > 0
            ? ((lastValue - firstValue) / firstValue) * 100
            : null;

        return {
          scenario,
          throughputByInstance,
          trend,
          changePct,
        };
      })
      .sort((a, b) => a.scenario.localeCompare(b.scenario));
  }, [warpRuns]);

  const warpSharedYMax = useMemo(
    () =>
      maxFinite([
        ...warpScenarioCharts.flatMap((chart) =>
          chart.data.map((d) => d.value),
        ),
        ...warpScalingChartData.map((d) => d.value),
      ]),
    [warpScenarioCharts, warpScalingChartData],
  );

  const microSharedYMax = useMemo(
    () =>
      maxFinite(
        microChartsByBenchmark.flatMap((chart) =>
          chart.data.map((d) => d.value),
        ),
      ),
    [microChartsByBenchmark],
  );

  const warpYMax =
    axisMode === "shared"
      ? warpSharedYMax
      : maxFinite(
          warpScenarioCharts.flatMap((chart) => chart.data.map((d) => d.value)),
        );
  const warpScalingYMax =
    axisMode === "shared"
      ? warpSharedYMax
      : maxFinite(warpScalingChartData.map((d) => d.value));
  const microYMaxByBenchmark = useMemo(
    () =>
      new Map(
        microChartsByBenchmark.map((chart) => [
          chart.benchmark,
          axisMode === "shared"
            ? microSharedYMax
            : maxFinite(chart.data.map((d) => d.value)),
        ]),
      ),
    [axisMode, microChartsByBenchmark, microSharedYMax],
  );

  const setFeedback = (message: string) => {
    setCopyFeedback(message);
    window.setTimeout(() => setCopyFeedback(null), 2200);
  };

  const onCopyChart = async (
    svgRef: React.RefObject<SVGSVGElement | null>,
    title: string,
  ) => {
    try {
      await copyChartSvgAsPng(svgRef.current);
      setFeedback(`${title} copied to clipboard`);
    } catch (error) {
      setFeedback(
        error instanceof Error ? error.message : "Failed to copy chart",
      );
    }
  };

  const onCopyChartData = async (
    data: ChartDatum[],
    yLabel: string,
    title: string,
  ) => {
    try {
      await navigator.clipboard.writeText(toCsvFromChartData(data, yLabel));
      setFeedback(`${title} CSV copied to clipboard`);
    } catch {
      setFeedback("Failed to copy chart data");
    }
  };

  const copyText = async (
    text: string,
    successMessage: string,
    errorMessage: string,
  ) => {
    try {
      await navigator.clipboard.writeText(text);
      setFeedback(successMessage);
    } catch {
      setFeedback(errorMessage);
    }
  };

  const kpiCsv = useMemo(
    () =>
      [
        "Metric,Value",
        `Total WARP Runs,${warpRuns.length}`,
        `Total WARP Requests,${formatNumber(totalWarpRequests, 0)}`,
        `Top Throughput MB/s,${formatNumber(topThroughput)}`,
        `Microbench Cases,${microRows.length}`,
        `Micro Mean Average ms,${formatNumber(microAvgMean)}`,
        `Micro Max StdDev ms,${formatNumber(microMaxStdDev)}`,
        `Total WARP Errors,${formatNumber(totalWarpErrors, 0)}`,
      ].join("\n"),
    [
      microAvgMean,
      microMaxStdDev,
      microRows.length,
      topThroughput,
      totalWarpErrors,
      totalWarpRequests,
      warpRuns.length,
    ],
  );

  const warpTableCsv = useMemo(
    () =>
      [
        "Run,InstanceCount,Scenario,DurationMs,Requests,Objects,Errors,TotalMB,ThroughputMBps,OpsPerSecond,FastestBps,MedianBps,SlowestBps",
        ...warpRuns.map((run) =>
          [
            run.label,
            run.instanceCount,
            run.scenario,
            run.durationMs,
            run.totalRequests,
            run.totalObjects,
            run.totalErrors,
            run.totalBytes / 1_000_000,
            run.throughputMBps,
            run.opsPerSecond,
            run.fastestBps,
            run.medianBps,
            run.slowestBps,
          ].join(","),
        ),
      ].join("\n"),
    [warpRuns],
  );

  const microTableCsv = useMemo(
    () =>
      [
        "Benchmark,Parameter,Value,Scenario,MeanMs,ErrorMs,StdDevMs",
        ...microRows.map((row) =>
          [
            row.benchmark,
            row.parameterName,
            row.parameterValue,
            row.scenario,
            row.meanMs,
            row.errorMs,
            row.stdDevMs,
          ]
            .map((cell) => `"${String(cell).replace(/"/g, '""')}"`)
            .join(","),
        ),
      ].join("\n"),
    [microRows],
  );

  const microRowsByBenchmark = useMemo(() => {
    const order: MicroBenchmarkKind[] = [
      "AuthorizationBenchmark",
      "KeyUnwrapBenchmark",
      "DecryptionBenchmark",
    ];

    return order.map((benchmark) => ({
      benchmark,
      rows: microRows.filter((row) => row.benchmark === benchmark),
    }));
  }, [microRows]);

  const warpOpRows = useMemo(
    () =>
      warpRuns.flatMap((run) =>
        run.opBreakdown.map((item) => ({
          runLabel: run.label,
          scenario: run.scenario,
          instanceCount: run.instanceCount,
          op: item.op,
          requests: item.requests,
          errors: item.errors,
          avgLatencyMs: item.avgLatencyMs,
        })),
      ),
    [warpRuns],
  );

  const warpRefByScenario: Record<
    string,
    React.RefObject<SVGSVGElement | null>
  > = {
    "osws-encryption-cache": warpOswsEcSvgRef,
    "osws-encryption-no-cache": warpOswsEncSvgRef,
    "osws-no-encryption": warpOswsNeSvgRef,
    "s3-direct": warpS3SvgRef,
  };

  const microRefByBenchmark: Record<
    MicroBenchmarkKind,
    React.RefObject<SVGSVGElement | null>
  > = {
    AuthorizationBenchmark: microAuthSvgRef,
    KeyUnwrapBenchmark: microUnwrapSvgRef,
    DecryptionBenchmark: microDecryptSvgRef,
  };

  return (
    <main className="dashboard-shell">
      <header className="hero">
        <div>
          <p className="eyebrow">OSWS Benchmark Frontend</p>
          <h1>WARP + Microbenchmark Results</h1>
          <p className="hero-copy">
            BenchmarkDotNet and WARP comparison workspace for thesis reporting.
          </p>
        </div>

        <div className="control-box">
          <p className="toggle-title">Chart Axis Mode</p>
          <div
            className="axis-toggle"
            role="group"
            aria-label="Chart axis mode"
          >
            <button
              type="button"
              className={axisMode === "shared" ? "active-toggle" : "ghost-btn"}
              onClick={() => setAxisMode("shared")}
            >
              Shared
            </button>
            <button
              type="button"
              className={axisMode === "auto" ? "active-toggle" : "ghost-btn"}
              onClick={() => setAxisMode("auto")}
            >
              Auto
            </button>
          </div>
          <p className="toggle-hint">
            Shared compares charts within each metric group. Auto scales each
            chart independently.
          </p>
          {copyFeedback ? (
            <p className="copy-feedback">{copyFeedback}</p>
          ) : null}
        </div>
      </header>

      {loadError ? <p className="error-text">{loadError}</p> : null}

      <section className="kpi-grid">
        <article className="kpi-card kpi-copy-card">
          <p>Export</p>
          <button
            type="button"
            onClick={() =>
              void copyText(
                kpiCsv,
                "KPI CSV copied to clipboard",
                "Failed to copy KPI CSV",
              )
            }
          >
            Copy KPI CSV
          </button>
        </article>
        <article className="kpi-card">
          <p>Total WARP Runs</p>
          <strong>{warpRuns.length}</strong>
        </article>
        <article className="kpi-card">
          <p>Total WARP Requests</p>
          <strong>{formatNumber(totalWarpRequests, 0)}</strong>
        </article>
        <article className="kpi-card">
          <p>Top Throughput</p>
          <strong>{formatNumber(topThroughput)} MB/s</strong>
        </article>
        <article className="kpi-card">
          <p>Microbench Cases</p>
          <strong>{microRows.length}</strong>
        </article>
        <article className="kpi-card">
          <p>Micro Mean Average</p>
          <strong>{formatNumber(microAvgMean)} ms</strong>
        </article>
        <article className="kpi-card">
          <p>Micro Max StdDev</p>
          <strong>{formatNumber(microMaxStdDev)} ms</strong>
        </article>
        <article className="kpi-card">
          <p>Total WARP Errors</p>
          <strong>{formatNumber(totalWarpErrors, 0)}</strong>
        </article>
        <article className="kpi-card">
          <p>OSWS Scaling Status</p>
          <strong>{scalingOverview.trend}</strong>
          <div className="kpi-subtext">
            <span>{scalingOverview.rangeLabel}</span>
            <span>
              {scalingOverview.changePct === null
                ? "n/a"
                : toSignedPercent(scalingOverview.changePct)}
            </span>
          </div>
        </article>
      </section>

      <section className="chart-grid">
        <article className="panel-soft split-chart-panel">
          <div className="panel-topline">
            <div>
              <h3>WARP Throughput by Scenario</h3>
              <p>
                Split view for readability. Each chart shows 1x, 2x, 4x, 8x only
                ({axisMode === "shared" ? "Shared y-axis" : "Auto y-axis"}).
              </p>
            </div>
          </div>
          <div className="mini-chart-grid">
            {warpScenarioCharts.map(({ scenario, data }) => {
              const ref = warpRefByScenario[scenario] ?? warpOswsEcSvgRef;
              const chartYMax =
                axisMode === "auto"
                  ? maxFinite(data.map((d) => d.value))
                  : warpYMax;
              return (
                <ComparisonBarChart
                  key={scenario}
                  title={`Scenario: ${toScenarioShortCode(scenario)}`}
                  subtitle={scenario}
                  yLabel="MB/s"
                  xLabel="Instance Count"
                  data={data}
                  yMax={chartYMax}
                  svgRef={ref}
                  onCopyImage={() =>
                    void onCopyChart(
                      ref,
                      `${toScenarioShortCode(scenario)} chart`,
                    )
                  }
                  onCopyData={() =>
                    void onCopyChartData(
                      data,
                      "throughput_mb_per_sec",
                      `${toScenarioShortCode(scenario)} chart data`,
                    )
                  }
                />
              );
            })}
          </div>
        </article>
        <ComparisonBarChart
          title="OSWS Instance Scaling (Average Throughput)"
          subtitle={`Average MB/s by instance count across OSWS scenarios (${axisMode === "shared" ? "Shared y-axis" : "Auto y-axis"})`}
          yLabel="MB/s"
          xLabel="Instance Count"
          data={warpScalingChartData}
          yMax={warpScalingYMax}
          svgRef={warpScalingSvgRef}
          onCopyImage={() =>
            void onCopyChart(warpScalingSvgRef, "WARP scaling chart")
          }
          onCopyData={() =>
            void onCopyChartData(
              warpScalingChartData,
              "avg_throughput_mb_per_sec",
              "WARP scaling chart data",
            )
          }
        />
        {!hasScalingComparison ? (
          <p className="chart-note">
            Only one OSWS instance count is currently available in loaded WARP
            files. Add files such as <strong>warp-2instances-*.json</strong> and{" "}
            <strong>warp-4instances-*.json</strong>
            under <strong>public/data/warp</strong> to visualize throughput
            scaling.
          </p>
        ) : null}
        <article className="panel-soft split-chart-panel">
          <div className="panel-topline">
            <div>
              <h3>Microbenchmark Mean Latency</h3>
              <p>
                Split by benchmark for readability (
                {axisMode === "shared" ? "Shared y-axis" : "Auto y-axis"}).
              </p>
            </div>
          </div>
          <div className="mini-chart-grid">
            {microChartsByBenchmark.map(({ benchmark, data, runCount }) => {
              const ref = microRefByBenchmark[benchmark];
              const title = benchmark.replace("Benchmark", "");
              const yMax =
                microYMaxByBenchmark.get(benchmark) ?? microSharedYMax;
              return (
                <ComparisonBarChart
                  key={benchmark}
                  title={`Micro: ${title}`}
                  subtitle={`Certainty: n=${runCount} runs`}
                  yLabel="ms"
                  xLabel="Benchmark Case"
                  data={data}
                  yMax={yMax}
                  svgRef={ref}
                  onCopyImage={() =>
                    void onCopyChart(ref, `${title} micro chart`)
                  }
                  onCopyData={() =>
                    void onCopyChartData(
                      data,
                      "mean_latency_ms",
                      `${title} micro chart data`,
                    )
                  }
                />
              );
            })}
          </div>
        </article>
      </section>

      <section className="panel">
        <div className="panel-head">
          <h2>WARP Run Summary</h2>
          <button
            type="button"
            className="ghost-btn"
            onClick={() =>
              void copyText(
                warpTableCsv,
                "WARP table CSV copied to clipboard",
                "Failed to copy WARP table CSV",
              )
            }
          >
            Copy WARP Table CSV
          </button>
        </div>
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Run</th>
                <th>Scenario</th>
                <th>Instances</th>
                <th>Duration</th>
                <th>Requests</th>
                <th>Objects</th>
                <th>Total Errors</th>
                <th>Total MB</th>
                <th>Throughput</th>
                <th>Ops/s</th>
                <th>Segmented Throughput</th>
              </tr>
            </thead>
            <tbody>
              {warpRuns.map((run) => (
                <tr key={run.fileName}>
                  <td>{run.label}</td>
                  <td>{run.scenario}</td>
                  <td>{run.instanceCount}x</td>
                  <td>{formatNumber(run.durationMs, 0)} ms</td>
                  <td>{formatNumber(run.totalRequests, 0)}</td>
                  <td>{formatNumber(run.totalObjects, 0)}</td>
                  <td>{formatNumber(run.totalErrors, 0)}</td>
                  <td>{formatNumber(run.totalBytes / 1_000_000)} MB</td>
                  <td>
                    <span>{formatNumber(run.throughputMBps)} MB/s</span>
                    <div className="bar-track" role="presentation">
                      <div
                        className="bar-fill"
                        style={{
                          width: `${Math.max(
                            6,
                            topThroughput > 0
                              ? (run.throughputMBps / topThroughput) * 100
                              : 0,
                          )}%`,
                        }}
                      />
                    </div>
                  </td>
                  <td>{formatNumber(run.opsPerSecond)}</td>
                  <td>
                    <strong>Fast:</strong> {formatNumber(run.fastestBps)} bps
                    <br />
                    <strong>Median:</strong> {formatNumber(run.medianBps)} bps
                    <br />
                    <strong>Slow:</strong> {formatNumber(run.slowestBps)} bps
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section className="panel">
        <div className="panel-head">
          <h2>WARP Operation Breakdown</h2>
        </div>
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Run</th>
                <th>Scenario</th>
                <th>Instances</th>
                <th>Operation</th>
                <th>Requests</th>
                <th>Errors</th>
                <th>Avg Latency</th>
              </tr>
            </thead>
            <tbody>
              {warpOpRows.map((row) => (
                <tr key={`${row.runLabel}-${row.op}`}>
                  <td>{row.runLabel}</td>
                  <td>{row.scenario}</td>
                  <td>{row.instanceCount}x</td>
                  <td>{row.op}</td>
                  <td>{formatNumber(row.requests, 0)}</td>
                  <td>{formatNumber(row.errors, 0)}</td>
                  <td>{formatNumber(row.avgLatencyMs)} ms</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section className="panel">
        <div className="panel-head">
          <h2>OSWS Scaling Status by Scenario</h2>
        </div>
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Scenario</th>
                <th>1x MB/s</th>
                <th>2x MB/s</th>
                <th>4x MB/s</th>
                <th>8x MB/s</th>
                <th>Trend</th>
                <th>1x -&gt; 8x</th>
              </tr>
            </thead>
            <tbody>
              {scenarioScalingRows.map((row) => (
                <tr key={row.scenario}>
                  <td>{row.scenario}</td>
                  {WARP_INSTANCE_COUNTS.map((instanceCount) => {
                    const value = row.throughputByInstance[instanceCount];
                    return (
                      <td key={`${row.scenario}-${instanceCount}`}>
                        {value === null ? "-" : formatNumber(value)}
                      </td>
                    );
                  })}
                  <td>
                    <span
                      className={`trend-pill trend-${row.trend.toLowerCase().replace(/\s+/g, "-")}`}
                    >
                      {row.trend}
                    </span>
                  </td>
                  <td>
                    {row.changePct === null
                      ? "n/a"
                      : toSignedPercent(row.changePct)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section className="panel">
        <div className="panel-head">
          <h2>Microbenchmark Results (BenchmarkDotNet)</h2>
          <button
            type="button"
            className="ghost-btn"
            onClick={() =>
              void copyText(
                microTableCsv,
                "Microbenchmark table CSV copied to clipboard",
                "Failed to copy microbenchmark table CSV",
              )
            }
          >
            Copy Micro Table CSV
          </button>
        </div>
        <div className="split-table-stack">
          {microRowsByBenchmark.map(({ benchmark, rows }) => (
            <article key={benchmark} className="inner-table-panel">
              <h3>{benchmark}</h3>
              <div className="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Parameter</th>
                      <th>Value</th>
                      <th>Scenario</th>
                      <th>Mean</th>
                      <th>Error</th>
                      <th>StdDev</th>
                    </tr>
                  </thead>
                  <tbody>
                    {rows.map((row) => (
                      <tr
                        key={`${row.benchmark}-${row.scenario}-${row.parameterValue}`}
                      >
                        <td>{row.parameterName}</td>
                        <td>{formatNumber(row.parameterValue, 0)}</td>
                        <td>{row.scenario}</td>
                        <td>{formatNumber(row.meanMs)} ms</td>
                        <td>{formatNumber(row.errorMs)} ms</td>
                        <td>{formatNumber(row.stdDevMs)} ms</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </article>
          ))}
        </div>
      </section>
    </main>
  );
}

export default App;
