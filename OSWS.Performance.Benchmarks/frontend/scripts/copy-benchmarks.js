import { copyFileSync, existsSync, mkdirSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const microSourceDir = join(
  __dirname,
  "../../BenchmarkDotNet.Artifacts/results",
);
const warpSourceDir = join(__dirname, "../../warp-results");
const microDestDir = join(__dirname, "../public/data/micro");
const warpDestDir = join(__dirname, "../public/data/warp");

const microFiles = [
  "OSWS.Performance.Benchmarks.Measurements.AuthorizationBenchmark-report.csv",
  "OSWS.Performance.Benchmarks.Measurements.PermissionServiceBenchmark-report.csv",
  "OSWS.Performance.Benchmarks.Measurements.KeyUnwrapBenchmark-report.csv",
  "OSWS.Performance.Benchmarks.Measurements.DecryptionBenchmark-report.csv",
];

const warpInstanceCounts = [1, 2, 4, 8];
const warpScenarios = [
  "osws-encryption-cache",
  "osws-encryption-no-cache",
  "osws-no-encryption",
  "s3-direct",
];
const warpFiles = warpInstanceCounts.flatMap((instanceCount) =>
  warpScenarios.map(
    (scenario) => `warp-${instanceCount}instances-${scenario}.json`,
  ),
);

mkdirSync(microDestDir, { recursive: true });
mkdirSync(warpDestDir, { recursive: true });

for (const fileName of microFiles) {
  const sourcePath = join(microSourceDir, fileName);
  const targetPath = join(microDestDir, fileName);

  if (!existsSync(sourcePath)) {
    throw new Error(`Missing benchmark CSV: ${sourcePath}`);
  }

  copyFileSync(sourcePath, targetPath);
}

for (const fileName of warpFiles) {
  const sourcePath = join(warpSourceDir, fileName);
  const targetPath = join(warpDestDir, fileName);

  if (!existsSync(sourcePath)) {
    console.log(`Skipping missing WARP result JSON: ${sourcePath}`);
    continue;
  }

  copyFileSync(sourcePath, targetPath);
}

console.log("Copied microbenchmark CSVs into public/data/micro/");
console.log("Copied available WARP benchmark JSONs into public/data/warp/");
