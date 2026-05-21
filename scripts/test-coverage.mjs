import { spawn } from "node:child_process";
import { dirname } from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const trackedFiles = ["_worker.js", "bin/main.js", "lib/envelope.js", "lib/mod.js", "lib/multipart.js"];
const testFiles = ["_worker_test.js", "bin/main_test.js", "examples/fetch_encode_repair_gzip_test.js", "lib/envelope_test.js", "lib/mod_test.js", "lib/multipart_test.js"];

function stripAnsi(value) {
  return value.replace(/\u001B\[[0-?]*[ -/]*[@-~]/g, "");
}

function parseCoverage(output) {
  const metrics = new Map();
  let pendingDirectory = "";

  for (const rawLine of stripAnsi(output).split(/\r?\n/u)) {
    const line = rawLine.replace(/^\s*[^A-Za-z0-9_./-]+\s*/u, "").trimEnd();
    const columns = line.split("|").map((value) => value.trim());
    if (columns.length < 4) {
      continue;
    }

    const [fileColumn, linePercent, branchPercent, functionPercent] = columns;
    if (fileColumn && !linePercent && !branchPercent && !functionPercent && !fileColumn.endsWith(".js")) {
      pendingDirectory = fileColumn;
      continue;
    }

    if (!fileColumn.endsWith(".js") || !linePercent || !branchPercent || !functionPercent) {
      pendingDirectory = "";
      continue;
    }

    const filePath = pendingDirectory ? `${pendingDirectory}/${fileColumn}` : fileColumn;
    metrics.set(filePath, {
      branch: Number(branchPercent),
      functions: Number(functionPercent),
      line: Number(linePercent),
    });
  }

  return metrics;
}

function runNode(args) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, args, {
      cwd: repoRoot,
      env: {
        ...process.env,
        NODE_DISABLE_COLORS: "1",
      },
      stdio: ["ignore", "pipe", "pipe"],
    });

    let output = "";
    child.stdout.on("data", (chunk) => {
      output += chunk;
      process.stdout.write(chunk);
    });
    child.stderr.on("data", (chunk) => {
      output += chunk;
      process.stderr.write(chunk);
    });
    child.on("error", reject);
    child.on("close", (code) => {
      resolve({ code, output });
    });
  });
}

const args = [
  "--experimental-wasm-modules",
  "--experimental-test-coverage",
  ...trackedFiles.flatMap((filePath) => [`--test-coverage-include=${filePath}`]),
  "--test",
  ...testFiles,
];
const result = await runNode(args);

if (result.code !== 0) {
  process.exit(result.code ?? 1);
}

const metrics = parseCoverage(result.output);
const failures = [];

for (const filePath of trackedFiles) {
  const fileMetrics = metrics.get(filePath);
  if (!fileMetrics) {
    failures.push(`${filePath}: missing coverage report entry`);
    continue;
  }

  if (fileMetrics.line !== 100 || fileMetrics.branch !== 100 || fileMetrics.functions !== 100) {
    failures.push(
      `${filePath}: line ${fileMetrics.line.toFixed(2)}%, branch ${fileMetrics.branch.toFixed(2)}%, functions ${fileMetrics.functions.toFixed(2)}%`,
    );
  }
}

if (failures.length > 0) {
  console.error("Coverage thresholds not met:");
  for (const failure of failures) {
    console.error(`- ${failure}`);
  }
  process.exit(1);
}

console.log("Coverage thresholds met for _worker.js, bin/main.js, lib/envelope.js, lib/mod.js, and lib/multipart.js.");
