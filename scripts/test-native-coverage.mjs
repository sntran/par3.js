import { spawn } from "node:child_process";
import { dirname } from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const trackedFile = "lib/mod.node.js";
const testFile = "lib/mod.node_test.js";
const requiredCoverage = Object.freeze({
  branch: 100,
  functions: 100,
  line: 100,
});

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
  "--experimental-test-coverage",
  `--test-coverage-include=${trackedFile}`,
  "--test",
  testFile,
];
const result = await runNode(args);

if (result.code !== 0) {
  process.exit(result.code ?? 1);
}

const metrics = parseCoverage(result.output).get(trackedFile);
if (!metrics) {
  console.error(`Coverage thresholds not met:\n- ${trackedFile}: missing coverage report entry`);
  process.exit(1);
}

if (
  metrics.line !== requiredCoverage.line
  || metrics.branch !== requiredCoverage.branch
  || metrics.functions !== requiredCoverage.functions
) {
  console.error("Coverage thresholds not met:");
  console.error(
    `- ${trackedFile}: line ${metrics.line.toFixed(2)}%, branch ${metrics.branch.toFixed(2)}%, functions ${metrics.functions.toFixed(2)}%`,
  );
  process.exit(1);
}

console.log("Coverage thresholds met for lib/mod.node.js.");
