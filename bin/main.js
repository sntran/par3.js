#!/usr/bin/env node

/**
 * Local repair CLI for shard sets that already exist on disk.
 *
 * The CLI intentionally mirrors the Worker's semantics:
 * - every absent slot is marked missing for the codec,
 * - `--missing-index` only controls which repaired shards are written back out,
 * - shard bytes are copied directly into wasm memory so local and remote repair paths share the
 *   same low-level implementation.
 */

import { mkdir, readFile, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { parseArgs } from "node:util";

import { Par3 } from "../lib/mod.js";
import packageMetadata from "../package.json" with { type: "json" };

function usage() {
  return [
    "Usage:",
    `  ${packageMetadata.name} -h`,
    `  ${packageMetadata.name} -V`,
    `  ${packageMetadata.name} r(epair) --original-count <count> --recovery-count <count> --shard-size <bytes> \\`,
    "    --shard <index=path> [--shard <index=path> ...] --output-dir <dir> [--missing-index <index> ...]",
    "",
    "Examples:",
    `  ${packageMetadata.name} repair --original-count 4 --recovery-count 2 --shard-size 65536 \\`,
    "    --shard 0=./shards/shard_0.bin --shard 2=./shards/shard_2.bin \\",
    "    --shard 3=./shards/shard_3.bin --shard 4=./shards/shard_4.bin \\",
    "    --output-dir ./repaired --missing-index 1",
    "",
    "Notes:",
    "  - The CLI repairs every shard that is absent from --shard inputs so the codec sees the full missing set.",
    "  - --missing-index limits which repaired shards are written back to disk.",
  ].join("\n");
}

function writeLine(stream, message) {
  stream.write(`${message}\n`);
}

function parseInteger(label, rawValue, minimum = 0) {
  if (rawValue === undefined) {
    throw new Error(`--${label} is required`);
  }

  const value = Number.parseInt(rawValue, 10);
  if (!Number.isInteger(value) || value < minimum) {
    throw new Error(`--${label} must be an integer greater than or equal to ${minimum}`);
  }

  return value;
}

function parseIndexList(values, label, minimum = 0) {
  const unique = new Set();

  for (const rawValue of values ?? []) {
    unique.add(parseInteger(label, rawValue, minimum));
  }

  return Array.from(unique).sort((left, right) => left - right);
}

function parseShardSpec(rawValue) {
  const separator = rawValue.indexOf("=");
  if (separator <= 0 || separator === rawValue.length - 1) {
    throw new Error(`invalid --shard value \"${rawValue}\"; expected <index=path>`);
  }

  return {
    index: parseInteger("shard", rawValue.slice(0, separator), 0),
    filePath: resolve(rawValue.slice(separator + 1)),
  };
}

async function loadShardInputs(rawSpecs = [], shardSize, slotCount) {
  const shards = new Map();

  for (const rawSpec of rawSpecs) {
    const spec = parseShardSpec(rawSpec);
    if (spec.index >= slotCount) {
      throw new Error(`shard index ${spec.index} is outside slot_count ${slotCount}`);
    }

    if (shards.has(spec.index)) {
      throw new Error(`duplicate --shard input for slot ${spec.index}`);
    }

    const bytes = new Uint8Array(await readFile(spec.filePath));
    if (bytes.byteLength !== shardSize) {
      throw new Error(
        `shard file ${spec.filePath} is ${bytes.byteLength} bytes but shard_size is ${shardSize}`,
      );
    }

    shards.set(spec.index, bytes);
  }

  return shards;
}

export async function repairShardSet({
  originalCount,
  recoveryCount,
  shardSize,
  shardSpecs = [],
  requestedMissingIndices = [],
  outputDir,
}) {
  const layout = Par3.resolveLayout({
    originalCount,
    recoveryCount,
    requestedMissingIndices,
    shardSize,
    fieldNames: {
      missingIndices: "missing-index",
      originalCount: "--original-count",
      recoveryCount: "--recovery-count",
      shardSize: "--shard-size",
    },
  });
  const shards = await loadShardInputs(shardSpecs, layout.shardSize, layout.slotCount);
  const codec = new Par3(layout);

  try {
    for (const [index, bytes] of shards.entries()) {
      codec.writeShard(index, bytes, {
        duplicateMessage: `duplicate --shard input for slot ${index}`,
        overflowMessage: `shard index ${index} is outside slot_count ${layout.slotCount}`,
        sizeMessage: `shard file for slot ${index} is ${bytes.byteLength} bytes but shard_size is ${layout.shardSize}`,
      });
    }

    // Repair against the full inferred missing set, then decide which repaired shards to persist.
    const inferredMissingIndices = codec.inferMissingIndices();
    const outputIndices = codec.selectOutputIndices(layout.requestedMissingIndices);

    if (inferredMissingIndices.length === 0) {
      return {
        repairedIndices: [],
        outputFiles: [],
      };
    }

    codec.repair();
    await mkdir(outputDir, { recursive: true });
    const outputFiles = [];

    for (const index of outputIndices) {
      const filePath = join(outputDir, `shard_${index}.bin`);
      await writeFile(filePath, Buffer.from(codec.readShard(index)));
      outputFiles.push({ index, filePath });
    }

    return {
      repairedIndices: inferredMissingIndices,
      outputFiles,
    };
  } finally {
    codec.free();
  }
}

/**
 * Execute the CLI with injectable stdio for tests.
 *
 * Returning an exit code instead of terminating the process keeps the implementation testable and
 * lets the module act as both a reusable helper and a standalone executable.
 */
export async function runCli({
  argv = process.argv.slice(2),
  stdout = process.stdout,
  stderr = process.stderr,
} = {}) {
  try {
    const { values, positionals } = parseArgs({
      args: argv,
      allowPositionals: true,
      options: {
        help: { type: "boolean", short: "h" },
        "missing-index": { type: "string", multiple: true },
        "original-count": { type: "string" },
        "output-dir": { type: "string" },
        "recovery-count": { type: "string" },
        shard: { type: "string", multiple: true },
        "shard-size": { type: "string" },
        version: { type: "boolean", short: "V" },
      },
    });

    const command = positionals[0];
    const isRepairCommand = command === "repair" || command === "r";
    if (values.version) {
      writeLine(stdout, `${packageMetadata.name} ${packageMetadata.version}`);
      return 0;
    }

    if (values.help || !isRepairCommand || positionals.length > 1) {
      const showError = Boolean(command) && !isRepairCommand && !values.help;
      writeLine(showError ? stderr : stdout, usage());
      return showError ? 1 : 0;
    }

    const originalCount = parseInteger("original-count", values["original-count"], 1);
    const recoveryCount = parseInteger("recovery-count", values["recovery-count"], 0);
    const shardSize = parseInteger("shard-size", values["shard-size"], 2);
    if (shardSize % 2 !== 0) {
      throw new Error("--shard-size must be even");
    }

    if (!values["output-dir"]) {
      throw new Error("--output-dir is required");
    }
    const outputDir = resolve(values["output-dir"]);

    const result = await repairShardSet({
      originalCount,
      recoveryCount,
      shardSize,
      shardSpecs: values.shard,
      requestedMissingIndices: parseIndexList(values["missing-index"], "missing-index", 0),
      outputDir,
    });

    if (result.repairedIndices.length === 0) {
      writeLine(stdout, "No missing shards were inferred from the provided inputs.");
      return 0;
    }

    writeLine(stdout, `Repaired ${result.repairedIndices.length} shard(s).`);
    for (const file of result.outputFiles) {
      writeLine(stdout, `wrote shard_${file.index} -> ${file.filePath}`);
    }

    return 0;
  } catch (error) {
    writeLine(stderr, error instanceof Error ? error.message : String(error));
    return 1;
  }
}

if (import.meta.main) {
  process.exitCode = await runCli();
}
