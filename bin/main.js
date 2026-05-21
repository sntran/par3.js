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

import { mkdir, readdir, readFile, writeFile } from "node:fs/promises";
import { basename, dirname, join, parse, resolve } from "node:path";
import { parseArgs } from "node:util";

import {
  computeFileDigests,
  computeFileId,
  packEnvelopeSet,
  parseEnvelopeSet,
} from "../lib/envelope.js";
import { Par3 } from "../lib/mod.js";
import packageMetadata from "../package.json" with { type: "json" };

const DEFAULT_PORCELAIN_SLICE_SIZE = 64 * 1024;
const ENVELOPE_FILE_PATTERN = /^(?<stem>.+?)(?:\.vol\d+-\d+)?\.(?<format>par2|par3)$/u;

function usage() {
  return [
    "Usage:",
    `  ${packageMetadata.name} -h`,
    `  ${packageMetadata.name} -V`,
    `  ${packageMetadata.name} c(reate) -r <count> [--shard-size <bytes>] [--format par2|par3] \\`,
    "    [--set-name <name>] --output-dir <dir> <file> [<file> ...]",
    `  ${packageMetadata.name} r(epair) <file.par2|file.par3> [--input-dir <dir>] [--output-dir <dir>]`,
    "",
    "Advanced raw-shard mode:",
    `  ${packageMetadata.name} c(reate) -n <count> -r <count> -s <bytes> \\`,
    "    --shard <index=path> [--shard <index=path> ...] --output-dir <dir>",
    `  ${packageMetadata.name} r(epair) -n <count> -r <count> -s <bytes> \\`,
    "    --shard <index=path> [--shard <index=path> ...] --output-dir <dir> [--missing-index <index> ...]",
    "",
    "Examples:",
    `  ${packageMetadata.name} create -r 2 -s 65536 --output-dir ./parity ./archive.bin ./photo.jpg`,
    "",
    `  ${packageMetadata.name} repair ./parity/archive.par2 --input-dir ./downloads --output-dir ./repaired`,
    "",
    "Raw shard interoperability example:",
    "  split -b 65536 --numeric-suffixes=0 --suffix-length=3 ./file.bin ./raw/shard_",
    "  cat ./raw/shard_000 > ./shards/shard_0.bin",
    "  cat ./raw/shard_001 > ./shards/shard_1.bin",
    "",
    "Notes:",
    "  - Porcelain create infers original shard count from the input files and uses packetized PAR2-style envelopes.",
    "  - Porcelain repair validates existing files against the stored file MD5 before deciding what to reconstruct.",
    "  - Create expects every original shard index `0..n-1` to be present in --shard inputs.",
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

// The CLI works on discrete files that already exist on disk, so it uses direct filesystem IO and
// bypasses the Worker's multipart streaming pipeline entirely.
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

async function writeOutputShards(codec, outputIndices, outputDir) {
  await mkdir(outputDir, { recursive: true });
  const outputFiles = [];

  for (const index of outputIndices) {
    const filePath = join(outputDir, `shard_${index}.bin`);
    await writeFile(filePath, Buffer.from(codec.readShard(index)));
    outputFiles.push({ index, filePath });
  }

  return outputFiles;
}

function resolveCliLayout({ originalCount, recoveryCount, requestedMissingIndices = [], shardSize }) {
  return Par3.resolveLayout({
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
}

function bytesEqual(left, right) {
  return left.byteLength === right.byteLength && Buffer.from(left).equals(Buffer.from(right));
}

function compareBytes(left, right) {
  return Buffer.from(left).compare(Buffer.from(right));
}

function normalizeEnvelopeFormat(rawValue = "par2") {
  if (rawValue !== "par2" && rawValue !== "par3") {
    throw new Error("--format must be either par2 or par3");
  }

  return rawValue;
}

function normalizeSetName(rawValue, inputPaths) {
  const fallback = inputPaths.length === 1 ? parse(inputPaths[0]).name : "par3-set";
  const value = rawValue ?? fallback;

  if (!value || basename(value) !== value) {
    throw new Error("--set-name must be a bare file name without directory separators");
  }

  return value;
}

function splitFileIntoShards(bytes, shardSize) {
  const shardCount = Math.ceil(bytes.byteLength / shardSize);

  return Array.from({ length: shardCount }, (_, shardIndex) => {
    const shard = new Uint8Array(shardSize);
    const start = shardIndex * shardSize;
    shard.set(bytes.subarray(start, Math.min(start + shardSize, bytes.byteLength)));
    return shard;
  });
}

function describeEnvelopeFamily(envelopePath) {
  const fileName = basename(envelopePath);
  const match = ENVELOPE_FILE_PATTERN.exec(fileName);

  if (!match?.groups) {
    throw new Error("repair expects a .par2 or .par3 envelope path");
  }

  return {
    format: match.groups.format,
    stem: match.groups.stem,
  };
}

function defaultVolumeFileName(setName, recoveryCount, format) {
  const width = Math.max(2, String(Math.max(0, recoveryCount - 1)).length);
  const first = String(0).padStart(width, "0");
  const last = String(Math.max(0, recoveryCount - 1)).padStart(width, "0");
  return `${setName}.vol${first}-${last}.${format}`;
}

async function readPorcelainFiles(inputPaths) {
  const seenNames = new Set();
  const loadedFiles = [];

  for (const inputPath of inputPaths) {
    const resolvedPath = resolve(inputPath);
    const name = basename(resolvedPath);

    if (seenNames.has(name)) {
      throw new Error(`duplicate input basename ${name}; porcelain mode stores basenames only`);
    }

    seenNames.add(name);

    const bytes = new Uint8Array(await readFile(resolvedPath));
    const digests = computeFileDigests(bytes);

    loadedFiles.push({
      bytes,
      fileId: computeFileId({
        headMd5: digests.headMd5,
        name,
        size: bytes.byteLength,
      }),
      fullMd5: digests.fullMd5,
      headMd5: digests.headMd5,
      name,
      size: bytes.byteLength,
      sourcePath: resolvedPath,
    });
  }

  return loadedFiles.toSorted((left, right) => compareBytes(left.fileId, right.fileId));
}

function buildPorcelainPlan(files, shardSize) {
  let nextSlotIndex = 0;

  return files.map((file) => {
    const sliceCount = Math.ceil(file.size / shardSize);
    const plannedFile = {
      ...file,
      shardSize,
      sliceCount,
      slotStart: nextSlotIndex,
    };

    nextSlotIndex += sliceCount;
    return plannedFile;
  });
}

function assembleFileFromCodec(codec, file) {
  if (file.sliceCount === 0) {
    return new Uint8Array(0);
  }

  const bytes = new Uint8Array(file.size);
  let offset = 0;

  for (let sliceIndex = 0; sliceIndex < file.sliceCount; sliceIndex += 1) {
    const shard = codec.readShard(file.slotStart + sliceIndex);
    const copyLength = Math.min(file.shardSize, file.size - offset);
    bytes.set(shard.subarray(0, copyLength), offset);
    offset += copyLength;
  }

  return bytes;
}

async function maybeReadFile(filePath) {
  try {
    return new Uint8Array(await readFile(filePath));
  } catch (error) {
    if (error && typeof error === "object" && "code" in error && error.code === "ENOENT") {
      return null;
    }

    throw error;
  }
}

async function resolveEnvelopePaths(inputPaths) {
  if (inputPaths.length === 0) {
    throw new Error("repair requires at least one envelope path");
  }

  if (inputPaths.length > 1) {
    return Array.from(new Set(inputPaths.map((inputPath) => resolve(inputPath)))).toSorted();
  }

  const anchorPath = resolve(inputPaths[0]);
  const family = describeEnvelopeFamily(anchorPath);
  const envelopeDirectory = dirname(anchorPath);
  const names = await readdir(envelopeDirectory);

  return names
    .filter((name) => {
      const match = ENVELOPE_FILE_PATTERN.exec(name);
      return match?.groups?.stem === family.stem && match.groups.format === family.format;
    })
    .map((name) => join(envelopeDirectory, name))
    .toSorted();
}

export async function createEnvelopeSet({
  format = "par2",
  inputPaths = [],
  outputDir,
  recoveryCount,
  setName,
  shardSize = DEFAULT_PORCELAIN_SLICE_SIZE,
}) {
  const resolvedOutputDir = resolve(outputDir);
  const resolvedInputPaths = inputPaths.map((inputPath) => resolve(inputPath));

  if (resolvedInputPaths.length === 0) {
    throw new Error("create requires at least one input file in porcelain mode");
  }

  const normalizedFormat = normalizeEnvelopeFormat(format);
  const normalizedSetName = normalizeSetName(setName, resolvedInputPaths);
  const normalizedShardSize = Par3.assertInteger(shardSize, "--shard-size", 4);

  if (normalizedShardSize % 4 !== 0) {
    throw new Error("--shard-size must be a multiple of 4 in porcelain mode");
  }

  const plannedFiles = buildPorcelainPlan(await readPorcelainFiles(resolvedInputPaths), normalizedShardSize);
  const originalCount = plannedFiles.reduce((sum, file) => sum + file.sliceCount, 0);

  if (originalCount === 0) {
    throw new Error("create requires at least one non-empty input file");
  }

  const layout = resolveCliLayout({
    originalCount,
    recoveryCount,
    shardSize: normalizedShardSize,
  });
  const codec = new Par3(layout);

  try {
    for (const file of plannedFiles) {
      for (const [sliceIndex, shard] of splitFileIntoShards(file.bytes, normalizedShardSize).entries()) {
        const slotIndex = file.slotStart + sliceIndex;
        codec.writeShard(slotIndex, shard, {
          duplicateMessage: `duplicate original shard ${slotIndex}`,
          overflowMessage: `original shard ${slotIndex} is outside slot_count ${layout.slotCount}`,
          sizeMessage: `original shard ${slotIndex} is ${shard.byteLength} bytes but shard_size is ${layout.shardSize}`,
        });
      }
    }

    const recoveryIndices = await codec.encode();
    const packedEnvelope = packEnvelopeSet({
      creator: `${packageMetadata.name} ${packageMetadata.version}`,
      files: plannedFiles.map(({ bytes, shardSize: ignoredShardSize, sliceCount, slotStart, sourcePath, ...file }) => file),
      format: normalizedFormat,
      recoverySlices: recoveryIndices.map((slotIndex) => ({
        exponent: slotIndex - originalCount,
        bytes: codec.readShard(slotIndex),
      })),
      sliceSize: normalizedShardSize,
    });
    const outputFiles = [];
    const manifestPath = join(resolvedOutputDir, `${normalizedSetName}.${normalizedFormat}`);

    await mkdir(resolvedOutputDir, { recursive: true });
    await writeFile(manifestPath, Buffer.from(packedEnvelope.manifestBytes));
    outputFiles.push(manifestPath);

    if (recoveryIndices.length > 0) {
      const volumePath = join(
        resolvedOutputDir,
        defaultVolumeFileName(normalizedSetName, recoveryIndices.length, normalizedFormat),
      );

      await writeFile(volumePath, Buffer.from(packedEnvelope.volumeBytes));
      outputFiles.push(volumePath);
    }

    return {
      fileCount: plannedFiles.length,
      originalCount,
      outputFiles,
      recoveryCount: recoveryIndices.length,
    };
  } finally {
    codec.free();
  }
}

export async function repairEnvelopeSet({
  envelopePaths = [],
  inputDir,
  outputDir,
}) {
  const resolvedEnvelopePaths = await resolveEnvelopePaths(envelopePaths);
  const packetFiles = await Promise.all(resolvedEnvelopePaths.map(async (envelopePath) => ({
    bytes: new Uint8Array(await readFile(envelopePath)),
    name: basename(envelopePath),
  })));
  const envelopeSet = parseEnvelopeSet(packetFiles);
  const resolvedInputDir = resolve(inputDir ?? dirname(resolvedEnvelopePaths[0]));
  const resolvedOutputDir = resolve(outputDir ?? resolvedInputDir);
  const plannedFiles = buildPorcelainPlan(envelopeSet.files, envelopeSet.sliceSize);
  const originalCount = plannedFiles.reduce((sum, file) => sum + file.sliceCount, 0);

  if (originalCount === 0) {
    await mkdir(resolvedOutputDir, { recursive: true });

    return {
      outputFiles: [],
      repairedFiles: [],
      resolvedEnvelopePaths,
    };
  }

  const recoveryCount = envelopeSet.recoverySlices.length === 0
    ? 0
    : envelopeSet.recoverySlices.at(-1).exponent + 1;
  const layout = resolveCliLayout({
    originalCount,
    recoveryCount,
    shardSize: envelopeSet.sliceSize,
  });
  const codec = new Par3(layout);

  try {
    const missingFiles = [];

    for (const file of plannedFiles) {
      const sourcePath = join(resolvedInputDir, file.name);
      const onDiskBytes = await maybeReadFile(sourcePath);
      const isValid = onDiskBytes !== null
        && onDiskBytes.byteLength === file.size
        && bytesEqual(computeFileDigests(onDiskBytes).fullMd5, file.fullMd5);

      if (!isValid) {
        missingFiles.push(file);
        continue;
      }

      for (const [sliceIndex, shard] of splitFileIntoShards(onDiskBytes, envelopeSet.sliceSize).entries()) {
        const slotIndex = file.slotStart + sliceIndex;
        codec.writeShard(slotIndex, shard, {
          duplicateMessage: `duplicate original shard ${slotIndex}`,
          overflowMessage: `original shard ${slotIndex} is outside slot_count ${layout.slotCount}`,
          sizeMessage: `original shard ${slotIndex} is ${shard.byteLength} bytes but shard_size is ${layout.shardSize}`,
        });
      }
    }

    for (const recoverySlice of envelopeSet.recoverySlices) {
      const slotIndex = originalCount + recoverySlice.exponent;
      codec.writeShard(slotIndex, recoverySlice.bytes, {
        duplicateMessage: `duplicate recovery shard ${slotIndex}`,
        overflowMessage: `recovery shard ${slotIndex} is outside slot_count ${layout.slotCount}`,
        sizeMessage: `recovery shard ${slotIndex} is ${recoverySlice.bytes.byteLength} bytes but shard_size is ${layout.shardSize}`,
      });
    }

    const filesNeedingRepair = missingFiles.filter((file) => file.sliceCount > 0);
    if (filesNeedingRepair.length > 0) {
      await codec.repair();
    }

    await mkdir(resolvedOutputDir, { recursive: true });

    const outputFiles = [];
    for (const file of missingFiles) {
      const repairedBytes = assembleFileFromCodec(codec, file);

      if (!bytesEqual(computeFileDigests(repairedBytes).fullMd5, file.fullMd5)) {
        throw new Error(`repaired file ${file.name} failed envelope MD5 verification`);
      }

      const outputPath = join(resolvedOutputDir, file.name);
      await writeFile(outputPath, Buffer.from(repairedBytes));
      outputFiles.push({ filePath: outputPath, name: file.name });
    }

    return {
      outputFiles,
      repairedFiles: missingFiles.map((file) => file.name),
      resolvedEnvelopePaths,
    };
  } finally {
    codec.free();
  }
}

export async function createShardSet({
  originalCount,
  recoveryCount,
  shardSize,
  shardSpecs = [],
  outputDir,
}) {
  const layout = resolveCliLayout({ originalCount, recoveryCount, shardSize });
  const shards = await loadShardInputs(shardSpecs, layout.shardSize, layout.slotCount);
  const codec = new Par3(layout);

  try {
    for (const index of shards.keys()) {
      if (index >= layout.originalCount) {
        throw new Error(
          `create only accepts --shard inputs for original indexes below original_count ${layout.originalCount}`,
        );
      }
    }

    for (let index = 0; index < layout.originalCount; index += 1) {
      const bytes = shards.get(index);
      if (!bytes) {
        throw new Error(`missing --shard input for original slot ${index}`);
      }

      codec.writeShard(index, bytes, {
        duplicateMessage: `duplicate --shard input for slot ${index}`,
        overflowMessage: `shard index ${index} is outside slot_count ${layout.slotCount}`,
        sizeMessage: `shard file for slot ${index} is ${bytes.byteLength} bytes but shard_size is ${layout.shardSize}`,
      });
    }

    const outputFiles = await writeOutputShards(codec, await codec.encode(), outputDir);
    return { outputFiles };
  } finally {
    codec.free();
  }
}

export async function repairShardSet({
  originalCount,
  recoveryCount,
  shardSize,
  shardSpecs = [],
  requestedMissingIndices = [],
  outputDir,
}) {
  const layout = resolveCliLayout({ originalCount, recoveryCount, requestedMissingIndices, shardSize });
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

    await codec.repair();
    const outputFiles = await writeOutputShards(codec, outputIndices, outputDir);

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
        format: { type: "string" },
        help: { type: "boolean", short: "h" },
        "input-dir": { type: "string" },
        "missing-index": { type: "string", multiple: true },
        "original-count": { type: "string", short: "n" },
        "output-dir": { type: "string" },
        "recovery-count": { type: "string", short: "r" },
        "set-name": { type: "string" },
        shard: { type: "string", multiple: true },
        "shard-size": { type: "string", short: "s" },
        version: { type: "boolean", short: "V" },
      },
    });

    const command = positionals[0];
    const commandArgs = positionals.slice(1);
    const isCreateCommand = command === "create" || command === "c";
    const isRepairCommand = command === "repair" || command === "r";
    const isKnownCommand = isCreateCommand || isRepairCommand;
    if (values.version) {
      writeLine(stdout, `${packageMetadata.name} ${packageMetadata.version}`);
      return 0;
    }

    if (values.help || !isKnownCommand) {
      const showError = Boolean(command) && !isKnownCommand && !values.help;
      writeLine(showError ? stderr : stdout, usage());
      return showError ? 1 : 0;
    }

    const shardSpecs = values.shard ?? [];
    const isPorcelainCreate = isCreateCommand && commandArgs.length > 0 && shardSpecs.length === 0;
    const isPorcelainRepair = isRepairCommand && commandArgs.length > 0 && shardSpecs.length === 0;

    if (commandArgs.length > 0 && shardSpecs.length > 0) {
      throw new Error("positional file arguments cannot be combined with --shard inputs");
    }

    if (isCreateCommand && isPorcelainCreate) {
      if ((values["missing-index"] ?? []).length > 0) {
        throw new Error("--missing-index is only supported for raw repair mode");
      }

      if (values["original-count"] !== undefined) {
        throw new Error("--original-count is inferred from input files in porcelain create mode");
      }

      if (!values["output-dir"]) {
        throw new Error("--output-dir is required");
      }

      const result = await createEnvelopeSet({
        format: values.format,
        inputPaths: commandArgs,
        outputDir: values["output-dir"],
        recoveryCount: parseInteger("recovery-count", values["recovery-count"], 0),
        setName: values["set-name"],
        shardSize: values["shard-size"] === undefined
          ? DEFAULT_PORCELAIN_SLICE_SIZE
          : parseInteger("shard-size", values["shard-size"], 4),
      });

      writeLine(stdout, `Created recovery set for ${result.fileCount} file(s).`);
      for (const filePath of result.outputFiles) {
        writeLine(stdout, `wrote ${filePath}`);
      }

      return 0;
    }

    if (isRepairCommand && isPorcelainRepair) {
      if ((values["missing-index"] ?? []).length > 0) {
        throw new Error("--missing-index is only supported for raw repair mode");
      }

      if (values["original-count"] !== undefined || values["recovery-count"] !== undefined || values["shard-size"] !== undefined) {
        throw new Error("porcelain repair reads layout information from the envelope and does not accept -n, -r, or -s");
      }

      const result = await repairEnvelopeSet({
        envelopePaths: commandArgs,
        inputDir: values["input-dir"],
        outputDir: values["output-dir"],
      });

      if (result.outputFiles.length === 0) {
        writeLine(stdout, "No missing or corrupt files were inferred from the available inputs.");
        return 0;
      }

      writeLine(stdout, `Repaired ${result.outputFiles.length} file(s).`);
      for (const file of result.outputFiles) {
        writeLine(stdout, `wrote ${file.name} -> ${file.filePath}`);
      }

      return 0;
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

    if (isCreateCommand) {
      if ((values["missing-index"] ?? []).length > 0) {
        throw new Error("--missing-index is only supported for repair");
      }

      const result = await createShardSet({
        originalCount,
        recoveryCount,
        shardSize,
        shardSpecs: values.shard,
        outputDir,
      });

      writeLine(stdout, `Created ${result.outputFiles.length} recovery shard(s).`);
      for (const file of result.outputFiles) {
        writeLine(stdout, `wrote shard_${file.index} -> ${file.filePath}`);
      }

      return 0;
    }

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
