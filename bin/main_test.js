import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdir, mkdtempDisposable, readdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

import { PACKET_DIALECTS, PACKET_TYPES, computeFileDigests, createPacket, packEnvelopeSet, parseEnvelopeSet, parsePacketFile } from "../lib/envelope.js";
import { createEnvelopeSet, repairEnvelopeSet, runCli } from "./main.js";
import packageMetadata from "../package.json" with { type: "json" };
import * as wasmModule from "../pkg/par3_bg.wasm";
import { alloc_shard_arena, free_shard_arena, leopard_encode, shard_arena_ptr } from "../pkg/par3.js";

const { memory } = wasmModule;
const execFileAsync = promisify(execFile);
const mainPath = fileURLToPath(new URL("./main.js", import.meta.url));
const repoRoot = fileURLToPath(new URL("../", import.meta.url));

function shardAt(view, shardSize, index) {
  const start = index * shardSize;
  return view.subarray(start, start + shardSize);
}

function buildOriginalShards(originalCount, shardSize) {
  return Array.from({ length: originalCount }, (_, shardIndex) => {
    const shard = new Uint8Array(shardSize);
    for (let offset = 0; offset < shardSize; offset += 1) {
      shard[offset] = (shardIndex * 37 + offset * 17) % 251;
    }
    return shard;
  });
}

async function buildRecoveryShards(originalShards, recoveryCount) {
  const originalCount = originalShards.length;
  const shardSize = originalShards[0].byteLength;
  const slotCount = originalCount + recoveryCount;
  const arenaHandle = alloc_shard_arena(slotCount, shardSize);
  const arenaPtr = shard_arena_ptr(arenaHandle);

  try {
    const bytes = new Uint8Array(memory.buffer, arenaPtr, slotCount * shardSize);
    for (let index = 0; index < originalCount; index += 1) {
      bytes.set(originalShards[index], index * shardSize);
    }

    leopard_encode(originalCount, shardSize, 0, shardSize, arenaHandle);

    const encodedBytes = new Uint8Array(memory.buffer, arenaPtr, slotCount * shardSize);
    return Array.from({ length: recoveryCount }, (_, recoveryIndex) =>
      Uint8Array.from(shardAt(encodedBytes, shardSize, originalCount + recoveryIndex)),
    );
  } finally {
    free_shard_arena(arenaHandle);
  }
}

function createWritableCapture() {
  let output = "";

  return {
    stream: {
      write(chunk) {
        output += String(chunk);
        return true;
      },
    },
    read() {
      return output;
    },
  };
}

async function withTempDir(run) {
  await using tempDir = await mkdtempDisposable(join(tmpdir(), "par3-cli-"));
  return await run(tempDir.path);
}

async function createPorcelainFixture(
  tempRoot,
  {
    files = [
      { bytes: Uint8Array.from([0, 1, 2, 3, 4, 5]), name: "archive.bin" },
      { bytes: Uint8Array.from([10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]), name: "photo.jpg" },
    ],
    format = "par2",
    recoveryCount = 2,
    setName = "bundle",
    shardSize = 8,
  } = {},
) {
  const inputDir = join(tempRoot, "inputs");
  const outputDir = join(tempRoot, "parity");

  await mkdir(inputDir, { recursive: true });

  for (const file of files) {
    await writeFile(join(inputDir, file.name), file.bytes);
  }

  const result = await createEnvelopeSet({
    format,
    inputPaths: files.map((file) => join(inputDir, file.name)),
    outputDir,
    recoveryCount,
    setName,
    shardSize,
  });

  return {
    files,
    inputDir,
    manifestPath: result.outputFiles[0],
    outputDir,
    result,
    volumePath: result.outputFiles[1] ?? null,
  };
}

test("CLI repairs every inferred missing shard into the output directory", async () => {
  await withTempDir(async (tempRoot) => {
    const originalCount = 4;
    const recoveryCount = 2;
    const shardSize = 64;
    const originalShards = buildOriginalShards(originalCount, shardSize);
    const recoveryShards = await buildRecoveryShards(originalShards, recoveryCount);
    const referenceSlots = [...originalShards, ...recoveryShards];
    const outputDir = join(tempRoot, "repaired");
    const stderr = createWritableCapture();
    const stdout = createWritableCapture();

    await writeFile(join(tempRoot, "shard_0.bin"), referenceSlots[0]);
    await writeFile(join(tempRoot, "shard_2.bin"), referenceSlots[2]);
    await writeFile(join(tempRoot, "shard_3.bin"), referenceSlots[3]);
    await writeFile(join(tempRoot, "shard_4.bin"), referenceSlots[4]);

    const exitCode = await runCli({
      argv: [
        "repair",
        "--original-count", String(originalCount),
        "--recovery-count", String(recoveryCount),
        "--shard-size", String(shardSize),
        "--shard", `0=${join(tempRoot, "shard_0.bin")}`,
        "--shard", `2=${join(tempRoot, "shard_2.bin")}`,
        "--shard", `3=${join(tempRoot, "shard_3.bin")}`,
        "--shard", `4=${join(tempRoot, "shard_4.bin")}`,
        "--output-dir", outputDir,
      ],
      stdout: stdout.stream,
      stderr: stderr.stream,
    });

    assert.equal(exitCode, 0);
    assert.equal(stderr.read(), "");
    assert.match(stdout.read(), /Repaired 2 shard\(s\)\./);

    assert.deepEqual(new Uint8Array(await readFile(join(outputDir, "shard_1.bin"))), referenceSlots[1]);
    assert.deepEqual(new Uint8Array(await readFile(join(outputDir, "shard_5.bin"))), referenceSlots[5]);
  });
});

test("CLI create writes recovery shards from the original shard set", async () => {
  await withTempDir(async (tempRoot) => {
    const originalCount = 3;
    const recoveryCount = 2;
    const shardSize = 64;
    const originalShards = buildOriginalShards(originalCount, shardSize);
    const expectedRecoveryShards = await buildRecoveryShards(originalShards, recoveryCount);
    const outputDir = join(tempRoot, "created");
    const stdout = createWritableCapture();
    const stderr = createWritableCapture();

    for (const [index, shard] of originalShards.entries()) {
      await writeFile(join(tempRoot, `shard_${index}.bin`), shard);
    }

    const exitCode = await runCli({
      argv: [
        "create",
        "-n", String(originalCount),
        "-r", String(recoveryCount),
        "-s", String(shardSize),
        "--shard", `0=${join(tempRoot, "shard_0.bin")}`,
        "--shard", `1=${join(tempRoot, "shard_1.bin")}`,
        "--shard", `2=${join(tempRoot, "shard_2.bin")}`,
        "--output-dir", outputDir,
      ],
      stdout: stdout.stream,
      stderr: stderr.stream,
    });

    assert.equal(exitCode, 0);
    assert.equal(stderr.read(), "");
    assert.match(stdout.read(), /Created 2 recovery shard\(s\)\./);
    assert.deepEqual(new Uint8Array(await readFile(join(outputDir, "shard_3.bin"))), expectedRecoveryShards[0]);
    assert.deepEqual(new Uint8Array(await readFile(join(outputDir, "shard_4.bin"))), expectedRecoveryShards[1]);
  });
});

test("CLI porcelain create writes packetized PAR2 envelopes", async () => {
  await withTempDir(async (tempRoot) => {
    const inputDir = join(tempRoot, "inputs");
    const outputDir = join(tempRoot, "parity");
    const stdout = createWritableCapture();
    const stderr = createWritableCapture();
    const archiveBytes = Uint8Array.from([0, 1, 2, 3, 4, 5]);
    const photoBytes = Uint8Array.from([10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]);

    await mkdir(inputDir, { recursive: true });
    await writeFile(join(inputDir, "archive.bin"), archiveBytes);
    await writeFile(join(inputDir, "photo.jpg"), photoBytes);

    const exitCode = await runCli({
      argv: [
        "create",
        "--recovery-count", "2",
        "--shard-size", "8",
        "--set-name", "bundle",
        "--output-dir", outputDir,
        join(inputDir, "archive.bin"),
        join(inputDir, "photo.jpg"),
      ],
      stdout: stdout.stream,
      stderr: stderr.stream,
    });

    assert.equal(exitCode, 0);
    assert.equal(stderr.read(), "");
    assert.match(stdout.read(), /Created recovery set for 2 file\(s\)\./);

    const manifestPath = join(outputDir, "bundle.par2");
    const volumePath = join(outputDir, "bundle.vol00-01.par2");
    const parsed = parseEnvelopeSet([
      { name: "bundle.par2", bytes: new Uint8Array(await readFile(manifestPath)) },
      { name: "bundle.vol00-01.par2", bytes: new Uint8Array(await readFile(volumePath)) },
    ]);

    assert.equal(parsed.sliceSize, 8);
    assert.deepEqual(parsed.files.map((file) => file.name).toSorted(), ["archive.bin", "photo.jpg"]);
    assert.equal(parsed.recoverySlices.length, 2);
  });
});

test("CLI porcelain create writes PAR3 packet headers when requested", async () => {
  await withTempDir(async (tempRoot) => {
    const inputDir = join(tempRoot, "inputs");
    const outputDir = join(tempRoot, "parity");
    const stdout = createWritableCapture();
    const stderr = createWritableCapture();

    await mkdir(inputDir, { recursive: true });
    await writeFile(join(inputDir, "archive.bin"), Uint8Array.from([0, 1, 2, 3, 4, 5]));

    const exitCode = await runCli({
      argv: [
        "create",
        "--format", "par3",
        "--recovery-count", "1",
        "--shard-size", "8",
        "--set-name", "bundle",
        "--output-dir", outputDir,
        join(inputDir, "archive.bin"),
      ],
      stdout: stdout.stream,
      stderr: stderr.stream,
    });

    assert.equal(exitCode, 0);
    assert.equal(stderr.read(), "");
    assert.match(stdout.read(), /Created recovery set for 1 file\(s\)\./);

    const manifestBytes = new Uint8Array(await readFile(join(outputDir, "bundle.par3")));
    const manifestPackets = parsePacketFile(manifestBytes);

    assert.deepEqual(Array.from(manifestBytes.subarray(0, 8)), Array.from(PACKET_DIALECTS.par3.magic));
    assert.equal(Buffer.from(manifestPackets[0].packetType).equals(Buffer.from(PACKET_DIALECTS.par3.types.main)), true);
  });
});

test("CLI porcelain repair reconstructs corrupt files from a scanned envelope family", async () => {
  await withTempDir(async (tempRoot) => {
    const inputDir = join(tempRoot, "inputs");
    const outputDir = join(tempRoot, "parity");
    const repairedDir = join(tempRoot, "repaired");
    const createStdout = createWritableCapture();
    const createStderr = createWritableCapture();
    const repairStdout = createWritableCapture();
    const repairStderr = createWritableCapture();
    const archiveBytes = Uint8Array.from([0, 1, 2, 3, 4, 5]);
    const photoBytes = Uint8Array.from([10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]);

    await mkdir(inputDir, { recursive: true });
    await writeFile(join(inputDir, "archive.bin"), archiveBytes);
    await writeFile(join(inputDir, "photo.jpg"), photoBytes);

    const createExitCode = await runCli({
      argv: [
        "create",
        "--recovery-count", "2",
        "--shard-size", "8",
        "--set-name", "bundle",
        "--output-dir", outputDir,
        join(inputDir, "archive.bin"),
        join(inputDir, "photo.jpg"),
      ],
      stdout: createStdout.stream,
      stderr: createStderr.stream,
    });

    assert.equal(createExitCode, 0);
    await writeFile(join(inputDir, "photo.jpg"), Uint8Array.from([9, 9, 9]));

    const repairExitCode = await runCli({
      argv: [
        "repair",
        join(outputDir, "bundle.vol00-01.par2"),
        "--input-dir", inputDir,
        "--output-dir", repairedDir,
      ],
      stdout: repairStdout.stream,
      stderr: repairStderr.stream,
    });

    assert.equal(repairExitCode, 0);
    assert.equal(repairStderr.read(), "");
    assert.match(repairStdout.read(), /Repaired 1 file\(s\)\./);
    assert.deepEqual(new Uint8Array(await readFile(join(repairedDir, "photo.jpg"))), photoBytes);
  });
});

test("createEnvelopeSet validates porcelain file inputs", async () => {
  await withTempDir(async (tempRoot) => {
    const singlePath = join(tempRoot, "alpha.bin");
    const emptyPath = join(tempRoot, "empty.bin");
    const firstDir = join(tempRoot, "a");
    const secondDir = join(tempRoot, "b");

    await mkdir(firstDir, { recursive: true });
    await mkdir(secondDir, { recursive: true });
    await writeFile(singlePath, Uint8Array.from([0, 1, 2, 3]));
    await writeFile(emptyPath, new Uint8Array(0));
    await writeFile(join(firstDir, "dup.bin"), Uint8Array.from([1, 2, 3, 4]));
    await writeFile(join(secondDir, "dup.bin"), Uint8Array.from([5, 6, 7, 8]));

    await assert.rejects(
      () => createEnvelopeSet({ inputPaths: [], outputDir: tempRoot, recoveryCount: 1 }),
      /create requires at least one input file in porcelain mode/,
    );
    await assert.rejects(
      () => createEnvelopeSet({ format: "zip", inputPaths: [singlePath], outputDir: tempRoot, recoveryCount: 1 }),
      /--format must be either par2 or par3/,
    );
    await assert.rejects(
      () => createEnvelopeSet({ inputPaths: [singlePath], outputDir: tempRoot, recoveryCount: 1, setName: "nested/out" }),
      /--set-name must be a bare file name/,
    );
    await assert.rejects(
      () => createEnvelopeSet({
        inputPaths: [join(firstDir, "dup.bin"), join(secondDir, "dup.bin")],
        outputDir: tempRoot,
        recoveryCount: 1,
      }),
      /duplicate input basename dup.bin/,
    );
    await assert.rejects(
      () => createEnvelopeSet({ inputPaths: [singlePath], outputDir: tempRoot, recoveryCount: 1, shardSize: 6 }),
      /--shard-size must be a multiple of 4 in porcelain mode/,
    );
    await assert.rejects(
      () => createEnvelopeSet({ inputPaths: [emptyPath], outputDir: tempRoot, recoveryCount: 1, shardSize: 8 }),
      /create requires at least one non-empty input file/,
    );
  });
});

test("repairEnvelopeSet handles explicit path lists, missing empty files, and empty-only envelopes", async () => {
  await withTempDir(async (tempRoot) => {
    const fixture = await createPorcelainFixture(tempRoot, {
      files: [
        { bytes: Uint8Array.from([0, 1, 2, 3]), name: "archive.bin" },
        { bytes: new Uint8Array(0), name: "empty.txt" },
      ],
      recoveryCount: 1,
      setName: "with-empty",
      shardSize: 8,
    });
    const repairedDir = join(tempRoot, "repaired");

    await assert.rejects(
      () => repairEnvelopeSet({ envelopePaths: [] }),
      /repair requires at least one envelope path/,
    );

    await rm(join(fixture.inputDir, "empty.txt"));

    const repairedResult = await repairEnvelopeSet({
      envelopePaths: [fixture.manifestPath, fixture.volumePath],
      inputDir: fixture.inputDir,
      outputDir: repairedDir,
    });

    assert.deepEqual(repairedResult.repairedFiles, ["empty.txt"]);
    assert.deepEqual(new Uint8Array(await readFile(join(repairedDir, "empty.txt"))), new Uint8Array(0));

    const emptyDigests = computeFileDigests(new Uint8Array(0));
    const emptyOnlySet = packEnvelopeSet({
      files: [{ name: "empty-only.txt", size: 0, ...emptyDigests }],
      sliceSize: 8,
    });
    const emptyManifestPath = join(tempRoot, "empty-only.par2");

    await writeFile(emptyManifestPath, Buffer.from(emptyOnlySet.manifestBytes));

    const emptyOnlyResult = await repairEnvelopeSet({
      envelopePaths: [emptyManifestPath],
    });

    assert.deepEqual(emptyOnlyResult.outputFiles, []);
    assert.deepEqual(emptyOnlyResult.repairedFiles, []);

    const zeroRecoveryFixture = await createPorcelainFixture(join(tempRoot, "zero-recovery"), {
      files: [{ bytes: Uint8Array.from([0, 1, 2, 3]), name: "solo.bin" }],
      recoveryCount: 0,
      setName: "zero-recovery",
      shardSize: 8,
    });
    const zeroRecoveryResult = await repairEnvelopeSet({
      envelopePaths: [zeroRecoveryFixture.manifestPath],
      inputDir: zeroRecoveryFixture.inputDir,
    });

    assert.deepEqual(zeroRecoveryResult.outputFiles, []);
  });
});

test("repairEnvelopeSet rethrows non-ENOENT input errors and verifies repaired file MD5s", async () => {
  await withTempDir(async (tempRoot) => {
    const fixture = await createPorcelainFixture(join(tempRoot, "md5-fixture"), { setName: "md5" });
    const tamperedVolumePath = join(fixture.outputDir, "md5.tampered.par2");
    const recoveryPackets = parsePacketFile(new Uint8Array(await readFile(fixture.volumePath)));
    const tamperedPackets = recoveryPackets.map((packet, index) => {
      const body = Uint8Array.from(packet.body);

      if (index === 0) {
        body[4] ^= 0xff;
      }

      return createPacket({
        packetType: PACKET_TYPES.recoverySlice,
        recoverySetId: packet.recoverySetId,
        body,
      });
    });

    await writeFile(
      tamperedVolumePath,
      Buffer.concat(tamperedPackets.map((packet) => Buffer.from(packet))),
    );
    await rm(join(fixture.inputDir, "photo.jpg"));

    await assert.rejects(
      () => repairEnvelopeSet({
        envelopePaths: [fixture.manifestPath, tamperedVolumePath],
        inputDir: fixture.inputDir,
        outputDir: join(tempRoot, "tampered-out"),
      }),
      /failed envelope MD5 verification/,
    );
    await assert.rejects(
      () => repairEnvelopeSet({
        envelopePaths: [fixture.manifestPath],
        inputDir: join(fixture.inputDir, "archive.bin"),
        outputDir: join(tempRoot, "bad-input-dir"),
      }),
      /ENOTDIR|not a directory/,
    );
  });
});

test("CLI porcelain mode validates option combinations and reports no-op repairs", async () => {
  await withTempDir(async (tempRoot) => {
    const filePath = join(tempRoot, "alpha.bin");
    const outputDir = join(tempRoot, "parity");
    const stderr = createWritableCapture();

    await writeFile(filePath, Uint8Array.from([0, 1, 2, 3]));

    let exitCode = await runCli({
      argv: ["create", "--shard", `0=${filePath}`, filePath],
      stdout: createWritableCapture().stream,
      stderr: stderr.stream,
    });
    assert.equal(exitCode, 1);
    assert.match(stderr.read(), /positional file arguments cannot be combined with --shard inputs/);

    exitCode = await runCli({
      argv: ["create", "--missing-index", "1", "--recovery-count", "1", "--output-dir", outputDir, filePath],
      stdout: createWritableCapture().stream,
      stderr: createWritableCapture().stream,
    });
    assert.equal(exitCode, 1);

    const originalCountError = createWritableCapture();
    exitCode = await runCli({
      argv: ["create", "--original-count", "1", "--recovery-count", "1", "--output-dir", outputDir, filePath],
      stdout: createWritableCapture().stream,
      stderr: originalCountError.stream,
    });
    assert.equal(exitCode, 1);
    assert.match(originalCountError.read(), /--original-count is inferred from input files/);

    const outputDirError = createWritableCapture();
    exitCode = await runCli({
      argv: ["create", "--recovery-count", "1", filePath],
      stdout: createWritableCapture().stream,
      stderr: outputDirError.stream,
    });
    assert.equal(exitCode, 1);
    assert.match(outputDirError.read(), /--output-dir is required/);

    const defaultShardStdout = createWritableCapture();
    const defaultShardStderr = createWritableCapture();
    exitCode = await runCli({
      argv: ["create", "--recovery-count", "1", "--output-dir", outputDir, filePath],
      stdout: defaultShardStdout.stream,
      stderr: defaultShardStderr.stream,
    });
    assert.equal(exitCode, 0);
    assert.equal(defaultShardStderr.read(), "");
    assert.match(defaultShardStdout.read(), /Created recovery set for 1 file\(s\)\./);

    const fixture = await createPorcelainFixture(join(tempRoot, "no-op"), { setName: "noop" });
    const missingIndexError = createWritableCapture();
    exitCode = await runCli({
      argv: ["repair", fixture.manifestPath, "--missing-index", "1"],
      stdout: createWritableCapture().stream,
      stderr: missingIndexError.stream,
    });
    assert.equal(exitCode, 1);
    assert.match(missingIndexError.read(), /--missing-index is only supported for raw repair mode/);

    const layoutFlagError = createWritableCapture();
    exitCode = await runCli({
      argv: ["repair", fixture.manifestPath, "-n", "1"],
      stdout: createWritableCapture().stream,
      stderr: layoutFlagError.stream,
    });
    assert.equal(exitCode, 1);
    assert.match(layoutFlagError.read(), /porcelain repair reads layout information from the envelope/);

    const noOpStdout = createWritableCapture();
    const noOpStderr = createWritableCapture();
    exitCode = await runCli({
      argv: ["repair", fixture.manifestPath, "--input-dir", fixture.inputDir, "--output-dir", join(tempRoot, "noop-repaired")],
      stdout: noOpStdout.stream,
      stderr: noOpStderr.stream,
    });
    assert.equal(exitCode, 0);
    assert.equal(noOpStderr.read(), "");
    assert.match(noOpStdout.read(), /No missing or corrupt files were inferred from the available inputs/);
  });
});

test("CLI create rejects recovery-slot shard inputs", async () => {
  await withTempDir(async (tempRoot) => {
    const stderr = createWritableCapture();

    await writeFile(join(tempRoot, "shard_0.bin"), Uint8Array.from([0, 1, 2, 3]));
    await writeFile(join(tempRoot, "shard_1.bin"), Uint8Array.from([4, 5, 6, 7]));

    const exitCode = await runCli({
      argv: [
        "create",
        "-n", "1",
        "-r", "1",
        "-s", "4",
        "--shard", `0=${join(tempRoot, "shard_0.bin")}`,
        "--shard", `1=${join(tempRoot, "shard_1.bin")}`,
        "--output-dir", join(tempRoot, "out"),
      ],
      stdout: createWritableCapture().stream,
      stderr: stderr.stream,
    });

    assert.equal(exitCode, 1);
    assert.match(stderr.read(), /create only accepts --shard inputs for original indexes below original_count 1/);
  });
});

test("CLI create requires every original shard input", async () => {
  await withTempDir(async (tempRoot) => {
    const stderr = createWritableCapture();

    await writeFile(join(tempRoot, "shard_0.bin"), Uint8Array.from([0, 1, 2, 3]));

    const exitCode = await runCli({
      argv: [
        "create",
        "-n", "2",
        "-r", "1",
        "-s", "4",
        "--shard", `0=${join(tempRoot, "shard_0.bin")}`,
        "--output-dir", join(tempRoot, "out"),
      ],
      stdout: createWritableCapture().stream,
      stderr: stderr.stream,
    });

    assert.equal(exitCode, 1);
    assert.match(stderr.read(), /missing --shard input for original slot 1/);
  });
});

test("CLI create rejects --missing-index", async () => {
  const stderr = createWritableCapture();

  const exitCode = await runCli({
    argv: [
      "create",
      "-n", "1",
      "-r", "1",
      "-s", "4",
      "--missing-index", "1",
      "--output-dir", join(tmpdir(), "par3-create-invalid"),
    ],
    stdout: createWritableCapture().stream,
    stderr: stderr.stream,
  });

  assert.equal(exitCode, 1);
  assert.match(stderr.read(), /--missing-index is only supported for repair/);
});

test("CLI can limit which repaired shards are written to disk", async () => {
  await withTempDir(async (tempRoot) => {
    const originalCount = 4;
    const recoveryCount = 2;
    const shardSize = 64;
    const originalShards = buildOriginalShards(originalCount, shardSize);
    const recoveryShards = await buildRecoveryShards(originalShards, recoveryCount);
    const referenceSlots = [...originalShards, ...recoveryShards];
    const outputDir = join(tempRoot, "selected");

    await writeFile(join(tempRoot, "shard_0.bin"), referenceSlots[0]);
    await writeFile(join(tempRoot, "shard_2.bin"), referenceSlots[2]);
    await writeFile(join(tempRoot, "shard_3.bin"), referenceSlots[3]);
    await writeFile(join(tempRoot, "shard_4.bin"), referenceSlots[4]);

    const exitCode = await runCli({
      argv: [
        "repair",
        "--original-count", String(originalCount),
        "--recovery-count", String(recoveryCount),
        "--shard-size", String(shardSize),
        "--shard", `0=${join(tempRoot, "shard_0.bin")}`,
        "--shard", `2=${join(tempRoot, "shard_2.bin")}`,
        "--shard", `3=${join(tempRoot, "shard_3.bin")}`,
        "--shard", `4=${join(tempRoot, "shard_4.bin")}`,
        "--missing-index", "1",
        "--output-dir", outputDir,
      ],
      stdout: createWritableCapture().stream,
      stderr: createWritableCapture().stream,
    });

    assert.equal(exitCode, 0);
    assert.deepEqual(new Uint8Array(await readFile(join(outputDir, "shard_1.bin"))), referenceSlots[1]);
    assert.deepEqual(await readdir(outputDir), ["shard_1.bin"]);
  });
});

test("CLI sorts multiple requested missing indices before writing output files", async () => {
  await withTempDir(async (tempRoot) => {
    const originalCount = 4;
    const recoveryCount = 2;
    const shardSize = 64;
    const originalShards = buildOriginalShards(originalCount, shardSize);
    const recoveryShards = await buildRecoveryShards(originalShards, recoveryCount);
    const referenceSlots = [...originalShards, ...recoveryShards];
    const outputDir = join(tempRoot, "sorted");

    await writeFile(join(tempRoot, "shard_0.bin"), referenceSlots[0]);
    await writeFile(join(tempRoot, "shard_2.bin"), referenceSlots[2]);
    await writeFile(join(tempRoot, "shard_3.bin"), referenceSlots[3]);
    await writeFile(join(tempRoot, "shard_4.bin"), referenceSlots[4]);

    const exitCode = await runCli({
      argv: [
        "repair",
        "--original-count", String(originalCount),
        "--recovery-count", String(recoveryCount),
        "--shard-size", String(shardSize),
        "--shard", `0=${join(tempRoot, "shard_0.bin")}`,
        "--shard", `2=${join(tempRoot, "shard_2.bin")}`,
        "--shard", `3=${join(tempRoot, "shard_3.bin")}`,
        "--shard", `4=${join(tempRoot, "shard_4.bin")}`,
        "--missing-index", "5",
        "--missing-index", "1",
        "--output-dir", outputDir,
      ],
      stdout: createWritableCapture().stream,
      stderr: createWritableCapture().stream,
    });

    assert.equal(exitCode, 0);
    assert.deepEqual(await readdir(outputDir), ["shard_1.bin", "shard_5.bin"]);
  });
});

test("CLI rejects requested outputs that were already provided", async () => {
  await withTempDir(async (tempRoot) => {
    const shardPath = join(tempRoot, "shard_0.bin");
    await writeFile(shardPath, Uint8Array.from([0, 1, 2, 3]));
    const stderr = createWritableCapture();

    const exitCode = await runCli({
      argv: [
        "repair",
        "--original-count", "2",
        "--recovery-count", "1",
        "--shard-size", "4",
        "--shard", `0=${shardPath}`,
        "--missing-index", "0",
        "--output-dir", join(tempRoot, "out"),
      ],
      stdout: createWritableCapture().stream,
      stderr: stderr.stream,
    });

    assert.equal(exitCode, 1);
    assert.match(stderr.read(), /requested missing index 0 was already provided/);
  });
});

test("CLI repair rejects empty shard input sets with a stable error", async () => {
  await withTempDir(async (tempRoot) => {
    const stderr = createWritableCapture();

    const exitCode = await runCli({
      argv: [
        "repair",
        "--original-count", "2",
        "--recovery-count", "1",
        "--shard-size", "4",
        "--output-dir", join(tempRoot, "out"),
      ],
      stdout: createWritableCapture().stream,
      stderr: stderr.stream,
    });

    assert.equal(exitCode, 1);
    assert.match(stderr.read(), /Insufficient shards provided for repair/);
  });
});

test("CLI reports usage errors for invalid commands", async () => {
  const stdout = createWritableCapture();
  const stderr = createWritableCapture();

  const exitCode = await runCli({
    argv: ["unknown"],
    stdout: stdout.stream,
    stderr: stderr.stream,
  });

  assert.equal(exitCode, 1);
  assert.equal(stdout.read(), "");
  assert.match(stderr.read(), /Usage:/);
});

test("CLI prints its package version for -V", async () => {
  const stdout = createWritableCapture();
  const stderr = createWritableCapture();

  const exitCode = await runCli({
    argv: ["-V"],
    stdout: stdout.stream,
    stderr: stderr.stream,
  });

  assert.equal(exitCode, 0);
  assert.equal(stderr.read(), "");
  assert.equal(stdout.read(), `${packageMetadata.name} ${packageMetadata.version}\n`);
});

test("CLI porcelain repair validates the envelope file extension", async () => {
  const stdout = createWritableCapture();
  const stderr = createWritableCapture();

  const exitCode = await runCli({
    argv: ["repair", "extra"],
    stdout: stdout.stream,
    stderr: stderr.stream,
  });

  assert.equal(exitCode, 1);
  assert.equal(stdout.read(), "");
  assert.match(stderr.read(), /repair expects a \.par2 or \.par3 envelope path/);
});

test("CLI stringifies non-Error failures in its top-level catch", async () => {
  await withTempDir(async (tempRoot) => {
    const originalCount = 3;
    const recoveryCount = 1;
    const shardSize = 32;
    const originalShards = buildOriginalShards(originalCount, shardSize);
    const recoveryShards = await buildRecoveryShards(originalShards, recoveryCount);
    const referenceSlots = [...originalShards, ...recoveryShards];
    const stderr = createWritableCapture();

    for (const [index, shard] of referenceSlots.entries()) {
      await writeFile(join(tempRoot, `shard_${index}.bin`), shard);
    }

    const exitCode = await runCli({
      argv: [
        "repair",
        "--original-count", String(originalCount),
        "--recovery-count", String(recoveryCount),
        "--shard-size", String(shardSize),
        "--shard", `0=${join(tempRoot, "shard_0.bin")}`,
        "--shard", `1=${join(tempRoot, "shard_1.bin")}`,
        "--shard", `2=${join(tempRoot, "shard_2.bin")}`,
        "--shard", `3=${join(tempRoot, "shard_3.bin")}`,
        "--output-dir", join(tempRoot, "out"),
      ],
      stdout: {
        write() {
          throw "forced string failure";
        },
      },
      stderr: stderr.stream,
    });

    assert.equal(exitCode, 1);
    assert.match(stderr.read(), /forced string failure/);
  });
});

test("CLI rejects invalid integer arguments", async () => {
  const stderr = createWritableCapture();

  const exitCode = await runCli({
    argv: [
      "repair",
      "--original-count", "nope",
      "--recovery-count", "1",
      "--shard-size", "4",
      "--output-dir", join(tmpdir(), "par3-invalid"),
    ],
    stdout: createWritableCapture().stream,
    stderr: stderr.stream,
  });

  assert.equal(exitCode, 1);
  assert.match(stderr.read(), /--original-count must be an integer/);
});

test("CLI requires original-count", async () => {
  const stderr = createWritableCapture();

  const exitCode = await runCli({
    argv: [
      "repair",
      "--recovery-count", "1",
      "--shard-size", "4",
      "--output-dir", join(tmpdir(), "par3-invalid"),
    ],
    stdout: createWritableCapture().stream,
    stderr: stderr.stream,
  });

  assert.equal(exitCode, 1);
  assert.match(stderr.read(), /--original-count is required/);
});

test("CLI rejects odd shard sizes", async () => {
  const stderr = createWritableCapture();

  const exitCode = await runCli({
    argv: [
      "repair",
      "--original-count", "2",
      "--recovery-count", "1",
      "--shard-size", "3",
      "--output-dir", join(tmpdir(), "par3-invalid"),
    ],
    stdout: createWritableCapture().stream,
    stderr: stderr.stream,
  });

  assert.equal(exitCode, 1);
  assert.match(stderr.read(), /--shard-size must be even/);
});

test("CLI requires an output directory", async () => {
  const stderr = createWritableCapture();

  const exitCode = await runCli({
    argv: [
      "repair",
      "--original-count", "2",
      "--recovery-count", "1",
      "--shard-size", "4",
    ],
    stdout: createWritableCapture().stream,
    stderr: stderr.stream,
  });

  assert.equal(exitCode, 1);
  assert.match(stderr.read(), /--output-dir is required/);
});

test("CLI rejects malformed shard specs", async () => {
  const stderr = createWritableCapture();

  const exitCode = await runCli({
    argv: [
      "repair",
      "--original-count", "2",
      "--recovery-count", "1",
      "--shard-size", "4",
      "--shard", "bad-spec",
      "--output-dir", join(tmpdir(), "par3-invalid"),
    ],
    stdout: createWritableCapture().stream,
    stderr: stderr.stream,
  });

  assert.equal(exitCode, 1);
  assert.match(stderr.read(), /invalid --shard value/);
});

test("CLI rejects duplicate shard declarations", async () => {
  await withTempDir(async (tempRoot) => {
    const shardPath = join(tempRoot, "shard_0.bin");
    await writeFile(shardPath, Uint8Array.from([0, 1, 2, 3]));
    const stderr = createWritableCapture();

    const exitCode = await runCli({
      argv: [
        "repair",
        "--original-count", "2",
        "--recovery-count", "1",
        "--shard-size", "4",
        "--shard", `0=${shardPath}`,
        "--shard", `0=${shardPath}`,
        "--output-dir", join(tempRoot, "out"),
      ],
      stdout: createWritableCapture().stream,
      stderr: stderr.stream,
    });

    assert.equal(exitCode, 1);
    assert.match(stderr.read(), /duplicate --shard input for slot 0/);
  });
});

test("CLI rejects shard indexes outside the declared slot count", async () => {
  await withTempDir(async (tempRoot) => {
    const shardPath = join(tempRoot, "shard_9.bin");
    await writeFile(shardPath, Uint8Array.from([0, 1, 2, 3]));
    const stderr = createWritableCapture();

    const exitCode = await runCli({
      argv: [
        "repair",
        "--original-count", "2",
        "--recovery-count", "1",
        "--shard-size", "4",
        "--shard", `9=${shardPath}`,
        "--output-dir", join(tempRoot, "out"),
      ],
      stdout: createWritableCapture().stream,
      stderr: stderr.stream,
    });

    assert.equal(exitCode, 1);
    assert.match(stderr.read(), /shard index 9 is outside slot_count 3/);
  });
});

test("CLI rejects shard files with the wrong size", async () => {
  await withTempDir(async (tempRoot) => {
    const shardPath = join(tempRoot, "shard_0.bin");
    await writeFile(shardPath, Uint8Array.from([0, 1, 2]));
    const stderr = createWritableCapture();

    const exitCode = await runCli({
      argv: [
        "repair",
        "--original-count", "2",
        "--recovery-count", "1",
        "--shard-size", "4",
        "--shard", `0=${shardPath}`,
        "--output-dir", join(tempRoot, "out"),
      ],
      stdout: createWritableCapture().stream,
      stderr: stderr.stream,
    });

    assert.equal(exitCode, 1);
    assert.match(stderr.read(), /is 3 bytes but shard_size is 4/);
  });
});

test("CLI reports when every shard is already present", async () => {
  await withTempDir(async (tempRoot) => {
    const originalCount = 3;
    const recoveryCount = 1;
    const shardSize = 32;
    const originalShards = buildOriginalShards(originalCount, shardSize);
    const recoveryShards = await buildRecoveryShards(originalShards, recoveryCount);
    const referenceSlots = [...originalShards, ...recoveryShards];
    const stdout = createWritableCapture();
    const stderr = createWritableCapture();

    for (const [index, shard] of referenceSlots.entries()) {
      await writeFile(join(tempRoot, `shard_${index}.bin`), shard);
    }

    const exitCode = await runCli({
      argv: [
        "repair",
        "--original-count", String(originalCount),
        "--recovery-count", String(recoveryCount),
        "--shard-size", String(shardSize),
        "--shard", `0=${join(tempRoot, "shard_0.bin")}`,
        "--shard", `1=${join(tempRoot, "shard_1.bin")}`,
        "--shard", `2=${join(tempRoot, "shard_2.bin")}`,
        "--shard", `3=${join(tempRoot, "shard_3.bin")}`,
        "--output-dir", join(tempRoot, "out"),
      ],
      stdout: stdout.stream,
      stderr: stderr.stream,
    });

    assert.equal(exitCode, 0);
    assert.equal(stderr.read(), "");
    assert.match(stdout.read(), /No missing shards were inferred/);
  });
});

test("CLI accepts the par3-style repair alias r", async () => {
  await withTempDir(async (tempRoot) => {
    const originalCount = 3;
    const recoveryCount = 1;
    const shardSize = 32;
    const originalShards = buildOriginalShards(originalCount, shardSize);
    const recoveryShards = await buildRecoveryShards(originalShards, recoveryCount);
    const referenceSlots = [...originalShards, ...recoveryShards];
    const outputDir = join(tempRoot, "alias");

    await writeFile(join(tempRoot, "shard_0.bin"), referenceSlots[0]);
    await writeFile(join(tempRoot, "shard_2.bin"), referenceSlots[2]);
    await writeFile(join(tempRoot, "shard_3.bin"), referenceSlots[3]);

    const exitCode = await runCli({
      argv: [
        "r",
        "-n", String(originalCount),
        "-r", String(recoveryCount),
        "-s", String(shardSize),
        "--shard", `0=${join(tempRoot, "shard_0.bin")}`,
        "--shard", `2=${join(tempRoot, "shard_2.bin")}`,
        "--shard", `3=${join(tempRoot, "shard_3.bin")}`,
        "--output-dir", outputDir,
      ],
      stdout: createWritableCapture().stream,
      stderr: createWritableCapture().stream,
    });

    assert.equal(exitCode, 0);
    assert.deepEqual(new Uint8Array(await readFile(join(outputDir, "shard_1.bin"))), referenceSlots[1]);
  });
});

test("CLI help includes the interoperability example", async () => {
  const stdout = createWritableCapture();
  const stderr = createWritableCapture();

  const exitCode = await runCli({
    argv: ["--help"],
    stdout: stdout.stream,
    stderr: stderr.stream,
  });

  assert.equal(exitCode, 0);
  assert.equal(stderr.read(), "");
  assert.match(stdout.read(), /split -b/);
  assert.match(stdout.read(), /cat .*shard_/i);
});

test("CLI prints usage for --help", async () => {
  const stdout = createWritableCapture();
  const stderr = createWritableCapture();

  const exitCode = await runCli({
    argv: ["--help"],
    stdout: stdout.stream,
    stderr: stderr.stream,
  });

  assert.equal(exitCode, 0);
  assert.equal(stderr.read(), "");
  assert.match(stdout.read(), /Usage:/);
});

test("CLI entrypoint works when executed as a process", async () => {
  const { stdout, stderr } = await execFileAsync(
    process.execPath,
    ["--experimental-wasm-modules", mainPath, "--help"],
    {
      cwd: repoRoot,
      env: {
        ...process.env,
        NODE_NO_WARNINGS: "1",
      },
    },
  );

  assert.equal(stderr, "");
  assert.match(stdout, /Usage:/);
});
