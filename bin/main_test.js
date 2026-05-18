import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdtempDisposable, readdir, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

import { runCli } from "./main.js";
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

    leopard_encode(originalCount, shardSize, arenaHandle);

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

test("CLI prints usage for extra positional arguments", async () => {
  const stdout = createWritableCapture();
  const stderr = createWritableCapture();

  const exitCode = await runCli({
    argv: ["repair", "extra"],
    stdout: stdout.stream,
    stderr: stderr.stream,
  });

  assert.equal(exitCode, 0);
  assert.equal(stderr.read(), "");
  assert.match(stdout.read(), /Usage:/);
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
        "--original-count", String(originalCount),
        "--recovery-count", String(recoveryCount),
        "--shard-size", String(shardSize),
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
