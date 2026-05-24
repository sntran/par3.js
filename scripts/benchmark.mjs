import assert from "node:assert/strict";
import { randomFillSync } from "node:crypto";
import { performance } from "node:perf_hooks";

const PAYLOAD_BYTES = 250 * 1024 * 1024;
const ORIGINAL_COUNT = 100;
const RECOVERY_COUNT = 20;
const MISSING_INDICES = [
  0,
  10,
  20,
  30,
  40,
  50,
  60,
  70,
  80,
  90,
  100,
  102,
  104,
  106,
  108,
  110,
  112,
  114,
  116,
  118,
];

function toMegabytes(byteLength) {
  return byteLength / (1024 * 1024);
}

function formatMilliseconds(value) {
  return Number(value.toFixed(2));
}

function formatThroughput(payloadBytes, elapsedMs) {
  return Number((toMegabytes(payloadBytes) / (elapsedMs / 1000)).toFixed(2));
}

function buildPayload(byteLength) {
  const payload = Buffer.allocUnsafe(byteLength);
  randomFillSync(payload);
  return payload;
}

function resolveShardSize(payloadBytes, originalCount) {
  const shardSize = Math.ceil(payloadBytes / originalCount);
  return shardSize % 2 === 0 ? shardSize : shardSize + 1;
}

function splitOriginalShards(payload, originalCount, shardSize) {
  const padded = Buffer.alloc(originalCount * shardSize);
  payload.copy(padded);

  return Array.from({ length: originalCount }, (_, shardIndex) => {
    const start = shardIndex * shardSize;
    return Buffer.from(padded.subarray(start, start + shardSize));
  });
}

async function benchmarkImplementation(label, Par3Class, originalShards, layout) {
  const encodeCodec = new Par3Class(layout);
  try {
    for (const [index, shard] of originalShards.entries()) {
      encodeCodec.writeShard(index, shard);
    }

    const encodeStartedAt = performance.now();
    await encodeCodec.encode();
    const encodeMs = performance.now() - encodeStartedAt;

    const expectedMissingShards = new Map(
      MISSING_INDICES.map((index) => [index, Buffer.from(encodeCodec.shardView(index))]),
    );

    const repairCodec = new Par3Class({
      ...layout,
      requestedMissingIndices: MISSING_INDICES,
    });

    try {
      for (let slotIndex = 0; slotIndex < layout.originalCount + RECOVERY_COUNT; slotIndex += 1) {
        if (expectedMissingShards.has(slotIndex)) {
          continue;
        }

        repairCodec.writeShard(slotIndex, Buffer.from(encodeCodec.shardView(slotIndex)));
      }

      const repairStartedAt = performance.now();
      const repairedIndices = await repairCodec.repair();
      const repairMs = performance.now() - repairStartedAt;

      assert.deepEqual(repairedIndices, MISSING_INDICES);

      for (const [index, expectedShard] of expectedMissingShards) {
        assert.deepEqual(
          Buffer.from(repairCodec.readShard(index)),
          expectedShard,
          `${label} repaired shard ${index} mismatch`,
        );
      }

      return {
        label,
        encodeMs,
        repairMs,
        encodeThroughput: formatThroughput(PAYLOAD_BYTES, encodeMs),
        repairThroughput: formatThroughput(PAYLOAD_BYTES, repairMs),
      };
    } finally {
      repairCodec.free();
    }
  } finally {
    encodeCodec.free();
  }
}

async function main() {
  const [{ Par3: WasmPar3 }, { Par3: NativePar3 }] = await Promise.all([
    import(new URL("../lib/mod.js", import.meta.url)),
    import(new URL("../lib/mod.node.js", import.meta.url)),
  ]);

  const shardSize = resolveShardSize(PAYLOAD_BYTES, ORIGINAL_COUNT);
  const arenaBytes = (ORIGINAL_COUNT + RECOVERY_COUNT) * shardSize;
  const payload = buildPayload(PAYLOAD_BYTES);
  const originalShards = splitOriginalShards(payload, ORIGINAL_COUNT, shardSize);
  const layout = {
    originalCount: ORIGINAL_COUNT,
    recoveryCount: RECOVERY_COUNT,
    shardSize,
  };

  WasmPar3.MAX_ARENA_BYTES = Math.max(WasmPar3.MAX_ARENA_BYTES, arenaBytes);
  NativePar3.MAX_ARENA_BYTES = Math.max(NativePar3.MAX_ARENA_BYTES, arenaBytes);

  const wasmResult = await benchmarkImplementation("WASM", WasmPar3, originalShards, layout);
  const nativeResult = await benchmarkImplementation("Native", NativePar3, originalShards, layout);

  console.log(`Payload: ${toMegabytes(PAYLOAD_BYTES).toFixed(0)} MiB`);
  console.log(`Layout: ${ORIGINAL_COUNT} original / ${RECOVERY_COUNT} recovery / shard_size=${shardSize}`);
  console.log(`Arena: ${toMegabytes(arenaBytes).toFixed(0)} MiB`);

  console.table([
    {
      implementation: wasmResult.label,
      encodeMs: formatMilliseconds(wasmResult.encodeMs),
      encodeMBps: wasmResult.encodeThroughput,
      repairMs: formatMilliseconds(wasmResult.repairMs),
      repairMBps: wasmResult.repairThroughput,
      encodeSpeedupVsWasm: "1.00x",
      repairSpeedupVsWasm: "1.00x",
    },
    {
      implementation: nativeResult.label,
      encodeMs: formatMilliseconds(nativeResult.encodeMs),
      encodeMBps: nativeResult.encodeThroughput,
      repairMs: formatMilliseconds(nativeResult.repairMs),
      repairMBps: nativeResult.repairThroughput,
      encodeSpeedupVsWasm: `${(wasmResult.encodeMs / nativeResult.encodeMs).toFixed(2)}x`,
      repairSpeedupVsWasm: `${(wasmResult.repairMs / nativeResult.repairMs).toFixed(2)}x`,
    },
  ]);
}

await main();
