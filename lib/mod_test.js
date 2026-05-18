import assert from "node:assert/strict";
import test from "node:test";

import { MAX_ARENA_BYTES, MAX_SHARD_SIZE, MAX_SLOT_COUNT, Par3 } from "./mod.js";
import * as wasmModule from "../pkg/par3_bg.wasm";
import { alloc_shard_arena, free_shard_arena, leopard_encode, shard_arena_ptr } from "../pkg/par3.js";

const { memory } = wasmModule;

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

function buildRecoveryShards(originalShards, recoveryCount) {
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

test("Par3 resolves layouts with inferred and locked slot counts", () => {
  const inferred = Par3.resolveLayout({
    originalCount: 2,
    requestedMissingIndices: [3, 1, 3],
    shardSize: 4,
  });
  const locked = Par3.resolveLayout({
    originalCount: 2,
    recoveryCount: 1,
    shardSize: 4,
  });

  assert.deepEqual(inferred.requestedMissingIndices, [1, 3]);
  assert.equal(inferred.slotCount, 4);
  assert.equal(inferred.slotCountLocked, false);
  assert.equal(locked.slotCount, 3);
  assert.equal(locked.slotCountLocked, true);
});

test("Par3 validates index and layout inputs", () => {
  assert.throws(() => Par3.normalizeIndices("1"), /indices must be an array of integer slot indexes/);
  assert.throws(() => Par3.normalizeIndices([-1], { field: "missing_indices" }), /missing_indices\[\] must be an integer greater than or equal to 0/);
  assert.throws(() => Par3.resolveLayout({ originalCount: 0, shardSize: 4 }), /original_count must be an integer greater than or equal to 1/);
  assert.throws(() => Par3.resolveLayout({ originalCount: 2, shardSize: 3 }), /shard_size must be even/);
  assert.throws(
    () => Par3.resolveLayout({ originalCount: 2, shardSize: 4, totalShardCount: MAX_SLOT_COUNT + 1 }),
    new RegExp(`total_shard_count must be less than or equal to ${MAX_SLOT_COUNT}`),
  );
  assert.throws(
    () => Par3.resolveLayout({ originalCount: 2, shardSize: MAX_SHARD_SIZE + 2, recoveryCount: 1 }),
    new RegExp(`shard_size must be less than or equal to ${MAX_SHARD_SIZE}`),
  );
  assert.throws(
    () => Par3.resolveLayout({
      originalCount: 2,
      shardSize: 8192,
      totalShardCount: Math.floor(MAX_ARENA_BYTES / 8192) + 1,
    }),
    new RegExp(`slot layout exceeds maximum arena size of ${MAX_ARENA_BYTES} bytes`),
  );
});

test("Par3 repairs missing shards and selects requested outputs", () => {
  const originalShards = buildOriginalShards(3, 32);
  const recoveryShards = buildRecoveryShards(originalShards, 2);
  const referenceSlots = [...originalShards, ...recoveryShards];
  const codec = new Par3(
    Par3.resolveLayout({
      originalCount: 3,
      recoveryCount: 2,
      requestedMissingIndices: [4, 1],
      shardSize: 32,
    }),
  );

  try {
    codec.writeShard(0, Array.from(referenceSlots[0]));
    codec.writeShard(2, referenceSlots[2]);
    codec.writeShard(3, referenceSlots[3]);

    assert.equal(codec.thresholdReached(), true);
    assert.deepEqual(codec.currentRequestedMissingIndices(), [1, 4]);
    assert.deepEqual(codec.inferMissingIndices(), [1, 4]);
    assert.deepEqual(codec.selectOutputIndices(), [1, 4]);
    assert.deepEqual(codec.repair(), [1, 4]);
    assert.deepEqual(codec.readShard(1), referenceSlots[1]);
    assert.deepEqual(codec.readShard(4), referenceSlots[4]);
  } finally {
    codec.free();
    codec.free();
  }
});

test("Par3 grows unlocked layouts and preserves existing shard bytes", () => {
  const codec = new Par3(Par3.resolveLayout({ originalCount: 2, shardSize: 4 }));

  try {
    codec.writeShard(0, [0, 1, 2, 3]);
    const initialArenaPtr = codec.arenaPtr;

    codec.ensureCapacity(4);

    assert.equal(codec.slotCount, 4);
    assert.notEqual(codec.arenaPtr, initialArenaPtr);
    assert.deepEqual(codec.readShard(0), Uint8Array.from([0, 1, 2, 3]));
  } finally {
    codec.free();
  }
});

test("Par3 rejects overflow, duplicate writes, invalid requested outputs, and wrong shard sizes", () => {
  const lockedCodec = new Par3(Par3.resolveLayout({ originalCount: 2, recoveryCount: 1, shardSize: 4 }));
  const writableCodec = new Par3(Par3.resolveLayout({ originalCount: 1, recoveryCount: 1, shardSize: 4 }));
  const boundedCodec = new Par3(Par3.resolveLayout({ originalCount: 1, shardSize: 4 }));

  try {
    assert.throws(() => lockedCodec.ensureCapacity(4), /outside declared total_shard_count \(3\)/);

    writableCodec.writeShard(0, Uint8Array.from([0, 1, 2, 3]));
    assert.throws(() => writableCodec.writeShard(0, Uint8Array.from([0, 1, 2, 3])), /duplicate shard received for slot 0/);
    assert.throws(() => writableCodec.writeShard(1, Uint8Array.from([0, 1])), /shard_size is 4/);
    assert.throws(() => writableCodec.selectOutputIndices([0]), /requested missing index 0 was already provided or is outside the inferred missing set/);
    assert.throws(() => boundedCodec.ensureCapacity(MAX_SLOT_COUNT + 1), /total_shard_count must be less than or equal to/);
  } finally {
    lockedCodec.free();
    writableCodec.free();
    boundedCodec.free();
  }
});

test("Par3 handles fully present shard sets without allocating extra outputs", () => {
  const originalShards = buildOriginalShards(2, 16);
  const recoveryShards = buildRecoveryShards(originalShards, 1);
  const referenceSlots = [...originalShards, ...recoveryShards];
  const codec = new Par3(Par3.resolveLayout({ originalCount: 2, recoveryCount: 1, shardSize: 16 }));

  try {
    for (const [index, shard] of referenceSlots.entries()) {
      codec.writeShard(index, shard);
    }

    assert.equal(codec.thresholdReached(), true);
    assert.deepEqual(codec.currentRequestedMissingIndices(), []);
    assert.deepEqual(codec.selectOutputIndices(), []);
    assert.deepEqual(codec.repair(), []);
  } finally {
    codec.free();
  }
});

test("Par3 rejects out-of-range shard reads", () => {
  const codec = new Par3(Par3.resolveLayout({ originalCount: 2, recoveryCount: 1, shardSize: 4 }));

  try {
    codec.writeShard(0, Uint8Array.from([0, 1, 2, 3]));
    assert.throws(() => codec.readShard(3), /slot index 3 is outside slot_count 3/);
  } finally {
    codec.free();
  }
});
