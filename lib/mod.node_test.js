import assert from "node:assert/strict";
import test from "node:test";

import * as modNode from "./mod.node.js";

const { Par3 } = modNode;

function buildOriginalShards(originalCount, shardSize) {
  return Array.from({ length: originalCount }, (_, shardIndex) => {
    const shard = Buffer.alloc(shardSize);
    for (let offset = 0; offset < shardSize; offset += 1) {
      shard[offset] = (shardIndex * 37 + offset * 17) % 251;
    }
    return shard;
  });
}

function buildFilledOriginalShards(originalCount, shardSize) {
  return Array.from({ length: originalCount }, (_, shardIndex) =>
    Buffer.alloc(shardSize, (shardIndex * 37) % 251));
}

async function awaitPending(promise) {
  if (promise) {
    await promise;
  }
}

test("native module keeps bindings private", () => {
  assert.equal(Object.hasOwn(modNode, "loadNativeBindings"), false);
  assert.equal(modNode.loadNativeBindings, undefined);
});

test("native Par3 keeps zero-copy views live while idle and revokes them on arena growth", () => {
  const codec = new Par3({
    originalCount: 1,
    shardSize: 4,
  });

  try {
    const arenaView = codec.arenaView(0, 4);
    arenaView[0] = 3;
    assert.equal(codec.readShard(0)[0], 3);
    assert.equal(codec.arenaView(1, 2).byteLength, 2);

    assert.equal(arenaView.fill(7), arenaView);
    const subview = arenaView.subarray(1, 3);
    subview[0] = 9;
    assert.deepEqual(Array.from(codec.memoryView(0, 4)), [7, 9, 7, 7]);

    const copiedValue = arenaView.valueOf();
    copiedValue[0] = 99;
    assert.deepEqual(Array.from(codec.readShard(0)), [7, 9, 7, 7]);
    assert.deepEqual(Buffer.from(codec.shardView(0)), Buffer.from([7, 9, 7, 7]));

    const retainedView = codec.shardView(0);
    codec.writeShard(1, Uint8Array.from([1, 2, 3, 4]));

    assert.throws(
      () => retainedView.byteLength,
      /invalidated by arena growth/u,
    );
    assert.deepEqual(Array.from(codec.readShard(0)), [7, 9, 7, 7]);
    assert.deepEqual(Array.from(codec.readShard(1)), [1, 2, 3, 4]);
  } finally {
    codec.free();
  }
});

test("native Par3 free logically invalidates arenas and retained views", () => {
  const codec = new Par3({
    originalCount: 1,
    shardSize: 4,
  });

  try {
    codec.writeShard(0, Uint8Array.from([1, 2, 3, 4]));

    const retainedView = codec.shardView(0);
    const retainedArenaView = codec.arenaView(0, 4);

    codec.free();

    assert.throws(
      () => retainedView.byteLength,
      /invalidated by free\(\)/u,
    );
    assert.throws(
      () => retainedArenaView[0],
      /invalidated by free\(\)/u,
    );
    assert.deepEqual(Array.from(codec.arenaView(0, 4)), [0, 0, 0, 0]);
  } finally {
    codec.free();
  }
});

test("native Par3 validates write growth, duplicates, sizes, and read bounds", () => {
  const lockedCodec = new Par3({
    originalCount: 1,
    recoveryCount: 0,
    shardSize: 4,
  });
  const codec = new Par3({
    originalCount: 1,
    recoveryCount: 1,
    shardSize: 4,
  });

  try {
    assert.throws(
      () => lockedCodec.writeShard(1, Uint8Array.from([1, 2, 3, 4])),
      /encountered slot index outside declared total_shard_count/,
    );

    codec.writeShard(0, Uint8Array.from([1, 2, 3, 4]));
    assert.throws(
      () => codec.writeShard(0, Uint8Array.from([4, 3, 2, 1])),
      /duplicate shard received for slot 0/,
    );
    assert.throws(
      () => codec.writeShard(1, Uint8Array.from([1, 2])),
      /slot 1 is 2 bytes but shard_size is 4/,
    );
    assert.throws(
      () => codec.readShard(2),
      /slot index 2 is outside slot_count 2/,
    );
  } finally {
    lockedCodec.free();
    codec.free();
  }
});

test("native Par3 preserves scalar proxy results and custom validation messages", () => {
  const codec = new Par3({
    originalCount: 1,
    recoveryCount: 0,
    shardSize: 4,
  });
  const arrayCodec = new Par3({
    originalCount: 1,
    recoveryCount: 0,
    shardSize: 4,
  });

  try {
    const arenaView = codec.arenaView(0, 4);
    arenaView.fill(5);
    assert.equal(arenaView.readUInt8(0), 5);

    codec.writeShard(0, Uint8Array.from([1, 2, 3, 4]));
    assert.throws(
      () => codec.writeShard(0, Uint8Array.from([4, 3, 2, 1]), { duplicateMessage: "custom duplicate" }),
      /custom duplicate/,
    );
    assert.throws(
      () => codec.writeShard(1, Uint8Array.from([1, 2]), { overflowMessage: "custom overflow" }),
      /custom overflow/,
    );

    const unlockedCodec = new Par3({
      originalCount: 1,
      shardSize: 4,
    });

    try {
      assert.throws(
        () => unlockedCodec.writeShard(0, Uint8Array.from([1, 2]), { sizeMessage: "custom size" }),
        /custom size/,
      );
    } finally {
      unlockedCodec.free();
    }

    arrayCodec.writeShard(0, [6, 7, 8, 9]);
    assert.deepEqual(Array.from(arrayCodec.readShard(0)), [6, 7, 8, 9]);
  } finally {
    arrayCodec.free();
    codec.free();
  }
});

test("native Par3 handles no-op and invalid encode and repair paths", async () => {
  const noRecoveryCodec = new Par3({
    originalCount: 1,
    recoveryCount: 0,
    shardSize: 4,
  });
  const incompleteCodec = new Par3({
    originalCount: 2,
    recoveryCount: 1,
    shardSize: 4,
  });

  try {
    noRecoveryCodec.writeShard(0, Uint8Array.from([1, 2, 3, 4]));
    assert.deepEqual(await noRecoveryCodec.encode(), []);
    assert.deepEqual(await noRecoveryCodec.repair(), []);

    incompleteCodec.writeShard(0, Uint8Array.from([1, 2, 3, 4]));
    await assert.rejects(
      incompleteCodec.encode(),
      /original shard 1 must be written before encode/,
    );
    await assert.rejects(
      incompleteCodec.repair(),
      /Insufficient shards provided for repair/,
    );
  } finally {
    noRecoveryCodec.free();
    incompleteCodec.free();
  }
});

test("native Par3 encodes recovery shards in place", async () => {
  const originalShards = buildOriginalShards(3, 32);
  const codec = new Par3({
    originalCount: 3,
    recoveryCount: 2,
    shardSize: 32,
  });

  try {
    for (const [index, shard] of originalShards.entries()) {
      codec.writeShard(index, shard);
    }

    assert.deepEqual(await codec.encode(), [3, 4]);
    assert.equal(codec.thresholdReached(), true);
    assert.equal(codec.shardView(3).byteLength, 32);
    assert.equal(codec.shardView(4).byteLength, 32);
  } finally {
    codec.free();
  }
});

test("native Par3 repairs requested missing shards", async () => {
  const originalShards = buildOriginalShards(3, 32);
  const encodedCodec = new Par3({
    originalCount: 3,
    recoveryCount: 2,
    shardSize: 32,
  });

  try {
    for (const [index, shard] of originalShards.entries()) {
      encodedCodec.writeShard(index, shard);
    }

    await encodedCodec.encode();

    const expectedMissing = new Map([
      [1, Buffer.from(encodedCodec.shardView(1))],
      [4, Buffer.from(encodedCodec.shardView(4))],
    ]);

    const repairCodec = new Par3({
      originalCount: 3,
      recoveryCount: 2,
      requestedMissingIndices: [1, 4],
      shardSize: 32,
    });

    try {
      repairCodec.writeShard(0, encodedCodec.shardView(0));
      repairCodec.writeShard(2, encodedCodec.shardView(2));
      repairCodec.writeShard(3, encodedCodec.shardView(3));

      assert.deepEqual(await repairCodec.repair(), [1, 4]);
      assert.deepEqual(Buffer.from(repairCodec.readShard(1)), expectedMissing.get(1));
      assert.deepEqual(Buffer.from(repairCodec.readShard(4)), expectedMissing.get(4));
    } finally {
      repairCodec.free();
    }
  } finally {
    encodedCodec.free();
  }
});

test("native Par3 encode runs off the event loop", async () => {
  const originalCount = 24;
  const recoveryCount = 8;
  const shardSize = 4 * 1024 * 1024;
  const codec = new Par3({
    originalCount,
    recoveryCount,
    shardSize,
  });

  try {
    for (const [index, shard] of buildFilledOriginalShards(originalCount, shardSize).entries()) {
      codec.writeShard(index, shard);
    }

    let timeoutResolved = false;
    let encodeResolved = false;
    const timeoutPromise = new Promise((resolve) => {
      setTimeout(() => {
        timeoutResolved = true;
        resolve("timer");
      }, 10);
    });
    const encodePromise = codec.encode().then((recoveryIndices) => {
      encodeResolved = true;
      return recoveryIndices;
    });

    const firstResolved = await Promise.race([
      timeoutPromise,
      encodePromise.then(() => "encode"),
    ]);

    assert.equal(firstResolved, "timer");
    assert.equal(timeoutResolved, true);
    assert.equal(encodeResolved, false);
    assert.deepEqual(
      await encodePromise,
      Array.from({ length: recoveryCount }, (_, index) => originalCount + index),
    );
  } finally {
    codec.free();
  }
});

test("native Par3 revokes retained zero-copy views while processing", async () => {
  const originalCount = 24;
  const recoveryCount = 8;
  const shardSize = 4 * 1024 * 1024;
  const codec = new Par3({
    originalCount,
    recoveryCount,
    shardSize,
  });
  let encodePromise;

  try {
    for (const [index, shard] of buildFilledOriginalShards(originalCount, shardSize).entries()) {
      codec.writeShard(index, shard);
    }

    const retainedView = codec.shardView(0);
    const readUInt8 = retainedView.readUInt8;
    encodePromise = codec.encode();

    assert.throws(
      () => {
        retainedView[0] = 255;
      },
      /invalidated before background processing/u,
    );
    assert.throws(
      () => retainedView.readUInt8(0),
      /invalidated before background processing/u,
    );
    assert.throws(
      () => readUInt8(0),
      /invalidated before background processing/u,
    );

    await encodePromise;
  } finally {
    await awaitPending(encodePromise);
    codec.free();
  }
});

test("native Par3 blocks the ArrayBuffer escape hatch while processing", async () => {
  const originalCount = 24;
  const recoveryCount = 8;
  const shardSize = 4 * 1024 * 1024;
  const codec = new Par3({
    originalCount,
    recoveryCount,
    shardSize,
  });
  let encodePromise;

  try {
    for (const [index, shard] of buildFilledOriginalShards(originalCount, shardSize).entries()) {
      codec.writeShard(index, shard);
    }

    const retainedView = codec.shardView(0);
    const escapedBuffer = retainedView.buffer;
    const escapedAlias = new Uint8Array(escapedBuffer);

    assert.equal(escapedBuffer.byteLength, 0);
    assert.equal(escapedAlias.byteLength, 0);
    assert.throws(
      () => new Uint8Array(escapedBuffer, retainedView.byteOffset, retainedView.byteLength),
      RangeError,
    );

    encodePromise = codec.encode();
    escapedAlias[0] = 255;

    assert.equal(escapedAlias[0], undefined);
    await encodePromise;
    assert.equal(codec.readShard(0)[0], 0);
  } finally {
    await awaitPending(encodePromise);
    codec.free();
  }
});

test("native Par3 deadens structural metadata and iterators while processing", async () => {
  const originalCount = 24;
  const recoveryCount = 8;
  const shardSize = 4 * 1024 * 1024;
  const codec = new Par3({
    originalCount,
    recoveryCount,
    shardSize,
  });
  let encodePromise;

  try {
    for (const [index, shard] of buildFilledOriginalShards(originalCount, shardSize).entries()) {
      codec.writeShard(index, shard);
    }

    const retainedView = codec.shardView(0);
    const retainedIterator = retainedView.values();
  const next = retainedIterator.next;

    assert.equal(retainedIterator[Symbol.iterator](), retainedIterator);
  assert.equal(retainedIterator.length, undefined);

    encodePromise = codec.encode();

    assert.equal(retainedView.buffer.byteLength, 0);
    assert.equal(retainedView.parent.byteLength, 0);
    assert.equal(retainedView.byteLength, 0);
    assert.equal(retainedView.byteOffset, 0);
    assert.equal(retainedView.offset, 0);
    assert.throws(
      () => retainedIterator.next(),
      /invalidated before background processing/u,
    );
    assert.throws(
      () => next(),
      /invalidated before background processing/u,
    );

    await encodePromise;
  } finally {
    await awaitPending(encodePromise);
    codec.free();
  }
});

test("native Par3 guards all direct arena access while processing", async () => {
  const originalCount = 24;
  const recoveryCount = 8;
  const shardSize = 4 * 1024 * 1024;
  const codec = new Par3({
    originalCount,
    recoveryCount,
    shardSize,
  });
  let encodePromise;

  try {
    assert.equal(codec.arena, undefined);

    for (const [index, shard] of buildFilledOriginalShards(originalCount, shardSize).entries()) {
      codec.writeShard(index, shard);
    }

    encodePromise = codec.encode();

    assert.throws(
      () => codec.arenaView(0, shardSize),
      /Par3 codec is currently processing on a background thread/,
    );
    assert.throws(
      () => codec.memoryView(0, shardSize),
      /Par3 codec is currently processing on a background thread/,
    );
    assert.throws(
      () => codec.shardView(0),
      /Par3 codec is currently processing on a background thread/,
    );
    assert.throws(
      () => codec.readShard(0),
      /Par3 codec is currently processing on a background thread/,
    );
    assert.throws(
      () => codec.writeShard(0, Buffer.alloc(shardSize)),
      /Par3 codec is currently processing on a background thread/,
    );
    assert.throws(
      () => codec.free(),
      /Par3 codec is currently processing on a background thread/,
    );

    await encodePromise;
  } finally {
    await awaitPending(encodePromise);
    codec.free();
  }
});
